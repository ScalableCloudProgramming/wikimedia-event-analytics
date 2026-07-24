# Fallback batch aggregator (boto3 + pandas) writes parquet to S3 for Athena
# Used if EMR step fails; same aggregates as batch_job.py
import io
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone

import boto3
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

STOP = {
    "the", "a", "an", "of", "and", "in", "to", "is", "it", "for",
    "on", "at", "by", "with", "from", "as", "or", "that", "this",
}
TOP_N = config.TOP_N
EXCLUDE_BOTS = os.getenv("EXCLUDE_BOTS", "true").lower() in ("1", "true", "yes")

s3 = boto3.client("s3", region_name=config.AWS_REGION)


def list_keys(bucket, prefix):
    token = None
    keys = []
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            if o["Key"].endswith((".jsonl", ".json")):
                keys.append(o["Key"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def load_records(bucket, keys, limit_keys=None):
    rows = []
    if limit_keys:
        keys = keys[:limit_keys]
    for i, key in enumerate(keys):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="ignore")
        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        if (i + 1) % 50 == 0:
            print(f"  loaded {i+1}/{len(keys)} files, {len(rows)} records")
    return rows


def write_parquet_df(df, bucket, prefix):
    # single-file parquet under prefix/
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    key = prefix.rstrip("/") + "/part-00000.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  wrote s3://{bucket}/{key} rows={len(df)}")


def keywords(title):
    if not title:
        return []
    return [w for w in re.findall(r"[a-zA-Z]{3,}", str(title).lower()) if w not in STOP]


def run():
    prefix = config.S3_RAW_PREFIX.lstrip("/")
    print(f"Scanning s3://{config.S3_RAW}/{prefix}")
    keys = list_keys(config.S3_RAW, prefix)
    print(f"Found {len(keys)} jsonl objects")
    if not keys:
        raise SystemExit("No raw data on S3")

    rows = load_records(config.S3_RAW, keys)
    print(f"Total records: {len(rows)}")
    df = pd.DataFrame(rows)
    if EXCLUDE_BOTS and "bot" in df.columns:
        df = df[df["bot"].fillna(False) != True]  # noqa: E712
        print(f"After bot filter: {len(df)}")

    out_bucket = config.S3_BATCH
    base = config.S3_BATCH_PREFIX.lstrip("/")

    # wiki edits
    wiki = (
        df.groupby("wiki").size().reset_index(name="edits")
        .sort_values("edits", ascending=False).head(TOP_N)
    )
    write_parquet_df(wiki, out_bucket, f"{base}wiki_edits")

    # keywords
    if "title" in df.columns:
        kw_counter = Counter()
        for t in df["title"].dropna():
            kw_counter.update(keywords(t))
        kw = pd.DataFrame(kw_counter.most_common(TOP_N), columns=["kw", "freq"])
        write_parquet_df(kw, out_bucket, f"{base}keywords")

    # hourly
    if "timestamp" in df.columns:
        ts = pd.to_numeric(df["timestamp"], errors="coerce").dropna()
        hours = pd.to_datetime(ts, unit="s", utc=True).dt.strftime("%Y-%m-%d %H")
        hourly = hours.value_counts().sort_index().reset_index()
        hourly.columns = ["hour", "edits"]
        write_parquet_df(hourly, out_bucket, f"{base}hourly")

    # edit types
    if "type" in df.columns:
        et = df.groupby("type").size().reset_index(name="edits").sort_values("edits", ascending=False)
        write_parquet_df(et, out_bucket, f"{base}edit_types")

    print("Local batch complete.")
    return len(rows)


if __name__ == "__main__":
    run()
