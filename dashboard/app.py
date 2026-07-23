# Streamlit results dashboard — runs on AWS EC2 only
# Reads DynamoDB (speed), Athena (batch), CloudWatch (throughput), S3 (benchmark plots)
import os
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

st.set_page_config(
    page_title="Wikimedia Lambda Analytics",
    page_icon="📊",
    layout="wide",
)

REGION = config.AWS_REGION
cw = boto3.client("cloudwatch", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
dynamo = boto3.resource("dynamodb", region_name=REGION)
kinesis = boto3.client("kinesis", region_name=REGION)
emr = boto3.client("emr", region_name=REGION)


@st.cache_data(ttl=30)
def speed_view(top_n=None):
    top_n = top_n or config.TOP_N
    table = dynamo.Table(config.DYNAMO_TABLE)
    rows = []
    for rank in range(1, top_n + 1):
        item = table.get_item(Key={"pk": f"speed#{rank}"}).get("Item")
        if item:
            rows.append({
                "rank": rank,
                "wiki": item.get("wiki"),
                "count": int(item.get("count", 0)),
                "ts": item.get("ts"),
                "window_s": int(item.get("window", config.WINDOW_SECONDS)),
            })
    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def batch_view(top_n=None):
    top_n = top_n or config.TOP_N
    try:
        from pyathena import connect
        cur = connect(
            s3_staging_dir=config.S3_ATHENA_OUT,
            region_name=REGION,
        ).cursor()
        cur.execute(
            f"SELECT wiki, edits FROM {config.ATHENA_DB}.{config.ATHENA_TABLE} "
            f"ORDER BY edits DESC LIMIT {top_n}"
        )
        return pd.DataFrame(cur.fetchall(), columns=["wiki", "edits"])
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


def lambda_merge(batch_df, speed_df, top_n=None):
    top_n = top_n or config.TOP_N
    counts = {}
    if not batch_df.empty and "wiki" in batch_df.columns and "error" not in batch_df.columns:
        for _, r in batch_df.iterrows():
            counts[str(r["wiki"])] = int(r["edits"])
    if not speed_df.empty and "wiki" in speed_df.columns:
        for _, r in speed_df.iterrows():
            w = str(r["wiki"])
            counts[w] = counts.get(w, 0) + int(r["count"])
    rows = sorted(counts.items(), key=lambda x: -x[1])[:top_n]
    return pd.DataFrame(rows, columns=["wiki", "edits_merged"])


@st.cache_data(ttl=45)
def kinesis_throughput(minutes=30):
    end = datetime.now(timezone.utc)
    start = end - pd.Timedelta(minutes=minutes)
    resp = cw.get_metric_statistics(
        Namespace="AWS/Kinesis",
        MetricName="IncomingRecords",
        Dimensions=[{"Name": "StreamName", "Value": config.KINESIS_STREAM}],
        StartTime=start,
        EndTime=end,
        Period=60,
        Statistics=["Sum"],
    )
    pts = sorted(resp["Datapoints"], key=lambda x: x["Timestamp"])
    if not pts:
        return pd.DataFrame(columns=["time", "records_per_min"])
    return pd.DataFrame({
        "time": [p["Timestamp"] for p in pts],
        "records_per_min": [p["Sum"] for p in pts],
    })


@st.cache_data(ttl=60)
def stream_status():
    try:
        d = kinesis.describe_stream(StreamName=config.KINESIS_STREAM)["StreamDescription"]
        return {
            "status": d.get("StreamStatus"),
            "shards": len(d.get("Shards", [])),
            "arn": d.get("StreamARN", ""),
        }
    except Exception as e:
        return {"status": f"error: {e}", "shards": 0, "arn": ""}


@st.cache_data(ttl=60)
def emr_status():
    cid = config.EMR_CLUSTER_ID
    if not cid:
        return {"state": "EMR_CLUSTER_ID not set", "instances": "—"}
    try:
        c = emr.describe_cluster(ClusterId=cid)["Cluster"]
        return {
            "state": c["Status"]["State"],
            "name": c.get("Name"),
            "instances": c.get("InstanceCollectionType", "INSTANCE_GROUP"),
            "normalized_hours": c.get("NormalizedInstanceHours", 0),
        }
    except Exception as e:
        return {"state": str(e), "instances": "—"}


@st.cache_data(ttl=90)
def s3_raw_object_count(max_keys=5000):
    # Approximate volume of batch input on S3
    token = None
    n = 0
    size = 0
    while True:
        kwargs = {
            "Bucket": config.S3_RAW,
            "Prefix": config.S3_RAW_PREFIX.lstrip("/"),
            "MaxKeys": 1000,
        }
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            n += 1
            size += obj.get("Size", 0)
            if n >= max_keys:
                return n, size, True
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return n, size, False


def list_benchmark_plots():
    prefix = "benchmarks/results/"
    try:
        resp = s3.list_objects_v2(Bucket=config.S3_BATCH, Prefix=prefix)
        keys = [
            o["Key"] for o in resp.get("Contents", [])
            if o["Key"].endswith((".png", ".csv"))
        ]
        return keys
    except Exception:
        return []


# ---------- UI ----------
st.title("Wikimedia Event Analytics — Lambda Pipeline")
st.caption(
    f"Region `{REGION}` · Stream `{config.KINESIS_STREAM}` · "
    f"Speed window `{config.WINDOW_SECONDS}s` · Top-N `{config.TOP_N}`"
)

col_a, col_b, col_c, col_d = st.columns(4)
st_stream = stream_status()
st_emr = emr_status()
try:
    obj_n, obj_sz, truncated = s3_raw_object_count()
except Exception as e:
    obj_n, obj_sz, truncated = 0, 0, False
    st.sidebar.warning(f"S3 list failed: {e}")

with col_a:
    st.metric("Kinesis", st_stream.get("status", "?"), f"{st_stream.get('shards', 0)} shards")
with col_b:
    st.metric("EMR", st_emr.get("state", "?")[:24])
with col_c:
    st.metric("S3 raw objects", f"{obj_n}{'+' if truncated else ''}", f"{obj_sz / 1e6:.1f} MB")
with col_d:
    thr = kinesis_throughput(15)
    last = int(thr["records_per_min"].iloc[-1]) if not thr.empty else 0
    st.metric("Ingest (last min)", f"{last:,} rec")

if st.button("Refresh all"):
    st.cache_data.clear()
    st.rerun()

tab_merge, tab_speed, tab_batch, tab_perf, tab_about = st.tabs([
    "Lambda merge", "Speed layer", "Batch layer", "Performance", "Architecture",
])

with tab_speed:
    st.subheader("Speed view — last 5 minutes (DynamoDB)")
    sdf = speed_view()
    if sdf.empty:
        st.info("No speed data yet. Start the EC2 speed consumer.")
    else:
        st.dataframe(sdf, use_container_width=True)
        st.bar_chart(sdf.set_index("wiki")["count"])

with tab_batch:
    st.subheader("Batch view — full history (Athena / S3 parquet)")
    bdf = batch_view()
    if bdf.empty or "error" in bdf.columns:
        st.warning("Batch/Athena empty or not ready. Run batch job + setup_athena.")
        if "error" in bdf.columns:
            st.code(bdf["error"].iloc[0])
    else:
        st.dataframe(bdf, use_container_width=True)
        st.bar_chart(bdf.set_index("wiki")["edits"])

with tab_merge:
    st.subheader("Serving layer — batch + speed merge")
    sdf = speed_view()
    bdf = batch_view()
    if "error" in bdf.columns:
        bdf = pd.DataFrame(columns=["wiki", "edits"])
    mdf = lambda_merge(bdf, sdf)
    if mdf.empty:
        st.info("Waiting for batch and/or speed data…")
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            st.bar_chart(mdf.set_index("wiki")["edits_merged"])
        with c2:
            st.dataframe(mdf, use_container_width=True)
        st.download_button(
            "Download merged CSV",
            mdf.to_csv(index=False),
            file_name="merged_top_wikis.csv",
            mime="text/csv",
        )

with tab_perf:
    st.subheader("Kinesis throughput (CloudWatch IncomingRecords)")
    thr = kinesis_throughput(60)
    if thr.empty:
        st.info("No CloudWatch datapoints yet — run high-scale ingest on EC2.")
    else:
        chart = thr.set_index("time")
        st.line_chart(chart["records_per_min"])
        st.caption(f"Peak: {int(thr['records_per_min'].max()):,} records/min")

    st.subheader("Benchmark artefacts on S3")
    keys = list_benchmark_plots()
    if not keys:
        st.caption(f"Upload plots to s3://{config.S3_BATCH}/benchmarks/results/")
    else:
        for k in keys:
            st.write(f"`s3://{config.S3_BATCH}/{k}`")
            if k.endswith(".png"):
                try:
                    obj = s3.get_object(Bucket=config.S3_BATCH, Key=k)
                    st.image(obj["Body"].read(), caption=k)
                except Exception as e:
                    st.write(e)

with tab_about:
    st.markdown(
        f"""
### Lambda architecture (live on AWS)

| Layer | AWS service | Role |
|-------|-------------|------|
| Ingest | Kinesis + EC2 producer | Live Wikimedia SSE, high-scale fan-out |
| Batch | EMR Spark + S3 + Athena | Full-history top wikis / keywords / hourly |
| Speed | EC2 consumer + DynamoDB | 5-min sliding window top-N |
| Serve | This Streamlit (EC2) | Merge batch + speed, charts |
| Scale | EMR managed scaling | 2–{config.EMR_MAX_INSTANCES} instances on YARN pressure |

**Real-time question:** Which Wikipedia projects get the most edits in the last 5 minutes, over full history, and merged?
"""
    )

st.sidebar.header("Config")
st.sidebar.code(
    f"S3_RAW={config.S3_RAW}\n"
    f"S3_BATCH={config.S3_BATCH}\n"
    f"DYNAMO={config.DYNAMO_TABLE}\n"
    f"ATHENA={config.ATHENA_DB}.{config.ATHENA_TABLE}\n"
    f"EMR={config.EMR_CLUSTER_ID or '(unset)'}"
)
st.sidebar.caption("Dashboard runs on EC2 — uses instance role credentials.")
