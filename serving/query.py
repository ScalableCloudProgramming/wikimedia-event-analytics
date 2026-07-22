# Serving layer: Lambda merge of batch (Athena) + speed (DynamoDB)
# Optional --plot writes a bar chart of the merged top-N
import argparse
import os
import sys

import boto3
from pyathena import connect

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

table = boto3.resource("dynamodb", region_name=config.AWS_REGION).Table(
    config.DYNAMO_TABLE
)


def get_speed_view():
    # Current top-N from DynamoDB speed layer
    results = []
    for rank in range(1, config.TOP_N + 1):
        item = table.get_item(Key={"pk": f"speed#{rank}"}).get("Item")
        if item:
            results.append((item["wiki"], int(item["count"])))
    return results


def get_batch_view():
    # Historical top-N from Athena over batch parquet
    cursor = connect(
        s3_staging_dir=config.S3_ATHENA_OUT,
        region_name=config.AWS_REGION,
    ).cursor()
    cursor.execute(
        f"SELECT wiki, edits FROM {config.ATHENA_DB}.{config.ATHENA_TABLE} "
        f"ORDER BY edits DESC LIMIT {config.TOP_N}"
    )
    return cursor.fetchall()


def merge(batch, speed):
    # batch = full history; speed = recent delta not yet in batch
    counts = {}
    for wiki, edits in batch:
        counts[wiki] = int(edits)
    for wiki, delta in speed:
        counts[wiki] = counts.get(wiki, 0) + int(delta)
    return sorted(counts.items(), key=lambda x: -x[1])[: config.TOP_N]


def plot_merged(merged, out_path):
    import matplotlib.pyplot as plt

    if not merged:
        print("Nothing to plot")
        return
    wikis = [w for w, _ in merged][::-1]
    counts = [c for _, c in merged][::-1]
    plt.figure(figsize=(8, 4.5))
    plt.barh(wikis, counts, color="steelblue")
    plt.xlabel("Edits (batch + speed)")
    plt.title(f"Top-{len(merged)} Wikipedia projects (Lambda merge)")
    plt.tight_layout()
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved {out_path}")


def query(plot=False, plot_path=None):
    speed = get_speed_view()
    try:
        batch = get_batch_view()
    except Exception as e:
        print(f"Batch/Athena unavailable ({e}); serving speed-only view")
        batch = []

    merged = merge(batch, speed)

    print(f"\nSpeed view ({len(speed)}): {speed[:5]}")
    print(f"Batch view ({len(batch)}): {batch[:5]}")
    print(f"\nTop-{config.TOP_N} (batch + speed merged):")
    for i, (wiki, count) in enumerate(merged, 1):
        print(f"  {i:2}. {wiki:<20} {count:,}")

    if plot:
        path = plot_path or os.path.join(
            os.path.dirname(__file__), "..", "benchmarks", "results", "merged_topn.png"
        )
        plot_merged(merged, path)
    return merged


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Query Lambda serving layer")
    p.add_argument("--plot", action="store_true", help="Save bar chart of merged view")
    p.add_argument("--plot-path", default=None)
    args = p.parse_args()
    query(plot=args.plot, plot_path=args.plot_path)
