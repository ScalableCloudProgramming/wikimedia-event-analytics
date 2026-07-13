import boto3, os, time
from pyathena import connect
from dotenv import load_dotenv

load_dotenv()

REGION      = os.getenv("AWS_REGION", "eu-west-1")
DYNAMO      = os.getenv("DYNAMO_TABLE", "wikimedia-speed-view")
ATHENA_DB   = os.getenv("ATHENA_DB", "wikimedia_pipeline")
ATHENA_TBL  = os.getenv("ATHENA_TABLE", "batch_view")
S3_OUT      = os.getenv("S3_ATHENA_OUT", "s3://wikimedia-pipeline-batch/athena-results/")
TOP_N       = int(os.getenv("TOP_N", "10"))

table = boto3.resource("dynamodb", region_name=REGION).Table(DYNAMO)


def get_speed_view():
    """Pull current top-N from DynamoDB (speed layer)."""
    results = []
    for rank in range(1, TOP_N + 1):
        item = table.get_item(Key={"pk": f"speed#{rank}"}).get("Item")
        if item:
            results.append((item["wiki"], int(item["count"])))
    return results


def get_batch_view():
    """Query Athena for historical top-N edit counts."""
    cursor = connect(
        s3_staging_dir=S3_OUT,
        region_name=REGION
    ).cursor()
    cursor.execute(
        f"SELECT wiki, edits FROM {ATHENA_DB}.{ATHENA_TBL} ORDER BY edits DESC LIMIT {TOP_N}"
    )
    return cursor.fetchall()   # list of (wiki, edits)


def merge(batch, speed):
    """
    Lambda merge: combine batch (full history) with speed (recent delta).
    Speed view covers events not yet absorbed into the batch layer.
    """
    counts = {}
    for wiki, edits in batch:
        counts[wiki] = edits
    for wiki, delta in speed:
        counts[wiki] = counts.get(wiki, 0) + delta
    return sorted(counts.items(), key=lambda x: -x[1])[:TOP_N]


def query():
    speed = get_speed_view()
    batch = get_batch_view()
    merged = merge(batch, speed)

    print(f"\nTop-{TOP_N} (batch + speed merged):")
    for i, (wiki, count) in enumerate(merged, 1):
        print(f"  {i:2}. {wiki:<20} {count:,}")
    return merged


if __name__ == "__main__":
    query()
