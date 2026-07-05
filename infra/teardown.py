import boto3, os
from dotenv import load_dotenv

load_dotenv()

REGION  = os.getenv("AWS_REGION", "eu-west-1")
STREAM  = os.getenv("KINESIS_STREAM", "wikimedia-stream")
S3_RAW  = os.getenv("S3_RAW", "wikimedia-pipeline-raw")
S3_BATCH = os.getenv("S3_BATCH", "wikimedia-pipeline-batch")
DYNAMO  = os.getenv("DYNAMO_TABLE", "wikimedia-speed-view")
CLUSTER = os.getenv("EMR_CLUSTER_ID", "")

session = boto3.Session(region_name=REGION)


def delete_stream():
    k = session.client("kinesis")
    try:
        k.delete_stream(StreamName=STREAM)
        print(f"Deleted stream: {STREAM}")
    except k.exceptions.ResourceNotFoundException:
        print("Stream not found")


def empty_bucket(name):
    bucket = boto3.resource("s3", region_name=REGION).Bucket(name)
    try:
        bucket.objects.all().delete()
        bucket.delete()
        print(f"Deleted bucket: {name}")
    except Exception:
        print(f"Bucket not found: {name}")


def delete_table():
    d = session.client("dynamodb")
    try:
        d.delete_table(TableName=DYNAMO)
        print(f"Deleted table: {DYNAMO}")
    except d.exceptions.ResourceNotFoundException:
        print("Table not found")


def terminate_cluster():
    if not CLUSTER:
        print("No cluster ID set")
        return
    session.client("emr").terminate_job_flows(JobFlowIds=[CLUSTER])
    print(f"Terminating cluster: {CLUSTER}")


if __name__ == "__main__":
    delete_stream()
    empty_bucket(S3_RAW)
    empty_bucket(S3_BATCH)
    delete_table()
    terminate_cluster()
    print("Teardown complete.")
