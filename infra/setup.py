import boto3, os
from dotenv import load_dotenv

load_dotenv()

REGION  = os.getenv("AWS_REGION", "eu-west-1")
STREAM  = os.getenv("KINESIS_STREAM", "wikimedia-stream")
SHARDS  = int(os.getenv("KINESIS_SHARDS", "2"))
S3_RAW  = os.getenv("S3_RAW", "wikimedia-pipeline-raw")
S3_BATCH = os.getenv("S3_BATCH", "wikimedia-pipeline-batch")
DYNAMO  = os.getenv("DYNAMO_TABLE", "wikimedia-speed-view")

session = boto3.Session(region_name=REGION)
kinesis = session.client("kinesis")
s3      = session.client("s3")
dynamo  = session.client("dynamodb")
emr     = session.client("emr")


def create_stream():
    try:
        kinesis.create_stream(StreamName=STREAM, ShardCount=SHARDS)
        kinesis.get_waiter("stream_exists").wait(StreamName=STREAM)
        print(f"Stream ready: {STREAM}")
    except kinesis.exceptions.ResourceInUseException:
        print(f"Stream exists: {STREAM}")


def create_bucket(name):
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=name)
        else:
            s3.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={"LocationConstraint": REGION}
            )
        print(f"Bucket created: {name}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket exists: {name}")


def create_dynamo_table():
    try:
        dynamo.create_table(
            TableName=DYNAMO,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        print(f"DynamoDB table created: {DYNAMO}")
    except dynamo.exceptions.ResourceInUseException:
        print(f"Table exists: {DYNAMO}")


def create_emr_cluster():
    resp = emr.run_job_flow(
        Name="wikimedia-pipeline",
        ReleaseLabel="emr-6.15.0",
        Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}],
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "SlaveInstanceType": "m5.xlarge",
            "InstanceCount": 2,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        LogUri=f"s3://{S3_BATCH}/emr-logs/",
        # scale between 2 and 8 nodes based on YARN memory pressure
        ManagedScalingPolicy={
            "ComputeLimits": {
                "UnitType": "Instances",
                "MinimumCapacityUnits": 2,
                "MaximumCapacityUnits": 8,
                "MaximumOnDemandCapacityUnits": 8,
            }
        },
    )
    print(f"EMR cluster launched: {resp['JobFlowId']}")
    return resp["JobFlowId"]


if __name__ == "__main__":
    create_stream()
    create_bucket(S3_RAW)
    create_bucket(S3_BATCH)
    create_dynamo_table()
    cluster_id = create_emr_cluster()
    print(f"\nDone. Set EMR_CLUSTER_ID={cluster_id} in .env")
