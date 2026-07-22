# Provision Kinesis, S3, DynamoDB, EMR (managed scaling), CloudWatch alarms
#
# EMR auto-scaling:
#   unit=Instances, min/max from EMR_MIN_INSTANCES / EMR_MAX_INSTANCES
#   trigger=YARN memory pressure (EMR managed scaling)
#   cool-down=EMR managed default
import os
import sys

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

session = boto3.Session(region_name=config.AWS_REGION)
kinesis = session.client("kinesis")
s3 = session.client("s3")
dynamo = session.client("dynamodb")
emr = session.client("emr")
cw = session.client("cloudwatch")


def create_stream():
    try:
        kinesis.create_stream(
            StreamName=config.KINESIS_STREAM,
            ShardCount=config.KINESIS_SHARDS,
        )
        kinesis.get_waiter("stream_exists").wait(StreamName=config.KINESIS_STREAM)
        print(f"Stream ready: {config.KINESIS_STREAM}")
    except kinesis.exceptions.ResourceInUseException:
        print(f"Stream exists: {config.KINESIS_STREAM}")


def create_bucket(name):
    try:
        if config.AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=name)
        else:
            s3.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={"LocationConstraint": config.AWS_REGION},
            )
        print(f"Bucket created: {name}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket exists: {name}")
    except s3.exceptions.BucketAlreadyExists:
        print(f"Bucket name taken globally: {name}")


def create_dynamo_table():
    try:
        dynamo.create_table(
            TableName=config.DYNAMO_TABLE,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        print(f"DynamoDB table created: {config.DYNAMO_TABLE}")
    except dynamo.exceptions.ResourceInUseException:
        print(f"Table exists: {config.DYNAMO_TABLE}")


def create_emr_cluster():
    # Managed scaling reacts to YARN resource pressure within min/max bounds
    resp = emr.run_job_flow(
        Name="wikimedia-pipeline",
        ReleaseLabel="emr-6.15.0",
        Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}],
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "SlaveInstanceType": "m5.xlarge",
            "InstanceCount": max(config.EMR_MIN_INSTANCES, 2),
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        LogUri=f"s3://{config.S3_BATCH}/emr-logs/",
        ManagedScalingPolicy={
            "ComputeLimits": {
                "UnitType": "Instances",
                "MinimumCapacityUnits": config.EMR_MIN_INSTANCES,
                "MaximumCapacityUnits": config.EMR_MAX_INSTANCES,
                "MaximumOnDemandCapacityUnits": config.EMR_MAX_INSTANCES,
                "MaximumCoreCapacityUnits": config.EMR_MAX_INSTANCES,
            }
        },
        Configurations=[
            {
                "Classification": "emrfs-site",
                "Properties": {"fs.s3.consistent": "false"},
            }
        ],
        VisibleToAllUsers=True,
        Tags=[
            {"Key": "Project", "Value": "wikimedia-event-analytics"},
            {"Key": "ScalingMin", "Value": str(config.EMR_MIN_INSTANCES)},
            {"Key": "ScalingMax", "Value": str(config.EMR_MAX_INSTANCES)},
            {"Key": "ScalingTrigger", "Value": "YARN-memory-pressure"},
            {"Key": "ScalingCooldown", "Value": "EMR-managed-default"},
        ],
    )
    print(f"EMR cluster launched: {resp['JobFlowId']}")
    print(
        f"  Managed scaling: min={config.EMR_MIN_INSTANCES} "
        f"max={config.EMR_MAX_INSTANCES} trigger=YARN memory pressure"
    )
    return resp["JobFlowId"]


def create_kinesis_alarms():
    # Demo / monitoring: high ingest rate + consumer lag
    dims = [{"Name": "StreamName", "Value": config.KINESIS_STREAM}]
    try:
        cw.put_metric_alarm(
            AlarmName=f"{config.KINESIS_STREAM}-high-incoming",
            AlarmDescription="High Kinesis ingest rate (scale signal for demo)",
            MetricName="IncomingRecords",
            Namespace="AWS/Kinesis",
            Statistic="Sum",
            Period=60,
            EvaluationPeriods=3,
            Threshold=500,
            ComparisonOperator="GreaterThanThreshold",
            Dimensions=dims,
            TreatMissingData="notBreaching",
        )
        cw.put_metric_alarm(
            AlarmName=f"{config.KINESIS_STREAM}-iterator-age",
            AlarmDescription="Consumer lag — speed layer falling behind",
            MetricName="GetRecords.IteratorAgeMilliseconds",
            Namespace="AWS/Kinesis",
            Statistic="Maximum",
            Period=60,
            EvaluationPeriods=2,
            Threshold=60000,
            ComparisonOperator="GreaterThanThreshold",
            Dimensions=dims,
            TreatMissingData="notBreaching",
        )
        print("CloudWatch alarms created for Kinesis")
    except Exception as e:
        print(f"CloudWatch alarms skipped: {e}")


if __name__ == "__main__":
    create_stream()
    create_bucket(config.S3_RAW)
    create_bucket(config.S3_BATCH)
    create_dynamo_table()
    cluster_id = create_emr_cluster()
    create_kinesis_alarms()
    print(f"\nDone. Set EMR_CLUSTER_ID={cluster_id} in .env")
