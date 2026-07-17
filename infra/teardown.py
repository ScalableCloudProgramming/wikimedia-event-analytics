"""Tear down AWS resources created by infra/setup.py."""
import os
import sys

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

session = boto3.Session(region_name=config.AWS_REGION)
cw = session.client("cloudwatch")


def delete_stream():
    k = session.client("kinesis")
    try:
        k.delete_stream(StreamName=config.KINESIS_STREAM)
        print(f"Deleted stream: {config.KINESIS_STREAM}")
    except k.exceptions.ResourceNotFoundException:
        print("Stream not found")


def empty_bucket(name):
    bucket = boto3.resource("s3", region_name=config.AWS_REGION).Bucket(name)
    try:
        bucket.objects.all().delete()
        bucket.delete()
        print(f"Deleted bucket: {name}")
    except Exception:
        print(f"Bucket not found / not empty-safe: {name}")


def delete_table():
    d = session.client("dynamodb")
    try:
        d.delete_table(TableName=config.DYNAMO_TABLE)
        print(f"Deleted table: {config.DYNAMO_TABLE}")
    except d.exceptions.ResourceNotFoundException:
        print("Table not found")


def terminate_cluster():
    if not config.EMR_CLUSTER_ID:
        print("No EMR_CLUSTER_ID set")
        return
    session.client("emr").terminate_job_flows(JobFlowIds=[config.EMR_CLUSTER_ID])
    print(f"Terminating cluster: {config.EMR_CLUSTER_ID}")


def delete_alarms():
    names = [
        f"{config.KINESIS_STREAM}-high-incoming",
        f"{config.KINESIS_STREAM}-iterator-age",
    ]
    try:
        cw.delete_alarms(AlarmNames=names)
        print(f"Deleted alarms: {names}")
    except Exception as e:
        print(f"Alarms cleanup: {e}")


if __name__ == "__main__":
    delete_alarms()
    delete_stream()
    empty_bucket(config.S3_RAW)
    empty_bucket(config.S3_BATCH)
    delete_table()
    terminate_cluster()
    print("Teardown complete.")
