# Create CloudWatch dashboard for pipeline metrics (screenshot-ready)
import json
import os
import sys

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

NAME = os.getenv("CW_DASHBOARD_NAME", "wikimedia-lambda-pipeline")


def body():
    stream = config.KINESIS_STREAM
    region = config.AWS_REGION
    widgets = [
        {
            "type": "metric",
            "x": 0, "y": 0, "width": 12, "height": 6,
            "properties": {
                "title": "Kinesis IncomingRecords",
                "region": region,
                "metrics": [
                    ["AWS/Kinesis", "IncomingRecords", "StreamName", stream, {"stat": "Sum"}],
                ],
                "period": 60,
                "view": "timeSeries",
            },
        },
        {
            "type": "metric",
            "x": 12, "y": 0, "width": 12, "height": 6,
            "properties": {
                "title": "Kinesis IncomingBytes",
                "region": region,
                "metrics": [
                    ["AWS/Kinesis", "IncomingBytes", "StreamName", stream, {"stat": "Sum"}],
                ],
                "period": 60,
                "view": "timeSeries",
            },
        },
        {
            "type": "metric",
            "x": 0, "y": 6, "width": 12, "height": 6,
            "properties": {
                "title": "Iterator age (consumer lag ms)",
                "region": region,
                "metrics": [
                    ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", stream, {"stat": "Maximum"}],
                ],
                "period": 60,
                "view": "timeSeries",
            },
        },
        {
            "type": "metric",
            "x": 12, "y": 6, "width": 12, "height": 6,
            "properties": {
                "title": "PutRecords success / throttle",
                "region": region,
                "metrics": [
                    ["AWS/Kinesis", "PutRecords.Success", "StreamName", stream, {"stat": "Sum"}],
                    [".", "WriteProvisionedThroughputExceeded", ".", ".", {"stat": "Sum"}],
                ],
                "period": 60,
                "view": "timeSeries",
            },
        },
        {
            "type": "text",
            "x": 0, "y": 12, "width": 24, "height": 2,
            "properties": {
                "markdown": (
                    f"## Wikimedia Lambda pipeline\\n"
                    f"Stream `{stream}` · DynamoDB `{config.DYNAMO_TABLE}` · "
                    f"S3 raw `{config.S3_RAW}` · batch `{config.S3_BATCH}` · "
                    f"EMR `{config.EMR_CLUSTER_ID or 'set after setup'}`"
                )
            },
        },
    ]
    return {"widgets": widgets}


def main():
    cw = boto3.client("cloudwatch", region_name=config.AWS_REGION)
    cw.put_dashboard(DashboardName=NAME, DashboardBody=json.dumps(body()))
    url = (
        f"https://{config.AWS_REGION}.console.aws.amazon.com/cloudwatch/home"
        f"?region={config.AWS_REGION}#dashboards:name={NAME}"
    )
    print(f"CloudWatch dashboard ready: {NAME}")
    print(url)


if __name__ == "__main__":
    main()
