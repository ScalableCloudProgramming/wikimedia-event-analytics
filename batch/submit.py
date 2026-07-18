"""Upload batch_job.py to S3 and submit as an EMR Spark step."""
import os
import sys
import time

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

SCRIPT = os.path.join(os.path.dirname(__file__), "batch_job.py")
S3_KEY = "scripts/batch_job.py"

s3 = boto3.client("s3", region_name=config.AWS_REGION)
emr = boto3.client("emr", region_name=config.AWS_REGION)


def upload():
    s3.upload_file(SCRIPT, config.S3_BATCH, S3_KEY)
    print(f"Uploaded to s3://{config.S3_BATCH}/{S3_KEY}")


def submit(executor_instances=None):
    if not config.EMR_CLUSTER_ID:
        raise SystemExit("Set EMR_CLUSTER_ID in .env")
    args = [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--master", "yarn",
        "--conf", "spark.sql.shuffle.partitions=100",
    ]
    if executor_instances:
        args += ["--num-executors", str(executor_instances)]
    args += [
        "--conf", f"spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        f"s3://{config.S3_BATCH}/{S3_KEY}",
    ]
    # Pass bucket paths into the job
    args = [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--master", "yarn",
        "--conf", "spark.sql.shuffle.partitions=100",
        *(["--num-executors", str(executor_instances)] if executor_instances else []),
        f"s3://{config.S3_BATCH}/{S3_KEY}",
    ]
    resp = emr.add_job_flow_steps(
        JobFlowId=config.EMR_CLUSTER_ID,
        Steps=[{
            "Name": "wikimedia-batch",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": args,
            },
        }],
    )
    return resp["StepIds"][0]


def wait(step_id, poll=20):
    done = {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}
    while True:
        step = emr.describe_step(ClusterId=config.EMR_CLUSTER_ID, StepId=step_id)["Step"]
        state = step["Status"]["State"]
        print(f"  {state}")
        if state in done:
            return state, step["Status"]
        time.sleep(poll)


def step_runtime_seconds(status):
    """Extract wall-clock runtime from EMR step status timestamps."""
    timeline = status.get("Timeline") or {}
    start = timeline.get("StartDateTime")
    end = timeline.get("EndDateTime")
    if start and end:
        return (end - start).total_seconds()
    return None


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--executors", type=int, default=None, help="Spark executor count for speedup runs")
    args = p.parse_args()

    upload()
    sid = submit(executor_instances=args.executors)
    print(f"Step: {sid}")
    state, status = wait(sid)
    runtime = step_runtime_seconds(status)
    print(f"Done: {state}" + (f" runtime={runtime:.1f}s" if runtime else ""))
