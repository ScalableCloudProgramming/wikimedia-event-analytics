import boto3, os, time
from dotenv import load_dotenv

load_dotenv()

REGION  = os.getenv("AWS_REGION", "eu-west-1")
CLUSTER = os.getenv("EMR_CLUSTER_ID")
S3_BATCH = os.getenv("S3_BATCH", "wikimedia-pipeline-batch")
SCRIPT  = os.path.join(os.path.dirname(__file__), "batch_job.py")
S3_KEY  = "scripts/batch_job.py"

s3  = boto3.client("s3",  region_name=REGION)
emr = boto3.client("emr", region_name=REGION)


def upload():
    s3.upload_file(SCRIPT, S3_BATCH, S3_KEY)
    print(f"Uploaded to s3://{S3_BATCH}/{S3_KEY}")


def submit():
    resp = emr.add_job_flow_steps(
        JobFlowId=CLUSTER,
        Steps=[{
            "Name": "wikimedia-batch",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit", "--deploy-mode", "cluster",
                    "--master", "yarn",
                    f"s3://{S3_BATCH}/{S3_KEY}",
                ],
            },
        }],
    )
    return resp["StepIds"][0]


def wait(step_id):
    done = {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}
    while True:
        state = emr.describe_step(ClusterId=CLUSTER, StepId=step_id)["Step"]["Status"]["State"]
        print(f"  {state}")
        if state in done:
            return state
        time.sleep(20)


if __name__ == "__main__":
    upload()
    sid = submit()
    print(f"Step: {sid}")
    result = wait(sid)
    print(f"Done: {result}")
