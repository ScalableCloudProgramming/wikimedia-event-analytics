# One-shot AWS deploy orchestrator (run once credentials are configured)
#
# Order:
#   1) core infra (Kinesis/S3/Dynamo/EMR/alarms)
#   2) package code → S3
#   3) CloudWatch dashboard
#   4) EC2 producer + speed + Streamlit
#   5) wait, then print screenshot checklist URLs
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def run(cmd, cwd=None):
    print(f"\n>>> {' '.join(cmd)}")
    subprocess.check_call(cmd, cwd=cwd or str(ROOT))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--skip-infra", action="store_true", help="Skip Kinesis/S3/EMR create")
    p.add_argument("--skip-ec2", action="store_true", help="Skip EC2 fleet")
    p.add_argument("--skip-emr-batch", action="store_true", help="Skip submitting batch step")
    p.add_argument(
        "--ingest-minutes",
        type=int,
        default=20,
        help="Suggested live ingest window before batch (informational)",
    )
    args = p.parse_args()

    # Sanity: credentials present
    import boto3
    import config

    sts = boto3.client("sts", region_name=config.AWS_REGION)
    ident = sts.get_caller_identity()
    print(f"AWS OK account={ident['Account']} arn={ident['Arn']} region={config.AWS_REGION}")

    if not args.skip_infra:
        run([sys.executable, "infra/setup.py"])
        # re-load EMR id if user pasted into .env mid-run
        print("If EMR_CLUSTER_ID was printed, put it in .env before batch submit.")

    run([sys.executable, "deploy/package_code.py"])
    run([sys.executable, "infra/cloudwatch_dashboard.py"])

    if not args.skip_ec2:
        run([sys.executable, "deploy/launch_ec2.py"])

    print(
        f"""
============================================================
HIGH-SCALE RUN PLAN (all on AWS)
============================================================
1. Wait ~5 minutes for EC2 bootstrap (producer + speed + dashboard).
2. Open Streamlit URL printed above (port 8501).
3. Let high-scale ingest run for at least {args.ingest_minutes} minutes
   so S3 raw + Kinesis accumulate large volume.
4. Submit batch on EMR (from this machine once — uses AWS API only):
     python batch/submit.py
5. Create Athena tables:
     python serving/setup_athena.py
6. Optional benchmarks (API only, results uploaded if configured):
     python benchmarks/benchmark.py --mode throughput
     python benchmarks/benchmark.py --mode latency --rates 50,100,200,500
     python benchmarks/benchmark.py --mode speedup --workers 1,2,4
7. Refresh Streamlit → Lambda merge + performance tabs.
8. Follow docs/SCREENSHOTS.md for console captures.

Nothing continuous needs to run on your laptop after EC2 is up.
============================================================
"""
    )


if __name__ == "__main__":
    main()
