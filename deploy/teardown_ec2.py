# Terminate EC2 instances recorded in deploy/ec2_instances.json
import json
import os
import sys
from pathlib import Path

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

PATH = Path(__file__).resolve().parent / "ec2_instances.json"


def main():
    if not PATH.exists():
        print(f"No {PATH} — nothing to terminate")
        return
    data = json.loads(PATH.read_text())
    ids = [v["instance_id"] for v in data.values() if v.get("instance_id")]
    if not ids:
        print("No instance ids")
        return
    ec2 = boto3.client("ec2", region_name=config.AWS_REGION)
    print(f"Terminating {ids}")
    ec2.terminate_instances(InstanceIds=ids)
    print("Done")


if __name__ == "__main__":
    main()
