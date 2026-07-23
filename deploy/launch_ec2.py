# Launch EC2 fleet on AWS: producer (high-scale), speed consumer, Streamlit dashboard
# Uses instance profile so nothing needs local long-running processes.
import argparse
import base64
import os
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

ROOT = Path(__file__).resolve().parent.parent
TPL = Path(__file__).resolve().parent / "userdata.sh.tpl"

# Prefer Ubuntu 22.04 in region (override with AMI_ID)
DEFAULT_AMI_FILTER = "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"


def load_env_blob():
    # Build .env content for EC2 (no secrets — instance role supplies AWS access)
    lines = [
        f"AWS_REGION={config.AWS_REGION}",
        f"KINESIS_STREAM={config.KINESIS_STREAM}",
        f"KINESIS_SHARDS={config.KINESIS_SHARDS}",
        f"S3_RAW={config.S3_RAW}",
        f"S3_BATCH={config.S3_BATCH}",
        f"S3_RAW_PREFIX={config.S3_RAW_PREFIX}",
        f"S3_BATCH_PREFIX={config.S3_BATCH_PREFIX}",
        f"DYNAMO_TABLE={config.DYNAMO_TABLE}",
        f"EMR_CLUSTER_ID={config.EMR_CLUSTER_ID}",
        f"EMR_MIN_INSTANCES={config.EMR_MIN_INSTANCES}",
        f"EMR_MAX_INSTANCES={config.EMR_MAX_INSTANCES}",
        f"ATHENA_DB={config.ATHENA_DB}",
        f"ATHENA_TABLE={config.ATHENA_TABLE}",
        f"ATHENA_KEYWORDS_TABLE={config.ATHENA_KEYWORDS_TABLE}",
        f"ATHENA_HOURLY_TABLE={config.ATHENA_HOURLY_TABLE}",
        f"S3_ATHENA_OUT={config.S3_ATHENA_OUT}",
        f"DATA_SOURCE={config.DATA_SOURCE}",
        f"WINDOW_SECONDS={config.WINDOW_SECONDS}",
        f"TOP_N={config.TOP_N}",
        f"S3_RAW_FLUSH_SIZE={os.getenv('S3_RAW_FLUSH_SIZE', '200')}",
        f"S3_RAW_FLUSH_SECONDS={os.getenv('S3_RAW_FLUSH_SECONDS', '5')}",
        # high-scale knobs
        f"INGEST_FANOUT={os.getenv('INGEST_FANOUT', '8')}",
        f"INGEST_SYNTH_HZ={os.getenv('INGEST_SYNTH_HZ', '150')}",
        f"INGEST_WORKERS={os.getenv('INGEST_WORKERS', '4')}",
        f"KINESIS_PUT_BATCH={os.getenv('KINESIS_PUT_BATCH', '25')}",
    ]
    return "\n".join(lines) + "\n"


def render_userdata(role: str) -> str:
    text = TPL.read_text()
    text = text.replace("__AWS_REGION__", config.AWS_REGION)
    text = text.replace("__S3_BATCH__", config.S3_BATCH)
    text = text.replace("__INSTANCE_ROLE__", role)
    text = text.replace("__ENV_FILE__", load_env_blob())
    return text


def latest_ubuntu_ami(ec2):
    ami = os.getenv("AMI_ID")
    if ami:
        return ami
    resp = ec2.describe_images(
        Owners=["099720109477"],
        Filters=[
            {"Name": "name", "Values": [DEFAULT_AMI_FILTER]},
            {"Name": "state", "Values": ["available"]},
            {"Name": "architecture", "Values": ["x86_64"]},
        ],
    )
    images = sorted(resp["Images"], key=lambda x: x["CreationDate"], reverse=True)
    if not images:
        raise SystemExit("No Ubuntu AMI found — set AMI_ID in env")
    return images[0]["ImageId"]


def ensure_security_group(ec2, name="wikimedia-pipeline-sg"):
    vpc_id = os.getenv("VPC_ID")
    if not vpc_id:
        vpc_id = ec2.describe_vpcs(
            Filters=[{"Name": "isDefault", "Values": ["true"]}]
        )["Vpcs"][0]["VpcId"]

    existing = ec2.describe_security_groups(
        Filters=[
            {"Name": "group-name", "Values": [name]},
            {"Name": "vpc-id", "Values": [vpc_id]},
        ]
    )["SecurityGroups"]
    if existing:
        sg_id = existing[0]["GroupId"]
        print(f"Security group exists: {sg_id}")
        return sg_id

    sg = ec2.create_security_group(
        GroupName=name,
        Description="Wikimedia pipeline producer/speed/dashboard",
        VpcId=vpc_id,
    )
    sg_id = sg["GroupId"]
    # SSH optional + Streamlit 8501 open (Academy demos)
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH"}],
            },
            {
                "IpProtocol": "tcp",
                "FromPort": 8501,
                "ToPort": 8501,
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Streamlit"}],
            },
        ],
    )
    print(f"Created security group: {sg_id}")
    return sg_id


def resolve_instance_profile():
    # AWS Academy often uses LabInstanceProfile / LabRole
    name = os.getenv("INSTANCE_PROFILE_NAME", "LabInstanceProfile")
    return name


def launch_one(ec2, role, ami, sg_id, instance_type, key_name, profile):
    userdata = render_userdata(role)
    kwargs = {
        "ImageId": ami,
        "InstanceType": instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "SecurityGroupIds": [sg_id],
        "UserData": userdata,
        "IamInstanceProfile": {"Name": profile},
        "TagSpecifications": [{
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Name", "Value": f"wiki-{role}"},
                {"Key": "Project", "Value": "wikimedia-event-analytics"},
                {"Key": "Role", "Value": role},
            ],
        }],
    }
    if key_name:
        kwargs["KeyName"] = key_name
    resp = ec2.run_instances(**kwargs)
    iid = resp["Instances"][0]["InstanceId"]
    print(f"Launched {role}: {iid}")
    return iid


def wait_public_ip(ec2, instance_id, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = ec2.describe_instances(InstanceIds=[instance_id])
        inst = r["Reservations"][0]["Instances"][0]
        ip = inst.get("PublicIpAddress")
        state = inst["State"]["Name"]
        if state == "running" and ip:
            return ip
        if state in ("terminated", "shutting-down"):
            raise SystemExit(f"Instance {instance_id} {state}")
        time.sleep(5)
    return None


def main():
    p = argparse.ArgumentParser(description="Launch EC2 producer/speed/dashboard")
    p.add_argument(
        "--roles",
        default="producer,speed,dashboard",
        help="Comma list: producer,speed,dashboard",
    )
    p.add_argument("--instance-type", default=os.getenv("EC2_INSTANCE_TYPE", "t3.medium"))
    p.add_argument("--key-name", default=os.getenv("EC2_KEY_NAME", ""))
    args = p.parse_args()

    ec2 = boto3.client("ec2", region_name=config.AWS_REGION)
    ami = latest_ubuntu_ami(ec2)
    sg = ensure_security_group(ec2)
    profile = resolve_instance_profile()
    key = args.key_name or None
    if not key:
        print("WARN: no EC2_KEY_NAME — instances launched without SSH key (dashboard still public:8501)")

    results = {}
    for role in [r.strip() for r in args.roles.split(",") if r.strip()]:
        iid = launch_one(ec2, role, ami, sg, args.instance_type, key, profile)
        ip = wait_public_ip(ec2, iid)
        results[role] = {"instance_id": iid, "public_ip": ip}
        print(f"  {role} public_ip={ip}")

    print("\n=== Deployed ===")
    for role, info in results.items():
        print(f"{role}: {info['instance_id']}  ip={info['public_ip']}")
    if "dashboard" in results and results["dashboard"]["public_ip"]:
        print(f"\nStreamlit dashboard: http://{results['dashboard']['public_ip']}:8501")
    print("\nProducer/speed need ~3–5 min for apt + pip bootstrap.")
    # Persist IDs for teardown
    out = ROOT / "deploy" / "ec2_instances.json"
    import json
    out.write_text(json.dumps(results, indent=2))
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
