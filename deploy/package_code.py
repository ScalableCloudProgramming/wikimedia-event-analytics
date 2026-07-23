# Zip project and upload to s3://S3_BATCH/deploy/pipeline-code.zip for EC2 bootstrap
import os
import sys
import zipfile
from pathlib import Path

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

ROOT = Path(__file__).resolve().parent.parent
OUT_ZIP = ROOT / "deploy" / "pipeline-code.zip"
S3_KEY = "deploy/pipeline-code.zip"

SKIP_DIRS = {".git", ".venv", "venv", "__pycache__", ".pytest_cache", "results"}
SKIP_FILES = {".env", "pipeline-code.zip"}


def build_zip():
    if OUT_ZIP.exists():
        OUT_ZIP.unlink()
    with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in ROOT.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(ROOT)
            if any(p in SKIP_DIRS for p in rel.parts):
                continue
            if path.name in SKIP_FILES:
                continue
            if path.suffix in {".pyc", ".png"} and "benchmarks/results" in str(rel):
                continue
            zf.write(path, arcname=str(rel))
    print(f"Built {OUT_ZIP} ({OUT_ZIP.stat().st_size / 1024:.1f} KB)")
    return OUT_ZIP


def upload():
    s3 = boto3.client("s3", region_name=config.AWS_REGION)
    s3.upload_file(str(OUT_ZIP), config.S3_BATCH, S3_KEY)
    print(f"Uploaded s3://{config.S3_BATCH}/{S3_KEY}")
    return f"s3://{config.S3_BATCH}/{S3_KEY}"


if __name__ == "__main__":
    build_zip()
    upload()
