"""
Shared configuration loaded from environment / .env.
Canonical env var names used across the whole pipeline.
"""
import os
from pathlib import Path

# Load .env from code/ root (works regardless of cwd)
_ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env")
    load_dotenv()  # also allow process env / cwd .env
except ImportError:
    pass

AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")

# Kinesis
KINESIS_STREAM = os.getenv("KINESIS_STREAM", "wikimedia-stream")
KINESIS_SHARDS = int(os.getenv("KINESIS_SHARDS", "2"))

# S3 (bucket names without s3:// prefix)
S3_RAW = os.getenv("S3_RAW", "wikimedia-pipeline-raw")
S3_BATCH = os.getenv("S3_BATCH", "wikimedia-pipeline-batch")
S3_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "data/")
S3_BATCH_PREFIX = os.getenv("S3_BATCH_PREFIX", "output/")

# DynamoDB
DYNAMO_TABLE = os.getenv("DYNAMO_TABLE", "wikimedia-speed-view")

# EMR
EMR_CLUSTER_ID = os.getenv("EMR_CLUSTER_ID", "")
EMR_MIN_INSTANCES = int(os.getenv("EMR_MIN_INSTANCES", "2"))
EMR_MAX_INSTANCES = int(os.getenv("EMR_MAX_INSTANCES", "8"))

# Athena / serving
ATHENA_DB = os.getenv("ATHENA_DB", "wikimedia_pipeline")
ATHENA_TABLE = os.getenv("ATHENA_TABLE", "batch_view")
ATHENA_KEYWORDS_TABLE = os.getenv("ATHENA_KEYWORDS_TABLE", "batch_keywords")
ATHENA_HOURLY_TABLE = os.getenv("ATHENA_HOURLY_TABLE", "batch_hourly")
S3_ATHENA_OUT = os.getenv(
    "S3_ATHENA_OUT", f"s3://{S3_BATCH}/athena-results/"
)

# Pipeline behaviour
DATA_SOURCE = os.getenv("DATA_SOURCE", "wikimedia")
WIKIMEDIA_URL = os.getenv(
    "WIKIMEDIA_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
USGS_URL = os.getenv(
    "USGS_URL",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
)
REPLAY_FILE = os.getenv("REPLAY_FILE", str(_ROOT / "data" / "sample_events.jsonl"))
REPLAY_RATE = float(os.getenv("REPLAY_RATE_HZ", "10"))
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "300"))
TOP_N = int(os.getenv("TOP_N", "10"))

# Dual-write: also land raw events on S3 for the batch layer
S3_RAW_FLUSH_SIZE = int(os.getenv("S3_RAW_FLUSH_SIZE", "50"))
S3_RAW_FLUSH_SECONDS = float(os.getenv("S3_RAW_FLUSH_SECONDS", "10"))

# Firehose (optional managed path raw → S3)
FIREHOSE_STREAM = os.getenv("FIREHOSE_STREAM", "wikimedia-raw-firehose")
USE_FIREHOSE = os.getenv("USE_FIREHOSE", "false").lower() in ("1", "true", "yes")


def s3_uri(bucket: str, prefix: str = "") -> str:
    prefix = prefix.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return f"s3://{bucket}/{prefix}"
