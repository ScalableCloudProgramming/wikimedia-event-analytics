"""
kinesis_producer.py
-------------------
Reads from a streaming data source and publishes records
to an AWS Kinesis Data Stream in real time.

Supported sources (set DATA_SOURCE in config/.env):
  wikimedia  — Wikimedia Event Streams SSE (no API key required)
  usgs       — USGS Earthquake GeoJSON feed (polled every 60 s)
  replay     — Replay a local JSON Lines file at a controlled rate

Usage:
    python kinesis_producer.py

Requirements:
    pip install boto3 sseclient-py requests python-dotenv
"""

import boto3
import json
import os
import time
import requests
import sseclient
from dotenv import load_dotenv

load_dotenv("../../config/.env")

AWS_REGION    = os.getenv("AWS_REGION", "eu-west-1")
STREAM_NAME   = os.getenv("KINESIS_STREAM_NAME", "scalable-pipeline-stream")
DATA_SOURCE   = os.getenv("DATA_SOURCE", "wikimedia")
REPLAY_FILE   = os.getenv("REPLAY_FILE", "../../config/sample_data.json")
REPLAY_RATE   = float(os.getenv("REPLAY_RATE_HZ", "10"))

WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USGS_URL      = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

kinesis = boto3.client("kinesis", region_name=AWS_REGION)


def put_record(data: dict, partition_key: str = "default"):
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(data).encode("utf-8"),
        PartitionKey=partition_key,
    )


def ingest_wikimedia():
    print(f"Connecting to Wikimedia SSE: {WIKIMEDIA_URL}")
    response = requests.get(WIKIMEDIA_URL, stream=True)
    client   = sseclient.SSEClient(response)
    for event in client.events():
        if event.data:
            try:
                record = json.loads(event.data)
                put_record(record, partition_key=record.get("wiki", "default"))
                print(f"  sent: {record.get('title', '')} [{record.get('wiki', '')}]")
            except json.JSONDecodeError:
                pass


def ingest_usgs(poll_interval: int = 60):
    seen_ids = set()
    print(f"Polling USGS every {poll_interval}s ...")
    while True:
        try:
            features = requests.get(USGS_URL, timeout=10).json().get("features", [])
            for f in features:
                fid = f.get("id")
                if fid and fid not in seen_ids:
                    seen_ids.add(fid)
                    put_record(f, partition_key=fid)
                    print(f"  sent earthquake: {fid}")
        except Exception as e:
            print(f"  error: {e}")
        time.sleep(poll_interval)


def ingest_replay():
    interval = 1.0 / REPLAY_RATE
    print(f"Replaying {REPLAY_FILE} at {REPLAY_RATE} rec/s ...")
    with open(REPLAY_FILE) as f:
        for line in f:
            try:
                put_record(json.loads(line))
            except json.JSONDecodeError:
                pass
            time.sleep(interval)


if __name__ == "__main__":
    sources = {"wikimedia": ingest_wikimedia, "usgs": ingest_usgs, "replay": ingest_replay}
    if DATA_SOURCE not in sources:
        raise ValueError(f"Unknown DATA_SOURCE: {DATA_SOURCE}. Choose: {list(sources)}")
    sources[DATA_SOURCE]()
