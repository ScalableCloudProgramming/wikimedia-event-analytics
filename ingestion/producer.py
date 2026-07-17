"""
Kinesis producer for Wikimedia Event Streams (and USGS / replay).

Dual-writes every record to Kinesis (speed layer) and buffered S3 (batch layer).
"""
import json
import os
import sys
import time

import boto3
import requests
import sseclient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config
try:
    from ingestion.s3_sink import S3RawSink
except ImportError:
    from s3_sink import S3RawSink

kinesis = boto3.client("kinesis", region_name=config.AWS_REGION)
s3_sink = S3RawSink()
_sent = 0


def put(record, key="default"):
    global _sent
    payload = json.dumps(record, default=str).encode()
    kinesis.put_record(
        StreamName=config.KINESIS_STREAM,
        Data=payload,
        PartitionKey=str(key)[:256] or "default",
    )
    s3_sink.add(record)
    _sent += 1


def wikimedia():
    print(f"Connecting to Wikimedia SSE: {config.WIKIMEDIA_URL}")
    print(f"Kinesis={config.KINESIS_STREAM}  S3 raw={config.S3_RAW}/{config.S3_RAW_PREFIX}")
    resp = requests.get(config.WIKIMEDIA_URL, stream=True, timeout=60)
    resp.raise_for_status()
    try:
        for event in sseclient.SSEClient(resp).events():
            if not event.data:
                continue
            try:
                rec = json.loads(event.data)
                put(rec, rec.get("wiki", "default"))
                if _sent % 25 == 0:
                    print(f"  sent={_sent} last={rec.get('wiki')} | {rec.get('title')}")
            except json.JSONDecodeError:
                pass
    finally:
        s3_sink.close()
        print(f"Stopped. total_sent={_sent} s3_objects={s3_sink.objects_written}")


def usgs(interval=60):
    print("Polling USGS earthquake feed...")
    seen = set()
    try:
        while True:
            try:
                features = requests.get(config.USGS_URL, timeout=10).json().get("features", [])
                for f in features:
                    fid = f.get("id")
                    if fid and fid not in seen:
                        seen.add(fid)
                        put(f, fid)
                        print(f"  earthquake: {fid}")
            except Exception as e:
                print(f"  error: {e}")
            time.sleep(interval)
    finally:
        s3_sink.close()


def replay(path=None, rate=None):
    path = path or config.REPLAY_FILE
    rate = rate or config.REPLAY_RATE
    interval = 1.0 / max(rate, 0.1)
    print(f"Replaying {path} at {rate} rec/s ...")
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    put(rec, rec.get("wiki", "default"))
                except json.JSONDecodeError:
                    pass
                time.sleep(interval)
    finally:
        s3_sink.close()
        print(f"Replay done. total_sent={_sent}")


if __name__ == "__main__":
    src = config.DATA_SOURCE
    if src == "wikimedia":
        wikimedia()
    elif src == "usgs":
        usgs()
    elif src == "replay":
        replay()
    else:
        raise ValueError(f"Unknown DATA_SOURCE: {src}")
