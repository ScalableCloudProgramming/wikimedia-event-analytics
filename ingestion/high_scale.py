# High-scale ingest on EC2: live Wikimedia + optional fan-out for load testing
# Uses put_records batches for higher throughput than single put_record
import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import requests
import sseclient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config
try:
    from ingestion.s3_sink import S3RawSink
except ImportError:
    from s3_sink import S3RawSink

# FANOUT: how many copies of each live event (stress test while still real titles/wikis)
FANOUT = int(os.getenv("INGEST_FANOUT", "5"))
# Extra synthetic events per second on top of live stream
SYNTH_HZ = float(os.getenv("INGEST_SYNTH_HZ", "100"))
WORKERS = int(os.getenv("INGEST_WORKERS", "4"))
BATCH_SIZE = int(os.getenv("KINESIS_PUT_BATCH", "25"))

kinesis = boto3.client("kinesis", region_name=config.AWS_REGION)
s3_sink = S3RawSink(
    flush_size=max(config.S3_RAW_FLUSH_SIZE, 200),
    flush_seconds=max(config.S3_RAW_FLUSH_SECONDS, 5),
)
_sent = 0
_buf = []


def _flush_kinesis():
    global _buf, _sent
    if not _buf:
        return
    # Kinesis put_records max 500; we use BATCH_SIZE
    for i in range(0, len(_buf), BATCH_SIZE):
        chunk = _buf[i : i + BATCH_SIZE]
        records = []
        for rec, key in chunk:
            records.append({
                "Data": json.dumps(rec, default=str).encode(),
                "PartitionKey": str(key)[:256] or "default",
            })
        resp = kinesis.put_records(StreamName=config.KINESIS_STREAM, Records=records)
        failed = resp.get("FailedRecordCount", 0)
        _sent += len(records) - failed
        if failed:
            print(f"  put_records failed={failed}")
    _buf.clear()


def enqueue(record, key="default"):
    _buf.append((record, key))
    s3_sink.add(record)
    if len(_buf) >= BATCH_SIZE:
        _flush_kinesis()


def fanout_event(rec):
    # One real event + FANOUT-1 variants (same wiki/title, different synthetic ids)
    wiki = rec.get("wiki", "enwiki")
    enqueue(rec, wiki)
    for i in range(1, max(FANOUT, 1)):
        clone = dict(rec)
        clone["_fanout"] = i
        clone["_scale_ts"] = time.time()
        enqueue(clone, f"{wiki}-{i}")


def synth_loop(stop_flag):
    # Sustained synthetic load derived from common wiki codes in the dataset domain
    wikis = [
        "enwiki", "dewiki", "frwiki", "eswiki", "ruwiki", "jawiki",
        "zhwiki", "ptwiki", "itwiki", "plwiki", "nlwiki", "arwiki",
    ]
    titles = [
        "Apache_Spark", "Lambda_architecture", "Amazon_Kinesis", "MapReduce",
        "Cloud_computing", "Big_data", "DynamoDB", "Wikimedia", "EMR", "Athena",
    ]
    interval = 1.0 / max(SYNTH_HZ, 0.1)
    n = 0
    while not stop_flag["stop"]:
        rec = {
            "wiki": random.choice(wikis),
            "title": random.choice(titles) + f"_{n}",
            "type": "edit",
            "bot": False,
            "timestamp": int(time.time()),
            "user": f"scale_user_{n % 50}",
            "namespace": 0,
            "_synthetic": True,
        }
        fanout_event(rec)
        n += 1
        if n % 500 == 0:
            print(f"  synth n={n} total_sent≈{_sent}")
        time.sleep(interval)


def wikimedia_high_scale():
    print(
        f"HIGH-SCALE ingest | fanout={FANOUT} synth_hz={SYNTH_HZ} "
        f"workers={WORKERS} stream={config.KINESIS_STREAM}"
    )
    stop = {"stop": False}
    pool = ThreadPoolExecutor(max_workers=max(WORKERS, 2))
    if SYNTH_HZ > 0:
        pool.submit(synth_loop, stop)

    # Wikimedia requires a descriptive User-Agent
    headers = {
        "User-Agent": "WikimediaEventAnalytics/1.0 (NCI MSc Cloud; academic pipeline)",
        "Accept": "text/event-stream",
    }
    try:
        while not stop["stop"]:
            try:
                resp = requests.get(
                    config.WIKIMEDIA_URL, stream=True, timeout=60, headers=headers
                )
                resp.raise_for_status()
                for event in sseclient.SSEClient(resp).events():
                    if stop["stop"]:
                        break
                    if not event.data:
                        continue
                    try:
                        rec = json.loads(event.data)
                        fanout_event(rec)
                        if _sent and _sent % 1000 < FANOUT:
                            print(f"  sent≈{_sent} last={rec.get('wiki')} | {rec.get('title')}")
                    except json.JSONDecodeError:
                        pass
            except KeyboardInterrupt:
                break
            except Exception as e:
                # Keep synthetic load running if live stream blips
                print(f"  wikimedia stream error (retry in 10s): {e}")
                time.sleep(10)
    finally:
        stop["stop"] = True
        _flush_kinesis()
        s3_sink.close()
        pool.shutdown(wait=False, cancel_futures=True)
        print(f"Stopped. approx_sent={_sent} s3_objects={s3_sink.objects_written}")


if __name__ == "__main__":
    wikimedia_high_scale()
