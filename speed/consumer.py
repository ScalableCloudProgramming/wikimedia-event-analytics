"""
Speed layer — Kinesis consumer with time-bucketed sliding window.

Writes top-N wikis for the last WINDOW_SECONDS into DynamoDB for the serving merge.
Filters bot edits and non-edit event types when present.
"""
import json
import os
import sys
import time
from collections import defaultdict, deque

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

kinesis = boto3.client("kinesis", region_name=config.AWS_REGION)
table = boto3.resource("dynamodb", region_name=config.AWS_REGION).Table(config.DYNAMO_TABLE)


class SlidingWindow:
    """Time-bucketed sliding window. Each bucket covers bucket_s seconds."""

    def __init__(self, window_s=300, bucket_s=10):
        self.window_s = window_s
        self.bucket_s = bucket_s
        self.buckets = deque()  # [(timestamp, {item: count})]

    def add(self, item, ts=None):
        now = ts if ts is not None else time.time()
        if not self.buckets or now - self.buckets[-1][0] >= self.bucket_s:
            self.buckets.append((now, defaultdict(int)))
        self.buckets[-1][1][item] += 1
        self._evict(now)

    def _evict(self, now):
        cutoff = now - self.window_s
        while self.buckets and self.buckets[0][0] < cutoff:
            self.buckets.popleft()

    def top(self, n):
        totals = defaultdict(int)
        for _, counts in self.buckets:
            for item, c in counts.items():
                totals[item] += c
        return sorted(totals.items(), key=lambda x: -x[1])[:n]

    def size(self):
        return sum(sum(c.values()) for _, c in self.buckets)


window = SlidingWindow(window_s=config.WINDOW_SECONDS)


def should_count(record):
    """Filter bots and non-content events when fields exist."""
    if record.get("bot") is True:
        return False
    etype = record.get("type")
    if etype and etype not in ("edit", "new", "categorize"):
        # still count unknowns without type (e.g. USGS)
        if "wiki" in record or "title" in record:
            return etype in ("edit", "new", "categorize")
    return True


def process(record):
    if not should_count(record):
        return
    wiki = record.get("wiki") or record.get("id") or "unknown"
    window.add(str(wiki))


def flush_to_dynamo():
    top = window.top(config.TOP_N)
    ts = str(int(time.time()))
    with table.batch_writer() as batch:
        for rank, (wiki, count) in enumerate(top, 1):
            batch.put_item(Item={
                "pk": f"speed#{rank}",
                "wiki": wiki,
                "count": int(count),
                "ts": ts,
                "window": config.WINDOW_SECONDS,
            })
    # clear stale ranks if top shrank
    for rank in range(len(top) + 1, config.TOP_N + 1):
        try:
            table.delete_item(Key={"pk": f"speed#{rank}"})
        except Exception:
            pass
    print(f"[{ts}] window_events={window.size()} top: {top[:3]}...")


def get_iterators():
    shards = kinesis.describe_stream(StreamName=config.KINESIS_STREAM)[
        "StreamDescription"
    ]["Shards"]
    its = []
    for s in shards:
        it = kinesis.get_shard_iterator(
            StreamName=config.KINESIS_STREAM,
            ShardId=s["ShardId"],
            ShardIteratorType="LATEST",
        )["ShardIterator"]
        its.append(it)
    return its


def run():
    iterators = get_iterators()
    print(
        f"Speed layer | stream={config.KINESIS_STREAM} "
        f"window={config.WINDOW_SECONDS}s top-{config.TOP_N}"
    )
    while True:
        new_its = []
        for it in iterators:
            if not it:
                continue
            try:
                resp = kinesis.get_records(ShardIterator=it, Limit=200)
            except kinesis.exceptions.ExpiredIteratorException:
                iterators = get_iterators()
                break
            for rec in resp["Records"]:
                try:
                    process(json.loads(rec["Data"]))
                except Exception:
                    pass
            new_its.append(resp.get("NextShardIterator"))
        else:
            iterators = new_its
            flush_to_dynamo()
            time.sleep(1)
            continue
        # refreshed iterators after expiry
        time.sleep(1)


if __name__ == "__main__":
    run()
