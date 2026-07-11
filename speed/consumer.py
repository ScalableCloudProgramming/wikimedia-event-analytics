import boto3, json, os, time
from collections import defaultdict, deque
from dotenv import load_dotenv

load_dotenv()

REGION  = os.getenv("AWS_REGION", "eu-west-1")
STREAM  = os.getenv("KINESIS_STREAM", "wikimedia-stream")
DYNAMO  = os.getenv("DYNAMO_TABLE", "wikimedia-speed-view")
WINDOW  = int(os.getenv("WINDOW_SECONDS", "300"))   # 5-min sliding window
TOP_N   = int(os.getenv("TOP_N", "10"))

kinesis = boto3.client("kinesis", region_name=REGION)
table   = boto3.resource("dynamodb", region_name=REGION).Table(DYNAMO)


class SlidingWindow:
    """Time-bucketed sliding window. Each bucket covers 10 seconds."""

    def __init__(self, window_s=300, bucket_s=10):
        self.window_s = window_s
        self.bucket_s = bucket_s
        self.buckets  = deque()   # [(timestamp, {item: count})]

    def add(self, item):
        now = time.time()
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


window = SlidingWindow(window_s=WINDOW)


def process(record):
    wiki = record.get("wiki", "unknown")
    window.add(wiki)


def flush_to_dynamo():
    top = window.top(TOP_N)
    ts  = str(int(time.time()))
    for rank, (wiki, count) in enumerate(top, 1):
        table.put_item(Item={
            "pk":    f"speed#{rank}",
            "wiki":  wiki,
            "count": count,
            "ts":    ts,
            "window": WINDOW,
        })
    print(f"[{ts}] top-{TOP_N}: {top[:3]}...")


def get_iterators():
    shards = kinesis.describe_stream(StreamName=STREAM)["StreamDescription"]["Shards"]
    its = []
    for s in shards:
        it = kinesis.get_shard_iterator(
            StreamName=STREAM,
            ShardId=s["ShardId"],
            ShardIteratorType="LATEST"
        )["ShardIterator"]
        its.append(it)
    return its


def run():
    iterators = get_iterators()
    print(f"Speed layer running | window={WINDOW}s | top-{TOP_N}")
    while True:
        new_its = []
        for it in iterators:
            resp = kinesis.get_records(ShardIterator=it, Limit=200)
            for rec in resp["Records"]:
                try:
                    process(json.loads(rec["Data"]))
                except Exception:
                    pass
            new_its.append(resp["NextShardIterator"])
        iterators = new_its
        flush_to_dynamo()
        time.sleep(1)


if __name__ == "__main__":
    run()
