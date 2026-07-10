import boto3, json, os, time, requests, sseclient
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "eu-west-1")
STREAM = os.getenv("KINESIS_STREAM", "wikimedia-stream")
SOURCE = os.getenv("DATA_SOURCE", "wikimedia")

WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

kinesis = boto3.client("kinesis", region_name=REGION)


def put(record, key="default"):
    kinesis.put_record(
        StreamName=STREAM,
        Data=json.dumps(record).encode(),
        PartitionKey=key
    )


def wikimedia():
    print("Connecting to Wikimedia SSE stream...")
    resp = requests.get(WIKIMEDIA_URL, stream=True)
    for event in sseclient.SSEClient(resp).events():
        if not event.data:
            continue
        try:
            rec = json.loads(event.data)
            put(rec, rec.get("wiki", "default"))
            print(f"  {rec.get('wiki')} | {rec.get('title')}")
        except json.JSONDecodeError:
            pass


def usgs(interval=60):
    print("Polling USGS earthquake feed...")
    seen = set()
    while True:
        try:
            features = requests.get(USGS_URL, timeout=10).json().get("features", [])
            for f in features:
                fid = f.get("id")
                if fid and fid not in seen:
                    seen.add(fid)
                    put(f, fid)
                    print(f"  earthquake: {fid}")
        except Exception as e:
            print(f"  error: {e}")
        time.sleep(interval)


def replay(path, rate=10):
    interval = 1.0 / rate
    with open(path) as f:
        for line in f:
            try:
                put(json.loads(line))
            except json.JSONDecodeError:
                pass
            time.sleep(interval)


if __name__ == "__main__":
    if SOURCE == "wikimedia":
        wikimedia()
    elif SOURCE == "usgs":
        usgs()
    elif SOURCE == "replay":
        replay(os.getenv("REPLAY_FILE", "sample.json"))
    else:
        raise ValueError(f"Unknown DATA_SOURCE: {SOURCE}")
# usgs and replay support added
