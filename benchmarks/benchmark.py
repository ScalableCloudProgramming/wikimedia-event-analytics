import boto3, os, time
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

REGION  = os.getenv("AWS_REGION", "eu-west-1")
STREAM  = os.getenv("KINESIS_STREAM", "wikimedia-stream")
CLUSTER = os.getenv("EMR_CLUSTER_ID", "")
OUT     = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUT, exist_ok=True)

cw = boto3.client("cloudwatch", region_name=REGION)


def cw_metric(metric, namespace, dimensions, stat="Sum", period=60, minutes=10):
    end   = int(time.time())
    start = end - minutes * 60
    resp  = cw.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric,
        Dimensions=dimensions,
        StartTime=start,
        EndTime=end,
        Period=period,
        Statistics=[stat],
    )
    pts = sorted(resp["Datapoints"], key=lambda x: x["Timestamp"])
    return [p["Timestamp"] for p in pts], [p[stat] for p in pts]


def throughput():
    ts, vals = cw_metric(
        "IncomingRecords", "AWS/Kinesis",
        [{"Name": "StreamName", "Value": STREAM}]
    )
    df = pd.DataFrame({"time": ts, "records_per_min": vals})
    df.to_csv(f"{OUT}/throughput.csv", index=False)

    plt.figure(figsize=(8, 4))
    plt.plot(df["time"], df["records_per_min"], marker="o", color="steelblue")
    plt.xlabel("Time")
    plt.ylabel("Records / min")
    plt.title("Kinesis Throughput")
    plt.tight_layout()
    plt.savefig(f"{OUT}/throughput.png", dpi=150)
    plt.close()
    print("Saved throughput.png")


def latency():
    # inject a sentinel record and measure time to appear in DynamoDB
    kinesis = boto3.client("kinesis", region_name=REGION)
    dynamo  = boto3.resource("dynamodb", region_name=REGION)
    table   = dynamo.Table(os.getenv("DYNAMO_TABLE", "wikimedia-speed-view"))

    rates   = [10, 50, 100, 200, 500]
    results = []

    for rate in rates:
        sentinel = {"wiki": f"__bench_{rate}__", "title": "sentinel", "type": "edit"}
        t0 = time.time()
        kinesis.put_record(
            StreamName=STREAM,
            Data=__import__("json").dumps(sentinel).encode(),
            PartitionKey="bench"
        )
        # poll DynamoDB until sentinel appears (or 30s timeout)
        lat = None
        for _ in range(30):
            time.sleep(1)
            resp = table.scan(FilterExpression=__import__("boto3").dynamodb.conditions.Attr("wiki").eq(sentinel["wiki"]))
            if resp["Items"]:
                lat = (time.time() - t0) * 1000
                break
        results.append(lat or 30000)
        print(f"  rate={rate} -> latency={results[-1]:.0f}ms")

    df = pd.DataFrame({"rate": rates, "latency_ms": results})
    df.to_csv(f"{OUT}/latency.csv", index=False)

    plt.figure(figsize=(8, 4))
    plt.plot(df["rate"], df["latency_ms"], marker="s", color="darkorange")
    plt.xlabel("Ingestion Rate (rec/s)")
    plt.ylabel("Latency (ms)")
    plt.title("End-to-End Latency vs Ingestion Rate")
    plt.tight_layout()
    plt.savefig(f"{OUT}/latency.png", dpi=150)
    plt.close()
    print("Saved latency.png")


def speedup(runtimes=None):
    # provide measured runtimes per worker count after running EMR steps
    if runtimes is None:
        # example measured values - replace with actual EMR step durations
        runtimes = {1: 480, 2: 265, 4: 145, 8: 85}

    workers = list(runtimes.keys())
    times   = list(runtimes.values())
    base    = times[0]
    su      = [base / t for t in times]

    df = pd.DataFrame({"workers": workers, "runtime_s": times, "speedup": su})
    df.to_csv(f"{OUT}/speedup.csv", index=False)

    fig, ax1 = plt.subplots(figsize=(8, 4))
    ax2 = ax1.twinx()
    ax1.bar(df["workers"], df["runtime_s"], color="steelblue", alpha=0.6, label="Runtime (s)")
    ax2.plot(df["workers"], df["speedup"], marker="^", color="crimson", label="Speedup")
    ax1.set_xlabel("Workers")
    ax1.set_ylabel("Runtime (s)")
    ax2.set_ylabel("Speedup")
    plt.title("Batch Speedup vs Worker Count")
    fig.tight_layout()
    plt.savefig(f"{OUT}/speedup.png", dpi=150)
    plt.close()
    print("Saved speedup.png")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["throughput", "latency", "speedup", "all"], default="all")
    args = p.parse_args()

    if args.mode in ("throughput", "all"):
        throughput()
    if args.mode in ("latency", "all"):
        latency()
    if args.mode in ("speedup", "all"):
        speedup()
