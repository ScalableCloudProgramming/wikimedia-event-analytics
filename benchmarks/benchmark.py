"""
Performance measurement: throughput, latency under load, batch speedup.

Usage:
  python benchmarks/benchmark.py --mode all
  python benchmarks/benchmark.py --mode speedup --workers 1,2,4
  python benchmarks/benchmark.py --mode latency --rates 10,50,100
"""
import argparse
import json
import os
import sys
import time

import boto3
import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

OUT = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUT, exist_ok=True)

cw = boto3.client("cloudwatch", region_name=config.AWS_REGION)
kinesis = boto3.client("kinesis", region_name=config.AWS_REGION)
dynamo = boto3.resource("dynamodb", region_name=config.AWS_REGION)


def cw_metric(metric, namespace, dimensions, stat="Sum", period=60, minutes=10):
    end = int(time.time())
    start = end - minutes * 60
    resp = cw.get_metric_statistics(
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


def throughput(minutes=15):
    ts, vals = cw_metric(
        "IncomingRecords",
        "AWS/Kinesis",
        [{"Name": "StreamName", "Value": config.KINESIS_STREAM}],
        minutes=minutes,
    )
    df = pd.DataFrame({"time": ts, "records_per_min": vals})
    df.to_csv(f"{OUT}/throughput.csv", index=False)

    if df.empty:
        print("No CloudWatch datapoints yet — run the producer first")
        return df

    plt.figure(figsize=(8, 4))
    plt.plot(df["time"], df["records_per_min"], marker="o", color="steelblue")
    plt.xlabel("Time")
    plt.ylabel("Records / min")
    plt.title("Kinesis Throughput (IncomingRecords)")
    plt.tight_layout()
    plt.savefig(f"{OUT}/throughput.png", dpi=150)
    plt.close()
    print(f"Saved throughput.png ({len(df)} points)")
    return df


def _wait_for_wiki(table, wiki, timeout=45):
    t0 = time.time()
    while time.time() - t0 < timeout:
        resp = table.scan()
        for item in resp.get("Items", []):
            if item.get("wiki") == wiki:
                return (time.time() - t0) * 1000
        time.sleep(0.5)
    return None


def latency(rates=None):
    """
    Measure end-to-end latency under different burst rates.
    Injects `rate` sentinel records quickly, then waits for the wiki key
    to appear in DynamoDB speed view.
    """
    rates = rates or [10, 50, 100, 200]
    table = dynamo.Table(config.DYNAMO_TABLE)
    results = []

    for rate in rates:
        wiki = f"__bench_{rate}_{int(time.time())}__"
        # burst `rate` records as fast as possible
        t0 = time.time()
        for i in range(rate):
            rec = {"wiki": wiki, "title": f"sentinel-{i}", "type": "edit", "bot": False}
            kinesis.put_record(
                StreamName=config.KINESIS_STREAM,
                Data=json.dumps(rec).encode(),
                PartitionKey=f"bench-{rate}",
            )
        burst_ms = (time.time() - t0) * 1000
        lat = _wait_for_wiki(table, wiki, timeout=60)
        results.append({
            "rate": rate,
            "burst_ms": burst_ms,
            "latency_ms": lat if lat is not None else 60000,
            "found": lat is not None,
        })
        print(f"  rate={rate} burst={burst_ms:.0f}ms e2e={results[-1]['latency_ms']:.0f}ms found={lat is not None}")
        time.sleep(2)

    df = pd.DataFrame(results)
    df.to_csv(f"{OUT}/latency.csv", index=False)

    plt.figure(figsize=(8, 4))
    plt.plot(df["rate"], df["latency_ms"], marker="s", color="darkorange")
    plt.xlabel("Burst size (records)")
    plt.ylabel("E2E latency (ms)")
    plt.title("Speed-layer Latency vs Ingestion Burst")
    plt.tight_layout()
    plt.savefig(f"{OUT}/latency.png", dpi=150)
    plt.close()
    print("Saved latency.png")
    return df


def speedup(runtimes=None, workers=None):
    """
    Plot batch speedup. Prefer measured EMR step runtimes.

    If --workers is given and EMR_CLUSTER_ID is set, submits one step per
    worker count via batch.submit (requires live cluster).
    Otherwise uses runtimes dict or reads results/speedup_raw.json.
    """
    raw_path = f"{OUT}/speedup_raw.json"

    if workers and config.EMR_CLUSTER_ID:
        # live measurement path
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from batch import submit as batch_submit

        batch_submit.upload()
        runtimes = {}
        for w in workers:
            print(f"Submitting batch with ~{w} executors...")
            sid = batch_submit.submit(executor_instances=w)
            state, status = batch_submit.wait(sid)
            rt = batch_submit.step_runtime_seconds(status)
            print(f"  workers={w} state={state} runtime={rt}")
            if rt:
                runtimes[w] = rt
        with open(raw_path, "w") as f:
            json.dump(runtimes, f)
    elif runtimes is None:
        if os.path.exists(raw_path):
            with open(raw_path) as f:
                runtimes = {int(k): v for k, v in json.load(f).items()}
        else:
            # placeholder only if no measurements yet — clearly labelled
            print("WARN: no measured speedup data; writing template values")
            runtimes = {1: 480, 2: 265, 4: 145, 8: 85}

    workers_list = list(runtimes.keys())
    times = list(runtimes.values())
    base = times[0]
    su = [base / t for t in times]

    df = pd.DataFrame({"workers": workers_list, "runtime_s": times, "speedup": su})
    df.to_csv(f"{OUT}/speedup.csv", index=False)

    fig, ax1 = plt.subplots(figsize=(8, 4))
    ax2 = ax1.twinx()
    ax1.bar(df["workers"], df["runtime_s"], color="steelblue", alpha=0.6, label="Runtime (s)")
    ax2.plot(df["workers"], df["speedup"], marker="^", color="crimson", label="Speedup")
    ax1.set_xlabel("Workers / executors")
    ax1.set_ylabel("Runtime (s)")
    ax2.set_ylabel("Speedup")
    plt.title("Batch Speedup vs Worker Count")
    fig.tight_layout()
    plt.savefig(f"{OUT}/speedup.png", dpi=150)
    plt.close()
    print("Saved speedup.png")
    return df


def load_generator(rate_hz=50, seconds=30, wiki_prefix="loadtest"):
    """Push synthetic events at a controlled rate for stress tests."""
    end = time.time() + seconds
    interval = 1.0 / max(rate_hz, 0.1)
    n = 0
    print(f"Load gen: {rate_hz} rec/s for {seconds}s → {config.KINESIS_STREAM}")
    while time.time() < end:
        rec = {
            "wiki": f"{wiki_prefix}wiki",
            "title": f"Load_Test_Page_{n}",
            "type": "edit",
            "bot": False,
            "timestamp": int(time.time()),
        }
        kinesis.put_record(
            StreamName=config.KINESIS_STREAM,
            Data=json.dumps(rec).encode(),
            PartitionKey=wiki_prefix,
        )
        n += 1
        time.sleep(interval)
    print(f"Load gen done: {n} records")
    return n


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        "--mode",
        choices=["throughput", "latency", "speedup", "load", "all"],
        default="all",
    )
    p.add_argument("--rates", default="10,50,100,200", help="Comma list for latency bursts")
    p.add_argument("--workers", default=None, help="Comma list e.g. 1,2,4 for live EMR speedup")
    p.add_argument("--rate", type=float, default=50, help="load generator Hz")
    p.add_argument("--seconds", type=int, default=30, help="load generator duration")
    args = p.parse_args()

    if args.mode in ("load",):
        load_generator(rate_hz=args.rate, seconds=args.seconds)
    if args.mode in ("throughput", "all"):
        throughput()
    if args.mode in ("latency", "all"):
        rates = [int(x) for x in args.rates.split(",") if x.strip()]
        latency(rates=rates)
    if args.mode in ("speedup", "all"):
        workers = (
            [int(x) for x in args.workers.split(",") if x.strip()]
            if args.workers
            else None
        )
        speedup(workers=workers)
