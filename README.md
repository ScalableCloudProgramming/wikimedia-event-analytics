# Wikimedia Event Analytics

Scalable real-time Wikipedia edit analytics on **AWS**, built as a **Lambda architecture**: continuous ingest, a **batch** path for full-history accuracy, a **speed** path for low-latency sliding windows, and a **serving** layer that merges both views.

**Live question the system answers:**  
*Which Wikipedia projects have the most edits in the last 5 minutes, over full history, and when both views are merged?*

| | |
|---|---|
| **Language** | Python 3 |
| **Stream** | [Wikimedia Event Streams](https://stream.wikimedia.org/v2/stream/recentchange) (SSE) |
| **Cloud** | AWS (Kinesis, S3, EMR/Spark, DynamoDB, Athena, CloudWatch, EC2) |
| **Repository** | https://github.com/ScalableCloudProgramming/wikimedia-event-analytics |

---

## Architecture

```
Wikimedia SSE
      │
      ▼
┌─────────────────┐     dual-write
│  Producer (EC2) │──────────────────► S3 (raw JSONL)
└────────┬────────┘                          │
         │ Kinesis                           ▼
         ▼                            Batch (EMR / PySpark)
  Speed consumer ──► DynamoDB              │
  (5-min window)                           ▼
         │                           Athena (batch view)
         └──────────────┬──────────────────┘
                        ▼
              Serving merge + Streamlit (EC2)
                        │
                        ▼
              CloudWatch metrics / charts
```

**Auto-scaling:** EMR managed scaling (default min 2 / max 8 instances) on YARN memory pressure; cooldown follows EMR managed-scaling defaults. CloudWatch alarms track ingest rate and consumer lag.

---

## Repository layout

```text
.
├── config.py                 # Shared settings (.env)
├── .env.example              # Template configuration
├── requirements.txt
├── infra/                    # Provision / tear down core AWS resources
├── deploy/                   # Package code, launch EC2 roles, orchestrate
├── ingestion/                # Kinesis producer, S3 sink, high-scale mode
├── batch/                    # PySpark batch job + EMR submit
├── speed/                    # Sliding-window consumer + Spark streaming
├── serving/                  # Athena setup + Lambda merge query
├── dashboard/                # Streamlit results UI (runs on EC2)
├── benchmarks/               # Throughput / latency / speedup helpers
├── tests/                    # Unit tests (window, S3 sink)
├── data/                     # Sample JSONL for offline replay
└── docs/                     # Deploy notes
```

---

## Prerequisites

- Python **3.10+**
- AWS account / **AWS Academy Learner Lab** credentials
- AWS CLI configured (`aws configure` or temporary session env vars)
- Permissions for Kinesis, S3, DynamoDB, EMR, EC2, Athena, CloudWatch, SSM (recommended)
- Instance profile name for Academy (often `LabInstanceProfile`)
- Optional: EC2 key pair for SSH (dashboard is HTTP on port 8501)

---

## Quick start

### 1. Clone and install

```bash
git clone https://github.com/ScalableCloudProgramming/wikimedia-event-analytics.git
cd wikimedia-event-analytics   # or cd code if the repo root is the parent folder

python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` at minimum:

| Variable | Notes |
|----------|--------|
| `AWS_REGION` | Often `us-east-1` for Academy |
| `S3_RAW` / `S3_BATCH` | **Globally unique** bucket names |
| `KINESIS_STREAM` | Stream name |
| `DYNAMO_TABLE` | Speed-view table |
| `INSTANCE_PROFILE_NAME` | e.g. `LabInstanceProfile` |
| `EC2_KEY_NAME` | Optional key pair |
| `INGEST_FANOUT` / `INGEST_SYNTH_HZ` | High-scale load knobs |

Configure credentials (local machine only for API calls):

```bash
aws configure
# or:
# export AWS_ACCESS_KEY_ID=...
# export AWS_SECRET_ACCESS_KEY=...
# export AWS_SESSION_TOKEN=...   # if temporary
aws sts get-caller-identity
```

### 3. Deploy the stack

```bash
python deploy/orchestrate.py
```

This provisions core resources (if needed), uploads the code package to S3, creates a CloudWatch dashboard, and launches EC2 roles:

| EC2 name | Role |
|----------|------|
| `wiki-producer` | High-scale ingest → Kinesis + S3 |
| `wiki-speed` | Sliding-window consumer → DynamoDB |
| `wiki-dashboard` | Streamlit UI on port **8501** |

Copy the printed **EMR cluster ID** into `.env` as `EMR_CLUSTER_ID=...`.

Allow **~5 minutes** for EC2 bootstrap (packages + systemd services).

### 4. Accumulate data, then batch + Athena

Let the producer run (15–30+ minutes for large S3 history), then:

```bash
python batch/submit.py
python serving/setup_athena.py
```

### 5. Query and visualise

- Streamlit: `http://<wiki-dashboard-public-ip>:8501`
- CLI merge:

```bash
python serving/query.py
python serving/query.py --plot
```

### 6. Benchmarks (optional)

```bash
python benchmarks/benchmark.py --mode throughput
python benchmarks/benchmark.py --mode latency --rates 50,100,200
python benchmarks/benchmark.py --mode speedup --workers 1,2,4
```

### 7. Tear down

```bash
python deploy/teardown_ec2.py
python infra/teardown.py
```

---

## Component commands

| Task | Command |
|------|---------|
| Infra only | `python infra/setup.py` |
| High-scale producer (local API) | `python ingestion/high_scale.py` |
| Normal producer | `python ingestion/producer.py` |
| Speed layer | `python speed/consumer.py` |
| EMR batch | `python batch/submit.py` |
| Offline batch → S3 parquet | `python batch/local_batch.py` |
| Athena tables | `python serving/setup_athena.py` |
| Unit tests | `python tests/test_window.py && python tests/test_s3_sink.py` |

---

## Configuration reference

See [`.env.example`](.env.example) for the full list. Important groups:

- **Kinesis** — stream name, shard count  
- **S3** — raw + batch buckets and prefixes  
- **EMR** — cluster id, min/max instances  
- **Athena** — database, tables, results location  
- **Speed** — `WINDOW_SECONDS` (default 300), `TOP_N`  
- **High-scale** — fan-out, synthetic rate, put batch size  

---

## Design notes (short)

| Layer | Responsibility |
|-------|----------------|
| **Batch** | Correct, complete aggregates over all raw data on S3 (Spark) |
| **Speed** | Fresh top-$N$ in a sliding time window (default 5 minutes) |
| **Serving** | `merged(wiki) = batch(wiki) + speed(wiki)` via Athena + DynamoDB |

---

## Troubleshooting

| Issue | What to check |
|-------|----------------|
| Bucket create fails | Name already taken — change `S3_RAW` / `S3_BATCH` |
| EC2 cannot read S3 zip | Instance profile permissions |
| Dashboard timeout | Wait for bootstrap; security group port **8501** |
| EMR step fails / 0 records | Explicit schema + recursive JSONL paths (batch job); raw data under `data/` |
| Empty Athena | Batch step must be COMPLETED; re-run `serving/setup_athena.py` |
| Stream 403 from Wikimedia | Producer sends a descriptive User-Agent (already in code) |

---

## Contributors

| Name | GitHub |
|------|--------|
| Kasireddy Vadicharla | [KASIREDDY009](https://github.com/KASIREDDY009) |
| Vishvaksen Machana | [vishvak55](https://github.com/vishvak55) |

---

## License

Academic project for MSc Cloud Computing coursework. Use and adapt with attribution unless otherwise stated by the institution.
