# Wikimedia Event Analytics

**Scalable Real-Time Wikipedia Event Analytics Using Lambda Architecture**

Python · Apache Spark · AWS (Kinesis, EMR, S3, Athena, DynamoDB)

Ingests the [Wikimedia Event Stream](https://stream.wikimedia.org/v2/stream/recentchange), processes it through a **batch layer** (full-history PySpark on EMR) and a **speed layer** (5-minute sliding window → DynamoDB), and serves a **merged** top-wikis view via Athena + DynamoDB.

---

## Architecture

```
Wikimedia SSE
      │
      ▼
┌─────────────┐     dual-write
│  Producer   │──────────────────► S3 raw (JSONL) ──► Batch (EMR/Spark) ──► S3 parquet
└──────┬──────┘                                              │
       │ Kinesis                                             ▼
       ▼                                                  Athena
 Speed consumer ──► DynamoDB                                 │
 (sliding window)                                            ▼
                     Serving layer ◄──── merge batch + speed
                            │
                            ▼
                     Query / charts
```

Auto-scaling: EMR managed scaling (min/max instances, YARN memory pressure). CloudWatch alarms on Kinesis ingest and iterator age.

---

## Repository layout

```
code/
├── config.py              Shared env / settings
├── .env.example
├── requirements.txt
├── infra/                 AWS setup + teardown
├── ingestion/             Producer + S3 raw sink
├── batch/                 PySpark batch job + EMR submit
├── speed/                 Sliding-window consumer + Spark streaming
├── serving/               Athena setup + Lambda merge query
├── benchmarks/            Throughput / latency / speedup
├── tests/
└── data/sample_events.jsonl
```

---

## AWS-only high-scale deploy (recommended)

Continuous work runs on **EC2 + EMR**; laptop only calls AWS APIs.

```bash
cd code
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # unique bucket names, region, LabInstanceProfile
# configure AWS credentials, then:
python deploy/orchestrate.py
```

After bootstrap (~5 min): open Streamlit at `http://<dashboard-ip>:8501`.

Full guide: [`docs/AWS_DEPLOY.md`](docs/AWS_DEPLOY.md)  
Screenshot checklist: [`docs/SCREENSHOTS.md`](docs/SCREENSHOTS.md)

| Role on AWS | Process |
|-------------|---------|
| EC2 `wiki-producer` | `ingestion/high_scale.py` (live + fan-out + synthetic load) |
| EC2 `wiki-speed` | `speed/consumer.py` |
| EC2 `wiki-dashboard` | Streamlit `dashboard/app.py` :8501 |
| EMR | `batch/submit.py` Spark job |
| Athena / DynamoDB / CloudWatch | serving + metrics |

### Batch + Athena (after large ingest)

```bash
python batch/submit.py
python serving/setup_athena.py
```

### Benchmarks

```bash
python benchmarks/benchmark.py --mode throughput
python benchmarks/benchmark.py --mode latency --rates 50,100,200,500
python benchmarks/benchmark.py --mode speedup --workers 1,2,4
aws s3 cp benchmarks/results/ s3://$S3_BATCH/benchmarks/results/ --recursive
```

### Teardown

```bash
python deploy/teardown_ec2.py
python infra/teardown.py
```

### Local unit tests only

```bash
python tests/test_window.py
python tests/test_s3_sink.py
```

---

## Real-time question

> Which Wikipedia projects receive the most edits in the last 5 minutes (speed), over full history (batch), and combined (serving merge)?

---

## Contributors

| Name | GitHub | Focus |
|------|--------|--------|
| Kasi Reddy | KASIREDDY009 | Infra, batch, speed consumer, benchmarks |
| Vishvaksen | vishvak55 | Ingestion, streaming, serving, tests |

Repo: https://github.com/ScalableCloudProgramming/wikimedia-event-analytics

## IEEE report

See `report/ieee_report.pdf` (source: `report/ieee_report.tex`) and `screenshots/` for demo evidence.
