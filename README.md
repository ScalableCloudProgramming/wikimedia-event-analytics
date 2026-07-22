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

## Setup

```bash
cd code
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill AWS credentials / region / names
```

### 1. Infrastructure

```bash
python infra/setup.py
# set EMR_CLUSTER_ID=... in .env
```

### 2. Ingestion (live or replay)

```bash
# live Wikimedia stream (Kinesis + S3 dual-write)
python ingestion/producer.py

# offline replay
DATA_SOURCE=replay python ingestion/producer.py
```

### 3. Batch layer

```bash
python batch/submit.py
# optional speedup run:
python batch/submit.py --executors 4
```

### 4. Speed layer

```bash
python speed/consumer.py
# optional Spark streaming variant:
# python speed/streaming.py   # on EMR / local Spark with Kinesis connector
```

### 5. Serving (Athena tables + merge)

```bash
python serving/setup_athena.py
python serving/query.py
python serving/query.py --plot
```

### 6. Benchmarks

```bash
python benchmarks/benchmark.py --mode load --rate 50 --seconds 60
python benchmarks/benchmark.py --mode throughput
python benchmarks/benchmark.py --mode latency --rates 10,50,100,200
python benchmarks/benchmark.py --mode speedup --workers 1,2,4
```

### 7. Tests

```bash
python tests/test_window.py
python tests/test_s3_sink.py
```

### Teardown

```bash
python infra/teardown.py
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
