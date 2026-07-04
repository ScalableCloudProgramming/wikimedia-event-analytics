# Wikimedia Event Analytics
### Scalable Real-Time Pipeline · Lambda Architecture · Python · Apache Spark · AWS

A cloud-based pipeline that ingests the **Wikimedia Event Stream** in real time, processes it through a **batch layer** (accurate full-history views via PySpark on EMR) and a **speed layer** (low-latency sliding-window views via Kinesis), and serves merged results through a **serving layer** backed by S3/Athena + DynamoDB.

---

## Architecture

```
Wikimedia SSE Stream
        │
        ▼
┌───────────────────┐
│  Kinesis Ingest   │   ← ingestion/kinesis_producer.py
└────────┬──────────┘
         │
   ┌─────┴──────┐
   │            │
   ▼            ▼
Batch Layer  Speed Layer
(EMR/Spark)  (Kinesis + DynamoDB)
   │            │
   ▼            ▼
S3 + Athena  DynamoDB
   │            │
   └─────┬──────┘
         ▼
  Serving Layer  ← serving/serving_layer.py
         │
         ▼
    Query / API
```

---

## Repository Structure

```
code/
├── infrastructure/        AWS provisioning (Kinesis, S3, EMR, DynamoDB)
├── ingestion/             Kinesis producer (Wikimedia SSE / USGS / replay)
├── batch/                 PySpark batch job + EMR submit script
├── speed/                 Stream processor + Spark Structured Streaming
├── serving/               Lambda merge (batch + speed → unified view)
└── benchmarks/            Throughput / latency / speedup measurement & plots
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | AWS Kinesis Data Streams + Python boto3 |
| Batch | PySpark on AWS EMR |
| Speed | Kinesis consumer + sliding-window counter → DynamoDB |
| Speed (H1) | Spark Structured Streaming with windowed aggregations |
| Serving | Amazon Athena (batch) + DynamoDB (speed) merge |
| Monitoring | Amazon CloudWatch |

---

## Getting Started

```bash
# 1. Install dependencies
pip install -r ../config/requirements.txt

# 2. Copy and fill in AWS credentials
cp ../config/.env.example ../config/.env

# 3. Stand up AWS infrastructure
python infrastructure/deploy_infrastructure.py

# 4. Start ingestion
python ingestion/kinesis_producer.py

# 5. Submit batch job to EMR
python batch/submit_spark_job.py

# 6. Start speed layer
python speed/stream_processor.py

# 7. Query serving layer
python serving/serving_layer.py --query top_wikis

# 8. Run benchmarks
python benchmarks/benchmark.py --mode all
```

---

## Dataset

**Wikimedia Event Streams** — true real-time SSE stream of every edit across Wikipedia and sister projects.

- URL: `https://stream.wikimedia.org/v2/stream/recentchange`
- No API key required
- Format: Server-Sent Events (JSON payload per event)
- Estimated rate: 10–50 events/s

**Real-time question answered:**
> *Which Wikipedia projects are receiving the most edits in the last 5 minutes?*

---

## Contributors

| Name | GitHub | Role |
|------|--------|------|
| Kasi Reddy | KASIREDDY009 | Batch layer, infrastructure, testing |
| Vishvaksen | vishvak55 | Ingestion, speed layer, serving, benchmarks |
