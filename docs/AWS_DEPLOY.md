# AWS-only deployment (high-scale)

Everything continuous runs on AWS: **EC2 producer**, **EC2 speed consumer**, **EC2 Streamlit**, **EMR batch**, **Kinesis / S3 / DynamoDB / Athena / CloudWatch**.

Your laptop is used only to **call AWS APIs** once (deploy + optional batch submit).

## 0. Prerequisites

- AWS Academy (or account) with rights for EC2, IAM instance profile, Kinesis, S3, DynamoDB, EMR, Athena, CloudWatch  
- Instance profile name (Academy default often `LabInstanceProfile`)  
- Optional: EC2 key pair for SSH debugging  

## 1. Configure credentials (on your machine — one time)

```bash
cd code
cp .env.example .env
# edit .env — region, unique bucket names, high-scale knobs

export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...   # if Academy temporary creds
# or: aws configure
```

Academy tips:

```bash
# in .env
AWS_REGION=us-east-1          # often required by Academy
INSTANCE_PROFILE_NAME=LabInstanceProfile
EC2_KEY_NAME=your-key         # optional
EC2_INSTANCE_TYPE=t3.medium
INGEST_FANOUT=8               # multiply each live edit
INGEST_SYNTH_HZ=150           # extra synthetic load
KINESIS_SHARDS=4              # higher parallel ingest
S3_RAW=yourname-wiki-raw-001  # must be globally unique
S3_BATCH=yourname-wiki-batch-001
```

## 2. Deploy full stack

```bash
pip install -r requirements.txt
python deploy/orchestrate.py
```

This will:

1. Create Kinesis / S3 / DynamoDB / EMR + alarms  
2. Upload code zip to S3  
3. Create CloudWatch dashboard  
4. Launch 3 EC2 instances (producer, speed, dashboard)  

Copy **EMR_CLUSTER_ID** from setup output into `.env`.

## 3. Accumulate large data (automatic on EC2)

Producer runs `ingestion/high_scale.py`:

- Live Wikimedia SSE  
- **Fan-out** copies per event  
- **Synthetic** high-Hz traffic (same domain: wiki/title patterns)  
- Batched `put_records` + dual-write JSONL to S3  

Wait **15–30+ minutes** for large S3 raw volume.

## 4. Batch + Athena (API from laptop is fine)

```bash
python batch/submit.py
python serving/setup_athena.py
```

## 5. Open results dashboard (on AWS)

```
http://<wiki-dashboard public IP>:8501
```

Tabs: Lambda merge · Speed · Batch · Performance · Architecture  

## 6. Benchmarks

```bash
python benchmarks/benchmark.py --mode throughput
python benchmarks/benchmark.py --mode latency --rates 50,100,200,500
python benchmarks/benchmark.py --mode speedup --workers 1,2,4
# then upload plots if you want them on the dashboard:
aws s3 cp benchmarks/results/ s3://$S3_BATCH/benchmarks/results/ --recursive
```

## 7. Screenshots

See `docs/SCREENSHOTS.md`.

## 8. Teardown (save credits)

```bash
python deploy/teardown_ec2.py
python infra/teardown.py
```

## Troubleshooting

| Issue | Fix |
|-------|-----|
| EC2 can't read S3 zip | Instance profile missing S3/Kinesis/Dynamo permissions |
| Dashboard timeout | Wait 5 min bootstrap; check SG port 8501 |
| Empty Athena | Batch step must COMPLETED; re-run setup_athena |
| Low throughput | Raise `INGEST_FANOUT`, `INGEST_SYNTH_HZ`, `KINESIS_SHARDS` |
| Bucket name taken | Change `S3_RAW` / `S3_BATCH` to unique names |
