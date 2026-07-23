# AWS screenshot checklist (results-oriented)

Capture these after high-scale ingest has run **≥15–20 minutes** and batch has completed once.

## A. Architecture / inventory

| # | Where | What to capture |
|---|--------|-----------------|
| A1 | **EC2 → Instances** | `wiki-producer`, `wiki-speed`, `wiki-dashboard` all **running** |
| A2 | **Kinesis → Data streams → wikimedia-stream** | Stream **ACTIVE**, shard count, open monitoring graphs |
| A3 | **S3 → raw bucket** | `data/yyyy/mm/dd/...` many `.jsonl` objects (large volume) |
| A4 | **S3 → batch bucket** | `output/wiki_edits/`, `keywords/`, `hourly/`, `deploy/` |
| A5 | **DynamoDB → wikimedia-speed-view** | Explore items `speed#1` … top-N with counts |
| A6 | **EMR → Clusters** | Cluster **WAITING/RUNNING**, managed scaling min/max visible |
| A7 | **Athena** | Database `wikimedia_pipeline`, tables `batch_view` (+ keywords/hourly) |

## B. Live scale / performance

| # | Where | What to capture |
|---|--------|-----------------|
| B1 | **CloudWatch → Dashboards → wikimedia-lambda-pipeline** | IncomingRecords rising under high-scale load |
| B2 | Same dashboard | IncomingBytes + PutRecords.Success |
| B3 | Same dashboard | IteratorAgeMilliseconds (lag under load) |
| B4 | **Kinesis → Monitoring** tab | Enhanced view of throughput |
| B5 | **EMR → Steps** | Completed `wikimedia-batch` step with duration |
| B6 | **EMR → Hardware / Application history** | Worker count / scaling if it scaled out |
| B7 | **CloudWatch → Alarms** | `…-high-incoming` / iterator-age alarms |

## C. Results UI (Streamlit on EC2)

Open: `http://<dashboard-public-ip>:8501`

| # | Tab | What to capture |
|---|-----|-----------------|
| C1 | Header metrics | Kinesis status, EMR state, S3 object count, last-min ingest |
| C2 | **Speed layer** | Top wikis bar chart (5-min window) |
| C3 | **Batch layer** | Full-history top wikis from Athena |
| C4 | **Lambda merge** | Merged chart + table + CSV download |
| C5 | **Performance** | Throughput line chart + any benchmark PNGs from S3 |

## D. Optional console proof of services

| # | Where | Notes |
|---|--------|------|
| D1 | EC2 → `wiki-producer` → Connect / logs | `journalctl -u wiki-producer -n 50` showing high sent count |
| D2 | EC2 → `wiki-speed` | Logs showing window top-N flushes |
| D3 | Security group | Inbound **8501** for dashboard |

## Suggested order (15 minutes)

1. EC2 running instances (A1)  
2. Kinesis ACTIVE + monitoring (A2, B4)  
3. CloudWatch dashboard peak load (B1–B3)  
4. S3 raw + batch prefixes (A3–A4)  
5. DynamoDB speed items (A5)  
6. EMR cluster + completed step (A6, B5–B6)  
7. Athena tables (A7)  
8. Streamlit C1–C5 (full results story)  

## Naming files

```
01_ec2_fleet.png
02_kinesis_stream.png
03_cw_incoming_records.png
04_s3_raw_volume.png
05_s3_batch_output.png
06_dynamodb_speed.png
07_emr_cluster_scaling.png
08_emr_batch_step.png
09_athena_tables.png
10_streamlit_merge.png
11_streamlit_performance.png
```
