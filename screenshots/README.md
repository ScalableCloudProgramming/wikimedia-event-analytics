# Screenshots — final set (ready for report / demo)

Organized after review. Prefer files in this folder over raw Desktop-style HEIC names in the repo root.

## Primary (use these)

| # | File | Content |
|---|------|---------|
| 01 | `01_ec2_fleet.png` | EC2 wiki-producer / speed / dashboard Running |
| 02 | `02_kinesis_stream.png` | Kinesis ACTIVE, 4 shards |
| 03 | `03_kinesis_monitoring.png` | Ingest metrics (IncomingRecords) |
| 04 | `04_cloudwatch_dashboard.png` | wikimedia-lambda-pipeline dashboard |
| 05 | `05_s3_raw_volume.png` | Scaled raw JSONL on S3 |
| 06 | `06_s3_batch_output.png` | Batch parquet wiki_edits |
| 07 | `07_dynamodb_speed.png` | Speed-layer DynamoDB items |
| 08 | `08_emr_cluster.png` | EMR Waiting + Spark |
| 09 | `09_emr_step_completed.png` | wikimedia-batch COMPLETED |
| 10 | `10_athena_query.png` | Athena SQL + results |
| 11 | `11_streamlit_metrics_and_merge.png` | Metrics EMR WAITING + merge |
| 12 | `12_streamlit_speed_layer.png` | Speed tab |
| 13 | `13_streamlit_batch_layer.png` | Batch tab |
| 14 | `14_streamlit_lambda_merge.png` | Lambda merge chart |
| 15 | `15_streamlit_performance.png` | Performance tab |

## Benchmarks (`benchmarks/`)

| File | Use |
|------|-----|
| `throughput.png` / `16_throughput.png` | Yes — report |
| `speedup.png` / `18_speedup.png` | Yes — report |
| `merged_topn.png` / `19_merged_topn.png` | Yes — report |
| `latency.png` / `17_latency_timeout_weak.png` | Optional only (60s timeout) |

## Archive (`_archive_duplicates/`)

Do **not** use in report:

- `streamlit_header_TERMINATED_do_not_use.png`
- superseded Athena / partial UI crops

## Report figure order

01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09 → 10 → 11 → 12 → 13 → 14 → 15 → benchmarks throughput → speedup → merged_topn
