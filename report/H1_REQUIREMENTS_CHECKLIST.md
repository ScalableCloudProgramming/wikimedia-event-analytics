# H1 Rubric Coverage — Word-for-Word Mapping

Source: `scalable_brief.pdf` (MSCCLOUD_JAN26BI CA).  
Report: `ieee_report.tex` → target ≤10 pages, IEEE double-column.

Legend: **Covered in report section** | Evidence (code / AWS / figure)

---

## Global submission rules (Instructions)

| Brief requirement | Report / delivery |
|-------------------|-------------------|
| Groups of two | Authors: Kasireddy Vadicharla (X25104047), Vishvaksen Machana (X25173421) |
| Python-based scalable cloud project on AWS Academy | Entire implementation on AWS Academy Learner Lab |
| IEEE double-column, **maximum 10 pages** | `ieee_report.tex` (IEEEtran) |
| GitHub link in report | https://github.com/ScalableCloudProgramming/wikimedia-event-analytics |
| Video link in report | Placeholder § Demo Video (fill after upload) |
| Objectives, tools, architecture, results, critical analysis | §§ I–VI structure |

---

## System must (Project Description)

| Brief text | Coverage |
|------------|----------|
| Ingest continuous stream into AWS | Kinesis + dual-write S3; §III-A, Fig. Kinesis/S3 |
| Batch layer: accurate full-history (MapReduce/Spark) | PySpark on EMR; §III-B |
| Speed layer: low-latency recent data + **windowing** | 5-min sliding window; §III-C |
| Store and visualise with AWS | S3, DynamoDB, Athena, Streamlit on EC2, CloudWatch; §III-D, §IV |
| Auto-scale compute under load | EMR managed scaling; triggers/cooldowns stated; §II-C |
| Lambda: batch correctness + speed freshness + serving merge | Architecture §II; merge §III-D |

---

## Phase 1 — Design & Setup (20)

### Problem Analysis & Use Case (5) — H1: *Innovative use case; strong justification for Lambda split*

| Brief bullet | Report location |
|--------------|-----------------|
| Define problem + real-time question | §I: “Which Wikipedia projects receive the most edits in the last 5 minutes, over full history, and merged?” |
| Justify Lambda vs batch-only or stream-only | §I: batch for correctness/full history; speed for freshness; merge for complete answer |

### AWS Setup & Architecture Diagram (15) — H1: *Complete cloud setup; clear Lambda diagram; auto-scaling with stated triggers*

| Brief bullet | Report location |
|--------------|-----------------|
| Architecture diagram: ingestion, batch, speed, serving, auto-scaling boundary | Fig. 1 architecture + prose §II |
| Stand up Kinesis/EMR/EC2/S3/Athena (etc.) | §II-B services table; Figs. EC2, Kinesis, EMR, S3 |
| Auto-scaling policies + **triggers** + **cooldowns** | §II-C: EMR managed scaling min 2–max 8; trigger YARN memory pressure; cooldown EMR managed default; CW alarms |

### Data Ingestion — H1: *Seamless stream ingestion Kinesis + Python*

| Brief bullet | Report location |
|--------------|-----------------|
| Kinesis (or Kafka) ingest | §III-A; Fig. Kinesis Active 4 shards |
| Python boto3 producer | `ingestion/producer.py`, `high_scale.py` |
| Demonstrate records flowing | Figs. Kinesis monitoring, S3 raw volume, CW IncomingRecords |

---

## Phase 2 — Parallel Processing (45)

### Batch (15) — H1: *Efficient Spark batch; accurate complete results*

| Brief bullet | Report location |
|--------------|-----------------|
| PySpark on EMR full history | §III-B; Fig. EMR step COMPLETED |
| Meaningful aggregates (top-N, keywords, etc.) | wiki_edits, keywords, hourly, edit_types, namespaces |
| Correct complete results on accumulated data | Athena results table; ~10⁵-scale records; S3 parquet |

### Speed (15) — H1: *Sliding window top-N in last N minutes; low-latency incremental*

| Brief bullet | Report location |
|--------------|-----------------|
| Real-time filter + count | Bot/type filters; per-wiki counts §III-C |
| Low-latency incremental updates | DynamoDB continuous flush; Streamlit speed tab |
| **Sliding window** (discriminator) | 300 s window, 10 s buckets; top-N; Fig. DynamoDB + Streamlit speed |

### Hybrid & Serving (15) — H1: *Coherent merge; data+task parallelism; clear benchmarks*

| Brief bullet | Report location |
|--------------|-----------------|
| Data parallelism (Spark) + task parallelism (EMR/EC2 workers) | §III-B, §III-E |
| Serving merge batch + speed | §III-D; Figs. Streamlit merge, Athena |
| Benchmark sequential vs parallel + speed under load | §IV speedup (1–8 workers); throughput under high-scale ingest |

---

## Phase 3 — Performance & Reporting (35)

### Performance (10) — H1: *Multiple metrics; clear graphs and analysis*

| Brief bullet | Report location |
|--------------|-----------------|
| Throughput, latency, speedup under different loads | §IV tables + figures |
| Graphs: speedup vs workers; latency vs rate; throughput over time | Figs. throughput, speedup; latency discussed honestly |

### Report (15) — H1: *Well-formatted IEEE; structured; citations; high clarity*

| Brief bullet | Report location |
|--------------|-----------------|
| IEEE double-column ≤10 pages | IEEEtran `ieee_report.tex` |
| Objectives, architecture, tools, implementation, results, critical analysis | §§ I–V |

### Demo Video (10) — H1: *Clear demo covering architecture, live pipeline, batch/speed, auto-scaling, benchmarks*

| Brief bullet | Report location |
|--------------|-----------------|
| Video walkthrough | §VI placeholder URL + script outline matching brief list |

---

## Honest gaps called out in Critical Analysis (does not hide them)

1. Sentinel latency probe timed out at 60 s under burst (use CW iterator age + operational flush as supporting latency evidence).  
2. Multi-worker speedup series partly uses measured 2-worker EMR time (~74.6 s) with additional calibrated points—state clearly.  
3. Demo video must still be recorded/uploaded for full Phase 3 video marks.

These are discussed in §V so the report remains credible for H1-level analysis.
