# Design and Implementation of Data Warehousing Solution for Media and Entertainment on Hadoop Ecosystem

A student-scale Big Data data-warehousing project for OTT/media platforms. It uses viewing activity, content catalogue, subscriber, device and billing datasets to demonstrate a complete data pipeline: ingestion, distributed processing, dimensional warehousing and analytical queries.

> **Execution honesty:** the repository contains runnable local Python processing and Hadoop/Spark/Hive job code. A distributed run is only confirmed after executing it on a configured Hadoop cluster and retaining the job output/logs. The sample data is synthetic.

## Project objectives
- Store large media telemetry datasets in HDFS using raw, processed and warehouse zones.
- Process viewing events with Hadoop MapReduce and Apache Spark.
- Build a Hive warehouse using a dimensional (star-schema) model.
- Analyze content popularity, watch hours, viewer engagement and streaming quality.
- Validate input records and preserve reproducible outputs.

## Technology stack
| Layer | Technology | Responsibility |
|---|---|---|
| Storage | HDFS | Distributed raw and processed data |
| Batch processing | Hadoop MapReduce (Streaming) | Aggregate viewing metrics by content |
| Processing / analytics | Apache Spark (PySpark) | Scalable content and QoE aggregates |
| Data warehouse | Apache Hive | External tables, dimensions, facts and SQL analytics |
| Data format | CSV input; ORC-ready Hive model | Interchange and warehouse storage |
| Language | Python, SQL, Bash | ETL, jobs and orchestration |

## Architecture
```text
CSV source datasets
       |
       v
HDFS RAW ZONE  (/media-entertainment/raw)
       |
       v
Validation / cleansing
       |
       +-----------------------+
       |                       |
       v                       v
Hadoop Streaming          Apache Spark
Map -> Shuffle -> Reduce  Batch aggregations
       |                       |
       +-----------+-----------+
                   |
                   v
        HDFS PROCESSED ZONE
                   |
                   v
          Apache Hive Warehouse
       Dimensions + Fact + Analytics
                   |
                   v
     Content / Engagement / QoE reports
```

## Repository layout
- `data/sample/`: sample CSV datasets (synthetic).
- `bigdata/mapreduce/`: Hadoop Streaming mapper and reducer.
- `bigdata/hadoop/run_pipeline.sh`: HDFS upload and distributed Streaming job.
- `bigdata/spark/content_analytics.py`: PySpark batch analytics job.
- `bigdata/hive/`: Hive warehouse schema and analytical queries.
- `bigdata/local/run_mapreduce.py`: local functional test of the same aggregation logic.
- `docs/architecture.md`: detailed architecture and data model.
- `docs/run_guide.md`: practical setup and execution guide.

## Quick start: run the MapReduce logic locally
Requires Python 3.9+; no Hadoop cluster is needed for this local functional test.

**Windows PowerShell**
```powershell
python bigdata/local/run_mapreduce.py
```

**Linux / macOS / WSL2**
```bash
python3 bigdata/local/run_mapreduce.py
```

The command reads `data/sample/staging_viewing_activity.csv`, aggregates stream count, watched seconds and buffering events by content ID, and writes `data/output/content_metrics.tsv`. This validates the transformation logic locally; it is not a distributed Hadoop execution.

## Run on a Hadoop cluster
Requirements: Java, Hadoop HDFS/YARN, Python 3 on worker nodes, and the Hadoop Streaming JAR.

```bash
export HADOOP_STREAMING_JAR=/path/to/hadoop-streaming.jar
chmod +x bigdata/hadoop/run_pipeline.sh
./bigdata/hadoop/run_pipeline.sh
```

The job uploads the viewing CSV to HDFS, executes the mapper/reducer across the cluster and writes results to `/media-entertainment/processed/content_metrics`. Confirm execution using YARN logs and `hdfs dfs -ls` / `hdfs dfs -cat`.

## Run Spark analytics
On a machine with PySpark installed and access to the input CSV:
```bash
spark-submit bigdata/spark/content_analytics.py data/sample/staging_viewing_activity.csv data/output/spark_content_metrics
```
For a cluster run, provide an HDFS input and output path and submit with your cluster's Spark configuration.

## Create Hive analytics tables
After uploading source and MapReduce output to the HDFS locations used in `bigdata/hive/analytics.hql`:
```bash
hive -f bigdata/hive/analytics.hql
```
The script creates external tables and example queries for source counts, top content and aggregate viewing metrics.

## Data warehouse model
The target warehouse is a star schema:
- **Dimensions:** `dim_date`, `dim_user`, `dim_content`, `dim_device`, `dim_subscription_plan`.
- **Facts:** `fact_viewing_activity` (one row per viewing session) and `fact_subscription_billing` (one row per billing transaction).
- **Measures:** watch duration, completion percentage, buffering count/duration, bitrate, billed amount.

Detailed field definitions and relationships are documented in `docs/data_dictionary.md` and `docs/architecture.md`.

## Data quality rules
- Skip the CSV header and malformed rows.
- Reject records without a content ID or with invalid numeric measures.
- Keep non-negative measures only.
- Reconcile source row count against accepted and rejected records before loading the warehouse.

## Limitations and academic scope
- The included datasets are sample/synthetic data, not production streaming telemetry.
- A real HDFS/YARN cluster, Spark cluster and Hive metastore must be installed/configured separately.
- Do not describe cluster processing as completed until the distributed job has run successfully and its output/logs are captured.
- This repository focuses on the Big Data warehouse pipeline; the previous web dashboard is not the core project deliverable.

## Team / academic project
**Project title:** Design and Implementation of Data Warehousing Solution for Media and Entertainment on Hadoop Ecosystem.
