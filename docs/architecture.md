# System Architecture

## Project
**Design and Implementation of Data Warehousing Solution for Media and Entertainment on Hadoop Ecosystem**

## 1. Scope
This implementation models an OTT/media platform data warehouse using supplied synthetic CSV datasets. It demonstrates the core Big Data stages with Hadoop ecosystem technologies: distributed storage (HDFS), batch processing (Hadoop MapReduce and Spark), and SQL warehousing/analytics (Hive). Dataset sizes in this repository are for academic demonstration; no production-scale throughput or SLA is claimed.

## 2. Logical architecture
```text
Source CSVs
(content, users, devices, plans, billing, viewing activity, interactions)
        |
        v
HDFS RAW ZONE
/media-entertainment/raw/<dataset>
        |
        v
Validation and cleansing
(header handling, required keys, numeric checks, invalid-row accounting)
        |
        +---------------------------+
        |                           |
        v                           v
Hadoop Streaming MapReduce      Apache Spark
content-level batch metrics     batch aggregation / data quality
        |                           |
        +-------------+-------------+
                      |
                      v
HDFS PROCESSED ZONE
                      |
                      v
Apache Hive Data Warehouse
dimensions + fact tables + external result tables
                      |
                      v
Analytical SQL / reporting
(content popularity, watch hours, completion, buffering)
```

## 3. HDFS zones
| Zone | Example path | Purpose |
|---|---|---|
| Raw | `/media-entertainment/raw/viewing_activity` | Immutable source extracts |
| Processed | `/media-entertainment/processed/content_metrics` | MapReduce/Spark output |
| Warehouse | Hive warehouse location | Curated dimensional tables |

Keep source files unchanged in the raw zone. Write transformations to a separate processed path so a run can be reproduced.

## 4. Hadoop MapReduce
The Streaming mapper reads viewing activity and emits:
`content_id<TAB>1<TAB>watch_seconds<TAB>buffer_count`

Hadoop sorts mapper output by content ID and sends grouped keys to the reducer. The reducer calculates stream count, total watched seconds and buffering-event count per content ID. The local runner at `bigdata/local/run_mapreduce.py` validates equivalent aggregation logic without claiming a cluster run.

## 5. Spark processing
The PySpark job reads a CSV file, casts measures to numeric types, filters invalid rows, groups by content ID and writes aggregate output. It can run in local mode or be submitted to a configured Spark cluster. The output path may be local or HDFS depending on the Spark environment.

## 6. Hive dimensional model
The warehouse schema is defined in `bigdata/hive/warehouse_schema.hql`.

### Dimensions
- `dim_date`: calendar attributes used for time-based reporting.
- `dim_user`: viewer and subscription segment attributes.
- `dim_content`: title, type, genre and release year.
- `dim_device`: device and operating-system attributes.

### Facts
- `fact_viewing_activity`: one row per viewing session, with watch duration, completion, bitrate, buffering, pause, seek and error measures.
- `fact_subscription_billing`: one row per billing transaction, with billed amount and payment status.

The fact tables are partitioned by year and month to support partition pruning. The DDL defines the target model; a separate dimension/fact load process is needed to populate it.

## 7. Data quality controls
1. Ignore the header row.
2. Require a non-empty content identifier.
3. Cast viewing duration and buffering count to numeric values.
4. Reject negative measures.
5. Record accepted/rejected counts in the local runner.
6. Compare source and output totals before accepting a warehouse load.

## 8. Execution evidence
A successful local run proves the local transformation only. A successful distributed run must be supported by:
- YARN application/job ID and successful final status.
- HDFS listing of the output directory.
- Non-empty reducer part files.
- Hive query results from the configured metastore.

Do not claim HDFS, YARN, Spark cluster or Hive execution until those checks have been completed on an actual configured environment.
