# Media & Entertainment Hadoop Data Warehouse — Run Guide

This guide explains how to run, test, and deploy the entire solution across two primary environments:
1. **Local Test Mode** (Generating test data, inspecting scripts, running Python & Java components locally)
2. **Hadoop Cluster / Docker Mode** (Executing the full Hive, Sqoop, Pig, MapReduce, and Oozie pipelines)

---

## Quick Reference: 3 Steps to Run Locally Right Now

You already have **Java 17** and **Python** installed on your system. You can test the data generator immediately:

```powershell
# 1. Navigate to the project root
cd c:\Users\abhis\Downloads\Bigdata

# 2. Generate 1,000 synthetic records across all 7 staging datasets
python scripts\generate_sample_data.py --records 1000 --output-dir data\sample

# 3. Check generated data files
Get-ChildItem data\sample
```

This generates:
- `staging_subscription_plans.csv`
- `staging_devices.csv`
- `staging_content.csv`
- `staging_users.csv`
- `staging_viewing_activity.csv`
- `staging_user_interactions.csv`
- `staging_subscription_billing.csv`

---

## Running on an Apache Hadoop / Hive Cluster

If you are running on an edge node or Hadoop gateway (Linux/CentOS/Ubuntu), follow these chronological phases:

### Phase 1: Initialize HDFS Storage Tiers
Sets up the 7-tier directory structure (`raw`, `staging`, `processed`, `dimensions`, `facts`, `analytics`, `archive`) with correct permissions:

```bash
chmod +x config/hdfs_directory_structure.sh
./config/hdfs_directory_structure.sh
```

---

### Phase 2: Ingest Upstream Data (Sqoop)
Import catalog and billing data from upstream MySQL into HDFS:

```bash
chmod +x sqoop/*.sh

# Run catalog ingestion (incremental lastmodified)
./sqoop/import_content_catalog.sh

# Run subscriptions ingestion
./sqoop/import_subscriptions.sh

# Run ad impressions ingestion
./sqoop/import_ad_data.sh
```

*(Note: Ensure your MySQL JDBC connection parameters and password files are set in each script before running).*

---

### Phase 3: Raw Telemetry Cleansing & Sessionization (Pig)
Process clickstream and viewing events using 30-minute sessionization:

```bash
# Sessionize user streaming events
pig -x tez -f pig/transform_user_activity.pig \
  -param INPUT_PATH=/data/media_warehouse/raw/user_activity \
  -param OUTPUT_PATH=/data/media_warehouse/processed/user_activity_sessionized

# Calculate daily engagement & co-viewing similarity matrix
pig -x tez -f pig/aggregate_content_metrics.pig \
  -param INPUT_PATH=/data/media_warehouse/processed/user_activity_sessionized \
  -param OUTPUT_PATH=/data/media_warehouse/analytics/content_trending
```

---

### Phase 4: Compile & Run MapReduce Jobs
Compile the custom Java MapReduce drivers with Secondary Sort and Composite Keys:

```bash
# Compile drivers
mkdir -p build/classes
javac -cp $(hadoop classpath) -d build/classes mapreduce/ContentViewCountDriver.java
javac -cp $(hadoop classpath) -d build/classes mapreduce/RevenueAnalysisDriver.java
jar -cvf build/media-warehouse-mr.jar -C build/classes/ .

# Run View Count Driver (Genre, Daypart, Device)
hadoop jar build/media-warehouse-mr.jar \
  com.media.dw.mapreduce.ContentViewCountDriver \
  /data/media_warehouse/processed/user_activity_sessionized \
  /data/media_warehouse/analytics/mr_view_counts

# Run Revenue Analysis Driver (Secondary Sort)
hadoop jar build/media-warehouse-mr.jar \
  com.media.dw.mapreduce.RevenueAnalysisDriver \
  /data/media_warehouse/staging/ad_impressions \
  /data/media_warehouse/analytics/mr_revenue_analysis
```

---

### Phase 5: Create Warehouse Schemas & Run Hive ETL
Deploy tables and execute SCD Type 2 dimension loads and fact table population:

```bash
# 1. Create Staging Tables
hive -f hive/schema/create_staging_tables.hql

# 2. Create Conformed Dimensions (ORC + Snappy)
hive -f hive/schema/create_dimension_tables.hql

# 3. Create Fact Tables (Partitioned by Year/Month, 32 Buckets)
hive -f hive/schema/create_fact_tables.hql

# 4. Run Dimension ETL (SCD Type 2 Stage-and-Merge)
hive -f hive/etl/load_dimensions.hql

# 5. Run Fact ETL (Joins staging to dimensions with DQ filters)
hive -hiveconf LOAD_YEAR=2026 -hiveconf LOAD_MONTH=11 -f hive/etl/load_facts.hql
```

---

### Phase 6: Run Data Quality Gate
Assert row count parity, null constraints, and referential integrity before committing data to reports:

```bash
chmod +x scripts/data_quality_checks.sh
./scripts/data_quality_checks.sh --table fact_content_streaming --partition "year=2026/month=11"
```

---

### Phase 7: Execute Analytical Queries & Gold Marts
Generate business intelligence reports:

```bash
# Content Performance, Top Charts, & Jaccard Recommendations
hive -f hive/analytics/content_analytics.hql

# Churn Prediction Features, LTV, & 12-Month Cohort Retention
hive -f hive/analytics/subscriber_analytics.hql
```

---

### Phase 8: Automated Scheduling via Apache Oozie
To run the entire pipeline automatically every day at 02:00 AM UTC:

```bash
# Upload workflow and scripts to HDFS
hdfs dfs -mkdir -p /apps/media_warehouse/oozie
hdfs dfs -put -f oozie/* /apps/media_warehouse/oozie/
hdfs dfs -put -f hive/ /apps/media_warehouse/
hdfs dfs -put -f pig/ /apps/media_warehouse/
hdfs dfs -put -f scripts/ /apps/media_warehouse/

# Submit and start the coordinator
oozie job -oozie http://oozie-server:11000/oozie -config oozie/job.properties -run
```

---

### Phase 9: Warehouse Health Monitoring
Check HDFS quota footprints, detect small-file anti-patterns, and verify partition freshness:

```bash
chmod +x scripts/monitoring_dashboard.sh
./scripts/monitoring_dashboard.sh --html /tmp/warehouse_status.html
```

---

## Running in a Local Docker Sandbox

Since **Docker** is installed on your machine, you can spin up a lightweight single-node Hadoop/Hive container to run the Hive HQL and MapReduce scripts locally:

```powershell
# 1. Launch an Apache Hive local container
docker run -d --name hive-dw-sandbox -p 10000:10000 -p 9870:9870 apache/hive:4.0.0

# 2. Copy the solution files into the container
docker cp c:\Users\abhis\Downloads\Bigdata hive-dw-sandbox:/workspace

# 3. Enter the container shell and run the Hive scripts
docker exec -it hive-dw-sandbox bash

# Inside the container:
cd /workspace
hive -f hive/schema/create_staging_tables.hql
hive -f hive/schema/create_dimension_tables.hql
hive -f hive/schema/create_fact_tables.hql
```
