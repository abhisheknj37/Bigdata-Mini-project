# Big Data Pipeline

This directory contains the runnable processing components for the media and entertainment warehouse.

## Components
- `mapreduce/mapper.py` and `mapreduce/reducer.py`: Hadoop Streaming aggregation by content ID.
- `hadoop/run_pipeline.sh`: uploads viewing activity to HDFS and submits the Streaming job to YARN.
- `spark/content_analytics.py`: PySpark batch aggregation and quality filtering.
- `hive/warehouse_schema.hql`: dimensional warehouse DDL (dimensions and partitioned fact tables).
- `hive/analytics.hql`: external tables over raw and MapReduce output plus analytical SQL.
- `local/run_mapreduce.py`: local test of content aggregation logic.

## Local validation
From the repository root:
```bash
python bigdata/local/run_mapreduce.py
```
It writes a TSV file to `data/output/content_metrics.tsv`. This does not require Hadoop and is not a distributed job.

## Hadoop Streaming execution
On a configured Hadoop host with HDFS and YARN:
```bash
export HADOOP_STREAMING_JAR=/path/to/hadoop-streaming.jar
chmod +x bigdata/hadoop/run_pipeline.sh
./bigdata/hadoop/run_pipeline.sh
```
The script uses `data/sample/staging_viewing_activity.csv` by default. It writes to `/media-entertainment/processed/content_metrics`.

## Spark execution
```bash
spark-submit bigdata/spark/content_analytics.py \
  data/sample/staging_viewing_activity.csv data/output/spark_content_metrics
```
For cluster execution, use an HDFS input and output path and your cluster's Spark configuration.

## Hive
Run `hive/warehouse_schema.hql` to create the target warehouse schema. Run `hive/analytics.hql` after placing source and result files in the HDFS locations declared by its external tables. The DDL is a model; dimension and fact population still requires an ETL load step.
