# Actual Big Data execution: HDFS + Hadoop MapReduce + Hive

This folder adds an executable distributed-processing path alongside the existing SQLite simulator.

## What runs for real
1. Upload the viewing-activity CSV into HDFS.
2. Hadoop Streaming launches a MapReduce job with two reducers.
3. Mapper tasks emit content-level stream, watched-time and buffering metrics.
4. Reducers aggregate those metrics across input splits.
5. Results are written to HDFS and exposed as an external Hive table.

The React/Flask dashboard is a presentation/API layer. Do not label SQLite results as Hadoop results.

## Prerequisites
- Hadoop installed and HDFS/YARN services running.
- Hadoop Streaming JAR from the same Hadoop distribution.
- Python 3 available on every worker that runs a task.
- Hive configured against the same HDFS namespace for Hive queries.
- Run on Linux, WSL2 or a Hadoop cluster host. Hadoop must be running, not merely installed.

## Run the distributed job
From the repository root:
```bash
export HADOOP_STREAMING_JAR=/path/to/hadoop/share/hadoop/tools/lib/hadoop-streaming-VERSION.jar
chmod +x bigdata/hadoop/run_pipeline.sh
bigdata/hadoop/run_pipeline.sh
```
The script fails if the Streaming JAR, input CSV, HDFS or Hadoop CLI is unavailable. It does not silently fall back to SQLite.

## Run Hive queries
After MapReduce succeeds:
```bash
hive -f bigdata/hive/analytics.hql
```
The queries report source rows, top content by stream count, watch hours and buffering totals.

## Evidence for faculty
Capture actual output from:
- `hdfs dfs -ls /media-entertainment/raw/viewing_activity`
- Hadoop job completion status and map/reduce task counters in YARN ResourceManager UI
- `hdfs dfs -ls /media-entertainment/processed/content_metrics`
- Hive query results from `analytics.hql`

Do not claim distributed execution until the job completes and output files exist in HDFS. A single-node pseudo-distributed cluster demonstrates Hadoop execution but is not a multi-node production cluster. For scale testing, generate larger synthetic data and report measured bytes, rows and job metrics.

## Output schema
Tab-separated: `content_id, stream_count, watched_seconds, buffering_events`.
The mapper uses the existing CSV schema: content_id column 4, watch_duration_sec column 9 and buffer_count column 12 (1-based).
