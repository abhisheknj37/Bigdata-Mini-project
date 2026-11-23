#!/usr/bin/env bash
# Execute on a Linux/WSL2 host with HDFS, YARN and Hadoop Streaming configured.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
INPUT_LOCAL="${1:-$ROOT/data/sample/staging_viewing_activity.csv}"
HDFS_INPUT="/media-entertainment/raw/viewing_activity"
HDFS_OUTPUT="/media-entertainment/processed/content_metrics"
STREAMING_JAR="${HADOOP_STREAMING_JAR:-}"

if [[ -z "$STREAMING_JAR" || ! -f "$STREAMING_JAR" ]]; then
  echo "Set HADOOP_STREAMING_JAR to the installed hadoop-streaming*.jar" >&2; exit 2
fi
if [[ ! -f "$INPUT_LOCAL" ]]; then echo "Input CSV not found: $INPUT_LOCAL" >&2; exit 2; fi
command -v hdfs >/dev/null
command -v hadoop >/dev/null

echo "[1/4] Preparing HDFS paths"
hdfs dfs -mkdir -p "$HDFS_INPUT" /media-entertainment/processed
hdfs dfs -rm -r -f "$HDFS_OUTPUT" >/dev/null 2>&1 || true
echo "[2/4] Uploading source CSV to HDFS"
hdfs dfs -put -f "$INPUT_LOCAL" "$HDFS_INPUT/viewing_activity.csv"
hdfs dfs -ls "$HDFS_INPUT"
echo "[3/4] Running distributed Hadoop Streaming MapReduce"
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="Media entertainment content metrics" \
  -D mapreduce.job.reduces=2 \
  -files "$ROOT/bigdata/mapreduce/mapper.py,$ROOT/bigdata/mapreduce/reducer.py" \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input "$HDFS_INPUT/viewing_activity.csv" -output "$HDFS_OUTPUT"
echo "[4/4] Verifying HDFS result files"
hdfs dfs -ls "$HDFS_OUTPUT"
hdfs dfs -cat "$HDFS_OUTPUT/part-*" | head -20
echo "SUCCESS: MapReduce results stored at $HDFS_OUTPUT"
