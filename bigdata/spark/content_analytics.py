"""PySpark batch aggregation for OTT viewing activity.

Usage:
  spark-submit content_analytics.py <input_csv> <output_path>
Input columns: content_id, watch_duration_sec, buffer_count
"""
import sys
from pyspark.sql import SparkSession, functions as F

def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: spark-submit content_analytics.py <input_csv> <output_path>")
    spark = SparkSession.builder.appName("MediaEntertainmentContentAnalytics").getOrCreate()
    source = (spark.read.option("header", True).option("mode", "PERMISSIVE")
              .csv(sys.argv[1]))
    clean = (source.select(
        F.trim("content_id").alias("content_id"),
        F.col("watch_duration_sec").cast("long").alias("watch_seconds"),
        F.col("buffer_count").cast("long").alias("buffer_events"))
        .filter(F.col("content_id").isNotNull() & (F.col("content_id") != "")
                & (F.col("watch_seconds") >= 0) & (F.col("buffer_events") >= 0)))
    result = (clean.groupBy("content_id").agg(
        F.count("*").alias("stream_count"),
        F.sum("watch_seconds").alias("watched_seconds"),
        F.sum("buffer_events").alias("buffering_events"))
        .withColumn("watch_hours", F.round(F.col("watched_seconds") / 3600, 2))
        .orderBy(F.desc("stream_count")))
    result.write.mode("overwrite").option("header", True).csv(sys.argv[2])
    spark.stop()

if __name__ == "__main__":
    main()
