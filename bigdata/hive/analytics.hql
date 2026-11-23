CREATE DATABASE IF NOT EXISTS media_entertainment;
USE media_entertainment;

CREATE EXTERNAL TABLE IF NOT EXISTS raw_viewing_activity (
  viewing_id STRING, session_id STRING, user_id STRING, content_id STRING,
  device_id STRING, plan_id STRING, stream_start_time STRING, stream_end_time STRING,
  watch_duration_sec BIGINT, completion_pct DOUBLE, bitrate_kbps BIGINT,
  buffer_count BIGINT, buffer_duration_sec BIGINT, pause_count BIGINT,
  seek_count BIGINT, error_code STRING, resolution STRING, is_completed STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/media-entertainment/raw/viewing_activity'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS content_stream_metrics (
  content_id STRING, stream_count BIGINT, watched_seconds BIGINT, buffering_events BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/media-entertainment/processed/content_metrics';

-- Faculty demonstration queries
SELECT COUNT(*) AS source_records FROM raw_viewing_activity;
SELECT content_id, stream_count, ROUND(watched_seconds / 3600.0, 2) AS watch_hours,
       buffering_events
FROM content_stream_metrics ORDER BY stream_count DESC LIMIT 20;
SELECT COUNT(*) AS processed_content_groups, SUM(stream_count) AS total_streams,
       SUM(watched_seconds) AS total_watched_seconds,
       SUM(buffering_events) AS total_buffering_events
FROM content_stream_metrics;
