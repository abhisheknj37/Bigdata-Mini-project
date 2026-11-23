CREATE DATABASE IF NOT EXISTS media_entertainment;
USE media_entertainment;

-- Conformed dimensions (warehouse target; surrogate keys are populated by ETL).
CREATE TABLE IF NOT EXISTS dim_content (
  content_key BIGINT, content_id STRING, title STRING,
  content_type STRING, genre STRING, release_year INT
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS dim_user (
  user_key BIGINT, user_id STRING, country STRING,
  age_group STRING, subscription_tier STRING
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS dim_device (
  device_key BIGINT, device_id STRING, device_type STRING,
  operating_system STRING
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS dim_date (
  date_key INT, calendar_date DATE, day_name STRING,
  month_num INT, month_name STRING, quarter_num INT, year_num INT
) STORED AS ORC;

-- One fact row per viewing session. Partition by event year/month for pruning.
CREATE TABLE IF NOT EXISTS fact_viewing_activity (
  viewing_id STRING, session_id STRING, user_key BIGINT,
  content_key BIGINT, device_key BIGINT, date_key INT,
  watch_duration_sec BIGINT, completion_pct DOUBLE,
  bitrate_kbps BIGINT, buffer_count BIGINT, buffer_duration_sec BIGINT,
  pause_count BIGINT, seek_count BIGINT, error_code STRING
)
PARTITIONED BY (event_year INT, event_month INT)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS fact_subscription_billing (
  billing_id STRING, user_key BIGINT, plan_id STRING,
  amount_billed DOUBLE, currency STRING, payment_status STRING
)
PARTITIONED BY (billing_year INT, billing_month INT)
STORED AS ORC;
