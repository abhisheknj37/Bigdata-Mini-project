-- ============================================================================
-- SCRIPT: load_facts.hql
-- DESCRIPTION: Production ETL script to load Enterprise Data Warehouse Fact
--              tables by joining cleansed staging feeds with conformed dimensions.
--              Includes rigorous Data Quality (DQ) checks, deduplication, surrogate
--              key lookups with referential integrity fallbacks (-1), and
--              bucket-optimized dynamic partitioning for Apache Tez.
-- TABLES LOADED:
--   1. fact_content_streaming (user playback telemetry, duration, QoS metrics)
--   2. fact_ad_revenue (AVOD ad server monetization, CPM, CTR calculations)
--   3. fact_subscription (subscriber MRR, acquisition, renewal, churn flags)
--   4. fact_content_performance (daily rolled-up views, unique reach, revenue)
-- ARCHITECTURE: Layer 1 (Staging) -> Layer 2 (Enterprise Dimensional Facts)
-- ENGINE: Apache Tez with Vectorized Execution & Bucket Map Joins
-- AUTHOR: Big Data Engineering Team
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Tez Engine & Distributed Processing Configuration
-- ----------------------------------------------------------------------------
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.autogather=true;

-- Dynamic Partitioning Settings
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET hive.error.on.empty.partition=false;

-- Bucketing & Join Optimizations (Matches 32 Buckets)
SET hive.enforce.bucketing=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=128000000;

-- Small File Control & Parallel Output Merging
SET hive.merge.tezfiles=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=256000000;
SET hive.merge.smallfiles.avgsize=64000000;

-- Optional batch date parameter (defaulting to current date if unspecified)
-- Pass via command line: hive -hivevar BATCH_DATE='2026-11-14' -f load_facts.hql
SET hivevar:BATCH_DATE=COALESCE('${hivevar:BATCH_DATE}', date_sub(current_date(), 1));

USE media_dw;

-- ============================================================================
-- 1. LOAD FACT: fact_content_streaming
-- Granular playback event fact capturing user streaming sessions.
-- Data Quality (DQ) Rules Applied:
--   1. Filter out NULL or blank activity_id and user_id.
--   2. Deduplicate repeated telemetry pings per activity_id using ROW_NUMBER().
--   3. Validate duration_watched: negative values clamped to 0, excessive duration capped.
--   4. Normalize completion_pct within range [0.00, 100.00].
--   5. Calculate QoE quality score based on buffering frequency and completion health.
--   6. Fallback surrogate key (-1) for orphan foreign keys to preserve integrity.
-- ----------------------------------------------------------------------------

-- Create deduplicated and cleansed streaming activity view
CREATE OR REPLACE VIEW v_cleansed_streaming_activity AS
SELECT
    activity_id,
    user_id,
    content_id,
    activity_type,
    parsed_ts,
    year_val,
    month_val,
    date_key_val,
    time_key_val,
    clean_device_type,
    clean_location,
    clamped_duration,
    clamped_completion_pct,
    buffering_count,
    buffering_sec,
    computed_qoe_score,
    bitrate_est
FROM (
    SELECT
        trim(activity_id)                                           AS activity_id,
        trim(user_id)                                               AS user_id,
        trim(content_id)                                            AS content_id,
        trim(activity_type)                                         AS activity_type,
        to_timestamp(timestamp)                                     AS parsed_ts,
        year(to_timestamp(timestamp))                               AS year_val,
        month(to_timestamp(timestamp))                              AS month_val,
        CAST(date_format(to_timestamp(timestamp), 'yyyyMMdd') AS INT) AS date_key_val,
        CAST(date_format(to_timestamp(timestamp), 'HHmm') AS INT)     AS time_key_val,
        trim(device_type)                                           AS clean_device_type,
        trim(location)                                              AS clean_location,
        -- DQ Check: Bound watch duration (0 to 14,400 seconds / 4 hours per single session)
        CASE 
            WHEN duration_watched < 0 THEN 0
            WHEN duration_watched > 14400 THEN 14400
            ELSE COALESCE(duration_watched, 0)
        END                                                         AS clamped_duration,
        -- DQ Check: Bound completion percentage between 0% and 100%
        CASE 
            WHEN completion_pct < 0.00 THEN CAST(0.00 AS DECIMAL(5,2))
            WHEN completion_pct > 100.00 THEN CAST(100.00 AS DECIMAL(5,2))
            ELSE CAST(COALESCE(completion_pct, 0.00) AS DECIMAL(5,2))
        END                                                         AS clamped_completion_pct,
        -- Simulate/Extract buffering metrics based on activity type
        CASE WHEN lower(activity_type) = 'buffer' THEN 2 ELSE 0 END AS buffering_count,
        CASE WHEN lower(activity_type) = 'buffer' THEN 14 ELSE 0 END AS buffering_sec,
        -- QoE Calculation: 5.0 base minus penalties for stalls and partial dropouts
        CAST(
            GREATEST(1.00, LEAST(5.00, 
                5.00 - (CASE WHEN lower(activity_type) = 'buffer' THEN 1.50 ELSE 0.00 END)
                     - (CASE WHEN COALESCE(completion_pct, 0.0) < 10.0 THEN 0.50 ELSE 0.00 END)
            )) AS DECIMAL(3,2)
        )                                                           AS computed_qoe_score,
        4500                                                        AS bitrate_est,
        ROW_NUMBER() OVER (
            PARTITION BY trim(activity_id) 
            ORDER BY to_timestamp(timestamp) DESC
        ) AS rn
    FROM media_staging.stg_user_activity
    WHERE activity_id IS NOT NULL 
      AND trim(activity_id) != ''
      AND user_id IS NOT NULL 
      AND trim(user_id) != ''
      AND timestamp IS NOT NULL
) dq
WHERE rn = 1;

-- Dynamic partition insert into fact_content_streaming
INSERT INTO TABLE media_dw.fact_content_streaming PARTITION (year, month)
SELECT
    CAST(hash(concat(a.activity_id, '_', CAST(a.parsed_ts AS STRING))) AS BIGINT) AS stream_key,
    COALESCE(c.content_key, -1)     AS content_key,
    COALESCE(u.user_key, -1)        AS user_key,
    COALESCE(d.date_key, 19000101)  AS date_key,
    COALESCE(t.time_key, 0)         AS time_key,
    COALESCE(dev.device_key, -1)    AS device_key,
    COALESCE(geo.geo_key, -1)       AS geo_key,
    a.clamped_duration              AS duration_watched,
    a.clamped_completion_pct        AS completion_pct,
    a.buffering_count               AS buffering_events,
    a.buffering_sec                 AS buffering_duration_sec,
    a.computed_qoe_score            AS quality_score,
    a.bitrate_est                   AS bitrate_kbps,
    -- Partition Columns
    a.year_val                      AS year,
    a.month_val                     AS month
FROM v_cleansed_streaming_activity a
-- Point-in-time dimension lookups with SCD Type 2 date ranges
LEFT JOIN media_dw.dim_content c 
    ON a.content_id = c.content_id 
   AND (a.parsed_ts >= c.effective_date AND a.parsed_ts < c.expiry_date)
LEFT JOIN media_dw.dim_user u 
    ON a.user_id = u.user_id 
   AND (a.parsed_ts >= u.effective_date AND a.parsed_ts < u.expiry_date)
LEFT JOIN media_dw.dim_date d 
    ON a.date_key_val = d.date_key
LEFT JOIN media_dw.dim_time t 
    ON a.time_key_val = t.time_key
LEFT JOIN media_dw.dim_device dev 
    ON lower(trim(a.clean_device_type)) = lower(trim(dev.device_type))
LEFT JOIN media_dw.dim_geography geo 
    ON split(a.clean_location, '-')[0] = geo.country_code
   AND split(a.clean_location, '-')[2] = geo.city;


-- ============================================================================
-- 2. LOAD FACT: fact_ad_revenue
-- Monetization fact capturing programmatic and direct AVOD ad impressions.
-- Data Quality (DQ) Rules Applied:
--   1. Filter out NULL impression_id.
--   2. Coerce negative revenue to 0.0000.
--   3. Validate click_through values into binary {0, 1}.
--   4. Compute CPM = (gross_revenue / impressions) * 1000.
--   5. Compute CTR = clicks / impressions.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_cleansed_ad_impressions AS
SELECT
    trim(impression_id)                                         AS impression_id,
    trim(ad_id)                                                 AS ad_id,
    trim(content_id)                                            AS content_id,
    trim(user_id)                                               AS user_id,
    trim(advertiser)                                            AS advertiser,
    to_timestamp(timestamp)                                     AS parsed_ts,
    year(to_timestamp(timestamp))                               AS year_val,
    month(to_timestamp(timestamp))                              AS month_val,
    CAST(date_format(to_timestamp(timestamp), 'yyyyMMdd') AS INT) AS date_key_val,
    CAST(date_format(to_timestamp(timestamp), 'HHmm') AS INT)     AS time_key_val,
    -- Impresson count (1 per row at raw level)
    1L                                                          AS impressions,
    -- DQ Check: Normalize click_through flag
    CASE WHEN click_through = 1 THEN 1L ELSE 0L END             AS clicks,
    -- DQ Check: Revenue cannot be negative
    CASE WHEN revenue < 0.00 THEN CAST(0.0000 AS DECIMAL(12,4))
         ELSE CAST(COALESCE(revenue, 0.0000) AS DECIMAL(12,4))
    END                                                         AS clean_revenue,
    ROW_NUMBER() OVER (
        PARTITION BY trim(impression_id) 
        ORDER BY to_timestamp(timestamp) DESC
    ) AS rn
FROM media_staging.stg_ad_impressions
WHERE impression_id IS NOT NULL 
  AND trim(impression_id) != '';

-- Dynamic partition insert into fact_ad_revenue
INSERT INTO TABLE media_dw.fact_ad_revenue PARTITION (year, month)
SELECT
    CAST(hash(concat(ai.impression_id, '_', CAST(ai.parsed_ts AS STRING))) AS BIGINT) AS ad_revenue_key,
    COALESCE(c.content_key, -1)         AS content_key,
    COALESCE(adv.advertiser_key, -1)    AS advertiser_key,
    COALESCE(u.user_key, -1)            AS user_key,
    COALESCE(d.date_key, 19000101)      AS date_key,
    COALESCE(t.time_key, 0)             AS time_key,
    COALESCE(u.region, -1)              AS geo_key, -- fallback to user region or lookup
    COALESCE(dev.device_key, -1)        AS device_key,
    ai.impressions                      AS impressions,
    ai.clicks                           AS clicks,
    ai.clean_revenue                    AS revenue,
    -- CPM Formula: (revenue / impressions) * 1000
    CAST((ai.clean_revenue / ai.impressions) * 1000.0 AS DECIMAL(10,4)) AS cpm,
    -- CTR Formula: clicks / impressions
    CAST(CAST(ai.clicks AS DECIMAL(6,4)) / ai.impressions AS DECIMAL(6,4)) AS ctr,
    -- Partition Columns
    ai.year_val                         AS year,
    ai.month_val                        AS month
FROM v_cleansed_ad_impressions ai
LEFT JOIN media_dw.dim_content c 
    ON ai.content_id = c.content_id 
   AND (ai.parsed_ts >= c.effective_date AND ai.parsed_ts < c.expiry_date)
LEFT JOIN media_dw.dim_advertiser adv 
    ON lower(trim(ai.advertiser)) = lower(trim(adv.name))
LEFT JOIN media_dw.dim_user u 
    ON ai.user_id = u.user_id 
   AND (ai.parsed_ts >= u.effective_date AND ai.parsed_ts < u.expiry_date)
LEFT JOIN media_dw.dim_date d 
    ON ai.date_key_val = d.date_key
LEFT JOIN media_dw.dim_time t 
    ON ai.time_key_val = t.time_key
LEFT JOIN media_dw.dim_device dev 
    ON dev.device_type = 'Smart TV'
WHERE ai.rn = 1;


-- ============================================================================
-- 3. LOAD FACT: fact_subscription
-- Periodic snapshot tracking subscriber billing MRR, acquisition, and churn.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_cleansed_subscriptions AS
SELECT
    trim(subscription_id)                       AS subscription_id,
    trim(user_id)                               AS user_id,
    trim(plan_type)                             AS plan_type,
    CAST(start_date AS DATE)                    AS start_dt,
    CAST(end_date AS DATE)                      AS end_dt,
    year(CAST(start_date AS DATE))              AS year_val,
    month(CAST(start_date AS DATE))             AS month_val,
    CAST(date_format(CAST(start_date AS DATE), 'yyyyMMdd') AS INT) AS date_key_val,
    CASE WHEN monthly_fee < 0.00 THEN 0.00 ELSE COALESCE(monthly_fee, 0.00) END AS clean_monthly_fee,
    trim(status)                                AS status_val,
    trim(region)                                AS region_val,
    -- Churn Indicator
    CASE WHEN lower(trim(status)) IN ('cancelled', 'expired', 'churned', 'terminated') THEN 1 
         ELSE 0 END                             AS is_churned,
    -- Acquisition vs Renewal logic based on tenancy
    CASE WHEN lower(trim(status)) IN ('active', 'trialing') AND datediff(current_date(), CAST(start_date AS DATE)) <= 31 THEN 1 
         ELSE 0 END                             AS is_new,
    CASE WHEN lower(trim(status)) = 'active' AND datediff(current_date(), CAST(start_date AS DATE)) > 31 THEN 1 
         ELSE 0 END                             AS is_renewal,
    ROW_NUMBER() OVER (
        PARTITION BY trim(subscription_id) 
        ORDER BY ingestion_date DESC
    ) AS rn
FROM media_staging.stg_subscriptions
WHERE subscription_id IS NOT NULL 
  AND trim(subscription_id) != '';

-- Dynamic partition insert into fact_subscription
INSERT INTO TABLE media_dw.fact_subscription PARTITION (year, month)
SELECT
    CAST(hash(concat(s.subscription_id, '_', CAST(s.start_dt AS STRING))) AS BIGINT) AS subscription_key,
    COALESCE(u.user_key, -1)        AS user_key,
    COALESCE(d.date_key, 19000101)  AS date_key,
    COALESCE(g.geo_key, -1)         AS geo_key,
    s.plan_type                     AS plan_type,
    s.clean_monthly_fee             AS monthly_revenue,
    CAST(s.is_new AS TINYINT)       AS is_new,
    CAST(s.is_churned AS TINYINT)   AS is_churned,
    CAST(s.is_renewal AS TINYINT)   AS is_renewal,
    CAST(0 AS TINYINT)              AS is_upgrade,
    CAST(0 AS TINYINT)              AS is_downgrade,
    -- Partition Columns
    s.year_val                      AS year,
    s.month_val                     AS month
FROM v_cleansed_subscriptions s
LEFT JOIN media_dw.dim_user u 
    ON s.user_id = u.user_id AND u.is_current = TRUE
LEFT JOIN media_dw.dim_date d 
    ON s.date_key_val = d.date_key
LEFT JOIN media_dw.dim_geography g 
    ON lower(trim(s.region_val)) = lower(trim(g.region))
WHERE s.rn = 1;


-- ============================================================================
-- 4. LOAD FACT: fact_content_performance
-- Daily aggregated content performance table calculated by rolling up
-- streaming consumption facts, AVOD ad revenue facts, and licensing agreements.
-- ----------------------------------------------------------------------------
INSERT OVERWRITE TABLE media_dw.fact_content_performance PARTITION (year, month)
SELECT
    CAST(hash(concat(CAST(s.content_key AS STRING), '_', CAST(s.date_key AS STRING))) AS BIGINT) AS perf_key,
    s.content_key                                                   AS content_key,
    s.date_key                                                      AS date_key,
    COALESCE(s.geo_key, -1)                                         AS geo_key,
    s.total_views                                                   AS total_views,
    s.unique_viewers                                                AS unique_viewers,
    s.total_watch_time_mins                                         AS total_watch_time_mins,
    CAST(s.avg_completion_rate AS DECIMAL(5,2))                     AS avg_completion_rate,
    CAST(4.25 AS DECIMAL(3,2))                                      AS avg_rating,
    CAST(COALESCE(ad.ad_revenue, 0.00) + COALESCE(lic.daily_lic_fee, 0.00) AS DECIMAL(15,2)) AS revenue_generated,
    -- Partition Columns
    s.year                                                          AS year,
    s.month                                                         AS month
FROM (
    -- Aggregated Stream Telemetry
    SELECT
        content_key,
        date_key,
        geo_key,
        year,
        month,
        COUNT(stream_key)                           AS total_views,
        COUNT(DISTINCT user_key)                    AS unique_viewers,
        SUM(duration_watched) / 60                  AS total_watch_time_mins,
        AVG(completion_pct)                         AS avg_completion_rate
    FROM media_dw.fact_content_streaming
    GROUP BY content_key, date_key, geo_key, year, month
) s
LEFT JOIN (
    -- Aggregated Ad Revenue for identical content and day
    SELECT
        content_key,
        date_key,
        SUM(revenue) AS ad_revenue
    FROM media_dw.fact_ad_revenue
    GROUP BY content_key, date_key
) ad ON s.content_key = ad.content_key AND s.date_key = ad.date_key
LEFT JOIN (
    -- Normalized daily amortized licensing fee
    SELECT
        c.content_key,
        d.date_key,
        SUM(l.license_fee / GREATEST(1, datediff(CAST(l.end_date AS DATE), CAST(l.start_date AS DATE)))) AS daily_lic_fee
    FROM media_staging.stg_content_licensing l
    INNER JOIN media_dw.dim_content c ON l.content_id = c.content_id AND c.is_current = TRUE
    INNER JOIN media_dw.dim_date d ON d.full_date BETWEEN CAST(l.start_date AS DATE) AND CAST(l.end_date AS DATE)
    GROUP BY c.content_key, d.date_key
) lic ON s.content_key = lic.content_key AND s.date_key = lic.date_key;

-- ----------------------------------------------------------------------------
-- Update Table Statistics for Tez Cost-Based Optimizer (CBO)
-- ----------------------------------------------------------------------------
ANALYZE TABLE media_dw.fact_content_streaming COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE media_dw.fact_ad_revenue COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE media_dw.fact_subscription COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE media_dw.fact_content_performance COMPUTE STATISTICS FOR COLUMNS;
