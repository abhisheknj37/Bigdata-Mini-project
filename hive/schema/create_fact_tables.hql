-- ============================================================================
-- SCRIPT: create_fact_tables.hql
-- DESCRIPTION: Creates enterprise fact tables for the Media & Entertainment
--              Data Warehouse in Apache Hive using ORC format, Snappy compression,
--              range/dynamic partitioning by year/month, and bucketing into 32 buckets.
-- ARCHITECTURE: Layer 2 - Enterprise Data Warehouse (EDW) Dimensional Model
-- STORAGE: ORC, SNAPPY Compression, Partitioned by (year, month), Bucketed into 32 buckets
-- AUTHOR: Big Data Engineering Team
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Optimization and Execution Settings
-- ----------------------------------------------------------------------------
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.autogather=true;

-- Hive Bucketing and Partitioning Configuration
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=5000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

USE media_dw;

-- ----------------------------------------------------------------------------
-- 1. Fact Table: fact_content_streaming
-- Granular playback event fact capturing user streaming sessions, duration,
-- playback quality of experience (QoE), and device touchpoints.
-- Partitioned: year, month
-- Bucketed: user_key into 32 buckets (optimizes user-centric join/aggregation)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_content_streaming (
    stream_key              BIGINT          COMMENT 'Surrogate key for the streaming event session',
    content_key             BIGINT          COMMENT 'Foreign key referencing dim_content (SCD Type 2 active key)',
    user_key                BIGINT          COMMENT 'Foreign key referencing dim_user (SCD Type 2 active key)',
    date_key                INT             COMMENT 'Foreign key referencing dim_date (YYYYMMDD)',
    time_key                INT             COMMENT 'Foreign key referencing dim_time (HHMM)',
    device_key              INT             COMMENT 'Foreign key referencing dim_device',
    geo_key                 INT             COMMENT 'Foreign key referencing dim_geography',
    duration_watched        INT             COMMENT 'Actual watch duration in seconds during this stream',
    completion_pct          DECIMAL(5,2)    COMMENT 'Percentage of full content duration watched (0.00 to 100.00)',
    buffering_events        INT             COMMENT 'Total count of video stalling/buffering occurrences',
    buffering_duration_sec  INT             COMMENT 'Total time spent waiting for buffer in seconds',
    quality_score           DECIMAL(3,2)    COMMENT 'Calculated Quality of Experience (QoE) metric: 1.00 (Poor) to 5.00 (Flawless)',
    bitrate_kbps            INT             COMMENT 'Average stream encoding bitrate consumed in kbps'
)
COMMENT 'Granular fact table tracking video and audio streaming consumption and QoS telemetry'
PARTITIONED BY (
    year                    INT             COMMENT 'Streaming event calendar year (e.g., 2026)',
    month                   INT             COMMENT 'Streaming event calendar month (1 to 12)'
)
CLUSTERED BY (user_key)
SORTED BY (stream_key ASC)
INTO 32 BUCKETS
STORED AS ORC
LOCATION '/data/edw/media/fact_content_streaming'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'content_key,user_key,date_key,device_key,geo_key',
    'orc.stripe.size' = '67108864',
    'orc.row.index.stride' = '10000'
);

-- ----------------------------------------------------------------------------
-- 2. Fact Table: fact_ad_revenue
-- Commercial monetization fact capturing AVOD impressions, clicks, gross revenue,
-- and ad performance KPIs (CPM, CTR).
-- Partitioned: year, month
-- Bucketed: content_key into 32 buckets (optimizes content monetization queries)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_ad_revenue (
    ad_revenue_key          BIGINT          COMMENT 'Surrogate primary key for ad monetization transaction',
    content_key             BIGINT          COMMENT 'Foreign key referencing dim_content',
    advertiser_key          INT             COMMENT 'Foreign key referencing dim_advertiser',
    user_key                BIGINT          COMMENT 'Foreign key referencing dim_user (viewer exposed to ad)',
    date_key                INT             COMMENT 'Foreign key referencing dim_date (YYYYMMDD)',
    time_key                INT             COMMENT 'Foreign key referencing dim_time (HHMM)',
    geo_key                 INT             COMMENT 'Foreign key referencing dim_geography',
    device_key              INT             COMMENT 'Foreign key referencing dim_device',
    impressions             BIGINT          COMMENT 'Number of ad impressions delivered (typically 1 at granular level)',
    clicks                  BIGINT          COMMENT 'Number of user click-through interactions recorded',
    revenue                 DECIMAL(12,4)   COMMENT 'Net advertising revenue earned in USD',
    cpm                     DECIMAL(10,4)   COMMENT 'Effective Cost Per Mille: (revenue / impressions) * 1000',
    ctr                     DECIMAL(6,4)    COMMENT 'Click-Through Rate: clicks / impressions'
)
COMMENT 'Commercial monetization fact capturing ad impressions, click events, and AVOD revenue'
PARTITIONED BY (
    year                    INT             COMMENT 'Impression calendar year (e.g., 2026)',
    month                   INT             COMMENT 'Impression calendar month (1 to 12)'
)
CLUSTERED BY (content_key)
SORTED BY (ad_revenue_key ASC)
INTO 32 BUCKETS
STORED AS ORC
LOCATION '/data/edw/media/fact_ad_revenue'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'content_key,advertiser_key,user_key,date_key,geo_key',
    'orc.stripe.size' = '67108864',
    'orc.row.index.stride' = '10000'
);

-- ----------------------------------------------------------------------------
-- 3. Fact Table: fact_subscription
-- Periodic subscriber lifecycle fact tracking billing recurring revenue,
-- subscriber acquisitions, renewals, upgrades, downgrades, and churn events.
-- Partitioned: year, month
-- Bucketed: user_key into 32 buckets (optimizes subscriber cohort analysis)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_subscription (
    subscription_key        BIGINT          COMMENT 'Surrogate key for the subscription billing period',
    user_key                BIGINT          COMMENT 'Foreign key referencing dim_user',
    date_key                INT             COMMENT 'Foreign key referencing dim_date (YYYYMMDD billing cycle date)',
    geo_key                 INT             COMMENT 'Foreign key referencing dim_geography',
    plan_type               STRING          COMMENT 'Subscription plan tier: Ad_Supported, Basic_SD, Standard_HD, Premium_4K',
    monthly_revenue         DECIMAL(10,2)   COMMENT 'Recognized monthly recurring subscription revenue (MRR) in USD',
    is_new                  TINYINT         COMMENT '1 if newly acquired customer in this cycle, otherwise 0',
    is_churned              TINYINT         COMMENT '1 if subscription was voluntarily or involuntarily terminated, otherwise 0',
    is_renewal              TINYINT         COMMENT '1 if existing active subscriber renewed cycle, otherwise 0',
    is_upgrade              TINYINT         COMMENT '1 if customer migrated to a higher ARPU tier, otherwise 0',
    is_downgrade            TINYINT         COMMENT '1 if customer migrated to a lower ARPU tier, otherwise 0'
)
COMMENT 'Periodic snapshot fact table tracking subscriber lifecycle transitions and recurring revenue'
PARTITIONED BY (
    year                    INT             COMMENT 'Billing calendar year (e.g., 2026)',
    month                   INT             COMMENT 'Billing calendar month (1 to 12)'
)
CLUSTERED BY (user_key)
SORTED BY (subscription_key ASC)
INTO 32 BUCKETS
STORED AS ORC
LOCATION '/data/edw/media/fact_subscription'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'user_key,date_key,geo_key,plan_type',
    'orc.stripe.size' = '67108864',
    'orc.row.index.stride' = '10000'
);

-- ----------------------------------------------------------------------------
-- 4. Fact Table: fact_content_performance
-- Daily aggregated content fact recording viewership reach, engagement depth,
-- completion health, audience sentiment rating, and total monetization.
-- Partitioned: year, month
-- Bucketed: content_key into 32 buckets (optimizes content catalog reporting)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_content_performance (
    perf_key                BIGINT          COMMENT 'Surrogate primary key for content daily performance record',
    content_key             BIGINT          COMMENT 'Foreign key referencing dim_content',
    date_key                INT             COMMENT 'Foreign key referencing dim_date (aggregation date YYYYMMDD)',
    geo_key                 INT             COMMENT 'Foreign key referencing dim_geography (regional performance slice)',
    total_views             BIGINT          COMMENT 'Total playback session count initiated on this day',
    unique_viewers          BIGINT          COMMENT 'Distinct count of individual users who streamed the asset',
    total_watch_time_mins   BIGINT          COMMENT 'Cumulative minutes streamed across all viewers',
    avg_completion_rate     DECIMAL(5,2)    COMMENT 'Average percentage of content watched (0.00 to 100.00%)',
    avg_rating              DECIMAL(3,2)    COMMENT 'Audience feedback star rating (1.00 to 5.00)',
    revenue_generated       DECIMAL(15,2)   COMMENT 'Cumulative daily revenue (ad revenue + licensing share in USD)'
)
COMMENT 'Aggregated daily fact table tracking content viewership volume, engagement, and revenue'
PARTITIONED BY (
    year                    INT             COMMENT 'Reporting performance calendar year (e.g., 2026)',
    month                   INT             COMMENT 'Reporting performance calendar month (1 to 12)'
)
CLUSTERED BY (content_key)
SORTED BY (perf_key ASC)
INTO 32 BUCKETS
STORED AS ORC
LOCATION '/data/edw/media/fact_content_performance'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'content_key,date_key,geo_key',
    'orc.stripe.size' = '67108864',
    'orc.row.index.stride' = '10000'
);
