-- ============================================================================
-- SCRIPT: create_staging_tables.hql
-- DESCRIPTION: Creates the staging database and external tables for raw data
--              ingestion into the Media & Entertainment Data Warehouse.
-- ARCHITECTURE: Layer 1 - Staging / Raw Ingestion Zone (HDFS Landing)
-- FORMAT: TEXTFILE, Comma-delimited (CSV), Partitioned by ingestion_date
-- AUTHOR: Big Data Engineering Team
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Optimization and Environment Configuration
-- ----------------------------------------------------------------------------
SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- ----------------------------------------------------------------------------
-- 1. Create Staging Database
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS media_staging
COMMENT 'Staging layer for Media and Entertainment raw data ingestion'
LOCATION '/data/staging/media/warehouse'
WITH DBPROPERTIES (
    'creator' = 'DataEngineering',
    'created_date' = '2026-11-14',
    'retention_policy' = '7_days'
);

USE media_staging;

-- ----------------------------------------------------------------------------
-- 2. Staging Table: Content Catalog
-- Raw catalog ingestion of movies, TV shows, music tracks, and original productions.
-- Source: CMS (Content Management System), Studio MAM (Media Asset Management)
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS stg_content_catalog (
    content_id          STRING          COMMENT 'Unique alphanumeric identifier for the content item (e.g., MOV10001, SER20042)',
    title               STRING          COMMENT 'Title of the movie, TV series, episode, or music track',
    genre               STRING          COMMENT 'Primary genre (e.g., Action, Drama, Sci-Fi, Comedy, Thriller, Pop, Rock)',
    type                STRING          COMMENT 'Content classification: Movie, TV Series, Episode, Music Track, Podcast',
    release_date        STRING          COMMENT 'Original global release or theatrical premiere date (YYYY-MM-DD)',
    duration_mins       INT             COMMENT 'Total runtime duration in minutes',
    language            STRING          COMMENT 'Original spoken/audio language (ISO 639-1 code or full name)',
    rating              STRING          COMMENT 'Content maturity rating (e.g., G, PG, PG-13, R, NC-17, TV-MA, TV-14)',
    production_cost     DECIMAL(15,2)   COMMENT 'Total production or acquisition budget in USD',
    studio              STRING          COMMENT 'Originating production studio, distributor, or record label'
)
COMMENT 'Raw ingestion table for media content catalog metadata'
PARTITIONED BY (
    ingestion_date      STRING          COMMENT 'Ingestion date partition formatted as YYYY-MM-DD'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '/data/staging/media/content_catalog'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = '',
    'compression.type' = 'gzip',
    'data.source' = 'CMS_METADATA_API'
);

-- ----------------------------------------------------------------------------
-- 3. Staging Table: User Activity
-- Raw streaming telemetry, playback events, heartbeats, and viewer interactions.
-- Source: Streaming Client Telemetry SDKs (Smart TV, Web, Mobile, Consoles)
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS stg_user_activity (
    activity_id         STRING          COMMENT 'Unique UUID for the client activity or playback session event',
    user_id             STRING          COMMENT 'User profile identifier initiating the session',
    content_id          STRING          COMMENT 'Identifier of content being streamed or interacted with',
    activity_type       STRING          COMMENT 'Event type: play, pause, seek, buffer, complete, rate, stop',
    timestamp           STRING          COMMENT 'ISO-8601 UTC timestamp of client event (YYYY-MM-DD HH:mm:ss)',
    device_type         STRING          COMMENT 'Hardware device category: SmartTV, Mobile, Desktop, Tablet, GameConsole',
    location            STRING          COMMENT 'IP-derived geolocation code (e.g., US-CA-Los Angeles, GB-ENG-London)',
    duration_watched    INT             COMMENT 'Playback duration recorded during session in seconds',
    completion_pct      DECIMAL(5,2)    COMMENT 'Percentage of total runtime completed (0.00 to 100.00)'
)
COMMENT 'Raw ingestion table for user streaming telemetry and interactions'
PARTITIONED BY (
    ingestion_date      STRING          COMMENT 'Ingestion date partition formatted as YYYY-MM-DD'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '/data/staging/media/user_activity'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = '',
    'compression.type' = 'gzip',
    'data.source' = 'TELEMETRY_EVENT_STREAM'
);

-- ----------------------------------------------------------------------------
-- 4. Staging Table: Subscriptions
-- Raw subscriber transactions, billing lifecycle changes, and plan management.
-- Source: Billing Gateway (Stripe, Recurly, Apple App Store, Google Play)
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS stg_subscriptions (
    subscription_id     STRING          COMMENT 'Unique billing contract or subscription agreement identifier',
    user_id             STRING          COMMENT 'Account holder user identifier',
    plan_type           STRING          COMMENT 'Subscription tier: Basic_SD, Standard_HD, Premium_4K, Ad_Supported',
    start_date          STRING          COMMENT 'Effective cycle start date (YYYY-MM-DD)',
    end_date            STRING          COMMENT 'Cycle expiration date or cancellation end date (YYYY-MM-DD)',
    monthly_fee         DECIMAL(10,2)   COMMENT 'Recurring monthly billed amount in USD',
    payment_method      STRING          COMMENT 'Payment vehicle: Credit_Card, PayPal, Apple_Pay, Direct_Debit, Carrier_Billing',
    status              STRING          COMMENT 'Lifecycle state: Active, Trialing, Cancelled, Paused, Past_Due',
    region              STRING          COMMENT 'Geographical billing jurisdiction or territory code'
)
COMMENT 'Raw ingestion table for subscriber billing transactions and subscription state'
PARTITIONED BY (
    ingestion_date      STRING          COMMENT 'Ingestion date partition formatted as YYYY-MM-DD'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '/data/staging/media/subscriptions'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = '',
    'compression.type' = 'gzip',
    'data.source' = 'BILLING_SYSTEM_EXTRACT'
);

-- ----------------------------------------------------------------------------
-- 5. Staging Table: Ad Impressions
-- Raw digital ad server telemetry (AVOD ad pods, pre-roll, mid-roll, post-roll).
-- Source: Ad Decision Server (Google Ad Manager, FreeWheel, SpringServe)
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS stg_ad_impressions (
    impression_id       STRING          COMMENT 'Unique impression transaction identifier generated by ad server',
    ad_id               STRING          COMMENT 'Creative asset identifier registered in ad campaign',
    content_id          STRING          COMMENT 'Host content stream in which ad pod was inserted',
    user_id             STRING          COMMENT 'Target viewer identifier exposed to impression',
    timestamp           STRING          COMMENT 'Impression render timestamp (YYYY-MM-DD HH:mm:ss)',
    ad_type             STRING          COMMENT 'Ad pod format: Pre_Roll, Mid_Roll, Post_Roll, Banner_Overlay, Sponsored_Card',
    advertiser          STRING          COMMENT 'Advertiser brand or agency corporate name',
    revenue             DECIMAL(10,4)   COMMENT 'Direct gross programmatic/direct revenue earned in USD',
    click_through       INT             COMMENT 'Binary indicator: 1 = click occurred, 0 = impression view only'
)
COMMENT 'Raw ingestion table for ad impressions and monetization events'
PARTITIONED BY (
    ingestion_date      STRING          COMMENT 'Ingestion date partition formatted as YYYY-MM-DD'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '/data/staging/media/ad_impressions'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = '',
    'compression.type' = 'gzip',
    'data.source' = 'AD_DECISION_ENGINE'
);

-- ----------------------------------------------------------------------------
-- 6. Staging Table: Content Licensing
-- Raw B2B licensing deals, distribution rights syndication, and territory contracts.
-- Source: Rights Management ERP / Rightsline / SAP S/4HANA Media
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS stg_content_licensing (
    license_id          STRING          COMMENT 'Legal contract agreement identifier for distribution license',
    content_id          STRING          COMMENT 'Content catalog asset ID covered by rights agreement',
    licensee            STRING          COMMENT 'Third-party distributor or partner platform (e.g., Netflix, Amazon, Sky, HBO)',
    territory           STRING          COMMENT 'Licensed geographical region (e.g., North_America, Western_Europe, APAC)',
    start_date          STRING          COMMENT 'Contractual window opening date (YYYY-MM-DD)',
    end_date            STRING          COMMENT 'Contractual window closing date (YYYY-MM-DD)',
    license_fee         DECIMAL(15,2)   COMMENT 'Gross licensing fee contract value in USD',
    license_type        STRING          COMMENT 'Distribution rights classification: SVOD, AVOD, TVOD, Broadcast_Linear, Inflight'
)
COMMENT 'Raw ingestion table for enterprise B2B content licensing and rights agreements'
PARTITIONED BY (
    ingestion_date      STRING          COMMENT 'Ingestion date partition formatted as YYYY-MM-DD'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '/data/staging/media/content_licensing'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = '',
    'compression.type' = 'gzip',
    'data.source' = 'RIGHTS_MANAGEMENT_SYSTEM'
);

-- ----------------------------------------------------------------------------
-- Partition Discovery Utility:
-- To register newly arrived partition folders from HDFS, run:
--   MSCK REPAIR TABLE media_staging.stg_content_catalog;
--   MSCK REPAIR TABLE media_staging.stg_user_activity;
--   MSCK REPAIR TABLE media_staging.stg_subscriptions;
--   MSCK REPAIR TABLE media_staging.stg_ad_impressions;
--   MSCK REPAIR TABLE media_staging.stg_content_licensing;
-- Or explicitly attach partitions via Sqoop/Oozie workflow scripts:
--   ALTER TABLE media_staging.stg_content_catalog ADD IF NOT EXISTS
--   PARTITION (ingestion_date = '2026-11-14')
--   LOCATION '/data/staging/media/content_catalog/ingestion_date=2026-11-14';
-- ----------------------------------------------------------------------------
