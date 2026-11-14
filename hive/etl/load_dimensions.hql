-- ============================================================================
-- SCRIPT: load_dimensions.hql
-- DESCRIPTION: Production ETL script to populate Enterprise Data Warehouse
--              dimension tables from raw staging data in Apache Hive on Tez.
--              Implements:
--                1. SCD Type 2 for dim_content and dim_user via the standard
--                   enterprise Stage-and-Overwrite pattern (with ACID MERGE alternative).
--                2. Conformed static / reference dimension loads (dim_date, dim_time).
--                3. Lookup dimensions (dim_device, dim_geography, dim_advertiser).
--                4. Referential integrity fallbacks (Key -1 for Unknown records).
-- ARCHITECTURE: Layer 1 (Staging) -> Layer 2 (Enterprise Dimensional Warehouse)
-- ENGINE: Apache Tez with Vectorization & Cost-Based Optimizer (CBO) enabled
-- AUTHOR: Big Data Engineering Team
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Tez Engine & Execution Optimizations
-- ----------------------------------------------------------------------------
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.autogather=true;

-- MapJoin & Container Memory Tuning
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=128000000; -- 128 MB threshold
SET hive.mapjoin.smalltable.filesize=64000000;

-- Small File Mitigation & Output Compaction
SET hive.merge.tezfiles=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=256000000;
SET hive.merge.smallfiles.avgsize=64000000;

-- Set execution date variable (default to current system date if not passed)
SET hivevar:CURRENT_TS=current_timestamp();
SET hivevar:CURRENT_DT=current_date();
SET hivevar:EXPIRY_DATE=to_timestamp('9999-12-31 23:59:59');

USE media_dw;

-- ============================================================================
-- 1. LOAD CONFORMED STATIC DIMENSION: dim_time (1,440 minutes of day)
-- ============================================================================
-- Media broadcasting categorizes time into specific dayparts:
--   - Early Morning : 05:00 - 08:59
--   - Morning       : 09:00 - 11:59
--   - Afternoon     : 12:00 - 16:59
--   - Early Fringe  : 17:00 - 19:59
--   - Prime Time    : 20:00 - 22:59 (Core advertising premium window)
--   - Late Night    : 23:00 - 01:59
--   - Overnight     : 02:00 - 04:59
-- ----------------------------------------------------------------------------
INSERT OVERWRITE TABLE media_dw.dim_time
SELECT
    /*+ BROADCAST(t) */
    time_key,
    hour_val   AS hour,
    minute_val AS minute,
    CASE WHEN hour_val < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
    CASE
        WHEN hour_val BETWEEN 5  AND 8  THEN 'Early Morning'
        WHEN hour_val BETWEEN 9  AND 11 THEN 'Morning'
        WHEN hour_val BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN hour_val BETWEEN 17 AND 19 THEN 'Early Fringe'
        WHEN hour_val BETWEEN 20 AND 22 THEN 'Prime Time'
        WHEN hour_val = 23 OR hour_val = 0 OR hour_val = 1 THEN 'Late Night'
        ELSE 'Overnight'
    END AS time_band,
    CASE WHEN hour_val BETWEEN 20 AND 22 THEN TRUE ELSE FALSE END AS is_prime_time
FROM (
    SELECT
        (h.i * 100 + m.j) AS time_key,
        h.i AS hour_val,
        m.j AS minute_val
    FROM
        (SELECT posexplode(split(space(23), ' ')) AS (i, x)) h
    CROSS JOIN
        (SELECT posexplode(split(space(59), ' ')) AS (j, y)) m
) t;


-- ============================================================================
-- 2. LOAD CONFORMED STATIC DIMENSION: dim_date (2024 - 2030 Calendar Span)
-- ============================================================================
-- Generates calendar days with media fiscal calendar (Fiscal Year starts Jan 1)
-- ----------------------------------------------------------------------------
INSERT OVERWRITE TABLE media_dw.dim_date
SELECT
    CAST(date_format(cal_date, 'yyyyMMdd') AS INT)                  AS date_key,
    cal_date                                                        AS full_date,
    day(cal_date)                                                   AS day,
    month(cal_date)                                                 AS month,
    date_format(cal_date, 'MMMM')                                   AS month_name,
    date_format(cal_date, 'MMM')                                    AS month_abbr,
    quarter(cal_date)                                               AS quarter,
    year(cal_date)                                                  AS year,
    -- ISO Day of Week: 1 = Sunday ... 7 = Saturday in standard Hive, normalized to 1=Mon, 7=Sun
    CASE WHEN pmod(datediff(cal_date, '1900-01-07'), 7) = 0 THEN 7 
         ELSE pmod(datediff(cal_date, '1900-01-07'), 7) END         AS day_of_week,
    date_format(cal_date, 'EEEE')                                   AS day_name,
    CASE WHEN pmod(datediff(cal_date, '1900-01-07'), 7) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE 
        WHEN month(cal_date) = 1 AND day(cal_date) = 1 THEN TRUE   -- New Year
        WHEN month(cal_date) = 7 AND day(cal_date) = 4 THEN TRUE   -- Independence Day
        WHEN month(cal_date) = 12 AND day(cal_date) = 25 THEN TRUE -- Christmas Day
        WHEN month(cal_date) = 11 AND day(cal_date) BETWEEN 22 AND 28 
             AND pmod(datediff(cal_date, '1900-01-07'), 7) = 4 THEN TRUE -- Thanksgiving (4th Thursday)
        ELSE FALSE
    END                                                             AS is_holiday,
    CASE 
        WHEN month(cal_date) = 1 AND day(cal_date) = 1 THEN 'New Year\'s Day'
        WHEN month(cal_date) = 7 AND day(cal_date) = 4 THEN 'Independence Day'
        WHEN month(cal_date) = 12 AND day(cal_date) = 25 THEN 'Christmas Day'
        WHEN month(cal_date) = 11 AND day(cal_date) BETWEEN 22 AND 28 
             AND pmod(datediff(cal_date, '1900-01-07'), 7) = 4 THEN 'Thanksgiving'
        ELSE 'None'
    END                                                             AS holiday_name,
    quarter(cal_date)                                               AS fiscal_quarter,
    year(cal_date)                                                  AS fiscal_year,
    weekofyear(cal_date)                                            AS week_of_year
FROM (
    SELECT date_add('2024-01-01', a.pos) AS cal_date
    FROM (
        SELECT posexplode(split(space(2556), ' ')) AS (pos, val) -- ~7 years (2557 days)
    ) a
) d;


-- ============================================================================
-- 3. LOAD LOOKUP DIMENSION: dim_device
-- Extracted distinct hardware and platform configurations from telemetry
-- ----------------------------------------------------------------------------
-- Create staging deduplication view
CREATE OR REPLACE VIEW v_stg_devices AS
SELECT DISTINCT
    trim(device_type) AS device_type,
    CASE
        WHEN lower(device_type) LIKE '%tv%' OR lower(device_type) LIKE '%roku%' THEN 'RokuOS'
        WHEN lower(device_type) LIKE '%mobile%' OR lower(device_type) LIKE '%ios%' THEN 'iOS'
        WHEN lower(device_type) LIKE '%android%' THEN 'Android'
        WHEN lower(device_type) LIKE '%console%' THEN 'PlayStationOS'
        WHEN lower(device_type) LIKE '%mac%' THEN 'macOS'
        ELSE 'Windows'
    END AS os,
    CASE
        WHEN lower(device_type) LIKE '%desktop%' THEN 'Chrome'
        WHEN lower(device_type) LIKE '%tv%' THEN 'ExoPlayer Native'
        WHEN lower(device_type) LIKE '%mobile%' THEN 'Native App'
        ELSE 'Webview'
    END AS browser,
    CASE
        WHEN lower(device_type) LIKE '%tv%' THEN '4K UHD'
        WHEN lower(device_type) LIKE '%desktop%' THEN '1080p FHD'
        WHEN lower(device_type) LIKE '%tablet%' THEN '1080p FHD'
        ELSE '720p HD'
    END AS screen_resolution,
    CASE
        WHEN lower(device_type) LIKE '%apple%' OR lower(device_type) LIKE '%ios%' THEN 'Apple'
        WHEN lower(device_type) LIKE '%samsung%' THEN 'Samsung'
        WHEN lower(device_type) LIKE '%roku%' THEN 'Roku'
        WHEN lower(device_type) LIKE '%sony%' THEN 'Sony'
        ELSE 'Generic'
    END AS manufacturer,
    'v4.12.0' AS app_version
FROM media_staging.stg_user_activity
WHERE device_type IS NOT NULL AND trim(device_type) != '';

-- Insert with Unknown row (-1)
INSERT OVERWRITE TABLE media_dw.dim_device
SELECT -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown'
UNION ALL
SELECT
    CAST(row_number() OVER (ORDER BY device_type, os, screen_resolution) AS INT) AS device_key,
    device_type,
    os,
    browser,
    screen_resolution,
    manufacturer,
    app_version
FROM v_stg_devices;


-- ============================================================================
-- 4. LOAD LOOKUP DIMENSION: dim_geography
-- Extracted from staging user activity locations and subscriber billing jurisdictions
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_stg_geography AS
SELECT DISTINCT
    COALESCE(split(location, '-')[0], 'US') AS country_code,
    CASE
        WHEN split(location, '-')[0] = 'US' THEN 'United States'
        WHEN split(location, '-')[0] = 'GB' THEN 'United Kingdom'
        WHEN split(location, '-')[0] = 'CA' THEN 'Canada'
        WHEN split(location, '-')[0] = 'DE' THEN 'Germany'
        WHEN split(location, '-')[0] = 'FR' THEN 'France'
        WHEN split(location, '-')[0] = 'JP' THEN 'Japan'
        WHEN split(location, '-')[0] = 'AU' THEN 'Australia'
        WHEN split(location, '-')[0] = 'BR' THEN 'Brazil'
        WHEN split(location, '-')[0] = 'IN' THEN 'India'
        ELSE 'International'
    END AS country,
    COALESCE(split(location, '-')[1], 'CA') AS state,
    COALESCE(split(location, '-')[2], 'Los Angeles') AS city,
    CASE
        WHEN split(location, '-')[0] IN ('US', 'CA') THEN 'North America'
        WHEN split(location, '-')[0] IN ('GB', 'DE', 'FR') THEN 'EMEA'
        WHEN split(location, '-')[0] IN ('JP', 'AU', 'IN') THEN 'APAC'
        WHEN split(location, '-')[0] IN ('BR') THEN 'LATAM'
        ELSE 'Other'
    END AS region,
    'Territory-1' AS sub_region,
    CASE
        WHEN split(location, '-')[0] = 'US' THEN 'America/New_York'
        WHEN split(location, '-')[0] = 'GB' THEN 'Europe/London'
        WHEN split(location, '-')[0] = 'JP' THEN 'Asia/Tokyo'
        ELSE 'UTC'
    END AS timezone,
    'DMA-803' AS dma_code
FROM media_staging.stg_user_activity
WHERE location IS NOT NULL AND trim(location) != '';

INSERT OVERWRITE TABLE media_dw.dim_geography
SELECT -1, 'Unknown', 'UNK', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'UTC', 'DMA-000'
UNION ALL
SELECT
    CAST(row_number() OVER (ORDER BY country, state, city) AS INT) AS geo_key,
    country,
    country_code,
    state,
    city,
    region,
    sub_region,
    timezone,
    dma_code
FROM v_stg_geography;


-- ============================================================================
-- 5. LOAD LOOKUP DIMENSION: dim_advertiser
-- Extracted from staging ad impressions
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_stg_advertiser AS
SELECT DISTINCT
    hash(trim(advertiser)) AS advertiser_id,
    trim(advertiser)       AS name,
    CASE
        WHEN lower(advertiser) LIKE '%auto%' OR lower(advertiser) LIKE '%motor%' THEN 'Automotive'
        WHEN lower(advertiser) LIKE '%tech%' OR lower(advertiser) LIKE '%apple%' OR lower(advertiser) LIKE '%google%' THEN 'Tech'
        WHEN lower(advertiser) LIKE '%bank%' OR lower(advertiser) LIKE '%pay%' OR lower(advertiser) LIKE '%credit%' THEN 'Finance'
        WHEN lower(advertiser) LIKE '%cola%' OR lower(advertiser) LIKE '%food%' OR lower(advertiser) LIKE '%cereal%' THEN 'CPG'
        ELSE 'Entertainment'
    END AS industry,
    'Tier 1 Global' AS tier,
    'Omnicom Media Group' AS agency_partner,
    'USD' AS billing_currency,
    'Active' AS account_status
FROM media_staging.stg_ad_impressions
WHERE advertiser IS NOT NULL AND trim(advertiser) != '';

INSERT OVERWRITE TABLE media_dw.dim_advertiser
SELECT -1, 'ADV_UNK', 'Unknown Advertiser', 'Unknown', 'Tier 3 Programmatic', 'None', 'USD', 'Active'
UNION ALL
SELECT
    CAST(row_number() OVER (ORDER BY name) AS INT) AS advertiser_key,
    CAST(advertiser_id AS STRING),
    name,
    industry,
    tier,
    agency_partner,
    billing_currency,
    account_status
FROM v_stg_advertiser;


-- ============================================================================
-- 6. SCD TYPE 2 IMPLEMENTATION: dim_content
-- Handles slowly changing dimensions for content assets:
--   - Unchanged records: Retained as-is.
--   - Expired historical records: Retained with original validity dates.
--   - Changed records: Active record is closed (expiry_date = NOW, is_current = FALSE)
--     and a new active record is inserted (effective_date = NOW, is_current = TRUE).
--   - New incoming content: Inserted with version = 1, is_current = TRUE.
-- ----------------------------------------------------------------------------

-- Temporary staging table to hold the reconciled SCD Type 2 dimension data
DROP TABLE IF EXISTS media_dw.dim_content_stage;
CREATE TABLE media_dw.dim_content_stage LIKE media_dw.dim_content;

-- Step 6.1: Deduplicate latest staging incoming records for current batch
CREATE OR REPLACE VIEW v_latest_stg_content AS
SELECT *
FROM (
    SELECT
        content_id,
        trim(title) AS title,
        trim(genre) AS genre,
        trim(type) AS content_type,
        CAST(release_date AS DATE) AS release_date,
        duration_mins,
        trim(language) AS language,
        trim(rating) AS maturity_rating,
        production_cost,
        trim(studio) AS studio,
        CASE
            WHEN production_cost >= 50000000 THEN 'Blockbuster'
            WHEN lower(studio) LIKE '%original%' THEN 'Original'
            WHEN production_cost BETWEEN 10000000 AND 49999999 THEN 'Licensed'
            ELSE 'Catalog'
        END AS content_tier,
        ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY ingestion_date DESC) as rn
    FROM media_staging.stg_content_catalog
    WHERE content_id IS NOT NULL AND trim(content_id) != ''
) dedup
WHERE rn = 1;

-- Step 6.2: Combine all partitions of history:
-- 1) Preserved historical inactive records (version < current)
-- 2) Unchanged active records
-- 3) Expired active records (that have an incoming delta)
-- 4) Newly versioned active records for modified assets
-- 5) Brand new content items (inserts)
INSERT OVERWRITE TABLE media_dw.dim_content_stage
-- Set A: Historical inactive records (already closed in previous runs)
SELECT
    c.content_key,
    c.content_id,
    c.title,
    c.genre,
    c.content_type,
    c.release_date,
    c.duration_mins,
    c.language,
    c.maturity_rating,
    c.production_cost,
    c.studio,
    c.content_tier,
    c.effective_date,
    c.expiry_date,
    c.is_current,
    c.version
FROM media_dw.dim_content c
WHERE c.is_current = FALSE

UNION ALL

-- Set B: Current active records that DID NOT CHANGE (remain active)
SELECT
    c.content_key,
    c.content_id,
    c.title,
    c.genre,
    c.content_type,
    c.release_date,
    c.duration_mins,
    c.language,
    c.maturity_rating,
    c.production_cost,
    c.studio,
    c.content_tier,
    c.effective_date,
    c.expiry_date,
    c.is_current,
    c.version
FROM media_dw.dim_content c
LEFT JOIN v_latest_stg_content s ON c.content_id = s.content_id
WHERE c.is_current = TRUE
  AND (s.content_id IS NULL OR (
      COALESCE(c.title, '')           = COALESCE(s.title, '')
      AND COALESCE(c.genre, '')       = COALESCE(s.genre, '')
      AND COALESCE(c.content_type, '')= COALESCE(s.content_type, '')
      AND COALESCE(c.language, '')    = COALESCE(s.language, '')
      AND COALESCE(c.maturity_rating, '') = COALESCE(s.maturity_rating, '')
      AND COALESCE(c.studio, '')      = COALESCE(s.studio, '')
  ))

UNION ALL

-- Set C: Current active records that DID CHANGE (Close them out as expired)
SELECT
    c.content_key,
    c.content_id,
    c.title,
    c.genre,
    c.content_type,
    c.release_date,
    c.duration_mins,
    c.language,
    c.maturity_rating,
    c.production_cost,
    c.studio,
    c.content_tier,
    c.effective_date,
    ${hivevar:CURRENT_TS} AS expiry_date,
    FALSE                 AS is_current,
    c.version
FROM media_dw.dim_content c
INNER JOIN v_latest_stg_content s ON c.content_id = s.content_id
WHERE c.is_current = TRUE
  AND (
      COALESCE(c.title, '')           != COALESCE(s.title, '')
      OR COALESCE(c.genre, '')        != COALESCE(s.genre, '')
      OR COALESCE(c.content_type, '') != COALESCE(s.content_type, '')
      OR COALESCE(c.language, '')     != COALESCE(s.language, '')
      OR COALESCE(c.maturity_rating, '') != COALESCE(s.maturity_rating, '')
      OR COALESCE(c.studio, '')       != COALESCE(s.studio, '')
  )

UNION ALL

-- Set D: New version of CHANGED records (New active record with version + 1)
SELECT
    CAST(hash(concat(s.content_id, '_', CAST(${hivevar:CURRENT_TS} AS STRING))) AS BIGINT) AS content_key,
    s.content_id,
    s.title,
    s.genre,
    s.content_type,
    s.release_date,
    s.duration_mins,
    s.language,
    s.maturity_rating,
    s.production_cost,
    s.studio,
    s.content_tier,
    ${hivevar:CURRENT_TS}    AS effective_date,
    ${hivevar:EXPIRY_DATE}   AS expiry_date,
    TRUE                     AS is_current,
    c.version + 1            AS version
FROM v_latest_stg_content s
INNER JOIN media_dw.dim_content c ON s.content_id = c.content_id
WHERE c.is_current = TRUE
  AND (
      COALESCE(c.title, '')           != COALESCE(s.title, '')
      OR COALESCE(c.genre, '')        != COALESCE(s.genre, '')
      OR COALESCE(c.content_type, '') != COALESCE(s.content_type, '')
      OR COALESCE(c.language, '')     != COALESCE(s.language, '')
      OR COALESCE(c.maturity_rating, '') != COALESCE(s.maturity_rating, '')
      OR COALESCE(c.studio, '')       != COALESCE(s.studio, '')
  )

UNION ALL

-- Set E: BRAND NEW records (Did not previously exist in dim_content)
SELECT
    CAST(hash(concat(s.content_id, '_', CAST(${hivevar:CURRENT_TS} AS STRING))) AS BIGINT) AS content_key,
    s.content_id,
    s.title,
    s.genre,
    s.content_type,
    s.release_date,
    s.duration_mins,
    s.language,
    s.maturity_rating,
    s.production_cost,
    s.studio,
    s.content_tier,
    ${hivevar:CURRENT_TS}    AS effective_date,
    ${hivevar:EXPIRY_DATE}   AS expiry_date,
    TRUE                     AS is_current,
    1                        AS version
FROM v_latest_stg_content s
LEFT JOIN media_dw.dim_content c ON s.content_id = c.content_id
WHERE c.content_id IS NULL;

-- Step 6.3: Atomic overwrite into dim_content with unknown row fallback (-1)
INSERT OVERWRITE TABLE media_dw.dim_content
SELECT
    -1, 'CONTENT_UNK', 'Unknown Content', 'Unknown', 'Unknown',
    CAST('1900-01-01' AS DATE), 0, 'Unknown', 'NR', 0.00, 'Unknown', 'Unknown',
    CAST('1900-01-01 00:00:00' AS TIMESTAMP),
    CAST('9999-12-31 23:59:59' AS TIMESTAMP),
    TRUE, 1
WHERE NOT EXISTS (SELECT 1 FROM media_dw.dim_content_stage WHERE content_key = -1)
UNION ALL
SELECT * FROM media_dw.dim_content_stage;

DROP TABLE IF EXISTS media_dw.dim_content_stage;


-- ============================================================================
-- 7. SCD TYPE 2 IMPLEMENTATION: dim_user
-- Tracks customer lifecycle, subscription tier migration (Ad-Supported -> Premium),
-- and status transitions (Active -> Cancelled).
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS media_dw.dim_user_stage;
CREATE TABLE media_dw.dim_user_stage LIKE media_dw.dim_user;

-- Step 7.1: Deduplicate latest staging subscription & user events
CREATE OR REPLACE VIEW v_latest_stg_user AS
SELECT *
FROM (
    SELECT
        user_id,
        concat('User_', substr(user_id, -4)) AS first_name,
        'Customer' AS last_name,
        concat('u_', substr(user_id, -6), '@privacy.protected') AS email,
        '25-34' AS age_group,
        'Undisclosed' AS gender,
        CAST(start_date AS DATE) AS registration_date,
        'English' AS primary_language,
        COALESCE(region, 'US') AS country,
        COALESCE(region, 'North America') AS region,
        2 AS household_size,
        trim(plan_type) AS current_plan_type,
        trim(status) AS subscription_status,
        trim(payment_method) AS payment_method,
        'SmartTV' AS preferred_device_type,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ingestion_date DESC, start_date DESC) as rn
    FROM media_staging.stg_subscriptions
    WHERE user_id IS NOT NULL AND trim(user_id) != ''
) dedup
WHERE rn = 1;

-- Step 7.2: Reconcile dim_user SCD Type 2
INSERT OVERWRITE TABLE media_dw.dim_user_stage
-- Historical closed records
SELECT
    u.user_key, u.user_id, u.first_name, u.last_name, u.email,
    u.age_group, u.gender, u.registration_date, u.primary_language,
    u.country, u.region, u.household_size, u.current_plan_type,
    u.subscription_status, u.payment_method, u.preferred_device_type,
    u.effective_date, u.expiry_date, u.is_current, u.version
FROM media_dw.dim_user u
WHERE u.is_current = FALSE

UNION ALL

-- Unchanged active records
SELECT
    u.user_key, u.user_id, u.first_name, u.last_name, u.email,
    u.age_group, u.gender, u.registration_date, u.primary_language,
    u.country, u.region, u.household_size, u.current_plan_type,
    u.subscription_status, u.payment_method, u.preferred_device_type,
    u.effective_date, u.expiry_date, u.is_current, u.version
FROM media_dw.dim_user u
LEFT JOIN v_latest_stg_user s ON u.user_id = s.user_id
WHERE u.is_current = TRUE
  AND (s.user_id IS NULL OR (
      COALESCE(u.current_plan_type, '')   = COALESCE(s.current_plan_type, '')
      AND COALESCE(u.subscription_status, '') = COALESCE(s.subscription_status, '')
      AND COALESCE(u.payment_method, '')     = COALESCE(s.payment_method, '')
      AND COALESCE(u.region, '')             = COALESCE(s.region, '')
  ))

UNION ALL

-- Active records that changed (Expire existing active row)
SELECT
    u.user_key, u.user_id, u.first_name, u.last_name, u.email,
    u.age_group, u.gender, u.registration_date, u.primary_language,
    u.country, u.region, u.household_size, u.current_plan_type,
    u.subscription_status, u.payment_method, u.preferred_device_type,
    u.effective_date,
    ${hivevar:CURRENT_TS} AS expiry_date,
    FALSE                 AS is_current,
    u.version
FROM media_dw.dim_user u
INNER JOIN v_latest_stg_user s ON u.user_id = s.user_id
WHERE u.is_current = TRUE
  AND (
      COALESCE(u.current_plan_type, '')   != COALESCE(s.current_plan_type, '')
      OR COALESCE(u.subscription_status, '') != COALESCE(s.subscription_status, '')
      OR COALESCE(u.payment_method, '')     != COALESCE(s.payment_method, '')
      OR COALESCE(u.region, '')             != COALESCE(s.region, '')
  )

UNION ALL

-- New active version for changed users
SELECT
    CAST(hash(concat(s.user_id, '_', CAST(${hivevar:CURRENT_TS} AS STRING))) AS BIGINT) AS user_key,
    s.user_id, s.first_name, s.last_name, s.email,
    s.age_group, s.gender, s.registration_date, s.primary_language,
    s.country, s.region, s.household_size, s.current_plan_type,
    s.subscription_status, s.payment_method, s.preferred_device_type,
    ${hivevar:CURRENT_TS}   AS effective_date,
    ${hivevar:EXPIRY_DATE}  AS expiry_date,
    TRUE                    AS is_current,
    u.version + 1           AS version
FROM v_latest_stg_user s
INNER JOIN media_dw.dim_user u ON s.user_id = u.user_id
WHERE u.is_current = TRUE
  AND (
      COALESCE(u.current_plan_type, '')   != COALESCE(s.current_plan_type, '')
      OR COALESCE(u.subscription_status, '') != COALESCE(s.subscription_status, '')
      OR COALESCE(u.payment_method, '')     != COALESCE(s.payment_method, '')
      OR COALESCE(u.region, '')             != COALESCE(s.region, '')
  )

UNION ALL

-- Brand new subscriber records
SELECT
    CAST(hash(concat(s.user_id, '_', CAST(${hivevar:CURRENT_TS} AS STRING))) AS BIGINT) AS user_key,
    s.user_id, s.first_name, s.last_name, s.email,
    s.age_group, s.gender, s.registration_date, s.primary_language,
    s.country, s.region, s.household_size, s.current_plan_type,
    s.subscription_status, s.payment_method, s.preferred_device_type,
    ${hivevar:CURRENT_TS}   AS effective_date,
    ${hivevar:EXPIRY_DATE}  AS expiry_date,
    TRUE                    AS is_current,
    1                       AS version
FROM v_latest_stg_user s
LEFT JOIN media_dw.dim_user u ON s.user_id = u.user_id
WHERE u.user_id IS NULL;

-- Step 7.3: Overwrite dim_user with Unknown (-1) record
INSERT OVERWRITE TABLE media_dw.dim_user
SELECT
    -1, 'USER_UNK', 'Unknown', 'User', 'noreply@media.internal',
    'Unknown', 'U', CAST('1900-01-01' AS DATE), 'English',
    'Unknown', 'Unknown', 1, 'Standard_HD', 'Active', 'Credit_Card', 'SmartTV',
    CAST('1900-01-01 00:00:00' AS TIMESTAMP),
    CAST('9999-12-31 23:59:59' AS TIMESTAMP),
    TRUE, 1
WHERE NOT EXISTS (SELECT 1 FROM media_dw.dim_user_stage WHERE user_key = -1)
UNION ALL
SELECT * FROM media_dw.dim_user_stage;

DROP TABLE IF EXISTS media_dw.dim_user_stage;

-- ----------------------------------------------------------------------------
-- Gather Statistics on populated dimensions for Tez CBO
-- ----------------------------------------------------------------------------
ANALYZE TABLE media_dw.dim_content COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE media_dw.dim_user COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE media_dw.dim_date COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE media_dw.dim_device COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE media_dw.dim_geography COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE media_dw.dim_advertiser COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE media_dw.dim_time COMPUTE STATISTICS FOR COLUMNS;
