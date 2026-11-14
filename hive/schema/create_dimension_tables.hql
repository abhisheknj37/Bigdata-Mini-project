-- ============================================================================
-- SCRIPT: create_dimension_tables.hql
-- DESCRIPTION: Creates enterprise dimension tables for the Media & Entertainment
--              Data Warehouse in Apache Hive using the ORC storage format with
--              Snappy compression, indexing, and SCD Type 2 support.
-- ARCHITECTURE: Layer 2 - Enterprise Data Warehouse (EDW) Dimensional Model
-- STORAGE: ORC (Optimized Row Columnar), SNAPPY Compression
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

-- ----------------------------------------------------------------------------
-- 1. Create Enterprise Data Warehouse Database
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS media_dw
COMMENT 'Enterprise Data Warehouse for Media & Entertainment Business Intelligence and Reporting'
LOCATION '/data/edw/media/warehouse'
WITH DBPROPERTIES (
    'creator' = 'DataEngineering',
    'created_date' = '2026-11-14',
    'governance.tier' = 'Gold',
    'data.classification' = 'Confidential'
);

USE media_dw;

-- ----------------------------------------------------------------------------
-- 2. Dimension Table: dim_content (SCD Type 2)
-- Tracks all content metadata across life cycle (movies, episodes, tracks)
-- Maintains historical changes in genre classification, rating, or studio rights.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_content (
    content_key         BIGINT          COMMENT 'Surrogate key generated via sequence/hash for SCD Type 2 tracking',
    content_id          STRING          COMMENT 'Natural Business Key from Content Catalog CMS',
    title               STRING          COMMENT 'Title of the content item',
    genre               STRING          COMMENT 'Primary genre classification (Action, Drama, Comedy, etc.)',
    content_type        STRING          COMMENT 'Type of content (Movie, TV Series, Episode, Music Track, Podcast)',
    release_date        DATE            COMMENT 'Original release/premiere date',
    duration_mins       INT             COMMENT 'Total running duration in minutes',
    language            STRING          COMMENT 'Primary audio language',
    maturity_rating     STRING          COMMENT 'Maturity content rating (G, PG, PG-13, R, TV-MA, etc.)',
    production_cost     DECIMAL(15,2)   COMMENT 'Budget or acquisition cost in USD',
    studio              STRING          COMMENT 'Production studio, distributor, or label',
    content_tier        STRING          COMMENT 'Classification tier: Blockbuster, Original, Licensed, Catalog',
    effective_date      TIMESTAMP       COMMENT 'SCD Type 2: Timestamp when this version became active',
    expiry_date         TIMESTAMP       COMMENT 'SCD Type 2: Timestamp when this version was superseded (or 9999-12-31 23:59:59)',
    is_current          BOOLEAN         COMMENT 'SCD Type 2: TRUE if current active version, FALSE if historical record',
    version             INT             COMMENT 'SCD Type 2: Monotonically increasing revision counter'
)
COMMENT 'Conformed Content Dimension tracking metadata history with SCD Type 2'
STORED AS ORC
LOCATION '/data/edw/media/dim_content'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'content_id,genre,content_type,studio',
    'orc.stripe.size' = '67108864',
    'orc.row.index.stride' = '10000'
);

-- ----------------------------------------------------------------------------
-- 3. Dimension Table: dim_user (SCD Type 2)
-- Tracks subscriber demographics, plan changes, payment methods, and profile states.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_user (
    user_key                BIGINT          COMMENT 'Surrogate key generated for SCD Type 2 user tracking',
    user_id                 STRING          COMMENT 'Natural Business Key for the customer profile / account',
    first_name              STRING          COMMENT 'Customer first name (or masked identifier)',
    last_name               STRING          COMMENT 'Customer last name (or masked identifier)',
    email                   STRING          COMMENT 'Masked customer contact email address',
    age_group               STRING          COMMENT 'Demographic age bracket (18-24, 25-34, 35-49, 50-64, 65+)',
    gender                  STRING          COMMENT 'Demographic gender code (M, F, Non-Binary, Undisclosed)',
    registration_date       DATE            COMMENT 'Initial sign-up registration date',
    primary_language        STRING          COMMENT 'Preferred UI / audio language',
    country                 STRING          COMMENT 'Primary residence country',
    region                  STRING          COMMENT 'Geographical billing / viewing region',
    household_size          INT             COMMENT 'Estimated household size or registered sub-profiles',
    current_plan_type       STRING          COMMENT 'Subscription plan tier: Ad_Supported, Basic_SD, Standard_HD, Premium_4K',
    subscription_status     STRING          COMMENT 'Current status: Active, Trialing, Cancelled, Paused, Past_Due',
    payment_method          STRING          COMMENT 'Primary billing instrument: Credit_Card, PayPal, Apple_Pay, Carrier',
    preferred_device_type   STRING          COMMENT 'Most frequently used hardware device platform',
    effective_date          TIMESTAMP       COMMENT 'SCD Type 2: Timestamp when this record became active',
    expiry_date             TIMESTAMP       COMMENT 'SCD Type 2: Timestamp when this record was superseded',
    is_current              BOOLEAN         COMMENT 'SCD Type 2: Current active flag',
    version                 INT             COMMENT 'SCD Type 2: Revision counter'
)
COMMENT 'Conformed User Dimension tracking demographic and subscription tier history with SCD Type 2'
STORED AS ORC
LOCATION '/data/edw/media/dim_user'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'user_id,current_plan_type,subscription_status,country',
    'orc.stripe.size' = '67108864',
    'orc.row.index.stride' = '10000'
);

-- ----------------------------------------------------------------------------
-- 4. Dimension Table: dim_date
-- Conformed calendar and fiscal date dimension for time-series aggregation.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INT             COMMENT 'Surrogate date integer key in YYYYMMDD format (e.g., 20261114)',
    full_date           DATE            COMMENT 'Calendar date value',
    day                 INT             COMMENT 'Day of month (1 to 31)',
    month               INT             COMMENT 'Month of year (1 to 12)',
    month_name          STRING          COMMENT 'Full month name (January, February, etc.)',
    month_abbr          STRING          COMMENT 'Abbreviated month name (Jan, Feb, Mar, etc.)',
    quarter             INT             COMMENT 'Calendar quarter (1 to 4)',
    year                INT             COMMENT 'Calendar year (e.g., 2026)',
    day_of_week         INT             COMMENT 'Day of week (1 = Monday through 7 = Sunday)',
    day_name            STRING          COMMENT 'Day of week name (Monday, Tuesday, etc.)',
    is_weekend          BOOLEAN         COMMENT 'TRUE for Saturday/Sunday, FALSE for Monday-Friday',
    is_holiday          BOOLEAN         COMMENT 'TRUE if official national holiday, otherwise FALSE',
    holiday_name        STRING          COMMENT 'Holiday designation (e.g., New Year, Christmas, Thanksgiving, or None)',
    fiscal_quarter      INT             COMMENT 'Media company fiscal quarter (Q1 to Q4)',
    fiscal_year         INT             COMMENT 'Media company fiscal accounting year',
    week_of_year        INT             COMMENT 'ISO-8601 calendar week of year (1 to 53)'
)
COMMENT 'Conformed Date Dimension covering calendar, fiscal, and holiday dimensions'
STORED AS ORC
LOCATION '/data/edw/media/dim_date'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'date_key,year,quarter,month'
);

-- ----------------------------------------------------------------------------
-- 5. Dimension Table: dim_device
-- Conformed hardware, OS, and client software profile dimension.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_device (
    device_key          INT             COMMENT 'Surrogate key for device profile',
    device_type         STRING          COMMENT 'Hardware category: Smart TV, Mobile, Desktop, Tablet, Game Console, Streaming Stick',
    os                  STRING          COMMENT 'Operating system: iOS, Android, RokuOS, Tizen, FireOS, Windows, macOS, tvOS',
    browser             STRING          COMMENT 'Client runtime: Safari, Chrome, Edge, Firefox, Native App, Exoplayer',
    screen_resolution   STRING          COMMENT 'Screen resolution tier: 4K UHD, 1080p FHD, 720p HD, 480p SD, Mobile Responsive',
    manufacturer        STRING          COMMENT 'Hardware vendor: Apple, Samsung, LG, Sony, Amazon, Google, Roku',
    app_version         STRING          COMMENT 'Streaming client application build version'
)
COMMENT 'Conformed Device Dimension tracking hardware platforms and streaming runtimes'
STORED AS ORC
LOCATION '/data/edw/media/dim_device'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'device_type,os,screen_resolution'
);

-- ----------------------------------------------------------------------------
-- 6. Dimension Table: dim_geography
-- Conformed spatial dimension for viewer location and broadcast market areas.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_geography (
    geo_key             INT             COMMENT 'Surrogate key for geographic location',
    country             STRING          COMMENT 'Full country name (e.g., United States, United Kingdom, Germany)',
    country_code        STRING          COMMENT 'ISO 3166-1 alpha-2 / alpha-3 country code (e.g., US, USA)',
    state               STRING          COMMENT 'State, province, or sub-national division (e.g., California, Ontario)',
    city                STRING          COMMENT 'City or municipal locality name',
    region              STRING          COMMENT 'Global macro region: North America, EMEA, APAC, LATAM',
    sub_region          STRING          COMMENT 'Sub-territory (e.g., Western Europe, Southeast Asia, US West)',
    timezone            STRING          COMMENT 'Standard IANA timezone (e.g., America/Los_Angeles, Europe/London)',
    dma_code            STRING          COMMENT 'Nielsen Designated Market Area code for TV advertising (e.g., DMA-803)'
)
COMMENT 'Conformed Geography Dimension for geospatial distribution and broadcast marketing'
STORED AS ORC
LOCATION '/data/edw/media/dim_geography'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'country,region,city,timezone'
);

-- ----------------------------------------------------------------------------
-- 7. Dimension Table: dim_advertiser
-- Conformed advertising client, agency, and media brand dimension.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_advertiser (
    advertiser_key      INT             COMMENT 'Surrogate key for advertiser brand',
    advertiser_id       STRING          COMMENT 'Natural business key for the advertising entity',
    name                STRING          COMMENT 'Corporate or brand name of the advertiser',
    industry            STRING          COMMENT 'Vertical sector: Automotive, CPG, Tech, Finance, Retail, Pharma, Telecom',
    tier                STRING          COMMENT 'Commercial importance tier: Tier 1 Global, Tier 2 National, Tier 3 Programmatic',
    agency_partner      STRING          COMMENT 'Media planning or buying holding agency (e.g., WPP, Omnicom, Publicis)',
    billing_currency    STRING          COMMENT 'Commercial billing currency (e.g., USD, EUR, GBP)',
    account_status      STRING          COMMENT 'Contract state: Active, Credit_Hold, Suspended, Archived'
)
COMMENT 'Conformed Advertiser Dimension for AVOD and ad server campaign tracking'
STORED AS ORC
LOCATION '/data/edw/media/dim_advertiser'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'advertiser_id,industry,tier'
);

-- ----------------------------------------------------------------------------
-- 8. Dimension Table: dim_time
-- Conformed time-of-day dimension with media broadcasting dayparts / time bands.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_time (
    time_key            INT             COMMENT 'Surrogate time key formatted as HHMM (0 to 2359)',
    hour                INT             COMMENT 'Hour of day in 24-hour clock (0 to 23)',
    minute              INT             COMMENT 'Minute of hour (0 to 59)',
    am_pm               STRING          COMMENT 'Meridiem indicator: AM or PM',
    time_band           STRING          COMMENT 'Broadcast daypart: Early Morning, Morning, Daytime, Early Fringe, Prime Time, Late Night, Overnight',
    is_prime_time       BOOLEAN         COMMENT 'TRUE for 20:00 to 23:00 (8:00 PM - 11:00 PM), otherwise FALSE'
)
COMMENT 'Conformed Time Dimension tracking hourly dayparts and broadcast prime-time bands'
STORED AS ORC
LOCATION '/data/edw/media/dim_time'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'orc.bloom.filter.columns' = 'hour,time_band,is_prime_time'
);
