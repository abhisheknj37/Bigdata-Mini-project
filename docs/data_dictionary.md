# Enterprise Data Dictionary & Data Lineage Specification

**Database Name**: `media_dw`  
**Storage Engine**: Apache Hive on Tez / LLAP (ORC File Format with Snappy Compression)  
**Industry Domain**: Media & Entertainment (Over-The-Top Video Streaming & Digital Content)  

---

## Table of Contents

1. [Staging Layer Tables (Raw / Landing Zone)](#1-staging-layer-tables-raw--landing-zone)
   - [stg_users](#11-stg_users)
   - [stg_content](#12-stg_content)
   - [stg_subscription_plans](#13-stg_subscription_plans)
   - [stg_devices](#14-stg_devices)
   - [stg_viewing_activity](#15-stg_viewing_activity)
   - [stg_user_interactions](#16-stg_user_interactions)
   - [stg_subscription_billing](#17-stg_subscription_billing)
2. [Dimension Tables (Curated Star Schema)](#2-dimension-tables-curated-star-schema)
   - [dim_user (SCD Type 2)](#21-dim_user-scd-type-2)
   - [dim_content (SCD Type 1)](#22-dim_content-scd-type-1)
   - [dim_device (SCD Type 1)](#23-dim_device-scd-type-1)
   - [dim_subscription_plan](#24-dim_subscription_plan)
   - [dim_date](#25-dim_date)
3. [Fact Tables (Curated Star Schema)](#3-fact-tables-curated-star-schema)
   - [fact_viewing_activity](#31-fact_viewing_activity)
   - [fact_user_engagement](#32-fact_user_engagement)
   - [fact_subscription_billing](#33-fact_subscription_billing)
   - [fact_ad_performance](#34-fact_ad_performance)
4. [Curated Gold Data Marts](#4-curated-gold-data-marts)
   - [dm_daily_content_performance](#41-dm_daily_content_performance)
   - [dm_subscriber_churn_metrics](#42-dm_subscriber_churn_metrics)
   - [dm_qos_streaming_health](#43-dm_qos_streaming_health)
5. [Business Rules, Formulas & Transformation Logic](#5-business-rules-formulas--transformation-logic)
6. [Partitioning and Bucketing Strategy](#6-partitioning-and-bucketing-strategy)
7. [End-to-End Data Lineage Mapping Matrix](#7-end-to-end-data-lineage-mapping-matrix)
8. [Data Privacy & Classification Standards](#8-data-privacy--classification-standards)

---

## 1. Staging Layer Tables (Raw / Landing Zone)

Staging tables reside in HDFS under `/data/staging` or `/data/landing`. Format is Text/TSV or JSON, reflecting upstream ingestion schemas.

### 1.1 `stg_users`
- **Location**: `${nameNode}/data/staging/users/{YYYY-MM-DD}`
- **Source**: Upstream PostgreSQL Database (`subscribers` table via Apache Sqoop)
- **Format**: TextFile, Tab-delimited (`\t`)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `user_id` | BIGINT | No | Natural unique identifier for the subscriber in the RDBMS |
| `username` | STRING | No | System login handle |
| `email` | STRING | No | Subscriber email address (PII) |
| `first_name` | STRING | Yes | Given name |
| `last_name` | STRING | Yes | Surname |
| `country` | STRING | No | ISO 3166-1 alpha-2 country code (e.g., 'US', 'GB') |
| `age_group` | STRING | Yes | Demographic age bracket ('18-24', '25-34', '35-44', '45-54', '55+') |
| `subscription_plan_id`| INT | No | Foreign key linking to active subscription plan |
| `registration_date` | STRING | No | Date user registered account (YYYY-MM-DD) |
| `status` | STRING | No | Account status ('ACTIVE', 'SUSPENDED', 'CANCELLED') |
| `updated_at` | TIMESTAMP | No | Timestamp of last profile mutation in source DB |

---

### 1.2 `stg_content`
- **Location**: `${nameNode}/data/staging/content/{YYYY-MM-DD}`
- **Source**: Upstream PostgreSQL Database (`media_catalog` table via Apache Sqoop)
- **Format**: TextFile, Tab-delimited (`\t`)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `content_id` | BIGINT | No | Natural primary key of the media asset |
| `title` | STRING | No | Commercial title of the movie or episode |
| `content_type` | STRING | No | Content classification: 'Movie', 'TV Series', 'Episode', 'Special' |
| `genre` | STRING | No | Primary genre (e.g., 'Sci-Fi', 'Action', 'Drama', 'Comedy') |
| `release_year` | INT | No | Calendar release year |
| `duration_sec` | INT | No | Total nominal running length of asset in seconds |
| `content_rating`| STRING | Yes | Age rating designation ('G', 'PG', 'PG-13', 'R', 'TV-MA') |
| `studio` | STRING | Yes | Production studio or distributor |
| `language` | STRING | No | Primary audio soundtrack language |
| `series_title` | STRING | Yes | Parent series name for episodic television |
| `season_num` | INT | Yes | Season sequence number |
| `episode_num` | INT | Yes | Episode sequence number within season |
| `updated_at` | TIMESTAMP | No | Catalog metadata last modification timestamp |

---

### 1.3 `stg_subscription_plans`
- **Location**: `${nameNode}/data/staging/plans/`
- **Source**: Billing system configuration / RDBMS master
- **Format**: TextFile, Tab-delimited (`\t`)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `plan_id` | INT | No | Master primary key for subscription tier |
| `plan_name` | STRING | No | Marketing tier title ('Basic Ad-Supported', 'Standard', 'Premium 4K') |
| `monthly_price_usd`| DECIMAL(6,2)| No | Monthly recurring charge in USD |
| `max_streams` | INT | No | Maximum concurrent streams allowed |
| `video_quality`| STRING | No | Maximum streaming resolution ('HD (720p)', 'Full HD (1080p)', '4K Ultra HD') |
| `is_ad_supported`| CHAR(1) | No | 'Y' if subscription includes programmatic advertisements, else 'N' |

---

### 1.4 `stg_devices`
- **Location**: `${nameNode}/data/staging/devices/`
- **Source**: Client telemetry registration metadata
- **Format**: TextFile, Tab-delimited (`\t`)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `device_id` | INT | No | Unique device client surrogate ID |
| `device_type` | STRING | No | Device form factor ('Smart TV', 'Mobile', 'Tablet', 'Web Browser', 'Console') |
| `brand` | STRING | No | Manufacturer brand ('Samsung', 'Apple', 'LG', 'Roku', 'Sony') |
| `os_name` | STRING | No | Operating system ('iOS', 'Android', 'Tizen OS', 'webOS', 'Windows 11') |
| `os_version` | STRING | Yes | Operating system version string |
| `app_version`| STRING | Yes | Video player client SDK version |

---

### 1.5 `stg_viewing_activity`
- **Location**: `${nameNode}/data/landing/viewing_logs/{YYYY-MM-DD}`
- **Source**: Client Video Player Beacon Streams (ExoPlayer, AVPlayer, Shaka Player via Kafka / Flume)
- **Format**: TextFile / JSON Lines

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `viewing_id` | BIGINT | No | Natural event identifier generated by player SDK |
| `session_id` | STRING | No | Client playback session UUID |
| `user_id` | BIGINT | No | Subscriber natural ID |
| `content_id` | BIGINT | No | Media asset natural ID |
| `device_id` | INT | No | Client device model identifier |
| `plan_id` | INT | No | Active subscription plan identifier |
| `stream_start_time`| TIMESTAMP | No | Playback initiate timestamp (UTC) |
| `stream_end_time` | TIMESTAMP | No | Playback terminate timestamp (UTC) |
| `watch_duration_sec`| INT | No | Total elapsed playback duration in seconds |
| `completion_pct` | DECIMAL(5,2)| No | Percentage of title viewed `(watch_duration / total_duration) * 100` |
| `bitrate_kbps` | INT | No | Mean video playback bitrate in kilobits per second |
| `buffer_count` | INT | No | Number of stalling rebuffering interruptions |
| `buffer_duration_sec`| INT| No | Cumulative rebuffering freeze duration in seconds |
| `pause_count` | INT | No | Number of manual user pause actions |
| `seek_count` | INT | No | Number of forward/backward seek scrub operations |
| `error_code` | STRING | No | Fatal playback error code ('NONE', 'ERR_NET_TIMEOUT', 'ERR_DRM_FAIL') |
| `resolution` | STRING | No | Video display resolution string ('1920x1080', '3840x2160') |
| `is_completed` | CHAR(1) | No | 'Y' if viewed >= 90% of duration, else 'N' |

---

### 1.6 `stg_user_interactions`
- **Location**: `${nameNode}/data/landing/interactions/{YYYY-MM-DD}`
- **Source**: Application UI clickstream & interaction logs
- **Format**: TextFile / CSV

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `interaction_id` | BIGINT | No | Unique natural ID of the engagement event |
| `user_id` | BIGINT | No | Subscriber natural ID |
| `content_id` | BIGINT | No | Targeted media content ID |
| `device_id` | INT | No | Client device model ID |
| `interaction_type`| STRING | No | Event type: 'RATING', 'LIKE', 'DISLIKE', 'WATCHLIST_ADD', 'SHARE' |
| `rating_value` | INT | Yes | Star score (1-5) if interaction_type='RATING', else NULL |
| `interaction_time`| TIMESTAMP | No | Event occurrence timestamp (UTC) |

---

### 1.7 `stg_subscription_billing`
- **Location**: `${nameNode}/data/staging/billing/{YYYY-MM-DD}`
- **Source**: Payment Gateway / Stripe / Recurly financial exports
- **Format**: TextFile, Tab-delimited (`\t`)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `transaction_id` | BIGINT | No | Unique payment transaction reference |
| `user_id` | BIGINT | No | Subscriber natural ID |
| `plan_id` | INT | No | Subscription plan billed |
| `billing_date` | STRING | No | Invoice posting date (YYYY-MM-DD) |
| `amount_billed_usd`| DECIMAL(8,2)| No | Gross amount charged to payment instrument |
| `discount_usd` | DECIMAL(8,2)| No | Promotional discount applied |
| `tax_usd` | DECIMAL(8,2)| No | Calculated sales / VAT tax |
| `payment_method` | STRING | No | Payment type ('CREDIT_CARD', 'PAYPAL', 'APPLE_PAY') |
| `payment_status` | STRING | No | Settlement outcome: 'PAID', 'FAILED', 'REFUNDED' |

---

## 2. Dimension Tables (Curated Star Schema)

Stored in Hive Database `media_dw`, format **ORC**, optimized with ZLIB/Snappy compression.

### 2.1 `dim_user` (SCD Type 2)
- **Table Name**: `media_dw.dim_user`
- **Storage**: ORC Format, Unpartitioned, Clustered by `user_id` into 16 Buckets
- **SCD Strategy**: Type 2 historical tracking for tier and country changes.

| Column Name | Data Type | Nullable | Key Type | Default | Description |
| :--- | :--- | :---: | :---: | :--- | :--- |
| `user_key` | BIGINT | No | Surrogate PK | Generated | Auto-incrementing surrogate dimension key |
| `user_id` | BIGINT | No | Natural Key | None | Subscriber ID from production source DB |
| `username` | VARCHAR(64)| No | None | None | Account username |
| `email` | VARCHAR(128)| No | PII | None | Subscriber email address (Masked in BI) |
| `first_name` | VARCHAR(64)| Yes | PII | NULL | Given name |
| `last_name` | VARCHAR(64)| Yes | PII | NULL | Family surname |
| `country` | CHAR(2) | No | None | 'US' | ISO 3166-1 alpha-2 country code |
| `age_group` | VARCHAR(10)| Yes | None | 'Unknown' | Age demographic segment |
| `subscription_tier` | VARCHAR(32)| No | None | None | Active plan name during validity window |
| `registration_date` | DATE | No | None | None | Account creation date |
| `account_status` | VARCHAR(16)| No | None | 'ACTIVE' | Status: 'ACTIVE', 'SUSPENDED', 'CANCELLED' |
| `effective_start_date`| DATE | No | SCD2 | None | Beginning of record version validity |
| `effective_end_date` | DATE | No | SCD2 | '9999-12-31'| Expiration date of record version |
| `is_current` | BOOLEAN | No | SCD2 Flag | TRUE | TRUE if this record represents the current state |

---

### 2.2 `dim_content` (SCD Type 1)
- **Table Name**: `media_dw.dim_content`
- **Storage**: ORC Format, Clustered by `content_id` into 8 Buckets
- **SCD Strategy**: Type 1 (Overwritten in place upon catalog updates).

| Column Name | Data Type | Nullable | Key Type | Default | Description |
| :--- | :--- | :---: | :---: | :--- | :--- |
| `content_key` | BIGINT | No | Surrogate PK | Generated | Auto-incrementing surrogate key |
| `content_id` | BIGINT | No | Natural Key | None | Upstream catalog asset ID |
| `title` | VARCHAR(255)| No | None | None | Commercial release title |
| `content_type`| VARCHAR(32) | No | None | None | 'Movie', 'TV Series', 'Episode' |
| `genre` | VARCHAR(64) | No | None | 'General' | Primary genre category |
| `release_year`| SMALLINT | No | None | None | Production release year |
| `duration_sec`| INT | No | Metric | 0 | Total duration in seconds |
| `duration_min`| DECIMAL(6,2)| No | Metric | 0.0 | Calculated duration in minutes |
| `content_rating`| VARCHAR(16)| Yes | None | 'NR' | Censorship rating |
| `studio` | VARCHAR(128)| Yes | None | 'Independent'| Production studio / network |
| `language` | VARCHAR(32) | No | None | 'English' | Primary soundtrack language |
| `series_title`| VARCHAR(255)| Yes | None | NULL | Parent series name for episodes |
| `season_num` | TINYINT | Yes | None | NULL | Season index |
| `episode_num` | SMALLINT | Yes | None | NULL | Episode index |

---

### 2.3 `dim_device` (SCD Type 1)
- **Table Name**: `media_dw.dim_device`
- **Storage**: ORC Format

| Column Name | Data Type | Nullable | Key Type | Description |
| :--- | :--- | :---: | :---: | :--- |
| `device_key` | INT | No | Surrogate PK | Auto-incrementing surrogate key |
| `device_id` | INT | No | Natural Key | Device model profile identifier |
| `device_type`| VARCHAR(32) | No | None | 'Smart TV', 'Mobile', 'Tablet', 'Web Browser', 'Console' |
| `brand` | VARCHAR(64) | No | None | Hardware manufacturer |
| `os_name` | VARCHAR(64) | No | None | Operating system name |
| `os_version` | VARCHAR(32) | Yes | None | Operating system release version |
| `app_version`| VARCHAR(32) | Yes | None | Streaming player client release build |

---

### 2.4 `dim_subscription_plan`
- **Table Name**: `media_dw.dim_subscription_plan`
- **Storage**: ORC Format

| Column Name | Data Type | Nullable | Key Type | Description |
| :--- | :--- | :---: | :---: | :--- |
| `plan_key` | TINYINT | No | Surrogate PK | Auto-incrementing plan surrogate key |
| `plan_id` | INT | No | Natural Key | Upstream master plan ID |
| `plan_name` | VARCHAR(64) | No | None | Marketing plan tier name |
| `monthly_price_usd`| DECIMAL(6,2)| No | None | Monthly price in USD |
| `max_streams`| TINYINT | No | None | Maximum concurrent playback streams allowed |
| `video_quality`| VARCHAR(32) | No | None | Maximum supported resolution |
| `is_ad_supported`| CHAR(1) | No | None | 'Y' = Ad-supported tier, 'N' = Commercial-free |

---

### 2.5 `dim_date`
- **Table Name**: `media_dw.dim_date`
- **Storage**: ORC Format, Pre-populated calendar table (2020 - 2035)

| Column Name | Data Type | Nullable | Key Type | Description |
| :--- | :--- | :---: | :---: | :--- |
| `date_key` | INT | No | PK | Numeric surrogate key in YYYYMMDD format (e.g., 20261114) |
| `calendar_date`| DATE | No | None | Standard SQL Date format (YYYY-MM-DD) |
| `day_of_month`| TINYINT | No | None | Day number within the month (1-31) |
| `day_of_week` | VARCHAR(10)| No | None | Day name ('Monday', 'Tuesday', ..., 'Sunday') |
| `month_num` | TINYINT | No | None | Month sequence (1-12) |
| `month_name` | VARCHAR(12)| No | None | Full month name ('January', ..., 'December') |
| `quarter` | TINYINT | No | None | Calendar quarter (1-4) |
| `year` | SMALLINT | No | None | 4-digit calendar year (e.g., 2026) |
| `is_weekend` | BOOLEAN | No | None | TRUE if Saturday or Sunday, else FALSE |
| `is_holiday` | BOOLEAN | No | None | TRUE if recognized federal/commercial holiday |

---

## 3. Fact Tables (Curated Star Schema)

### 3.1 `fact_viewing_activity`
- **Table Name**: `media_dw.fact_viewing_activity`
- **Grain**: One row per continuous video playback session
- **Partitioning**: Partitioned by `event_date` (STRING `YYYY-MM-DD`)
- **Bucketing**: Clustered by `user_key` into **32 Buckets**, sorted by `stream_start_time`
- **Storage**: ORC Format, Snappy compressed

| Column Name | Data Type | Nullable | Key Type | Description |
| :--- | :--- | :---: | :---: | :--- |
| `viewing_id` | BIGINT | No | Business Key | Telemetry session unique identifier |
| `session_id` | VARCHAR(64) | No | Degenerate Dim| Player client session UUID |
| `user_key` | BIGINT | No | Foreign Key | References `dim_user.user_key` |
| `content_key` | BIGINT | No | Foreign Key | References `dim_content.content_key` |
| `device_key` | INT | No | Foreign Key | References `dim_device.device_key` |
| `plan_key` | TINYINT | No | Foreign Key | References `dim_subscription_plan.plan_key` |
| `date_key` | INT | No | Foreign Key | References `dim_date.date_key` (YYYYMMDD) |
| `stream_start_time`| TIMESTAMP | No | Timestamp | Session playback initiate timestamp |
| `stream_end_time` | TIMESTAMP | No | Timestamp | Session playback terminate timestamp |
| `watch_duration_sec`| INT | No | Additive Metric| Total active video playback elapsed seconds |
| `completion_pct` | DECIMAL(5,2)| No | Non-additive | Percent of title viewed `(watch_sec / title_sec) * 100` |
| `bitrate_kbps` | INT | No | Semi-additive | Average encoded stream bitrate |
| `buffer_count` | SMALLINT | No | Additive Metric| Number of video playback buffering stalls |
| `buffer_duration_sec`| INT | No | Additive Metric| Cumulative buffering freeze wait duration |
| `pause_count` | TINYINT | No | Additive Metric| Number of manual user pause actions |
| `seek_count` | TINYINT | No | Additive Metric| Number of seek/scrub interactions |
| `error_code` | VARCHAR(32) | No | Degenerate Dim| Terminal error code encountered ('NONE' for clean) |
| `resolution` | VARCHAR(16) | No | Dimension | Display stream resolution ('1920x1080', '3840x2160') |
| `is_completed` | CHAR(1) | No | Indicator | 'Y' if viewed >= 90% of duration, else 'N' |
| `event_date` | STRING | No | **PARTITION** | Partition date key format `YYYY-MM-DD` |

---

### 3.2 `fact_user_engagement`
- **Table Name**: `media_dw.fact_user_engagement`
- **Grain**: One row per distinct subscriber feedback or catalog action
- **Partitioning**: Partitioned by `event_date` (STRING `YYYY-MM-DD`)
- **Storage**: ORC Format, Snappy compressed

| Column Name | Data Type | Nullable | Key Type | Description |
| :--- | :--- | :---: | :---: | :--- |
| `interaction_id` | BIGINT | No | Business Key | Engagement event unique ID |
| `user_key` | BIGINT | No | Foreign Key | References `dim_user.user_key` |
| `content_key` | BIGINT | No | Foreign Key | References `dim_content.content_key` |
| `device_key` | INT | No | Foreign Key | References `dim_device.device_key` |
| `date_key` | INT | No | Foreign Key | References `dim_date.date_key` |
| `interaction_type`| VARCHAR(32) | No | Degenerate Dim| 'RATING', 'LIKE', 'DISLIKE', 'WATCHLIST_ADD', 'SHARE' |
| `rating_val` | TINYINT | Yes | Metric | Star rating value (1-5) or NULL for actions |
| `interaction_time`| TIMESTAMP | No | Timestamp | Exact event timestamp |
| `event_date` | STRING | No | **PARTITION** | Partition date key `YYYY-MM-DD` |

---

### 3.3 `fact_subscription_billing`
- **Table Name**: `media_dw.fact_subscription_billing`
- **Grain**: One row per financial billing transaction
- **Partitioning**: Partitioned by `billing_date` (STRING `YYYY-MM-DD`)
- **Storage**: ORC Format

| Column Name | Data Type | Nullable | Key Type | Description |
| :--- | :--- | :---: | :---: | :--- |
| `transaction_id` | BIGINT | No | Business Key | Payment gateway invoice transaction ID |
| `user_key` | BIGINT | No | Foreign Key | References `dim_user.user_key` |
| `plan_key` | TINYINT | No | Foreign Key | References `dim_subscription_plan.plan_key` |
| `date_key` | INT | No | Foreign Key | References `dim_date.date_key` |
| `amount_billed_usd`| DECIMAL(8,2)| No | Additive Metric| Gross monetary charge |
| `discount_usd` | DECIMAL(8,2)| No | Additive Metric| Coupon/promotional reduction |
| `tax_usd` | DECIMAL(8,2)| No | Additive Metric| Remitted tax amount |
| `net_revenue_usd`| DECIMAL(8,2)| No | Additive Metric| Calculated net revenue `amount_billed - tax` |
| `payment_method` | VARCHAR(32) | No | Degenerate Dim| 'CREDIT_CARD', 'PAYPAL', 'APPLE_PAY' |
| `payment_status` | VARCHAR(16) | No | Degenerate Dim| 'PAID', 'FAILED', 'REFUNDED' |
| `billing_date` | STRING | No | **PARTITION** | Partition date key `YYYY-MM-DD` |

---

### 3.4 `fact_ad_performance`
- **Table Name**: `media_dw.fact_ad_performance`
- **Grain**: One row per programmatic video ad insertion break
- **Partitioning**: Partitioned by `event_date` (STRING `YYYY-MM-DD`)
- **Storage**: ORC Format

| Column Name | Data Type | Nullable | Key Type | Description |
| :--- | :--- | :---: | :---: | :--- |
| `ad_event_id` | BIGINT | No | Business Key | Unique ad pod impression ID |
| `user_key` | BIGINT | No | Foreign Key | References `dim_user.user_key` |
| `content_key` | BIGINT | No | Foreign Key | References `dim_content.content_key` |
| `device_key` | INT | No | Foreign Key | References `dim_device.device_key` |
| `date_key` | INT | No | Foreign Key | References `dim_date.date_key` |
| `ad_duration_sec`| INT | No | Additive Metric| Length of commercial in seconds |
| `time_viewed_sec`| INT | No | Additive Metric| Total duration commercial was viewed |
| `is_skipped` | CHAR(1) | No | Indicator | 'Y' if skipped after 5s threshold, else 'N' |
| `is_completed` | CHAR(1) | No | Indicator | 'Y' if viewed 100% to completion, else 'N' |
| `cpm_rate_usd` | DECIMAL(6,2)| No | Metric | Cost Per Mille advertiser rate |
| `event_date` | STRING | No | **PARTITION** | Partition date key `YYYY-MM-DD` |

---

## 4. Curated Gold Data Marts

Stored in Hive Database `media_marts`, pre-aggregated daily to power executive dashboards with sub-second response times.

### 4.1 `dm_daily_content_performance`
- **Purpose**: Tracks title engagement, total hours watched, unique audience reach, and completion rates by calendar day.
- **Grain**: One row per `content_key` per `event_date`.

| Column Name | Data Type | Formula / Business Definition |
| :--- | :--- | :--- |
| `event_date` | DATE | Performance reporting date |
| `content_key` | BIGINT | Foreign key to `dim_content` |
| `content_title` | VARCHAR(255) | Denormalized title |
| `content_type` | VARCHAR(32) | 'Movie' or 'Episode' |
| `genre` | VARCHAR(64) | Content genre |
| `total_stream_sessions`| BIGINT | `COUNT(viewing_id)` |
| `unique_viewers` | BIGINT | `COUNT(DISTINCT user_key)` |
| `total_hours_watched` | DECIMAL(12,2)| `SUM(watch_duration_sec) / 3600.0` |
| `completed_views` | BIGINT | `COUNT(CASE WHEN is_completed = 'Y' THEN 1 END)` |
| `completion_rate_pct` | DECIMAL(5,2) | `(completed_views / total_stream_sessions) * 100.0` |
| `avg_watch_time_min` | DECIMAL(6,2) | `AVG(watch_duration_sec) / 60.0` |
| `avg_buffering_ratio` | DECIMAL(5,2) | `AVG(buffer_duration_sec / NULLIF(watch_duration_sec, 0)) * 100.0` |

---

### 4.2 `dm_subscriber_churn_metrics`
- **Purpose**: Powers ML churn classification and subscriber lifetime value (LTV) models.
- **Grain**: One row per active subscriber per calendar month.

| Column Name | Data Type | Formula / Business Definition |
| :--- | :--- | :--- |
| `reporting_month` | STRING | Format 'YYYY-MM' |
| `user_key` | BIGINT | Foreign key to `dim_user` |
| `days_active_30d` | INT | Distinct calendar days with >= 1 viewing session in last 30 days |
| `total_watch_hours_30d`| DECIMAL(8,2) | Total cumulative hours streamed in last 30 days |
| `unique_titles_30d` | INT | Number of distinct content titles consumed |
| `failed_payment_count` | INT | Billing retry attempts failed in current month |
| `churn_risk_score` | DECIMAL(4,3) | Model prediction score [0.000 - 1.000] |
| `is_churned` | CHAR(1) | 'Y' if account cancelled or 0 streams in 30 days, else 'N' |

---

### 4.3 `dm_qos_streaming_health`
- **Purpose**: Network engineering & CDN performance monitoring.
- **Grain**: One row per `device_type` per `event_date`.

| Column Name | Data Type | Formula / Business Definition |
| :--- | :--- | :--- |
| `event_date` | DATE | Observation date |
| `device_type` | VARCHAR(32) | Device form factor |
| `total_sessions` | BIGINT | Total session volume |
| `rebuffering_ratio_pct`| DECIMAL(5,3) | `(SUM(buffer_duration_sec) / SUM(watch_duration_sec)) * 100.0` |
| `error_rate_pct` | DECIMAL(5,2) | `(COUNT(CASE WHEN error_code != 'NONE' THEN 1 END) / total_sessions) * 100.0` |
| `avg_bitrate_kbps` | INT | `AVG(bitrate_kbps)` |
| `p95_buffer_duration_sec`| INT | 95th percentile rebuffering stall time |

---

## 5. Business Rules, Formulas & Transformation Logic

### 5.1 Playback Sessionization Rules (Apache Pig)
- **Heartbeat Aggregation**: Raw video players emit periodic telemetry heartbeats every 10 seconds.
- **Session Boundary**: Heartbeats sharing the same `(session_id, user_id, content_id)` are grouped. A gap of > 30 minutes of inactivity indicates a session termination.
- **Anomaly Rejection**:
  - Heartbeats where `stream_end_time < stream_start_time` are dropped.
  - Heartbeats with `watch_duration_sec > 86400` (24 hours continuous) are flagged as stalled clients and capped.

### 5.2 Completion Percentage Formula
$$\text{completion\_pct} = \min\left(100.00, \left(\frac{\text{watch\_duration\_sec}}{\text{content.duration\_sec}}\right) \times 100.00\right)$$
- If `completion_pct >= 90.00%`, `is_completed = 'Y'`; otherwise, `is_completed = 'N'`.

### 5.3 Quality of Service (QoS) Rebuffering Ratio
$$\text{rebuffering\_ratio\_pct} = \left(\frac{\sum \text{buffer\_duration\_sec}}{\sum \text{watch\_duration\_sec}}\right) \times 100.00$$
- **Engineering SLA**: Platforms aim for a rebuffering ratio $< 0.50\%$. A ratio $> 1.00\%$ triggers CDN traffic rerouting.

### 5.4 Monthly Recurring Revenue (MRR)
$$\text{MRR} = \sum_{\text{active users}} \left(\text{plan.monthly\_price\_usd} - \text{discount\_usd}\right)$$

### 5.5 Active User Classifications
- **Daily Active User (DAU)**: Unique `user_key` with at least one stream with `watch_duration_sec >= 120` (2 minutes) within a 24-hour UTC day.
- **Monthly Active User (MAU)**: Unique `user_key` with at least one stream within the trailing 30 rolling days.

---

## 6. Partitioning and Bucketing Strategy

| Table Name | Partition Key | Cardinality | Average Partition Size | Bucketing Key | Number of Buckets | Performance Rationale |
| :--- | :--- | :---: | :---: | :--- | :---: | :--- |
| `fact_viewing_activity`| `event_date` (YYYY-MM-DD)| 365 / yr | ~35 - 50 GB / day | `user_key` | 32 | Partition pruning limits disk scan to requested dates. Bucketing by `user_key` facilitates Sort-Merge-Bucket (SMB) joins with `dim_user`. |
| `fact_user_engagement`| `event_date` (YYYY-MM-DD)| 365 / yr | ~8 - 12 GB / day | `user_key` | 16 | Aligns with fact viewing bucketing for multi-fact correlation joins. |
| `fact_subscription_billing`| `billing_date` (YYYY-MM-DD)| 365 / yr | ~2 - 4 GB / day | `user_key` | 8 | Partition pruning by billing cycle date. |
| `dim_user` | None (Unpartitioned) | 1 table | ~5 - 10 GB | `user_key` | 32 | Co-located bucket count matches fact tables for shuffle-free map joins. |

---

## 7. End-to-End Data Lineage Mapping Matrix

| Source Entity & Column | Cleansing / Staging Logic | Warehouse Target Table & Column | Target Hive Type | Business Definition |
| :--- | :--- | :--- | :--- | :--- |
| `subscribers.user_id` | Direct Sqoop Import | `dim_user.user_id` | `BIGINT` | Subscriber business key |
| `subscribers.email` | Lowercased, trimmed, PII flag | `dim_user.email` | `VARCHAR(128)` | User email address |
| `subscribers.plan_id` | Joined with `plans` for tier name | `dim_user.subscription_tier`| `VARCHAR(32)` | Marketing tier |
| `media_catalog.content_id` | Direct Sqoop Import | `dim_content.content_id` | `BIGINT` | Catalog asset ID |
| `media_catalog.title` | Cleaned special characters | `dim_content.title` | `VARCHAR(255)` | Media title |
| `media_catalog.duration_sec`| Cast to integer seconds | `dim_content.duration_sec` | `INT` | Nominal length |
| `player_beacon.session_id` | Pig sessionization grouping | `fact_viewing_activity.session_id` | `VARCHAR(64)` | Playback session UUID |
| `player_beacon.play_sec` | Pig sum aggregate over session | `fact_viewing_activity.watch_duration_sec` | `INT` | Total watched seconds |
| `player_beacon.buf_count` | Pig sum aggregate over session | `fact_viewing_activity.buffer_count` | `SMALLINT` | Stall count |
| `player_beacon.bitrate` | Pig weighted mean by duration | `fact_viewing_activity.bitrate_kbps` | `INT` | Mean streaming bitrate |
| `invoices.amount` | Sqoop import currency format | `fact_subscription_billing.amount_billed_usd` | `DECIMAL(8,2)` | Invoice gross charge |

---

## 8. Data Privacy & Classification Standards

| Classification Level | Definition | Tables & Columns Affected | Masking / Protection Policy |
| :--- | :--- | :--- | :--- |
| **PII (Personally Identifiable Information)** | Information that directly identifies an individual subscriber. | `dim_user.email`, `dim_user.first_name`, `dim_user.last_name` | Apache Ranger dynamic SHA-256 masking. Redacted for all non-privileged analytics roles. |
| **Financial / PCI-DSS** | Monetary transaction and invoice amounts. | `fact_subscription_billing.amount_billed_usd`, `discount_usd` | Restricted access. Limited to Financial Planning & Analysis (FP&A) team group. |
| **Commercial Confidential** | Content performance, contract licensing terms, and studio distributors. | `dim_content.studio`, `dm_daily_content_performance` | Internal analytics authorized users only. |
| **Internal Telemetry** | QoS and player performance statistics. | `fact_viewing_activity`, `dm_qos_streaming_health` | Unmasked for Engineering and Data Science teams. |
