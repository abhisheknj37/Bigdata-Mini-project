# Enterprise Data Warehousing Architecture for Media & Entertainment

**Platform**: Apache Hadoop Ecosystem (HDFS, Hive on Tez, Apache Pig, Apache Sqoop, Apache Oozie)  
**Industry Domain**: Over-The-Top (OTT) Streaming, Video-On-Demand (VOD), and Digital Media  
**Target Workload**: Multi-Petabyte Video Telemetry, Subscriber Behavior, and Financial Analytics  

---

## 1. Executive Summary & Solution Overview

In the highly competitive digital streaming landscape (comparable to platforms like Netflix, Disney+, Spotify, and HBO Max), data is the foundational asset driving subscriber retention, content acquisition investments, Quality of Service (QoS) delivery, and hyper-personalized recommendations.

Modern OTT platforms generate billions of client-side player telemetry events per day—encompassing video playback heartbeats, buffering episodes, bitrate transitions, and user interactions (ratings, watchlist updates, searches). Simultaneously, transactional systems process subscriber registrations, subscription upgrades/downgrades, and recurring billing cycles.

### 1.1 Core Business Objectives

1. **Viewer Experience & QoS Monitoring**: Real-time and batch identification of video playback degradation (rebuffering ratio, start-time latency, dropped frames, CDN edge failures).
2. **Content Performance & ROI Analytics**: Precise measurement of title engagement, completion rates, binge-watching patterns, and drop-off points to guide multi-million dollar licensing and production investments.
3. **Subscriber Churn & Lifetime Value (LTV) Modeling**: Historical feature store creation capturing viewing frequency, payment history, and engagement trends to predict churn risk weeks before cancellation.
4. **Ad-Supported Monetization (AVOD)**: Tracking ad impression rates, view-through rates (VTR), and fill rates for advertising-tier subscribers.
5. **Operational Excellence**: Cost-efficient, fault-tolerant batch ETL processing capable of processing 100+ TBs daily within strict 2-hour SLA completion windows.

---

## 2. End-to-End Data Flow Architecture

The data platform implements a structured multi-tier **Medallion Lakehouse / Data Warehouse Architecture** over HDFS, orchestrated via Apache Oozie workflows and coordinators.

### 2.1 Architectural Flow Diagram

```
+-------------------------------------------------------------------------------------------------------+
|                                         DATA SOURCE LAYER                                             |
+------------------------------------+-----------------------------------+------------------------------+
|   Client Video Player Telemetry    |    Upstream Transactional RDBMS   |     Clickstream & Edge       |
| (ExoPlayer, AVPlayer, Web Player)  |    (PostgreSQL / MySQL Masters)   |      (Fastly, Cloudflare)    |
| - Playback beacons (every 10s)     | - Subscriber Account Master       | - CDN access logs            |
| - Bitrate, Buffering, Errors       | - Media Catalog & Metadata        | - Web & mobile clickstream   |
| - Start/Pause/Seek events          | - Invoices & Billing Transactions | - Search & discovery events  |
+-----------------+------------------+-----------------+-----------------+--------------+---------------+
                  |                                    |                                |
                  | Kafka Connect / Flume              | Apache Sqoop (Parallel JDBC)   | Fluentd / Logstash
                  v                                    v                                v
+-------------------------------------------------------------------------------------------------------+
|                                    HDFS STORAGE TIERS (LAKEHOUSE)                                     |
+-------------------------------------------------------------------------------------------------------+
|  1. LANDING & RAW ZONE: /data/landing/{entity}/{YYYY-MM-DD}                                           |
|     - Raw JSON beacons, immutable compressed CSV logs, raw CDN web server text files                  |
|     - Unmodified audit trail with 90-day retention                                                    |
+--------------------------------------------------+----------------------------------------------------+
                                                   |
                                                   v
|  2. STAGING & BRONZE ZONE: /data/staging/{entity}/{YYYY-MM-DD}                                        |
|     - Parallel Sqoop extractions partitioned by business date                                         |
|     - Ephemeral landing datasets for incremental processing                                           |
+--------------------------------------------------+----------------------------------------------------+
                                                   |
                         +-------------------------+-------------------------+
                         |                                                   |
                         v (Apache Pig Sessionization)                       v (Pig Deduplication)
+-------------------------------------------------------------------------------------------------------+
|  3. CLEANSED & ENRICHED ZONE (SILVER): /data/cleansed/{entity}/{YYYY-MM-DD}                           |
|     - Filtered corrupt beacons, reconstructed stream sessions (sessionization)                        |
|     - Normalized durations, calculated completion percentages, anomaly removal                        |
+--------------------------------------------------+----------------------------------------------------+
                                                   |
                                                   v (Hive on Tez SCD Merge & Dim/Fact Population)
+-------------------------------------------------------------------------------------------------------+
|  4. CURATED ENTERPRISE WAREHOUSE (ORC FORMAT): /warehouse/tablespace/external/hive/media_dw.db        |
|     - Star Schema: Type 2 Slowly Changing Dimensions (dim_user, dim_content, dim_device)             |
|     - High-volume Facts: fact_viewing_activity, fact_user_engagement, fact_subscription_billing       |
|     - Partitioned by event_date, bucketed by user_key into 32 buckets, Snappy/ZLIB compression        |
+--------------------------------------------------+----------------------------------------------------+
                                                   |
                                                   v (Hive Scheduled Aggregations)
+-------------------------------------------------------------------------------------------------------+
|  5. GOLD DATA MARTS & AGGREGATE LAYER: /warehouse/tablespace/external/hive/media_marts.db             |
|     - dm_daily_content_performance (Watch hours, unique viewers, completion rate per title)           |
|     - dm_subscriber_churn_metrics (Rolling 30-day activity, billing health, churn indicators)         |
|     - dm_qos_streaming_health (Rebuffering ratio, error rates, average bitrate by device/ISP)         |
+--------------------------------------------------+----------------------------------------------------+
                                                   |
                                                   v
+-------------------------------------------------------------------------------------------------------+
|                                       CONSUMPTION & SERVING LAYER                                     |
+--------------------+---------------------+---------------------+--------------------------------------+
|  Tableau / PowerBI |  Presto / Trino SQL |  Data Science & ML  |  Executive Operational Dashboards    |
|  Executive Reports |  Ad-hoc Interactive |  Spark / MLlib      |  Prometheus + Grafana                |
|  & KPI Dashboards  |  Exploration (< 2s) |  Churn & RecSys     |  Real-time Alerts                    |
+--------------------+---------------------+---------------------+--------------------------------------+
```

### 2.2 Processing Stages Breakdown

1. **Ingestion (00:00 - 01:30 UTC)**:
   - Upstream transactional records (Subscribers, Content Catalog, Invoices) are extracted in parallel into HDFS staging using multi-mapper **Apache Sqoop** jobs.
   - High-throughput video telemetry logs from Kafka/Flume complete final end-of-day landing flushes.
2. **Cleansing & Sessionization via Apache Pig (01:30 - 02:15 UTC)**:
   - Reconstructs fragmented 10-second player heartbeat beacons into coherent, end-to-end user watch sessions.
   - Discards noise, invalid negative durations, clock-skewed future timestamps, and corrupt payloads.
   - Computes stream-level metrics: total watch time, total buffer duration, buffer count, seek frequency, and true completion percentage.
3. **Dimensional Updates & SCD Type 2 Merge via Hive on Tez (02:15 - 02:35 UTC)**:
   - Merges delta updates into dimension tables (`dim_user`, `dim_content`, `dim_device`).
   - Maintains historical validity tracking (`start_date`, `end_date`, `is_current`) for subscriber plan changes.
4. **Fact Table Partition Loading (02:35 - 03:00 UTC)**:
   - Resolves dimension natural keys to surrogate keys.
   - Inserts into partitioned ORC tables with bucket sorting.
5. **Data Quality Gatekeeper & Alerting (03:00 - 03:15 UTC)**:
   - Executes automated assertions (Row reconciliation, null checks, orphan detection, duplicate checks).
   - Fails the pipeline and halts downstream marts if anomalies exceed strict tolerance thresholds.
6. **Data Mart Aggregation & BI Refresh (03:15 - 03:45 UTC)**:
   - Pre-computes dimensional rollups and OLAP cubes for BI consumption.

---

## 3. Dimensional Data Model (Star Schema)

The enterprise warehouse uses Kimball dimensional modeling principles. Data is organized into clean star schemas centered around specific business measurement events.

### 3.1 Entity-Relationship Diagram (Star Schema)

```
                            +----------------------------------+
                            |            dim_date              |
                            +----------------------------------+
                            | PK  date_key (INT: YYYYMMDD)     |
                            |     calendar_date (DATE)         |
                            |     day_of_week (VARCHAR)        |
                            |     month_name (VARCHAR)         |
                            |     quarter (TINYINT)            |
                            |     year (SMALLINT)              |
                            |     is_weekend (BOOLEAN)         |
                            |     is_holiday (BOOLEAN)         |
                            +-----------------+----------------+
                                              |
                                              | 1:N
                                              v
+-------------------------------+   +------------------------------------+   +----------------------------------+
|           dim_user            |   |       fact_viewing_activity        |   |           dim_content            |
+-------------------------------+   +------------------------------------+   +----------------------------------+
| PK  user_key (BIGINT)         |<--| FK  user_key (BIGINT)              |-->| PK  content_key (BIGINT)         |
|     user_id (BIGINT)          |   | FK  content_key (BIGINT)           |   |     content_id (BIGINT)          |
|     username (VARCHAR)        |   | FK  device_key (INT)               |   |     title (VARCHAR)              |
|     country (VARCHAR)         |   | FK  plan_key (TINYINT)             |   |     content_type (VARCHAR)       |
|     age_group (VARCHAR)       |   | FK  date_key (INT)                 |   |     genre (VARCHAR)              |
|     subscription_tier (VARCHAR)|  |     viewing_id (BIGINT)            |   |     release_year (SMALLINT)      |
|     effective_start_date (DATE)|  |     session_id (VARCHAR)           |   |     duration_sec (INT)           |
|     effective_end_date (DATE) |   |     stream_start_time (TIMESTAMP)  |   |     content_rating (VARCHAR)     |
|     is_current (BOOLEAN)      |   |     stream_end_time (TIMESTAMP)    |   |     studio (VARCHAR)             |
+-------------------------------+   |     watch_duration_sec (INT)       |   |     language (VARCHAR)           |
                                    |     completion_pct (DECIMAL(5,2))  |   |     series_title (VARCHAR)       |
+-------------------------------+   |     bitrate_kbps (INT)             |   |     season_num (TINYINT)         |
|          dim_device           |   |     buffer_count (SMALLINT)        |   |     episode_num (SMALLINT)       |
+-------------------------------+   |     buffer_duration_sec (INT)      |   +----------------------------------+
| PK  device_key (INT)          |<--|     pause_count (TINYINT)          |
|     device_id (INT)           |   |     seek_count (TINYINT)           |   +----------------------------------+
|     device_type (VARCHAR)     |   |     error_code (VARCHAR)           |   |      dim_subscription_plan       |
|     brand (VARCHAR)           |   |     resolution (VARCHAR)           |   +----------------------------------+
|     os_name (VARCHAR)         |   |     is_completed (CHAR(1))         |-->| PK  plan_key (TINYINT)           |
|     os_version (VARCHAR)      |   | PK* event_date (STRING - PARTITION)|   |     plan_id (TINYINT)            |
|     app_version (VARCHAR)     |   +------------------------------------+   |     plan_name (VARCHAR)          |
+-------------------------------+                                            |     monthly_price_usd (DECIMAL)  |
                                                                             |     max_streams (TINYINT)        |
                                                                             |     video_quality (VARCHAR)      |
                                                                             |     is_ad_supported (CHAR(1))    |
                                                                             +----------------------------------+
```

### 3.2 Granularity & Fact Design

| Fact Table Name | Grain | Fact Type | Description |
| :--- | :--- | :--- | :--- |
| `fact_viewing_activity` | 1 row per video playback stream session | Transaction Fact | Captures playback duration, QoS buffering metrics, completion rate, bitrate, and network error codes. |
| `fact_user_engagement` | 1 row per discrete subscriber interaction | Transaction Fact | Captures likes, dislikes, 5-star ratings, watchlist additions/removals, and social shares. |
| `fact_subscription_billing`| 1 row per financial transaction | Transaction Fact | Captures invoice generation, credit card charges, promotional discounts, and payment failures. |
| `fact_ad_performance` | 1 row per ad-break impression | Transaction Fact | Captures advertising telemetry: ad duration, view duration, completion flag, and CPM pricing. |

### 3.3 Slowly Changing Dimensions (SCD Strategy)

- **`dim_user` (SCD Type 2)**: Subscriber profile attributes such as subscription tier (`Basic`, `Standard`, `Premium`) and country of residence frequently change. Retaining history is critical to correctly attributing historical viewing behavior to the subscription plan active at the time of playback.
  - Surrogate Key: `user_key` (auto-incrementing / hash)
  - Business Key: `user_id`
  - Version Control: `effective_start_date`, `effective_end_date`, `is_current` (Flag `Y`/`N`).
- **`dim_content` (SCD Type 1)**: Metadata corrections (fixing typo in actor list, adjusting genre classification, updating studio distributor) overwrite existing attributes in place, as historical categorization is not required.
- **`dim_device` (SCD Type 1)**: Client hardware profiles, operating systems, and client application versions.

---

## 4. Technology Stack Justification & Trade-offs

| Component | Selected Technology | Alternative Evaluated | Engineering Justification |
| :--- | :--- | :--- | :--- |
| **Distributed Storage** | **Apache HDFS 3.x** | AWS S3 / Cloud Storage | **High-Throughput IO & Predictable Cost**: HDFS delivers maximum local disk sequential read/write bandwidth for multi-petabyte clusters without cloud egress fees or per-request API costs ($/PUT, $/GET). |
| **SQL Engine** | **Apache Hive on Tez / LLAP** | Traditional Hive on MapReduce | **50x Performance Acceleration**: Tez eliminates disk serialization between pipeline stages via memory-pipelined DAGs. LLAP adds persistent daemon executors and in-memory columnar caching. |
| **Batch Cleansing** | **Apache Pig** | MapReduce Java / Spark | **Declarative Telemetry Processing**: Pig Latin excels at tuple-oriented semi-structured parsing, cogrouping, session windowing, and anomaly filtering with concise, maintainable code compared to raw MapReduce. |
| **Relational Ingestion** | **Apache Sqoop** | Debezium / JDBC Scripts | **Parallel Bulk Throughput**: Sqoop binds directly to Hadoop input formats, allowing multiple parallel mappers to read partitioned database ranges directly into HDFS with zero intermediary bottlenecks. |
| **Storage Format** | **Apache ORC (ZLIB / Snappy)**| Apache Parquet / Text / AVRO | **Hive-Native Optimizations**: ORC is built specifically for Hive, featuring Stripe-level metadata, lightweight bloom filter indexes, vectorization primitives, and superior compression ratios (up to 75% space reduction). |
| **Workflow Scheduler**| **Apache Oozie** | Airflow / Cron | **Native Hadoop Security & SLA Support**: Oozie runs as an in-cluster service with native Kerberos delegation tokens, YARN queue integration, coordinator data-availability triggers, and robust kill/retry semantics. |

---

## 5. Data Governance, Security & Compliance

Enterprise media platforms manage sensitive subscriber data (names, billing details, viewing history) requiring strict governance controls under international privacy standards (GDPR, CCPA, PCI-DSS).

### 5.1 Access Control & Data Masking (Apache Ranger)

- **Role-Based Access Control (RBAC)**:
  - `Data Engineers`: Full DDL/DML access to Landing, Staging, and Curated layers.
  - `BI Analysts`: Read-only access restricted exclusively to Curated Warehouse (`media_dw`) and Gold Marts (`media_marts`).
  - `Data Scientists`: Read-only access with automatic dynamic masking applied to user identification columns.
- **Dynamic Column Masking**:
  - `dim_user.email`: Masked using SHA-256 hash or partial mask (`j***n@gmail.com`).
  - `dim_user.first_name` & `last_name`: Fully redacted for non-privileged query users.
- **Row-Level Filtering**:
  - Regional content licensing analysts can only query rows matching their territory (e.g., `WHERE country = 'US'`).

### 5.2 Lineage Tracking & Cataloging (Apache Atlas)

- Automated metadata hooks in Hive, Sqoop, and Oozie automatically publish lineage graphs to **Apache Atlas**.
- Provides comprehensive auditability from source PostgreSQL tables down to BI Tableau dashboards.
- Classifications applied: `PII`, `FINANCIAL`, `CONFIDENTIAL_CONTENT`, `ANALYTICS_PUBLIC`.

### 5.3 Regulatory Compliance: GDPR & CCPA "Right to Be Forgotten"

- Under GDPR Article 17, subscribers have the right to request erasure of personal data.
- **Hive ACID Merge / Compaction Strategy**:
  - HDFS files are inherently immutable. To facilitate individual record deletions without rewriting multi-terabyte tables, Hive ACID tables with delta compaction are implemented:
    ```sql
    DELETE FROM media_dw.dim_user WHERE user_id = :requested_user_id;
    UPDATE media_dw.fact_viewing_activity SET user_key = -1 WHERE user_key = :target_surrogate_key;
    ```
  - Anonymization maintains statistical aggregate integrity while permanently expunging subscriber linkage.

### 5.4 Encryption Standards

- **Data at Rest**: HDFS Transparent Data Encryption (TDE) utilizing Hadoop Key Management Server (KMS) with AES-256 encryption zones for all warehouse directories.
- **Data in Transit**: TLS 1.3 encryption across all intra-cluster RPC communication (DataNode-to-DataNode, NameNode RPC) and HTTPS/JDBC connections.

---

## 6. Scalability & Performance Optimization Strategies

To ensure deterministic daily runtimes across petabyte-scale datasets, the architecture implements multi-tier optimizations:

### 6.1 Partitioning & Bucketing Architecture

- **Partition Pruning by `event_date`**:
  - Fact tables are partitioned by calendar date (`event_date = 'YYYY-MM-DD'`).
  - Queries filtering on recent time windows (e.g., 7-day or 30-day trailing metrics) scan only relevant date directories, skipping 95%+ of stored data.
- **Bucketing & Sort-Merge-Bucket (SMB) Joins**:
  - Fact tables and high-cardinality dimensions (`dim_user`) are bucketed by `user_key` into **32 buckets** and pre-sorted:
    ```sql
    CLUSTERED BY (user_key) SORTED BY (user_key) INTO 32 BUCKETS
    ```
  - Enables Hive to perform **Bucket Map Joins** and **Sort-Merge-Bucket Joins**, executing joins directly within corresponding mapper buckets with zero shuffle-and-sort network overhead.

### 6.2 Hive Execution Engine Tuning

```properties
# Enable Tez DAG execution engine
set hive.execution.engine=tez;

# Vectorization: Process batches of 1024 rows simultaneously in CPU registers
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;

# Cost-Based Optimizer (CBO) enabled using Apache Calcite
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;

# Dynamic Partitioning Optimization
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=2000;

# Compression Codec
set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
```

### 6.3 Small-Files Problem Mitigation

Streaming ingestion and high partition counts can cause file fragmentation (millions of small 1-5 MB files overloading NameNode heap memory).
- **Consolidation at Output**:
  ```sql
  set hive.merge.mapfiles=true;
  set hive.merge.mapredfiles=true;
  set hive.merge.size.per.task=268435456; -- 256 MB target file size
  set hive.merge.smallfiles.avgsize=134217728; -- Merge if avg size < 128 MB
  ```
- **Automated Compaction Daemon**: Weekly Oozie cron task triggers `ALTER TABLE ... CONCATENATE` on ORC partitions.

---

## 7. High Availability & Disaster Recovery (DR)

### 7.1 Master Node High Availability

```
+----------------------------------------------------------------------------------------------------+
|                                 HIGH AVAILABILITY TOPOLOGY                                         |
+------------------------------------+----------------------------------+----------------------------+
|        Active NameNode             |        Standby NameNode          |    Apache ZooKeeper (3x)   |
|  - Serving client read/writes      |  - Synchronizing edit logs       |  - Automated failover ZKFC |
|  - In-memory filesystem metadata   |  - Warm standby ready in < 30s   |  - Cluster health consensus|
+-----------------+------------------+-----------------+----------------+-------------+--------------+
                  |                                    |                              |
                  +------------------+-----------------+                              |
                                     v                                                v
                    +------------------------------------+          +--------------------------------+
                    |    Quorum Journal Manager (QJM)    |          |    Hive Metastore Cluster      |
                    | (3x JournalNodes for edit logs)    |          | (Active/Active behind HAProxy) |
                    +------------------------------------+          +--------------------------------+
```

1. **HDFS NameNode HA**:
   - Active and Standby NameNodes coordinated via Quorum Journal Manager (QJM) across 3 JournalNodes.
   - ZooKeeper Failover Controller (ZKFC) provides automatic heartbeat monitoring and sub-second failover.
2. **Hive Metastore HA**:
   - Multiple stateless Hive Metastore daemon instances hosted behind an HAProxy software load balancer.
   - Backed by an Active/Passive relational database pair (PostgreSQL) with automated replication.
3. **YARN Resource Manager HA**:
   - Active/Standby ResourceManagers with state recovery persisted in ZooKeeper.

### 7.2 Disaster Recovery Strategy & SLA

- **Cross-Datacenter Asynchronous Mirroring via Apache DistCp**:
  - Critical raw landing data and curated warehouse tables are replicated to a secondary DR data center daily:
    ```bash
    hadoop distcp -update -diff -delete -strategy dynamic \
      hdfs://namenode01.corp.media.internal:8020/warehouse/tablespace/external/hive/media_dw.db/fact_viewing_activity/event_date=${DATE} \
      hdfs://namenode-dr.dr.media.internal:8020/warehouse/tablespace/external/hive/media_dw.db/fact_viewing_activity/event_date=${DATE}
    ```
- **Recovery Metrics**:
  - **Recovery Point Objective (RPO)**:
    - Master Dimensions & Financial Transactions: **1 hour**
    - Video Playback Telemetry: **24 hours**
  - **Recovery Time Objective (RTO)**: **4 hours** to resume full cluster ETL processing.
