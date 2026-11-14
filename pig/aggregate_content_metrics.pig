-- ==============================================================================
-- Script Name : aggregate_content_metrics.pig
-- Description : Production Apache Pig ETL pipeline for Media & Entertainment:
--               1. Multi-temporal Content Consumption Aggregations (Daily/Weekly)
--               2. Recency-Weighted Content Trending Score Engine
--               3. Co-Viewing Collaborative Content Similarity Matrix (Jaccard)
-- Target Layers: HDFS Analytics Layer (/data/media_warehouse/analytics/...)
-- Author      : Big Data Engineering Team
-- Version     : 2.1.0
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Cluster Optimization & Performance Tunings
-- ------------------------------------------------------------------------------
SET default_parallel 30;
SET pig.exec.reducers.bytes.per.reducer 134217728; -- 128 MB per reducer
SET pig.cachedbag.memusage 0.25;
SET pig.skewedjoin.reduce.memusage 0.3;
SET pig.user.cache.enabled true;

-- Input & Output path defaults (overridable via Pig -param)
%default INPUT_DIR           '/data/media_warehouse/processed/content_views';
%default OUTPUT_DAILY_DIR   '/data/media_warehouse/analytics/content_daily_metrics';
%default OUTPUT_TRENDING_DIR '/data/media_warehouse/analytics/content_trending';
%default OUTPUT_SIMILARITY_DIR '/data/media_warehouse/analytics/content_similarity';
%default PIGGYBANK_JAR       '/usr/lib/pig/piggybank.jar';
%default DATAFU_JAR          '/usr/lib/pig/datafu.jar';
%default CURRENT_DATE_STR    '2026-11-14'; -- Anchor analysis date YYYY-MM-DD

-- ------------------------------------------------------------------------------
-- 2. UDF Registration
-- ------------------------------------------------------------------------------
REGISTER '$PIGGYBANK_JAR';
REGISTER '$DATAFU_JAR';

-- Date parsing & math UDFs from Piggybank
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE DaysBetween       org.apache.pig.piggybank.evaluation.datetime.interval.DaysBetween();
DEFINE ToDate            org.apache.pig.builtin.ToDate();

-- ------------------------------------------------------------------------------
-- 3. Load Processed Content Consumption Events
-- ------------------------------------------------------------------------------
-- Event dataset produced by upstream staging & sessionization jobs
content_views = LOAD '$INPUT_DIR' USING PigStorage('\t') AS (
    view_id:             chararray,
    user_id:             chararray,
    content_id:          chararray,
    content_type:        chararray,  -- MOVIE, EPISODE, LIVE_SPORTS, CLIP
    genre:               chararray,  -- ACTION, DRAMA, SCI_FI, COMEDY, etc.
    view_date:           chararray,  -- YYYY-MM-DD
    view_timestamp_ms:   long,       -- Epoch milliseconds
    watch_duration_sec:  long,       -- Duration streamed in seconds
    content_runtime_sec: long,       -- Full asset runtime in seconds
    completion_rate:     double,     -- watch_duration_sec / content_runtime_sec (0.0 to 1.0)
    device_category:     chararray   -- CONNECTED_TV, MOBILE, TABLET, DESKTOP
);

-- Prune anomalous or corrupt viewing records
valid_views = FILTER content_views BY
    content_id IS NOT NULL AND TRIM(content_id) != '' AND
    user_id IS NOT NULL AND TRIM(user_id) != '' AND
    watch_duration_sec > 0 AND
    view_date IS NOT NULL AND TRIM(view_date) != '';

-- ==============================================================================
-- PART 1: DAILY CONTENT CONSUMPTION AGGREGATIONS
-- ==============================================================================
-- Group by (content_id, view_date) to compute temporal rollups
grouped_daily_content = GROUP valid_views BY (content_id, view_date);

content_daily_metrics = FOREACH grouped_daily_content {
    distinct_users = DISTINCT valid_views.user_id;
    completed_views = FILTER valid_views BY completion_rate >= 0.90;
    tv_views = FILTER valid_views BY device_category == 'CONNECTED_TV';
    mobile_views = FILTER valid_views BY device_category == 'MOBILE';

    GENERATE
        group.content_id                                            AS content_id: chararray,
        group.view_date                                             AS view_date: chararray,
        COUNT(valid_views)                                          AS total_view_count: long,
        COUNT(distinct_users)                                       AS unique_viewer_count: long,
        SUM(valid_views.watch_duration_sec)                         AS total_watch_seconds: long,
        ROUND((double)SUM(valid_views.watch_duration_sec) / 3600.0) AS total_watch_hours: double,
        ROUND(AVG(valid_views.completion_rate) * 100.0) / 100.0    AS avg_completion_rate: double,
        COUNT(completed_views)                                      AS completed_view_count: long,
        COUNT(tv_views)                                             AS connected_tv_views: long,
        COUNT(mobile_views)                                         AS mobile_views: long;
};

-- Persist daily summary metrics to Data Warehouse HDFS analytics zone
STORE content_daily_metrics INTO '$OUTPUT_DAILY_DIR' USING PigStorage('\t');

-- ==============================================================================
-- PART 2: RECENCY-WEIGHTED CONTENT TRENDING SCORE ENGINE
-- ==============================================================================
-- Trending algorithm incorporates:
-- 1. Volume of unique viewers within a 7-day rolling window
-- 2. Exponential recency decay weights:
--    - Day 0 (today)     : Weight = 1.00
--    - Day 1 (yesterday) : Weight = 0.80
--    - Day 2             : Weight = 0.60
--    - Day 3             : Weight = 0.45
--    - Day 4             : Weight = 0.30
--    - Day 5-6           : Weight = 0.15
-- 3. Completion multiplier reward for high-retention content

current_date_obj = ToDate('$CURRENT_DATE_STR', 'yyyy-MM-dd');

-- Calculate day distance and apply recency weight per view
weighted_views = FOREACH valid_views {
    view_date_obj = ToDate(view_date, 'yyyy-MM-dd');
    days_ago = DaysBetween(current_date_obj, view_date_obj);
    
    -- Compute decaying weight factor based on age in days
    recency_weight = (
        days_ago == 0L ? 1.00 :
        days_ago == 1L ? 0.80 :
        days_ago == 2L ? 0.60 :
        days_ago == 3L ? 0.45 :
        days_ago == 4L ? 0.30 :
        days_ago <= 6L ? 0.15 : 0.00
    );

    -- Higher weight for views that were watched to completion
    retention_multiplier = (completion_rate >= 0.80 ? 1.25 : (completion_rate >= 0.50 ? 1.00 : 0.75));

    GENERATE
        content_id,
        content_type,
        genre,
        days_ago,
        (recency_weight * retention_multiplier) AS weighted_score: double;
};

-- Filter only events within the active 7-day trending horizon
recent_window_views = FILTER weighted_views BY days_ago >= 0L AND days_ago <= 6L;

-- Aggregate trending score per content item
grouped_trending = GROUP recent_window_views BY (content_id, content_type, genre);

trending_scores = FOREACH grouped_trending GENERATE
    group.content_id                            AS content_id: chararray,
    group.content_type                          AS content_type: chararray,
    group.genre                                 AS genre: chararray,
    COUNT(recent_window_views)                  AS total_recent_views: long,
    ROUND(SUM(recent_window_views.weighted_score) * 100.0) / 100.0 AS raw_trending_score: double,
    '$CURRENT_DATE_STR'                         AS calculation_date: chararray;

-- Order content descending by trending score to identify top viral/popular media
ranked_trending = ORDER trending_scores BY raw_trending_score DESC;

-- Store trending results to Data Warehouse HDFS analytics zone
STORE ranked_trending INTO '$OUTPUT_TRENDING_DIR' USING PigStorage('\t');

-- ==============================================================================
-- PART 3: CO-VIEWING COLLABORATIVE CONTENT SIMILARITY MATRIX
-- ==============================================================================
-- Formula: Jaccard Similarity Coefficient = |Viewers(A) ∩ Viewers(B)| / |Viewers(A) ∪ Viewers(B)|
--                                         = CoViewers(A, B) / (Viewers(A) + Viewers(B) - CoViewers(A, B))

-- Step 3a: Extract distinct (user, content) pairs to prevent multi-view bias
user_content_pairs = DISTINCT (
    FOREACH valid_views GENERATE
        user_id,
        content_id
);

-- Step 3b: Precompute total unique viewer count per content item (Denominator term)
grouped_by_content = GROUP user_content_pairs BY content_id;
content_viewer_counts = FOREACH grouped_by_content GENERATE
    group                               AS content_id: chararray,
    COUNT(user_content_pairs)           AS total_viewers: long;

-- Prune tail assets with fewer than 5 viewers to maintain matrix density & performance
eligible_content = FILTER content_viewer_counts BY total_viewers >= 5L;

filtered_user_views = JOIN 
    user_content_pairs BY content_id, 
    eligible_content BY content_id USING 'replicated';

filtered_user_content = FOREACH filtered_user_views GENERATE
    user_content_pairs::user_id    AS user_id: chararray,
    user_content_pairs::content_id AS content_id: chararray;

-- Step 3c: Self-join on user_id with strict ordering (content_A < content_B)
-- This eliminates identical pairs (A, A) and avoids symmetric duplicate computation (B, A)
views_A = FOREACH filtered_user_content GENERATE user_id, content_id AS content_a;
views_B = FOREACH filtered_user_content GENERATE user_id, content_id AS content_b;

co_view_candidates = JOIN views_A BY user_id, views_B BY user_id;

strictly_ordered_pairs = FILTER co_view_candidates BY content_a < content_b;

-- Step 3d: Count intersection of co-viewing audience |A ∩ B|
grouped_pairs = GROUP strictly_ordered_pairs BY (content_a, content_b);

co_view_counts = FOREACH grouped_pairs GENERATE
    group.content_a               AS content_a: chararray,
    group.content_b               AS content_b: chararray,
    COUNT(strictly_ordered_pairs) AS co_view_count: long;

-- Filter pairs with at least 3 co-viewers to prune noise
significant_pairs = FILTER co_view_counts BY co_view_count >= 3L;

-- Step 3e: Join with content_viewer_counts for Content A and Content B
pair_with_cnt_a = JOIN 
    significant_pairs BY content_a, 
    content_viewer_counts BY content_id USING 'replicated';

flat_pair_a = FOREACH pair_with_cnt_a GENERATE
    significant_pairs::content_a     AS content_a: chararray,
    significant_pairs::content_b     AS content_b: chararray,
    significant_pairs::co_view_count AS co_view_count: long,
    content_viewer_counts::total_viewers AS viewers_a: long;

pair_with_both = JOIN 
    flat_pair_a BY content_b, 
    content_viewer_counts BY content_id USING 'replicated';

-- Step 3f: Calculate final Jaccard Similarity index
similarity_matrix = FOREACH pair_with_both {
    coviews = flat_pair_a::co_view_count;
    total_a = flat_pair_a::viewers_a;
    total_b = content_viewer_counts::total_viewers;
    union_viewers = (double)(total_a + total_b - coviews);
    jaccard_score = (union_viewers > 0.0 ? (double)coviews / union_viewers : 0.0);

    GENERATE
        flat_pair_a::content_a                                AS content_a: chararray,
        flat_pair_a::content_b                                AS content_b: chararray,
        coviews                                               AS co_view_count: long,
        total_a                                               AS viewers_a: long,
        total_b                                               AS viewers_b: long,
        ROUND(jaccard_score * 10000.0) / 10000.0              AS jaccard_similarity: double,
        '$CURRENT_DATE_STR'                                   AS generated_date: chararray;
};

-- Filter high-affinity relationships (Jaccard >= 0.05) and store matrix
high_affinity_pairs = FILTER similarity_matrix BY jaccard_similarity >= 0.05;

STORE high_affinity_pairs INTO '$OUTPUT_SIMILARITY_DIR' USING PigStorage('\t');

-- End of aggregate_content_metrics.pig
