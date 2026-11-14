-- ============================================================================
-- SCRIPT: content_analytics.hql
-- DESCRIPTION: Executive & Operational Analytics for Media Content Performance,
--              Audience Engagement, Genre Trends, Co-viewing Recommendations,
--              Daypart Consumption, and Territory Revenue Optimization.
-- ARCHITECTURE: Layer 3 - Analytics & Business Intelligence Layer
-- ENGINE: Apache Tez with Vectorized Execution & Cost-Based Optimizer (CBO)
-- AUTHOR: Big Data Engineering Team
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Optimization Configuration for Analytical Queries
-- ----------------------------------------------------------------------------
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=128000000;
SET hive.groupby.skewindata=false;

USE media_dw;

-- ============================================================================
-- 1. TOP PERFORMING CONTENT BY VIEWS, REVENUE, AND COMPLETION RATE
-- Combines streaming engagement with commercial monetization.
-- Uses window ranking functions and a normalized Composite Content Score.
-- ============================================================================
WITH content_aggregates AS (
    SELECT
        c.content_key,
        c.content_id,
        c.title,
        c.genre,
        c.content_type,
        c.content_tier,
        c.production_cost,
        SUM(p.total_views)                                              AS total_views,
        SUM(p.unique_viewers)                                           AS total_unique_viewers,
        ROUND(SUM(p.total_watch_time_mins) / 60.0, 2)                   AS total_watch_hours,
        ROUND(AVG(p.avg_completion_rate), 2)                            AS avg_completion_pct,
        ROUND(AVG(p.avg_rating), 2)                                     AS avg_user_rating,
        ROUND(SUM(p.revenue_generated), 2)                              AS total_gross_revenue
    FROM media_dw.fact_content_performance p
    INNER JOIN media_dw.dim_content c 
        ON p.content_key = c.content_key
    WHERE c.content_key > 0 -- Exclude Unknown record
    GROUP BY 
        c.content_key, 
        c.content_id, 
        c.title, 
        c.genre, 
        c.content_type, 
        c.content_tier,
        c.production_cost
),
ranked_content AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY total_views DESC)           AS rank_by_views,
        DENSE_RANK() OVER (ORDER BY total_gross_revenue DESC)   AS rank_by_revenue,
        DENSE_RANK() OVER (ORDER BY avg_completion_pct DESC)    AS rank_by_completion,
        -- Percentile ranks (0.0 to 1.0) for score normalization
        PERCENT_RANK() OVER (ORDER BY total_views ASC)          AS p_views,
        PERCENT_RANK() OVER (ORDER BY total_gross_revenue ASC)  AS p_revenue,
        PERCENT_RANK() OVER (ORDER BY avg_completion_pct ASC)   AS p_completion
    FROM content_aggregates
)
SELECT
    title,
    genre,
    content_type,
    content_tier,
    total_views,
    total_unique_viewers,
    total_watch_hours,
    avg_completion_pct,
    total_gross_revenue,
    rank_by_views,
    rank_by_revenue,
    rank_by_completion,
    -- Composite Index: 40% Views Weight, 40% Monetization Weight, 20% Retention/Completion Weight
    ROUND(((0.40 * p_views) + (0.40 * p_revenue) + (0.20 * p_completion)) * 100, 2) AS composite_performance_index,
    CASE 
        WHEN production_cost > 0 THEN ROUND(((total_gross_revenue - production_cost) / production_cost) * 100.0, 2)
        ELSE NULL
    END AS net_roi_percentage
FROM ranked_content
ORDER BY composite_performance_index DESC
LIMIT 50;


-- ============================================================================
-- 2. GENRE TREND ANALYSIS OVER TIME
-- Tracks monthly viewership trajectory, watch time market share, and MoM growth.
-- Utilizes LAG window functions to evaluate shifting audience tastes.
-- ============================================================================
WITH monthly_genre_stats AS (
    SELECT
        c.genre,
        d.year,
        d.month,
        d.month_name,
        COUNT(s.stream_key)                                     AS total_streams,
        COUNT(DISTINCT s.user_key)                              AS unique_streamers,
        ROUND(SUM(s.duration_watched) / 3600.0, 2)              AS total_stream_hours,
        ROUND(AVG(s.completion_pct), 2)                         AS avg_genre_completion,
        ROUND(AVG(s.quality_score), 2)                          AS avg_qoe_score
    FROM media_dw.fact_content_streaming s
    INNER JOIN media_dw.dim_content c 
        ON s.content_key = c.content_key
    INNER JOIN media_dw.dim_date d 
        ON s.date_key = d.date_key
    WHERE c.genre IS NOT NULL AND c.genre != 'Unknown'
    GROUP BY 
        c.genre, 
        d.year, 
        d.month, 
        d.month_name
),
genre_market_share AS (
    SELECT
        genre,
        year,
        month,
        month_name,
        total_streams,
        unique_streamers,
        total_stream_hours,
        avg_genre_completion,
        -- Share of total platform viewing hours in that month
        ROUND(
            (total_stream_hours / SUM(total_stream_hours) OVER (PARTITION BY year, month)) * 100.0, 
            2
        ) AS monthly_watch_time_share_pct,
        -- Month-over-Month (MoM) Stream volume growth tracking
        LAG(total_streams, 1) OVER (PARTITION BY genre ORDER BY year, month) AS prev_month_streams,
        LAG(total_stream_hours, 1) OVER (PARTITION BY genre ORDER BY year, month) AS prev_month_hours
    FROM monthly_genre_stats
)
SELECT
    genre,
    year,
    month,
    month_name,
    total_streams,
    unique_streamers,
    total_stream_hours,
    monthly_watch_time_share_pct,
    avg_genre_completion,
    prev_month_streams,
    CASE 
        WHEN prev_month_streams IS NOT NULL AND prev_month_streams > 0 
        THEN ROUND(((total_streams - prev_month_streams) / CAST(prev_month_streams AS DOUBLE)) * 100.0, 2)
        ELSE 0.00
    END AS stream_volume_mom_growth_pct,
    CASE 
        WHEN prev_month_hours IS NOT NULL AND prev_month_hours > 0 
        THEN ROUND(((total_stream_hours - prev_month_hours) / CAST(prev_month_hours AS DOUBLE)) * 100.0, 2)
        ELSE 0.00
    END AS watch_hours_mom_growth_pct
FROM genre_market_share
ORDER BY genre ASC, year ASC, month ASC;


-- ============================================================================
-- 3. CONTENT RECOMMENDATION SCORING (COLLABORATIVE FILTERING CO-VIEWING)
-- Calculates pairwise item affinity: "Users who watched Content A also completed Content B"
-- Computes the Jaccard Similarity Index blended with Completion Quality.
-- ============================================================================
WITH qualified_views AS (
    -- Filter high-intent views (completion >= 40% or watch time > 15 mins)
    SELECT DISTINCT
        s.user_key,
        s.content_key,
        c.title,
        c.genre
    FROM media_dw.fact_content_streaming s
    INNER JOIN media_dw.dim_content c ON s.content_key = c.content_key
    WHERE s.completion_pct >= 40.00 
      AND s.content_key > 0
),
content_audience_size AS (
    SELECT
        content_key,
        COUNT(DISTINCT user_key) AS audience_size
    FROM qualified_views
    GROUP BY content_key
),
co_viewing_pairs AS (
    SELECT
        v1.content_key  AS content_a_key,
        v1.title        AS content_a_title,
        v1.genre        AS content_a_genre,
        v2.content_key  AS content_b_key,
        v2.title        AS content_b_title,
        v2.genre        AS content_b_genre,
        COUNT(DISTINCT v1.user_key) AS shared_viewers
    FROM qualified_views v1
    INNER JOIN qualified_views v2 
        ON v1.user_key = v2.user_key 
       AND v1.content_key < v2.content_key -- Avoid duplicate symmetric pairs (A,B) vs (B,A)
    GROUP BY 
        v1.content_key, v1.title, v1.genre,
        v2.content_key, v2.title, v2.genre
    HAVING COUNT(DISTINCT v1.user_key) >= 5 -- Minimum support threshold for statistical validity
)
SELECT
    p.content_a_title,
    p.content_a_genre,
    p.content_b_title,
    p.content_b_genre,
    p.shared_viewers,
    s1.audience_size AS audience_a,
    s2.audience_size AS audience_b,
    -- Jaccard Similarity Coefficient: |A ∩ B| / |A ∪ B|
    ROUND(
        CAST(p.shared_viewers AS DOUBLE) / (s1.audience_size + s2.audience_size - p.shared_viewers),
        4
    ) AS jaccard_similarity_coefficient,
    -- Recommendation Confidence Score boosted if genres align
    ROUND(
        (CAST(p.shared_viewers AS DOUBLE) / (s1.audience_size + s2.audience_size - p.shared_viewers)) 
        * (CASE WHEN p.content_a_genre = p.content_b_genre THEN 1.25 ELSE 1.00 END) * 100.0,
        2
    ) AS recommendation_affinity_score
FROM co_viewing_pairs p
INNER JOIN content_audience_size s1 ON p.content_a_key = s1.content_key
INNER JOIN content_audience_size s2 ON p.content_b_key = s2.content_key
ORDER BY recommendation_affinity_score DESC
LIMIT 100;


-- ============================================================================
-- 4. PEAK VIEWING HOURS ANALYSIS (DAYPART CONCURRENCY & QUALITY AUDIT)
-- Evaluates hourly platform load, Prime Time surge vs Late Night,
-- and QoS (Buffering & Degradation) across Weekdays vs Weekends.
-- ============================================================================
SELECT
    t.hour,
    t.time_band,
    t.is_prime_time,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_category,
    COUNT(s.stream_key)                                     AS total_stream_sessions,
    COUNT(DISTINCT s.user_key)                              AS concurrent_unique_users,
    ROUND(SUM(s.duration_watched) / 3600.0, 2)              AS total_stream_hours,
    ROUND(AVG(s.duration_watched) / 60.0, 2)                AS avg_session_length_mins,
    SUM(s.buffering_events)                                 AS total_buffer_stalls,
    ROUND(
        (SUM(CASE WHEN s.buffering_events > 0 THEN 1 ELSE 0 END) / CAST(COUNT(s.stream_key) AS DOUBLE)) * 100.0,
        2
    ) AS sessions_with_stalls_pct,
    ROUND(AVG(s.quality_score), 2)                          AS avg_qoe_score,
    ROUND(AVG(s.bitrate_kbps), 0)                           AS avg_bandwidth_consumed_kbps
FROM media_dw.fact_content_streaming s
INNER JOIN media_dw.dim_time t 
    ON s.time_key = t.time_key
INNER JOIN media_dw.dim_date d 
    ON s.date_key = d.date_key
GROUP BY 
    t.hour, 
    t.time_band, 
    t.is_prime_time,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END
ORDER BY 
    day_category ASC, 
    t.hour ASC;


-- ============================================================================
-- 5. REVENUE PER CONTENT PER REGION
-- Regional commercial monetization breakdown joining AVOD ad revenues,
-- regional viewership density, and production cost amortization.
-- ============================================================================
WITH regional_ad_monetization AS (
    SELECT
        ar.content_key,
        COALESCE(g.region, 'Unknown')   AS region,
        COALESCE(g.country, 'Unknown')  AS country,
        SUM(ar.impressions)             AS total_ad_impressions,
        SUM(ar.clicks)                  AS total_ad_clicks,
        ROUND(SUM(ar.revenue), 2)       AS total_ad_revenue,
        ROUND((SUM(ar.revenue) / GREATEST(1, SUM(ar.impressions))) * 1000.0, 2) AS effective_cpm,
        ROUND((SUM(ar.clicks) / CAST(GREATEST(1, SUM(ar.impressions)) AS DOUBLE)) * 100.0, 2) AS ctr_pct
    FROM media_dw.fact_ad_revenue ar
    LEFT JOIN media_dw.dim_geography g 
        ON ar.geo_key = g.geo_key
    GROUP BY 
        ar.content_key, 
        COALESCE(g.region, 'Unknown'), 
        COALESCE(g.country, 'Unknown')
),
regional_streaming_stats AS (
    SELECT
        s.content_key,
        COALESCE(g.region, 'Unknown')   AS region,
        COALESCE(g.country, 'Unknown')  AS country,
        COUNT(s.stream_key)             AS total_views,
        COUNT(DISTINCT s.user_key)      AS unique_viewers,
        ROUND(SUM(s.duration_watched) / 3600.0, 2) AS total_watch_hours
    FROM media_dw.fact_content_streaming s
    LEFT JOIN media_dw.dim_geography g 
        ON s.geo_key = g.geo_key
    GROUP BY 
        s.content_key, 
        COALESCE(g.region, 'Unknown'), 
        COALESCE(g.country, 'Unknown')
)
SELECT
    c.title,
    c.genre,
    c.content_type,
    c.studio,
    st.region,
    st.country,
    COALESCE(st.total_views, 0)         AS total_views,
    COALESCE(st.unique_viewers, 0)      AS unique_viewers,
    COALESCE(st.total_watch_hours, 0)   AS total_watch_hours,
    COALESCE(ad.total_ad_impressions, 0) AS total_ad_impressions,
    COALESCE(ad.total_ad_revenue, 0.00) AS total_ad_revenue,
    COALESCE(ad.effective_cpm, 0.00)    AS effective_cpm,
    COALESCE(ad.ctr_pct, 0.00)          AS ctr_pct,
    -- Revenue per 1,000 Stream Hours (RPH)
    ROUND(
        CASE WHEN COALESCE(st.total_watch_hours, 0) > 0 
             THEN (COALESCE(ad.total_ad_revenue, 0.00) / st.total_watch_hours) * 1000.0
             ELSE 0.00 
        END, 
        2
    ) AS revenue_per_thousand_watch_hours
FROM media_dw.dim_content c
INNER JOIN regional_streaming_stats st 
    ON c.content_key = st.content_key
LEFT JOIN regional_ad_monetization ad 
    ON st.content_key = ad.content_key 
   AND st.region = ad.region 
   AND st.country = ad.country
WHERE c.content_key > 0
ORDER BY total_ad_revenue DESC, total_views DESC
LIMIT 100;
