-- ============================================================================
-- SCRIPT: subscriber_analytics.hql
-- DESCRIPTION: Subscriber Lifecycle Intelligence, Churn Prediction Feature Store,
--              Customer Lifetime Value (CLV/LTV), Cohort Retention Matrix,
--              Plan Tier Migration Dynamics, and Regional Subscription Penetration.
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

USE media_dw;

-- ============================================================================
-- 1. CHURN PREDICTION FEATURE STORE
-- Generates an ML-ready feature extraction table for predictive churn models
-- (Logistic Regression, Gradient Boosted Trees, Random Forest).
-- Features incorporate Recency, Frequency, Monetary (RFM), QoE, and Drop-off velocity.
-- ============================================================================
WITH reference_snapshot AS (
    -- Reference baseline date for recency calculations
    SELECT CAST('2026-11-14' AS DATE) AS ref_date
),
user_streaming_features AS (
    SELECT
        s.user_key,
        -- Recency: Days elapsed since latest stream
        datediff(r.ref_date, MAX(d.full_date))                          AS days_since_last_stream,
        -- Frequency & Volume: Last 30 Days
        SUM(CASE WHEN datediff(r.ref_date, d.full_date) BETWEEN 0 AND 30 THEN 1 ELSE 0 END) AS streams_last_30d,
        ROUND(SUM(CASE WHEN datediff(r.ref_date, d.full_date) BETWEEN 0 AND 30 THEN s.duration_watched ELSE 0 END) / 3600.0, 2) AS watch_hours_last_30d,
        -- Frequency & Volume: Prior 30 Days (Days 31 to 60)
        SUM(CASE WHEN datediff(r.ref_date, d.full_date) BETWEEN 31 AND 60 THEN 1 ELSE 0 END) AS streams_prior_30d,
        ROUND(SUM(CASE WHEN datediff(r.ref_date, d.full_date) BETWEEN 31 AND 60 THEN s.duration_watched ELSE 0 END) / 3600.0, 2) AS watch_hours_prior_30d,
        -- Quality of Experience (QoE) Health: 30-Day Degradation Flags
        SUM(CASE WHEN datediff(r.ref_date, d.full_date) BETWEEN 0 AND 30 THEN s.buffering_events ELSE 0 END) AS buffer_stalls_30d,
        ROUND(AVG(CASE WHEN datediff(r.ref_date, d.full_date) BETWEEN 0 AND 30 THEN s.quality_score ELSE NULL END), 2) AS avg_qoe_score_30d,
        ROUND(AVG(CASE WHEN datediff(r.ref_date, d.full_date) BETWEEN 0 AND 30 THEN s.completion_pct ELSE NULL END), 2) AS avg_completion_pct_30d,
        -- Breadth of catalog engagement
        COUNT(DISTINCT CASE WHEN datediff(r.ref_date, d.full_date) BETWEEN 0 AND 30 THEN s.content_key ELSE NULL END) AS unique_titles_30d
    FROM media_dw.fact_content_streaming s
    INNER JOIN media_dw.dim_date d ON s.date_key = d.date_key
    CROSS JOIN reference_snapshot r
    WHERE s.user_key > 0
    GROUP BY s.user_key, r.ref_date
),
user_ad_exposure AS (
    SELECT
        ar.user_key,
        SUM(ar.impressions) AS total_ad_impressions_30d,
        SUM(ar.clicks)      AS total_ad_clicks_30d
    FROM media_dw.fact_ad_revenue ar
    INNER JOIN media_dw.dim_date d ON ar.date_key = d.date_key
    CROSS JOIN reference_snapshot r
    WHERE ar.user_key > 0
      AND datediff(r.ref_date, d.full_date) BETWEEN 0 AND 30
    GROUP BY ar.user_key
),
user_billing_tenure AS (
    SELECT
        u.user_key,
        u.user_id,
        u.age_group,
        u.gender,
        u.country,
        u.region,
        u.current_plan_type,
        u.subscription_status,
        u.preferred_device_type,
        u.payment_method,
        u.registration_date,
        datediff(r.ref_date, u.registration_date) AS tenure_days,
        CASE WHEN lower(u.subscription_status) IN ('cancelled', 'expired', 'past_due') THEN 1 ELSE 0 END AS is_churned_target
    FROM media_dw.dim_user u
    CROSS JOIN reference_snapshot r
    WHERE u.is_current = TRUE 
      AND u.user_key > 0
)
SELECT
    b.user_key,
    b.user_id,
    b.age_group,
    b.gender,
    b.country,
    b.region,
    b.current_plan_type,
    b.payment_method,
    b.preferred_device_type,
    b.tenure_days,
    -- Behavioral Features
    COALESCE(s.days_since_last_stream, 999)                 AS days_since_last_stream,
    COALESCE(s.streams_last_30d, 0)                        AS streams_last_30d,
    COALESCE(s.watch_hours_last_30d, 0.0)                  AS watch_hours_last_30d,
    COALESCE(s.streams_prior_30d, 0)                       AS streams_prior_30d,
    COALESCE(s.watch_hours_prior_30d, 0.0)                 AS watch_hours_prior_30d,
    -- Engagement Velocity: Ratio of recent watch time vs prior window (< 0.50 signifies steep drop-off risk)
    ROUND(
        COALESCE(s.watch_hours_last_30d, 0.0) / GREATEST(0.1, COALESCE(s.watch_hours_prior_30d, 0.0)),
        3
    ) AS consumption_velocity_ratio,
    COALESCE(s.buffer_stalls_30d, 0)                       AS buffer_stalls_30d,
    COALESCE(s.avg_qoe_score_30d, 3.00)                    AS avg_qoe_score_30d,
    COALESCE(s.avg_completion_pct_30d, 0.00)               AS avg_completion_pct_30d,
    COALESCE(s.unique_titles_30d, 0)                       AS unique_titles_30d,
    COALESCE(ad.total_ad_impressions_30d, 0)               AS total_ad_impressions_30d,
    -- Ground Truth ML Classification Label
    b.is_churned_target                                    AS label_churn
FROM user_billing_tenure b
LEFT JOIN user_streaming_features s ON b.user_key = s.user_key
LEFT JOIN user_ad_exposure ad ON b.user_key = ad.user_key
ORDER BY b.tenure_days DESC;


-- ============================================================================
-- 2. SUBSCRIBER LIFETIME VALUE (LTV) CALCULATION
-- Calculates total historical realized revenue (Subscription MRR + Net Ad Revenue)
-- and projects forward-looking Customer Lifetime Value based on cohort retention.
-- ============================================================================
WITH user_subscription_revenue AS (
    SELECT
        user_key,
        COUNT(DISTINCT date_key)        AS billing_cycles_paid,
        SUM(monthly_revenue)            AS total_subscription_revenue,
        AVG(monthly_revenue)            AS avg_monthly_subscription_spend
    FROM media_dw.fact_subscription
    WHERE user_key > 0
    GROUP BY user_key
),
user_ad_revenue AS (
    SELECT
        user_key,
        SUM(revenue)                    AS total_ad_revenue_generated
    FROM media_dw.fact_ad_revenue
    WHERE user_key > 0
    GROUP BY user_key
),
churn_rate_by_plan AS (
    -- Plan-level annual churn rate benchmark for projection
    SELECT
        current_plan_type,
        COUNT(user_key) AS total_users,
        SUM(CASE WHEN lower(subscription_status) = 'cancelled' THEN 1 ELSE 0 END) AS churned_users,
        GREATEST(0.02, CAST(SUM(CASE WHEN lower(subscription_status) = 'cancelled' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(user_key)) AS plan_churn_rate
    FROM media_dw.dim_user
    WHERE is_current = TRUE AND user_key > 0
    GROUP BY current_plan_type
)
SELECT
    u.user_id,
    u.current_plan_type,
    u.country,
    u.registration_date,
    COALESCE(sub.billing_cycles_paid, 0)                                AS billing_cycles_completed,
    ROUND(COALESCE(sub.total_subscription_revenue, 0.00), 2)            AS realized_subscription_revenue,
    ROUND(COALESCE(ad.total_ad_revenue_generated, 0.00), 2)             AS realized_ad_revenue,
    -- Total Historical Realized Customer Value
    ROUND(COALESCE(sub.total_subscription_revenue, 0.00) + COALESCE(ad.total_ad_revenue_generated, 0.00), 2) AS historical_ltv,
    -- Blended Monthly ARPU (Subscription Fee + Ad Earnings)
    ROUND(
        COALESCE(sub.avg_monthly_subscription_spend, 0.00) + 
        (COALESCE(ad.total_ad_revenue_generated, 0.00) / GREATEST(1, COALESCE(sub.billing_cycles_paid, 1))),
        2
    ) AS blended_monthly_arpu,
    -- Projected Remaining Customer Lifetime in Months: (1 / monthly_churn_rate)
    ROUND(1.0 / cp.plan_churn_rate, 1)                                  AS estimated_remaining_lifespan_months,
    -- Projected Lifetime Value: Historical LTV + (Monthly ARPU * Projected Months)
    ROUND(
        (COALESCE(sub.total_subscription_revenue, 0.00) + COALESCE(ad.total_ad_revenue_generated, 0.00)) +
        ((COALESCE(sub.avg_monthly_subscription_spend, 0.00) + 
          (COALESCE(ad.total_ad_revenue_generated, 0.00) / GREATEST(1, COALESCE(sub.billing_cycles_paid, 1)))) * (1.0 / cp.plan_churn_rate)),
        2
    ) AS projected_total_ltv,
    -- LTV Tier Segmentation
    CASE 
        WHEN (COALESCE(sub.total_subscription_revenue, 0.00) + COALESCE(ad.total_ad_revenue_generated, 0.00)) >= 500.00 THEN 'Diamond VIP'
        WHEN (COALESCE(sub.total_subscription_revenue, 0.00) + COALESCE(ad.total_ad_revenue_generated, 0.00)) >= 250.00 THEN 'Platinum Tier'
        WHEN (COALESCE(sub.total_subscription_revenue, 0.00) + COALESCE(ad.total_ad_revenue_generated, 0.00)) >= 100.00 THEN 'Gold Tier'
        ELSE 'Silver Tier'
    END AS customer_value_segment
FROM media_dw.dim_user u
LEFT JOIN user_subscription_revenue sub ON u.user_key = sub.user_key
LEFT JOIN user_ad_revenue ad ON u.user_key = ad.user_key
LEFT JOIN churn_rate_by_plan cp ON u.current_plan_type = cp.current_plan_type
WHERE u.is_current = TRUE AND u.user_key > 0
ORDER BY historical_ltv DESC
LIMIT 100;


-- ============================================================================
-- 3. COHORT RETENTION ANALYSIS (TRIANGULAR RETENTION MATRIX)
-- Tracks monthly subscriber acquisition cohorts across Month 0 to Month 12.
-- Computes the percentage of subscribers remaining active in subsequent months.
-- ============================================================================
WITH cohort_definition AS (
    SELECT
        user_key,
        date_format(registration_date, 'yyyy-MM') AS cohort_month
    FROM media_dw.dim_user
    WHERE is_current = TRUE AND user_key > 0
),
cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT user_key) AS initial_cohort_size
    FROM cohort_definition
    GROUP BY cohort_month
),
subscriber_monthly_activity AS (
    SELECT DISTINCT
        c.cohort_month,
        s.user_key,
        -- Calculate index of month relative to acquisition: Month 0, 1, 2, 3...
        CAST(
            (s.year - CAST(split(c.cohort_month, '-')[0] AS INT)) * 12 +
            (s.month - CAST(split(c.cohort_month, '-')[1] AS INT)) 
            AS INT
        ) AS month_number
    FROM media_dw.fact_subscription s
    INNER JOIN cohort_definition c ON s.user_key = c.user_key
    WHERE s.is_churned = 0 -- Only active / renewing subscriptions
      AND s.user_key > 0
),
cohort_matrix AS (
    SELECT
        c.cohort_month,
        cs.initial_cohort_size                                              AS month_0_users,
        COUNT(DISTINCT CASE WHEN a.month_number = 1 THEN a.user_key END)    AS month_1_users,
        COUNT(DISTINCT CASE WHEN a.month_number = 2 THEN a.user_key END)    AS month_2_users,
        COUNT(DISTINCT CASE WHEN a.month_number = 3 THEN a.user_key END)    AS month_3_users,
        COUNT(DISTINCT CASE WHEN a.month_number = 6 THEN a.user_key END)    AS month_6_users,
        COUNT(DISTINCT CASE WHEN a.month_number = 12 THEN a.user_key END)   AS month_12_users
    FROM cohort_definition c
    INNER JOIN cohort_sizes cs ON c.cohort_month = cs.cohort_month
    LEFT JOIN subscriber_monthly_activity a ON c.cohort_month = a.cohort_month AND c.user_key = a.user_key
    GROUP BY c.cohort_month, cs.initial_cohort_size
)
SELECT
    cohort_month,
    month_0_users,
    ROUND((month_1_users  / CAST(month_0_users AS DOUBLE)) * 100.0, 1) AS m1_retention_pct,
    ROUND((month_2_users  / CAST(month_0_users AS DOUBLE)) * 100.0, 1) AS m2_retention_pct,
    ROUND((month_3_users  / CAST(month_0_users AS DOUBLE)) * 100.0, 1) AS m3_retention_pct,
    ROUND((month_6_users  / CAST(month_0_users AS DOUBLE)) * 100.0, 1) AS m6_retention_pct,
    ROUND((month_12_users / CAST(month_0_users AS DOUBLE)) * 100.0, 1) AS m12_retention_pct
FROM cohort_matrix
ORDER BY cohort_month ASC;


-- ============================================================================
-- 4. PLAN UPGRADE / DOWNGRADE PATTERNS (TIER TRANSITION DYNAMICS)
-- Tracks transitions between subscription tiers (e.g., Ad_Supported -> Premium_4K)
-- and determines Net Migration Revenue Impact.
-- ============================================================================
WITH subscription_transitions AS (
    SELECT
        user_key,
        plan_type                                                       AS to_plan,
        monthly_revenue                                                 AS to_fee,
        date_key,
        LAG(plan_type) OVER (PARTITION BY user_key ORDER BY date_key ASC) AS from_plan,
        LAG(monthly_revenue) OVER (PARTITION BY user_key ORDER BY date_key ASC) AS from_fee
    FROM media_dw.fact_subscription
    WHERE user_key > 0
),
migration_events AS (
    SELECT
        from_plan,
        to_plan,
        from_fee,
        to_fee,
        CASE
            WHEN to_fee > from_fee THEN 'Upgrade'
            WHEN to_fee < from_fee THEN 'Downgrade'
            ELSE 'Lateral / Unchanged'
        END AS migration_type,
        (to_fee - from_fee) AS delta_mrr
    FROM subscription_transitions
    WHERE from_plan IS NOT NULL 
      AND from_plan != to_plan
)
SELECT
    from_plan                               AS previous_plan_tier,
    to_plan                                 AS new_plan_tier,
    migration_type,
    COUNT(*)                                AS total_migration_events,
    ROUND(SUM(delta_mrr), 2)                AS net_monthly_mrr_impact,
    ROUND(AVG(delta_mrr), 2)                AS avg_mrr_change_per_subscriber
FROM migration_events
GROUP BY 
    from_plan, 
    to_plan, 
    migration_type
ORDER BY 
    migration_type ASC, 
    total_migration_events DESC;


-- ============================================================================
-- 5. REGIONAL SUBSCRIPTION TRENDS
-- Global subscriber density, regional ARPU, active subscriber counts,
-- and tier popularity distribution across global territories.
-- ============================================================================
WITH regional_subscriber_snapshot AS (
    SELECT
        COALESCE(g.region, 'Unknown')   AS region,
        COALESCE(g.country, 'Unknown')  AS country,
        s.plan_type,
        COUNT(DISTINCT s.user_key)      AS active_subscribers,
        SUM(s.monthly_revenue)          AS total_monthly_revenue,
        SUM(s.is_new)                   AS new_subscriber_acquisitions,
        SUM(s.is_churned)               AS churned_subscribers
    FROM media_dw.fact_subscription s
    LEFT JOIN media_dw.dim_geography g ON s.geo_key = g.geo_key
    WHERE s.user_key > 0
    GROUP BY 
        COALESCE(g.region, 'Unknown'), 
        COALESCE(g.country, 'Unknown'), 
        s.plan_type
)
SELECT
    region,
    country,
    SUM(active_subscribers)                                         AS total_active_subscribers,
    ROUND(SUM(total_monthly_revenue), 2)                            AS total_regional_mrr,
    -- Regional ARPU (Average Revenue Per User)
    ROUND(SUM(total_monthly_revenue) / GREATEST(1, SUM(active_subscribers)), 2) AS regional_arpu,
    SUM(new_subscriber_acquisitions)                                AS total_new_acquisitions,
    SUM(churned_subscribers)                                        AS total_churns,
    -- Regional Net Churn Rate
    ROUND(
        (SUM(churned_subscribers) / CAST(GREATEST(1, SUM(active_subscribers) + SUM(churned_subscribers)) AS DOUBLE)) * 100.0, 
        2
    ) AS regional_churn_rate_pct,
    -- Tier Distribution Breakdown
    SUM(CASE WHEN plan_type = 'Ad_Supported' THEN active_subscribers ELSE 0 END) AS ad_supported_subscribers,
    SUM(CASE WHEN plan_type = 'Basic_SD'     THEN active_subscribers ELSE 0 END) AS basic_subscribers,
    SUM(CASE WHEN plan_type = 'Standard_HD'  THEN active_subscribers ELSE 0 END) AS standard_subscribers,
    SUM(CASE WHEN plan_type = 'Premium_4K'   THEN active_subscribers ELSE 0 END) AS premium_4k_subscribers,
    -- Premium Tier Penetration % (Standard_HD + Premium_4K)
    ROUND(
        (SUM(CASE WHEN plan_type IN ('Standard_HD', 'Premium_4K') THEN active_subscribers ELSE 0 END) / 
         CAST(GREATEST(1, SUM(active_subscribers)) AS DOUBLE)) * 100.0,
        2
    ) AS high_tier_penetration_pct
FROM regional_subscriber_snapshot
GROUP BY region, country
ORDER BY total_regional_mrr DESC, total_active_subscribers DESC;
