"""
Local SQL Runner & Warehouse Simulator for Media & Entertainment.
Executes the staging ingestion, dimensional models, fact loading,
and analytics queries directly on Windows using SQLite.
"""

import sqlite3
import csv
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "sample")
DB_PATH = os.path.join(BASE_DIR, "data", "media_warehouse.db")

def init_db():
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except Exception:
            pass
    conn = sqlite3.connect(DB_PATH)
    return conn

def load_csv(conn, table_name, file_name):
    path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(path):
        print(f"[WARN] File not found: {path}")
        return
    
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        cols = ", ".join([f'"{h}" TEXT' for h in headers])
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" ({cols})')
        
        placeholders = ", ".join(["?"] * len(headers))
        conn.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', reader)
    conn.commit()
    count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    print(f"[LOADED] {table_name:30} -> {count:5} records")

def run_simulation():
    print("=" * 70)
    print("Media & Entertainment Data Warehouse — Local Pipeline Execution")
    print("=" * 70)

    conn = init_db()
    
    print("\n>>> Phase 1: Ingesting Staging Datasets into Warehouse...")
    load_csv(conn, "stg_content_catalog", "staging_content.csv")
    load_csv(conn, "stg_viewing_activity", "staging_viewing_activity.csv")
    load_csv(conn, "stg_subscription_billing", "staging_subscription_billing.csv")
    load_csv(conn, "stg_users", "staging_users.csv")
    load_csv(conn, "stg_devices", "staging_devices.csv")
    load_csv(conn, "stg_plans", "staging_subscription_plans.csv")

    print("\n>>> Phase 2: Building Dimensional Models (SCD Type 2 & Conformed)...")
    
    conn.executescript("""
    -- Dimension: Content (SCD Type 2 with active flags)
    DROP TABLE IF EXISTS dim_content;
    CREATE TABLE dim_content AS
    SELECT 
        ROW_NUMBER() OVER(ORDER BY content_id) AS content_key,
        content_id,
        title,
        genre,
        content_type,
        CAST(duration_sec AS INTEGER) / 60 AS duration_minutes,
        studio,
        content_rating,
        release_year,
        '2026-01-01' AS effective_date,
        '9999-12-31' AS expiry_date,
        1 AS is_current
    FROM stg_content_catalog;

    -- Dimension: User (SCD Type 2)
    DROP TABLE IF EXISTS dim_user;
    CREATE TABLE dim_user AS
    SELECT 
        ROW_NUMBER() OVER(ORDER BY u.user_id) AS user_key,
        u.user_id,
        u.first_name || ' ' || u.last_name AS full_name,
        u.country,
        u.age_group,
        p.plan_name AS subscription_tier,
        u.status,
        '2026-01-01' AS effective_date,
        '9999-12-31' AS expiry_date,
        1 AS is_current
    FROM stg_users u
    LEFT JOIN stg_plans p ON u.subscription_plan_id = p.plan_id;

    -- Dimension: Device
    DROP TABLE IF EXISTS dim_device;
    CREATE TABLE dim_device AS
    SELECT 
        ROW_NUMBER() OVER(ORDER BY device_id) AS device_key,
        device_id,
        device_type AS device_category,
        brand,
        os_name,
        os_version
    FROM stg_devices;

    -- Dimension: Date
    DROP TABLE IF EXISTS dim_date;
    CREATE TABLE dim_date AS
    SELECT DISTINCT
        CAST(strftime('%Y%m%d', stream_start_time) AS INTEGER) AS date_key,
        date(stream_start_time) AS full_date,
        CAST(strftime('%Y', stream_start_time) AS INTEGER) AS year,
        CAST(strftime('%m', stream_start_time) AS INTEGER) AS month,
        CAST(strftime('%d', stream_start_time) AS INTEGER) AS day
    FROM stg_viewing_activity
    WHERE stream_start_time IS NOT NULL;
    """)
    conn.commit()
    print("[SUCCESS] Created and populated: dim_content, dim_user, dim_device, dim_date")

    print("\n>>> Phase 3: Building Fact Tables (Star Schema Joins & QoS Scores)...")
    conn.executescript("""
    DROP TABLE IF EXISTS fact_content_streaming;
    CREATE TABLE fact_content_streaming AS
    SELECT 
        ROW_NUMBER() OVER(ORDER BY v.viewing_id) AS stream_key,
        COALESCE(c.content_key, -1) AS content_key,
        COALESCE(u.user_key, -1) AS user_key,
        COALESCE(d.date_key, -1) AS date_key,
        COALESCE(dv.device_key, -1) AS device_key,
        v.viewing_id,
        v.session_id,
        v.user_id,
        v.content_id,
        ROUND(CAST(v.watch_duration_sec AS FLOAT) / 60.0, 2) AS duration_watched_minutes,
        CAST(v.completion_pct AS FLOAT) AS completion_percentage,
        CAST(v.buffer_count AS INTEGER) AS buffering_events,
        CAST(v.buffer_duration_sec AS INTEGER) AS buffer_duration_sec,
        CAST(v.bitrate_kbps AS INTEGER) AS bitrate_kbps,
        ROUND(MAX(1.0, 5.0 - (CAST(v.buffer_count AS FLOAT) * 0.8) - (CAST(v.buffer_duration_sec AS FLOAT) / 30.0)), 2) AS quality_score,
        strftime('%Y', v.stream_start_time) AS year,
        strftime('%m', v.stream_start_time) AS month
    FROM stg_viewing_activity v
    LEFT JOIN dim_content c ON v.content_id = c.content_id AND c.is_current = 1
    LEFT JOIN dim_user u ON v.user_id = u.user_id AND u.is_current = 1
    LEFT JOIN dim_device dv ON v.device_id = dv.device_id
    LEFT JOIN dim_date d ON date(v.stream_start_time) = d.full_date;
    """)
    conn.commit()
    
    fact_cnt = conn.execute("SELECT COUNT(*) FROM fact_content_streaming").fetchone()[0]
    print(f"[SUCCESS] Loaded fact_content_streaming with {fact_cnt} transactions.")

    print("\n" + "=" * 70)
    print(">>> Phase 4: Executing Analytics & Business Intelligence Queries")
    print("=" * 70)

    # Analytics Query 1: Top Performing Content by Views & Watch Hours
    print("\n--- [Report 1] Top 5 Most-Watched Content Titles ---")
    query1 = """
    SELECT 
        c.title,
        c.genre,
        c.studio,
        COUNT(f.stream_key) AS total_views,
        ROUND(SUM(f.duration_watched_minutes)/60.0, 2) AS total_watch_hours,
        ROUND(AVG(f.completion_percentage), 1) AS avg_completion_pct,
        ROUND(AVG(f.quality_score), 2) AS avg_qoe_score
    FROM fact_content_streaming f
    JOIN dim_content c ON f.content_key = c.content_key
    GROUP BY c.title, c.genre, c.studio
    ORDER BY total_views DESC, total_watch_hours DESC
    LIMIT 5;
    """
    cur = conn.cursor()
    cur.execute(query1)
    headers = ["Title", "Genre", "Studio", "Views", "Watch (Hrs)", "Avg Compl %", "QoE Score"]
    row_format = "{:<30} {:<12} {:<20} {:<8} {:<12} {:<12} {:<10}"
    print(row_format.format(*headers))
    print("-" * 110)
    for row in cur.fetchall():
        print(row_format.format(str(row[0])[:28], str(row[1])[:10], str(row[2])[:18], str(row[3]), str(row[4]), str(row[5]), str(row[6])))

    # Analytics Query 2: Viewership by Genre & Device Category
    print("\n--- [Report 2] Viewership & Engagement by Genre ---")
    query2 = """
    SELECT 
        c.genre,
        COUNT(f.stream_key) AS stream_count,
        ROUND(AVG(f.duration_watched_minutes), 1) AS avg_duration_mins,
        ROUND(AVG(f.completion_percentage), 1) AS avg_completion_pct,
        SUM(f.buffering_events) AS total_buffering_incidents
    FROM fact_content_streaming f
    JOIN dim_content c ON f.content_key = c.content_key
    GROUP BY c.genre
    ORDER BY stream_count DESC;
    """
    cur.execute(query2)
    headers2 = ["Genre", "Stream Count", "Avg Mins", "Avg Compl %", "Buffer Incidents"]
    row_format2 = "{:<18} {:<14} {:<10} {:<14} {:<16}"
    print(row_format2.format(*headers2))
    print("-" * 75)
    for row in cur.fetchall():
        print(row_format2.format(str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4])))

    # Analytics Query 3: Subscriber Churn & Quality of Experience (QoE) Analysis
    print("\n--- [Report 3] Quality of Experience (QoE) by Subscription Tier ---")
    query3 = """
    SELECT 
        u.subscription_tier,
        COUNT(DISTINCT u.user_id) AS active_subscribers,
        COUNT(f.stream_key) AS total_streams,
        ROUND(AVG(f.quality_score), 2) AS avg_qoe_score,
        ROUND(AVG(f.buffering_events), 2) AS avg_buffering_per_stream
    FROM fact_content_streaming f
    JOIN dim_user u ON f.user_key = u.user_key
    GROUP BY u.subscription_tier
    ORDER BY total_streams DESC;
    """
    cur.execute(query3)
    headers3 = ["Subscription Tier", "Active Users", "Total Streams", "Avg QoE", "Avg Buffering"]
    row_format3 = "{:<25} {:<14} {:<14} {:<10} {:<14}"
    print(row_format3.format(*headers3))
    print("-" * 80)
    for row in cur.fetchall():
        print(row_format3.format(str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4])))

    print("\n" + "=" * 70)
    print("Warehouse Simulation Complete! Output saved to: data/media_warehouse.db")
    print("=" * 70)
    conn.close()

if __name__ == "__main__":
    run_simulation()
