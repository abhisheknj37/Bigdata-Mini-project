-- ==============================================================================
-- Script Name : transform_user_activity.pig
-- Description : Production Pig Latin ETL script to clean, validate, sessionize,
--               and aggregate raw media streaming user activity logs.
-- Data Flow   : HDFS Raw (/data/media_warehouse/raw/user_activity)
--               -> Quality Filtering (Null checks, invalid duration pruning)
--               -> 30-Minute Inactivity Window Sessionization
--               -> Session-Level Engagement Metrics Calculation
--               -> HDFS Processed (/data/media_warehouse/processed/user_sessions)
-- Engine      : Apache Pig (Tez / MapReduce execution mode)
-- Author      : Big Data Engineering Team
-- Version     : 2.1.0
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Job Runtime Configuration & Dynamic Defaults
-- ------------------------------------------------------------------------------
-- Set reducer parallelism according to input cluster sizing
SET default_parallel 20;

-- Optimize memory and intermediate spill handling
SET pig.cachedbag.memusage 0.2;
SET pig.exec.reducers.bytes.per.reducer 134217728; -- 128 MB per reducer
SET pig.user.cache.enabled true;

-- Default input, output, and dependency paths (overridable via -param)
%default INPUT_DIR  '/data/media_warehouse/raw/user_activity';
%default OUTPUT_DIR '/data/media_warehouse/processed/user_sessions';
%default ERROR_DIR  '/data/media_warehouse/raw/user_activity_corrupt';
%default PIGGYBANK_JAR '/usr/lib/pig/piggybank.jar';
%default DATAFU_JAR    '/usr/lib/pig/datafu.jar';
%default SESSION_INACTIVITY_TIMEOUT '30m'; -- 30 minutes inactivity window

-- ------------------------------------------------------------------------------
-- 2. UDF Registrations & Function Definitions
-- ------------------------------------------------------------------------------
-- Register Piggybank UDF collection for string manipulation and date parsing
REGISTER '$PIGGYBANK_JAR';
REGISTER '$DATAFU_JAR';

-- Define DataFu Sessionize UDF for dynamic session demarcation
-- Sessionize emits a unique session ID for sequences of events where time gap <= 30m
DEFINE Sessionize datafu.pig.sessions.Sessionize('$SESSION_INACTIVITY_TIMEOUT');

-- Define ISO date and string formatting helpers
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToUnix         org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

-- ------------------------------------------------------------------------------
-- 3. Ingestion: Load Raw Activity Logs from HDFS
-- ------------------------------------------------------------------------------
-- Raw activity format: TSV with structured schema emitted by streaming players & API gateways
raw_activity = LOAD '$INPUT_DIR' USING PigStorage('\t') AS (
    event_id:               chararray,
    user_id:                chararray,
    content_id:             chararray,
    event_type:             chararray,  -- PLAY, PAUSE, BUFFER, HEARTBEAT, STOP, SEEK
    event_time_str:         chararray,  -- ISO-8601 string: 2026-11-14T04:15:30Z
    playback_position_sec:  int,        -- Current playback offset in stream
    duration_sec:           int,        -- Active viewing duration reported in this event
    device_type:            chararray,  -- SMART_TV, MOBILE_IOS, MOBILE_ANDROID, WEB, ROKU
    ip_address:             chararray,  -- Client IPv4 / IPv6
    geo_country:            chararray,  -- ISO 2-letter country code
    geo_city:               chararray,  -- Client city
    bitrate_kbps:           int         -- Streaming video bitrate in kilobits/sec
);

-- ------------------------------------------------------------------------------
-- 4. Quality Filtering & Cleansing
-- ------------------------------------------------------------------------------
-- Rule 1: user_id must be non-null, non-empty, and not a literal 'null' string
-- Rule 2: duration_sec must be non-negative (>= 0)
-- Rule 3: event_time_str must be populated and non-empty
-- Rule 4: content_id must be valid
SPLIT raw_activity INTO 
    valid_records IF (
        user_id IS NOT NULL AND 
        TRIM(user_id) != '' AND 
        LOWER(TRIM(user_id)) != 'null' AND 
        event_time_str IS NOT NULL AND 
        TRIM(event_time_str) != '' AND
        content_id IS NOT NULL AND 
        TRIM(content_id) != '' AND
        duration_sec >= 0
    ),
    corrupt_records OTHERWISE;

-- Store bad / quarantined records to separate path for audit and alerting
STORE corrupt_records INTO '$ERROR_DIR' USING PigStorage('\t');

-- ------------------------------------------------------------------------------
-- 5. Timestamp Conversion & Pre-Session Preparation
-- ------------------------------------------------------------------------------
-- Convert ISO timestamps to epoch milliseconds for chronological windowing
prepared_activity = FOREACH valid_records GENERATE
    event_id,
    TRIM(user_id)                                           AS user_id: chararray,
    TRIM(content_id)                                        AS content_id: chararray,
    UPPER(TRIM(event_type))                                 AS event_type: chararray,
    -- Convert ISO date format into Unix Epoch milliseconds (long)
    ToUnixTime(ToDate(event_time_str, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')) * 1000L AS event_epoch_ms: long,
    event_time_str                                          AS event_time_str: chararray,
    (playback_position_sec >= 0 ? playback_position_sec : 0) AS playback_position_sec: int,
    duration_sec                                            AS duration_sec: int,
    UPPER(TRIM(device_type))                                AS device_type: chararray,
    geo_country                                             AS geo_country: chararray,
    geo_city                                                AS geo_city: chararray,
    (bitrate_kbps > 0 ? bitrate_kbps : 1500)                AS bitrate_kbps: int;

-- ------------------------------------------------------------------------------
-- 6. Sessionization (30-Minute Inactivity Boundary)
-- ------------------------------------------------------------------------------
-- Group events by user_id to chronological stream ordering
grouped_by_user = GROUP prepared_activity BY user_id;

-- Order each user's event stream chronologically and apply DataFu Sessionize
sessionized_stream = FOREACH grouped_by_user {
    sorted_events = ORDER prepared_activity BY event_epoch_ms ASC;
    -- DataFu Sessionize evaluates the timestamp delta between consecutive rows.
    -- If delta > 30 minutes, it generates a new session UUID.
    session_tuples = Sessionize(sorted_events);
    GENERATE FLATTEN(session_tuples) AS (
        event_id:               chararray,
        user_id:                chararray,
        content_id:             chararray,
        event_type:             chararray,
        event_epoch_ms:         long,
        event_time_str:         chararray,
        playback_position_sec:  int,
        duration_sec:           int,
        device_type:            chararray,
        geo_country:            chararray,
        geo_city:               chararray,
        bitrate_kbps:           int,
        session_id:             chararray
    );
};

-- ------------------------------------------------------------------------------
-- 7. Calculate Engagement Metrics Per Session
-- ------------------------------------------------------------------------------
-- Group all events belonging to the same (user_id, session_id)
grouped_sessions = GROUP sessionized_stream BY (user_id, session_id);

-- Compute granular KPIs per session
session_metrics = FOREACH grouped_sessions {
    -- Distinct content items viewed within this session
    distinct_contents = DISTINCT sessionized_stream.content_id;
    
    -- Filter specific playback lifecycle signals
    play_events = FILTER sessionized_stream BY event_type == 'PLAY' OR event_type == 'HEARTBEAT';
    buffer_events = FILTER sessionized_stream BY event_type == 'BUFFER';
    stop_events = FILTER sessionized_stream BY event_type == 'STOP';

    min_ts_ms = MIN(sessionized_stream.event_epoch_ms);
    max_ts_ms = MAX(sessionized_stream.event_epoch_ms);

    GENERATE
        group.user_id                                           AS user_id: chararray,
        group.session_id                                        AS session_id: chararray,
        ToString(ToDate(min_ts_ms), 'yyyy-MM-dd HH:mm:ss')      AS session_start_time: chararray,
        ToString(ToDate(max_ts_ms), 'yyyy-MM-dd HH:mm:ss')      AS session_end_time: chararray,
        -- Total wall clock duration of session in minutes
        (int)ROUND((float)(max_ts_ms - min_ts_ms) / 60000.0f)   AS wall_clock_duration_mins: int,
        -- Total aggregate viewing time consumed (seconds)
        SUM(sessionized_stream.duration_sec)                    AS total_watch_duration_sec: long,
        -- Total content items consumed
        COUNT(distinct_contents)                                AS unique_content_count: long,
        -- Total events registered
        COUNT(sessionized_stream)                               AS total_event_count: long,
        -- Play & Heartbeat count
        COUNT(play_events)                                      AS play_event_count: long,
        -- Quality indicator: Buffer event count
        COUNT(buffer_events)                                    AS buffer_event_count: long,
        -- Session completion indicator (did user reach STOP event?)
        (COUNT(stop_events) > 0 ? 1 : 0)                        AS clean_session_exit: int,
        -- Average quality bitrate in kbps
        (int)ROUND(AVG(sessionized_stream.bitrate_kbps))        AS avg_bitrate_kbps: int,
        -- Capture first device and location seen in session
        TOP(1, 0, sessionized_stream.device_type)               AS primary_device_bag,
        TOP(1, 0, sessionized_stream.geo_country)               AS primary_country_bag;
};

-- Flatten composite baggage fields for clean relational output
flattened_session_summary = FOREACH session_metrics GENERATE
    user_id,
    session_id,
    session_start_time,
    session_end_time,
    wall_clock_duration_mins,
    (total_watch_duration_sec IS NOT NULL ? total_watch_duration_sec : 0L) AS total_watch_duration_sec: long,
    unique_content_count,
    total_event_count,
    play_event_count,
    buffer_event_count,
    clean_session_exit,
    avg_bitrate_kbps,
    -- Extract the single string value from the bag
    FLATTEN(primary_device_bag)                                             AS primary_device: chararray,
    FLATTEN(primary_country_bag)                                            AS primary_country: chararray;

-- ------------------------------------------------------------------------------
-- 8. Persist Transformed Session Data to HDFS for Hive External Tables
-- ------------------------------------------------------------------------------
-- Output is stored as tab-delimited records matching media_warehouse.fact_user_sessions
STORE flattened_session_summary INTO '$OUTPUT_DIR' USING PigStorage('\t');

-- End of transform_user_activity.pig
