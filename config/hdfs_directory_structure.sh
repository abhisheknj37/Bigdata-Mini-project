#!/usr/bin/env bash
# ==============================================================================
# Script Name   : hdfs_directory_structure.sh
# Description   : Idempotent initialization script to provision the production
#                 HDFS multi-tiered directory structure for the Media &
#                 Entertainment Big Data Warehouse. Configures hierarchical paths,
#                 POSIX permissions, ownership, and HDFS tiered storage policies.
# Target Layers : /data/media_warehouse/{raw, staging, processed, dimensions, facts, analytics, archive}
# Author        : Big Data Engineering Team
# Version       : 2.1.0
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# 1. Environment & Configurable Variables
# ------------------------------------------------------------------------------
TIMESTAMP="$(date +'%Y-%m-%d %H:%M:%S')"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"

# Cluster Identity & Ownership (Default service user & group)
WAREHOUSE_USER="${WAREHOUSE_USER:-media_etl}"
WAREHOUSE_GROUP="${WAREHOUSE_GROUP:-hadoop}"

# Root Warehouse Namespace
HDFS_WAREHOUSE_ROOT="/data/media_warehouse"

# Flag controls
DRY_RUN=false
APPLY_STORAGE_POLICIES=true

# ------------------------------------------------------------------------------
# 2. Logging Helpers
# ------------------------------------------------------------------------------
log_info()  { echo -e "[${TIMESTAMP}] [INFO ] $*"; }
log_warn()  { echo -e "[${TIMESTAMP}] [WARN ] $*"; }
log_error() { echo -e "[${TIMESTAMP}] [ERROR] $*" >&2; }

# ------------------------------------------------------------------------------
# 3. CLI Argument Parsing
# ------------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: ${SCRIPT_NAME} [OPTIONS]

Options:
  --user <username>       HDFS owner username (default: ${WAREHOUSE_USER})
  --group <groupname>     HDFS primary group (default: ${WAREHOUSE_GROUP})
  --skip-policies         Skip setting HDFS tiered storage policies
  --dry-run               Preview directory operations without mutating HDFS
  -h, --help              Show this help message

Examples:
  ./${SCRIPT_NAME}
  ./${SCRIPT_NAME} --user hdfs --group supergroup
  ./${SCRIPT_NAME} --dry-run
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)
            WAREHOUSE_USER="$2"
            shift 2
            ;;
        --group)
            WAREHOUSE_GROUP="$2"
            shift 2
            ;;
        --skip-policies)
            APPLY_STORAGE_POLICIES=false
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_error "Unknown parameter: $1"
            usage
            ;;
    esac
done

# ------------------------------------------------------------------------------
# 4. Pre-requisite Checks
# ------------------------------------------------------------------------------
log_info "Verifying HDFS connectivity and client tools..."
if ! command -v hdfs &>/dev/null; then
    log_error "HDFS CLI binary 'hdfs' not detected in system PATH. Please initialize Hadoop environment."
    exit 2
fi

# Verify NameNode is accessible and not stuck in safemode
if [ "${DRY_RUN}" = false ]; then
    SAFEMODE_STATUS="$(hdfs dfsadmin -safemode get 2>/dev/null || echo "UNKNOWN")"
    log_info "HDFS NameNode Safemode check: ${SAFEMODE_STATUS}"
    if echo "${SAFEMODE_STATUS}" | grep -qi "ON"; then
        log_error "NameNode is currently in SAFEMODE. Cannot provision directory structure."
        exit 3
    fi
fi

# ------------------------------------------------------------------------------
# 5. Directory Topology Definition
# ------------------------------------------------------------------------------
# Format: <HDFS_PATH> | <PERMISSIONS> | <STORAGE_POLICY> | <DESCRIPTION>
DIRECTORY_DEFINITIONS=(
    # Root Namespace
    "${HDFS_WAREHOUSE_ROOT}|755|HOT|Root media warehouse directory"

    # Zone 1: Raw Ingestion Zone (Unmodified feeds from Kafka, logs, partner dumps)
    "${HDFS_WAREHOUSE_ROOT}/raw|770|HOT|Raw ingestion root"
    "${HDFS_WAREHOUSE_ROOT}/raw/user_activity|770|HOT|Raw streaming player events"
    "${HDFS_WAREHOUSE_ROOT}/raw/ad_impressions|770|HOT|Raw advertising logs"
    "${HDFS_WAREHOUSE_ROOT}/raw/subscriptions|770|HOT|Raw billing and subscription events"
    "${HDFS_WAREHOUSE_ROOT}/raw/content_catalog|770|HOT|Raw catalog dumps from MySQL"
    "${HDFS_WAREHOUSE_ROOT}/raw/third_party_ratings|770|HOT|External IMDB/RottenTomatoes ratings"

    # Zone 2: Staging Zone (Sqoop landing & transient ETL delta tables)
    "${HDFS_WAREHOUSE_ROOT}/staging|770|HOT|Transient ETL staging root"
    "${HDFS_WAREHOUSE_ROOT}/staging/content_catalog|770|HOT|Sqoop delta landing for catalog"
    "${HDFS_WAREHOUSE_ROOT}/staging/subscriptions|770|HOT|Sqoop delta landing for subscriptions"
    "${HDFS_WAREHOUSE_ROOT}/staging/ad_impressions|770|HOT|Sqoop delta landing for ad impressions"

    # Zone 3: Processed Zone (Cleansed, validated, sessionized datasets)
    "${HDFS_WAREHOUSE_ROOT}/processed|775|HOT|Cleansed processed datasets"
    "${HDFS_WAREHOUSE_ROOT}/processed/user_sessions|775|HOT|Sessionized user viewing streams"
    "${HDFS_WAREHOUSE_ROOT}/processed/content_views|775|HOT|Cleaned viewing duration events"
    "${HDFS_WAREHOUSE_ROOT}/processed/sanitized_ad_logs|775|HOT|Verified ad impression metrics"

    # Zone 4: Dimensions Layer (Slowly Changing Dimensions & Reference Data)
    "${HDFS_WAREHOUSE_ROOT}/dimensions|755|HOT|Dimensional tables root"
    "${HDFS_WAREHOUSE_ROOT}/dimensions/dim_content|755|HOT|Content metadata dimension (movies, series)"
    "${HDFS_WAREHOUSE_ROOT}/dimensions/dim_user|755|HOT|User profile & subscriber dimension"
    "${HDFS_WAREHOUSE_ROOT}/dimensions/dim_device|755|HOT|Device taxonomy dimension"
    "${HDFS_WAREHOUSE_ROOT}/dimensions/dim_advertiser|755|HOT|Advertiser & brand account dimension"
    "${HDFS_WAREHOUSE_ROOT}/dimensions/dim_date|755|HOT|Enterprise date & holiday calendar dimension"
    "${HDFS_WAREHOUSE_ROOT}/dimensions/dim_subscription_plan|755|HOT|Subscription tier & pricing dimension"

    # Zone 5: Facts Layer (Enterprise Partitioned ORC/Parquet Fact Tables)
    "${HDFS_WAREHOUSE_ROOT}/facts|755|HOT|Fact tables root"
    "${HDFS_WAREHOUSE_ROOT}/facts/fact_content_views|755|HOT|Granular content consumption fact table"
    "${HDFS_WAREHOUSE_ROOT}/facts/fact_ad_impressions|755|HOT|Ad placement and impression fact table"
    "${HDFS_WAREHOUSE_ROOT}/facts/fact_subscriptions|755|HOT|Subscription lifecycle fact table"
    "${HDFS_WAREHOUSE_ROOT}/facts/fact_daily_engagement|755|HOT|Aggregated daily subscriber engagement"

    # Zone 6: Analytics & Data Science Layer (Derived feature stores, similarities, trending)
    "${HDFS_WAREHOUSE_ROOT}/analytics|755|HOT|Analytics and ML marts root"
    "${HDFS_WAREHOUSE_ROOT}/analytics/content_daily_metrics|755|HOT|Daily rollup metrics by content"
    "${HDFS_WAREHOUSE_ROOT}/analytics/content_trending|755|HOT|Recency-weighted viral content ranks"
    "${HDFS_WAREHOUSE_ROOT}/analytics/content_similarity|755|HOT|Co-viewing collaborative Jaccard matrices"
    "${HDFS_WAREHOUSE_ROOT}/analytics/revenue_reports|755|HOT|Monetization and eCPM performance marts"
    "${HDFS_WAREHOUSE_ROOT}/analytics/churn_features|755|HOT|Subscriber churn prediction feature store"

    # Zone 7: Cold Storage & Long-Term Archival (Cold Tier / Erasure Coding)
    "${HDFS_WAREHOUSE_ROOT}/archive|750|COLD|Cold tier archive root"
    "${HDFS_WAREHOUSE_ROOT}/archive/cold_logs|750|COLD|Historical raw activity archives (> 90 days)"
    "${HDFS_WAREHOUSE_ROOT}/archive/historical_snapshots|750|COLD|Annual database backup snapshots"
)

# ------------------------------------------------------------------------------
# 6. Execution: Directory Creation, Permissions, Ownership & Storage Policies
# ------------------------------------------------------------------------------
log_info "Initiating deployment of Media Warehouse HDFS directory architecture..."
log_info "Target Ownership: ${WAREHOUSE_USER}:${WAREHOUSE_GROUP}"

TOTAL_DIRS=${#DIRECTORY_DEFINITIONS[@]}
CREATED_COUNT=0

for entry in "${DIRECTORY_DEFINITIONS[@]}"; do
    IFS="|" read -r dir_path permissions storage_policy description <<< "${entry}"

    log_info "Processing [${dir_path}] - ${description}"

    if [ "${DRY_RUN}" = true ]; then
        log_info "  [DRY RUN] Would create: hdfs dfs -mkdir -p ${dir_path}"
        log_info "  [DRY RUN] Would chmod : hdfs dfs -chmod ${permissions} ${dir_path}"
        log_info "  [DRY RUN] Would chown : hdfs dfs -chown ${WAREHOUSE_USER}:${WAREHOUSE_GROUP} ${dir_path}"
        if [ "${APPLY_STORAGE_POLICIES}" = true ]; then
            log_info "  [DRY RUN] Would policy: hdfs storagepolicies -setStoragePolicy -path ${dir_path} -policy ${storage_policy}"
        fi
        continue
    fi

    # 1. Idempotent directory creation
    if ! hdfs dfs -test -d "${dir_path}" 2>/dev/null; then
        hdfs dfs -mkdir -p "${dir_path}"
        log_info "  Directory created: ${dir_path}"
    else
        log_info "  Directory already exists: ${dir_path}"
    fi

    # 2. Apply explicit POSIX permissions
    hdfs dfs -chmod "${permissions}" "${dir_path}"

    # 3. Apply service account ownership
    hdfs dfs -chown "${WAREHOUSE_USER}:${WAREHOUSE_GROUP}" "${dir_path}"

    # 4. Apply Tiered Storage Policy (HOT / COLD / WARM)
    if [ "${APPLY_STORAGE_POLICIES}" = true ]; then
        # Check if storage policy tool is available on this Hadoop cluster
        if command -v hdfs storagepolicies &>/dev/null || hdfs storagepolicies -help &>/dev/null; then
            hdfs storagepolicies -setStoragePolicy -path "${dir_path}" -policy "${storage_policy}" 2>/dev/null || {
                log_warn "  Could not assign storage policy '${storage_policy}' to ${dir_path} (Storage policy might require admin privilege or custom pool)"
            }
        fi
    fi

    CREATED_COUNT=$((CREATED_COUNT + 1))
done

# ------------------------------------------------------------------------------
# 7. Verification Summary
# ------------------------------------------------------------------------------
log_info "=========================================================================="
log_info "HDFS Directory Provisioning Summary"
log_info "=========================================================================="
log_info "Total directories processed : ${TOTAL_DIRS}"
if [ "${DRY_RUN}" = false ]; then
    log_info "Verification listing for ${HDFS_WAREHOUSE_ROOT}:"
    hdfs dfs -ls -R "${HDFS_WAREHOUSE_ROOT}" | head -n 30 || true
    log_info "Media warehouse HDFS layout initialization completed successfully."
else
    log_info "DRY RUN completed. No file system changes were applied."
fi

exit 0
