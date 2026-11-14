#!/usr/bin/env bash
# ==============================================================================
# Script Name   : import_subscriptions.sh
# Description   : Production Apache Sqoop ETL script to import subscription
#                 lifecycle records (plans, billing, renewals, cancellations)
#                 from MySQL RDBMS into HDFS staging and Hive data warehouse.
# Strategy      : Incremental import using 'lastmodified' mode on updated_at
# Target Layers : HDFS Staging (/data/media_warehouse/staging/subscriptions)
#                 Hive Staging Table (media_warehouse.stg_subscriptions)
# Author        : Big Data Engineering Team
# Version       : 2.1.0
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# 1. Environment & Global Configuration
# ------------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
TIMESTAMP="$(date +'%Y%m%d_%H%M%S')"

# Logging and watermark state directories
LOG_DIR="${SCRIPT_DIR}/../logs"
STATE_DIR="${SCRIPT_DIR}/../state"
mkdir -p "${LOG_DIR}" "${STATE_DIR}"

LOG_FILE="${LOG_DIR}/${SCRIPT_NAME%.sh}_${TIMESTAMP}.log"
WATERMARK_FILE="${STATE_DIR}/subscriptions.watermark"

# RDBMS Connection Parameters (Overridable via environment variables)
DB_HOST="${DB_HOST:-rdbms-host}"
DB_PORT="${DB_PORT:-3306}"
DB_NAME="${DB_NAME:-media_db}"
DB_USER="${DB_USER:-media_sqoop_user}"
JDBC_URL="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}?useSSL=false&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull"
SOURCE_TABLE="subscriptions"

# Security: Password file located in secure storage (HDFS / Local)
PASSWORD_FILE="${PASSWORD_FILE:-/user/media_etl/.passwords/mysql_media.pass}"

# Target Storage Configuration
HDFS_BASE_DIR="/data/media_warehouse/staging/subscriptions"
HDFS_TARGET_DIR="${HDFS_BASE_DIR}/delta_${TIMESTAMP}"
HIVE_DB="${HIVE_DB:-media_warehouse}"
HIVE_TABLE="${HIVE_TABLE:-stg_subscriptions}"

# Concurrency & Ingestion Parameters
NUM_MAPPERS="${NUM_MAPPERS:-4}"
SPLIT_BY_COL="subscription_id"
CHECK_COLUMN="updated_at"
FETCH_SIZE="10000"
DEFAULT_LAST_VALUE="1970-01-01 00:00:00"

# ------------------------------------------------------------------------------
# 2. Logging & Trap Handling
# ------------------------------------------------------------------------------
log() {
    local level="$1"
    shift
    local msg="$*"
    local formatted="[$(date +'%Y-%m-%d %H:%M:%S')] [${level}] ${msg}"
    echo -e "${formatted}" | tee -a "${LOG_FILE}"
}

log_info()  { log "INFO " "$*"; }
log_warn()  { log "WARN " "$*"; }
log_error() { log "ERROR" "$*" >&2; }

cleanup() {
    local exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
        log_error "Pipeline failed with exit code: ${exit_code}."
    else
        log_info "Pipeline finished successfully."
    fi
}
trap cleanup EXIT ERR

# ------------------------------------------------------------------------------
# 3. CLI Argument Handling
# ------------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: ${SCRIPT_NAME} [OPTIONS]

Options:
  --last-value <timestamp>    Explicit timestamp cutoff (format: 'YYYY-MM-DD HH:MM:SS')
  --mappers <int>             Number of parallel map tasks (default: ${NUM_MAPPERS})
  --full-reload               Perform full table reload instead of incremental delta
  --dry-run                   Print resolved execution command without running
  -h, --help                  Display usage details

Examples:
  ./${SCRIPT_NAME}
  ./${SCRIPT_NAME} --last-value "2026-03-01 00:00:00"
  ./${SCRIPT_NAME} --mappers 6
EOF
    exit 1
}

MANUAL_LAST_VALUE=""
FULL_RELOAD=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --last-value)
            MANUAL_LAST_VALUE="$2"
            shift 2
            ;;
        --mappers)
            NUM_MAPPERS="$2"
            shift 2
            ;;
        --full-reload)
            FULL_RELOAD=true
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
            log_error "Unrecognized parameter: $1"
            usage
            ;;
    esac
done

# ------------------------------------------------------------------------------
# 4. Pre-requisite Checks
# ------------------------------------------------------------------------------
log_info "Performing environment verification..."

if ! command -v sqoop &>/dev/null; then
    if [[ -n "${SQOOP_HOME:-}" && -x "${SQOOP_HOME}/bin/sqoop" ]]; then
        export PATH="${SQOOP_HOME}/bin:${PATH}"
    else
        log_error "Sqoop utility not accessible. Check SQOOP_HOME and PATH."
        exit 2
    fi
fi

if ! command -v hdfs &>/dev/null; then
    log_error "HDFS client binary not found in PATH."
    exit 3
fi

# Ensure target HDFS directory exists
if ! hdfs dfs -test -d "${HDFS_BASE_DIR}" 2>/dev/null; then
    log_info "Creating base HDFS staging path: ${HDFS_BASE_DIR}"
    hdfs dfs -mkdir -p "${HDFS_BASE_DIR}"
fi

# ------------------------------------------------------------------------------
# 5. Incremental Watermark Determination
# ------------------------------------------------------------------------------
if [ "${FULL_RELOAD}" = true ]; then
    log_warn "FULL RELOAD initiated. Existing state watermark will be bypassed."
    IMPORT_MODE="full"
    LAST_VALUE=""
    HDFS_TARGET_DIR="${HDFS_BASE_DIR}/full_${TIMESTAMP}"
elif [ -n "${MANUAL_LAST_VALUE}" ]; then
    log_info "Using manually passed watermark: ${MANUAL_LAST_VALUE}"
    IMPORT_MODE="lastmodified"
    LAST_VALUE="${MANUAL_LAST_VALUE}"
elif [ -f "${WATERMARK_FILE}" ]; then
    LAST_VALUE="$(cat "${WATERMARK_FILE}" | tr -d '\r\n')"
    log_info "Read persisted watermark: ${LAST_VALUE}"
    IMPORT_MODE="lastmodified"
else
    log_warn "Watermark state file (${WATERMARK_FILE}) not found. Falling back to default: ${DEFAULT_LAST_VALUE}"
    LAST_VALUE="${DEFAULT_LAST_VALUE}"
    IMPORT_MODE="lastmodified"
fi

RUN_TIMESTAMP="$(date +'%Y-%m-%d %H:%M:%S')"

# ------------------------------------------------------------------------------
# 6. Build Sqoop Import Command with Column Type Mappings
# ------------------------------------------------------------------------------
log_info "Preparing Sqoop arguments for [${SOURCE_TABLE}] table ingestion..."

SQOOP_CMD=(
    sqoop import
    -Dmapreduce.job.user.classpath.first=true
    -Dorg.apache.sqoop.splitter.allow_text_splitter=true
    -Dmapreduce.output.fileoutputformat.compress=true
    -Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
    --connect "${JDBC_URL}"
    --username "${DB_USER}"
    --password-file "${PASSWORD_FILE}"
    --table "${SOURCE_TABLE}"
    --target-dir "${HDFS_TARGET_DIR}"
    --num-mappers "${NUM_MAPPERS}"
    --split-by "${SPLIT_BY_COL}"
    --fetch-size "${FETCH_SIZE}"
    --map-column-java "billing_amount=Double,discount_rate=Double"
    --as-parquetfile
    --compress
    --compression-codec snappy
    --null-string '\\N'
    --null-non-string '\\N'
    --fields-terminated-by '\001'
    --lines-terminated-by '\n'
    --hive-import
    --hive-database "${HIVE_DB}"
    --hive-table "${HIVE_TABLE}"
    --hive-drop-import-delims
)

if [ "${IMPORT_MODE}" == "lastmodified" ]; then
    SQOOP_CMD+=(
        --incremental lastmodified
        --check-column "${CHECK_COLUMN}"
        --last-value "${LAST_VALUE}"
        --append
    )
else
    SQOOP_CMD+=(
        --hive-overwrite
    )
fi

# ------------------------------------------------------------------------------
# 7. Job Execution
# ------------------------------------------------------------------------------
log_info "Target JDBC: ${JDBC_URL}"
log_info "Mappers: ${NUM_MAPPERS} | Split-by: ${SPLIT_BY_COL}"
log_info "HDFS Destination: ${HDFS_TARGET_DIR}"
log_info "Hive Target: ${HIVE_DB}.${HIVE_TABLE}"
[ "${IMPORT_MODE}" == "lastmodified" ] && log_info "Incremental criteria: ${CHECK_COLUMN} > ${LAST_VALUE}"

if [ "${DRY_RUN}" = true ]; then
    log_info "DRY RUN MODE. Command preview:"
    echo "${SQOOP_CMD[@]}"
    exit 0
fi

log_info "Executing Sqoop subscriptions ingestion..."
START_TIME=$(date +%s)

if "${SQOOP_CMD[@]}" 2>&1 | tee -a "${LOG_FILE}"; then
    DURATION=$(( $(date +%s) - START_TIME ))
    log_info "Subscription data import completed successfully in ${DURATION}s."

    # --------------------------------------------------------------------------
    # 8. Post-Import Auditing & Watermark Update
    # --------------------------------------------------------------------------
    echo "${RUN_TIMESTAMP}" > "${WATERMARK_FILE}"
    log_info "Updated watermark state file to: ${RUN_TIMESTAMP}"

    # Verify HDFS directory
    FILE_COUNT="$(hdfs dfs -count "${HDFS_TARGET_DIR}" 2>/dev/null | awk '{print $2}' || echo "N/A")"
    log_info "Output files written in HDFS: ${FILE_COUNT}"

    # Compute Hive statistics
    log_info "Refreshing Hive metadata statistics on ${HIVE_DB}.${HIVE_TABLE}..."
    hive -e "ANALYZE TABLE ${HIVE_DB}.${HIVE_TABLE} COMPUTE STATISTICS FOR COLUMNS;" 2>>"${LOG_FILE}" || {
        log_warn "Hive column statistics gathering emitted warnings."
    }

    log_info "Subscription import pipeline finalized successfully."
    exit 0
else
    STATUS=$?
    log_error "Sqoop import failed with exit code: ${STATUS}. Refer to log: ${LOG_FILE}"
    exit ${STATUS}
fi
