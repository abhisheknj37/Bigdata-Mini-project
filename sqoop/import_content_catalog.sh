#!/usr/bin/env bash
# ==============================================================================
# Script Name   : import_content_catalog.sh
# Description   : Production Apache Sqoop ETL script to import content catalog
#                 metadata from MySQL RDBMS into HDFS staging and Hive data
#                 warehouse for Media & Entertainment platform.
# Strategy      : Incremental import using 'lastmodified' mode on updated_at
# Target Layers : HDFS Staging (/data/media_warehouse/staging/content_catalog)
#                 Hive Staging Table (media_warehouse.stg_content_catalog)
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

# Logging directory and file
LOG_DIR="${SCRIPT_DIR}/../logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/${SCRIPT_NAME%.sh}_${TIMESTAMP}.log"

# State directory for maintaining incremental watermarks
STATE_DIR="${SCRIPT_DIR}/../state"
mkdir -p "${STATE_DIR}"
WATERMARK_FILE="${STATE_DIR}/content_catalog.watermark"

# RDBMS Connection Parameters (Overridable via environment variables)
DB_HOST="${DB_HOST:-rdbms-host}"
DB_PORT="${DB_PORT:-3306}"
DB_NAME="${DB_NAME:-media_db}"
DB_USER="${DB_USER:-media_sqoop_user}"
JDBC_URL="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}?useSSL=false&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull"
SOURCE_TABLE="content_catalog"

# Security: Password file stored securely in HDFS or local file system
# Never hardcode database credentials in production scripts!
PASSWORD_FILE="${PASSWORD_FILE:-/user/media_etl/.passwords/mysql_media.pass}"

# Hadoop / HDFS / Hive Target Configuration
HDFS_BASE_DIR="/data/media_warehouse/staging/content_catalog"
HDFS_TARGET_DIR="${HDFS_BASE_DIR}/delta_${TIMESTAMP}"
HIVE_DB="${HIVE_DB:-media_warehouse}"
HIVE_TABLE="${HIVE_TABLE:-stg_content_catalog}"

# Sqoop Tuning & Concurrency
NUM_MAPPERS="${NUM_MAPPERS:-4}"
SPLIT_BY_COL="content_id"
CHECK_COLUMN="updated_at"
FETCH_SIZE="5000"

# Default baseline watermark if no prior watermark exists (Epoch default)
DEFAULT_LAST_VALUE="1970-01-01 00:00:00"

# ------------------------------------------------------------------------------
# 2. Logging & Error Handling Infrastructure
# ------------------------------------------------------------------------------
log() {
    local level="$1"
    shift
    local message="$*"
    local log_entry="[$(date +'%Y-%m-%d %H:%M:%S')] [${level}] ${message}"
    echo -e "${log_entry}" | tee -a "${LOG_FILE}"
}

log_info()  { log "INFO " "$*"; }
log_warn()  { log "WARN " "$*"; }
log_error() { log "ERROR" "$*" >&2; }

cleanup() {
    local exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
        log_error "Job terminated abnormally with exit code ${exit_code}."
    else
        log_info "Job completed successfully."
    fi
}
trap cleanup EXIT ERR

# ------------------------------------------------------------------------------
# 3. CLI Argument Parsing & Help Display
# ------------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: ${SCRIPT_NAME} [OPTIONS]

Options:
  --last-value <timestamp>    Override watermark timestamp (format: 'YYYY-MM-DD HH:MM:SS')
  --mappers <int>             Number of parallel Sqoop map tasks (default: ${NUM_MAPPERS})
  --full-reload               Execute full historical reload instead of incremental
  --dry-run                   Print Sqoop command without executing
  -h, --help                  Display this help message and exit

Examples:
  ./${SCRIPT_NAME}
  ./${SCRIPT_NAME} --last-value "2026-01-01 00:00:00"
  ./${SCRIPT_NAME} --mappers 8 --dry-run
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
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# ------------------------------------------------------------------------------
# 4. Pre-flight Validation Checks
# ------------------------------------------------------------------------------
log_info "Starting pre-flight validations..."

# Check Sqoop executable
if ! command -v sqoop &>/dev/null; then
    log_warn "Sqoop CLI binary not found in system PATH. Checking HADOOP_HOME and SQOOP_HOME..."
    if [[ -n "${SQOOP_HOME:-}" && -x "${SQOOP_HOME}/bin/sqoop" ]]; then
        export PATH="${SQOOP_HOME}/bin:${PATH}"
    else
        log_error "Sqoop is not installed or not available in PATH. Please verify your environment."
        exit 2
    fi
fi

# Check HDFS CLI availability
if ! command -v hdfs &>/dev/null; then
    log_error "HDFS CLI ('hdfs') is not accessible. Ensure Hadoop environment variables are loaded."
    exit 3
fi

# Verify HDFS directory accessibility
log_info "Verifying HDFS target base directory: ${HDFS_BASE_DIR}"
if ! hdfs dfs -test -d "${HDFS_BASE_DIR}" 2>/dev/null; then
    log_warn "HDFS directory ${HDFS_BASE_DIR} does not exist. Creating it now..."
    hdfs dfs -mkdir -p "${HDFS_BASE_DIR}"
fi

# ------------------------------------------------------------------------------
# 5. Determine Import Watermark (Incremental vs Full)
# ------------------------------------------------------------------------------
if [ "${FULL_RELOAD}" = true ]; then
    log_warn "FULL RELOAD requested. Ignoring state watermark file."
    IMPORT_MODE="full"
    LAST_VALUE=""
    HDFS_TARGET_DIR="${HDFS_BASE_DIR}/full_${TIMESTAMP}"
elif [ -n "${MANUAL_LAST_VALUE}" ]; then
    log_info "Using manual watermark override: ${MANUAL_LAST_VALUE}"
    IMPORT_MODE="lastmodified"
    LAST_VALUE="${MANUAL_LAST_VALUE}"
elif [ -f "${WATERMARK_FILE}" ]; then
    LAST_VALUE="$(cat "${WATERMARK_FILE}" | tr -d '\r\n')"
    log_info "Loaded last-modified watermark from state file: ${LAST_VALUE}"
    IMPORT_MODE="lastmodified"
else
    log_warn "No watermark state file found at ${WATERMARK_FILE}. Defaulting to initial epoch: ${DEFAULT_LAST_VALUE}"
    LAST_VALUE="${DEFAULT_LAST_VALUE}"
    IMPORT_MODE="lastmodified"
fi

CURRENT_EXECUTION_TIME="$(date +'%Y-%m-%d %H:%M:%S')"

# ------------------------------------------------------------------------------
# 6. Build Sqoop Import Command
# ------------------------------------------------------------------------------
log_info "Assembling Sqoop import parameters for table [${SOURCE_TABLE}]..."

SQOOP_CMD=(
    sqoop import
    -Dmapreduce.job.user.classpath.first=true
    -Dorg.apache.sqoop.splitter.allow_text_splitter=true
    --connect "${JDBC_URL}"
    --username "${DB_USER}"
    --password-file "${PASSWORD_FILE}"
    --table "${SOURCE_TABLE}"
    --target-dir "${HDFS_TARGET_DIR}"
    --num-mappers "${NUM_MAPPERS}"
    --split-by "${SPLIT_BY_COL}"
    --fetch-size "${FETCH_SIZE}"
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

# Append incremental import configuration if not a full reload
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
# 7. Execute Sqoop Job
# ------------------------------------------------------------------------------
log_info "Sqoop connection target: ${JDBC_URL}"
log_info "Source table: ${SOURCE_TABLE}"
log_info "Split key: ${SPLIT_BY_COL} with ${NUM_MAPPERS} parallel mappers"
log_info "Destination HDFS: ${HDFS_TARGET_DIR}"
log_info "Destination Hive: ${HIVE_DB}.${HIVE_TABLE}"
[ "${IMPORT_MODE}" == "lastmodified" ] && log_info "Incremental check on column '${CHECK_COLUMN}' with last-value '${LAST_VALUE}'"

if [ "${DRY_RUN}" = true ]; then
    log_info "DRY RUN MODE ENABLED. Command to be executed:"
    echo "${SQOOP_CMD[@]}"
    exit 0
fi

log_info "Triggering Sqoop import process..."
START_TIME=$(date +%s)

# Execute command and redirect stdout & stderr to log
if "${SQOOP_CMD[@]}" 2>&1 | tee -a "${LOG_FILE}"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log_info "Sqoop import completed successfully in ${DURATION} seconds."

    # --------------------------------------------------------------------------
    # 8. Post-Import Verification & State Persistence
    # --------------------------------------------------------------------------
    log_info "Running post-import verification..."
    
    # Check if files were created in HDFS target directory
    HDFS_FILE_COUNT="$(hdfs dfs -count -q "${HDFS_TARGET_DIR}" 2>/dev/null | awk '{print $2}' || echo "0")"
    log_info "HDFS target directory file count: ${HDFS_FILE_COUNT}"

    # Update state watermark for future incremental runs
    echo "${CURRENT_EXECUTION_TIME}" > "${WATERMARK_FILE}"
    log_info "State watermark updated to: ${CURRENT_EXECUTION_TIME} in ${WATERMARK_FILE}"

    # Trigger Hive statistics computation for query optimization
    log_info "Analyzing Hive table stats for ${HIVE_DB}.${HIVE_TABLE}..."
    hive -e "ANALYZE TABLE ${HIVE_DB}.${HIVE_TABLE} COMPUTE STATISTICS;" 2>>"${LOG_FILE}" || {
        log_warn "Hive stats analysis encountered a non-fatal warning."
    }

    log_info "Content catalog ingestion pipeline finished successfully."
    exit 0
else
    IMPORT_EXIT_CODE=$?
    log_error "Sqoop import failed with exit code: ${IMPORT_EXIT_CODE}. Check log: ${LOG_FILE}"
    exit ${IMPORT_EXIT_CODE}
fi
