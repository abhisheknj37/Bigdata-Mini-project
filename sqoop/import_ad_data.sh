#!/usr/bin/env bash
# ==============================================================================
# Script Name   : import_ad_data.sh
# Description   : High-throughput Apache Sqoop ETL script to ingest high-volume
#                 advertising impression, campaign, and revenue event logs
#                 from MySQL RDBMS into HDFS staging and Hive data warehouse.
# Strategy      : Incremental import using 'append' mode on monotonic impression_id
#                 or 'lastmodified' on impression_timestamp.
# Target Layers : HDFS Staging (/data/media_warehouse/staging/ad_impressions)
#                 Hive Staging (media_warehouse.stg_ad_impressions)
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
PARTITION_DATE="$(date +'%Y-%m-%d')"

# Logging and watermark directories
LOG_DIR="${SCRIPT_DIR}/../logs"
STATE_DIR="${SCRIPT_DIR}/../state"
mkdir -p "${LOG_DIR}" "${STATE_DIR}"

LOG_FILE="${LOG_DIR}/${SCRIPT_NAME%.sh}_${TIMESTAMP}.log"
WATERMARK_FILE="${STATE_DIR}/ad_impressions.watermark"

# RDBMS Connection Parameters
DB_HOST="${DB_HOST:-rdbms-host}"
DB_PORT="${DB_PORT:-3306}"
DB_NAME="${DB_NAME:-media_db}"
DB_USER="${DB_USER:-media_sqoop_user}"
JDBC_URL="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}?useSSL=false&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
SOURCE_TABLE="ad_impressions"

# Security: Password file in secure storage
PASSWORD_FILE="${PASSWORD_FILE:-/user/media_etl/.passwords/mysql_media.pass}"

# Target Storage Configuration
HDFS_BASE_DIR="/data/media_warehouse/staging/ad_impressions"
HDFS_TARGET_DIR="${HDFS_BASE_DIR}/dt=${PARTITION_DATE}/run_${TIMESTAMP}"
HIVE_DB="${HIVE_DB:-media_warehouse}"
HIVE_TABLE="${HIVE_TABLE:-stg_ad_impressions}"

# Performance & Parallelism Tuning for High-Volume Ad Data
NUM_MAPPERS="${NUM_MAPPERS:-8}"
SPLIT_BY_COL="impression_id"
CHECK_COLUMN="impression_id"
INCREMENTAL_MODE="append"
FETCH_SIZE="20000"
DEFAULT_LAST_VALUE="0"

# ------------------------------------------------------------------------------
# 2. Logging & Trap Handling
# ------------------------------------------------------------------------------
log() {
    local level="$1"
    shift
    local msg="$*"
    local log_str="[$(date +'%Y-%m-%d %H:%M:%S')] [${level}] ${msg}"
    echo -e "${log_str}" | tee -a "${LOG_FILE}"
}

log_info()  { log "INFO " "$*"; }
log_warn()  { log "WARN " "$*"; }
log_error() { log "ERROR" "$*" >&2; }

cleanup() {
    local exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
        log_error "Ad impression import pipeline encountered a fatal error (code ${exit_code})."
    else
        log_info "Ad impression import pipeline finished."
    fi
}
trap cleanup EXIT ERR

# ------------------------------------------------------------------------------
# 3. CLI Options & Help
# ------------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: ${SCRIPT_NAME} [OPTIONS]

Options:
  --last-value <id_or_ts>     Watermark value (ID for append mode, Timestamp for lastmodified)
  --mode <append|lastmodified> Ingestion mode (default: append)
  --mappers <int>             Number of parallel Sqoop map tasks (default: ${NUM_MAPPERS})
  --direct                    Use MySQL direct import dump provider (high performance)
  --full-reload               Perform full table reload
  --dry-run                   Echo the generated command without executing
  -h, --help                  Show this usage guide

Examples:
  ./${SCRIPT_NAME}
  ./${SCRIPT_NAME} --mode append --last-value 5000000 --mappers 12
  ./${SCRIPT_NAME} --direct
EOF
    exit 1
}

MANUAL_LAST_VALUE=""
USE_DIRECT=false
FULL_RELOAD=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --last-value)
            MANUAL_LAST_VALUE="$2"
            shift 2
            ;;
        --mode)
            INCREMENTAL_MODE="$2"
            if [ "${INCREMENTAL_MODE}" == "lastmodified" ]; then
                CHECK_COLUMN="impression_timestamp"
                DEFAULT_LAST_VALUE="1970-01-01 00:00:00"
            fi
            shift 2
            ;;
        --mappers)
            NUM_MAPPERS="$2"
            shift 2
            ;;
        --direct)
            USE_DIRECT=true
            shift
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
            log_error "Unknown parameter: $1"
            usage
            ;;
    esac
done

# ------------------------------------------------------------------------------
# 4. Pre-flight Environment Checks
# ------------------------------------------------------------------------------
log_info "Verifying execution runtime prerequisites..."

if ! command -v sqoop &>/dev/null; then
    if [[ -n "${SQOOP_HOME:-}" && -x "${SQOOP_HOME}/bin/sqoop" ]]; then
        export PATH="${SQOOP_HOME}/bin:${PATH}"
    else
        log_error "Sqoop binary is not present in PATH."
        exit 2
    fi
fi

if ! command -v hdfs &>/dev/null; then
    log_error "HDFS CLI binary is not present in PATH."
    exit 3
fi

# Ensure partition target directory exists in HDFS
if ! hdfs dfs -test -d "${HDFS_BASE_DIR}/dt=${PARTITION_DATE}" 2>/dev/null; then
    log_info "Creating partition directory: ${HDFS_BASE_DIR}/dt=${PARTITION_DATE}"
    hdfs dfs -mkdir -p "${HDFS_BASE_DIR}/dt=${PARTITION_DATE}"
fi

# ------------------------------------------------------------------------------
# 5. Watermark Resolution
# ------------------------------------------------------------------------------
if [ "${FULL_RELOAD}" = true ]; then
    log_warn "FULL RELOAD requested for ad impressions. Ignoring previous watermarks."
    IS_INCREMENTAL=false
    LAST_VALUE=""
    HDFS_TARGET_DIR="${HDFS_BASE_DIR}/full_${TIMESTAMP}"
elif [ -n "${MANUAL_LAST_VALUE}" ]; then
    log_info "Manual watermark override specified: ${MANUAL_LAST_VALUE}"
    IS_INCREMENTAL=true
    LAST_VALUE="${MANUAL_LAST_VALUE}"
elif [ -f "${WATERMARK_FILE}" ]; then
    LAST_VALUE="$(cat "${WATERMARK_FILE}" | tr -d '\r\n')"
    log_info "Retrieved existing watermark: ${LAST_VALUE}"
    IS_INCREMENTAL=true
else
    log_warn "No watermark found at ${WATERMARK_FILE}. Using default seed: ${DEFAULT_LAST_VALUE}"
    LAST_VALUE="${DEFAULT_LAST_VALUE}"
    IS_INCREMENTAL=true
fi

# ------------------------------------------------------------------------------
# 6. Assemble Sqoop Command
# ------------------------------------------------------------------------------
log_info "Constructing high-performance Sqoop job definition..."

SQOOP_CMD=(
    sqoop import
    -Dmapreduce.job.user.classpath.first=true
    -Dmapreduce.map.memory.mb=4096
    -Dmapreduce.map.java.opts="-Xmx3072m -XX:+UseG1GC"
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
    --map-column-java "bid_price=Double,clearing_price=Double,revenue=Double"
    --null-string '\\N'
    --null-non-string '\\N'
    --hive-import
    --hive-database "${HIVE_DB}"
    --hive-table "${HIVE_TABLE}"
    --hive-partition-key "dt"
    --hive-partition-value "${PARTITION_DATE}"
    --hive-drop-import-delims
)

if [ "${USE_DIRECT}" = true ]; then
    log_info "Enabling --direct fast path for MySQL..."
    SQOOP_CMD+=(--direct)
fi

if [ "${IS_INCREMENTAL}" = true ]; then
    SQOOP_CMD+=(
        --incremental "${INCREMENTAL_MODE}"
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
# 7. Execute Ingestion Job
# ------------------------------------------------------------------------------
log_info "JDBC Target      : ${JDBC_URL}"
log_info "Source Table     : ${SOURCE_TABLE}"
log_info "Mappers / Split  : ${NUM_MAPPERS} parallel tasks / column '${SPLIT_BY_COL}'"
log_info "HDFS Destination : ${HDFS_TARGET_DIR}"
log_info "Hive Partition   : ${HIVE_DB}.${HIVE_TABLE} [dt=${PARTITION_DATE}]"
[ "${IS_INCREMENTAL}" = true ] && log_info "Incremental Mode : ${INCREMENTAL_MODE} on '${CHECK_COLUMN}' (last-value: ${LAST_VALUE})"

if [ "${DRY_RUN}" = true ]; then
    log_info "DRY RUN MODE. Command output:"
    echo "${SQOOP_CMD[@]}"
    exit 0
fi

log_info "Triggering ad data ingestion..."
START_TIME=$(date +%s)

# Execute and capture output
if "${SQOOP_CMD[@]}" 2>&1 | tee -a "${LOG_FILE}"; then
    DURATION=$(( $(date +%s) - START_TIME ))
    log_info "Ad impression Sqoop import completed successfully in ${DURATION}s."

    # --------------------------------------------------------------------------
    # 8. Watermark Capture & Post-Validation
    # --------------------------------------------------------------------------
    # Extract the new high-water mark reported by Sqoop in the log file
    # Sqoop reports: "INFO tool.ImportTool:   --last-value <VALUE>"
    NEW_WATERMARK=$(grep -Eo -- "--last-value [^ ]+" "${LOG_FILE}" | tail -n 1 | awk '{print $2}' || echo "")

    if [ -n "${NEW_WATERMARK}" ]; then
        echo "${NEW_WATERMARK}" > "${WATERMARK_FILE}"
        log_info "Watermark state successfully updated to: ${NEW_WATERMARK}"
    else
        log_warn "Could not parse automatic watermark from log. Retaining existing or timestamp."
        if [ "${INCREMENTAL_MODE}" == "lastmodified" ]; then
            date +'%Y-%m-%d %H:%M:%S' > "${WATERMARK_FILE}"
        fi
    fi

    # Audit HDFS storage
    TOTAL_BYTES=$(hdfs dfs -du -s "${HDFS_TARGET_DIR}" 2>/dev/null | awk '{print $1}' || echo "0")
    log_info "Total ingested volume in delta: $(( TOTAL_BYTES / 1024 / 1024 )) MB"

    # Analyze Hive partition
    log_info "Gathering Hive statistics on partition dt=${PARTITION_DATE}..."
    hive -e "ANALYZE TABLE ${HIVE_DB}.${HIVE_TABLE} PARTITION(dt='${PARTITION_DATE}') COMPUTE STATISTICS;" 2>>"${LOG_FILE}" || {
        log_warn "Hive partition statistics generation issued a non-fatal warning."
    }

    log_info "Ad impression ingestion pipeline finished successfully."
    exit 0
else
    ERR_CODE=$?
    log_error "Sqoop ad import failed with exit code ${ERR_CODE}. Consult log: ${LOG_FILE}"
    exit ${ERR_CODE}
fi
