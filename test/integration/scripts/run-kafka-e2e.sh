#!/usr/bin/env bash
# Build go-cdc, ensure MySQL+Kafka are up, run Kafka sink harness (TP-K01..K08).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
IT="${ROOT}/test/integration"
WORKDIR="${IT}/workdir"
mkdir -p "${WORKDIR}" "${ROOT}/docs/test/reports"
# shellcheck source=test/integration/scripts/_log.sh
source "${IT}/scripts/_log.sh"

export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
export SRC_USER="${SRC_USER:-u_a7k9m2x4}"
export SRC_PASSWORD="${SRC_PASSWORD:-P_w8q3n5v1r6}"
export FUNC_SRC_DB="${FUNC_SRC_DB:-db_tp_func_src}"
export LOAD_SRC_DB="${LOAD_SRC_DB:-db_tp_load_src}"
export LOAD_SNK_DB="${LOAD_SNK_DB:-db_tp_load_snk}"
export SRC_DB="${SRC_DB:-${FUNC_SRC_DB}}"
export KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-19092}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:${KAFKA_HOST_PORT}}"
export CHECKPOINT_KAFKA_DIR="${CHECKPOINT_KAFKA_DIR:-${WORKDIR}/ckpts-kafka}"
export CHECKPOINT_KAFKA_LOAD_DIR="${CHECKPOINT_KAFKA_LOAD_DIR:-${WORKDIR}/ckpts-kafka-load}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi
# Load knobs (same defaults / CI GITHUB_ENV as mysql→mysql load suite).
export LOAD_ROWS_PER_TABLE="${LOAD_ROWS_PER_TABLE:-50000}"
export LOAD_PEAK_SECONDS="${LOAD_PEAK_SECONDS:-60}"
export LOAD_PEAK_WORKERS="${LOAD_PEAK_WORKERS:-4}"
export LOAD_PEAK_BATCH="${LOAD_PEAK_BATCH:-200}"
export LOAD_PARALLELISM="${LOAD_PARALLELISM:-${LOAD_PEAK_WORKERS}}"
export LOAD_LARGE_ROWS="${LOAD_LARGE_ROWS:-50}"
export LOAD_LARGE_FIELD_BYTES="${LOAD_LARGE_FIELD_BYTES:-2097152}"
export LOAD_PROFILE="${LOAD_PROFILE:-}"

absolutize_go_cdc
absolutize_checkpoint_dirs
mkdir -p "${CHECKPOINT_KAFKA_DIR}" "${CHECKPOINT_KAFKA_LOAD_DIR}"

# Local-only: tear down kafka+mysql compose project on exit unless KEEP_IT_MYSQL=1.
if [ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ]; then
  cleanup() {
    local status=$?
    if [ "${KEEP_IT_MYSQL:-}" = "1" ]; then
      exit "${status}"
    fi
    log "==> remove integration compose (mysql+kafka)"
    bash "${IT}/scripts/mysql-down.sh" || true
    exit "${status}"
  }
  trap cleanup EXIT
fi

mysql_as() {
  local user="$1" pass="$2"
  shift 2
  MYSQL_PWD="${pass}" mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${user}" --protocol=TCP "$@"
}

log "==> build go-cdc"
(cd "${ROOT}" && go build -o "${GO_CDC}" ./cmd/go-cdc)

log "==> install harness deps"
resolve_python_bin
log "    using ${PYTHON_BIN}"
"${PYTHON_BIN}" -m pip install -q -r "${IT}/harness/requirements.txt"

log "==> wait mysql"
bash "${IT}/scripts/wait-for-mysql.sh"

log "==> wait kafka"
bash "${IT}/scripts/wait-for-kafka.sh"

log "==> seed kafka functional suite tables"
mysql_as "${SRC_USER}" "${SRC_PASSWORD}" < "${IT}/fixtures/seed_kafka.sql"
rm -rf "${CHECKPOINT_KAFKA_DIR}" "${CHECKPOINT_KAFKA_LOAD_DIR}"
mkdir -p "${CHECKPOINT_KAFKA_DIR}" "${CHECKPOINT_KAFKA_LOAD_DIR}"

log "==> run kafka sink harness (TP-K01..K08)"
log "    LOAD_PROFILE=${LOAD_PROFILE:-"(unset)"} LOAD_ROWS_PER_TABLE=${LOAD_ROWS_PER_TABLE}"
log "    LOAD_PEAK_SECONDS=${LOAD_PEAK_SECONDS} LOAD_PEAK_WORKERS=${LOAD_PEAK_WORKERS}"
log "    LOAD_LARGE_ROWS=${LOAD_LARGE_ROWS} LOAD_LARGE_FIELD_BYTES=${LOAD_LARGE_FIELD_BYTES}"
cd "${IT}"
"${PYTHON_BIN}" -m harness.kafka_it

log "==> kafka e2e done"
