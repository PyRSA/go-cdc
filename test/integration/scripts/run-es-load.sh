#!/usr/bin/env bash
# Build go-cdc, ensure MySQL + Elasticsearch 8 are up, run the ES8 load suite.
# Reuses the MySQL load tables (db_tp_load_src) and load knobs; the sink is ES8.
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
export LOAD_SRC_DB="${LOAD_SRC_DB:-db_tp_load_src}"
export ES_HOST_PORT="${ES_HOST_PORT:-19200}"
export ES_URL="${ES_URL:-http://127.0.0.1:${ES_HOST_PORT}}"
export ES_LOAD_SNK_DB="${ES_LOAD_SNK_DB:-db_tp_es_load_snk}"
# Cleanup namespace = the logical sink DB (`db_tp_es_load_snk.<table>[-hash]`).
export ES_LOAD_INDEX_PREFIX="${ES_LOAD_INDEX_PREFIX:-${ES_LOAD_SNK_DB}}"
export CHECKPOINT_ES_LOAD_DIR="${CHECKPOINT_ES_LOAD_DIR:-${WORKDIR}/ckpts-es-load}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

# Load knobs (same defaults / CI GITHUB_ENV as the mysql→mysql load suite).
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
mkdir -p "${CHECKPOINT_ES_LOAD_DIR}"

if [ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ]; then
  cleanup() {
    local status=$?
    if [ "${KEEP_IT_MYSQL:-}" = "1" ]; then
      exit "${status}"
    fi
    log "==> remove integration compose (mysql+es)"
    bash "${IT}/scripts/mysql-down.sh" || true
    exit "${status}"
  }
  trap cleanup EXIT
fi

log "==> build go-cdc"
(cd "${ROOT}" && go build -o "${GO_CDC}" ./cmd/go-cdc)

log "==> install harness deps"
resolve_python_bin
log "    using ${PYTHON_BIN}"
"${PYTHON_BIN}" -m pip install -q -r "${IT}/harness/requirements.txt"

log "==> wait mysql"
bash "${IT}/scripts/wait-for-mysql.sh"

log "==> wait elasticsearch"
bash "${IT}/scripts/wait-for-es.sh"

rm -rf "${CHECKPOINT_ES_LOAD_DIR}"
mkdir -p "${CHECKPOINT_ES_LOAD_DIR}"

log "==> run elasticsearch sink load harness (TP-EL01..)"
log "    LOAD_PROFILE=${LOAD_PROFILE:-"(unset)"} LOAD_ROWS_PER_TABLE=${LOAD_ROWS_PER_TABLE}"
log "    LOAD_PEAK_SECONDS=${LOAD_PEAK_SECONDS} LOAD_PEAK_WORKERS=${LOAD_PEAK_WORKERS}"
log "    LOAD_LARGE_ROWS=${LOAD_LARGE_ROWS} LOAD_LARGE_FIELD_BYTES=${LOAD_LARGE_FIELD_BYTES}"
cd "${IT}"
"${PYTHON_BIN}" -m harness.es_load_it

log "==> elasticsearch load done"
