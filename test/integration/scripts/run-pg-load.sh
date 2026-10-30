#!/usr/bin/env bash
# Build go-cdc, ensure MySQL + PostgreSQL are up, run the PostgreSQL sink load suite.
#
#   bash scripts/run-pg-load.sh                # TP-PL01..TP-PL04
#   bash scripts/run-pg-load.sh TP-PL04        # a subset, for a re-run of a failure
#   LOAD_ROWS_PER_TABLE=5000 bash scripts/run-pg-load.sh   # quick local smoke
#
# Every write happens on the MySQL source; the PostgreSQL sink is only read back.
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
# The load suite reuses the mysql2mysql load source database.
export LOAD_SRC_DB="${LOAD_SRC_DB:-db_tp_load_src}"
export PG_HOST="${PG_HOST:-127.0.0.1}"
export PG_PORT="${PG_PORT:-15432}"
export PG_USER="${PG_USER:-postgres}"
export PG_PASSWORD="${PG_PASSWORD:-postgres}"
export PG_DB="${PG_DB:-cdc_it}"
export PG_SSLMODE="${PG_SSLMODE:-prefer}"
export PG_LOAD_SNK_SCHEMA="${PG_LOAD_SNK_SCHEMA:-tp_pg_load}"
export CHECKPOINT_PG_LOAD_DIR="${CHECKPOINT_PG_LOAD_DIR:-${WORKDIR}/ckpts-pg-load}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

absolutize_go_cdc
absolutize_checkpoint_dirs
mkdir -p "${CHECKPOINT_PG_LOAD_DIR}"

# Pin load knobs so ambient shell leftovers are visible and defaults are explicit.
# CI may override via GITHUB_ENV / step env before this script runs.
export LOAD_PROFILE="${LOAD_PROFILE:-local}"
export LOAD_ROWS_PER_TABLE="${LOAD_ROWS_PER_TABLE:-50000}"
export LOAD_LARGE_ROWS="${LOAD_LARGE_ROWS:-50}"
export LOAD_LARGE_FIELD_BYTES="${LOAD_LARGE_FIELD_BYTES:-2097152}"
export LOAD_PEAK_SECONDS="${LOAD_PEAK_SECONDS:-60}"
export LOAD_PEAK_WORKERS="${LOAD_PEAK_WORKERS:-4}"
export LOAD_PEAK_BATCH="${LOAD_PEAK_BATCH:-200}"
export LOAD_PARALLELISM="${LOAD_PARALLELISM:-${LOAD_PEAK_WORKERS}}"

# Local-only: tear the compose project down on exit unless KEEP_IT_MYSQL=1.
if [ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ]; then
  cleanup() {
    local status=$?
    if [ "${KEEP_IT_MYSQL:-}" = "1" ]; then
      exit "${status}"
    fi
    log "==> remove integration compose (mysql+postgres)"
    bash "${IT}/scripts/pg-down.sh" || true
    bash "${IT}/scripts/mysql-down.sh" || true
    exit "${status}"
  }
  trap cleanup EXIT
fi

log "==> load knobs"
log "    LOAD_PROFILE=${LOAD_PROFILE}"
log "    LOAD_ROWS_PER_TABLE=${LOAD_ROWS_PER_TABLE}"
log "    LOAD_LARGE_ROWS=${LOAD_LARGE_ROWS}"
log "    LOAD_LARGE_FIELD_BYTES=${LOAD_LARGE_FIELD_BYTES}"
log "    LOAD_PEAK_SECONDS=${LOAD_PEAK_SECONDS} x LOAD_PEAK_WORKERS=${LOAD_PEAK_WORKERS}"
log "    LOAD_PEAK_BATCH=${LOAD_PEAK_BATCH}"
log "    LOAD_PARALLELISM=${LOAD_PARALLELISM}"

log "==> build go-cdc"
(cd "${ROOT}" && go build -o "${GO_CDC}" ./cmd/go-cdc)

log "==> install harness deps"
resolve_python_bin
log "    using ${PYTHON_BIN}"
"${PYTHON_BIN}" -m pip install -q -r "${IT}/harness/requirements.txt"

log "==> wait mysql"
bash "${IT}/scripts/wait-for-mysql.sh"

log "==> wait postgres"
bash "${IT}/scripts/wait-for-postgres.sh"

log "==> reset PostgreSQL load checkpoint"
rm -rf "${CHECKPOINT_PG_LOAD_DIR}"
mkdir -p "${CHECKPOINT_PG_LOAD_DIR}"

log "==> run postgresql sink load harness (TP-PL01..TP-PL04)"
log "    source ${LOAD_SRC_DB} (MySQL) -> schema ${PG_LOAD_SNK_SCHEMA} (PostgreSQL)"
cd "${IT}"
"${PYTHON_BIN}" -m harness.pg_load_it "$@"

log "==> postgresql load done"
