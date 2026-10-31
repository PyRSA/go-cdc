#!/usr/bin/env bash
# Build go-cdc, ensure MySQL + PostgreSQL are up, run the PostgreSQL sink E2E suite.
#
#   bash scripts/run-pg-e2e.sh                 # every test point (TP-P01..TP-P20)
#   bash scripts/run-pg-e2e.sh TP-P05 TP-P14   # a subset, for a re-run of a failure
#   PG_RESTART_ENABLED=1 bash scripts/run-pg-e2e.sh TP-P17
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
export PG_HOST="${PG_HOST:-127.0.0.1}"
export PG_PORT="${PG_PORT:-15432}"
export PG_USER="${PG_USER:-postgres}"
export PG_PASSWORD="${PG_PASSWORD:-postgres}"
export PG_DB="${PG_DB:-cdc_it}"
export PG_SSLMODE="${PG_SSLMODE:-prefer}"
export PG_SRC_DB_A="${PG_SRC_DB_A:-db_tp_pg_src_a}"
export PG_SRC_DB_B="${PG_SRC_DB_B:-db_tp_pg_src_b}"
export PG_SNK_SCHEMA_A="${PG_SNK_SCHEMA_A:-tp_pg_a}"
export PG_SNK_SCHEMA_B="${PG_SNK_SCHEMA_B:-tp_pg_b}"
export CHECKPOINT_PG_DIR="${CHECKPOINT_PG_DIR:-${WORKDIR}/ckpts-pg}"
export CHECKPOINT_PG_RESTART_DIR="${CHECKPOINT_PG_RESTART_DIR:-${WORKDIR}/ckpts-pg-restart}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

absolutize_go_cdc
absolutize_checkpoint_dirs
mkdir -p "${CHECKPOINT_PG_DIR}" "${CHECKPOINT_PG_RESTART_DIR}"

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

log "==> reset PostgreSQL checkpoints"
rm -rf "${CHECKPOINT_PG_DIR}" "${CHECKPOINT_PG_RESTART_DIR}"
mkdir -p "${CHECKPOINT_PG_DIR}" "${CHECKPOINT_PG_RESTART_DIR}"

log "==> run postgresql sink e2e harness (TP-P01..TP-P20)"
log "    source DBs are (re)created per case; target schemas are dropped first"
cd "${IT}"
"${PYTHON_BIN}" -m harness.pg_it "$@"

log "==> postgresql e2e done"
