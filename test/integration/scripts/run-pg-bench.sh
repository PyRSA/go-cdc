#!/usr/bin/env bash
# Build go-cdc, ensure MySQL + PostgreSQL are up, run the PostgreSQL sink benchmark.
#
#   bash scripts/run-pg-bench.sh                      # 2 workloads x batch 100/1000/5000
#   bash scripts/run-pg-bench.sh --batches 100        # one column of the matrix
#   bash scripts/run-pg-bench.sh --workloads mixed --rows 5000   # quick smoke
#   bash scripts/run-pg-bench.sh --list               # print the matrix, run nothing
#
# This is a performance benchmark (spec PG-210), not an acceptance test: it owns
# its own source database, target schema, checkpoint directory and report, so it
# neither reads nor overwrites an E2E / load / scale result. Every write happens
# on the MySQL source; the PostgreSQL sink is only read back.
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
export ROOT_USER="${ROOT_USER:-root}"
export ROOT_PASSWORD="${ROOT_PASSWORD:-rootpass}"
export PG_HOST="${PG_HOST:-127.0.0.1}"
export PG_PORT="${PG_PORT:-15432}"
export PG_USER="${PG_USER:-postgres}"
export PG_PASSWORD="${PG_PASSWORD:-postgres}"
export PG_DB="${PG_DB:-cdc_it}"
export PG_SSLMODE="${PG_SSLMODE:-prefer}"
# The benchmark owns this database/schema pair; nothing else may reuse them.
export PG_BENCH_SRC_DB="${PG_BENCH_SRC_DB:-db_tp_pg_bench_src}"
export PG_BENCH_SNK_SCHEMA="${PG_BENCH_SNK_SCHEMA:-tp_pg_bench}"
export CHECKPOINT_PG_BENCH_DIR="${CHECKPOINT_PG_BENCH_DIR:-${WORKDIR}/ckpts-pg-bench}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

absolutize_go_cdc
absolutize_checkpoint_dirs
mkdir -p "${CHECKPOINT_PG_BENCH_DIR}"

# Pin the matrix so an ambient shell value cannot quietly change what a report is
# compared against; CI may override via step env before this script runs.
export PG_BENCH_BATCHES="${PG_BENCH_BATCHES:-100,1000,5000}"
export PG_BENCH_WORKLOADS="${PG_BENCH_WORKLOADS:-insert,mixed}"
export PG_BENCH_ROWS="${PG_BENCH_ROWS:-20000}"
export PG_BENCH_WORKERS="${PG_BENCH_WORKERS:-1}"
export PG_BENCH_SEED="${PG_BENCH_SEED:-20261030}"
export PG_BENCH_BATCH_INTERVAL="${PG_BENCH_BATCH_INTERVAL:-200ms}"
export PG_BENCH_PARALLELISM="${PG_BENCH_PARALLELISM:-1}"
export PG_BENCH_PROBE_INTERVAL_S="${PG_BENCH_PROBE_INTERVAL_S:-2}"
export PG_BENCH_CATCHUP_BUDGET_S="${PG_BENCH_CATCHUP_BUDGET_S:-600}"

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

log "==> benchmark knobs"
log "    batches=${PG_BENCH_BATCHES} workloads=${PG_BENCH_WORKLOADS}"
log "    rows/cell=${PG_BENCH_ROWS} workers=${PG_BENCH_WORKERS} seed=${PG_BENCH_SEED}"
log "    batch interval=${PG_BENCH_BATCH_INTERVAL} parallelism=${PG_BENCH_PARALLELISM}"
log "    probe every ${PG_BENCH_PROBE_INTERVAL_S}s, catch-up budget ${PG_BENCH_CATCHUP_BUDGET_S}s"
log "    source ${PG_BENCH_SRC_DB} (MySQL) -> schema ${PG_BENCH_SNK_SCHEMA} (PostgreSQL)"

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

log "==> reset PostgreSQL benchmark checkpoint"
rm -rf "${CHECKPOINT_PG_BENCH_DIR}"
mkdir -p "${CHECKPOINT_PG_BENCH_DIR}"

log "==> run postgresql sink benchmark (PG-210)"
cd "${IT}"
"${PYTHON_BIN}" -m harness.pg_bench_it "$@"

log "==> postgresql benchmark done"
