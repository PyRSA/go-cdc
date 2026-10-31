#!/usr/bin/env bash
# Build go-cdc, ensure MySQL + PostgreSQL are up, run the PostgreSQL scale suite.
#
#   bash scripts/run-pg-scale.sh                    # normal profile (20 tables, 50k rows)
#   bash scripts/run-pg-scale.sh normal snapshot    # stop after the snapshot phase
#   PG_SCALE_HEAVY=1 bash scripts/run-pg-scale.sh heavy   # 50 tables, 300k rows
#
# `heavy` has to be asked for twice on purpose: it writes ~300k rows and runs the
# hotspot burst + 45 s peak, which is not something a default local run should do.
#
# Every write happens on the MySQL source; the PostgreSQL sink is only read back.
# Isolation (B3): the run owns only `tp_pg_scale_<profile>_<nn>` schemas and
# `ckpts-pg-scale/<profile>`; the orchestrator drops/clears only those.
set -euo pipefail
PROFILE="${1:-${PG_SCALE_PROFILE:-normal}}"
UNTIL="${2:-${PG_SCALE_UNTIL:-all}}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
IT="${ROOT}/test/integration"
WORKDIR="${IT}/workdir"
mkdir -p "${WORKDIR}" "${ROOT}/docs/test/reports"
# shellcheck source=test/integration/scripts/_log.sh
source "${IT}/scripts/_log.sh"

if [ "${PROFILE}" = "heavy" ] && [ "${PG_SCALE_HEAVY:-0}" != "1" ]; then
  echo "refusing 'heavy': set PG_SCALE_HEAVY=1 to run the 300k-row profile" >&2
  exit 2
fi

export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
export SRC_USER="${SRC_USER:-u_a7k9m2x4}"
export SRC_PASSWORD="${SRC_PASSWORD:-P_w8q3n5v1r6}"
# The scale orchestrator creates its databases and seeds them as root.
export ROOT_USER="${ROOT_USER:-root}"
export ROOT_PASSWORD="${ROOT_PASSWORD:-rootpass}"
export PG_HOST="${PG_HOST:-127.0.0.1}"
export PG_PORT="${PG_PORT:-15432}"
export PG_USER="${PG_USER:-postgres}"
export PG_PASSWORD="${PG_PASSWORD:-postgres}"
export PG_DB="${PG_DB:-cdc_it}"
export PG_SSLMODE="${PG_SSLMODE:-prefer}"
# Per-profile subdirectories are created by the orchestrator, never by this script.
export CHECKPOINT_PG_SCALE_DIR="${CHECKPOINT_PG_SCALE_DIR:-${WORKDIR}/ckpts-pg-scale}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

absolutize_go_cdc
absolutize_checkpoint_dirs
mkdir -p "${CHECKPOINT_PG_SCALE_DIR}"

# Historical load model: 50 000-row hotspot burst, 45 s peak at batch 200 with
# one writer per profile worker (4 normal / 8 heavy). Pinned so an ambient shell
# value cannot quietly re-tune the numbers a report is compared against.
export PG_SCALE_BURST_ROWS_PER_HOT="${PG_SCALE_BURST_ROWS_PER_HOT:-50000}"
export PG_SCALE_PEAK_SECONDS="${PG_SCALE_PEAK_SECONDS:-45}"
export PG_SCALE_PEAK_BATCH="${PG_SCALE_PEAK_BATCH:-200}"
export PG_SCALE_PEAK_WORKERS="${PG_SCALE_PEAK_WORKERS:-$([ "${PROFILE}" = "heavy" ] && echo 8 || echo 4)}"
export PG_SCALE_RESTART_ROWS="${PG_SCALE_RESTART_ROWS:-100}"
export PG_SCALE_VERIFY_CHUNK="${PG_SCALE_VERIFY_CHUNK:-20000}"

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

log "==> scale knobs"
log "    profile=${PROFILE} until=${UNTIL}"
log "    PG_SCALE_BURST_ROWS_PER_HOT=${PG_SCALE_BURST_ROWS_PER_HOT}"
log "    PG_SCALE_PEAK_SECONDS=${PG_SCALE_PEAK_SECONDS} x PG_SCALE_PEAK_WORKERS=${PG_SCALE_PEAK_WORKERS}"
log "    PG_SCALE_PEAK_BATCH=${PG_SCALE_PEAK_BATCH}"
log "    PG_SCALE_RESTART_ROWS=${PG_SCALE_RESTART_ROWS}"
log "    checkpoint dir base=${CHECKPOINT_PG_SCALE_DIR} (per-profile subdirectory)"
log "    target schemas=tp_pg_scale_${PROFILE}_<nn> sources=db_tp_pg_scale_src_<nn>"

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

log "==> run postgresql scale harness (profile=${PROFILE} until=${UNTIL})"
log "    the orchestrator drops only tp_pg_scale_${PROFILE}_<nn> and clears ckpts-pg-scale/${PROFILE}"
cd "${IT}"
"${PYTHON_BIN}" -m scale.pg_scale_it "${PROFILE}" --until "${UNTIL}"

log "==> postgresql scale done"
