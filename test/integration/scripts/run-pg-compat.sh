#!/usr/bin/env bash
# Build go-cdc, ensure MySQL is reachable, then run the PostgreSQL 14-18
# compatibility matrix: one shared E2E suite (harness.pg_it, TP-P01..TP-P20)
# against each server major version, in turn.
#
#   bash scripts/run-pg-compat.sh                     # PG 14 15 16 17 18
#   bash scripts/run-pg-compat.sh 16 17               # a subset, for a re-run
#   bash scripts/run-pg-compat.sh 16 --cases TP-P01   # one test point per version
#   PG_COMPAT_IMAGE_16=postgres:16.4 bash scripts/run-pg-compat.sh 16   # pin a minor
#
# Unlike run-pg-e2e.sh this script does not use one PostgreSQL container: the
# matrix owns the compose service and switches its image through pg-up.sh, which
# drops the volume when the image changes (an initialized data directory is
# version-bound). MySQL is started once by the caller (scripts/mysql-up.sh) and
# only waited on here.
#
# Report: docs/test/reports/mysql-postgresql-compat.md (matrix) plus one
# per-version E2E report, docs/test/reports/mysql-postgresql-func-pg<major>.md.
# A version that could not run is recorded as `ENV BLOCKED` / `NOT RUN`; only a
# real run of all five rows makes PG-209 complete.
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
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

absolutize_go_cdc

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

log "==> postgresql 14-18 compatibility matrix"
log "    one suite per version: python -m harness.pg_it (TP-P01..TP-P20)"
log "    per-version checkpoints under ${WORKDIR}/ckpts-pg-compat-<major>"
cd "${IT}"
"${PYTHON_BIN}" -m harness.pg_compat "$@"

log "==> postgresql compatibility matrix done"
