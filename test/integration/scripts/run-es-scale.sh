#!/usr/bin/env bash
# Build go-cdc, ensure MySQL + Elasticsearch 8 are up, run the ES8 scale suite.
#   bash scripts/run-es-scale.sh normal   # 5 DBs / 20 tables (dev + CI)
#   bash scripts/run-es-scale.sh heavy    # 10 DBs / 50 tables + hotspot burst/peak
# Same profiles and generators as the MySQL scale suite (scale/profiles.py); the
# sink is Elasticsearch and every check reads documents back over HTTP.
set -euo pipefail
PROFILE="${1:-${ES_SCALE_PROFILE:-${SCALE_PROFILE:-normal}}}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
IT="${ROOT}/test/integration"
WORKDIR="${IT}/workdir"
mkdir -p "${WORKDIR}" "${ROOT}/docs/test/reports"
# shellcheck source=test/integration/scripts/_log.sh
source "${IT}/scripts/_log.sh"

export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
# Anonymous principals (must match mysql/init/01-databases.sql).
export SRC_USER="${SRC_USER:-u_a7k9m2x4}"
export SRC_PASSWORD="${SRC_PASSWORD:-P_w8q3n5v1r6}"
export ROOT_USER="${ROOT_USER:-root}"
export ROOT_PASSWORD="${ROOT_PASSWORD:-rootpass}"
export ES_HOST_PORT="${ES_HOST_PORT:-19200}"
export ES_URL="${ES_URL:-http://127.0.0.1:${ES_HOST_PORT}}"
# Every final table is routed to `db_tp_scale_snk_<nn>`; that namespace is also
# the index cleanup prefix, so the ES scale suite never touches other suites.
export ES_SCALE_INDEX_PREFIX="${ES_SCALE_INDEX_PREFIX:-db_tp_scale_snk_*}"
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

log "==> run elasticsearch scale harness (profile=${PROFILE})"
log "    budget: SCALE_TOTAL_TIMEOUT=${SCALE_TOTAL_TIMEOUT:-1800}s (heavy is explicit opt-in)"
cd "${IT}"
"${PYTHON_BIN}" -m scale.es_scale_it "${PROFILE}"

log "==> elasticsearch scale done"
