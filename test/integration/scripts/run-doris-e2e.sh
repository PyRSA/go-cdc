#!/usr/bin/env bash
# Build go-cdc, ensure MySQL+Doris are up, run:
#   1) MySQL→Doris pipeline harness (TP-DR01..DR04) — same shape as kafka_it
#   2) Real-Doris sink Go suite (doris_integration tag) — sink-level checks
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
export SRC_DB="${SRC_DB:-${FUNC_SRC_DB}}"

export DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
export DORIS_HTTP_PORT="${DORIS_HTTP_PORT:-8030}"
export DORIS_SQL_PORT="${DORIS_SQL_PORT:-9030}"
export DORIS_USER="${DORIS_USER:-root}"
export DORIS_PASSWORD="${DORIS_PASSWORD:-}"
export DORIS_REPLICATION_NUM="${DORIS_REPLICATION_NUM:-1}"
export DORIS_SNK_DB="${DORIS_SNK_DB:-db_tp_doris_snk}"
export DORIS_BUCKETS="${DORIS_BUCKETS:-auto}"

export CHECKPOINT_DORIS_DIR="${CHECKPOINT_DORIS_DIR:-${WORKDIR}/ckpts-doris}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

absolutize_go_cdc
absolutize_checkpoint_dirs
mkdir -p "${CHECKPOINT_DORIS_DIR}"

# Local-only: tear down mysql + doris on exit unless KEEP_IT_MYSQL=1 / KEEP_IT_DORIS=1.
if [ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ]; then
  cleanup() {
    local status=$?
    if [ "${KEEP_IT_DORIS:-}" != "1" ]; then
      log "==> remove Doris compose"
      bash "${IT}/scripts/doris-down.sh" || true
    fi
    if [ "${KEEP_IT_MYSQL:-}" != "1" ]; then
      log "==> remove MySQL compose"
      bash "${IT}/scripts/mysql-down.sh" || true
    fi
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

log "==> wait doris"
bash "${IT}/scripts/wait-for-doris.sh"

log "==> seed doris pipeline suite tables"
mysql_as "${SRC_USER}" "${SRC_PASSWORD}" < "${IT}/fixtures/seed_doris.sql"
rm -rf "${CHECKPOINT_DORIS_DIR}"
mkdir -p "${CHECKPOINT_DORIS_DIR}"

status=0

log "==> run MySQL→Doris pipeline harness (TP-DR01..DR04)"
cd "${IT}"
if ! "${PYTHON_BIN}" -m harness.doris_it; then
  status=1
  log "pipeline harness FAILED (continuing to sink Go suite)"
fi

log "==> run sink-level Go suite (doris_integration)"
SINK_LOG="${WORKDIR}/doris-integration.log"
set +e
(cd "${ROOT}" && go test -tags doris_integration ./test/integration/doris/ -count=1 -v -timeout 20m) \
  2>&1 | tee "${SINK_LOG}"
go_status=${PIPESTATUS[0]}
set -e
if [ "${go_status}" -ne 0 ]; then
  status=1
  log "sink Go suite FAILED (exit=${go_status})"
fi

log "Doris sink Go log: ${SINK_LOG}"
log "Doris pipeline report: ${ROOT}/docs/test/reports/mysql-doris.md"
if [ "${status}" -ne 0 ]; then
  log "==> doris e2e FAILED"
  exit "${status}"
fi
log "==> doris e2e done"
