#!/usr/bin/env bash
# Build go-cdc, ensure MySQL + Elasticsearch 8 are up, run the ES8 E2E suite.
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
export SINK_USER="${SINK_USER:-u_b3j6c8y1}"
export SINK_PASSWORD="${SINK_PASSWORD:-P_z2h5t9k4m7}"
export ES_SRC_DB="${ES_SRC_DB:-db_tp_es_src}"
export ES_SNK_DB="${ES_SNK_DB:-db_tp_es_snk}"
export ES_HOST_PORT="${ES_HOST_PORT:-19200}"
export ES_URL="${ES_URL:-http://127.0.0.1:${ES_HOST_PORT}}"
# Cleanup namespace = the logical sink DB, i.e. the prefix of every derived index
# name (`db_tp_es_snk.<table>[-hash]`). See harness/config.py.
export ES_INDEX_PREFIX="${ES_INDEX_PREFIX:-${ES_SNK_DB}}"
export CHECKPOINT_ES_DIR="${CHECKPOINT_ES_DIR:-${WORKDIR}/ckpts-es}"
export CHECKPOINT_ES_RESTART_DIR="${CHECKPOINT_ES_RESTART_DIR:-${WORKDIR}/ckpts-es-restart}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

absolutize_go_cdc
absolutize_checkpoint_dirs
mkdir -p "${CHECKPOINT_ES_DIR}" "${CHECKPOINT_ES_RESTART_DIR}"

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

log "==> reset elasticsearch checkpoints"
rm -rf "${CHECKPOINT_ES_DIR}" "${CHECKPOINT_ES_RESTART_DIR}"
mkdir -p "${CHECKPOINT_ES_DIR}" "${CHECKPOINT_ES_RESTART_DIR}"

log "==> run elasticsearch sink e2e harness (TP-E01..)"
log "    source DB is (re)created by the harness; indices are cleaned per test"
cd "${IT}"
"${PYTHON_BIN}" -m harness.es_it

log "==> elasticsearch e2e done"
