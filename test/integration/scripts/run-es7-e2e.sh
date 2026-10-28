#!/usr/bin/env bash
# Build go-cdc, ensure MySQL + Elasticsearch 7 are up, run the CDC E2E suite with
# sink.version=7. Same harness and test points as the ES8 suite (TP-E01..E10):
# MySQL source -> snapshot + binlog -> runner -> ES7. No load/scale layers.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
IT="${ROOT}/test/integration"
WORKDIR="${IT}/workdir"
mkdir -p "${WORKDIR}" "${ROOT}/docs/test/reports"
# shellcheck source=test/integration/scripts/_log.sh
source "${IT}/scripts/_log.sh"

export MYSQL_TAG="${MYSQL_TAG:-80}"
# Project that owns the MySQL container. Locally that is mysql-up.sh's default
# project; a CI job that pins COMPOSE_PROJECT_NAME owns both containers.
MYSQL_PROJECT="${MYSQL_PROJECT:-${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}}"
export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
export SRC_USER="${SRC_USER:-u_a7k9m2x4}"
export SRC_PASSWORD="${SRC_PASSWORD:-P_w8q3n5v1r6}"
export SINK_USER="${SINK_USER:-u_b3j6c8y1}"
export SINK_PASSWORD="${SINK_PASSWORD:-P_z2h5t9k4m7}"
export ES_SRC_DB="${ES_SRC_DB:-db_tp_es_src}"
export ES_SNK_DB="${ES_SNK_DB:-db_tp_es_snk}"
# The ES7 node lives in its own compose project (es7-up.sh) on host port 19201.
# go-cdc runs on the host, so the two projects only share published ports; no
# docker network is assumed between them.
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-es7-${MYSQL_TAG}}"
export ES_SERVICE="${ES_SERVICE:-elasticsearch7}"
export ES7_HOST_PORT="${ES7_HOST_PORT:-19201}"
# Same precedence as the other ES7 scripts: explicit ES_URL, then ES7_URL, then
# the host port. es7-up.sh and run-es7-integration.sh honour ES7_URL too.
export ES_URL="${ES_URL:-${ES7_URL:-http://127.0.0.1:${ES7_HOST_PORT}}}"
# The point of this run: pick the v7 adapter by configuration, never by probing.
export ES_SINK_VERSION=7
export ES_EXPECT_MAJOR=7
export ES_INDEX_PREFIX="${ES_INDEX_PREFIX:-${ES_SNK_DB}}"
# Separate report + checkpoints so an ES8 run on the same box is not overwritten.
export REPORT_ES="${REPORT_ES:-${ROOT}/docs/test/reports/mysql-elasticsearch7.md}"
export CHECKPOINT_ES_DIR="${CHECKPOINT_ES_DIR:-${WORKDIR}/ckpts-es7}"
export CHECKPOINT_ES_RESTART_DIR="${CHECKPOINT_ES_RESTART_DIR:-${WORKDIR}/ckpts-es7-restart}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

absolutize_go_cdc
absolutize_checkpoint_dirs
mkdir -p "${CHECKPOINT_ES_DIR}" "${CHECKPOINT_ES_RESTART_DIR}"

# Local-only: tear both projects down on exit unless KEEP_IT_MYSQL=1.
if [ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ]; then
  cleanup() {
    local status=$?
    if [ "${KEEP_IT_MYSQL:-}" = "1" ]; then
      exit "${status}"
    fi
    log "==> remove integration compose (es7+mysql)"
    bash "${IT}/scripts/es7-down.sh" || true
    COMPOSE_PROJECT_NAME="${MYSQL_PROJECT}" bash "${IT}/scripts/mysql-down.sh" || true
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

log "==> wait elasticsearch 7 (${ES_URL})"
bash "${IT}/scripts/wait-for-es.sh"

log "==> reset elasticsearch 7 checkpoints"
rm -rf "${CHECKPOINT_ES_DIR}" "${CHECKPOINT_ES_RESTART_DIR}"
mkdir -p "${CHECKPOINT_ES_DIR}" "${CHECKPOINT_ES_RESTART_DIR}"

log "==> run elasticsearch 7 sink e2e harness (TP-E01.., sink.version=7)"
log "    source DB is (re)created by the harness; indices are cleaned per test"
cd "${IT}"
"${PYTHON_BIN}" -m harness.es_it

log "==> elasticsearch 7 e2e done"
