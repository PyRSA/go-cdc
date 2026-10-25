#!/usr/bin/env bash
# Build go-cdc, wait for MySQL + Doris, run the Doris scale integration (normal|heavy).
# Mirrors scale/run-scale.sh with the Doris stack added, so the source DB and the
# Doris FE/BE are both reachable before the orchestrator starts.
set -euo pipefail
PROFILE="${1:-${DORIS_SCALE_PROFILE:-normal}}"
UNTIL="${DORIS_SCALE_UNTIL:-all}"
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

export DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
export DORIS_HTTP_PORT="${DORIS_HTTP_PORT:-8030}"
export DORIS_SQL_PORT="${DORIS_SQL_PORT:-9030}"
export DORIS_USER="${DORIS_USER:-root}"
export DORIS_PASSWORD="${DORIS_PASSWORD:-}"
export DORIS_REPLICATION_NUM="${DORIS_REPLICATION_NUM:-1}"

# Absolute under WORKDIR (layout-derived); CDC_CLI overrides when set.
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
export CHECKPOINT_DORIS_SCALE_DIR="${CHECKPOINT_DORIS_SCALE_DIR:-${WORKDIR}/ckpts-doris-scale}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi
absolutize_go_cdc
absolutize_checkpoint_dirs

# Doris containers live in their own compose project so the MySQL stack used by
# wait-for-doris.sh log dumping does not collide with them.
DORIS_COMPOSE_PROJECT="${DORIS_COMPOSE_PROJECT:-${COMPOSE_PROJECT_NAME:-gocdc-doris}}"

# Tear down on exit — local only. In CI the workflow owns teardown (if: always()).
if [ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ]; then
  cleanup() {
    local status=$?
    if [ "${KEEP_IT_DORIS:-}" != "1" ]; then
      log "==> remove Doris compose"
      COMPOSE_PROJECT_NAME="${DORIS_COMPOSE_PROJECT}" bash "${IT}/scripts/doris-down.sh" || true
    fi
    if [ "${KEEP_IT_MYSQL:-}" != "1" ]; then
      log "==> remove integration MySQL"
      bash "${IT}/scripts/mysql-down.sh" || true
    fi
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

log "==> wait doris"
COMPOSE_PROJECT_NAME="${DORIS_COMPOSE_PROJECT}" bash "${IT}/scripts/wait-for-doris.sh"

log "==> run Doris scale harness (profile=${PROFILE} until=${UNTIL})"
cd "${IT}"
"${PYTHON_BIN}" -m scale.doris_scale_it "${PROFILE}" --until "${UNTIL}"

log "==> done"
