#!/usr/bin/env bash
# Start Doris FE + BE (2.1.x) for the real-Doris integration suite.
# Linux only: the compose file uses host networking so FE redirects to BE are
# reachable from the host-side test process.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

export DORIS_VERSION="${DORIS_VERSION:-2.1.8}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-doris}"
export DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
export DORIS_HTTP_PORT="${DORIS_HTTP_PORT:-8030}"
export DORIS_SQL_PORT="${DORIS_SQL_PORT:-9030}"
export DORIS_USER="${DORIS_USER:-root}"
export DORIS_PASSWORD="${DORIS_PASSWORD:-}"

if [ -z "${DOCKER_HOST:-}" ] && [ ! -S /var/run/docker.sock ]; then
  if [ -S "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock" ]; then
    export DOCKER_HOST="unix://${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock"
    log "using rootless DOCKER_HOST=${DOCKER_HOST}"
  fi
fi

if ! docker info >/dev/null 2>&1; then
  log "docker daemon is not available." >&2
  log "  - rootful: start dockerd / ensure /var/run/docker.sock" >&2
  log "  - rootless: start dockerd-rootless.sh and export DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/docker.sock" >&2
  exit 1
fi

# Doris FE runs BerkeleyDB JE, which registers an inotify watch per meta
# directory. The default 8192 is not enough and FE exits with
# "User limit of inotify watches reached", which is easy to misread as OOM.
# CI runners are passwordless-sudo, so try to raise it; locally this falls back
# to an actionable error instead of a 5-minute BE-registration timeout.
ensure_sysctl() {
  local key="$1" min="$2" want="$3" cur
  cur="$(sysctl -n "${key}" 2>/dev/null || echo 0)"
  if [ "${cur}" -ge "${min}" ]; then
    return 0
  fi
  if sudo -n sysctl -w "${key}=${want}" >/dev/null 2>&1; then
    log "raised ${key} ${cur} -> ${want}"
    return 0
  fi
  log "error: ${key}=${cur}, needs >= ${min}" >&2
  log "       run: sudo sysctl -w ${key}=${want}" >&2
  return 1
}

# Collect all missing kernel knobs up front so one sudo batch can unblock FE+BE.
need_fixctl=0
if ! ensure_sysctl fs.inotify.max_user_watches 65536 1048576; then
  need_sysctl=1
fi
# Instances are only a warning: FE usually needs a few dozen, but a busy
# desktop can exhaust 128 before Doris even starts.
ensure_sysctl fs.inotify.max_user_instances 1024 8192 || true
if ! ensure_sysctl vm.max_map_count 2000000 2000000; then
  need_sysctl=1
fi
if [ "${need_sysctl}" -ne 0 ]; then
  log "Doris FE/BE cannot start until kernel limits are raised (needs passwordless sudo or a one-shot sudo)." >&2
  log "  sudo sysctl -w fs.inotify.max_user_watches=1048576" >&2
  log "  sudo sysctl -w fs.inotify.max_user_instances=8192" >&2
  log "  sudo sysctl -w vm.max_map_count=2000000" >&2
  log "Then re-run: make integration-doris-up" >&2
  exit 1
fi

mkdir -p "${ROOT}/workdir/doris/fe/doris-meta" "${ROOT}/workdir/doris/fe/log" \
  "${ROOT}/workdir/doris/be/storage" "${ROOT}/workdir/doris/be/log"

log "==> Doris ${DORIS_VERSION} FE+BE (host network, project=${COMPOSE_PROJECT_NAME})"
# A stale FE meta image from an interrupted run can advertise an unreachable
# BE. Recreate the bind directories and container state for a deterministic IT.
docker compose -f docker-compose.doris.yml down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf "${ROOT}/workdir/doris/fe/doris-meta" "${ROOT}/workdir/doris/fe/log" \
  "${ROOT}/workdir/doris/be/storage" "${ROOT}/workdir/doris/be/log"
mkdir -p "${ROOT}/workdir/doris/fe/doris-meta" "${ROOT}/workdir/doris/fe/log" \
  "${ROOT}/workdir/doris/be/storage" "${ROOT}/workdir/doris/be/log"
docker compose -f docker-compose.doris.yml pull 2>/dev/null || true
docker compose -f docker-compose.doris.yml up -d fe be

bash "${ROOT}/scripts/wait-for-doris.sh"
log "integration Doris ready at ${DORIS_HOST}:${DORIS_HTTP_PORT}/${DORIS_SQL_PORT} (${DORIS_VERSION})"
