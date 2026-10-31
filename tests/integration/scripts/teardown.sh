#!/usr/bin/env bash
# Remove the containers (and volumes) of one sink run.
#
#   bash scripts/teardown.sh <sink> [variant]
#
# Resolves the compose files and the project name exactly like setup.sh, so a
# teardown can only ever remove the stack the matching setup created. Safe to
# call from an `if: always()` step: a missing project is not an error.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=tests/integration/scripts/_log.sh
source "${SCRIPT_DIR}/_log.sh"

SINK="${1:-}"
VARIANT="${2:-}"
if [ -z "${SINK}" ]; then
  echo "usage: teardown.sh <sink> [variant]" >&2
  exit 2
fi

export COMPOSE_PROJECT_NAME="$(it_project_name "${SINK}")"
COMPOSE=()
while IFS= read -r file; do
  COMPOSE+=(-f "${file}")
done < <(it_compose_files "${SINK}")

if [ -z "${DOCKER_HOST:-}" ] && [ ! -S /var/run/docker.sock ]; then
  if [ -S "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock" ]; then
    export DOCKER_HOST="unix://${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock"
  fi
fi

if ! docker info >/dev/null 2>&1; then
  log "docker daemon is not available; nothing to tear down"
  exit 0
fi

log "==> down ${SINK}${VARIANT:+ (${VARIANT})} project=${COMPOSE_PROJECT_NAME}"
# Profiles are named too: a profiled service (ES7/ES9) is not stopped by a plain
# down, and leaving it behind would hold its host port for the next run.
docker compose "${COMPOSE[@]}" --profile es7 --profile es9 \
  down -v --remove-orphans --timeout 20 >/dev/null 2>&1 ||
  docker compose "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

# Doris writes through bind mounts, not volumes; drop them so the next FE starts
# from an empty meta image instead of a half-registered BE.
if [ "${SINK}" = "doris" ] && [ "${IT_KEEP_DORIS_DATA:-0}" != "1" ]; then
  rm -rf "${IT}/workdir/doris"
fi

log "==> ${SINK} removed"
