#!/usr/bin/env bash
# Stop and remove the real-Doris integration containers and volumes.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

export DORIS_VERSION="${DORIS_VERSION:-2.1.8}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-doris}"

log "==> stopping Doris (project=${COMPOSE_PROJECT_NAME})"
docker compose -f docker-compose.doris.yml down -v --remove-orphans >/dev/null 2>&1 || true
