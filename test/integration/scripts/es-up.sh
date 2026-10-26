#!/usr/bin/env bash
# Start docker Elasticsearch 8 (single node, security off) for the ES sink suites.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}"
export ES_IMAGE="${ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:8.15.3}"
export ES_HOST_PORT="${ES_HOST_PORT:-19200}"
export ES_URL="${ES_URL:-http://127.0.0.1:${ES_HOST_PORT}}"
export ES_HEAP="${ES_HEAP:-512m}"

if [ -z "${DOCKER_HOST:-}" ] && [ ! -S /var/run/docker.sock ]; then
  if [ -S "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock" ]; then
    export DOCKER_HOST="unix://${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock"
    log "using rootless DOCKER_HOST=${DOCKER_HOST}"
  fi
fi

if ! docker info >/dev/null 2>&1; then
  log "docker daemon is not available." >&2
  exit 1
fi

log "==> Elasticsearch image ${ES_IMAGE} (project=${COMPOSE_PROJECT_NAME}, url=${ES_URL})"
docker compose pull elasticsearch 2>/dev/null || true
docker compose up -d --wait elasticsearch 2>/dev/null || docker compose up -d elasticsearch

log "==> wait elasticsearch ${ES_URL}"
bash "${ROOT}/scripts/wait-for-es.sh"
log "integration Elasticsearch ready at ${ES_URL}"
