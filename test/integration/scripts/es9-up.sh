#!/usr/bin/env bash
# Start docker Elasticsearch 9 (single node, security off) for the ES v9 sink suite.
# Separate project/port from es-up.sh and es7-up.sh so all three nodes can coexist.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-es9-${MYSQL_TAG}}"
export ES_SERVICE="elasticsearch9"
export ES9_IMAGE="${ES9_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:9.5.2}"
export ES9_HOST_PORT="${ES9_HOST_PORT:-19202}"
export ES9_URL="${ES9_URL:-http://127.0.0.1:${ES9_HOST_PORT}}"
export ES9_HEAP="${ES9_HEAP:-512m}"

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

log "==> Elasticsearch 9 image ${ES9_IMAGE} (project=${COMPOSE_PROJECT_NAME}, url=${ES9_URL})"
docker compose pull elasticsearch9 2>/dev/null || true
# elasticsearch9 is behind the `es9` profile so mysql-up.sh's full `up` leaves
# it alone; name the profile here to start this node (and only this node).
docker compose --profile es9 up -d --wait elasticsearch9 2>/dev/null || docker compose --profile es9 up -d elasticsearch9

log "==> wait elasticsearch9 ${ES9_URL}"
ES_URL="${ES9_URL}" bash "${ROOT}/scripts/wait-for-es.sh"
log "integration Elasticsearch 9 ready at ${ES9_URL}"
