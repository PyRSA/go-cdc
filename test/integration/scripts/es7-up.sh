#!/usr/bin/env bash
# Start docker Elasticsearch 7.17 (single node, security off) for the ES v7 sink suite.
# Separate project/port from es-up.sh so the ES8 node can keep running.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-es7-${MYSQL_TAG}}"
export ES_SERVICE="elasticsearch7"
export ES7_IMAGE="${ES7_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:7.17.24}"
export ES7_HOST_PORT="${ES7_HOST_PORT:-19201}"
export ES7_URL="${ES7_URL:-http://127.0.0.1:${ES7_HOST_PORT}}"
export ES7_HEAP="${ES7_HEAP:-512m}"

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

log "==> Elasticsearch 7 image ${ES7_IMAGE} (project=${COMPOSE_PROJECT_NAME}, url=${ES7_URL})"
docker compose pull elasticsearch7 2>/dev/null || true
# elasticsearch7 is behind the `es7` profile so mysql-up.sh's full `up` leaves
# it alone; name the profile here to start this node (and only this node).
docker compose --profile es7 up -d --wait elasticsearch7 2>/dev/null || docker compose --profile es7 up -d elasticsearch7

log "==> wait elasticsearch7 ${ES7_URL}"
ES_URL="${ES7_URL}" bash "${ROOT}/scripts/wait-for-es.sh"
log "integration Elasticsearch 7 ready at ${ES7_URL}"
