#!/usr/bin/env bash
# Start docker Kafka (KRaft) alongside the integration compose project.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}"
export KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.8.1}"
export KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-19092}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:${KAFKA_HOST_PORT}}"

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

log "==> Kafka image ${KAFKA_IMAGE} (project=${COMPOSE_PROJECT_NAME}, port=${KAFKA_HOST_PORT})"
docker compose pull kafka 2>/dev/null || true
docker compose up -d --wait kafka 2>/dev/null || docker compose up -d kafka

log "==> wait kafka ${KAFKA_BROKERS}"
bash "${ROOT}/scripts/wait-for-kafka.sh"
log "integration Kafka ready at ${KAFKA_BROKERS}"
