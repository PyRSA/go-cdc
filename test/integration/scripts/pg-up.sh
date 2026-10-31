#!/usr/bin/env bash
# Start the integration PostgreSQL sink target (single node, port ${PG_HOST_PORT}).
# PG_IMAGE selects the server version, e.g.:
#   PG_IMAGE=postgres:14.18 bash scripts/pg-up.sh
# A major-version switch cannot reuse an initialized data directory, so the
# container and its volume are dropped when the running image differs.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}"
export PG_IMAGE="${PG_IMAGE:-postgres:16.4}"
export PG_HOST_PORT="${PG_HOST_PORT:-15432}"
export PG_PORT="${PG_PORT:-${PG_HOST_PORT}}"
export PG_USER="${PG_USER:-postgres}"
export PG_PASSWORD="${PG_PASSWORD:-postgres}"
export PG_DB="${PG_DB:-cdc_it}"

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

log "==> PostgreSQL image ${PG_IMAGE} (project=${COMPOSE_PROJECT_NAME}, port=${PG_HOST_PORT})"
existing="$(docker compose ps -q postgres 2>/dev/null || true)"
if [ -n "${existing}" ]; then
  running_image="$(docker inspect --format '{{.Config.Image}}' "${existing}" 2>/dev/null || true)"
  if [ "${running_image}" != "${PG_IMAGE}" ]; then
    log "    replacing ${running_image} with ${PG_IMAGE} (data directory is version-bound)"
    docker compose rm -sfv postgres >/dev/null 2>&1 || true
  fi
fi
docker compose pull postgres 2>/dev/null || true
docker compose up -d --wait postgres 2>/dev/null || docker compose up -d postgres

bash "${ROOT}/scripts/wait-for-postgres.sh"
log "integration PostgreSQL ready at 127.0.0.1:${PG_HOST_PORT} (db=${PG_DB}, ${PG_IMAGE})"
