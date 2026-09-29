#!/usr/bin/env bash
# Start docker MySQL for integration tests (supports rootless via DOCKER_HOST).
# Optional: MYSQL_IMAGE / MYSQL_TAG to pin version (CI matrix: 5.7 + 8.0).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

export MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0.36}"
export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}"
export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
export MYSQL_HOST_PORT="${MYSQL_HOST_PORT:-${MYSQL_PORT}}"

if [ -z "${DOCKER_HOST:-}" ] && [ ! -S /var/run/docker.sock ]; then
  if [ -S "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock" ]; then
    export DOCKER_HOST="unix://${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock"
    echo "using rootless DOCKER_HOST=${DOCKER_HOST}"
  fi
fi

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is not available." >&2
  echo "  - rootful: start dockerd / ensure /var/run/docker.sock" >&2
  echo "  - rootless: start dockerd-rootless.sh and export DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/docker.sock" >&2
  exit 1
fi

echo "==> MySQL image ${MYSQL_IMAGE} (tag=${MYSQL_TAG}, project=${COMPOSE_PROJECT_NAME})"
# Recreate volume so init scripts re-run when switching versions.
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose pull mysql 2>/dev/null || true
docker compose up -d --wait 2>/dev/null || docker compose up -d

export WAIT_MYSQL_USER="${WAIT_MYSQL_USER:-root}"
export WAIT_MYSQL_PASSWORD="${WAIT_MYSQL_PASSWORD:-rootpass}"
bash "${ROOT}/scripts/wait-for-mysql.sh"
echo "integration MySQL ready at ${MYSQL_HOST}:${MYSQL_PORT} (${MYSQL_IMAGE})"
