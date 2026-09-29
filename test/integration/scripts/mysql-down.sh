#!/usr/bin/env bash
# Stop and remove integration MySQL.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}"
export MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0.36}"

docker compose down -v --remove-orphans
