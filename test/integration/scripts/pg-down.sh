#!/usr/bin/env bash
# Remove only the integration PostgreSQL container (MySQL/ES keep running).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}"
export PG_IMAGE="${PG_IMAGE:-postgres:16.4}"

# -v drops the version-bound data directory, so the next up() starts clean.
docker compose rm -sfv postgres >/dev/null 2>&1 || true
