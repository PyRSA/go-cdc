#!/usr/bin/env bash
# Remove only the integration Elasticsearch 9 container (ES8 / ES7 / MySQL keep running).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-es9-${MYSQL_TAG}}"
export ES9_IMAGE="${ES9_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:9.5.2}"

docker compose rm -sf elasticsearch9 >/dev/null 2>&1 || true
