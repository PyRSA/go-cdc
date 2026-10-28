#!/usr/bin/env bash
# Remove only the integration Elasticsearch 7 container (ES8 / MySQL keep running).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-es7-${MYSQL_TAG}}"
export ES7_IMAGE="${ES7_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:7.17.24}"

docker compose rm -sf elasticsearch7 >/dev/null 2>&1 || true
