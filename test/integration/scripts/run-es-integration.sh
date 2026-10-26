#!/usr/bin/env bash
# Go-level Elasticsearch sink integration tests against a real ES8 (`-tags esintegration`).
# Only Elasticsearch is required; MySQL is not used by this layer.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
IT="${ROOT}/test/integration"
# shellcheck source=test/integration/scripts/_log.sh
source "${IT}/scripts/_log.sh"

export ES_URL="${ES_URL:-http://127.0.0.1:${ES_HOST_PORT:-19200}}"

log "==> ES integration (real cluster) ES_URL=${ES_URL}"
cd "${ROOT}"
go test -tags esintegration -count=1 -timeout 20m -v \
  -run 'TestES' ./connector/sink/elasticsearch/
log "==> ES integration done"
