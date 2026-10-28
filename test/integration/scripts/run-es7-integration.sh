#!/usr/bin/env bash
# Go-level Elasticsearch 7 integration tests for the v7 client adapter
# (`-tags esintegration`). Only the ES 7 node is required; MySQL is not used.
#
# The ES 8 suite runs the same test binary with `-run 'TestES'`, so the ES 7
# tests are named `TestElasticsearch7*` and never run against an ES 8 node.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
IT="${ROOT}/test/integration"
# shellcheck source=test/integration/scripts/_log.sh
source "${IT}/scripts/_log.sh"

export ES7_URL="${ES7_URL:-http://127.0.0.1:${ES7_HOST_PORT:-19201}}"

log "==> ES7 integration (real cluster) ES7_URL=${ES7_URL}"
cd "${ROOT}"
go test -tags esintegration -count=1 -timeout 20m -v \
  -run 'TestElasticsearch7' ./connector/sink/elasticsearch/
log "==> ES7 integration done"
