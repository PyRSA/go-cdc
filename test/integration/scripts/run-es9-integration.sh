#!/usr/bin/env bash
# Go-level Elasticsearch 9 integration tests for the v9 client adapter
# (`-tags esintegration`). Only the ES 9 node is required; MySQL is not used.
#
# The ES 8 suite runs the same test binary with `-run 'TestES'` and the ES 7
# suite with `-run 'TestElasticsearch7'`, so the ES 9 tests are named
# `TestElasticsearch9*` and never run against another node by accident.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
IT="${ROOT}/test/integration"
# shellcheck source=test/integration/scripts/_log.sh
source "${IT}/scripts/_log.sh"

export ES9_URL="${ES9_URL:-http://127.0.0.1:${ES9_HOST_PORT:-19202}}"

log "==> ES9 integration (real cluster) ES9_URL=${ES9_URL}"
cd "${ROOT}"
go test -tags esintegration -count=1 -timeout 20m -v \
  -run 'TestElasticsearch9' ./connector/sink/elasticsearch/
log "==> ES9 integration done"
