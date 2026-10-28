#!/usr/bin/env bash
# Wait until the ES8 cluster reports yellow/green from the host or via compose exec.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}"
export ES_URL="${ES_URL:-http://127.0.0.1:${ES_HOST_PORT:-19200}}"
# Compose service to probe through docker exec when the host URL is not routable.
export ES_SERVICE="${ES_SERVICE:-elasticsearch}"
deadline=$((SECONDS + ${WAIT_ES_SECONDS:-180}))
health_path='_cluster/health?wait_for_status=yellow&timeout=5s'

host_ok() {
  python3 - "$ES_URL" "$health_path" <<'PY' 2>/dev/null
import json
import sys
import urllib.request

url, path = sys.argv[1], sys.argv[2]
with urllib.request.urlopen(f"{url}/{path}", timeout=10) as resp:
    body = json.load(resp)
status = body.get("status")
if status not in ("yellow", "green"):
    raise SystemExit(1)
PY
}

while (( SECONDS < deadline )); do
  if host_ok; then
    exit 0
  fi
  if docker compose exec -T "${ES_SERVICE}" \
    curl -fsS "http://127.0.0.1:9200/${health_path}" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
done

log "timeout waiting for Elasticsearch at ${ES_URL}" >&2
docker compose logs --tail=40 "${ES_SERVICE}" >&2 || true
exit 1
