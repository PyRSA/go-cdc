#!/usr/bin/env bash
# Wait until the integration PostgreSQL service accepts connections.
# Probes through `docker compose exec` first (no host psql needed), then the host.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${SCRIPT_DIR}/_log.sh"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}"
export PG_HOST_PORT="${PG_HOST_PORT:-15432}"
export PG_PORT="${PG_PORT:-${PG_HOST_PORT}}"
export PG_USER="${PG_USER:-postgres}"
export PG_PASSWORD="${PG_PASSWORD:-postgres}"
export PG_DB="${PG_DB:-cdc_it}"
deadline=$((SECONDS + ${WAIT_PG_SECONDS:-180}))

in_container_ok() {
  docker compose exec -T postgres \
    pg_isready -U "${PG_USER}" -d "${PG_DB}" -q >/dev/null 2>&1
}

host_ok() {
  "${PYTHON_BIN:-python3}" - <<PY 2>/dev/null
import sys
try:
    import psycopg2
except ImportError:
    sys.exit(1)
try:
    psycopg2.connect(host="127.0.0.1", port=int("${PG_PORT}"), user="${PG_USER}",
                     password="${PG_PASSWORD}", dbname="${PG_DB}",
                     connect_timeout=3).close()
except Exception:
    sys.exit(1)
PY
}

log "waiting for postgres 127.0.0.1:${PG_PORT} (db=${PG_DB}) ..."
while (( SECONDS < deadline )); do
  if in_container_ok; then
    log "postgres is up"
    exit 0
  fi
  if host_ok; then
    log "postgres is up"
    exit 0
  fi
  sleep 2
done

log "timeout waiting for PostgreSQL at 127.0.0.1:${PG_PORT}" >&2
docker compose logs --tail=40 postgres >&2 || true
exit 1
