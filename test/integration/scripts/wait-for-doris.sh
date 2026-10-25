#!/usr/bin/env bash
# Wait until the Doris FE accepts SQL and at least one BE is alive.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

HOST="${DORIS_HOST:-127.0.0.1}"
SQL_PORT="${DORIS_SQL_PORT:-9030}"
HTTP_PORT="${DORIS_HTTP_PORT:-8030}"
USER="${DORIS_USER:-root}"
PASS="${DORIS_PASSWORD:-}"
FE_TRIES="${WAIT_DORIS_FE_TRIES:-180}"
BE_TRIES="${WAIT_DORIS_BE_TRIES:-300}"

if ! command -v mysql >/dev/null 2>&1; then
  log "mysql client is required to wait for Doris (install mysql-client)." >&2
  exit 1
fi

MYSQL_ARGS=(-h "${HOST}" -P "${SQL_PORT}" -u "${USER}" --connect-timeout=5)
if [ -n "${PASS}" ]; then
  MYSQL_ARGS+=("-p${PASS}")
fi

dump_logs() {
  log "dumping Doris container logs" >&2
  docker compose -f "${ROOT}/docker-compose.doris.yml" logs --no-color --tail=200 fe be >&2 || true
}

log "waiting for Doris FE ${HOST}:${SQL_PORT} ..."
fe_ready=0
for _ in $(seq 1 "${FE_TRIES}"); do
  if mysql "${MYSQL_ARGS[@]}" -N -B -e "SELECT 1" >/dev/null 2>&1; then
    fe_ready=1
    break
  fi
  sleep 2
done
if [ "${fe_ready}" -ne 1 ]; then
  dump_logs
  log "Doris FE not ready after $((FE_TRIES * 2))s" >&2
  exit 1
fi
log "Doris FE is up; waiting for an alive BE ..."

backend_count() {
  local count
  count="$(mysql "${MYSQL_ARGS[@]}" -N -B -e \
    "SELECT COUNT(*) FROM backends() WHERE Alive = 1" 2>/dev/null || true)"
  if [[ "${count}" =~ ^[0-9]+$ ]]; then
    echo "${count}"
    return
  fi
  # Fallback for Doris variants without the backends() table function.
  mysql "${MYSQL_ARGS[@]}" -B -e "SHOW BACKENDS" 2>/dev/null |
    awk -F'\t' 'NR==1 { for (i=1;i<=NF;i++) if (tolower($i)=="alive") c=i; next }
                c && tolower($c)=="true" { n++ }
                END { print n+0 }'
}

be_ready=0
for _ in $(seq 1 "${BE_TRIES}"); do
  if [ "$(backend_count)" -gt 0 ]; then
    be_ready=1
    break
  fi
  sleep 2
done
if [ "${be_ready}" -ne 1 ]; then
  dump_logs
  log "Doris BE not alive after $((BE_TRIES * 2))s (FE HTTP ${HOST}:${HTTP_PORT})" >&2
  exit 1
fi
log "Doris FE/BE ready"
