#!/usr/bin/env bash
# Wait until MySQL accepts connections.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=test/integration/scripts/_log.sh
source "${SCRIPT_DIR}/_log.sh"

HOST="${MYSQL_HOST:-127.0.0.1}"
PORT="${MYSQL_PORT:-13306}"
USER="${WAIT_MYSQL_USER:-root}"
PASS="${WAIT_MYSQL_PASSWORD:-rootpass}"
TRIES="${WAIT_MYSQL_TRIES:-60}"

log "waiting for mysql ${HOST}:${PORT} ..."
for i in $(seq 1 "${TRIES}"); do
  if mysqladmin ping -h"${HOST}" -P"${PORT}" -u"${USER}" -p"${PASS}" --silent 2>/dev/null; then
    log "mysql is up"
    exit 0
  fi
  # fallback: python pymysql
  if python3 - <<PY 2>/dev/null
import pymysql,sys
try:
  pymysql.connect(host="${HOST}",port=int("${PORT}"),user="${USER}",password="${PASS}",connect_timeout=2)
  sys.exit(0)
except Exception:
  sys.exit(1)
PY
  then
    log "mysql is up"
    exit 0
  fi
  sleep 2
done
log "mysql not ready after ${TRIES} tries" >&2
exit 1
