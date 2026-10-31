#!/usr/bin/env bash
# Readiness probes: block until the containers a sink needs accept work.
#
#   bash scripts/wait.sh <sink> [variant]
#
# One implementation, two callers:
#
#   setup.sh   sources this file and calls it_wait_sink, so raising the stack
#              still blocks before returning - that is what every reusable
#              workflow relies on (`Setup stack` then `Run suites`);
#   wait.sh    runs on its own against a stack that is already up: a warm start
#              (`IT_REUSE=1 setup.sh`), a `docker compose up` you started by
#              hand, or a runner that raised the stack in an earlier step.
#
# Every probe tries the host first (that is what the suites use) and falls back
# to `docker compose exec`, so a runner without mysql-client/psql still works.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/integration/scripts/logger.sh
source "${SCRIPT_DIR}/logger.sh"

it_wait_mysql() {
  local deadline=$((SECONDS + ${WAIT_MYSQL_SECONDS:-180}))
  local user="${WAIT_MYSQL_USER:-root}" pass="${WAIT_MYSQL_PASSWORD:-rootpass}"
  log "waiting for mysql ${MYSQL_HOST}:${MYSQL_PORT} ..."
  while ((SECONDS < deadline)); do
    if mysqladmin ping -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${user}" -p"${pass}" \
      --silent >/dev/null 2>&1; then
      log "mysql is up"
      return 0
    fi
    if "${PYTHON_BIN}" - "$MYSQL_HOST" "$MYSQL_PORT" "$user" "$pass" <<'PY' >/dev/null 2>&1
import sys
try:
    import pymysql
except ImportError:
    sys.exit(1)
host, port, user, password = sys.argv[1:5]
try:
    pymysql.connect(host=host, port=int(port), user=user, password=password,
                    connect_timeout=2).close()
except Exception:
    sys.exit(1)
PY
    then
      log "mysql is up"
      return 0
    fi
    sleep 2
  done
  log "timeout waiting for MySQL at ${MYSQL_HOST}:${MYSQL_PORT}" >&2
  docker compose "${COMPOSE[@]}" logs --tail=40 mysql >&2 || true
  return 1
}

it_wait_postgres() {
  local deadline=$((SECONDS + ${WAIT_PG_SECONDS:-180}))
  export PG_HOST="${PG_HOST:-127.0.0.1}"
  export PG_PORT="${PG_PORT:-${PG_HOST_PORT:-15432}}"
  export PG_USER="${PG_USER:-postgres}"
  export PG_PASSWORD="${PG_PASSWORD:-postgres}"
  export PG_DB="${PG_DB:-cdc_it}"
  log "waiting for postgres ${PG_HOST}:${PG_PORT} (db=${PG_DB}) ..."
  while ((SECONDS < deadline)); do
    if docker compose "${COMPOSE[@]}" exec -T postgres \
      pg_isready -U "${PG_USER}" -d "${PG_DB}" -q >/dev/null 2>&1; then
      log "postgres is up"
      return 0
    fi
    if "${PYTHON_BIN}" - "${PG_PORT}" "${PG_USER}" "${PG_PASSWORD}" "${PG_DB}" <<'PY' >/dev/null 2>&1
import sys
try:
    import psycopg2
except ImportError:
    sys.exit(1)
port, user, password, db = sys.argv[1:5]
try:
    psycopg2.connect(host="127.0.0.1", port=int(port), user=user,
                     password=password, dbname=db, connect_timeout=3).close()
except Exception:
    sys.exit(1)
PY
    then
      log "postgres is up"
      return 0
    fi
    sleep 2
  done
  log "timeout waiting for PostgreSQL at ${PG_HOST}:${PG_PORT}" >&2
  docker compose "${COMPOSE[@]}" logs --tail=40 postgres >&2 || true
  return 1
}

it_wait_kafka() {
  export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:${KAFKA_HOST_PORT:-19092}}"
  local deadline=$((SECONDS + ${WAIT_KAFKA_SECONDS:-90}))
  log "waiting for kafka ${KAFKA_BROKERS} ..."
  while ((SECONDS < deadline)); do
    if "${PYTHON_BIN}" - "${KAFKA_BROKERS}" <<'PY' >/dev/null 2>&1
import sys
try:
    from kafka import KafkaConsumer
except ImportError:
    sys.exit(1)
consumer = KafkaConsumer(bootstrap_servers=sys.argv[1].split(","), consumer_timeout_ms=2000)
consumer.topics()
consumer.close()
PY
    then
      log "kafka is up"
      return 0
    fi
    if docker compose "${COMPOSE[@]}" exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list \
      >/dev/null 2>&1; then
      log "kafka is up"
      return 0
    fi
    sleep 2
  done
  log "timeout waiting for Kafka at ${KAFKA_BROKERS}" >&2
  docker compose "${COMPOSE[@]}" logs --tail=40 kafka >&2 || true
  return 1
}

it_wait_es() {
  local service="${ES_SERVICE:-elasticsearch}"
  local deadline=$((SECONDS + ${WAIT_ES_SECONDS:-180}))
  local path='_cluster/health?wait_for_status=yellow&timeout=5s'
  log "waiting for elasticsearch ${ES_URL} (service=${service}) ..."
  while ((SECONDS < deadline)); do
    if "${PYTHON_BIN}" - "${ES_URL}" "${path}" <<'PY' >/dev/null 2>&1
import json
import sys
import urllib.request

url, path = sys.argv[1], sys.argv[2]
with urllib.request.urlopen(f"{url}/{path}", timeout=10) as resp:
    body = json.load(resp)
if body.get("status") not in ("yellow", "green"):
    raise SystemExit(1)
PY
    then
      log "elasticsearch is up"
      return 0
    fi
    if docker compose "${COMPOSE[@]}" exec -T "${service}" \
      curl -fsS "http://127.0.0.1:9200/${path}" >/dev/null 2>&1; then
      log "elasticsearch is up"
      return 0
    fi
    sleep 2
  done
  log "timeout waiting for Elasticsearch at ${ES_URL}" >&2
  docker compose "${COMPOSE[@]}" logs --tail=40 "${service}" >&2 || true
  return 1
}

# Doris FE registers an inotify watch per meta directory; the default 8192 is
# not enough and FE exits with "User limit of inotify watches reached", which
# reads like OOM. CI runners are passwordless-sudo; locally this fails with an
# actionable message instead of a 5-minute BE-registration timeout. It runs
# before the stack is raised, so it lives here for setup.sh to call first.
it_ensure_sysctl() {
  local key="$1" min="$2" want="$3" cur
  cur="$(sysctl -n "${key}" 2>/dev/null || echo 0)"
  if [ "${cur}" -ge "${min}" ]; then
    return 0
  fi
  if sudo -n sysctl -w "${key}=${want}" >/dev/null 2>&1; then
    log "raised ${key} ${cur} -> ${want}"
    return 0
  fi
  log "error: ${key}=${cur}, needs >= ${min} (sudo sysctl -w ${key}=${want})" >&2
  return 1
}

it_wait_doris() {
  export DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
  export DORIS_SQL_PORT="${DORIS_SQL_PORT:-9030}"
  export DORIS_HTTP_PORT="${DORIS_HTTP_PORT:-8030}"
  export DORIS_USER="${DORIS_USER:-root}"
  export DORIS_PASSWORD="${DORIS_PASSWORD:-}"
  local fe_tries="${WAIT_DORIS_FE_TRIES:-180}" be_tries="${WAIT_DORIS_BE_TRIES:-300}"
  if ! command -v mysql >/dev/null 2>&1; then
    log "mysql client is required to wait for Doris (install mysql-client)." >&2
    return 1
  fi
  local args=(-h "${DORIS_HOST}" -P "${DORIS_SQL_PORT}" -u "${DORIS_USER}" --connect-timeout=5)
  if [ -n "${DORIS_PASSWORD}" ]; then
    args+=("-p${DORIS_PASSWORD}")
  fi
  local i ready=0 count
  log "waiting for Doris FE ${DORIS_HOST}:${DORIS_SQL_PORT} ..."
  for ((i = 1; i <= fe_tries; i++)); do
    if mysql "${args[@]}" -N -B -e "SELECT 1" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 2
  done
  if [ "${ready}" -ne 1 ]; then
    docker compose "${COMPOSE[@]}" logs --no-color --tail=200 fe be >&2 || true
    log "Doris FE not ready after $((fe_tries * 2))s" >&2
    return 1
  fi
  log "Doris FE is up; waiting for an alive BE ..."
  ready=0
  for ((i = 1; i <= be_tries; i++)); do
    count="$(mysql "${args[@]}" -N -B -e \
      "SELECT COUNT(*) FROM backends() WHERE Alive = 1" 2>/dev/null || true)"
    if [[ ! "${count}" =~ ^[0-9]+$ ]]; then
      # Fallback for Doris variants without the backends() table function.
      count="$(mysql "${args[@]}" -B -e "SHOW BACKENDS" 2>/dev/null |
        awk -F'\t' 'NR==1 { for (i=1;i<=NF;i++) if (tolower($i)=="alive") c=i; next }
                    c && tolower($c)=="true" { n++ }
                    END { print n+0 }')"
    fi
    if [ "${count:-0}" -gt 0 ]; then
      ready=1
      break
    fi
    sleep 2
  done
  if [ "${ready}" -ne 1 ]; then
    docker compose "${COMPOSE[@]}" logs --no-color --tail=200 fe be >&2 || true
    log "Doris BE not alive after $((be_tries * 2))s" >&2
    return 1
  fi
  log "Doris FE/BE ready"
}

# The MySQL instance is the CDC source for every sink, so it is always waited on
# first; anything else is the sink under test.
it_wait_sink() {
  local sink="$1"
  it_wait_mysql || return 1
  case "${sink}" in
    mysql | stdout) ;;
    kafka) it_wait_kafka || return 1 ;;
    doris) it_wait_doris || return 1 ;;
    elasticsearch) it_wait_es || return 1 ;;
    postgresql) it_wait_postgres || return 1 ;;
    *)
      log "error: unknown sink '${sink}'" >&2
      return 2
      ;;
  esac
  return 0
}

# --------------------------------------------------------------------- standalone
# Only when executed: setup.sh sources this file and calls the functions above.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  SINK="${1:-}"
  VARIANT="${2:-}"
  if [ -z "${SINK}" ]; then
    echo "usage: wait.sh <sink> [variant]" >&2
    exit 2
  fi
  resolve_python_bin
  export PYTHON_BIN
  it_apply_variant "${SINK}" "${VARIANT}" || exit 2
  export COMPOSE_PROJECT_NAME="$(it_project_name "${SINK}")"
  COMPOSE=()
  while IFS= read -r file; do
    COMPOSE+=(-f "${file}")
  done < <(it_compose_files "${SINK}")
  export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
  export MYSQL_PORT="${MYSQL_PORT:-13306}"
  export MYSQL_HOST_PORT="${MYSQL_HOST_PORT:-${MYSQL_PORT}}"
  it_wait_sink "${SINK}"
fi
