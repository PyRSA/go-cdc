#!/usr/bin/env bash
# Wait until Kafka accepts metadata from the host (KAFKA_BROKERS) or via compose exec.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
# shellcheck source=test/integration/scripts/_log.sh
source "${ROOT}/scripts/_log.sh"

export MYSQL_TAG="${MYSQL_TAG:-80}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-gocdc-it-${MYSQL_TAG}}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:${KAFKA_HOST_PORT:-19092}}"
deadline=$((SECONDS + ${WAIT_KAFKA_SECONDS:-90}))

# Host-side check: advertised listeners point at the published host port, so
# in-container kafka-topics against 127.0.0.1:9092 often fails after metadata.
host_ok() {
  python3 - "$KAFKA_BROKERS" <<'PY' 2>/dev/null
import sys
from kafka import KafkaConsumer
brokers = sys.argv[1]
c = KafkaConsumer(bootstrap_servers=brokers.split(","), consumer_timeout_ms=2000)
c.topics()
c.close()
PY
}

while (( SECONDS < deadline )); do
  if host_ok; then
    exit 0
  fi
  if docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
done

log "timeout waiting for Kafka at ${KAFKA_BROKERS}" >&2
exit 1
