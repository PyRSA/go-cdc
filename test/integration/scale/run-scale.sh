#!/usr/bin/env bash
# Build go-cdc, wait for MySQL, run scale integration (normal|heavy).
set -euo pipefail
PROFILE="${1:-${SCALE_PROFILE:-normal}}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
IT="${ROOT}/test/integration"
WORKDIR="${IT}/workdir"
mkdir -p "${WORKDIR}" "${ROOT}/docs/test/reports"

export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
# Anonymous principals (must match mysql/init/01-databases.sql).
export SRC_USER="${SRC_USER:-u_a7k9m2x4}"
export SRC_PASSWORD="${SRC_PASSWORD:-P_w8q3n5v1r6}"
export SINK_USER="${SINK_USER:-u_b3j6c8y1}"
export SINK_PASSWORD="${SINK_PASSWORD:-P_z2h5t9k4m7}"
export ROOT_USER="${ROOT_USER:-root}"
export ROOT_PASSWORD="${ROOT_PASSWORD:-rootpass}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

echo "==> build go-cdc"
(cd "${ROOT}" && go build -o "${GO_CDC}" ./cmd/go-cdc)

echo "==> install harness deps"
PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "${PYTHON_BIN}" ]; then
  for c in python3.11 python3.10 python3.9 python3; do
    if command -v "$c" >/dev/null 2>&1; then
      PYTHON_BIN="$c"
      break
    fi
  done
fi
echo "    using ${PYTHON_BIN}"
"${PYTHON_BIN}" -m pip install -q -r "${IT}/harness/requirements.txt"

echo "==> wait mysql"
bash "${IT}/scripts/wait-for-mysql.sh"

echo "==> run scale harness (profile=${PROFILE})"
cd "${IT}"
"${PYTHON_BIN}" -m scale.scale_it "${PROFILE}"

echo "==> done"
