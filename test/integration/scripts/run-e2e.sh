#!/usr/bin/env bash
# Build go-cdc, seed MySQL, run functional + types + load harnesses.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
IT="${ROOT}/test/integration"
WORKDIR="${IT}/workdir"
mkdir -p "${WORKDIR}" "${ROOT}/docs/test"

export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
# Anonymous principals (must match mysql/init/01-databases.sql).
export SRC_USER="${SRC_USER:-u_a7k9m2x4}"
export SRC_PASSWORD="${SRC_PASSWORD:-P_w8q3n5v1r6}"
export SINK_USER="${SINK_USER:-u_b3j6c8y1}"
export SINK_PASSWORD="${SINK_PASSWORD:-P_z2h5t9k4m7}"
export FUNC_SRC_DB="${FUNC_SRC_DB:-db_tp_func_src}"
export FUNC_SNK_DB="${FUNC_SNK_DB:-db_tp_func_snk}"
export TYPES_SRC_DB="${TYPES_SRC_DB:-db_tp_types_src}"
export TYPES_SNK_DB="${TYPES_SNK_DB:-db_tp_types_snk}"
export LOAD_SRC_DB="${LOAD_SRC_DB:-db_tp_load_src}"
export LOAD_SNK_DB="${LOAD_SNK_DB:-db_tp_load_snk}"
export SRC_DB="${SRC_DB:-${FUNC_SRC_DB}}"
export SINK_DB="${SINK_DB:-${FUNC_SNK_DB}}"
export CHECKPOINT_DIR="${CHECKPOINT_DIR:-${WORKDIR}/ckpts}"
export CHECKPOINT_STDOUT_DIR="${CHECKPOINT_STDOUT_DIR:-${WORKDIR}/ckpts-stdout}"
export CHECKPOINT_TYPES_DIR="${CHECKPOINT_TYPES_DIR:-${WORKDIR}/ckpts-types}"
export CHECKPOINT_LOAD_DIR="${CHECKPOINT_LOAD_DIR:-${WORKDIR}/ckpts-load}"
export GO_CDC="${GO_CDC:-${WORKDIR}/go-cdc}"
# Back-compat: CDC_CLI still accepted if set.
if [ -n "${CDC_CLI:-}" ]; then
  export GO_CDC="${CDC_CLI}"
fi

# Pin load knobs so ambient shell leftovers are visible and defaults are explicit.
# CI may override via GITHUB_ENV / step env before this script runs.
export LOAD_PROFILE="${LOAD_PROFILE:-local}"
export LOAD_ROWS_PER_TABLE="${LOAD_ROWS_PER_TABLE:-50000}"
export LOAD_LARGE_ROWS="${LOAD_LARGE_ROWS:-50}"
export LOAD_LARGE_FIELD_BYTES="${LOAD_LARGE_FIELD_BYTES:-2097152}"
export LOAD_PEAK_SECONDS="${LOAD_PEAK_SECONDS:-90}"
export LOAD_PEAK_WORKERS="${LOAD_PEAK_WORKERS:-4}"
export LOAD_PEAK_BATCH="${LOAD_PEAK_BATCH:-200}"
export LOAD_PARALLELISM="${LOAD_PARALLELISM:-${LOAD_PEAK_WORKERS}}"

echo "==> load knobs"
echo "    LOAD_PROFILE=${LOAD_PROFILE}"
echo "    LOAD_ROWS_PER_TABLE=${LOAD_ROWS_PER_TABLE}"
echo "    LOAD_LARGE_ROWS=${LOAD_LARGE_ROWS}"
echo "    LOAD_LARGE_FIELD_BYTES=${LOAD_LARGE_FIELD_BYTES}"
echo "    LOAD_PEAK_SECONDS=${LOAD_PEAK_SECONDS}"
echo "    LOAD_PEAK_WORKERS=${LOAD_PEAK_WORKERS}"
echo "    LOAD_PEAK_BATCH=${LOAD_PEAK_BATCH}"
echo "    LOAD_PARALLELISM=${LOAD_PARALLELISM}"

# mysql client without -pPASSWORD on argv (use MYSQL_PWD for the child only).
mysql_as() {
  local user="$1" pass="$2"
  shift 2
  MYSQL_PWD="${pass}" mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${user}" --protocol=TCP "$@"
}

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

echo "==> seed functional + types"
mysql_as "${SRC_USER}" "${SRC_PASSWORD}" < "${IT}/fixtures/seed.sql"
mysql_as "${SRC_USER}" "${SRC_PASSWORD}" < "${IT}/fixtures/seed_types.sql"

echo "==> clean sink tables"
mysql_as "${SINK_USER}" "${SINK_PASSWORD}" -e "
DROP TABLE IF EXISTS
  ${FUNC_SNK_DB}.tp01_rows_out, ${FUNC_SNK_DB}.tp02_keys_out,
  ${FUNC_SNK_DB}.tp09_add_out, ${FUNC_SNK_DB}.tp10_ddl_out, ${FUNC_SNK_DB}.tp07_fail_out,
  ${TYPES_SNK_DB}.tpd01_types_out,
  ${LOAD_SNK_DB}.tpl01_t1_out, ${LOAD_SNK_DB}.tpl01_t2_out,
  ${LOAD_SNK_DB}.tpl01_t3_out, ${LOAD_SNK_DB}.tpl01_t4_out,
  ${LOAD_SNK_DB}.tpl01_typed_out, ${LOAD_SNK_DB}.tpl01_large_out;
"
rm -rf "${CHECKPOINT_DIR}" "${CHECKPOINT_STDOUT_DIR}" "${CHECKPOINT_TYPES_DIR}" "${CHECKPOINT_LOAD_DIR}"

echo "==> run functional harness (TP-01..TP-13)"
cd "${IT}"
"${PYTHON_BIN}" -m harness.run_it

echo "==> run types/timezone harness (TP-D01..)"
"${PYTHON_BIN}" -m harness.types_it

echo "==> run load/peak harness (TP-L01..)"
"${PYTHON_BIN}" -m harness.load_it

echo "==> done"
