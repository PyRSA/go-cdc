#!/usr/bin/env bash
# One entry point for a sink run: containers, build, seed, suites, report.
#
#   bash scripts/run.sh <sink> [profile] [variant] [suites] [test-point...]
#
#   sink       mysql | stdout | kafka | doris | elasticsearch | postgresql
#   profile    normal | heavy      (default: normal)
#   variant    57 | 80 | pg14..pg18 | es7|es8|es9 | 2.1.8   (default: sink default)
#   suites     comma list that replaces the tier default, e.g. `func` or
#              `func,scale`; `IT_SUITES` is the env spelling of the same thing
#   test-point anything after that is forwarded to the suite (e.g. `E2E-03`)
#
# Pass an empty string for a positional you want to skip, so the next one keeps
# its slot: `run.sh mysql normal '' func`.
#
# The version matrix is never dropped: the variant is mapped to MYSQL_TAG /
# MYSQL_IMAGE, PG_IMAGE / PG_EXPECT_MAJOR, ES_SINK_VERSION or DORIS_VERSION by
# `logger.sh: it_apply_variant`, called twice so it works across the process
# boundary: once here, in this process (the suites below read ES_URL,
# ES_SINK_VERSION and PG_EXPECT_MAJOR themselves), and once by setup.sh in its
# own child process (it needs the image names to start the stack). There is no
# second copy of that table - change it in logger.sh only.
#
# Profile tiers (what CI's `normal` / `heavy` mean):
#
#             suites                                    load / scale knobs
#   normal    correctness (+ es8 load/scale)            100k rows, 50k hotspot, 60 s peak, 4 writers
#   heavy     correctness + load + scale (+ bench)      100k rows, 50k hotspot, 60 s peak, 8 writers
#
# es8 is the one exception to "normal is correctness only": its load and scale
# suites run in both tiers, because the ES write path has no other coverage and a
# PR that changes it has to see the numbers. The tier still changes the writer
# count and the two timeouts, so a normal report stays readable against a heavy
# one. es7/es9 remain `go,func` - they are adapter-compatibility nodes.
#
# The seed volume, the peak duration and the hotspot burst are deliberately NOT
# tiered: they are the shape of the measurement, not its cost, so a `normal`
# report has to be readable against a `heavy` one. Only the peak writer count and
# the two timeouts follow the profile (see the tier block below).
#
# Examples:
#   bash scripts/run.sh mysql normal 57          # func + types + load on MySQL 5.7
#   bash scripts/run.sh postgresql normal pg14   # E2E suite against PostgreSQL 14
#   bash scripts/run.sh elasticsearch heavy es8  # go + e2e + load + scale
#   IT_SUITES=func bash scripts/run.sh kafka     # a single suite
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${IT}/../.." && pwd)"
WORKDIR="${IT}/workdir"
mkdir -p "${WORKDIR}" "${ROOT}/docs/test/reports"
# shellcheck source=tests/integration/scripts/logger.sh
source "${SCRIPT_DIR}/logger.sh"

SINK="${1:-}"
PROFILE="${2:-normal}"
VARIANT="${3:-}"
SUITES_ARG="${4:-}"
if [ -z "${SINK}" ]; then
  echo "usage: run.sh <sink> [profile] [variant] [suites] [test-point...]" >&2
  exit 2
fi
# Anything past the fourth positional is handed to the suite (test-point ids).
EXTRA=()
if [ "$#" -gt 4 ]; then
  EXTRA=("${@:5}")
fi

case "${PROFILE}" in
  normal | heavy) ;;
  *)
    log "error: unknown profile '${PROFILE}' (use normal or heavy)" >&2
    exit 2
    ;;
esac

# ------------------------------------------------------------------- tier knobs
# One table for both tiers so a report can be compared across runs: the numbers
# are the same order of magnitude as the historical CI jobs, and every value is
# still overridable from the environment.
if [ "${PROFILE}" = "heavy" ]; then
  TIER_PEAK_WORKERS=8
  TIER_CATCHUP_BUDGET=5400
  TIER_SCALE_TIMEOUT=7200
else
  TIER_PEAK_WORKERS=4
  TIER_CATCHUP_BUDGET=1800
  TIER_SCALE_TIMEOUT=2400
fi
# Identical in both tiers on purpose: these three are the shape of the measurement
# (`LOAD_ROWS_PER_TABLE` seeds every suite, `peak seconds` is how long the writers
# run, `SCALE_BURST_ROWS_PER_HOT` is the hotspot their burst creates), so a smaller
# `normal` value silently produced a different experiment rather than a cheaper
# one. Keep them out of the if/else above.
TIER_ROWS=100000
TIER_PEAK_SECONDS=60
TIER_HOTSPOT_ROWS=50000

# Which sink is under test: config.py reads this to pick sinks/<sink>/fixtures.
export CDC_SINK="${SINK}"
export IT_PROFILE="${PROFILE}"
export IT_VARIANT="${VARIANT}"

# The same variant table setup.sh uses. setup.sh is a child process, so its own
# exports die with it; the suites here read ES_URL / ES_SINK_VERSION /
# PG_EXPECT_MAJOR themselves, so resolve them in this process as well.
it_apply_variant "${SINK}" "${VARIANT}"

# Same child-process problem for the Doris FE address. In CI the stack is raised
# by a separate workflow step, so whatever setup.sh exported while waiting for FE
# is gone by the time the suites run here; the Go suite reads DORIS_HOST from the
# environment and refuses to skip under CI, so resolve it in this process.
if [ "${SINK}" = "doris" ]; then
  it_doris_endpoint
fi

# ES7/ES9 keep their own report and checkpoints: locally the three variants run
# back to back, so a shared file would let the last run overwrite the previous
# result. CI does not need the separation (one runner and one artifact per
# variant), but it costs nothing and keeps `make it-es7 && make it-es9` honest.
case "${SINK}:${VARIANT:-es8}" in
  elasticsearch:7 | elasticsearch:es7)
    export REPORT_ES="${REPORT_ES:-${ROOT}/docs/test/reports/mysql-elasticsearch7.md}"
    export CHECKPOINT_ES_DIR="${CHECKPOINT_ES_DIR:-${WORKDIR}/ckpts-es7}"
    export CHECKPOINT_ES_RESTART_DIR="${CHECKPOINT_ES_RESTART_DIR:-${WORKDIR}/ckpts-es7-restart}"
    ;;
  elasticsearch:9 | elasticsearch:es9)
    export REPORT_ES="${REPORT_ES:-${ROOT}/docs/test/reports/mysql-elasticsearch9.md}"
    export CHECKPOINT_ES_DIR="${CHECKPOINT_ES_DIR:-${WORKDIR}/ckpts-es9}"
    export CHECKPOINT_ES_RESTART_DIR="${CHECKPOINT_ES_RESTART_DIR:-${WORKDIR}/ckpts-es9-restart}"
    ;;
esac

# `docker compose` calls made by a harness (pg_it restarts the server) resolve
# the stack from the environment instead of a compose file in the cwd.
export COMPOSE_PROJECT_NAME="$(it_project_name "${SINK}")"
export COMPOSE_FILE="$(it_compose_file_list "${SINK}")"

export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
# Anonymous principals (must match sources/mysql/init/01-databases.sql).
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
absolutize_go_cdc

# Load knobs (mysql func/load, kafka, doris and the ES/PG load suites share them).
# The peak duration is 60s in both tiers (see the tier table at the top): only the
# writer count follows the profile.
export LOAD_PROFILE="${LOAD_PROFILE:-${PROFILE}}"
export LOAD_ROWS_PER_TABLE="${LOAD_ROWS_PER_TABLE:-${TIER_ROWS}}"
export LOAD_LARGE_ROWS="${LOAD_LARGE_ROWS:-50}"
export LOAD_LARGE_FIELD_BYTES="${LOAD_LARGE_FIELD_BYTES:-2097152}"
export LOAD_PEAK_SECONDS="${LOAD_PEAK_SECONDS:-${TIER_PEAK_SECONDS}}"
export LOAD_PEAK_WORKERS="${LOAD_PEAK_WORKERS:-${TIER_PEAK_WORKERS}}"
export LOAD_PEAK_BATCH="${LOAD_PEAK_BATCH:-200}"
export LOAD_PARALLELISM="${LOAD_PARALLELISM:-${LOAD_PEAK_WORKERS}}"

# Scale knobs. PEAK_CATCHUP_BUDGET_S / SCALE_TOTAL_TIMEOUT are the "two-tier
# timeout": a heavy run may wait twice as long on a slower runner. The hotspot
# size itself does not follow the tier (see the tier table at the top).
export SCALE_BURST_ROWS_PER_HOT="${SCALE_BURST_ROWS_PER_HOT:-${TIER_HOTSPOT_ROWS}}"
export SCALE_PEAK_SECONDS="${SCALE_PEAK_SECONDS:-${TIER_PEAK_SECONDS}}"
export SCALE_PEAK_WORKERS="${SCALE_PEAK_WORKERS:-${TIER_PEAK_WORKERS}}"
export SCALE_PEAK_BATCH="${SCALE_PEAK_BATCH:-200}"
export PEAK_CATCHUP_BUDGET_S="${PEAK_CATCHUP_BUDGET_S:-${TIER_CATCHUP_BUDGET}}"
export SCALE_TOTAL_TIMEOUT="${SCALE_TOTAL_TIMEOUT:-${TIER_SCALE_TIMEOUT}}"

# Checkpoint directories: absolute under tests/integration, wiped per run so a
# stale ack offset can never turn a failure into a pass.
export CHECKPOINT_DIR="${CHECKPOINT_DIR:-${WORKDIR}/ckpts}"
export CHECKPOINT_STDOUT_DIR="${CHECKPOINT_STDOUT_DIR:-${WORKDIR}/ckpts-stdout}"
export CHECKPOINT_TYPES_DIR="${CHECKPOINT_TYPES_DIR:-${WORKDIR}/ckpts-types}"
export CHECKPOINT_LOAD_DIR="${CHECKPOINT_LOAD_DIR:-${WORKDIR}/ckpts-load}"
export CHECKPOINT_KAFKA_DIR="${CHECKPOINT_KAFKA_DIR:-${WORKDIR}/ckpts-kafka}"
export CHECKPOINT_KAFKA_LOAD_DIR="${CHECKPOINT_KAFKA_LOAD_DIR:-${WORKDIR}/ckpts-kafka-load}"
export CHECKPOINT_DORIS_DIR="${CHECKPOINT_DORIS_DIR:-${WORKDIR}/ckpts-doris}"
export CHECKPOINT_DORIS_LOAD_DIR="${CHECKPOINT_DORIS_LOAD_DIR:-${WORKDIR}/ckpts-doris-load}"
export CHECKPOINT_ES_DIR="${CHECKPOINT_ES_DIR:-${WORKDIR}/ckpts-es}"
export CHECKPOINT_ES_RESTART_DIR="${CHECKPOINT_ES_RESTART_DIR:-${WORKDIR}/ckpts-es-restart}"
export CHECKPOINT_ES_LOAD_DIR="${CHECKPOINT_ES_LOAD_DIR:-${WORKDIR}/ckpts-es-load}"
export CHECKPOINT_PG_DIR="${CHECKPOINT_PG_DIR:-${WORKDIR}/ckpts-pg}"
export CHECKPOINT_PG_RESTART_DIR="${CHECKPOINT_PG_RESTART_DIR:-${WORKDIR}/ckpts-pg-restart}"
export CHECKPOINT_PG_LOAD_DIR="${CHECKPOINT_PG_LOAD_DIR:-${WORKDIR}/ckpts-pg-load}"
export CHECKPOINT_PG_SCALE_DIR="${CHECKPOINT_PG_SCALE_DIR:-${WORKDIR}/ckpts-pg-scale}"
export CHECKPOINT_PG_BENCH_DIR="${CHECKPOINT_PG_BENCH_DIR:-${WORKDIR}/ckpts-pg-bench}"
absolutize_checkpoint_dirs

# ---------------------------------------------------------------------- suites
# `normal` runs the correctness layer only, except es8, which carries its load and
# scale suites in both tiers (see the tier table above); `heavy` adds the whole
# performance layer for everything else. Both stay explicit instead of "run
# everything and hope".
default_suites() {
  local es_variant="${VARIANT:-es8}"
  case "${SINK}" in
    mysql)
      [ "${PROFILE}" = "heavy" ] && echo "func,types,load,scale" || echo "func,types,load"
      ;;
    stdout)
      echo "func"
      ;;
    kafka)
      # kafka_it already covers the load / peak test points; the tier only
      # changes their size, so there is no separate load suite.
      echo "func"
      ;;
    doris)
      [ "${PROFILE}" = "heavy" ] && echo "go,func,scale" || echo "go,func"
      ;;
    elasticsearch)
      case "${es_variant}" in
        # The v7/v9 adapter nodes are correctness-only: no load, no scale.
        es7 | 7 | es9 | 9) echo "go,func" ;;
        # es8 carries the performance layer in both tiers: the tier changes the
        # writer count and the timeouts, never which suites run.
        *) echo "go,func,load,scale" ;;
      esac
      ;;
    postgresql)
      # `compat` replaces the stack once per major (pg14..pg18 via setup.sh), so
      # it runs last: whatever it leaves behind is then irrelevant to the run.
      if [ "${PROFILE}" = "heavy" ]; then
        echo "func,load,bench,scale,compat"
      else
        echo "func"
      fi
      ;;
  esac
}

# Explicit list wins over the tier default; `IT_SUITES` stays supported because
# the older docs and the harness internals still set it.
EXPLICIT_SUITES="${IT_SUITES:-${SUITES_ARG}}"
SUITES="${EXPLICIT_SUITES:-$(default_suites)}"
SUITES="${SUITES// /}"

# MySQL 5.7 is correctness-only in every tier: the scale suite is pinned to the
# default version so a heavy run does not duplicate the longest job. `5.7` /
# `5.7.42` are accepted spellings of the same variant, so match all of them.
case "${SINK}:${VARIANT:-80}" in
  mysql:57 | mysql:5.7 | mysql:5.7.*)
    if [ -z "${EXPLICIT_SUITES}" ]; then
      SUITES="func,types,load"
    fi
    ;;
esac

# -------------------------------------------------------------------- preflight
log "==> ${SINK}${VARIANT:+ variant=${VARIANT}} profile=${PROFILE} suites=${SUITES}"

if [ -z "${IT_SKIP_SETUP:-}" ]; then
  bash "${SCRIPT_DIR}/setup.sh" "${SINK}" "${VARIANT}" "${PROFILE}"
else
  log "==> skipping setup.sh (IT_SKIP_SETUP=${IT_SKIP_SETUP})"
fi

# Local runs own their teardown; CI keeps the containers until the workflow's
# final `if: always()` step so the report upload never races a `down -v`.
if [ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ] && [ -z "${IT_KEEP:-}" ]; then
  cleanup() {
    local status=$?
    log "==> teardown (local run; IT_KEEP=1 keeps the containers)"
    bash "${SCRIPT_DIR}/teardown.sh" "${SINK}" "${VARIANT}" || true
    exit "${status}"
  }
  trap cleanup EXIT
fi

log "==> build go-cdc"
(cd "${ROOT}" && go build -o "${GO_CDC}" ./cmd/go-cdc)

log "==> install harness deps"
resolve_python_bin
log "    using ${PYTHON_BIN}"
"${PYTHON_BIN}" -m pip install -q -r "${IT}/common/requirements.txt"

# ------------------------------------------------------------------------ seed
mysql_as() {
  local user="$1" pass="$2"
  shift 2
  MYSQL_PWD="${pass}" mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${user}" \
    --protocol=TCP "$@"
}

seed_sql() {
  local file="$1"
  [ -f "${file}" ] || return 0
  log "==> seed ${file#"${IT}/"}"
  mysql_as "${SRC_USER}" "${SRC_PASSWORD}" <"${file}"
}

reset_checkpoints() {
  local name value
  for name in CHECKPOINT_DIR CHECKPOINT_STDOUT_DIR CHECKPOINT_TYPES_DIR \
    CHECKPOINT_LOAD_DIR CHECKPOINT_KAFKA_DIR CHECKPOINT_KAFKA_LOAD_DIR \
    CHECKPOINT_DORIS_DIR CHECKPOINT_DORIS_LOAD_DIR CHECKPOINT_ES_DIR \
    CHECKPOINT_ES_RESTART_DIR CHECKPOINT_ES_LOAD_DIR CHECKPOINT_PG_DIR \
    CHECKPOINT_PG_RESTART_DIR CHECKPOINT_PG_LOAD_DIR CHECKPOINT_PG_SCALE_DIR \
    CHECKPOINT_PG_BENCH_DIR; do
    value="${!name:-}"
    [ -n "${value}" ] || continue
    rm -rf "${value}"
    mkdir -p "${value}"
  done
}

case "${SINK}" in
  mysql | stdout)
    reset_checkpoints
    seed_sql "${IT}/sinks/mysql/fixtures/seed.sql"
    seed_sql "${IT}/sinks/mysql/fixtures/seed_types.sql"
    ;;
  kafka)
    reset_checkpoints
    seed_sql "${IT}/sinks/kafka/fixtures/seed_kafka.sql"
    ;;
  doris)
    reset_checkpoints
    seed_sql "${IT}/sinks/doris/fixtures/seed_doris.sql"
    ;;
  elasticsearch | postgresql)
    # These suites create and seed their own source databases per test point.
    reset_checkpoints
    ;;
esac

# ------------------------------------------------------------------ dispatch
cd "${IT}"

go_es() {
  local pattern="$1"
  log "==> go: elasticsearch sink (${pattern})"
  (cd "${ROOT}" && ES_URL="${ES_URL:-http://127.0.0.1:${ES_HOST_PORT:-19200}}" \
    go test -tags esintegration -count=1 -timeout 20m -v \
    -run "${pattern}" ./connector/sink/elasticsearch/)
}

go_doris() {
  log "==> go: doris sink (doris_integration)"
  # DORIS_* arrive from it_doris_endpoint above; without DORIS_HOST the suite
  # fails on CI instead of skipping, which is what turned a missing export into
  # a red job rather than a green one that never touched Doris.
  (cd "${ROOT}" && go test -tags doris_integration -count=1 -timeout 20m -v \
    ./tests/integration/sinks/doris/go/)
}

run_suite() {
  local suite="$1"
  case "${SINK}:${suite}" in
    mysql:func) "${PYTHON_BIN}" -m sinks.mysql.harness.run_it "${EXTRA[@]+"${EXTRA[@]}"}" ;;
    mysql:types) "${PYTHON_BIN}" -m sinks.mysql.harness.types_it ;;
    mysql:load) "${PYTHON_BIN}" -m sinks.mysql.harness.load_it ;;
    mysql:scale) "${PYTHON_BIN}" -m common.scale_it "${PROFILE}" ;;
    stdout:func) "${PYTHON_BIN}" -m sinks.stdout.harness.stdout_it ;;
    kafka:func) "${PYTHON_BIN}" -m sinks.kafka.harness.kafka_it ;;
    doris:go) go_doris ;;
    doris:func) "${PYTHON_BIN}" -m sinks.doris.harness.doris_it ;;
    doris:scale) "${PYTHON_BIN}" -m sinks.doris.scale.doris_scale_it "${PROFILE}" --until all ;;
    elasticsearch:go)
      case "${VARIANT:-es8}" in
        es7 | 7) go_es 'TestElasticsearch7' ;;
        es9 | 9) go_es 'TestElasticsearch9' ;;
        *) go_es 'TestES' ;;
      esac
      ;;
    elasticsearch:func) "${PYTHON_BIN}" -m sinks.elasticsearch.harness.es_it ;;
    elasticsearch:load) "${PYTHON_BIN}" -m sinks.elasticsearch.harness.es_load_it ;;
    elasticsearch:scale)
      "${PYTHON_BIN}" -m sinks.elasticsearch.scale.es_scale_it "${PROFILE}"
      ;;
    postgresql:func) "${PYTHON_BIN}" -m sinks.postgresql.harness.pg_it "${EXTRA[@]+"${EXTRA[@]}"}" ;;
    postgresql:load) "${PYTHON_BIN}" -m sinks.postgresql.harness.pg_load_it "${EXTRA[@]+"${EXTRA[@]}"}" ;;
    postgresql:bench) "${PYTHON_BIN}" -m sinks.postgresql.harness.pg_bench_it "${EXTRA[@]+"${EXTRA[@]}"}" ;;
    postgresql:compat) "${PYTHON_BIN}" -m sinks.postgresql.harness.pg_compat "${EXTRA[@]+"${EXTRA[@]}"}" ;;
    postgresql:scale)
      "${PYTHON_BIN}" -m sinks.postgresql.scale.pg_scale_it "${PROFILE}" --until all
      ;;
    *)
      log "error: suite '${suite}' does not exist for sink '${SINK}'" >&2
      return 2
      ;;
  esac
}

# Every suite runs even after an earlier one fails: the report is worth more
# than the first non-zero exit, and the final status is still non-zero.
status=0
for suite in ${SUITES//,/ }; do
  log "==> suite ${suite}"
  if ! run_suite "${suite}"; then
    status=1
    log "suite ${suite} FAILED (continuing)"
  fi
done

if [ "${status}" -ne 0 ]; then
  log "==> ${SINK}${VARIANT:+ ${VARIANT}} FAILED"
  exit "${status}"
fi
log "==> ${SINK}${VARIANT:+ ${VARIANT}} passed"
