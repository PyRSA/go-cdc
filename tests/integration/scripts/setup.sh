#!/usr/bin/env bash
# Start the containers one sink needs, then block until they accept work.
#
#   bash scripts/setup.sh <sink> [variant] [profile]
#
#   sink     mysql | stdout | kafka | doris | elasticsearch | postgresql
#   variant  the version of the thing under test (see below); empty = default
#   profile  normal | heavy - only recorded here, the knobs live in run.sh
#
# Variant -> image mapping (the version matrix this repo must never lose):
#
#   mysql / stdout / kafka   57 | 80                 -> MYSQL_TAG, MYSQL_IMAGE
#   postgresql               pg14..pg18 | 14..18     -> PG_IMAGE, PG_EXPECT_MAJOR
#   elasticsearch            es7 | es8 | es9         -> ES_SINK_VERSION (+ port)
#   doris                    2.1.8                   -> DORIS_VERSION
#
# The mapping itself is it_apply_variant() in logger.sh, shared with run.sh.
#
# Waiting lives in wait.sh (sourced below), so the readiness probes have one
# implementation: setup.sh raises the stack and then blocks, which is why a CI
# template is a single `bash scripts/setup.sh <sink> <variant> <profile>` step.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${IT}/../.." && pwd)"
# shellcheck source=tests/integration/scripts/logger.sh
source "${SCRIPT_DIR}/logger.sh"
# shellcheck source=tests/integration/scripts/wait.sh
source "${SCRIPT_DIR}/wait.sh"

SINK="${1:-}"
VARIANT="${2:-}"
PROFILE="${3:-normal}"
if [ -z "${SINK}" ]; then
  echo "usage: setup.sh <sink> [variant] [profile]" >&2
  exit 2
fi
case "${SINK}" in
  mysql | stdout | kafka | doris | elasticsearch | postgresql) ;;
  *)
    log "error: unknown sink '${SINK}' (mysql|stdout|kafka|doris|elasticsearch|postgresql)" >&2
    exit 2
    ;;
esac

resolve_python_bin
export PYTHON_BIN

mkdir -p "${IT}/workdir"

# --------------------------------------------------------------------- variant
# The variant table lives in logger.sh (it_apply_variant): setup.sh is a child
# process of run.sh, so anything exported here would die with it - and run.sh
# needs ES_URL/ES_SINK_VERSION/PG_EXPECT_MAJOR for the suites themselves.
#
# An empty variant is the normal case, not an error: CI's matrix carries
# `"variant": ""` for every sink without a version matrix (doris, kafka, stdout)
# and `it_apply_variant` resolves "" to that sink's default. Each `${...}` in the
# compose files also carries a `:-` fallback of its own, so an unset image name
# can never leave compose looking for a project it cannot resolve.
it_apply_variant "${SINK}" "${VARIANT}"

# ------------------------------------------------------------------- compose set
# The MySQL instance is the CDC source for every sink, so it is always part of
# the project; `mysql`/`stdout` add nothing else because the sink is that very
# instance.
export COMPOSE_PROJECT_NAME="$(it_project_name "${SINK}")"
COMPOSE=()
while IFS= read -r file; do
  COMPOSE+=(-f "${file}")
done < <(it_compose_files "${SINK}")

# ES7/ES9 sit behind compose profiles so an unrelated `up` never starts them.
COMPOSE_UP=("${COMPOSE[@]}")
case "${SINK}:${ES_SERVICE:-}" in
  elasticsearch:elasticsearch7) COMPOSE_UP+=(--profile es7) ;;
  elasticsearch:elasticsearch9) COMPOSE_UP+=(--profile es9) ;;
esac

export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
export MYSQL_HOST_PORT="${MYSQL_HOST_PORT:-${MYSQL_PORT}}"

if [ -z "${DOCKER_HOST:-}" ] && [ ! -S /var/run/docker.sock ]; then
  if [ -S "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock" ]; then
    export DOCKER_HOST="unix://${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock"
    log "using rootless DOCKER_HOST=${DOCKER_HOST}"
  fi
fi

if ! docker info >/dev/null 2>&1; then
  log "docker daemon is not available." >&2
  log "  - rootful: start dockerd / ensure /var/run/docker.sock" >&2
  log "  - rootless: export DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/docker.sock" >&2
  exit 1
fi

# ------------------------------------------------------------------------ start
# A version switch reuses nothing: MySQL only runs its init SQL on an empty data
# directory and a PostgreSQL data directory is bound to one major version. The
# project is therefore dropped before every start unless IT_REUSE=1 asks for a
# fast warm restart.
if [ "${IT_REUSE:-0}" = "1" ]; then
  log "==> reusing ${COMPOSE_PROJECT_NAME} volumes (IT_REUSE=1)"
else
  log "==> dropping ${COMPOSE_PROJECT_NAME} (containers + volumes)"
  docker compose "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
fi

if [ "${SINK}" = "doris" ]; then
  need_sysctl=0
  it_ensure_sysctl fs.inotify.max_user_watches 65536 1048576 || need_sysctl=1
  it_ensure_sysctl fs.inotify.max_user_instances 1024 8192 ||
    log "warning: fs.inotify.max_user_instances still low; continuing." >&2
  it_ensure_sysctl vm.max_map_count 2000000 2000000 || need_sysctl=1
  if [ "${need_sysctl}" -ne 0 ]; then
    log "Doris cannot start until the kernel limits above are raised." >&2
    exit 1
  fi
  # A stale FE meta image from an interrupted run can advertise an unreachable
  # BE; recreate the bind directories for a deterministic run. Doris writes them
  # as root, so a previous run's leftovers cannot be unlinked by the runner user:
  # `it_rm_rf` escalates and tolerates an absent directory. The parent `workdir/`
  # is ours, so the `mkdir -p` below still succeeds afterwards.
  it_rm_rf "${IT}/workdir/doris"
  mkdir -p "${IT}/workdir/doris/fe/doris-meta" "${IT}/workdir/doris/fe/log" \
    "${IT}/workdir/doris/be/storage" "${IT}/workdir/doris/be/log"
fi

log "==> up ${SINK}${VARIANT:+ (${VARIANT})} profile=${PROFILE} project=${COMPOSE_PROJECT_NAME}"
docker compose "${COMPOSE_UP[@]}" pull >/dev/null 2>&1 || true
# `--wait` needs health checks on every service; Doris has none, so fall back to
# a plain up and let the sink-specific wait below do the blocking.
# shellcheck disable=SC2086
docker compose "${COMPOSE_UP[@]}" up -d --wait 2>/dev/null ||
  docker compose "${COMPOSE_UP[@]}" up -d

# wait.sh: MySQL first (the CDC source), then whatever the sink needs.
it_wait_sink "${SINK}"

log "==> ${SINK} is ready"
