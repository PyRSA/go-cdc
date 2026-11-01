#!/usr/bin/env bash
# Remove the containers (and volumes) of one sink run.
#
#   bash scripts/teardown.sh <sink> [variant]
#
# Resolves the compose files and the project name exactly like setup.sh, so a
# teardown can only ever remove the stack the matching setup created. Safe to
# call from an `if: always()` step: a missing project is not an error.
#
# The variant is allowed to be empty - CI passes `teardown.sh doris ""` for every
# sink without a version matrix (doris, kafka, stdout). Empty means "the sink's
# default", which is what `it_apply_variant` already resolves it to, so nothing
# here needs a project name of its own: `it_project_name` never looks at the
# variant, and every `${...}` in the compose files carries a `:-` fallback.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=tests/integration/scripts/logger.sh
source "${SCRIPT_DIR}/logger.sh"

SINK="${1:-}"
VARIANT="${2:-}"
if [ -z "${SINK}" ]; then
  echo "usage: teardown.sh <sink> [variant]" >&2
  exit 2
fi

export COMPOSE_PROJECT_NAME="$(it_project_name "${SINK}")"
# Resolve the image names the same way setup.sh did, so `docker compose down`
# parses the files with the values they were started with. An empty variant is
# the documented "sink default"; an unknown one is a caller bug, but the project
# name does not depend on it, so warn and keep tearing down instead of adding a
# failure of our own to an `if: always()` step.
it_apply_variant "${SINK}" "${VARIANT}" ||
  log "warning: variant '${VARIANT}' not recognised; tearing down ${SINK} with its defaults" >&2
COMPOSE=()
while IFS= read -r file; do
  COMPOSE+=(-f "${file}")
done < <(it_compose_files "${SINK}")

if [ -z "${DOCKER_HOST:-}" ] && [ ! -S /var/run/docker.sock ]; then
  if [ -S "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock" ]; then
    export DOCKER_HOST="unix://${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock"
  fi
fi

if ! docker info >/dev/null 2>&1; then
  log "docker daemon is not available; nothing to tear down"
  exit 0
fi

log "==> down ${SINK}${VARIANT:+ (${VARIANT})} project=${COMPOSE_PROJECT_NAME}"
# Profiles are named too: a profiled service (ES7/ES9) is not stopped by a plain
# down, and leaving it behind would hold its host port for the next run.
docker compose "${COMPOSE[@]}" --profile es7 --profile es9 \
  down -v --remove-orphans --timeout 20 >/dev/null 2>&1 ||
  docker compose "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

# The only container that writes back through a bind mount is Doris: its FE and
# BE run as root and own everything under `workdir/doris/**`, which the runner
# user therefore cannot unlink (EACCES). Drop it so the next FE starts from an
# empty meta image instead of a half-registered BE - `it_rm_rf` escalates and
# never aborts. Nothing else needs this: ES / Kafka / PostgreSQL / MySQL keep
# their data in named volumes (removed by `down -v`), and the MySQL source's two
# bind mounts (`conf.d`, `init`) are mounted `:ro`.
if [ "${SINK}" = "doris" ] && [ "${IT_KEEP_DORIS_DATA:-0}" != "1" ]; then
  it_rm_rf "${IT}/workdir/doris"
fi

log "==> ${SINK} removed"
