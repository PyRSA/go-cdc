# Shared helpers for the IT entry scripts (source this file, never execute it).
# Usage: log "==> build go-cdc"
#
# Everything more than one script needs lives here - logging, python discovery,
# path absolutizing, the bind-mount cleanup and the single compose-project /
# variant table - so setup.sh and teardown.sh cannot disagree about which stack
# they are talking to.
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Set PYTHON_BIN if unset (prefers python3.11 … python3).
resolve_python_bin() {
  if [ -n "${PYTHON_BIN:-}" ]; then
    return
  fi
  local c
  for c in python3.11 python3.10 python3.9 python3; do
    if command -v "$c" >/dev/null 2>&1; then
      PYTHON_BIN="$c"
      return
    fi
  done
  echo "error: no python3 interpreter found (tried python3.11..python3)" >&2
  return 1
}

# Make path absolute under IT when relative. Requires IT set by the caller.
_absolutize_under_it() {
  local var="$1"
  local val="${!var}"
  case "${val}" in
    /*) ;;
    *) export "${var}=${IT}/${val}" ;;
  esac
}

# go build -o is relative to ROOT; keep GO_CDC absolute (under IT when relative).
absolutize_go_cdc() {
  _absolutize_under_it GO_CDC
}

# Checkpoint dirs used by rm -rf before cd IT; keep absolute under IT.
# Only touch variables that the caller exported (a suite that omits, say, the
# Kafka dirs must keep them unset instead of pointing at IT/).
absolutize_checkpoint_dirs() {
  local name
  for name in CHECKPOINT_DIR CHECKPOINT_STDOUT_DIR CHECKPOINT_TYPES_DIR \
    CHECKPOINT_LOAD_DIR CHECKPOINT_KAFKA_DIR CHECKPOINT_KAFKA_LOAD_DIR \
    CHECKPOINT_DORIS_DIR CHECKPOINT_DORIS_LOAD_DIR CHECKPOINT_DORIS_SCALE_DIR \
    CHECKPOINT_ES_DIR CHECKPOINT_ES_RESTART_DIR CHECKPOINT_ES_LOAD_DIR \
    CHECKPOINT_PG_DIR CHECKPOINT_PG_RESTART_DIR CHECKPOINT_PG_LOAD_DIR \
    CHECKPOINT_PG_SCALE_DIR CHECKPOINT_PG_BENCH_DIR; do
    if [ -n "${!name:-}" ]; then
      _absolutize_under_it "${name}"
    fi
  done
}

# Remove one or more bind-mount directories that a container may have written as
# root.
#
# A bind mount is the one place where the container's uid survives onto the host:
# Doris' FE/BE run as root and leave `workdir/doris/**` owned by root, so the
# runner user cannot unlink it and a plain `rm -rf` dies with EACCES. Escalate
# first. `sudo -n` (non-interactive) is what keeps a host that would ask for a
# password from hanging a CI job; if it is unavailable or refused, the plain rm
# still handles the local case where the leftovers are ours.
#
# Best-effort by design: the path is normally absent (a fresh runner), and the
# caller must never abort under `set -e` because a cleanup had nothing to remove.
it_rm_rf() {
  local path
  for path in "$@"; do
    # `-e` alone misses a dangling symlink; nothing here creates one, but the
    # test is cheaper than knowing that.
    if [ ! -e "${path}" ] && [ ! -L "${path}" ]; then
      continue
    fi
    if sudo -n rm -rf "${path}" 2>/dev/null; then
      continue
    fi
    rm -rf "${path}" 2>/dev/null ||
      log "warning: could not remove ${path} (still owned by a container?)" >&2
  done
}

# ----------------------------------------------------------------- compose set
# The two helpers below are the single source of truth for "which project do I
# talk to": setup.sh and teardown.sh must resolve a sink to the exact same name,
# otherwise a teardown deletes another run's stack.
it_project_name() {
  local sink="$1"
  if [ -n "${COMPOSE_PROJECT_NAME:-}" ]; then
    echo "${COMPOSE_PROJECT_NAME}"
    return
  fi
  # Per sink, deliberately NOT per variant: pg_compat loops pg14..pg18 inside a
  # single job, and a per-variant project would leave the previous variant's
  # MySQL holding host port 13306 while the next one starts. setup.sh always
  # drops the project before `up`, which is what makes a version switch clean.
  # CI matrix rows never share a runner, so per-variant isolation buys nothing.
  # The trailing `-mysql` is not decoration: the MySQL source is part of every
  # project, so the name says what the project actually contains.
  echo "gocdc-${sink}-mysql"
}

# The same file set, colon-joined, so it can be exported as COMPOSE_FILE: a
# harness that shells out to ``docker compose`` (pg_it restarts the server)
# then resolves this stack from any working directory.
it_compose_file_list() {
  local sink="$1" out="" file
  while IFS= read -r file; do
    out="${out:+${out}:}${file}"
  done < <(it_compose_files "${sink}")
  echo "${out}"
}

# One compose file per participant: the MySQL source always, plus the sink's own
# file unless the sink IS that MySQL instance (mysql/stdout).
it_compose_files() {
  local sink="$1"
  echo "${IT}/sources/mysql/docker-compose.yml"
  case "${sink}" in
    mysql | stdout) ;;
    *) echo "${IT}/sinks/${sink}/docker-compose.yml" ;;
  esac
}

# ---------------------------------------------------------------- variant table
# Variant -> image mapping (the version matrix this repo must never lose):
#
#   mysql / stdout / kafka   57 | 80              -> MYSQL_TAG, MYSQL_IMAGE
#   postgresql               pg14..pg18 | 14..18  -> PG_IMAGE, PG_EXPECT_MAJOR
#   elasticsearch            es7 | es8 | es9      -> ES_SINK_VERSION (+ URL)
#   doris                    2.1.8                -> DORIS_VERSION
#
# It lives here, not inside setup.sh, because setup.sh runs as a child process:
# whatever it exports is gone when it returns. run.sh needs the same values
# (ES_URL/ES_SINK_VERSION for the HTTP suites, PG_EXPECT_MAJOR for the guard),
# so both callers resolve the variant through this one function.
#
# One variant per process: run.sh and setup.sh each call this once, and pg_compat
# re-execs setup.sh per major with PG_IMAGE pinned in the child env. Version
# *selectors* are therefore assigned unconditionally (the variant is the source
# of truth, and a stale value from an earlier call in the same shell cannot win);
# only the image / URL stay overridable with `${VAR:-default}`, which is what
# lets CI pin a minor (PG_COMPAT_IMAGE_<major>) or an external endpoint.
it_apply_variant() {
  local sink="$1" variant="${2:-}" major
  case "${sink}" in
    mysql | stdout | kafka)
      case "${variant}" in
        "" | 80 | 8.0 | 8.0.*)
          # The variant owns the tag; only the image stays overridable. A sticky
          # `${MYSQL_TAG:-80}` would keep "57" when a caller switches variants in
          # one shell (pg_compat-style loops), which is exactly the confusion the
          # per-variant table exists to prevent.
          export MYSQL_TAG="80"
          export MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0.36}"
          ;;
        57 | 5.7 | 5.7.*)
          export MYSQL_TAG="57"
          export MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:5.7.42}"
          ;;
        *)
          log "error: unknown mysql variant '${variant}' (use 57 or 80)" >&2
          return 2
          ;;
      esac
      ;;
    postgresql)
      major="${variant#pg}"
      major="${major:-${PG_EXPECT_MAJOR:-16}}"
      case "${major}" in
        14 | 15 | 16 | 17 | 18) ;;
        *)
          log "error: unknown postgresql variant '${variant}' (use pg14..pg18)" >&2
          return 2
          ;;
      esac
      # `postgres:16.4` is the everyday pin and must stay identical to the
      # fallback in sinks/postgresql/docker-compose.yml: pg_compat reports the
      # image *the compose file names*, so a bare `postgres:16` here would make
      # the report describe a tag that never ran.
      if [ "${major}" = "16" ]; then
        export PG_IMAGE="${PG_IMAGE:-postgres:16.4}"
      else
        export PG_IMAGE="${PG_IMAGE:-postgres:${major}}"
      fi
      # The E2E suite refuses to seed a server of another major version, so a
      # stale container can never be reported as this variant's result. The
      # variant owns the expectation (a stale value from an earlier iteration in
      # the same shell would otherwise pin the guard to the wrong major); the
      # image stays overridable because pinning a minor is documented.
      export PG_EXPECT_MAJOR="${major}"
      ;;
    elasticsearch)
      case "${variant:-es8}" in
        8 | es8)
          export ES_SINK_VERSION="8"
          # Cleared, not inherited: after an es7/es9 run in the same shell the
          # guard would otherwise reject the es8 node as "another major".
          export ES_EXPECT_MAJOR=""
          export ES_SERVICE="elasticsearch"
          export ES_HEAP="${ES_HEAP:-512m}"
          export ES_URL="${ES_URL:-http://127.0.0.1:${ES_HOST_PORT:-19200}}"
          ;;
        7 | es7)
          export ES_SINK_VERSION="7"
          export ES_EXPECT_MAJOR="7"
          export ES_SERVICE="elasticsearch7"
          export ES7_HEAP="${ES7_HEAP:-512m}"
          export ES_URL="${ES7_URL:-http://127.0.0.1:${ES7_HOST_PORT:-19201}}"
          ;;
        9 | es9)
          export ES_SINK_VERSION="9"
          export ES_EXPECT_MAJOR="9"
          export ES_SERVICE="elasticsearch9"
          export ES9_HEAP="${ES9_HEAP:-512m}"
          export ES_URL="${ES9_URL:-http://127.0.0.1:${ES9_HOST_PORT:-19202}}"
          ;;
        *)
          log "error: unknown elasticsearch variant '${variant}' (use es7|es8|es9)" >&2
          return 2
          ;;
      esac
      ;;
    doris)
      case "${variant}" in
        "" | 2.1.*) export DORIS_VERSION="${DORIS_VERSION:-${variant:-2.1.8}}" ;;
        *)
          log "error: unknown doris variant '${variant}' (use a 2.1.x tag)" >&2
          return 2
          ;;
      esac
      ;;
  esac
}
