# Shared helpers for IT entry scripts (source this file).
# Usage: log "==> build go-cdc"
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
# Only touch variables that the caller exported (kafka-e2e may omit mysql dirs).
absolutize_checkpoint_dirs() {
  local name
  for name in CHECKPOINT_DIR CHECKPOINT_STDOUT_DIR CHECKPOINT_TYPES_DIR \
    CHECKPOINT_LOAD_DIR CHECKPOINT_KAFKA_DIR CHECKPOINT_KAFKA_LOAD_DIR \
    CHECKPOINT_DORIS_DIR; do
    if [ -n "${!name:-}" ]; then
      _absolutize_under_it "${name}"
    fi
  done
}
