# Shared timestamped echo for IT entry scripts (source this file).
# Usage: log "==> build go-cdc"
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}
