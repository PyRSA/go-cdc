#!/usr/bin/env bash
# Install go-cdc by building from source, then copying the binary into a bindir.
#
# Usage (from a clone):
#   ./install.sh                      # → $HOME/bin/go-cdc
#   ./install.sh --user               # → $HOME/bin/go-cdc
#   ./install.sh --global             # → /usr/local/bin/go-cdc (may need sudo)
#   ./install.sh -b /opt/go-cdc/bin   # custom dir + ensure PATH in ~/.bashrc
#   ./install.sh -b "$(go env GOPATH)/bin"
#   ./install.sh -b ./bin             # project-local ./bin
#
# Remote (curl | bash), same flags as golangci-lint style:
#   curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s
#   curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s -- -b "$(go env GOPATH)/bin"
#   curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s -- -b ./bin v0.1.0
#
# Optional trailing VERSION is a git tag/branch/ref used when cloning (ignored for local tree builds).
#
# Windows (PowerShell):
#   irm https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.ps1 | iex

set -euo pipefail

REPO_URL="${GO_CDC_REPO:-https://github.com/PyRSA/go-cdc.git}"
DEFAULT_REF="${GO_CDC_REF:-main}"
BINARY_NAME="go-cdc"

BINDIR=""
MODE="" # user | global | (empty → user default, or set by -b)
VERSION=""
UPDATE_BASHRC=0
CLEANUP_DIR=""

usage() {
  cat <<'EOF'
Install go-cdc (build from source, then install the binary).

Usage:
  install.sh [--user|--global] [-b DIR] [VERSION]
  curl -sSfL <install.sh URL> | bash -s -- [flags] [VERSION]

Options:
  --user          Install to $HOME/bin (default)
  --global        Install to /usr/local/bin (may prompt for sudo)
  -b, --bin-dir   Install directory (golangci-lint style). For custom dirs,
                  ensures PATH is exported in $HOME/.bashrc when needed.
  -h, --help      Show this help

VERSION (optional):
  Git tag/branch/ref when the script clones the repo (curl|bash). Ignored when
  running inside an existing go-cdc checkout (builds the current tree).

Examples:
  ./install.sh
  ./install.sh --global
  ./install.sh -b "$(go env GOPATH)/bin"
  ./install.sh -b ./bin
  curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s -- -b "$(go env GOPATH)/bin"
EOF
}

log() { printf '+ %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      -h | --help)
        usage
        exit 0
        ;;
      --user)
        MODE="user"
        shift
        ;;
      --global)
        MODE="global"
        shift
        ;;
      -b | --bin-dir)
        [ $# -ge 2 ] || die "-b requires a directory"
        BINDIR="$2"
        UPDATE_BASHRC=1
        shift 2
        ;;
      --)
        shift
        break
        ;;
      -*)
        die "unknown option: $1 (try --help)"
        ;;
      *)
        VERSION="$1"
        shift
        ;;
    esac
  done
  while [ $# -gt 0 ]; do
    VERSION="$1"
    shift
  done
}

resolve_bindir() {
  if [ -n "${BINDIR}" ]; then
    # Expand ~/ and relative paths after we know cwd for ./bin
    case "${BINDIR}" in
      ~/*) BINDIR="${HOME}/${BINDIR#~/}" ;;
    esac
    return
  fi
  case "${MODE}" in
    global) BINDIR="/usr/local/bin" ;;
    user | "") BINDIR="${HOME}/bin" ;;
    *) die "internal: unknown mode ${MODE}" ;;
  esac
}

# Prefer the local checkout when this file lives in a go-cdc repo.
find_local_repo() {
  local src here
  src="${BASH_SOURCE[0]:-}"
  # Piped to bash: BASH_SOURCE may be empty or not a real file.
  if [ -z "${src}" ] || [ ! -f "${src}" ]; then
    return 1
  fi
  here="$(cd "$(dirname "${src}")" && pwd)"
  if [ -f "${here}/go.mod" ] && [ -f "${here}/Makefile" ] && [ -d "${here}/cmd/go-cdc" ]; then
    printf '%s\n' "${here}"
    return 0
  fi
  return 1
}

clone_repo() {
  need_cmd git
  local tmp ref
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/go-cdc-install.XXXXXX")"
  CLEANUP_DIR="${tmp}"
  ref="${VERSION:-${DEFAULT_REF}}"
  log "cloning ${REPO_URL} (ref=${ref}) into ${tmp}"
  if git clone --depth 1 --branch "${ref}" "${REPO_URL}" "${tmp}/go-cdc" 2>/dev/null; then
    printf '%s\n' "${tmp}/go-cdc"
    return 0
  fi
  # Fallback: full clone + checkout (works for arbitrary SHAs).
  git clone "${REPO_URL}" "${tmp}/go-cdc"
  git -C "${tmp}/go-cdc" checkout "${ref}"
  printf '%s\n' "${tmp}/go-cdc"
}

build_binary() {
  local root="$1"
  need_cmd go
  need_cmd make
  log "building go-cdc in ${root}"
  (cd "${root}" && make build)
  [ -x "${root}/bin/${BINARY_NAME}" ] || die "build did not produce ${root}/bin/${BINARY_NAME}"
}

install_binary() {
  local src_bin="$1"
  local dest_dir="$2"
  local dest="${dest_dir%/}/${BINARY_NAME}"
  local installer="install"

  # Resolve relative bindir (e.g. ./bin) against current working directory.
  case "${dest_dir}" in
    /*) ;;
    *) dest_dir="$(pwd)/${dest_dir}" ; dest="${dest_dir%/}/${BINARY_NAME}" ;;
  esac

  if [ ! -d "${dest_dir}" ]; then
    if [ -w "$(dirname "${dest_dir}")" ] 2>/dev/null || mkdir -p "${dest_dir}" 2>/dev/null; then
      mkdir -p "${dest_dir}"
    else
      need_cmd sudo
      log "creating ${dest_dir} with sudo"
      sudo mkdir -p "${dest_dir}"
    fi
  fi

  if [ -w "${dest_dir}" ]; then
    install -m 755 "${src_bin}" "${dest}"
  else
    need_cmd sudo
    log "installing to ${dest} with sudo"
    sudo install -m 755 "${src_bin}" "${dest}"
  fi
  log "installed ${dest}"
  BINDIR="${dest_dir}"
}

path_has_dir() {
  local dir="$1"
  case ":${PATH}:" in
    *":${dir}:"*) return 0 ;;
    *) return 1 ;;
  esac
}

ensure_bashrc_path() {
  local dir="$1"
  local rc="${HOME}/.bashrc"
  local marker

  # /usr/local/bin is normally already on PATH; do not touch bashrc for --global.
  if [ "${dir}" = "/usr/local/bin" ]; then
    return 0
  fi

  # Skip ephemeral dirs (e.g. under /tmp).
  case "${dir}" in
    /tmp/* | /var/tmp/* | "${TMPDIR:-/tmp}"/*)
      log "skip bashrc PATH update for temporary dir ${dir}"
      return 0
      ;;
  esac

  # Update bashrc for: explicit -b, or default $HOME/bin when not already on PATH.
  if [ "${UPDATE_BASHRC}" -ne 1 ] && [ "${dir}" != "${HOME}/bin" ]; then
    return 0
  fi

  if path_has_dir "${dir}"; then
    log "PATH already contains ${dir}"
    return 0
  fi

  marker="# go-cdc PATH (${dir})"
  if [ -f "${rc}" ] && grep -Fq "${marker}" "${rc}" 2>/dev/null; then
    log "${rc} already configures PATH for ${dir}"
  else
    touch "${rc}"
    {
      printf '\n'
      printf '%s\n' "${marker}"
      printf 'export PATH="%s:$PATH"\n' "${dir}"
    } >>"${rc}"
    log "added ${dir} to PATH in ${rc}"
    log "run: source ${rc}   (or open a new shell)"
  fi
}

cleanup() {
  if [ -n "${CLEANUP_DIR}" ] && [ -d "${CLEANUP_DIR}" ]; then
    rm -rf "${CLEANUP_DIR}"
  fi
}
trap cleanup EXIT

main() {
  parse_args "$@"
  resolve_bindir

  need_cmd uname
  case "$(uname -s)" in
    Linux | Darwin) ;;
    MINGW* | MSYS* | CYGWIN*)
      die "use install.ps1 on Windows: irm https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.ps1 | iex"
      ;;
    *)
      die "unsupported OS: $(uname -s)"
      ;;
  esac

  local root
  if root="$(find_local_repo)"; then
    log "using local repository ${root}"
    if [ -n "${VERSION}" ]; then
      log "note: VERSION=${VERSION} ignored for local checkout (building current tree)"
    fi
  else
    root="$(clone_repo)"
  fi

  build_binary "${root}"
  install_binary "${root}/bin/${BINARY_NAME}" "${BINDIR}"
  ensure_bashrc_path "${BINDIR}"

  log "done. verify: ${BINDIR}/${BINARY_NAME} --help"
  if ! path_has_dir "${BINDIR}" && ! command -v "${BINARY_NAME}" >/dev/null 2>&1; then
    log "if 'go-cdc' is not found, add ${BINDIR} to PATH or: source ~/.bashrc"
  fi
}

main "$@"
