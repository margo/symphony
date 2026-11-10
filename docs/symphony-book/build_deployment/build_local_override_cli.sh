#!/usr/bin/env bash

set -euo pipefail

print_usage() {
  cat <<'EOF'
Build and export the Maestro CLI binary with optional local sandbox override.

Usage:
  build_local_override_cli.sh [options]

Options:
  -h, --help            Show this help text.
  --local-override      Enable local sandbox override mode (default: true).
  --no-local-override   Disable local sandbox override mode.
  --output <path>       Output path for the built maestro binary (default: $REPO_ROOT/cli/maestro).

Environment variables:
  USE_LOCAL_OVERRIDE    true|false, default: true.
  OUTPUT_BIN            Binary output path.

Examples:
  bash docs/symphony-book/build_deployment/build_local_override_cli.sh
  bash docs/symphony-book/build_deployment/build_local_override_cli.sh --local-override
  bash docs/symphony-book/build_deployment/build_local_override_cli.sh --no-local-override
  bash docs/symphony-book/build_deployment/build_local_override_cli.sh --output /custom/path/maestro
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
CLI_DIR="${REPO_ROOT}/cli"
OUTPUT_BIN="${OUTPUT_BIN:-${CLI_DIR}/maestro}"
GO_MOD_PATH="${CLI_DIR}/go.mod"
LOCAL_SANDBOX_REPLACE="replace github.com/margo/sandbox => ../../sandbox"
USE_LOCAL_OVERRIDE="${USE_LOCAL_OVERRIDE:-true}"

PATCHED_FILES=()

backup_file() {
  local file="$1"
  cp -f "$file" "${file}.bak.local-override"
  PATCHED_FILES+=("$file")
}

restore_patched_files() {
  local file
  for file in "${PATCHED_FILES[@]}"; do
    if [[ -f "${file}.bak.local-override" ]]; then
      mv -f "${file}.bak.local-override" "$file"
    fi
  done
}

apply_local_override_inplace() {
  backup_file "$GO_MOD_PATH"

  if grep -q '^replace github.com/margo/sandbox => ' "$GO_MOD_PATH"; then
    sed -i "s|^replace github.com/margo/sandbox => .*|${LOCAL_SANDBOX_REPLACE}|g" "$GO_MOD_PATH"
  elif grep -q '^replace github.com/eclipse-symphony/symphony/coa => ../coa$' "$GO_MOD_PATH"; then
    sed -i '/^replace github.com\/eclipse-symphony\/symphony\/coa => ..\/coa$/a \
\
'"$LOCAL_SANDBOX_REPLACE" "$GO_MOD_PATH"
  else
    printf '\n%s\n' "$LOCAL_SANDBOX_REPLACE" >> "$GO_MOD_PATH"
  fi

  trap restore_patched_files EXIT
}

ensure_go() {
  if ! command -v go >/dev/null 2>&1; then
    if [[ -x "/usr/local/go/bin/go" ]]; then
      export PATH="/usr/local/go/bin:${PATH}"
    elif [[ -x "${HOME}/go/bin/go" ]]; then
      export PATH="${HOME}/go/bin:${PATH}"
    else
      echo "Error: 'go' command not found in PATH." >&2
      exit 1
    fi
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print_usage
      exit 0
      ;;
    --local-override)
      USE_LOCAL_OVERRIDE=true
      ;;
    --no-local-override)
      USE_LOCAL_OVERRIDE=false
      ;;
    --output)
      if [[ $# -lt 2 ]]; then
        echo "Error: --output requires a file path argument." >&2
        exit 1
      fi
      OUTPUT_BIN="$2"
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Run with --help to see available options." >&2
      exit 1
      ;;
  esac
  shift
done

ensure_go

cd "${CLI_DIR}"

if [[ "${USE_LOCAL_OVERRIDE}" == "true" ]]; then
  echo "Applying local override in ${GO_MOD_PATH}..."
  apply_local_override_inplace
fi

echo "Building Maestro CLI binary..."
mkdir -p "$(dirname "${OUTPUT_BIN}")"
go build -o "${OUTPUT_BIN}" main.go

OUTPUT_DIR="$(cd "$(dirname "${OUTPUT_BIN}")" && pwd)"
echo "Binary created successfully:"
echo "  Path:      ${OUTPUT_BIN}"
echo "  Directory: ${OUTPUT_DIR}"
echo "Done."
