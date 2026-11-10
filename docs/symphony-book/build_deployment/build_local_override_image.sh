#!/usr/bin/env bash

set -euo pipefail

print_usage() {
  cat <<'EOF'
Build and export the Symphony API Docker image.

Usage:
  build_local_override_image.sh [options]

Options:
  -h, --help            Show this help text.
  --local-override      Enable local sandbox override mode.
  --no-local-override   Disable local sandbox override mode.
  --no-cache            Build without Docker cache (default).
  --cache               Allow Docker build cache.
  --output-tar <path>   Output path for docker save tar archive.

Environment variables:
  USE_LOCAL_OVERRIDE    true|false, default: false.
  NO_CACHE              true|false, default: true.
  OUTPUT_TAR            Archive output path, default: <repo>/symphony-api.tar.

Examples:
  bash docs/symphony-book/build_deployment/build_local_override_image.sh
  USE_LOCAL_OVERRIDE=true bash docs/symphony-book/build_deployment/build_local_override_image.sh
  bash docs/symphony-book/build_deployment/build_local_override_image.sh --local-override
  bash docs/symphony-book/build_deployment/build_local_override_image.sh --cache --output-tar /tmp/symphony-api.tar
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
API_DIR="${REPO_ROOT}/api"
OUTPUT_TAR="${OUTPUT_TAR:-${REPO_ROOT}/symphony-api.tar}"
DOCKERFILE_PATH="${API_DIR}/Dockerfile"
COMPOSE_PATH="${API_DIR}/docker-compose.yaml"
GO_MOD_PATH="${API_DIR}/go.mod"
LOCAL_SANDBOX_REPLACE="replace github.com/margo/sandbox => ../../sandbox"
USE_LOCAL_OVERRIDE="${USE_LOCAL_OVERRIDE:-false}"
NO_CACHE="${NO_CACHE:-true}"

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
  backup_file "$COMPOSE_PATH"
  backup_file "$DOCKERFILE_PATH"
  backup_file "$GO_MOD_PATH"

  # Patch compose paths only when they are at baseline values.
  # This keeps the operation idempotent across repeated runs.
  if grep -qE '^[[:space:]]*context:[[:space:]]*../$' "$COMPOSE_PATH"; then
    sed -i -E 's|^([[:space:]]*context:[[:space:]]*)../$|\1../../|' "$COMPOSE_PATH"
  fi
  if grep -qE '^[[:space:]]*dockerfile:[[:space:]]*api/Dockerfile$' "$COMPOSE_PATH"; then
    sed -i -E 's|^([[:space:]]*dockerfile:[[:space:]]*)api/Dockerfile$|\1symphony/api/Dockerfile|' "$COMPOSE_PATH"
  fi

  sed -i 's|COPY ./packages /workspace/packages|COPY ./symphony/packages /workspace/packages|g' "$DOCKERFILE_PATH"
  sed -i 's|COPY ./coa /workspace/coa|COPY ./symphony/coa /workspace/coa|g' "$DOCKERFILE_PATH"
  sed -i 's|COPY ./api /workspace/api|COPY ./symphony/api /workspace/api|g' "$DOCKERFILE_PATH"
  if ! grep -q '^COPY ./sandbox /sandbox$' "$DOCKERFILE_PATH"; then
    sed -i '/^COPY \.\/symphony\/api \/workspace\/api$/a COPY ./sandbox /sandbox' "$DOCKERFILE_PATH"
  fi
  sed -i 's|ADD ./api/symphony-api.json /|ADD ./symphony/api/symphony-api.json /|g' "$DOCKERFILE_PATH"
  sed -i 's|ADD ./api/symphony-api-margo.json /|ADD ./symphony/api/symphony-api-margo.json /|g' "$DOCKERFILE_PATH"

  if grep -q '^replace github.com/margo/sandbox => ' "$GO_MOD_PATH"; then
    sed -i "s|^replace github.com/margo/sandbox => .*|${LOCAL_SANDBOX_REPLACE}|g" "$GO_MOD_PATH"
  elif grep -q '^replace github.com/eclipse-symphony/symphony/packages/mage => ../packages/mage$' "$GO_MOD_PATH"; then
    sed -i '/^replace github.com\/eclipse-symphony\/symphony\/packages\/mage => ..\/packages\/mage$/a \
\
'"$LOCAL_SANDBOX_REPLACE" "$GO_MOD_PATH"
  else
    printf '\n%s\n' "$LOCAL_SANDBOX_REPLACE" >> "$GO_MOD_PATH"
  fi

  trap restore_patched_files EXIT
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
    --no-cache)
      NO_CACHE=true
      ;;
    --cache)
      NO_CACHE=false
      ;;
    --output-tar)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --output-tar" >&2
        exit 1
      fi
      OUTPUT_TAR="$2"
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

cd "${API_DIR}"

COMPOSE_ARGS=(-f docker-compose.yaml)
if [[ "${USE_LOCAL_OVERRIDE}" == "true" ]]; then
  echo "Applying local override directly in compose and Dockerfile"
  apply_local_override_inplace
fi

echo "Building Symphony API image (no cache)..."
BUILD_ARGS=()
if [[ "${NO_CACHE}" == "true" ]]; then
  BUILD_ARGS+=(--no-cache)
fi
docker compose "${COMPOSE_ARGS[@]}" build "${BUILD_ARGS[@]}" api

IMAGE_ID="$(docker compose "${COMPOSE_ARGS[@]}" images -q api | head -n 1 || true)"
IMAGE_REF="$(docker compose "${COMPOSE_ARGS[@]}" config | awk '
  $1 == "api:" { in_api=1; next }
  in_api && $1 == "image:" { print $2; exit }
')"

if [[ -z "${IMAGE_REF}" ]]; then
  IMAGE_REF="ghcr.io/eclipse-symphony/symphony-api:latest"
fi

if [[ -z "${IMAGE_ID}" ]] && docker image inspect "${IMAGE_REF}" >/dev/null 2>&1; then
  IMAGE_ID="$(docker image inspect --format '{{.Id}}' "${IMAGE_REF}")"
fi

if [[ -z "${IMAGE_ID}" ]]; then
  echo "Failed to resolve built image ID for service 'api'." >&2
  echo "Tried compose image query and local inspect for '${IMAGE_REF}'." >&2
  exit 1
fi

SHORT_SHA="${IMAGE_ID#sha256:}"
SHORT_SHA="${SHORT_SHA:0:12}"

echo "Saving image ${IMAGE_ID} to ${OUTPUT_TAR}..."
cd "${REPO_ROOT}"
docker save "${IMAGE_ID}" -o "${OUTPUT_TAR}"

echo "Done. Short image SHA: ${SHORT_SHA}"
echo "Packed image archive: ${OUTPUT_TAR}"