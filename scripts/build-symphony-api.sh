#!/usr/bin/env bash
set -Eeuo pipefail

# --------------------------------------------------
# Resolve repo root safely
# --------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# --------------------------------------------------
# Image configuration
# --------------------------------------------------
REGISTRY="ghcr.io"
OWNER="margo"
REPO="symphony"
IMAGE="margo-symphony-api-v5"
IMAGE_BASE="${REGISTRY}/${OWNER}/${IMAGE}"
DOCKERFILE="${REPO_ROOT}/api/Dockerfile"
TAG="latest"

# --------------------------------------------------
# GitHub auth (custom env names)
# --------------------------------------------------
TOKEN_GITHUB="${TOKEN_GITHUB:-}"
ACTOR_GITHUB="${ACTOR_GITHUB:-}"

info() { echo "ℹ️  $1"; }
ok()   { echo "✅ $1"; }
warn() { echo "⚠️  $1"; }

info "Image      : ${IMAGE_BASE}:${TAG}"
info "Dockerfile : ${DOCKERFILE}"

# --------------------------------------------------
# Authenticate to GHCR
# --------------------------------------------------
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  info "GitHub Actions detected"
  if [[ -z "$TOKEN_GITHUB" || -z "$ACTOR_GITHUB" ]]; then
    echo "❌ TOKEN_GITHUB or ACTOR_GITHUB is not set"
    exit 1
  fi
  echo "$TOKEN_GITHUB" | docker login ghcr.io -u "$ACTOR_GITHUB" --password-stdin
  ok "Authenticated to GHCR"
else
  info "Local run detected – skipping login"
fi

# --------------------------------------------------
# Ensure buildx builder exists
# --------------------------------------------------
if ! docker buildx inspect symphony-builder >/dev/null 2>&1; then
  docker buildx create --name symphony-builder --use
else
  docker buildx use symphony-builder
fi

# --------------------------------------------------
# Build & Push image
# --------------------------------------------------
info "Building and pushing image..."
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --push \
  --cache-from type=gha \
  --cache-to type=gha,mode=max \
  --tag "${IMAGE_BASE}:${TAG}" \
  -f "${DOCKERFILE}" \
  "${REPO_ROOT}"
ok "Image pushed"

# --------------------------------------------------
# GHCR visibility (Option 2 - REST API Fix)
# --------------------------------------------------
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  info "Ensuring GHCR image is public via REST API"
  
  # Check if OWNER is an Org or a User to use the correct endpoint
  IS_ORG=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${TOKEN_GITHUB}" "https://api.github.com{OWNER}")
  
  if [[ "$IS_ORG" == "200" ]]; then
    PKG_API="https://api.github.com{OWNER}/packages/container/${IMAGE}"
  else
    PKG_API="https://api.github.com{OWNER}/packages/container/${IMAGE}"
  fi

  # Apply visibility patch
  RESPONSE=$(curl -s -X PATCH \
    -H "Authorization: Bearer ${TOKEN_GITHUB}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "${PKG_API}" \
    -d '{"visibility":"public"}')

  # Validate response
  if echo "$RESPONSE" | grep -q '"visibility":"public"'; then
    ok "Image visibility set to PUBLIC"
  else
    warn "Could not update visibility. API Response: $RESPONSE"
    warn "Ensure TOKEN_GITHUB has 'admin:package' or 'write:packages' scopes."
  fi
fi

echo "--------------------------------------------------"
ok "Done!"
echo "docker pull ${IMAGE_BASE}:${TAG}"
