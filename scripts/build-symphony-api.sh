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
IMAGE="margo-symphony-api"
IMAGE_BASE="$REGISTRY/$OWNER/$IMAGE"
# TAG_TS="margo-symphony-api-$(date +%Y%m%d%H%M%S)"
DOCKERFILE="$REPO_ROOT/api/Dockerfile"

info() { echo "ℹ️  $1"; }
ok()   { echo "✅ $1"; }

info "Image      : $IMAGE_BASE"
info "Dockerfile : $DOCKERFILE"
info "Tags       : latest"

# --------------------------------------------------
# Authenticate ONLY in GitHub Actions
# --------------------------------------------------
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  info "GitHub Actions detected"

  if ! docker system info | grep -q ghcr.io; then
    info "Logging into GHCR using GITHUB_TOKEN"
    echo "$GITHUB_TOKEN" | docker login ghcr.io \
      -u "$GITHUB_ACTOR" \
      --password-stdin
    ok "Authenticated to GHCR"
  fi
else
  info "Local run detected – skipping login"
fi

# --------------------------------------------------
# Ensure buildx builder exists
# --------------------------------------------------
if ! docker buildx inspect symphony-builder >/dev/null 2>&1; then
  info "Creating buildx builder"
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
  --tag "$IMAGE_BASE:latest" \
  -f "$DOCKERFILE" \
  "$REPO_ROOT"

ok "Image pushed"

# --------------------------------------------------
# Make image PUBLIC (critical step)
# --------------------------------------------------
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  info "Setting GHCR image visibility to PUBLIC"

  gh api \
    -X PATCH \
    /orgs/$OWNER/packages/container/$IMAGE/visibility \
    -f visibility=public

  ok "Image is now PUBLIC"
fi

echo "🎉 Done!"
echo "Public image:"
echo "  docker pull $IMAGE_BASE:latest"