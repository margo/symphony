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
OWNER="margo"                      # user or org
IMAGE="margo-symphony-api-v2"
IMAGE_BASE="$REGISTRY/$OWNER/$IMAGE"
DOCKERFILE="$REPO_ROOT/api/Dockerfile"
TAG="V1"
info() { echo "ℹ️  $1"; }
ok()   { echo "✅ $1"; }
warn() { echo "⚠️  $1"; }
info "Image      : $IMAGE_BASE:$TAG"
info "Dockerfile : $DOCKERFILE"
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
 --tag "$IMAGE_BASE:$TAG" \
 -f "$DOCKERFILE" \
 "$REPO_ROOT"
ok "Image pushed"
# --------------------------------------------------
# Make GHCR image public (GitHub Actions only)
# --------------------------------------------------
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
 info "Setting GHCR image visibility to PUBLIC"
 API_URL="https://api.github.com"
 # Use org API if OWNER is an org, otherwise user API
 if curl -s -o /dev/null -w "%{http_code}" \
     -H "Authorization: Bearer $GITHUB_TOKEN" \
     "$API_URL/orgs/$OWNER" | grep -q "200"; then
   PKG_API="$API_URL/orgs/$OWNER/packages/container/$IMAGE"
 else
   PKG_API="$API_URL/user/packages/container/$IMAGE"
 fi
 curl -fsSL -X PATCH \
   -H "Authorization: Bearer $GITHUB_TOKEN" \
   -H "Accept: application/vnd.github+json" \
   "$PKG_API" \
   -d '{"visibility":"public"}' && \
   ok "Image is now PUBLIC" || \
   warn "Failed to update visibility (may already be public)"
else
 info "Skipping visibility update (not in GitHub Actions)"
fi
echo "--------------------------------------------------"
ok "Done!"
echo "docker pull $IMAGE_BASE:$TAG"