#!/usr/bin/env bash
set -Eeuo pipefail
# --------------------------------------------------
# Resolve repo root safely
# (.github/workflows → repo root)
# --------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"
# --------------------------------------------------
# Image configuration
# --------------------------------------------------
REGISTRY="ghcr.io"
OWNER="margo"
REPO="symphony"
IMAGE="margo-symphony-api"
IMAGE_BASE="${REGISTRY}/${OWNER}/${IMAGE}"
DOCKERFILE="${REPO_ROOT}/api/Dockerfile"
TAG="latest"
# --------------------------------------------------
# GitHub auth variables
# --------------------------------------------------
TOKEN_GITHUB="${TOKEN_GITHUB:-}"
ACTOR_GITHUB="${ACTOR_GITHUB:-}"
info() { echo "ℹ️  $1"; }
ok()   { echo "✅ $1"; }
warn() { echo "⚠️  $1"; }
info "Repo root  : ${REPO_ROOT}"
info "Image      : ${IMAGE_BASE}:${TAG}"
info "Dockerfile : ${DOCKERFILE}"
# --------------------------------------------------
# Validate Dockerfile path (fail fast)
# --------------------------------------------------
if [[ ! -f "$DOCKERFILE" ]]; then
 echo "❌ Dockerfile not found at $DOCKERFILE"
 exit 1
fi
# --------------------------------------------------
# Authenticate to GHCR (GitHub Actions only)
# --------------------------------------------------
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
 info "GitHub Actions detected"
 if [[ -z "$TOKEN_GITHUB" || -z "$ACTOR_GITHUB" ]]; then
   echo "❌ TOKEN_GITHUB or ACTOR_GITHUB is not set"
   exit 1
 fi
 echo "$TOKEN_GITHUB" | docker login ghcr.io \
   -u "$ACTOR_GITHUB" \
   --password-stdin
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
 .
ok "Image pushed"
ok "Done"