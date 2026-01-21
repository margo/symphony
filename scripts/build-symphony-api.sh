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
OWNER="margo"                         # GitHub org or user
IMAGE="margo-symphony-api-v3"
IMAGE_BASE="${REGISTRY}/${OWNER}/${IMAGE}"
DOCKERFILE="${REPO_ROOT}/api/Dockerfile"
TAG="V1"
info() { echo "ℹ️  $1"; }
ok()   { echo "✅ $1"; }
warn() { echo "⚠️  $1"; }
info "Image      : ${IMAGE_BASE}:${TAG}"
info "Dockerfile : ${DOCKERFILE}"
# --------------------------------------------------
# Authenticate ONLY in GitHub Actions
# --------------------------------------------------
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
 info "GitHub Actions detected"
 if [[ -z "${TOKEN_GITHUB:-}" || -z "${ACTOR_GITHUB:-}" ]]; then
   echo "❌ TOKEN_GITHUB or ACTOR_GITHUB is not set"
   exit 1
 fi
 echo "${TOKEN_GITHUB}" | docker login ghcr.io \
   -u "${ACTOR_GITHUB}" \
   --password-stdin
 ok "Authenticated to GHCR"
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
 --tag "${IMAGE_BASE}:${TAG}" \
 -f "${DOCKERFILE}" \
 "${REPO_ROOT}"
ok "Image pushed"
# --------------------------------------------------
# Make GHCR image PUBLIC (GitHub Actions only)
# --------------------------------------------------
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
 info "Setting GHCR image visibility to PUBLIC"
 PKG_API="https://api.github.com/orgs/${OWNER}/packages/container/${IMAGE}"
 HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
   -X PATCH \
   -H "Authorization: Bearer ${TOKEN_GITHUB}" \
   -H "Accept: application/vnd.github+json" \
   "${PKG_API}" \
   -d '{"visibility":"public"}')
 if [[ "${HTTP_CODE}" == "200" ]]; then
   ok "Image visibility set to PUBLIC"
 elif [[ "${HTTP_CODE}" == "422" ]]; then
   ok "Image already PUBLIC"
 else
   warn "Failed to update visibility (HTTP ${HTTP_CODE})"
 fi
else
 info "Skipping visibility update (not in GitHub Actions)"
fi
echo "--------------------------------------------------"
ok "Done!"
echo "docker pull ${IMAGE_BASE}:${TAG}"