#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/publish-zig-runtime-dev.sh [--tag TAG] [--arch amd64|arm64] [--manifest]

Build the local native Zig runtime artifact, upload it to GCS, and ask Cloud
Build to package/push a single-arch GAR image. Run once on amd64 and once on
arm64 with the same tag, then run with --manifest to create the multi-arch tag.

Environment overrides:
  GCP_PROJECT               default: antfly-image-artifacts
  GCP_REGION                default: us-central1
  GCP_REPOSITORY            default: containers
  ZIG_ARTIFACT_BUCKET       default: antfly-image-artifacts-zig-build-artifacts
  CLOUD_BUILD_WORKER_POOL   default: projects/$GCP_PROJECT/locations/$GCP_REGION/workerPools/antfly-container-builders
  ZIG_GLOBAL_CACHE_DIR      default: $HOME/.cache/zig
USAGE
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

gcp_project="${GCP_PROJECT:-antfly-image-artifacts}"
gcp_region="${GCP_REGION:-us-central1}"
gcp_repository="${GCP_REPOSITORY:-containers}"
artifact_bucket="${ZIG_ARTIFACT_BUCKET:-antfly-image-artifacts-zig-build-artifacts}"
worker_pool="${CLOUD_BUILD_WORKER_POOL:-projects/${gcp_project}/locations/${gcp_region}/workerPools/antfly-container-builders}"
gar_registry="${gcp_region}-docker.pkg.dev"
tag="dev-$(git -C "$repo_root" rev-parse --short=8 HEAD)"
manifest=false
arch=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      tag="${2:?--tag requires a value}"
      shift 2
      ;;
    --arch)
      arch="${2:?--arch requires amd64 or arm64}"
      shift 2
      ;;
    --manifest)
      manifest=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$arch" != "" && "$arch" != "amd64" && "$arch" != "arm64" ]]; then
  echo "--arch must be amd64 or arm64" >&2
  exit 2
fi

host_arch="$(uname -m)"
case "$host_arch" in
  x86_64) native_arch=amd64; zig_target=x86_64-linux-musl ;;
  arm64|aarch64) native_arch=arm64; zig_target=aarch64-linux-musl ;;
  *) echo "unsupported host architecture: $host_arch" >&2; exit 2 ;;
esac

if [[ "$arch" == "" ]]; then
  arch="$native_arch"
fi

if [[ "$arch" != "$native_arch" ]]; then
  echo "refusing non-native Zig artifact build: host is $native_arch, requested $arch" >&2
  echo "run this script on a native $arch machine/runner, or omit --arch" >&2
  exit 2
fi

image_base="${gar_registry}/${gcp_project}/${gcp_repository}/antfly"
artifact_uri="gs://${artifact_bucket}/zig/dev/${tag}/antfly-zig-${arch}.tar.gz"

inspect_image() {
  local image="$1"
  local digest
  digest="$(gcloud artifacts docker images describe "$image" \
    --project="$gcp_project" \
    --format='value(image_summary.digest)' 2>/dev/null || true)"
  if [[ -n "$digest" ]]; then
    echo "Verified GAR image: $image"
    echo "Digest: $digest"
    return 0
  fi

  echo "GAR image metadata lookup failed; trying docker buildx imagetools inspect" >&2
  docker buildx imagetools inspect "$image"
}

if [[ "$manifest" == true ]]; then
  gcloud builds submit "$repo_root" \
    --project="$gcp_project" \
    --region="$gcp_region" \
    --worker-pool="$worker_pool" \
    --config=zig/cloudbuild.manifest.yaml \
    --substitutions="_IMAGE_NAME=antfly,_VERSION_TAG=zig-${tag},_ALIAS_TAG=__skip_alias__,_AMD64_TAG=zig-${tag}-amd64,_ARM64_TAG=zig-${tag}-arm64"
  inspect_image "${image_base}:zig-${tag}"
  exit 0
fi

tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

out_dir="$tmpdir/out"
mkdir -p "$out_dir"

cache_dir="${ZIG_GLOBAL_CACHE_DIR:-$HOME/.cache/zig}"
mkdir -p "$cache_dir"

echo "Building $arch Zig runtime artifact for $zig_target"
(
  cd "$repo_root/zig"
  zig build \
    -Dtarget="$zig_target" \
    -Doptimize=ReleaseFast \
    install-antfly \
    --prefix "$out_dir" \
    --global-cache-dir "$cache_dir"
)

test -x "$out_dir/bin/antfly"
tar -C "$out_dir" -czf "$tmpdir/antfly-zig-${arch}.tar.gz" bin share

echo "Uploading $artifact_uri"
gcloud storage cp "$tmpdir/antfly-zig-${arch}.tar.gz" "$artifact_uri" --project="$gcp_project"

echo "Packaging ${image_base}:zig-${tag}-${arch}"
gcloud builds submit "$repo_root" \
  --project="$gcp_project" \
  --region="$gcp_region" \
  --worker-pool="$worker_pool" \
  --config=zig/cloudbuild.runtime.yaml \
  --substitutions="_ARTIFACT_URI=${artifact_uri},_IMAGE_NAME=antfly,_DOCKERFILE=zig/Dockerfile.runtime,_CONTEXT=/workspace/.zig-container,_ALIAS_TAG=__skip_alias__,_VERSION_TAG=zig-${tag}-${arch},_PLATFORMS=linux/${arch},_DESCRIPTION=AntflyDB Zig runtime image"

inspect_image "${image_base}:zig-${tag}-${arch}"

cat <<EOF

Pushed: ${image_base}:zig-${tag}-${arch}

To create the multi-arch tag after both arch images exist:
  scripts/publish-zig-runtime-dev.sh --tag ${tag} --manifest
EOF
