#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/publish-zig-runtime-dev.sh [--tag TAG] [--arch amd64|arm64] [--optimize ReleaseFast|ReleaseSmall] [--jobs N] [--manifest [--amd64-digest DIGEST --arm64-digest DIGEST]|--image-ref]

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
image_ref=false
arch=""
optimize="ReleaseFast"
jobs=""
amd64_digest=""
arm64_digest=""

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
    --optimize)
      optimize="${2:?--optimize requires a value}"
      shift 2
      ;;
    --jobs)
      jobs="${2:?--jobs requires a value}"
      shift 2
      ;;
    --amd64-digest)
      amd64_digest="${2:?--amd64-digest requires a value}"
      shift 2
      ;;
    --arm64-digest)
      arm64_digest="${2:?--arm64-digest requires a value}"
      shift 2
      ;;
    --manifest)
      manifest=true
      shift
      ;;
    --image-ref)
      image_ref=true
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

if [[ "$manifest" == true && "$image_ref" == true ]]; then
  echo "--manifest and --image-ref cannot be used together" >&2
  exit 2
fi

if [[ "$arch" != "" && "$arch" != "amd64" && "$arch" != "arm64" ]]; then
  echo "--arch must be amd64 or arm64" >&2
  exit 2
fi

case "$optimize" in ReleaseFast|ReleaseSmall) ;; *) echo "--optimize must be ReleaseFast or ReleaseSmall" >&2; exit 2;; esac
[[ -z "$jobs" || "$jobs" =~ ^[1-9][0-9]*$ ]] || { echo "--jobs must be a positive integer" >&2; exit 2; }
[[ -z "$amd64_digest" || "$amd64_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "invalid amd64 digest" >&2; exit 2; }
[[ -z "$arm64_digest" || "$arm64_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "invalid arm64 digest" >&2; exit 2; }

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

if [[ "$image_ref" == true ]]; then
  printf '%s\n' "${image_base}:${tag}"
  exit 0
fi

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
  [[ -z "$amd64_digest" && -z "$arm64_digest" || ( -n "$amd64_digest" && -n "$arm64_digest" ) ]] || { echo "both child digests are required" >&2; exit 2; }
  if lookup="$(gcloud artifacts docker images describe "${image_base}:${tag}" \
      --project="$gcp_project" \
      --format='value(image_summary.digest)' 2>&1)"; then
    echo "refusing to retag existing immutable image: ${image_base}:${tag}" >&2
    exit 1
  elif [[ "$lookup" != *NOT_FOUND* && "$lookup" != *"not found"* ]]; then
    echo "unable to determine whether immutable image exists: $lookup" >&2
    exit 1
  fi
  amd64_ref="${image_base}:${tag}-amd64"
  arm64_ref="${image_base}:${tag}-arm64"
  [[ -z "$amd64_digest" ]] || amd64_ref="${image_base}@${amd64_digest}"
  [[ -z "$arm64_digest" ]] || arm64_ref="${image_base}@${arm64_digest}"
  gcloud builds submit "$repo_root" \
    --project="$gcp_project" \
    --region="$gcp_region" \
    --worker-pool="$worker_pool" \
    --config=zig/cloudbuild.manifest.yaml \
    --substitutions="_IMAGE_NAME=antfly,_VERSION_TAG=${tag},_ALIAS_TAG=__skip_alias__,_AMD64_REF=${amd64_ref},_ARM64_REF=${arm64_ref}"
  inspect_image "${image_base}:${tag}"
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
jobs_arg=()
[[ -z "$jobs" ]] || jobs_arg=("-j${jobs}")
(
  cd "$repo_root/zig"
  zig build \
    install \
    -Dtarget="$zig_target" \
    -Doptimize="$optimize" \
    -Dedition=full \
    --prefix "$out_dir" \
    --global-cache-dir "$cache_dir" \
    "${jobs_arg[@]+${jobs_arg[@]}}"
)

test -x "$out_dir/bin/antfly"
test -d "$out_dir/share/antfly"
archive_dir="$tmpdir/archive"
mkdir -p "$archive_dir/share"
install -m 0755 "$out_dir/bin/antfly" "$archive_dir/antfly"
cp -a "$out_dir/share/antfly" "$archive_dir/share/antfly"
archive="$tmpdir/antfly-zig-${arch}.tar.gz"
tar -C "$archive_dir" -czf "$archive" antfly share
extract_dir="$tmpdir/extract"
mkdir "$extract_dir"
tar -xzf "$archive" -C "$extract_dir"
test -x "$extract_dir/antfly"
test -d "$extract_dir/share/antfly"

echo "Uploading $artifact_uri"
gcloud storage cp "$archive" "$artifact_uri" --project="$gcp_project"

echo "Packaging ${image_base}:${tag}-${arch}"
gcloud builds submit "$repo_root" \
  --project="$gcp_project" \
  --region="$gcp_region" \
  --worker-pool="$worker_pool" \
  --config=zig/cloudbuild.runtime.yaml \
  --substitutions="_ARTIFACT_URI=${artifact_uri},_IMAGE_NAME=antfly,_DOCKERFILE=zig/Dockerfile.runtime,_CONTEXT=/workspace/.zig-container,_ALIAS_TAG=__skip_alias__,_VERSION_TAG=${tag}-${arch},_PLATFORMS=linux/${arch},_DESCRIPTION=AntflyDB Zig runtime image"

inspect_image "${image_base}:${tag}-${arch}"

cat <<EOF

Pushed: ${image_base}:${tag}-${arch}

To create the multi-arch tag after both arch images exist:
  scripts/publish-zig-runtime-dev.sh --tag ${tag} --manifest
EOF
