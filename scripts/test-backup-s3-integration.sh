#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container_name="antfly-backup-s3-$$_${RANDOM}"
minio_image="${MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z}"
access_key="antfly-integration"
secret_key="antfly-integration-secret"
bucket="antfly-backup-integration"

cleanup() {
  docker rm -f "${container_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm --detach \
  --name "${container_name}" \
  --publish 127.0.0.1::9000 \
  --env "MINIO_ROOT_USER=${access_key}" \
  --env "MINIO_ROOT_PASSWORD=${secret_key}" \
  "${minio_image}" server /data >/dev/null

endpoint=""
for _ in $(seq 1 60); do
  endpoint="$(docker port "${container_name}" 9000/tcp 2>/dev/null | head -n 1 || true)"
  if [[ -n "${endpoint}" ]] && curl --fail --silent "http://${endpoint}/minio/health/ready" >/dev/null; then
    break
  fi
  sleep 0.5
done
if [[ -z "${endpoint}" ]] || ! curl --fail --silent "http://${endpoint}/minio/health/ready" >/dev/null; then
  docker logs "${container_name}" >&2
  exit 1
fi

cd "${repo_root}/zig"
OBJECTSTORE_S3_INTEGRATION=1 \
OBJECTSTORE_S3_TEST_BUCKET="${bucket}" \
AWS_ENDPOINT_URL="http://${endpoint}" \
AWS_ACCESS_KEY_ID="${access_key}" \
AWS_SECRET_ACCESS_KEY="${secret_key}" \
AWS_REGION="us-east-1" \
zig build lib-api-storage-authority-test
