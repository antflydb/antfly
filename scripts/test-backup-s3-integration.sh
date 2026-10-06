#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container_name="antfly-backup-s3-$$_${RANDOM}"
minio_image="${MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z}"
access_key="antfly-integration"
secret_key="antfly-integration-secret"
bucket="antfly-backup-integration"

for command in docker curl zig; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "required command not found: ${command}" >&2
    exit 1
  fi
done
if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is unavailable" >&2
  exit 1
fi

cleanup() {
  docker rm -f "${container_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Registry availability is outside the test's control. Pull explicitly with a
# bounded retry, then disable implicit pulls so `docker run` cannot reintroduce
# a one-shot network failure after the image is ready.
if ! docker image inspect "${minio_image}" >/dev/null 2>&1; then
  pulled=false
  for attempt in 1 2 3 4; do
    if docker pull "${minio_image}"; then
      pulled=true
      break
    fi
    if ((attempt < 4)); then
      delay_seconds=$((attempt * 2))
      echo "MinIO image pull failed (attempt ${attempt}/4); retrying in ${delay_seconds}s" >&2
      sleep "${delay_seconds}"
    fi
  done
  if [[ "${pulled}" != true ]]; then
    echo "failed to pull MinIO image after 4 attempts: ${minio_image}" >&2
    exit 1
  fi
fi

docker run --pull=never --rm --detach \
  --name "${container_name}" \
  --publish 127.0.0.1::9000 \
  --env "MINIO_ROOT_USER=${access_key}" \
  --env "MINIO_ROOT_PASSWORD=${secret_key}" \
  "${minio_image}" server /data >/dev/null

endpoint=""
for ((attempt = 0; attempt < 60; attempt++)); do
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
zig build antfly-api-storage-authority-test
