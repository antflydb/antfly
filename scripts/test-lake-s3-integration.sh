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
version="v0.0.0-20250422221226-0d7408fc9969"
access_key="antfly-lake-fixture"
secret_key="antfly-lake-fixture-secret"
bucket="antfly-lake-qualification"
report_dir="${repo_root}/.benchmark-results/lake-s3"
cache_dir="${repo_root}/zig/.zig-cache/lake-s3-runtime"
mkdir -p "${report_dir}" "${cache_dir}/bin"
rm -f "${report_dir}/result.json"
for command in curl zig python3 go; do
  command -v "${command}" >/dev/null || { echo "required command not found: ${command}" >&2; exit 1; }
done
# Build the official fixed source through Go's checksum-verified module
# resolver, recording both module and compiled-binary provenance.
export GOCACHE="${cache_dir}/go-build" GOMODCACHE="${cache_dir}/go-mod" GOBIN="${cache_dir}/bin"
export CGO_ENABLED=0 GOMAXPROCS=2
if ! go mod download -json "github.com/minio/minio@${version}" > "${report_dir}/source-module.json"; then
  cat "${report_dir}/source-module.json" >&2
  exit 1
fi
minio="${cache_dir}/bin/minio"
if [[ ! -x "${minio}" ]]; then
  go install "github.com/minio/minio@${version}" > "${report_dir}/source-build.log" 2>&1
fi
go version -m "${minio}" > "${report_dir}/binary-build.txt"
python3 - "${report_dir}" "${minio}" <<'PYVERIFY'
import hashlib, json, pathlib, sys
root, binary = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
module = json.loads((root / "source-module.json").read_text())
assert module["Version"] == "v0.0.0-20250422221226-0d7408fc9969", "unexpected MinIO source version"
assert module["Sum"] == "h1:XqfWRNtM5qMJyGQ4IFsnfHb83fCaRYhrKxJJ0ajXdrQ=", "unexpected MinIO source checksum"
assert module["Version"] in (root / "binary-build.txt").read_text(), "cached MinIO version differs"
assert module["Sum"] in (root / "binary-build.txt").read_text(), "cached MinIO checksum differs"
(root / "binary-sha256.txt").write_text(hashlib.sha256(binary.read_bytes()).hexdigest() + "\n")
PYVERIFY
fixture_dir="$(mktemp -d "${cache_dir}/run.XXXXXX")"
minio_pid=""
cleanup() {
  if [[ -n "${minio_pid}" ]]; then kill "${minio_pid}" >/dev/null 2>&1 || true; wait "${minio_pid}" 2>/dev/null || true; fi
  rm -rf -- "${fixture_dir}"
}
trap cleanup EXIT
port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
endpoint="127.0.0.1:${port}"
env -i PATH="${PATH}" MINIO_ROOT_USER="${access_key}" MINIO_ROOT_PASSWORD="${secret_key}" MINIO_BROWSER=off MINIO_UPDATE=off \
  "${minio}" --config-dir "${fixture_dir}/config" server --address "${endpoint}" "${fixture_dir}/data" > "${report_dir}/minio.log" 2>&1 &
minio_pid=$!
ready=false
for ((attempt = 0; attempt < 60; attempt++)); do
  if curl --fail --silent "http://${endpoint}/minio/health/ready" >/dev/null; then ready=true; break; fi
  kill -0 "${minio_pid}" || break
  sleep 0.5
done
if [[ "${ready}" != true ]]; then echo "MinIO readiness failed; inspect ${report_dir}/minio.log" >&2; exit 1; fi
# The fixture uses versioned object I/O, including delete markers on catalog
# HEAD. These are throwaway local credentials, never production configuration.
curl --fail --silent --show-error --aws-sigv4 'aws:amz:us-east-1:s3' --user "${access_key}:${secret_key}" \
  --request PUT "http://${endpoint}/${bucket}" > /dev/null
curl --fail --silent --show-error --aws-sigv4 'aws:amz:us-east-1:s3' --user "${access_key}:${secret_key}" \
  --request PUT --data '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>' \
  "http://${endpoint}/${bucket}?versioning" > /dev/null
export ANTFLY_LAKE_S3_INTEGRATION=1 OBJECTSTORE_S3_TEST_BUCKET="${bucket}"
export AWS_ENDPOINT_URL="http://${endpoint}" AWS_ACCESS_KEY_ID="${access_key}" AWS_SECRET_ACCESS_KEY="${secret_key}" AWS_REGION=us-east-1
export ZIG_LOCAL_CACHE_DIR="${repo_root}/zig/.zig-cache" ZIG_GLOBAL_CACHE_DIR="${repo_root}/zig/.zig-cache/global"
cd "${repo_root}/zig"
python3 tools/run_bounded_zig_build.py -- build lake-execution-abi-integration-test \
  -Doptimize=debug -Dstrip=false -Dcpu=baseline -Dshared-image-operations=true -Dshared-lake-execution=true \
  -Dmetal=false -Dcuda=false -Dsystem-blas=false --summary all -j1 2>&1 | tee "${report_dir}/test.log"
# An environment-gated skip must never be reported as cloud/transport success.
python3 - "${report_dir}" <<'PYREPORT'
import json, pathlib, platform, sys
root = pathlib.Path(sys.argv[1])
assert "LAKE_S3_QUALIFIED source=s3 artifacts=s3 catalog=managed pinned=7 head=8 missing_head=fenced incarnation=fenced retired=fenced" in (root / "test.log").read_text(), "missing successful real S3 completion marker"
(root / "result.json").write_text(json.dumps({"passed": True, "scope": "local MinIO S3-compatible object/catalog/pin reopen; not AWS/GCS or cloud retention qualification", "host": platform.platform(), "source_module": json.loads((root / "source-module.json").read_text()), "binary_sha256": (root / "binary-sha256.txt").read_text().strip(), "pinned_snapshot": 7, "later_head": 8, "missing_head_fenced": True, "wrong_incarnation_fenced": True, "retirement_fenced": True}, indent=2) + "\n")
PYREPORT
