#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
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

repo_root=$(CDPATH='' cd "$(dirname "$0")/../.." && pwd -P)
policy="$repo_root/scripts/ci/check_sdk_policy.py"
policy_python=${ANTFLY_POLICY_PYTHON:-python3}

section() {
  echo
  echo "==> $*"
}

check_generated_status() {
  local status
  status=$(git status --porcelain --untracked-files=all -- "$@")
  if [[ -n $status ]]; then
    echo "$status" >&2
    git diff -- "$@" >&2
    return 1
  fi
}

check_sdk() {
  if [[ -z ${ANTFLY_POLICY_PYTHON:-} ]]; then
    local build_python
    build_python=$(python3 "$policy" --get python-build)
    policy_python=$(uv python find "$build_python")
  fi

  section "Checking the SDK support policy"
  "$policy_python" "$policy"

  section "Checking the joined public OpenAPI contract"
  uv run --project "$repo_root/scripts" --locked python \
    "$repo_root/scripts/join_public_openapi.py" --compare openapi.yaml

  section "Checking shared generated identifier policy"
  uv run --project "$repo_root/scripts" --locked python \
    "$repo_root/scripts/generate_graph_identifier_policy.py" --check

  section "Checking the Python SDK"
  (
    cd "$repo_root/py/packages/sdk"
    uv run --locked python generate_client.py --check
    uv run --locked ruff check src tests
    uv run --locked pyright src tests
  )

  local python_versions
  python_versions=$("$policy_python" "$policy" --get python-supported)
  local version
  for version in $python_versions; do
    section "Testing the Python SDK on Python $version"
    (
      cd "$repo_root/py/packages/sdk"
      uv run --isolated --locked --python "$version" pytest tests
    )
    "$(uv python find "$version")" -m compileall -q \
      "$repo_root/py/packages/cli/src/antfly_cli"
  done

  section "Building the Python SDK distributions"
  (cd "$repo_root/py/packages/sdk" && uv build)

  section "Checking the TypeScript SDK"
  (
    cd "$repo_root/ts"
    node scripts/run-pinned-toolchain.mjs pnpm --filter @antfly/sdk generate
    check_generated_status \
      packages/sdk/src/public-api.d.ts \
      packages/sdk/src/query.d.ts
    node scripts/run-pinned-toolchain.mjs pnpm --filter @antfly/sdk exec biome check .
    node scripts/run-pinned-toolchain.mjs pnpm --filter @antfly/sdk typecheck
    node scripts/run-pinned-toolchain.mjs pnpm --filter @antfly/sdk build
    node scripts/run-pinned-toolchain.mjs pnpm --filter @antfly/sdk test
  )

  section "Checking the Go SDK"
  (
    cd "$repo_root/go/pkg/sdk"
    GOWORK=off go generate ./...
    check_generated_status \
      chunking/openapi.gen.go \
      oapi/client.gen.go \
      admin/oapi/client.gen.go \
      oapi/validate.go \
      query/query.gen.go
    GOWORK=off go mod tidy
    git diff --exit-code -- go.mod go.sum
    GOWORK=off go vet ./...
    CGO_ENABLED=0 GOWORK=off go test -count=1 ./...
    CGO_ENABLED=1 GOWORK=off go test -race -count=1 ./...
  )

  section "Checking the Rust SDK"
  cargo fmt --manifest-path "$repo_root/rs/Cargo.toml" --all --check
  cargo test --locked --manifest-path "$repo_root/rs/Cargo.toml" --package antfly-sdk
}

check_memoryaf() {
  section "Checking memoryaf"
  (
    cd "$repo_root/go/pkg/memoryaf"
    GOWORK=off go mod tidy
    git diff --exit-code -- go.mod go.sum
    GOWORK=off go vet ./...
    CGO_ENABLED=0 GOWORK=off go test -count=1 ./...
  )
}

check_release() {
  section "Checking release packaging and scripts"
  "$repo_root/scripts/release/test.sh"
}

usage() {
  echo "usage: $0 {format|sdk|memoryaf|release|all} [format language ...]" >&2
  exit 2
}

command=${1:-}
shift || true
case "$command" in
  format)
    "$repo_root/scripts/format.sh" --check "$@"
    ;;
  sdk)
    check_sdk
    ;;
  memoryaf)
    check_memoryaf
    ;;
  release)
    check_release
    ;;
  all)
    "$repo_root/scripts/format.sh" --check
    check_sdk
    check_memoryaf
    check_release
    ;;
  *)
    usage
    ;;
esac
