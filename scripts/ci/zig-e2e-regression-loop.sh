#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
repeats="${ANTFLY_E2E_REGRESSION_REPEATS:-20}"

if [[ ! "$repeats" =~ ^[1-9][0-9]*$ ]]; then
  echo "ANTFLY_E2E_REGRESSION_REPEATS must be a positive integer" >&2
  exit 2
fi

if [[ "$#" -gt 0 ]]; then
  tests=("$@")
else
  tests=(
    e2e/antfly/test_schema_migration.py::test_schema_migration_full_text_rebuild
    e2e/antfly/test_scaling.py::test_autoscaling_finalizes_shard_split_from_size_threshold
  )
fi

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  (
    cd "$repo_root/zig"
    zig build -Dedition=full install -fincremental
  )
fi

cd "$repo_root/zig"
export ANTFLY_BIN="${ANTFLY_BIN:-./zig-out/bin/antfly}"
export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/antfly-pycache}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/antfly-ci-uv-cache}"
export ANTFLY_E2E_PHASE_TIMINGS="${ANTFLY_E2E_PHASE_TIMINGS:-1}"
export ANTFLY_E2E_PRESERVE_ROOT_ON_FAILURE="${ANTFLY_E2E_PRESERVE_ROOT_ON_FAILURE:-1}"
export ANTFLY_E2E_NATIVE_STACKS="${ANTFLY_E2E_NATIVE_STACKS:-1}"
export ANTFLY_LSM_OPEN_DEBUG="${ANTFLY_LSM_OPEN_DEBUG:-1}"
export PYTHONFAULTHANDLER="${PYTHONFAULTHANDLER:-1}"

failures=0
for ((iteration = 1; iteration <= repeats; iteration++)); do
  for test_name in "${tests[@]}"; do
    printf '\nE2E regression iteration %d/%d test=%s\n' "$iteration" "$repeats" "$test_name"
    if ! uv run --project e2e/antfly pytest -q -s --durations=10 "$test_name"; then
      failures=$((failures + 1))
    fi
  done
done

if ((failures > 0)); then
  printf '\nE2E regression loop recorded %d failed test runs\n' "$failures" >&2
  exit 1
fi
