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
#
# Serial regression loop:
#   ANTFLY_E2E_REGRESSION_REPEATS=20 scripts/ci/zig-e2e-regression-loop.sh
#
# Mixed-load lifecycle race soak:
#   ANTFLY_E2E_REGRESSION_WORKERS=3 ANTFLY_E2E_REGRESSION_REPEATS=20 \
#     scripts/ci/zig-e2e-regression-loop.sh

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
repeats="${ANTFLY_E2E_REGRESSION_REPEATS:-20}"
workers="${ANTFLY_E2E_REGRESSION_WORKERS:-1}"
preserve_failure_limit="${ANTFLY_E2E_PRESERVE_FAILURE_LIMIT:-1}"

if [[ ! "$repeats" =~ ^[1-9][0-9]*$ ]]; then
  echo "ANTFLY_E2E_REGRESSION_REPEATS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$workers" =~ ^[1-9][0-9]*$ ]]; then
  echo "ANTFLY_E2E_REGRESSION_WORKERS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$preserve_failure_limit" =~ ^[0-9]+$ ]]; then
  echo "ANTFLY_E2E_PRESERVE_FAILURE_LIMIT must be a non-negative integer" >&2
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

if ((workers > 1)); then
  log_root="$(mktemp -d "${TMPDIR:-/tmp}/antfly-e2e-regression.XXXXXX")"
  preserve_log_root=0
  pids=()

  # shellcheck disable=SC2329 # Invoked indirectly by the signal traps below.
  terminate_workers() {
    local pid
    for pid in "${pids[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    exit 130
  }
  cleanup_worker_logs() {
    if ((preserve_log_root == 0)); then
      rm -rf -- "$log_root"
    else
      printf '\nPreserving E2E regression worker logs: %s\n' "$log_root" >&2
    fi
  }
  trap terminate_workers INT TERM
  trap cleanup_worker_logs EXIT

  for ((worker = 1; worker <= workers; worker++)); do
    env \
      SKIP_BUILD=1 \
      ANTFLY_E2E_REGRESSION_WORKERS=1 \
      ANTFLY_E2E_REGRESSION_WORKER_ID="$worker" \
      UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/antfly-ci-uv-cache}-worker-${worker}" \
      PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/antfly-pycache}-worker-${worker}" \
      "$script_dir/zig-e2e-regression-loop.sh" "${tests[@]}" \
      >"$log_root/worker-${worker}.log" 2>&1 &
    pids+=("$!")
  done

  result=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      result=1
    fi
  done
  trap - INT TERM

  for ((worker = 1; worker <= workers; worker++)); do
    printf '\n===== E2E regression worker %d/%d =====\n' "$worker" "$workers"
    cat "$log_root/worker-${worker}.log"
  done
  if ((result != 0)); then
    preserve_log_root=1
  fi
  exit "$result"
fi

cd "$repo_root/zig"
export ANTFLY_BIN="${ANTFLY_BIN:-./zig-out/bin/antfly}"
export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/antfly-pycache}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/antfly-ci-uv-cache}"
export ANTFLY_E2E_PHASE_TIMINGS="${ANTFLY_E2E_PHASE_TIMINGS:-1}"
export ANTFLY_E2E_NATIVE_STACKS="${ANTFLY_E2E_NATIVE_STACKS:-1}"
export ANTFLY_LSM_OPEN_DEBUG="${ANTFLY_LSM_OPEN_DEBUG:-1}"
export PYTHONFAULTHANDLER="${PYTHONFAULTHANDLER:-1}"

failures=0
preserved_failures=0
worker_id="${ANTFLY_E2E_REGRESSION_WORKER_ID:-1}"
for ((iteration = 1; iteration <= repeats; iteration++)); do
  for test_name in "${tests[@]}"; do
    printf '\nE2E regression worker=%s iteration=%d/%d test=%s\n' \
      "$worker_id" "$iteration" "$repeats" "$test_name"
    preserve_root=0
    if ((preserved_failures < preserve_failure_limit)); then
      preserve_root=1
    fi
    if ANTFLY_E2E_PRESERVE_ROOT_ON_FAILURE="$preserve_root" \
      uv run --project e2e/antfly pytest -q -s --durations=10 "$test_name"; then
      status=0
    else
      status=$?
    fi
    if ((status == 130 || status == 143)); then
      printf '\nE2E regression loop interrupted with exit code %d\n' "$status" >&2
      exit "$status"
    fi
    if ((status != 0)); then
      failures=$((failures + 1))
      if ((preserve_root == 1)); then
        preserved_failures=$((preserved_failures + 1))
      fi
    fi
  done
done

if ((failures > 0)); then
  printf '\nE2E regression loop recorded %d failed test runs (%d roots preserved)\n' \
    "$failures" "$preserved_failures" >&2
  exit 1
fi
