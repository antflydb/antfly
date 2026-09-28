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

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
# Lightweight checks can use four workers while the scheduler separately caps
# concurrent Antfly processes and clusters.
default_workers=4
detected_workers="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
if [[ "$detected_workers" =~ ^[1-9][0-9]*$ ]] && (( detected_workers < default_workers )); then
  default_workers="$detected_workers"
fi
workers="${ANTFLY_E2E_WORKERS:-$default_workers}"
process_slots="${ANTFLY_E2E_PROCESS_SLOTS:-2}"

# Approved PR CI runs the workflow from main while checking out the PR code.
# Its legacy one-slot setting meant one cluster workload per runner, but a
# mixed stateful/serverless test requires two actual processes on one worker.
# Preserve that isolation until main picks up the two-slot workflow setting.
if [[ "${GITHUB_ACTIONS:-}" == "true" && "${ANTFLY_E2E_SUITE:-}" == antfly* && -n "${ANTFLY_E2E_SHARD:-}" && "${ANTFLY_E2E_PROCESS_SLOTS:-}" == "1" && -z "${ANTFLY_E2E_PROCESS_WORKERS+x}" ]]; then
  process_slots=2
  export ANTFLY_E2E_PROCESS_WORKERS=1
fi

if [[ ! "$workers" =~ ^(0|[1-9][0-9]*)$ ]]; then
  echo "ANTFLY_E2E_WORKERS must be a non-negative integer; got: $workers" >&2
  exit 2
fi
if [[ ! "$process_slots" =~ ^[1-9][0-9]*$ ]]; then
  echo "ANTFLY_E2E_PROCESS_SLOTS must be a positive integer; got: $process_slots" >&2
  exit 2
fi

report_args=()
if [[ -n "${ANTFLY_E2E_REPORT_DIR:-}" ]]; then
  mkdir -p "$ANTFLY_E2E_REPORT_DIR"
  report_args+=("--junitxml=$ANTFLY_E2E_REPORT_DIR/junit.xml")
fi
cd "$repo_root/zig"
if (( workers > 1 )); then
  # Isolation groups preserve shared fixture lifecycles; independent tests are
  # scheduled longest-first without exceeding the Antfly process budget.
  # Keep test identities visible even if the job is cancelled before pytest's
  # final summary; quiet progress dots hide the failing or stalled scenario.
  exec uv run --project e2e/antfly pytest -v --tb=short --continue-on-collection-errors "${report_args[@]}" \
    -n "$workers" --dist=loadgroup --e2e-process-slots "$process_slots" "$@"
fi

exec uv run --project e2e/antfly pytest -v --tb=short --continue-on-collection-errors "${report_args[@]}" "$@"
