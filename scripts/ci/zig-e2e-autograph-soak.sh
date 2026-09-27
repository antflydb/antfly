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

# Exercise the production cross-shard resolver/promoter under normal and
# constrained descriptor budgets. Every run uses a fresh six-process cluster.
# The regression driver preserves failed roots and rejects missing/skipped cases.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
: "${ANTFLY_E2E_REGRESSION_REPORT_DIR:?Set a fresh report directory for the soak}"
report_root="$ANTFLY_E2E_REGRESSION_REPORT_DIR"
export ANTFLY_E2E_REGRESSION_WORKERS="${ANTFLY_E2E_REGRESSION_WORKERS:-2}"
export ANTFLY_E2E_REGRESSION_REPEATS="${ANTFLY_E2E_REGRESSION_REPEATS:-25}"
export ANTFLY_E2E_NATIVE_STACKS=1
tests=(
  e2e/antfly/test_resolution.py::test_multinode_autograph_resolves_promotes_and_hydrates_entities
  e2e/antfly/test_resolution.py::test_multinode_autograph_recovers_after_data_restart
)
result=0
for profile in normal constrained; do
  nofile_limit=""
  if [[ "$profile" == constrained ]]; then nofile_limit=256; fi
  if ANTFLY_E2E_NOFILE_LIMIT="$nofile_limit" \
    ANTFLY_E2E_REGRESSION_REPORT_DIR="$report_root/$profile" \
    "$script_dir/zig-e2e-regression-loop.sh" "${tests[@]}"; then
    status=0
  else
    status=$?
  fi
  if ((status == 130 || status == 143)); then exit "$status"; fi
  if ((status != 0)); then
    result=1
  fi
  # Build once when invoked locally without a prebuilt executable.
  export SKIP_BUILD=1
done
exit "$result"
