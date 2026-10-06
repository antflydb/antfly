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

# Native query contention and session recovery complement deterministic regressions.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
: "${ANTFLY_E2E_REGRESSION_REPORT_DIR:?Set a fresh report directory for the soak}"
report_root="$ANTFLY_E2E_REGRESSION_REPORT_DIR"
export ANTFLY_E2E_REGRESSION_WORKERS="${ANTFLY_E2E_REGRESSION_WORKERS:-2}"
export ANTFLY_E2E_REGRESSION_REPEATS="${ANTFLY_E2E_REGRESSION_REPEATS:-25}"
export ANTFLY_E2E_NATIVE_STACKS=1
result=0
for profile in normal constrained; do
  nofile_limit=""
  if [[ "$profile" == constrained ]]; then nofile_limit=256; fi
  if ANTFLY_E2E_NOFILE_LIMIT="$nofile_limit" \
    ANTFLY_E2E_REGRESSION_REPORT_DIR="$report_root/$profile" \
    "$script_dir/zig-e2e-regression-loop.sh" \
      e2e/antfly/test_aggregations.py::test_aggregations_remain_exact_during_concurrent_inserts \
      e2e/antfly/test_transactions.py::test_session_transform_savepoint_rollback; then
    status=0
  else
    status=$?
  fi
  if ((status == 130 || status == 143)); then exit "$status"; fi
  if ((status != 0)); then result=1; fi
  export SKIP_BUILD=1
done
exit "$result"
