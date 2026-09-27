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

# Exercise three-shard backup/delete/restore through a three-node metadata
# cluster and three data replicas, under normal and constrained FD budgets.
# The regression driver preserves failed roots and rejects missing/skipped cases.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
: "${ANTFLY_E2E_REGRESSION_REPORT_DIR:?Set a fresh report directory for the soak}"
report_root="$ANTFLY_E2E_REGRESSION_REPORT_DIR"
export ANTFLY_E2E_REGRESSION_WORKERS="${ANTFLY_E2E_REGRESSION_WORKERS:-2}"
export ANTFLY_E2E_REGRESSION_REPEATS="${ANTFLY_E2E_REGRESSION_REPEATS:-25}"
export ANTFLY_E2E_NATIVE_STACKS=1
tests=(
  e2e/antfly/test_backup_restore.py::test_three_by_three_cluster_backup_restore_through_metadata_public_api
  e2e/antfly/test_metadata_mutation_discovery.py::test_backup_restore_discovers_leader_past_stalled_status
)
profiles=(normal constrained)
if [[ -n "${ANTFLY_E2E_REGRESSION_PROFILE:-}" ]]; then
  case "$ANTFLY_E2E_REGRESSION_PROFILE" in
    normal|constrained) profiles=("$ANTFLY_E2E_REGRESSION_PROFILE") ;;
    *) echo "invalid cluster restore profile: $ANTFLY_E2E_REGRESSION_PROFILE" >&2; exit 2 ;;
  esac
fi
result=0
for profile in "${profiles[@]}"; do
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
