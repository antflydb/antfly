#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

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
