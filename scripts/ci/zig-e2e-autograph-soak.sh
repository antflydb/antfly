#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

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
