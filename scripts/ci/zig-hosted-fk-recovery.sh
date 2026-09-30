#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
# Execute only prebuilt proof binaries: recovery runners never compile Zig.
set -euo pipefail
export ANTFLY_TEST_TIMINGS="${ANTFLY_TEST_TIMINGS:-1}"
case "${1:-}" in
  initial|self|truncate) suite="$1" ;;
  *) echo "usage: $0 initial|self|truncate" >&2; exit 2 ;;
esac
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
binary="$repo_root/zig/zig-out/bin/antfly-hosted-$suite-fk-recovery"
log_dir="${ANTFLY_FK_RECOVERY_LOG_DIR:-${RUNNER_TEMP:-/tmp}/antfly-fk-recovery}"
mkdir -p "$log_dir"
if [[ ! -x "$binary" ]]; then
  echo "missing prebuilt recovery artifact: $binary" >&2
  exit 1
fi
# Run from zig so std.testing temporary databases stay on the runner's disk.
# TERM ends the process group; KILL bounds a stuck native shutdown as well.
cd "$repo_root/zig"
timeout --signal=TERM --kill-after=30s 15m "$binary" 2>&1 | tee "$log_dir/$suite.log"
# Local test runners permit unavailable-listener skips. A required mounted
# recovery lane must prove execution, not go green on an environment skip.
if ! grep -Eq '^[1-9][0-9]* passed; 0 skipped; 0 failed; 0 leaked\.$' "$log_dir/$suite.log"; then
  echo "missing complete, non-skipped hosted FK proof summary" >&2
  exit 1
fi
