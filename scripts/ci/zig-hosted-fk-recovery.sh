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
