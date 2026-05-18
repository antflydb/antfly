#!/bin/sh
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

set -eu

if [ "${1-}" = "--help" ]; then
  cat <<'EOF'
Usage: tools/measure_runtime_memory.sh [metadata-pid [data-pid]]

If no PIDs are provided, the script auto-detects the newest
`zig-out/bin/antfly metadata` and `zig-out/bin/antfly data` processes.

This helper is macOS-specific and uses `vmmap -summary`.
EOF
  exit 0
fi

find_pid() {
  pattern="$1"
  pgrep -fn "$pattern"
}

metadata_pid="${1-}"
data_pid="${2-}"

if [ -z "$metadata_pid" ]; then
  metadata_pid="$(find_pid 'zig-out/bin/antfly metadata')"
fi
if [ -z "$data_pid" ]; then
  data_pid="$(find_pid 'zig-out/bin/antfly data')"
fi

report_pid() {
  label="$1"
  pid="$2"
  if [ -z "$pid" ]; then
    echo "$label: not found"
    return 1
  fi
  echo "== $label (pid $pid) =="
  vmmap -summary "$pid" | egrep 'Physical footprint|VM_ALLOCATE|STACK'
  echo
}

report_pid metadata "$metadata_pid"
report_pid data "$data_pid"
