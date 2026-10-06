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

output_dir="${1:-completions}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
zig_exe="${ANTFLY_ZIG:-zig}"

rm -rf "$output_dir"
mkdir -p "$output_dir"

# Use a host tool that imports the exact same command specification and
# renderer as `antfly completion`. Release builds may target another OS or CPU,
# so executing the just-built target binary here is not always possible.
for shell in bash zsh fish; do
  (
    cd "$repo_root/zig"
    "$zig_exe" run pkg/antfly/src/completion_generator.zig -- "$shell"
  ) >"$output_dir/antfly.$shell"
done
