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

# Return success when a native build is active OR activity cannot be inspected.
# Cache mtimes alone miss a long LLVM pass reusing an old output directory.
native_builds_active() {
    local snapshot command
    snapshot="$(ps -axo comm= 2>/dev/null)" || return 0
    while IFS= read -r command; do
        if [[ "$command" =~ (^|/|[[:space:]])(zig|maker|cargo|rustc|clippy-driver|rust-lld)$ ]]; then
            return 0
        fi
    done <<< "$snapshot"
    return 1
}
