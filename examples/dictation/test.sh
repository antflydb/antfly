#!/bin/bash
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
cd "$(dirname "$0")"
mkdir -p .build/clang .build/cache .build/modules
export CLANG_MODULE_CACHE_PATH="$PWD/.build/clang"
args=(--disable-xctest --cache-path "$PWD/.build/cache" -Xswiftc -module-cache-path -Xswiftc "$PWD/.build/modules")
# Swift 6.4's CLI swiftbuild engine can omit Testing macro discovery.
toolchain="$(xcrun --find swift)"
plugin="$(dirname "$toolchain")/../lib/swift/host/plugins/testing/libTestingMacros.dylib"
if [[ -f "$plugin" ]]; then
  args+=(-Xswiftc -load-plugin-library -Xswiftc "$plugin")
fi
swift test "${args[@]}"
