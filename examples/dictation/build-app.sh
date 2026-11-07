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
swift build -c release --cache-path "$PWD/.build/cache" -Xswiftc -module-cache-path -Xswiftc "$PWD/.build/modules"
app="${APP_OUTPUT_DIR:-$PWD/dist}/Antfly Dictation.app"
mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources"
# SwiftPM's native and Xcode build engines have different product layouts.
bin_dir="$(swift build -c release --show-bin-path --cache-path "$PWD/.build/cache")"
# Replace the executable inode, so rebuilding cannot invalidate pages mapped by
# an older running app while the user is still using it.
cp "$bin_dir/AntflyDictation" "$app/Contents/MacOS/.AntflyDictation.next"
mv -f "$app/Contents/MacOS/.AntflyDictation.next" "$app/Contents/MacOS/AntflyDictation"
cp Info.plist "$app/Contents/Info.plist"
cp Assets/AppIcon.icns "$app/Contents/Resources/AppIcon.icns"
codesign --force --sign "${SIGNING_IDENTITY:--}" "$app"
printf 'Built %s\n' "$app"
