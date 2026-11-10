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
args=(--arch arm64 -c release --cache-path "$PWD/.build/cache" -Xswiftc -module-cache-path -Xswiftc "$PWD/.build/modules")
swift build "${args[@]}"
app="${APP_OUTPUT_DIR:-$PWD/dist}/Antfly Dictation.app"
mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources" "$app/Contents/Frameworks"
bin_dir="$(swift build "${args[@]}" --show-bin-path)"
cp "$bin_dir/AntflyDictation" "$app/Contents/MacOS/.AntflyDictation.next"
mv -f "$app/Contents/MacOS/.AntflyDictation.next" "$app/Contents/MacOS/AntflyDictation"
cp Info.plist "$app/Contents/Info.plist"
metadata=("$app/Contents/Info.plist")
if [[ -n "${RELEASE_TAG:-}" ]]; then
  metadata+=(--tag "$RELEASE_TAG" --key "${SPARKLE_PUBLIC_KEY:?Release builds require SPARKLE_PUBLIC_KEY}")
fi
python3 release/release.py stamp "${metadata[@]}"
cp Assets/AppIcon.icns "$app/Contents/Resources/AppIcon.icns"
framework="$PWD/.build/artifacts/sparkle/Sparkle/Sparkle.xcframework/macos-arm64_x86_64/Sparkle.framework"
# ditto preserves the versioned framework's symlinks and nested helper bundles.
rm -rf "$app/Contents/Frameworks/Sparkle.framework"
ditto "$framework" "$app/Contents/Frameworks/Sparkle.framework"
cp .build/checkouts/Sparkle/LICENSE "$app/Contents/Resources/.Sparkle-LICENSE.next"
chmod 644 "$app/Contents/Resources/.Sparkle-LICENSE.next"
mv -f "$app/Contents/Resources/.Sparkle-LICENSE.next" "$app/Contents/Resources/Sparkle-LICENSE.txt"
if [[ "${BUNDLE_RUNTIME:-${RELEASE_TAG:+1}}" == 1 ]]; then
  python3 release/fetch-runtime.py
  rm -rf "$app/Contents/Resources/Runtime"
  ditto .build/runtime/released "$app/Contents/Resources/Runtime"
else
  rm -rf "$app/Contents/Resources/Runtime"
fi
bash release/sign-app.sh "$app"
printf 'Built %s\n' "$app"
