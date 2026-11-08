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
app="$1"
identity="${SIGNING_IDENTITY:--}"
args=(--force --sign "$identity")
if [[ "$identity" != '-' ]]; then args+=(--options runtime --timestamp); fi
runtime="$app/Contents/Resources/Runtime"
if [[ -d "$runtime" ]]; then
  codesign "${args[@]}" "$runtime/lib/libantfly-apple.dylib"
  codesign "${args[@]}" "$runtime/antfly"
fi
framework="$app/Contents/Frameworks/Sparkle.framework"
# Sign from the inside out. --deep is for verification, never for signing.
codesign "${args[@]}" "$framework/Versions/B/XPCServices/Installer.xpc"
codesign "${args[@]}" --preserve-metadata=entitlements "$framework/Versions/B/XPCServices/Downloader.xpc"
codesign "${args[@]}" "$framework/Versions/B/Autoupdate"
codesign "${args[@]}" "$framework/Versions/B/Updater.app"
codesign "${args[@]}" "$framework"
codesign "${args[@]}" --entitlements "$(dirname "$0")/app.entitlements" "$app"
codesign --verify --deep --strict "$app"
