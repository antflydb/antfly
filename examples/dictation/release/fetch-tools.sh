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
cd "$(dirname "$0")/.."
version="$(python3 -c 'import json; print(json.load(open("release/policy.json"))["sparkle_version"])')"
expected="$(python3 -c 'import json; print(json.load(open("release/policy.json"))["sparkle_tools_sha256"])')"
mkdir -p .build/updater-tools
archive="$PWD/.build/updater-tools/Sparkle.tar.xz"
if [[ ! -f "$archive" ]] || [[ "$(shasum -a 256 "$archive" | cut -d ' ' -f 1)" != "$expected" ]]; then
  curl --fail --location --retry 3 --output "$archive" "https://github.com/sparkle-project/Sparkle/releases/download/$version/Sparkle-$version.tar.xz"
fi
[[ "$(shasum -a 256 "$archive" | cut -d ' ' -f 1)" == "$expected" ]]
tar -xJf "$archive" -C .build/updater-tools
