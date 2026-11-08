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
root="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$root"
export ZIG_GLOBAL_CACHE_DIR="$root/zig/.zig-cache/global"
sdk_version="$(xcrun --sdk macosx --show-sdk-version)"
if (( ${sdk_version%%.*} < 27 )); then
  echo 'Bundled Apple providers require a macOS 27 SDK / Swift 6 toolchain.' >&2
  exit 1
fi
stage="$root/examples/dictation/.build/runtime"
rm -rf "$stage/universal"
mkdir -p "$stage/slices" "$stage/universal/lib"
# App and engine are one compatibility unit, pinned to this source commit.
# Keep one Zig prefix and reuse its worktree-local cache across architectures.
for arch in arm64 x86_64; do
  target=aarch64-macos.14.0
  if [[ "$arch" == x86_64 ]]; then target=x86_64-macos.14.0; fi
  (cd zig && python3 tools/run_bounded_zig_build.py --zig zig --report "$stage/progress-$arch.json" -- build --summary all antfly apple-native-bridge -Dtarget="$target" -Dcpu=baseline \
    -Doptimize=fast -Dstrip=true -Dapple-providers=true -Dmetal=true -Dsystem-blas=true \
    --prefix "$root/zig/zig-out" --cache-dir "$root/zig/.zig-cache")
  mkdir -p "$stage/slices/$arch"
  cp zig/zig-out/bin/antfly "$stage/slices/$arch/antfly"
  cp zig/zig-out/lib/libantfly-apple.dylib "$stage/slices/$arch/libantfly-apple.dylib"
done
lipo -create "$stage/slices/arm64/antfly" "$stage/slices/x86_64/antfly" -output "$stage/universal/antfly"
lipo -create "$stage/slices/arm64/libantfly-apple.dylib" "$stage/slices/x86_64/libantfly-apple.dylib" -output "$stage/universal/lib/libantfly-apple.dylib"
cp -R zig/zig-out/share/licenses "$stage/universal/"
python3 - "$stage/universal/source.json" <<'PY'
import json, subprocess, sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps({"schema_version": 1, "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()}) + "\n")
PY
chmod +x "$stage/universal/antfly"
# Execute only in the credential-free source build, never in the signing job.
env -i HOME="$HOME" PATH=/usr/bin:/bin:/usr/sbin:/sbin "$stage/universal/antfly" --version
