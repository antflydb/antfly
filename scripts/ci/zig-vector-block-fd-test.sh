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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root/zig"

# Build before lowering the limit: only isolated test children are constrained.
# All output remains in this checkout's ordinary .zig-cache and zig-out.
python3 tools/run_bounded_zig_build.py --zig "${ZIG:-zig}" -- build \
  install-vector-block-fd-tests -Doptimize=debug -Dstrip=true \
  -Dmetal=false -Dsystem-blas=false "$@"
for limit in 256 1024; do
  (
    ulimit -Sn "$limit"
    echo "Qualifying vector descriptor ownership at RLIMIT_NOFILE=$limit"
    ./zig-out/bin/vector-block-fd-tests
  )
done
