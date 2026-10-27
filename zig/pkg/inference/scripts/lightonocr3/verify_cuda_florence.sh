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
pkg_root="$(cd "$(dirname "$0")/../.." && pwd)"
repo_root="$(cd "$pkg_root/../../.." && pwd)"
output_dir="${ANTFLY_FLORENCE_CUDA_CHECK_DIR:-$repo_root/.cache/florence-cuda-fused}"
case "$output_dir/" in
  "$repo_root/"*) ;;
  *) echo "qualification output must be inside the worktree" >&2; exit 2 ;;
esac
nvcc="${NVCC:-/usr/local/cuda/bin/nvcc}"
case "$("$nvcc" --version)" in
  *"release 13.0"*) ;;
  *) echo "Use CUDA 13.0 to reproduce the driver-13.0-compatible PTX image" >&2; exit 2 ;;
esac
mkdir -p "$output_dir"
"$nvcc" -ptx -arch=compute_80 -O3 \
  "$pkg_root/src/ops/cuda/prototypes/florence_fused.cu" \
  -o "$output_dir/florence_fused.ptx"
"${PYTHON:-python3}" - "$output_dir/florence_fused.ptx" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1])
p.write_bytes(p.read_bytes().rstrip(b"\n") + b"\n")
PY
cmp "$pkg_root/src/ops/cuda/florence_fused.ptx" "$output_dir/florence_fused.ptx"
# Numeric, ownership and model-isolation checks are in the focused native
# `LightOnOCR3 CUDA` suite. An already built binary can be checked here too.
if [[ -n "${ANTFLY_FLORENCE_CUDA_TEST_BINARY:-}" ]]; then
  ANTFLY_TEST_REQUIRE_LIGHTON_CUDNN=1 "$ANTFLY_FLORENCE_CUDA_TEST_BINARY"
fi
