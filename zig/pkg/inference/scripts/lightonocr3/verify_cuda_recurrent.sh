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
output_dir="${ANTFLY_LIGHTON_CUDA_CHECK_DIR:-$repo_root/.cache/lighton-cuda-recurrent}"
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
"$nvcc" -ptx -arch=compute_75 -O3 --fmad=false -std=c++17 \
  "$pkg_root/src/ops/cuda/prototypes/lighton_qwen35_recurrent.cu" \
  -o "$output_dir/lighton_recurrent.ptx"
"${PYTHON:-python3}" - "$output_dir/lighton_recurrent.ptx" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1])
p.write_bytes(p.read_bytes().rstrip(b"\n") + b"\n")
PY
cmp "$pkg_root/src/ops/cuda/lighton_recurrent.ptx" "$output_dir/lighton_recurrent.ptx"
"${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_recurrent.py" \
  --artifact "$output_dir/lighton_recurrent.ptx" \
  --output "$output_dir/qualification.json"

# The wide check is additional and requires PyTorch; the CPU oracle above is
# always dependency-free. Both use the exact reproduced PTX image.
if [[ "${ANTFLY_LIGHTON_CUDA_CHECK_BATCH:-0}" == 1 ]]; then
  "${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_recurrent_batch.py" \
    --artifact "$output_dir/lighton_recurrent.ptx" \
    --output "$output_dir/batch-qualification.json"
  "${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_recurrent_batch.py" \
    --artifact "$output_dir/lighton_recurrent.ptx" --fused \
    --output "$output_dir/fused-batch-qualification.json"
fi

if [[ "${ANTFLY_LIGHTON_CUDA_CHECK_CACHED_GATES:-0}" == 1 ]]; then
  "${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_recurrent.py" \
    --artifact "$output_dir/lighton_recurrent.ptx" --cached-gates \
    --output "$output_dir/cached-gates-qualification.json"
  "${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_recurrent_batch.py" \
    --artifact "$output_dir/lighton_recurrent.ptx" --fused --cached-gates \
    --output "$output_dir/cached-gates-batch-qualification.json"
fi

if [[ "${ANTFLY_LIGHTON_CUDA_CHECK_PARALLEL_PREFIX:-0}" == 1 ]]; then
  "${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_recurrent.py" \
    --artifact "$output_dir/lighton_recurrent.ptx" --cached-gates --parallel-prefix \
    --output "$output_dir/parallel-prefix-qualification.json"
fi

if [[ "${ANTFLY_LIGHTON_CUDA_CHECK_SHARED_QK:-0}" == 1 ]]; then
  "${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_prefill_batch.py" \
    --artifact "$output_dir/lighton_recurrent.ptx" --shared-qk \
    --output "$output_dir/shared-qk-bitwise-qualification.json"
fi
"${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_recurrent_ragged.py" \
  --artifact "$output_dir/lighton_recurrent.ptx" --output "$output_dir/ragged_qualification.json"

"${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_recurrent_ragged.py" \
  --artifact "$output_dir/lighton_recurrent.ptx" --fused-decode \
  --output "$output_dir/fused_decode_bitwise_qualification.json"
