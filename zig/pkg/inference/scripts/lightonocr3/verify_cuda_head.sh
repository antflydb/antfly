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
output_dir="${ANTFLY_LIGHTON_HEAD_CHECK_DIR:-$repo_root/.cache/lighton-head-repro}"
case "$output_dir/" in
  "$repo_root/"*) ;;
  *) echo "qualification output must be inside the worktree" >&2; exit 2 ;;
esac
"${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/make_cuda_head.py" \
  --output-dir "$output_dir" --ptxas "${PTXAS:-/usr/local/cuda/bin/ptxas}"
for image in lighton_head_08.ptx lighton_head_4b.ptx lighton_head_finish.ptx; do
  cmp "$pkg_root/src/ops/cuda/$image" "$output_dir/$image"
done
"${PYTHON:-python3}" "$pkg_root/scripts/lightonocr3/check_cuda_head.py" --artifact-dir "$output_dir"
# Native ownership, tie, model-isolation, capture and training checks are in
# `LightOnOCR3 CUDA fused dense head`. Full-model benchmarks additionally
# require positive timed fused-head counters and unchanged serial references.
