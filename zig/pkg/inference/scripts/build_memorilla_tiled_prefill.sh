#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Rebuilds the research-only Memorilla dense prefill attention cubin
# (src/ops/cuda/memorilla_tiled_prefill.cu -> .sm121.cubin, GB10) with the
# pinned CUDA 13.2 toolkit. Loaded only under ANTFLY_MEMORILLA_RESEARCH_FAST_PREFILL=1.
set -euo pipefail
pkg_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
nvcc="${NVCC:-/usr/local/cuda-13.2/bin/nvcc}"
"$nvcc" -cubin -arch=sm_121 -O3 \
  -o "$pkg_dir/src/ops/cuda/memorilla_tiled_prefill.sm121.cubin" \
  "$pkg_dir/src/ops/cuda/memorilla_tiled_prefill.cu"
