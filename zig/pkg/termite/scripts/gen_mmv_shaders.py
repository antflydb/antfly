#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Generate MMV (matrix-vector) WGSL kernels from the existing GEMM shaders.

For decode-time inference qLen=1 (single-token batches), the tiled 16x16 GEMM
shaders waste 15/16 of every workgroup. ggml's Metal/CUDA backends ship a
separate qLen=1 path that places one workgroup per output column and
cooperatively reduces over K. This script produces the same shape for every
GGUF quant kernel by reusing the existing dequant function from the GEMM
shader and wrapping it in MMV scaffolding.

Behaviour:
  * Reads each `web/shaders/matmul_transb_<fmt>.wgsl` (skipping any *_mmv).
  * Strips the GEMM-specific TILE_M/N/K constants and tile_a/tile_b workgroup
    declarations from the prologue.
  * Truncates at `@compute` so all helpers, grids and dequant functions stay.
  * Appends MMV scaffolding that calls the same dequant function via either
    the (b_row, k_abs) or (b_row, block_idx, in_block) calling convention,
    whichever the GEMM shader uses.
  * Writes `web/shaders/matmul_transb_<fmt>_mmv.wgsl`.

Run from pkg/termite root:  python3 scripts/gen_mmv_shaders.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SHADERS_DIR = ROOT / "web" / "shaders"

# Workgroup size for MMV. 128 keeps shared memory tiny (512 B) and stays well
# under WebGPU's 256-thread minimum-required-limit.
WORKGROUP_SIZE = 128

# Per-format dequant calling convention. Most kernels use (b_row, k_abs);
# the older block-style ones split that into (b_row, block_idx, in_block) and
# need a small wrapper. Keys are filename stems (without `matmul_transb_`).
DEQUANT_SPECS: dict[str, dict] = {
    # Single-arg dequant(b_row, k_abs).
    "q5_0":    {"fn": "dequant_q5_0",    "style": "kabs"},
    "q5_1":    {"fn": "dequant_q5_1",    "style": "kabs"},
    "q8_1":    {"fn": "dequant_q8_1",    "style": "kabs"},
    "q2_k":    {"fn": "dequant_q2_k",    "style": "kabs"},
    "q3_k":    {"fn": "dequant_q3_k",    "style": "kabs"},
    "q4_k":    {"fn": "dequant_q4_k",    "style": "kabs"},
    "q5_k":    {"fn": "dequant_q5_k",    "style": "kabs"},
    "q6_k":    {"fn": "dequant_q6_k",    "style": "kabs"},
    "q8_k":    {"fn": "dequant_q8_k",    "style": "kabs"},
    "iq4_xs":  {"fn": "dequant_iq4_xs",  "style": "kabs"},
    "i8_s":    {"fn": "dequant_i8_s",    "style": "kabs"},
    "q1_0":    {"fn": "dequant_q1_0",    "style": "kabs"},
    "tq1_0":   {"fn": "dequant_tq1_0",   "style": "kabs"},
    "tq2_0":   {"fn": "dequant_tq2_0",   "style": "kabs"},
    "mxfp4":   {"fn": "dequant_mxfp4",   "style": "kabs"},
    "nvfp4":   {"fn": "dequant_nvfp4",   "style": "kabs"},
    "iq1_s":   {"fn": "dequant_iq1_s",   "style": "kabs"},
    "iq1_m":   {"fn": "dequant_iq1_m",   "style": "kabs"},
    "iq2_xxs": {"fn": "dequant_iq2_xxs", "style": "kabs"},
    "iq2_xs":  {"fn": "dequant_iq2_xs",  "style": "kabs"},
    "iq2_s":   {"fn": "dequant_iq2_s",   "style": "kabs"},
    "iq3_xxs": {"fn": "dequant_iq3_xxs", "style": "kabs"},
    "iq3_s":   {"fn": "dequant_iq3_s",   "style": "kabs"},
    # Block-split dequant(b_row, block_idx, in_block) with block_size values
    # per quantization block.
    "q4_0":    {"fn": "dequant_q4_0_block",    "style": "split", "block": 32},
    "q4_1":    {"fn": "dequant_q4_1_block",    "style": "split", "block": 32},
    "q8_0":    {"fn": "dequant_q8_0_block",    "style": "split", "block": 32},
    "iq4_nl":  {"fn": "dequant_iq4_nl_block",  "style": "split", "block": 32},
}

# Skip list. I2_S's GEMM shader does BitNet-style per-row int8 activation
# quantization before the dot product (see web/shaders/matmul_transb_i2_s.wgsl)
# so a naïve A[k] * dequant(...) MMV would have different semantics. Adding the
# proper MMV-aware activation pre-pass is its own follow-up.
SKIP = {"i2_s"}

GEMM_PRELUDE_STRIPS = [
    re.compile(r"^const TILE_[MNK]: u32 = \d+u?;\s*\n", re.M),
    re.compile(r"^var<workgroup> tile_[ab]: array<f32, \d+>;\s*\n", re.M),
]


def extract_prologue(text: str) -> str:
    """Return everything before the @compute attribute, with TILE_* and
    tile_a/tile_b removed so we don't waste workgroup memory in the MMV variant.
    """
    cut = text.find("@compute")
    if cut == -1:
        raise ValueError("no @compute attribute found")
    head = text[:cut]
    for pat in GEMM_PRELUDE_STRIPS:
        head = pat.sub("", head)
    return head.rstrip() + "\n\n"


def mmv_compute_block(fmt: str, spec: dict) -> str:
    fn = spec["fn"]
    if spec["style"] == "kabs":
        call = f"{fn}(col, k)"
    elif spec["style"] == "split":
        block = spec["block"]
        call = f"{fn}(col, k / {block}u, k % {block}u)"
    else:
        raise ValueError(spec["style"])

    return (
        f"// === MMV (matrix-vector) variant — auto-generated by scripts/gen_mmv_shaders.py.\n"
        f"// One workgroup per output column. Threads cooperatively reduce over K\n"
        f"// using a workgroup-shared partial-sum array and a tree reduction.\n"
        f"const MMV_WORKGROUP_SIZE: u32 = {WORKGROUP_SIZE}u;\n"
        f"var<workgroup> mmv_partial: array<f32, {WORKGROUP_SIZE}>;\n\n"
        f"@compute @workgroup_size(MMV_WORKGROUP_SIZE, 1, 1)\n"
        f"fn matmul_transb_{fmt}_mmv(\n"
        f"    @builtin(workgroup_id) wid: vec3<u32>,\n"
        f"    @builtin(local_invocation_id) lid: vec3<u32>,\n"
        f") {{\n"
        f"    let col = wid.x;\n"
        f"    let tid = lid.x;\n"
        f"    if (col >= params.N) {{ return; }}\n"
        f"\n"
        f"    var acc: f32 = 0.0;\n"
        f"    var k: u32 = tid;\n"
        f"    loop {{\n"
        f"        if (k >= params.K) {{ break; }}\n"
        f"        acc = acc + A[k] * {call};\n"
        f"        k = k + MMV_WORKGROUP_SIZE;\n"
        f"    }}\n"
        f"\n"
        f"    mmv_partial[tid] = acc;\n"
        f"    workgroupBarrier();\n"
        f"\n"
        f"    var stride: u32 = MMV_WORKGROUP_SIZE / 2u;\n"
        f"    loop {{\n"
        f"        if (stride == 0u) {{ break; }}\n"
        f"        if (tid < stride) {{\n"
        f"            mmv_partial[tid] = mmv_partial[tid] + mmv_partial[tid + stride];\n"
        f"        }}\n"
        f"        workgroupBarrier();\n"
        f"        stride = stride / 2u;\n"
        f"    }}\n"
        f"\n"
        f"    if (tid == 0u) {{\n"
        f"        C[col] = mmv_partial[0];\n"
        f"    }}\n"
        f"}}\n"
    )


def main() -> int:
    missing = []
    written = []
    for fmt, spec in sorted(DEQUANT_SPECS.items()):
        gemm_path = SHADERS_DIR / f"matmul_transb_{fmt}.wgsl"
        if not gemm_path.exists():
            missing.append(str(gemm_path.relative_to(ROOT)))
            continue
        gemm = gemm_path.read_text()
        prologue = extract_prologue(gemm)
        body = mmv_compute_block(fmt, spec)

        out_path = SHADERS_DIR / f"matmul_transb_{fmt}_mmv.wgsl"
        out_path.write_text(prologue + body)
        written.append(out_path.relative_to(ROOT))

    for p in written:
        print(f"wrote {p}")
    for skipped in sorted(SKIP):
        print(f"skipped {skipped}: requires special handling")
    if missing:
        print(f"\nmissing source GEMM shaders for: {missing}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
