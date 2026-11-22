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

"""Exact-boundary BF16 projection and GELU, Triton 3.7.1/SM121.
No FP8/FP4, no split-K, FP32 tensor-core accumulation and original bias values.
"""

import triton
import triton.language as tl
from triton.language.extra.cuda import libdevice
from projection_dot_bias_bf16 import dot_bias


@triton.jit(do_not_specialize=["rows"], do_not_specialize_on_alignment=["rows"])
def lighton_vision_up(
    x,
    w,
    bias,
    out,
    rows,
    K: tl.constexpr,
    N: tl.constexpr,
    ROUND_BEFORE: tl.constexpr,
    BM: tl.constexpr,
    BN: tl.constexpr,
    BK: tl.constexpr,
):
    m, n, value = dot_bias(x, w, bias, rows, K, N, BM, BN, BK)
    if ROUND_BEFORE:
        value = value.to(tl.bfloat16, fp_downcast_rounding="rtne").to(tl.float32)
    t = 0.7978845608028654 * (value + ((0.044715 * value) * value) * value)
    activated = (0.5 * value) * (1.0 + libdevice.tanh(t))
    result = activated.to(tl.bfloat16, fp_downcast_rounding="rtne")
    tl.store(
        out + n[None, :] * N + m[:, None],
        result,
        (m[:, None] < N) & (n[None, :] < rows),
    )


def generate(output, libdevice_path):
    import hashlib
    from pathlib import Path
    from producer_support import profiles, compile_program, emit, write_manifest, LIBDEVICE_SHA256

    if hashlib.sha256(Path(libdevice_path).read_bytes()).hexdigest() != LIBDEVICE_SHA256:
        raise RuntimeError("Regeneration requires the qualified CUDA 13.0 libdevice")
    records = []
    for spec in profiles("projection_gelu"):
        rounded = spec["bf16"]
        data = compile_program(
            lighton_vision_up,
            dict(x="*bf16", w="*bf16", bias="*bf16" if rounded else "*fp32", out="*bf16", rows="i32"),
            dict(K=spec["hidden"], N=spec["wide"], ROUND_BEFORE=rounded, BM=128, BN=64, BK=32),
            range(4), spec,
            dict(num_stages=3, enable_fp_fusion=not rounded, extern_libs={"libdevice": str(libdevice_path)}))
        records.append(emit(output, spec, data))
    write_manifest(output, records, triton=triton.__version__, libdevice_sha256=LIBDEVICE_SHA256)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate SM121 BF16-input vision GEMM+exact GELU PTX without a GPU."
    )
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--libdevice", default="/usr/local/cuda-13.0/nvvm/libdevice/libdevice.10.bc"
    )
    args = parser.parse_args()
    generate(args.output, args.libdevice)
