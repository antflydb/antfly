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
    import json
    from pathlib import Path
    from triton.compiler import ASTSource
    from triton.backends.compiler import GPUTarget

    if triton.__version__ != "3.7.1":
        raise RuntimeError("Regeneration requires Triton 3.7.1")
    triton.knobs.compilation.disable_line_info = True
    if (
        hashlib.sha256(Path(libdevice_path).read_bytes()).hexdigest()
        != "91334d6e12748f6cb5bbf0a1cd965a56bcd93dc4f496d2e5c5f8c6e523094356"
    ):
        raise RuntimeError("Regeneration requires the qualified CUDA 13.0 libdevice")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    records = []
    for hidden, wide in [(768, 3072), (1024, 4096)]:
        for rounded in [False, True]:
            signature = dict(
                x="*bf16",
                w="*bf16",
                bias="*bf16" if rounded else "*fp32",
                out="*bf16",
                rows="i32",
            )
            constants = dict(
                K=hidden, N=wide, ROUND_BEFORE=rounded, BM=128, BN=64, BK=32
            )
            compiled = triton.compile(
                ASTSource(
                    lighton_vision_up,
                    signature,
                    constexprs=constants,
                    attrs={(i,): [["tt.divisibility", 16]] for i in range(4)},
                ),
                target=GPUTarget("cuda", 121, 32),
                options=dict(
                    ptx_version=90,
                    num_warps=4,
                    num_stages=3,
                    enable_fp_fusion=not rounded,
                    extern_libs={"libdevice": str(libdevice_path)},
                ),
            )
            meta = compiled.metadata
            assert meta.shared == 24576 and meta.num_warps == 4 and meta.num_ctas == 1
            assert meta.global_scratch_size == 0 and meta.profile_scratch_size == 0
            name = f"projection_gelu_sm121_k{hidden}_n{wide}_bias_{'bf16' if rounded else 'f32'}.ptx"
            data = compiled.asm["ptx"].encode()
            (output / name).write_bytes(data)
            records.append(
                dict(
                    file=name,
                    sha256=hashlib.sha256(data).hexdigest(),
                    shared_bytes=meta.shared,
                    threads=128,
                )
            )
    (output / "projection_gelu_bf16_manifest.json").write_text(
        json.dumps(
            dict(
                triton=triton.__version__,
                libdevice_sha256=hashlib.sha256(
                    Path(libdevice_path).read_bytes()
                ).hexdigest(),
                kernels=records,
            ),
            indent=2,
        )
        + "\n"
    )


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
