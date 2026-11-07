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

"""SM121 vision down GEMM with original bias and residual rounding, Triton3.7.1."""

import triton
import triton.language as tl
from projection_dot_bias_bf16 import dot_bias


@triton.jit(do_not_specialize=["rows"], do_not_specialize_on_alignment=["rows"])
def lighton_vision_down(
    x,
    w,
    bias,
    residual,
    out,
    rows,
    K: tl.constexpr,
    N: tl.constexpr,
    BM: tl.constexpr,
    BN: tl.constexpr,
    BK: tl.constexpr,
    BF16_IO: tl.constexpr,
):
    m, n, value = dot_bias(x, w, bias, rows, K, N, BM, BN, BK)
    if BF16_IO:
        value = value.to(tl.bfloat16, fp_downcast_rounding="rtne").to(tl.float32)
    rv = tl.load(
        residual + n[None, :] * N + m[:, None],
        (m[:, None] < N) & (n[None, :] < rows),
        other=0,
    ).to(tl.float32)
    value = tl.inline_asm_elementwise(
        "add.rn.f32 $0, $1, $2;",
        constraints="=f,f,f",
        args=[value, rv],
        dtype=tl.float32,
        is_pure=True,
        pack=1,
    )
    tl.store(
        out + n[None, :] * N + m[:, None], value, (m[:, None] < N) & (n[None, :] < rows)
    )


def generate(output):
    from producer_support import profiles, compile_program, emit, write_manifest

    records = []
    for spec in profiles("projection_residual"):
        scalar = "*bf16" if spec["bf16"] else "*fp32"
        data = compile_program(
            lighton_vision_down,
            dict(x="*bf16", w="*bf16", bias=scalar, residual=scalar, out=scalar, rows="i32"),
            dict(K=spec["wide"], N=spec["hidden"], BM=128, BN=64, BK=64, BF16_IO=spec["bf16"]),
            range(5), spec, dict(num_stages=3, enable_fp_fusion=False))
        records.append(emit(output, spec, data))
    write_manifest(output, records)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    generate(parser.parse_args().output)
