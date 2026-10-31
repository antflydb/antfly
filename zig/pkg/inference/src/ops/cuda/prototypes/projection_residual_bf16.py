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
    import hashlib
    import json
    from pathlib import Path
    from triton.compiler import ASTSource
    from triton.backends.compiler import GPUTarget

    assert triton.__version__ == "3.7.1"
    triton.knobs.compilation.disable_line_info = True
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    records = []
    for hidden, wide in [(768, 3072), (1024, 4096)]:
        for bf16 in [False, True]:
            scalar = "*bf16" if bf16 else "*fp32"
            signature = dict(
                x="*bf16",
                w="*bf16",
                bias=scalar,
                residual=scalar,
                out=scalar,
                rows="i32",
            )
            compiled = triton.compile(
                ASTSource(
                    lighton_vision_down,
                    signature,
                    constexprs=dict(
                        K=wide, N=hidden, BM=128, BN=64, BK=64, BF16_IO=bf16
                    ),
                    attrs={(i,): [["tt.divisibility", 16]] for i in range(5)},
                ),
                target=GPUTarget("cuda", 121, 32),
                options=dict(
                    num_warps=4, num_stages=3, ptx_version=90, enable_fp_fusion=False
                ),
            )
            meta = compiled.metadata
            assert meta.global_scratch_size == 0 and meta.profile_scratch_size == 0
            assert meta.num_ctas == 1 and meta.shared == 49152 and meta.num_warps == 4
            name = f"projection_residual_sm121_k{wide}_n{hidden}_io_{'bf16' if bf16 else 'f32'}.ptx"
            data = compiled.asm["ptx"].encode()
            (output / name).write_bytes(data)
            records.append(
                dict(
                    file=name,
                    sha256=hashlib.sha256(data).hexdigest(),
                    shared=meta.shared,
                    threads=meta.num_warps * 32,
                )
            )
    (output / "manifest.json").write_text(json.dumps(records, indent=2) + "\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    generate(parser.parse_args().output)
