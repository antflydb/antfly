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

"""SM121 F32-input/BF16-weight head with fused top1 reduction, Triton 3.7.1."""

import triton
import triton.language as tl


@triton.jit(
    do_not_specialize=["rows", "suppress_count"],
    do_not_specialize_on_alignment=["rows", "suppress_count", "suppress", "x"],
)
def lighton_head_top1_partial(
    x,
    w,
    partial,
    indices,
    suppress,
    rows,
    suppress_count,
    K: tl.constexpr,
    N: tl.constexpr,
    BM: tl.constexpr,
    BN: tl.constexpr,
    BK: tl.constexpr,
):
    m = tl.program_id(0) * BM + tl.arange(0, BM)
    n = tl.program_id(1) * BN + tl.arange(0, BN)
    kk = tl.arange(0, BK)
    acc = tl.full((BM, BN), 0, tl.float32)
    for block in range(tl.cdiv(K, BK)):
        k = block * BK + kk
        a = tl.load(
            w + m[:, None] * K + k[None, :],
            (m[:, None] < N) & (k[None, :] < K),
            other=0,
        )
        b = tl.load(
            x + n[None, :] * K + k[:, None],
            (n[None, :] < rows) & (k[:, None] < K),
            other=0,
        ).to(tl.bfloat16, fp_downcast_rounding="rtne")
        acc = tl.dot(a, b, acc)
    valid = m < N
    for i in range(suppress_count):
        valid = valid & (m != tl.load(suppress + i))
    # Production starts with -FLT_MAX and index0 and ignores NaNs.
    value = tl.where(
        valid[:, None] & (acc == acc) & (acc >= -3.402823466e38), acc, float("-inf")
    )
    maximum = tl.max(value, axis=0)
    index = tl.min(tl.where(value == maximum[None, :], m[:, None], 2147483647), axis=0)
    tiles = tl.cdiv(N, BM)
    tl.store(partial + n * tiles + tl.program_id(0), maximum, n < rows)
    tl.store(indices + n * tiles + tl.program_id(0), index, n < rows)


@triton.jit(do_not_specialize_on_alignment=["output"])
def lighton_head_top1_finish(
    partial, indices, output, TILES: tl.constexpr, B: tl.constexpr
):
    row = tl.program_id(0)
    i = tl.arange(0, B)
    values = tl.load(partial + row * TILES + i, i < TILES, other=float("-inf"))
    maximum = tl.maximum(tl.max(values, axis=0), -3.402823466e38)
    ids = tl.load(indices + row * TILES + i, i < TILES, other=2147483647)
    best = tl.min(tl.where((i < TILES) & (values == maximum), ids, 2147483647), axis=0)
    best = tl.where((best == 2147483647) | (maximum == -3.402823466e38), 0, best)
    tl.store(output + row, best)


def generate(output):
    import json
    import hashlib
    from pathlib import Path
    from triton.compiler import ASTSource
    from triton.backends.compiler import GPUTarget

    assert triton.__version__ == "3.7.1"
    triton.knobs.compilation.disable_line_info = True
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    jobs = [
        (
            lighton_head_top1_partial,
            dict(
                x="*fp32",
                w="*bf16",
                partial="*fp32",
                indices="*i32",
                suppress="*i32",
                rows="i32",
                suppress_count="i32",
            ),
            dict(K=1024, N=248320, BM=256, BN=128, BK=32),
            {(i,): [["tt.divisibility", 16]] for i in range(1, 4)},
            dict(num_warps=8, num_stages=2, ptx_version=90),
        ),
        (
            lighton_head_top1_finish,
            dict(partial="*fp32", indices="*i32", output="*i32"),
            dict(TILES=970, B=1024),
            {(i,): [["tt.divisibility", 16]] for i in range(2)},
            dict(num_warps=4, ptx_version=90),
        ),
    ]
    records = []
    for fn, signature, constants, attrs, options in jobs:
        compiled = triton.compile(
            ASTSource(fn, signature, constexprs=constants, attrs=attrs),
            target=GPUTarget("cuda", 121, 32),
            options=options,
        )
        meta = compiled.metadata
        assert meta.global_scratch_size == 0 and meta.profile_scratch_size == 0
        assert meta.num_ctas == 1
        assert (
            meta.shared == 32768
            if fn is lighton_head_top1_partial
            else meta.shared <= 4096
        )
        data = compiled.asm["ptx"].encode()
        (output / (fn.__name__ + ".ptx")).write_bytes(data)
        records.append(
            dict(
                file=fn.__name__ + ".ptx",
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
    args = parser.parse_args()
    generate(args.output)
