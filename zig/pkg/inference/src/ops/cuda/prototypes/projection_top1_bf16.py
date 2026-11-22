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
    from producer_support import profiles, compile_program, emit, write_manifest

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
    for spec, (fn, signature, constants, attrs, options) in zip(profiles("projection_top1"), jobs, strict=True):
        # Alignment positions remain ABI policy; resources and image identities
        # come from the same catalog used by serving launches.
        data = compile_program(fn, signature, constants, (i[0] for i in attrs), spec, options)
        records.append(emit(output, spec, data))
    write_manifest(output, records)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    generate(args.output)
