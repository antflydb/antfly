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

"""Shared BF16 projection dot/bias phase with explicit FP32 bias rounding."""
import triton
import triton.language as tl


@triton.jit
def dot_bias(x, w, bias, rows, K: tl.constexpr, N: tl.constexpr, BM: tl.constexpr, BN: tl.constexpr, BK: tl.constexpr):
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
        )
        acc = tl.dot(a, b, acc)
    bv = tl.load(bias + m, m < N, other=0).to(tl.float32)
    # Prevent moving bias into the tensor-core accumulator: the original
    # operator publishes a separately rounded FP32 sum before activation.
    value = tl.inline_asm_elementwise(
        "add.rn.f32 $0, $1, $2;",
        constraints="=f,f,f",
        args=[acc, bv[:, None]],
        dtype=tl.float32,
        is_pure=True,
        pack=1,
    )
    return m, n, value
