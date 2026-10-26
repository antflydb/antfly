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

"""Build-time source for the SM12.1 BF16-input, F32-accumulating OCR head.

The runtime exports plain greedy kernels. Compile-time suppression tuples and
full-matrix output are available to numerical qualification only.
"""

import triton
import triton.language as tl


@triton.jit(do_not_specialize=["Rows"])
def lighton_head(
    X,
    W,
    Full,
    Max,
    Ids,
    Rows,
    N: tl.constexpr,
    K: tl.constexpr,
    BM: tl.constexpr,
    BN: tl.constexpr,
    BK: tl.constexpr,
    WRITE: tl.constexpr,
    SUPPRESS_IDS: tl.constexpr = (),
):
    m = tl.program_id(0) * BM + tl.arange(0, BM)
    n = tl.program_id(1) * BN + tl.arange(0, BN)
    ks = tl.arange(0, BK)
    acc = tl.full((BM, BN), 0, tl.float32)
    for kb in range(tl.cdiv(K, BK)):
        k = kb * BK + ks
        a = tl.load(
            X + m[:, None] * K + k[None, :], (m[:, None] < Rows) & (k[None, :] < K), 0
        )
        b = tl.load(
            W + n[None, :] * K + k[:, None], (n[None, :] < N) & (k[:, None] < K), 0
        )
        acc += tl.dot(a, b, allow_tf32=False)
    if WRITE:
        tl.store(
            Full + m[:, None] * N + n[None, :],
            acc,
            (m[:, None] < Rows) & (n[None, :] < N),
        )
    valid = n < N
    for suppressed in tl.static_range(len(SUPPRESS_IDS)):
        valid = valid & (n != SUPPRESS_IDS[suppressed])
    values = tl.where(valid[None, :] & (acc == acc), acc, float("-inf"))
    best = tl.max(values, 1)
    idx = tl.min(tl.where(values == best[:, None], n[None, :], 2147483647), 1)
    pos = m * tl.cdiv(N, BN) + tl.program_id(1)
    tl.store(Max + pos, best, m < Rows)
    tl.store(Ids + pos, idx, m < Rows)


@triton.jit
def lighton_head_finish(Max, Ids, Out, T: tl.constexpr, B: tl.constexpr):
    row = tl.program_id(0)
    t = tl.arange(0, B)
    values = tl.load(Max + row * T + t, t < T, float("-inf"))
    indices = tl.load(Ids + row * T + t, t < T, 2147483647)
    best = tl.max(values, 0)
    idx = tl.min(tl.where(values == best, indices, 2147483647), 0)
    tl.store(Out + row, idx)
