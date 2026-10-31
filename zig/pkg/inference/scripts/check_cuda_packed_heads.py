#!/usr/bin/env python3
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

"""Qualify packed BF16 head normalization and rotary after row-owner changes."""

import argparse
import array
import ctypes as C
import math
from pathlib import Path

from check_cuda_row_families import (
    Device,
    benchmark,
    emit,
    f32,
    from_bf16,
    packed,
    rounded,
    values,
)


def qualify_case(d, fn, dim, vision, weighted, run_bench):
    rows, qh, kvh, seq = 7, 4, 2, 7
    width = (qh + 2 * kvh) * dim
    x = list(map(rounded, values(rows * width)))
    w = [rounded(0.75 + t / 16) for t in values(dim, 1)]
    mask = [int(i % 3 != 0) for i in range(rows)]
    positions = [(i * 7 + 3) % 17 for i in range(rows * qh * 2)]
    inp = d.alloc(packed(x, "bf16"))
    weight = d.alloc(packed(w, "bf16"))
    maskptr = d.alloc(array.array("q", mask).tobytes())
    posptr = d.alloc(array.array("I", positions).tobytes())
    sizes = [rows * h * dim * 2 for h in (qh, kvh, kvh)]
    outputs = [d.alloc(bytes(n)) for n in sizes]
    u, f = C.c_uint, C.c_float
    args = outputs + [
        inp,
        weight if weighted else C.c_uint64(0),
        weight if weighted else C.c_uint64(0),
        maskptr,
        posptr,
        u(rows),
        u(seq),
        u(qh),
        u(kvh),
        u(dim),
        f(1e-6),
        f(10000),
        u(vision),
        f(0.5),
    ]

    def reference():
        result = [[], [], []]
        for row in range(rows):
            for slot in range(qh + 2 * kvh):
                kind = 0 if slot < qh else 1 if slot < qh + kvh else 2
                head = slot - (0 if kind == 0 else qh if kind == 1 else qh + kvh)
                raw = x[row * width + slot * dim : row * width + (slot + 1) * dim]
                inv = 1 / math.sqrt(sum(t * t for t in raw) / dim + 1e-6)
                norm = [
                    rounded(t * inv * (w[c] if weighted and kind != 2 else 1))
                    for c, t in enumerate(raw)
                ]
                for col in range(dim):
                    value = norm[col]
                    if kind != 2 and (vision or mask[row]):
                        if vision:
                            pair = col % 16
                            base = col - col % 32
                            pos = positions[(row * qh + head) * 2 + col // 32]
                            angle = pos / (10000 ** (2 * pair / 32))
                            sn, cs = math.sin(angle), math.cos(angle)
                            lo, hi = norm[base + pair], norm[base + 16 + pair]
                            value = (
                                lo * cs - hi * sn
                                if col % 32 < 16
                                else lo * sn + hi * cs
                            )
                            value = rounded(value * (0.5 if kind == 0 else 1))
                        else:
                            pair = col % (dim // 2)
                            angle = f32(row * (1 / (10000 ** (2 * pair / dim))))
                            sn, cs = (
                                rounded(math.sin(angle)),
                                rounded(math.cos(angle)),
                            )
                            lo, hi = norm[pair], norm[pair + dim // 2]
                            value = rounded(
                                rounded((lo if col < dim // 2 else hi) * cs)
                                + rounded((-hi if col < dim // 2 else lo) * sn)
                            )
                    result[kind].append(value)
        return result

    def check(outputs_read):
        for data, ref in zip(outputs_read, reference()):
            actual = array.array("H")
            actual.frombytes(data)
            for i, (bits, expected) in enumerate(zip(actual, ref)):
                val = from_bf16(bits)
                assert math.isfinite(val) and abs(val - expected) < 0.03 * (
                    1 + abs(expected)
                ), (dim, vision, weighted, i, val, expected)

    results = []
    for fcn in fn:
        d.launch(fcn, args, (rows, qh + 2 * kvh))
        d.synchronize()
        results.append([d.download(p, n) for p, n in zip(outputs, sizes)])
    assert results[0] == results[1], (dim, vision, weighted, "bitwise")
    check(results[1])
    graphs = [
        d.graph(lambda fcn=fcn: d.launch(fcn, args, (rows, qh + 2 * kvh))) for fcn in fn
    ]
    x = list(map(rounded, values(rows * width, 7)))
    d.upload(inp, packed(x, "bf16"))
    results = []
    for graph, exe in graphs:
        d.call("cuGraphLaunch", exe, d.stream)
        d.synchronize()
        results.append([d.download(p, n) for p, n in zip(outputs, sizes)])
    assert results[0] == results[1], (
        dim,
        vision,
        weighted,
        "mutable graph bitwise",
    )
    check(results[1])
    for graph, exe in graphs:
        d.call("cuGraphExecDestroy", exe)
        d.call("cuGraphDestroy", graph)
    label = f"packed-heads/dim{dim}/vision{vision}/weighted{int(weighted)}"
    emit(
        kind="correctness",
        case=label,
        bitwise=True,
        reference=True,
        mutable_graph_replays=1,
    )
    if run_bench and weighted and dim in (64, 256, 512):
        benchmark(d, fn, args, (rows, qh + 2 * kvh), label)
    d.free_case()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("baseline", type=Path)
    p.add_argument("candidate", type=Path)
    p.add_argument("--bench", action="store_true")
    opts = p.parse_args()
    d = Device()
    modules = [d.module(x / "kernels.cubin") for x in (opts.baseline, opts.candidate)]
    fn = [d.function(m, "termite_embedding_gemma2_packed_heads_bf16") for m in modules]
    cases = 0
    for dim, vision in [
        (32, 0),
        (64, 0),
        (96, 0),
        (128, 0),
        (256, 0),
        (512, 0),
        (64, 1),
    ]:
        for weighted in (False, True):
            qualify_case(d, fn, dim, vision, weighted, opts.bench)
            cases += 1
    emit(kind="complete", cases=cases)


if __name__ == "__main__":
    main()
