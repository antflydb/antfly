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

"""Frozen-original parity and stdlib references for generic training row programs.

Images contain the stable LayerNorm/softmax/log-softmax ABI exports. No Torch
installation is needed. --bench uses paired stream/graph AA/AB/BA measurements
from check_cuda_row_families; median <=1.05 and p95 <=1.10, retaining misses.
"""

import argparse
import array
import ctypes as C
import math
from pathlib import Path

from check_cuda_row_families import Device, benchmark, emit, f32, packed, values


def read(d, pointer, n):
    result = array.array("f")
    result.frombytes(d.download(pointer, n * 4))
    return list(result)


def check(actual, reference):
    worst = max((abs(a - b) for a, b in zip(actual, reference)), default=0)
    assert all(
        math.isfinite(a) and abs(a - b) <= 2e-5 * (1 + abs(b))
        for a, b in zip(actual, reference)
    ), worst
    return worst


def softmax_case(d, modules, rows, width, backward, logarithmic, timing):
    n = rows * width
    x = values(n, 7)
    probability = []
    for row in range(rows):
        logits = values(width, row)
        maximum = max(logits)
        denominator = sum(math.exp(v - maximum) for v in logits)
        probability.extend(
            f32((v - maximum) - math.log(denominator))
            if logarithmic
            else f32(math.exp(v - maximum) / denominator)
            for v in logits
        )
    if backward and not logarithmic:
        x = [f32(a * b) for a, b in zip(x, probability)]
    reference = []
    for row in range(rows):
        v = x[row * width : (row + 1) * width]
        p = probability[row * width : (row + 1) * width]
        if backward:
            total = sum(v)
            reference.extend(
                a - (math.exp(b) if logarithmic else b) * total for a, b in zip(v, p)
            )
        else:
            maximum = max(v)
            denominator = sum(math.exp(a - maximum) for a in v)
            reference.extend(
                (a - maximum) - math.log(denominator)
                if logarithmic
                else math.exp(a - maximum) / denominator
                for a in v
            )
    inputs = [d.alloc(packed(v, "f32")) for v in [x, probability]]
    output = d.alloc(packed([123.0] * (n + 8), "f32"))
    args = [output, *inputs, C.c_uint(rows), C.c_uint(width), C.c_uint(backward)]
    symbol = (
        "termite_gliner25_log_softmax_warp_f32"
        if logarithmic
        else "termite_gliner25_softmax_f32"
    )
    functions = [d.function(m, symbol) for m in modules]
    padded = 1 << (width - 1).bit_length()
    lanes = min(padded, 32)
    warps = 128 // lanes
    batches = 2 if padded <= 128 else 1
    grid = (rows + batches * warps - 1) // (batches * warps)
    results = []
    for function in functions:
        d.launch(function, args, grid, (lanes, warps))
        d.synchronize()
        results.append(d.download(output, (n + 8) * 4))
    assert results[0] == results[1], (symbol, rows, width, backward)
    actual = read(d, output, n + 8)
    assert actual[n:] == [123.0] * 8
    worst = check(actual[:n], reference)
    # Captured executable must read caller-owned buffers on every replay.
    graphs = [
        d.graph(lambda fn=fn: d.launch(fn, args, grid, (lanes, warps)))
        for fn in functions
    ]
    d.upload(inputs[0], packed([-v for v in x], "f32"))
    replay = []
    for graph, executable in graphs:
        d.call("cuGraphLaunch", executable, d.stream)
        d.synchronize()
        replay.append(d.download(output, (n + 8) * 4))
        d.call("cuGraphExecDestroy", executable)
        d.call("cuGraphDestroy", graph)
    assert replay[0] == replay[1], "mutable graph parity"
    d.upload(inputs[0], packed(x, "f32"))
    name = f"{'log_' if logarithmic else ''}softmax/{rows}/{width}/{backward}"
    emit(kind="correctness", case=name, max_abs=worst, passed=True)
    if timing and rows == 129 and width in (128, 1024, 2048):
        benchmark(d, functions, args, grid, name, threads=(lanes, warps))
    d.free_case()


def norm_case(d, modules, rows, width, phase, timing):
    n = rows * width
    x, gamma, beta, dy = values(n, 1), values(width, 2), values(width, 3), values(n, 4)
    eps = f32(1e-5)
    stats = [[], []]
    dx, dg, db, forward = [], [0.0] * width, [0.0] * width, []
    for row in range(rows):
        r = x[row * width : (row + 1) * width]
        grad = dy[row * width : (row + 1) * width]
        mean = sum(r) / width
        inv = 1 / math.sqrt(sum((v - mean) ** 2 for v in r) / width + eps)
        stats[0].append(f32(mean))
        stats[1].append(f32(inv))
        normalized = [(v - mean) * inv for v in r]
        a = sum(g * w for g, w in zip(grad, gamma))
        b = sum(g * w * v for g, w, v in zip(grad, gamma, normalized))
        dx.extend(
            inv * (g * w - (a + v * b) / width)
            for g, w, v in zip(grad, gamma, normalized)
        )
        forward.extend(w * v + z for w, v, z in zip(gamma, normalized, beta))
        for col in range(width):
            dg[col] += grad[col] * normalized[col]
            db[col] += grad[col]
    output = d.alloc(packed([123.0] * (n + 2 * width + 8), "f32"))
    pointers = [
        d.alloc(packed(v, "f32")) for v in [x, gamma, beta, dy, stats[0] + stats[1]]
    ]
    args = [
        output,
        *pointers,
        C.c_uint(rows),
        C.c_uint(width),
        C.c_float(eps),
        C.c_uint(phase),
    ]
    functions = [d.function(m, "termite_gliner25_layer_norm_f32") for m in modules]
    partitions = 1 if rows < 64 else 8 if rows < 128 else 16 if rows < 256 else 32
    grid, threads, shared = (
        ((width + 31) // 32, (32, partitions), partitions * 33 * 8)
        if phase == 2
        else (rows, 128, 32)
    )
    results = []
    for fn in functions:
        d.launch(fn, args, grid, threads, shared)
        d.synchronize()
        results.append(d.download(output, (n + 2 * width + 8) * 4))
    assert results[0] == results[1], (rows, width, phase)
    actual = read(d, output, n + 2 * width + 8)
    assert actual[-8:] == [123.0] * 8
    worst = (
        check(actual[n : n + 2 * width], dg + db)
        if phase == 2
        else check(actual[:n], forward if phase == 0 else dx)
    )
    name = f"welford/{rows}/{width}/{phase}"
    emit(kind="correctness", case=name, max_abs=worst, passed=True)
    if timing and rows == 129 and width in (768, 1024):
        benchmark(d, functions, args, grid, name, threads=threads, shared_bytes=shared)
    d.free_case()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--bench", action="store_true")
    args = parser.parse_args()
    d = Device()
    modules = [d.module(p) for p in (args.baseline, args.candidate)]
    for rows in (1, 3, 129):
        for width in (1, 2, 7, 31, 32, 33, 65, 128, 129, 257, 512, 769, 1024):
            for backward in (False, True):
                for logarithmic in (False, True):
                    softmax_case(
                        d, modules, rows, width, backward, logarithmic, args.bench
                    )
        softmax_case(d, modules, rows, 2048, False, True, args.bench)
    for rows in (1, 3, 63, 64, 127, 129, 259):
        for width in (4, 252, 768, 1024):
            for phase in range(3):
                norm_case(d, modules, rows, width, phase, args.bench)


if __name__ == "__main__":
    main()
