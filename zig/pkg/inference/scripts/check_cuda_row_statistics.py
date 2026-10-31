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

"""Qualify existing VJP, pooling and masked-softmax users of shared row statistics."""

import argparse
import array
import ctypes as C
import math
from pathlib import Path

from check_cuda_row_families import Device, benchmark, emit, packed, rounded, values


def qualify(
    d,
    modules,
    name,
    args,
    grid,
    outputs,
    source,
    data,
    element_bytes,
    label,
    bench,
    shared=0,
):
    functions = [d.function(m, name) for m in modules]
    reads = []
    for fn in functions:
        d.launch(fn, args, grid, shared_bytes=shared)
        d.synchronize()
        reads.append([d.download(ptr, len(ref) * 4) for ptr, ref, _ in outputs])
    assert reads[0] == reads[1], (label, "original bitwise mismatch")
    for blob, (_, ref, tol) in zip(reads[1], outputs):
        actual = array.array("f")
        actual.frombytes(blob)
        for i, (a, b) in enumerate(zip(actual, ref)):
            assert a == b or (
                math.isfinite(a)
                and math.isfinite(b)
                and abs(a - b) <= tol * (1 + abs(b))
            ), (label, i, a, b)
    graphs = [
        d.graph(lambda fn=fn: d.launch(fn, args, grid, shared_bytes=shared))
        for fn in functions
    ]
    d.upload(
        source,
        b"".join(
            data[i : i + element_bytes]
            for i in reversed(range(0, len(data), element_bytes))
        ),
    )
    reads = []
    for graph, exe in graphs:
        d.call("cuGraphLaunch", exe, d.stream)
        d.synchronize()
        reads.append([d.download(ptr, len(ref) * 4) for ptr, ref, _ in outputs])
    assert reads[0] == reads[1], (label, "mutable graph bitwise mismatch")
    for graph, exe in graphs:
        d.call("cuGraphExecDestroy", exe)
        d.call("cuGraphDestroy", graph)
    d.upload(source, data)
    emit(
        kind="correctness",
        case=label,
        bitwise=True,
        reference=True,
        mutable_graph_replays=1,
    )
    if bench:
        benchmark(d, functions, args, grid, label, shared_bytes=shared)
    d.free_case()


def rms_vjp(d, modules, width, weighted, bench):
    rows = 3
    n = rows * width
    x = list(map(rounded, values(n)))
    dy = values(n, 1)
    weight = [rounded(0.75 + t / 16) for t in values(width, 2)]
    data = packed(x, "bf16")
    inp = d.alloc(data)
    yp = d.alloc(packed(dy, "f32"))
    wp = d.alloc(packed(weight, "bf16"))
    out = d.alloc(bytes(n * 4))
    ref = []
    for r in range(rows):
        v = x[r * width : (r + 1) * width]
        g = list(map(rounded, dy[r * width : (r + 1) * width]))
        w = weight if weighted else [1] * width
        inv = 1 / math.sqrt(sum(t * t for t in v) / width + 1e-6)
        correction = sum(a * b * c for a, b, c in zip(g, w, v)) * inv * inv / width
        ref += [rounded(inv * (a * b - c * correction)) for a, b, c in zip(g, w, v)]
    args = [
        out,
        yp,
        inp,
        wp if weighted else C.c_uint64(0),
        C.c_uint(rows),
        C.c_uint(width),
        C.c_float(1e-6),
    ]
    qualify(
        d,
        modules,
        "termite_embedding_gemma2_rms_norm_input_vjp_f32",
        args,
        rows,
        [(out, ref, 0.03)],
        inp,
        data,
        2,
        f"rms-vjp/width{width}/weighted{int(weighted)}",
        bench,
    )


def mean_l2(d, modules, width, masked, bench):
    batch, seq = 3, 7
    n = batch * seq * width
    x = list(map(rounded, values(n)))
    mask = [int(b < 2 and s % 3 != 0) for b in range(batch) for s in range(seq)]
    data = packed(x, "bf16")
    inp = d.alloc(data)
    mp = d.alloc(array.array("q", mask).tobytes())
    out = d.alloc(bytes(batch * width * 4))
    ref = []
    for b in range(batch):
        active = [s for s in range(seq) if not masked or mask[b * seq + s]]
        mean = [
            sum(x[(b * seq + s) * width + c] for s in active) / len(active)
            if active
            else 0
            for c in range(width)
        ]
        norm = math.sqrt(sum(t * t for t in mean))
        ref += [t / norm if norm else 0 for t in mean]
    args = [
        out,
        inp,
        mp if masked else C.c_uint64(0),
        C.c_uint(batch),
        C.c_uint(seq),
        C.c_uint(width),
    ]
    qualify(
        d,
        modules,
        "termite_embedding_gemma2_mean_l2_f32",
        args,
        batch,
        [(out, ref, 2e-5)],
        inp,
        data,
        2,
        f"mean-l2/width{width}/masked{int(masked)}",
        bench,
    )


def probabilities(d, modules, count, masked, bench):
    rows, width = 3, 32
    q = [t / 16 for t in values(rows * width)]
    k = [t / 16 for t in values(count * width, 1)]
    valid = [int(r < 2 and c % 3 != 0) for r in range(rows) for c in range(count)]
    targets = [1, 2, 0]
    data = packed(q, "f32")
    qp = d.alloc(data)
    kp = d.alloc(packed(k, "f32"))
    vp = d.alloc(bytes(valid))
    tp = d.alloc(array.array("I", targets).tobytes())
    out = d.alloc(bytes(rows * count * 4))
    loss = d.alloc(bytes(rows * 4))
    ref = []
    losses = []
    for r in range(rows):
        logits = [
            sum(q[r * width + c] * k[j * width + c] for c in range(width)) * 0.3
            if not masked or valid[r * count + j]
            else -math.inf
            for j in range(count)
        ]
        maximum = max(logits)
        weights = [math.exp(t - maximum) if math.isfinite(t) else 0 for t in logits]
        denom = sum(weights)
        normalized = [t / denom if denom else 0 for t in weights]
        ref += normalized
        p = normalized[targets[r]]
        losses.append(-math.log(p) * 0.7 if p else math.inf)
    args = [
        out,
        loss,
        qp,
        kp,
        vp if masked else C.c_uint64(0),
        tp,
        C.c_uint(rows),
        C.c_uint(count),
        C.c_uint(width),
        C.c_float(0.3),
        C.c_float(0.7),
    ]
    qualify(
        d,
        modules,
        "termite_embedding_gemma2_contrastive_prob_f32",
        args,
        rows,
        [(out, ref, 2e-5), (loss, losses, 2e-5)],
        qp,
        data,
        4,
        f"probabilities/count{count}/masked{int(masked)}",
        bench,
        shared=32,
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("baseline", type=Path)
    p.add_argument("candidate", type=Path)
    p.add_argument("--bench", action="store_true")
    opts = p.parse_args()
    d = Device()
    modules = [d.module(x / "kernels.cubin") for x in (opts.baseline, opts.candidate)]
    for width in (35, 128, 1536):
        for flag in (False, True):
            rms_vjp(d, modules, width, flag, opts.bench and width == 1536 and flag)
            mean_l2(d, modules, width, flag, opts.bench and width == 1536 and flag)
    for count in (33, 257):
        for masked in (False, True):
            probabilities(
                d, modules, count, masked, opts.bench and count == 257 and masked
            )
    emit(kind="complete", cases=16)


if __name__ == "__main__":
    main()
