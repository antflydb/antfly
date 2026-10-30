#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Frozen-original and F64 qualification of prefix/pool/training row programs.

Every side starts from independent nonzero output state, including VJP suffixes.
--bench retains the shared fixed stream/graph AA/AB/BA assay; no retry gate.
"""

import argparse
import array
import ctypes as C
import math
from pathlib import Path

from check_cuda_row_families import Device, benchmark, emit, packed, rounded, values


def samples(n, salt=0, scale=0.2):
    return list(array.array("f", (x * scale for x in values(n, salt))))


def check(
    d,
    modules,
    symbol,
    args,
    out,
    initial,
    reference,
    name,
    grid,
    threads=256,
    dtype="f32",
    timing=False,
    generic=False,
):
    functions = [d.function(m, symbol) for m in modules]
    results = []
    for function in functions:
        d.upload(out, initial)
        d.launch(function, args, grid, threads)
        d.synchronize()
        results.append(d.download(out, len(initial)))
    assert results[0] == results[1], (name, "original bits")
    unit = 2 if dtype == "bf16" else 4
    assert results[1][-8 * unit :] == initial[-8 * unit :], (name, "canary")
    raw = array.array("H" if dtype == "bf16" else "f", results[1][: -8 * unit])
    if dtype == "bf16":
        from check_cuda_row_families import from_bf16

        raw = [from_bf16(x) for x in raw]
    assert len(raw) == len(reference)
    tolerance = 0.015 if dtype == "bf16" or "rms" in name else 8e-5
    assert all(
        math.isfinite(x) and abs(x - y) <= tolerance * (1 + abs(y))
        for x, y in zip(raw, reference)
    ), (name, max(abs(x - y) for x, y in zip(raw, reference)))
    emit(
        kind="correctness",
        case=name,
        passed=True,
        max_abs=max(abs(x - y) for x, y in zip(raw, reference)),
        generic=generic,
    )
    if timing:
        d.upload(out, initial)
        benchmark(d, functions, args, grid, name, threads)
    d.free_case()


def prefix(d, modules, rows, width, stride, backward, mode):
    n = rows * stride
    x = samples(n, 1)
    dy = samples(rows * width, 2)
    weight = 0.25
    if mode:
        x = [0.0 if mode == 1 else v * 1e-14 for v in x]
    reference = [7.0] * n if backward else []
    for r in range(rows):
        row = x[r * stride : r * stride + width]
        norm = math.sqrt(sum(v * v for v in row))
        clamp = max(norm, 1e-12)
        if backward:
            dot = (
                0
                if norm < 1e-12
                else -sum(dy[r * width + i] * v for i, v in enumerate(row)) / (norm**3)
            )
            for i, v in enumerate(row):
                reference[r * stride + i] += weight * (
                    dy[r * width + i] / clamp + (dot * v if norm else 0)
                )
        else:
            reference.extend(v / clamp for v in row)
    initial = packed(
        ([7.0] * n if backward else [123.0] * (rows * width)) + [123.0] * 8, "f32"
    )
    out = d.alloc(initial)
    ip = d.alloc(packed(x, "f32"))
    args = [out]
    if backward:
        args.append(d.alloc(packed(dy, "f32")))
    args.extend([ip, C.c_uint(rows), C.c_uint(width)])
    if backward:
        args.append(C.c_float(weight))
    args.append(C.c_uint(stride))
    stem = "accumulate_vjp" if backward else "normalize"
    # Same generic program on both sides for arbitrary strides; independent F64
    # catches indexing errors. The legacy 768 path compares frozen vs candidate.
    if stride == 768:
        legacy = (
            "termite_embedding_gemma2_mrl_normalize"
            + ("_vjp" if backward else "")
            + "_f32"
        )
        functions = [
            d.function(modules[0], legacy),
            d.function(modules[1], "antfly_prefix_l2_" + stem + "_f32"),
        ]
        results = []
        for f in functions:
            d.upload(out, initial)
            d.launch(f, args, rows)
            d.synchronize()
            results.append(d.download(out, len(initial)))
        assert results[0] == results[1], (
            "prefix generic vs original specialization",
            width,
            backward,
            mode,
        )
        mods = modules
        symbol = legacy
    else:
        mods = [modules[1], modules[1]]
        symbol = "antfly_prefix_l2_" + stem + "_f32"
    check(
        d,
        mods,
        symbol,
        args,
        out,
        initial,
        reference,
        f"prefix/{stem}/{rows}/{width}/{stride}/{mode}",
        rows,
        generic=stride != 768,
    )


def rms(d, modules, width, ty, backward, weighted, timing):
    rows = 4
    n = rows * width
    x = [rounded(v) for v in samples(n, 1)]
    w = [rounded(v + 1) for v in samples(width, 2)] if weighted else [1.0] * width
    dy = samples(n, 3)
    ref = []
    for r in range(rows):
        row = x[r * width : (r + 1) * width]
        inv = 1 / math.sqrt(sum(v * v for v in row) / width + 1e-5)
        dot = sum(rounded(dy[r * width + i]) * w[i] * v for i, v in enumerate(row))
        for i, v in enumerate(row):
            value = (
                inv * (rounded(dy[r * width + i]) * w[i] - v * dot * inv * inv / width)
                if backward
                else v * inv * w[i]
            )
            ref.append(rounded(value))
    dtype = "f32" if backward else "bf16"
    initial = packed([123.0] * (n + 8), dtype)
    out = d.alloc(initial)
    args = [out]
    if backward:
        args.append(d.alloc(packed(dy, "f32")))
    args.extend(
        [
            d.alloc(packed(x, "bf16")),
            d.alloc(packed(w, "bf16")) if weighted else C.c_uint64(0),
            C.c_uint(rows),
            C.c_uint(width),
            C.c_float(1e-5),
        ]
    )
    symbol = "termite_embedding_gemma2_training_rms_norm" + (
        "_input_vjp_f32" if backward else "_bf16"
    )
    check(
        d,
        modules,
        symbol,
        args,
        out,
        initial,
        ref,
        f"rms/{width}/{ty}/{backward}/{weighted}",
        rows // ty,
        (128, ty),
        dtype,
        timing,
    )


def pool(d, modules, seq, width, empty, timing):
    batch = 3
    x = [rounded(v) for v in samples(batch * seq * width)]
    mask = [
        int(i // seq != 1 and (not empty and i % 3 != 0)) for i in range(batch * seq)
    ]
    ref = []
    for b in range(batch):
        count = sum(mask[b * seq : (b + 1) * seq])
        row = []
        for col in range(width):
            row.append(
                sum(
                    x[(b * seq + s) * width + col]
                    for s in range(seq)
                    if mask[b * seq + s]
                )
                / count
                if count
                else 0
            )
        norm = math.sqrt(sum(v * v for v in row))
        ref.extend(v / norm if norm else 0 for v in row)
    initial = packed([123.0] * (batch * width + 8), "f32")
    out = d.alloc(initial)
    args = [
        out,
        d.alloc(packed(x, "bf16")),
        d.alloc(array.array("q", mask).tobytes()),
        C.c_uint(batch),
        C.c_uint(seq),
        C.c_uint(width),
    ]
    check(
        d,
        modules,
        "termite_embedding_gemma2_mean_l2_f32",
        args,
        out,
        initial,
        ref,
        f"pool/{seq}/{width}/{empty}",
        batch,
        timing=timing,
    )


def convolution(d, modules, rows, dim, kernel, timing):
    x = samples(rows * dim, 1)
    w = samples(kernel * dim, 2)
    starts = [float((r // 11) * 11) for r in range(rows)]
    ref = []
    for r in range(rows):
        for col in range(dim):
            ref.append(
                sum(
                    x[(r + k - (kernel - 1)) * dim + col] * w[k * dim + col]
                    for k in range(kernel)
                    if r + k >= kernel - 1 + starts[r]
                )
            )
    initial = packed([123.0] * (rows * dim + 8), "f32")
    out = d.alloc(initial)
    args = [
        out,
        d.alloc(packed(x, "f32")),
        d.alloc(packed(w, "f32")),
        d.alloc(packed(starts, "f32")),
        C.c_uint(rows),
        C.c_uint(dim),
        C.c_uint(kernel),
    ]
    check(
        d,
        modules,
        "termite_embedding_gemma2_audio_segment_conv_f32",
        args,
        out,
        initial,
        ref,
        f"conv/{rows}/{dim}/{kernel}",
        (rows * dim + 255) // 256,
        timing=timing,
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("original", type=Path)
    p.add_argument("candidate", type=Path)
    p.add_argument("--bench", action="store_true")
    p.add_argument(
        "--bench-pooling",
        action="store_true",
        help="time only the changed pooling family",
    )
    a = p.parse_args()
    d = Device()
    modules = [d.module(a.original), d.module(a.candidate)]
    for rows, width, stride in [
        (1, 1, 768),
        (3, 63, 768),
        (7, 257, 768),
        (4, 768, 768),
        (513, 7, 17),
        (3, 65, 257),
        (2, 1025, 1152),
    ]:
        for backward in [False, True]:
            for mode in range(3):
                prefix(d, modules, rows, width, stride, backward, mode)
    for width in [64, 768, 1152]:
        for ty in [1, 2, 4]:
            for backward in [False, True]:
                for weighted in [False, True]:
                    rms(
                        d,
                        modules,
                        width,
                        ty,
                        backward,
                        weighted,
                        a.bench and width == 768 and ty == 2 and weighted,
                    )
    for seq, width in [(1, 1), (7, 63), (32, 768), (65, 1025)]:
        for empty in [False, True]:
            pool(
                d,
                modules,
                seq,
                width,
                empty,
                (a.bench or a.bench_pooling) and seq == 32 and not empty,
            )
    for rows, dim, k in [(1, 1, 1), (7, 63, 3), (65, 768, 5), (257, 1025, 7)]:
        convolution(d, modules, rows, dim, k, a.bench and rows == 65)


if __name__ == "__main__":
    main()
