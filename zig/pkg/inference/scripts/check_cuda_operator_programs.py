#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualify operation-based CUDA programs against originals and stdlib references.

Directories contain florence/activation/vision/recurrent.cubin (or .ptx).
--bench retains stream and captured-graph AA/AB/BA samples, using the shared
31x32 assay: median <=1.05, p95 <=1.10. No Python GPU framework is required.
"""

import argparse
import ctypes as C
import math
import struct
from pathlib import Path

from check_cuda_row_families import (
    Device,
    benchmark,
    emit,
    f32,
    from_bf16,
    packed,
    values,
)


def unpack(data, dtype):
    if dtype == "bf16":
        return [from_bf16(x[0]) for x in struct.iter_unpack("<H", data)]
    return [x[0] for x in struct.iter_unpack("<e" if dtype == "f16" else "<f", data)]


def qualify(
    d, modules, symbols, args, grid, outputs, references, name, bench, threads=256
):
    functions = [d.function(module, symbol) for module, symbol in zip(modules, symbols)]
    results = []
    for fn in functions:
        d.launch(fn, args, grid, threads)
        d.synchronize()
        results.append([d.download(ptr, size) for ptr, size, _ in outputs])
    assert results[0] == results[1], (name, "original bitwise parity")
    worst = 0
    for data, (_, size, dtype), reference in zip(results[1], outputs, references):
        unit = 4 if dtype == "f32" else 2
        assert (
            data[-8 * unit :]
            == struct.pack("<f" if unit == 4 else "<H", 123.0 if unit == 4 else 0x3555)
            * 8
        ), (name, "output canary")
        actual = unpack(data[: -8 * unit], dtype)
        assert all(math.isfinite(x) for x in actual), name
        if reference is None:
            continue
        tolerance = 1e-2 if dtype == "bf16" else 3e-3 if dtype == "f16" else 3e-5
        assert len(actual) == len(reference), name
        error = max((abs(a - b) for a, b in zip(actual, reference)), default=0)
        assert all(
            abs(a - b) <= tolerance * (1 + abs(b)) for a, b in zip(actual, reference)
        ), (name, error)
        worst = max(worst, error)
    emit(kind="correctness", case=name, max_abs=worst, passed=True)
    if bench:
        benchmark(d, functions, args, grid, name, threads=threads)
    d.free_case()


def output(d, n, dtype):
    unit = 4 if dtype == "f32" else 2
    pattern = struct.pack("<f", 123.0) if unit == 4 else struct.pack("<H", 0x3555)
    pointer = d.alloc(pattern * (n + 8))
    return pointer, (n + 8) * unit, dtype


def norm_case(d, modules, rows, dim, window, high_offset, timing):
    n = rows * dim
    x = values(n, 1)
    gamma = values(dim, 2)
    beta = values(dim, 3)
    if high_offset:
        x = [f32(10000 + (i % 17) / 8) for i in range(n)]
    inp, g, b = [d.alloc(packed(v, "f32")) for v in (x, gamma, beta)]
    out = output(d, n, "f16")
    args = [out[0], inp, g, b, C.c_uint(rows), C.c_uint(dim), C.c_float(1e-5)]
    reference = [0.0] * n
    for row in range(rows):
        v = x[row * dim : (row + 1) * dim]
        mean = sum(v) / dim
        inv = 1 / math.sqrt(sum((a - mean) ** 2 for a in v) / dim + 1e-5)
        target = row
        if window:
            page = row // 16
            y = (row // 4) % 4
            col = row % 4
            target = (((page * 2 + y // 2) * 2 + col // 2) * 2 + y % 2) * 2 + col % 2
        reference[target * dim : (target + 1) * dim] = [
            (a - mean) * inv * w + z for a, w, z in zip(v, gamma, beta)
        ]
    if window:
        args += [C.c_uint(4), C.c_uint(4), C.c_uint(2)]
    symbols = (
        ("florence_window_norm_f16", "antfly_window_layer_norm_warp_finish_f16")
        if window
        else ("florence_layer_norm_f16", "antfly_layer_norm_warp_finish_f16")
    )
    qualify(
        d,
        modules,
        symbols,
        args,
        rows,
        [out],
        [None if high_offset else reference],
        f"layer_norm/{rows}/{dim}/{window}/{high_offset}",
        timing and rows == 129 and dim in (768, 2560),
    )


def rms_case(d, modules, rows, dim, offset, timing):
    n = rows * dim
    x = values(n, 4)
    w = values(dim, 2)
    inp, weight = [d.alloc(packed(v, "f32")) for v in (x, w)]
    out = output(d, n, "bf16")
    args = [
        out[0],
        inp,
        weight,
        C.c_uint(rows),
        C.c_uint(dim),
        C.c_float(1e-5),
        C.c_float(offset),
    ]
    ref = []
    for row in range(rows):
        v = x[row * dim : (row + 1) * dim]
        inv = 1 / math.sqrt(sum(a * a for a in v) / dim + 1e-5)
        ref.extend(a * inv * (b + offset) for a, b in zip(v, w))
    qualify(
        d,
        modules,
        ("lighton_rms_input_bf16", "antfly_rms_tree_bf16"),
        args,
        rows,
        [out],
        [ref],
        f"rms/{rows}/{dim}/{offset}",
        timing and rows == 129 and dim in (1024, 2560),
    )


def rope_case(d, modules, rows, heads, dim, timing):
    width = heads * dim
    n = rows * width
    half = dim // 2
    axis_pairs = half // 2
    x = values(n * 3, 7)
    positions = [(i * 7 + 3) % 31 for i in range(rows * 2)]
    inp = d.alloc(packed(x, "f32"))
    pos = d.alloc(struct.pack("<" + "I" * len(positions), *positions))
    outs = [output(d, n, "bf16") for _ in range(3)]
    refs = [[0.0] * n for _ in range(3)]
    for row in range(rows):
        for head in range(heads):
            for pair in range(half):
                axis = 0 if pair < axis_pairs else 1
                local = pair % axis_pairs
                angle = positions[axis * rows + row] / (10000 ** (2 * local / half))
                sn, cs = math.sin(angle), math.cos(angle)
                src = row * 3 * width + head * dim + pair
                dst = (row * heads + head) * dim + pair
                for part in range(2):
                    lo, hi = x[src + part * width], x[src + part * width + half]
                    refs[part][dst] = lo * cs - hi * sn
                    refs[part][dst + half] = lo * sn + hi * cs
                refs[2][dst] = x[src + 2 * width]
                refs[2][dst + half] = x[src + 2 * width + half]
    args = [o[0] for o in outs] + [
        inp,
        pos,
        C.c_uint(rows),
        C.c_uint(heads),
        C.c_uint(dim),
        C.c_float(10000),
    ]
    qualify(
        d,
        modules,
        ("lighton_vision_qkv_rope_bf16", "antfly_qkv_rope_two_axis_bf16"),
        args,
        (n // 2 + 255) // 256,
        outs,
        refs,
        f"rotary/{rows}/{heads}/{dim}",
        timing and rows == 129 and dim == 128,
    )


def stencil_case(d, modules, height, width, channels, pages, timing):
    n = pages * height * width * channels
    x = values(n, 1)
    w = values(channels * 9, 3)
    b = values(channels, 7)
    inp, weight, bias = [d.alloc(packed(v, "f32")) for v in (x, w, b)]
    out = output(d, n, "f32")
    # CUDA __fmaf_rn is the declared independent convolution contract.
    lib = C.CDLL("libm.so.6")
    fma = lib.fmaf
    fma.argtypes = [C.c_float] * 3
    fma.restype = C.c_float
    ref = []
    for i in range(n):
        c = i % channels
        token = i // channels
        xx = token % width
        yy = (token // width) % height
        page = token // (width * height)
        acc = b[c]
        for ky in range(3):
            iy = yy + ky - 1
            if not 0 <= iy < height:
                continue
            for kx in range(3):
                ix = xx + kx - 1
                if not 0 <= ix < width:
                    continue
                source = ((page * height + iy) * width + ix) * channels + c
                acc = fma(x[source], w[c * 9 + ky * 3 + kx], acc)
        ref.append(f32(x[i] + acc))
    args = [
        out[0],
        inp,
        weight,
        bias,
        C.c_uint(n),
        C.c_uint(height),
        C.c_uint(width),
        C.c_uint(channels),
    ]
    qualify(
        d,
        modules,
        ("florence_token_depthwise_residual", "antfly_depthwise_3x3_residual_f32"),
        args,
        (n + 255) // 256,
        [out],
        [ref],
        f"depthwise_3x3/{height}/{width}/{channels}/{pages}",
        timing and height == 17,
    )


def causal_case(d, modules, seq, channels, initialized, timing):
    x = values(seq * channels, 1)
    w = [f32(v * 0.03) for v in values(channels * 4, 2)]
    h = values(channels * 4, 3)
    inp, weight, hist = [d.alloc(packed(v, "f32")) for v in (x, w, h)]
    nxt = output(d, channels * 4, "f32")
    out = output(d, seq * channels, "f32")
    ref = []
    history = []
    for t in range(seq):
        for c in range(channels):
            acc = 0.0
            for k in range(4):
                src = t + k - 3
                sample = (
                    x[src * channels + c]
                    if src >= 0
                    else h[c * 4 + src + 4]
                    if initialized
                    else 0
                )
                acc = f32(acc + f32(sample * w[c * 4 + k]))
            ref.append(acc / (1 + math.exp(-acc)))
    for c in range(channels):
        for k in range(4):
            src = seq + k - 4
            history.append(
                x[src * channels + c]
                if src >= 0
                else h[c * 4 + src + 4]
                if initialized
                else 0
            )
    args = [
        inp,
        weight,
        hist,
        nxt[0],
        out[0],
        C.c_int(seq),
        C.c_int(channels),
        C.c_int(initialized),
    ]
    qualify(
        d,
        modules,
        ("lighton_conv", "antfly_gdn_causal_conv"),
        args,
        (channels + 255) // 256,
        [nxt, out],
        [history, ref],
        f"causal/{seq}/{channels}/{initialized}",
        timing and seq == 17,
    )


def audio_causal_case(d, modules, rows, dim, kernel, timing):
    n = rows * dim
    x = values(n, 1)
    w = values(kernel * dim, 3)
    inp, weight = [d.alloc(packed(v, "f32")) for v in (x, w)]
    out = output(d, n, "f32")
    reference = []
    for t in range(rows):
        for c in range(dim):
            total = 0.0
            for k in range(kernel):
                source = t + k + 1 - kernel
                if source >= 0:
                    total = f32(total + f32(x[source * dim + c] * w[k * dim + c]))
            reference.append(total)
    args = [out[0], inp, weight, C.c_uint(rows), C.c_uint(dim), C.c_uint(kernel)]
    qualify(
        d,
        modules,
        (
            "termite_gemma4_audio_depthwise_causal_conv1d_f32",
            "termite_causal_depthwise_f32",
        ),
        args,
        (n + 255) // 256,
        [out],
        [reference],
        f"audio_causal/{rows}/{dim}/{kernel}",
        timing and rows == 129 and dim in (64, 1024),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--bench", action="store_true")
    parser.add_argument("--ptx", action="store_true")
    parser.add_argument("--core-only", action="store_true")
    parser.add_argument("--core-baseline", type=Path)
    parser.add_argument("--core-candidate", type=Path)
    args = parser.parse_args()
    d = Device()
    suffix = "ptx" if args.ptx else "cubin"
    modules = {
        name: [
            d.module(p / (name + "." + suffix)) for p in (args.baseline, args.candidate)
        ]
        for name in ("florence", "activation", "vision", "recurrent")
    }
    if args.core_only:
        assert args.core_baseline and args.core_candidate
        core = [d.module(path) for path in (args.core_baseline, args.core_candidate)]
        for rows in (1, 7, 129):
            for dim in (1, 7, 64, 1024):
                for kernel in (1, 3, 4, 7):
                    audio_causal_case(d, core, rows, dim, kernel, args.bench)
        return
    for rows in (1, 3, 129):
        for dim in (4, 32, 128, 768, 1024, 2560):
            norm_case(d, modules["florence"], rows, dim, False, False, args.bench)
    norm_case(d, modules["florence"], 3, 4, False, True, False)
    for rows in (16, 32):
        for dim in (32, 768):
            norm_case(d, modules["florence"], rows, dim, True, False, args.bench)
    for rows in (1, 129):
        for dim in (4, 128, 1024, 2560):
            for offset in (0, 1):
                rms_case(d, modules["activation"], rows, dim, offset, args.bench)
    for rows in (1, 7, 129):
        for heads in (1, 4):
            for dim in (32, 64, 128, 256):
                rope_case(d, modules["vision"], rows, heads, dim, args.bench)
    for h, w, c, pages in ((1, 1, 1, 1), (3, 5, 7, 2), (7, 7, 64, 1), (17, 19, 64, 2)):
        stencil_case(d, modules["florence"], h, w, c, pages, args.bench)
    for seq in (1, 2, 3, 4, 7, 17):
        for channels in (7, 128, 1024):
            for initialized in (False, True):
                causal_case(
                    d, modules["recurrent"], seq, channels, initialized, args.bench
                )


if __name__ == "__main__":
    main()
