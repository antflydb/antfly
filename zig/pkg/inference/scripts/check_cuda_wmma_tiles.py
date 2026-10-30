#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualify native WMMA score phases with original bits and stdlib F64 references.

Arguments are frozen-original and candidate CUDA images. --bench optionally
uses the shared fixed stream/graph AA/AB/BA assay on 65-row cases. Reference
bounds are declared per storage contract before execution; no GPU framework.
"""

import argparse
import array
import ctypes as C
import math
import struct
from pathlib import Path

from check_cuda_row_families import (
    Device,
    benchmark,
    emit,
    f32,
    packed,
    rounded,
    values,
)


class Boundary(C.Structure):
    _fields_ = [("kind", C.c_uint), ("dims", C.c_uint * 8), ("scalars", C.c_float * 4)]


assert C.sizeof(Boundary) == 52


def half(value):
    return struct.unpack("<e", struct.pack("<e", value))[0]


def samples(n, salt, scale):
    return [f32(x * scale) for x in values(n, salt)]


def attention_reference(
    q,
    k,
    v,
    batch,
    seq,
    heads,
    narrow,
    mask=None,
    qr=None,
    kr=None,
    buckets=None,
    zero_query=False,
):
    """Independent masked softmax of the original staged operands."""
    q, k, v = ([narrow(x) for x in t] for t in (q, k, v))
    relative = qr is not None
    if relative:
        qr, kr = ([narrow(x) for x in t] for t in (qr, kr))
    hidden = heads * 64
    scale = 1 / math.sqrt(192 if relative else 64)
    out = [0.0] * (batch * seq * hidden)
    for b in range(batch):
        keys = [key for key in range(seq) if mask is None or mask[b * seq + key]]
        if not keys:
            continue
        for row in range(seq):
            if zero_query and not mask[b * seq + row]:
                continue
            for h in range(heads):
                base = (b * seq + row) * hidden + h * 64
                qv = q[base : base + 64]
                scores = []
                for key in keys:
                    at = (b * seq + key) * hidden + h * 64
                    kv = k[at : at + 64]
                    score = sum(a * z for a, z in zip(qv, kv))
                    if relative:
                        rel = row + seq - 1 - key
                        rel = buckets[rel] if buckets is not None else rel
                        at = rel * hidden + h * 64
                        score += sum(a * z for a, z in zip(qv, kr[at : at + 64]))
                        score += sum(a * z for a, z in zip(kv, qr[at : at + 64]))
                    scores.append(score * scale)
                maximum = max(scores)
                weights = [math.exp(x - maximum) for x in scores]
                total = sum(weights)
                accum = [0.0] * 64
                for key, weight in zip(keys, weights):
                    at = (b * seq + key) * hidden + h * 64
                    for d in range(64):
                        accum[d] += weight * v[at + d]
                out[base : base + 64] = [x / total for x in accum]
    return out


def qualify(
    device,
    modules,
    symbol,
    args,
    grid,
    output,
    count,
    dtype,
    reference,
    tolerance,
    name,
    timing,
):
    functions = [device.function(m, symbol) for m in modules]
    sentinel = (
        packed([123.0] * (count + 8), "f32")
        if dtype == "f32"
        else array.array("H", [0x3555] * (count + 8)).tobytes()
    )
    result = []
    for function in functions:
        # Each side must publish its own output; never inherit the other side's writes.
        device.upload(output, sentinel)
        device.launch(function, args, grid, 256)
        device.synchronize()
        result.append(device.download(output, len(sentinel)))
    assert result[0] == result[1], (name, "original bitwise parity")
    unit = 4 if dtype == "f32" else 2
    assert result[1][-8 * unit :] == sentinel[-8 * unit :], (name, "canary")
    actual = (
        list(array.array("f", result[1][:-32]))
        if dtype == "f32"
        else [
            struct.unpack("<f", struct.pack("<I", x << 16))[0]
            for x in array.array("H", result[1][:-16])
        ]
    )
    assert len(actual) == len(reference) == count
    assert all(math.isfinite(x) for x in actual), name
    error = max(abs(a - b) for a, b in zip(actual, reference))
    assert all(
        abs(a - b) <= tolerance * (1 + abs(b)) for a, b in zip(actual, reference)
    ), (name, error, tolerance)
    emit(kind="correctness", case=name, passed=True, max_abs=error, tolerance=tolerance)
    if timing:
        benchmark(device, functions, args, grid, name)
    device.free_case()


def relative_case(device, modules, seq, mode, timing):
    batch, heads = (2 if seq % 3 == 1 else 1), (2 if seq % 2 else 1)
    n = batch * seq * heads * 64
    q, k, v = (
        samples(n, salt, scale) for salt, scale in [(1, 0.12), (2, 0.09), (3, 0.2)]
    )
    boundary = mode == "boundary"
    rows = 17 if boundary else 2 * seq - 1
    qr, kr = (samples(rows * heads * 64, salt, 0.07) for salt in (4, 5))
    mask = [
        int(b == 0 and (seq < 65 or key >= 32) and key % 7 != 3)
        for b in range(batch)
        for key in range(seq)
    ]
    buckets = [(i * 7 + 3) % rows for i in range(2 * seq - 1)] if boundary else None
    reference = attention_reference(
        q, k, v, batch, seq, heads, half, mask, qr, kr, buckets, boundary
    )
    inputs = [device.alloc(packed(x, "f32")) for x in (q, k, v, qr, kr)]
    output = device.alloc(packed([123.0] * (n + 8), "f32"))
    if boundary:
        bucket = device.alloc(array.array("i", buckets).tobytes())
        valid = device.alloc(array.array("i", mask).tobytes())
        p = Boundary(30, (C.c_uint * 8)(batch, seq, heads, 64, rows, 0, 0, 0))
        args = [
            *inputs,
            bucket,
            valid,
            inputs[0],
            inputs[0],
            inputs[0],
            output,
            p,
            C.c_uint(n),
        ]
        symbol, tile = "termite_gliner_boundary_attention_tc_f16", 16
    else:
        valid = device.alloc(array.array("q", mask).tobytes())
        args = [output, *inputs, valid, *map(C.c_uint, (batch, seq, heads, 64))]
        tile = 16 if mode == "m16n32" else 32
        symbol = "termite_deberta_attention_tc_f16_" + mode
    qualify(
        device,
        modules,
        symbol,
        args,
        ((seq + tile - 1) // tile * batch * heads, 1),
        output,
        n,
        "f32",
        reference,
        3e-4,
        f"relative/{mode}/{seq}/{batch}/{heads}",
        timing and seq == 65,
    )


def vision_case(device, modules, seq, native, timing):
    batch, heads = (2 if seq % 3 == 1 else 1), (2 if seq % 2 else 1)
    n = batch * seq * heads * 64
    q, k, v = (
        samples(n, salt, scale) for salt, scale in [(1, 0.12), (2, 0.09), (3, 0.2)]
    )
    reference = attention_reference(q, k, v, batch, seq, heads, rounded)
    dtype = "bf16" if native else "f32"
    inputs = [device.alloc(packed(x, dtype)) for x in (q, k, v)]
    sentinel = (
        packed([123.0] * (n + 8), "f32")
        if not native
        else array.array("H", [0x3555] * (n + 8)).tobytes()
    )
    output = device.alloc(sentinel)
    args = [output, *inputs, *map(C.c_uint, (batch, seq, heads, 64))]
    symbol = (
        "termite_embedding_gemma2_native_vision_bf16"
        if native
        else "termite_qwen3vl_vision_attention_tc_bf16_m32n16"
    )
    qualify(
        device,
        modules,
        symbol,
        args,
        ((seq + 31) // 32 * batch * heads, 1),
        output,
        n,
        dtype,
        reference,
        1e-2 if native else 2e-3,
        f"vision/{native}/{seq}/{batch}/{heads}",
        timing and seq == 65,
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("baseline", type=Path)
    p.add_argument("candidate", type=Path)
    p.add_argument("--bench", action="store_true")
    a = p.parse_args()
    device = Device()
    modules = [device.module(path) for path in (a.baseline, a.candidate)]
    for seq in (1, 7, 15, 16, 17, 31, 32, 33, 65, 129, 255, 256):
        for mode in ("m16n32", "m32n16", "boundary"):
            relative_case(device, modules, seq, mode, a.bench)
    relative_case(device, modules, 513, "boundary", a.bench)
    for seq in (1, 7, 15, 16, 17, 31, 32, 33, 65, 129, 257, 513):
        for native in (False, True):
            vision_case(device, modules, seq, native, a.bench)


if __name__ == "__main__":
    main()
