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

"""Stdlib-only parity/reference/timing coverage for GDN convolution schedules.

Covers cached gates, uniform and ragged prefixes, independent page histories and
both production head geometries. Reuses the fixed AA/AB/BA stream/graph assay.
"""

import argparse
import ctypes as C
import math
from pathlib import Path

from check_cuda_operator_programs import output, qualify
from check_cuda_row_families import Device, f32, packed, values


class Page(C.Structure):
    _fields_ = [(name, C.c_uint64) for name in ("history", "next_history", "state")]


class Batch(C.Structure):
    _fields_ = (
        [
            (name, C.c_uint64)
            for name in (
                "mixed",
                "conv_weight",
                "conv",
                "z",
                "beta",
                "a",
                "a_log",
                "dt_bias",
                "core",
                "norm_weight",
                "out",
            )
        ]
        + [(name, C.c_int) for name in ("key_heads", "value_heads", "items")]
        + [("eps", C.c_float), ("pages", Page * 64)]
    )


class Prefix(C.Structure):
    _fields_ = [("base", Batch), ("seq", C.c_int), ("reserved", C.c_int)]


class Ragged(C.Structure):
    _fields_ = [("base", Batch)] + [
        (name, C.c_int * 64) for name in ("offsets", "lengths", "initialized")
    ]


assert (C.sizeof(Batch), C.sizeof(Prefix), C.sizeof(Ragged)) == (1640, 1648, 2408)


def case(d, modules, kind, seq, vh, timing):
    kh = 16
    channels = (2 * kh + vh) * 128
    width = vh * 128
    ragged = kind.startswith("ragged")
    batch = kind in ("batch", "prefix_batch") or ragged
    lengths = (
        ([2, 3, max(2, seq)] if kind == "ragged" else [1, 3, seq])
        if ragged
        else [1] * 3
        if kind == "batch"
        else [seq] * 3
        if batch
        else [seq]
    )
    initialized = (
        [0, 1, 1]
        if ragged
        else [1] * 3
        if kind == "batch"
        else [0] * len(lengths)
        if kind == "prefix_batch"
        else [1]
    )
    offsets = []
    rows = 0
    for length in lengths:
        offsets.append(rows)
        rows += length
    x = [f32(v * 0.2) for v in values(rows * channels, 1)]
    w = [f32(v * 0.03) for v in values(channels * 4, 2)]
    beta = values(rows * vh, 4)
    a = values(rows * vh, 5)
    alog = [f32(-0.8 + 0.03 * h) for h in range(vh)]
    dt = [f32(0.2 - 0.01 * h) for h in range(vh)]
    pointers = [d.alloc(packed(v, "f32")) for v in (x, w, beta, a, alog, dt)]
    conv = output(d, rows * channels, "f32")
    cache = output(d, rows * width, "f32")
    dummy = d.alloc(packed([0.0] * 8, "f32"))
    histories = [
        [f32(v * 0.2) for v in values(channels * 4, p + 7)] for p in range(len(lengths))
    ]
    hptr = [d.alloc(packed(h, "f32")) for h in histories]
    nexts = [output(d, channels * 4, "f32") for _ in lengths]
    reference = []
    nextref = []
    normalized = kind in ("prefix", "prefix_batch", "ragged", "ragged_norm")
    for page, length in enumerate(lengths):
        offset = offsets[page]
        local = []
        for t in range(length):
            for c in range(channels):
                total = 0.0
                for k in range(4):
                    src = t + k - 3
                    sample = (
                        x[(offset + src) * channels + c]
                        if src >= 0
                        else histories[page][c * 4 + src + 4]
                        if initialized[page]
                        else 0.0
                    )
                    total = f32(total + f32(sample * w[c * 4 + k]))
                local.append(f32(total / (1 + math.exp(-total))))
        if normalized:
            for t in range(length):
                for h in range(2 * kh):
                    start = t * channels + h * 128
                    v = local[start : start + 128]
                    inverse = 1 / math.sqrt(sum(z * z for z in v) + 1e-6)
                    scale = 0.08838834764831845 if h < kh else 1
                    local[start : start + 128] = [z * inverse * scale for z in v]
        reference.extend(local)
        history = []
        for c in range(channels):
            for k in range(4):
                src = length + k - 4
                history.append(
                    x[(offset + src) * channels + c]
                    if src >= 0
                    else histories[page][c * 4 + src + 4]
                    if initialized[page]
                    else 0.0
                )
        nextref.append(history)
    cache_ref = [123.0] * (rows * width)
    cached = kind in ("cached", "prefix", "prefix_batch", "ragged", "ragged_norm")
    if cached:
        for t in range(rows):
            for h in range(vh):
                z = f32(a[t * vh + h] + dt[h])
                softplus = z if z > 20 else math.log1p(math.exp(z))
                base = (t * vh + h) * 128
                cache_ref[base] = math.exp(-math.exp(alog[h]) * softplus)
                cache_ref[base + 1] = 1 / (1 + math.exp(-beta[t * vh + h]))
    if batch:
        p = Batch()
        p.mixed = pointers[0].value
        p.conv_weight = pointers[1].value
        p.conv = conv[0].value
        p.z = dummy.value
        p.beta = pointers[2].value
        p.a = pointers[3].value
        p.a_log = pointers[4].value
        p.dt_bias = pointers[5].value
        p.core = dummy.value
        p.norm_weight = dummy.value
        p.out = cache[0].value
        p.key_heads = kh
        p.value_heads = vh
        p.items = len(lengths)
        p.eps = 1e-6
        for page in range(len(lengths)):
            p.pages[page] = Page(hptr[page].value, nexts[page][0].value, dummy.value)
        if kind == "batch":
            arg = p
            symbol = "lighton_batch_conv"
            grid = (channels // 128, len(lengths))
        elif kind == "prefix_batch":
            arg = Prefix(p, seq, 0)
            symbol = "lighton_prefix_batch_conv_normalize"
            grid = (channels // 128, rows)
        else:
            arg = Ragged()
            arg.base = p
            for page in range(len(lengths)):
                arg.offsets[page] = offsets[page]
                arg.lengths[page] = lengths[page]
                arg.initialized[page] = initialized[page]
            symbol = (
                "lighton_ragged_prefix_conv"
                if kind == "ragged"
                else "lighton_ragged_conv_normalize_cached_gates"
            )
            grid = (channels // 128, rows)
        args = [arg]
    else:
        args = [
            pointers[0],
            pointers[1],
            hptr[0],
            nexts[0][0],
            conv[0],
            C.c_int(seq),
            C.c_int(channels),
            C.c_int(1),
            *pointers[2:],
            cache[0],
            C.c_int(vh),
        ]
        symbol = (
            "lighton_conv_cached_gates"
            if kind == "cached"
            else "lighton_prefix_conv_normalize_cached_gates"
        )
        grid = (channels // 128, seq if kind == "prefix" else 1)
    outs = [conv, *nexts]
    refs = [reference, *nextref]
    if cached:
        outs.append(cache)
        refs.append(cache_ref)
    qualify(
        d,
        modules,
        (symbol, symbol),
        args,
        grid,
        outs,
        refs,
        f"gdn/{kind}/{seq}/{vh}",
        timing and seq == 17,
        threads=128,
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("baseline", type=Path)
    p.add_argument("candidate", type=Path)
    p.add_argument("--bench", action="store_true")
    a = p.parse_args()
    d = Device()
    modules = [d.module(path) for path in (a.baseline, a.candidate)]
    for vh in (16, 32):
        for seq in (1, 3, 17):
            for kind in (
                "cached",
                "prefix",
                "batch",
                "prefix_batch",
                "ragged",
                "ragged_norm",
            ):
                case(d, modules, kind, seq, vh, a.bench)


if __name__ == "__main__":
    main()
