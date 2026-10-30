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

"""Original/candidate paged chronological-attention qualification on CUDA.

Exercises both head widths, packed polar4/F16 keys, F32/F16 values, invalid
pages, sliding windows, empty/tail/long rows and mutable captured graph replay.
Uses independent dense dot/softmax references plus bitwise original parity.
Timings use the row fixture's fixed AA/AB/BA stream/graph protocol.
"""

import argparse
import array
import ctypes as C
import math
import struct
from pathlib import Path

from check_cuda_row_families import Device, benchmark, emit, f32, values


def floats(blob):
    x = array.array("f")
    x.frombytes(blob)
    return x


def qualify_case(d, modules, dim, key_format, value_format, sequence, run_bench):
    case_count = 0
    heads, kv_heads, page = 4, 2, 4
    blocks = max(1, (sequence + page - 1) // page)
    capacity = blocks * page
    table = list(reversed(range(blocks)))
    if blocks > 2:
        table[1] = 0xFFFFFFFF
    q = [f32(t / 8) for t in values(heads * dim, 1)]
    count = capacity * kv_heads * dim
    if key_format == 0:
        codes = [(i * 7 + i // 17) % 16 for i in range(count)]
        keys = [f32(f32(code / 7.5) - 1) for code in codes]
        key_data = bytes(codes[i] | codes[i + 1] << 4 for i in range(0, count, 2))
    else:
        key_data = b"".join(struct.pack("<e", t / 4) for t in values(count, 2))
        keys = [x[0] for x in struct.iter_unpack("<e", key_data)]
    if value_format == 0:
        vals = values(count, 3)
        value_data = array.array("f", vals).tobytes()
    else:
        value_data = b"".join(struct.pack("<e", t) for t in values(count, 3))
        vals = [x[0] for x in struct.iter_unpack("<e", value_data)]
    qp = d.alloc(array.array("f", q).tobytes())
    kp = d.alloc(key_data)
    vp = d.alloc(value_data)
    tp = d.alloc(array.array("I", table).tobytes())
    score_capacity = max(1, sequence)
    sentinel = array.array("f", [7.0] * (heads * score_capacity)).tobytes()
    sp = d.alloc(sentinel)
    out = d.alloc(bytes(heads * dim * 4))
    kv_offset = 7
    window = 31 if sequence == 128 else 0

    def scalars(length):
        return array.array(
            "I",
            [0, kv_offset + max(0, length - 1), length, length, kv_offset],
        ).tobytes()

    scalar = d.alloc(scalars(sequence))
    u = C.c_uint
    # Fallback values deliberately disagree with device scalars.
    common = [
        u(1),
        u(1),
        u(0),
        u(heads),
        u(kv_heads),
        u(dim),
        u(0),
        u(0),
        u(window),
        u(0),
    ]
    chunk = max(1, (sequence + 127) // 128)
    stage_args = (
        [sp, qp, kp, tp]
        + common
        + [
            u(len(key_data) // capacity),
            u(len(key_data) // capacity),
            u(blocks),
            u(page),
            u(key_format),
            u(capacity),
            u(score_capacity),
            u(chunk),
            u(128),
            scalar,
        ]
    )
    consume_args = (
        [out, sp, vp, tp]
        + common
        + [
            u(len(value_data) // capacity),
            u(blocks),
            u(page),
            u(value_format),
            u(capacity),
            u(score_capacity),
            scalar,
        ]
    )
    prefix = "antfly_gqa_attention_decode_turboquant_score_prework_"
    stages = [d.function(m, prefix + f"hd{dim}_f32_v1") for m in modules]

    def reference(length):
        end = length
        start = max(0, end - window) if window else 0
        visible = []
        for t in range(start, end):
            physical = table[t // page] * page + t % page
            if physical < capacity:
                visible.append((t, physical))
        ref_scores = {}
        ref_out = []
        for h in range(heads):
            logits = []
            for t, phys in visible:
                base = (phys * kv_heads + h // (heads // kv_heads)) * dim
                score = sum(
                    float(q[h * dim + c]) * keys[base + c] for c in range(dim)
                ) / math.sqrt(dim)
                ref_scores[h * score_capacity + t - start] = score
                logits.append(score)
            maximum = max(logits, default=0)
            weights = [math.exp(z - maximum) for z in logits]
            denom = sum(weights)
            for c in range(dim):
                ref_out.append(
                    sum(
                        w * vals[(phys * kv_heads + h // (heads // kv_heads)) * dim + c]
                        for w, (_, phys) in zip(weights, visible)
                    )
                    / denom
                    if denom
                    else 0
                )
        return ref_scores, ref_out

    def check_output(blob, length):
        _, expected = reference(length)
        for i, (a, b) in enumerate(zip(floats(blob), expected)):
            assert math.isfinite(a) and abs(a - b) <= 4e-5 * (1 + abs(b)), (
                dim,
                sequence,
                i,
                a,
                b,
            )

    label = f"hd{dim}/k{key_format}/v{value_format}/seq{sequence}"
    results = []
    for fn in stages:
        d.upload(sp, sentinel)
        d.launch(fn, stage_args, heads * 128, dim)
        d.synchronize()
        results.append(d.download(sp, len(sentinel)))
    assert results[0] == results[1], label + ": score bitwise mismatch"
    expected, _ = reference(sequence)
    actual = floats(results[1])
    for i, b in expected.items():
        assert abs(actual[i] - b) <= 3e-5 * (1 + abs(b)), (
            label,
            "score reference",
            i,
            actual[i],
            b,
        )
    emit(
        kind="correctness",
        case=label + "/score",
        bitwise=True,
        reference=True,
    )
    case_count += 1
    if run_bench and sequence == 128 and key_format == 0:
        benchmark(
            d,
            stages,
            stage_args,
            heads * 128,
            label + "/score",
            threads=dim,
        )
    for schedule in ("serial", "tiled64"):
        if schedule == "tiled64" and dim == 256 and sequence > 512:
            continue
        threads = dim if schedule == "serial" else 64
        grid = heads if schedule == "serial" else (heads, dim // 64)
        functions = [
            d.function(m, prefix + f"{schedule}_hd{dim}_f32_v1") for m in modules
        ]
        outputs = []
        for fn in functions:
            d.launch(fn, consume_args, grid, threads)
            d.synchronize()
            outputs.append(d.download(out, heads * dim * 4))
        assert outputs[0] == outputs[1], (
            label + "/" + schedule + ": consumer bitwise mismatch"
        )
        check_output(outputs[1], sequence)
        # Capture the complete two-stage program. Replay the same
        # graph with a shorter device count, then restore the count.
        graphs = []
        for stage, consumer in zip(stages, functions):

            def run(stage=stage, consumer=consumer, grid=grid, threads=threads):
                d.launch(stage, stage_args, heads * 128, dim)
                d.launch(consumer, consume_args, grid, threads)

            graphs.append(d.graph(run))
        for length in (sequence // 2, sequence):
            d.upload(scalar, scalars(length))
            outputs = []
            for graph, exe in graphs:
                d.call("cuGraphLaunch", exe, d.stream)
                d.synchronize()
                outputs.append(d.download(out, heads * dim * 4))
            assert outputs[0] == outputs[1], (
                label + "/" + schedule + ": mutable graph bitwise mismatch"
            )
            check_output(outputs[1], length)
        for graph, exe in graphs:
            d.call("cuGraphExecDestroy", exe)
            d.call("cuGraphDestroy", graph)
        emit(
            kind="correctness",
            case=label + "/" + schedule,
            bitwise=True,
            reference=True,
            mutable_graph_replays=2,
        )
        case_count += 1
        if run_bench and sequence == 128 and key_format == 0:
            benchmark(
                d,
                functions,
                consume_args,
                grid,
                label + "/" + schedule,
                threads=threads,
            )
    d.free_case()
    return case_count


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("baseline", type=Path)
    p.add_argument("candidate", type=Path)
    p.add_argument("--bench", action="store_true")
    opts = p.parse_args()
    d = Device()
    modules = [d.module(v / "kernels.cubin") for v in (opts.baseline, opts.candidate)]
    total = 0
    for dim in (256, 512):
        for key_format, value_format in ((0, 0), (2, 2)):
            for sequence in (0, 1, 17, 128, 513):
                total += qualify_case(
                    d, modules, dim, key_format, value_format, sequence, opts.bench
                )
    emit(kind="complete", cases=total)


if __name__ == "__main__":
    main()
