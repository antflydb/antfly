#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exact RMS+Q8_1 fusion differential against the published CUDA artifact.

Opt-in standalone experiment; no runtime dispatch is modified. Checks separate
F32, Q8_1 and optional BF16 outputs, guards, source immutability, determinism,
three residual/scale operations, and captured AB/BA timing. Tiled candidate is
out-of-place only. Ordinary candidate also checks F32-input output aliasing.
"""

from __future__ import annotations

import argparse
import ctypes as C
import hashlib
import json
import random
import statistics
import struct
from pathlib import Path

from benchmark_cuda_q4_pair_activation_prototype import Cuda as BaseCuda

U = C.c_uint
P = C.c_uint64
F = C.c_float
V = C.c_void_p
GUARD = 256
DIM = 2560


def require(condition, message):
    """Keep correctness and canary checks active under python -O."""
    if not condition:
        raise AssertionError(message)


class Cuda(BaseCuda):
    def __init__(self):
        self.lib = C.CDLL("libcuda.so.1")
        self._bind()
        self.check(self.cuInit(0), "cuInit")
        device = C.c_int()
        self.check(self.cuDeviceGet(C.byref(device), 0), "cuDeviceGet")
        self.device = device.value
        name = C.create_string_buffer(256)
        self.check(self.cuDeviceGetName(name, len(name), device), "cuDeviceGetName")
        self.device_name = name.value.decode()
        context = V()
        self.check(self.cuCtxCreate(C.byref(context), 0, device), "cuCtxCreate")
        self.context = context
        self.allocations = []
        self.modules = []

        def bind(name, arguments):
            fn = getattr(self.lib, name)
            fn.argtypes = arguments
            fn.restype = C.c_int
            return fn

        self.stream_create = bind("cuStreamCreate", [C.POINTER(V), U])
        self.stream_destroy = bind("cuStreamDestroy_v2", [V])
        self.capture_begin = bind("cuStreamBeginCapture_v2", [V, U])
        self.capture_end = bind("cuStreamEndCapture", [V, C.POINTER(V)])
        self.graph_instantiate = bind(
            "cuGraphInstantiateWithFlags", [C.POINTER(V), V, C.c_uint64]
        )
        self.graph_destroy = bind("cuGraphDestroy", [V])
        self.graph_exec_destroy = bind("cuGraphExecDestroy", [V])
        self.graph_launch = bind("cuGraphLaunch", [V, V])
        self.stream = V()
        self.check(self.stream_create(C.byref(self.stream), 1), "cuStreamCreate")

    def launch(self, function, grid_x, block_x, arguments):
        values = list(arguments)
        params = (V * len(values))(*(C.cast(C.byref(v), V) for v in values))
        self.check(
            self.cuLaunchKernel(
                function, grid_x, 1, 1, block_x, 1, 1, 0, self.stream, params, None
            ),
            "cuLaunchKernel",
        )

    def upload(self, pointer, data):
        super().upload(pointer, data)
        # Setup copies use the legacy stream, while kernel timing uses a
        # nonblocking stream. Establish the explicit dependency before launch.
        self.synchronize()

    def memset(self, pointer, value, size):
        super().memset(pointer, value, size)
        # Driver memset is asynchronous with respect to the host. Never race
        # output poison writes against the nonblocking experiment stream.
        self.synchronize()

    def graph(self, operation, repeats=128):
        graph = V()
        executable = V()
        self.check(self.capture_begin(self.stream, 0), "cuStreamBeginCapture")
        for _ in range(repeats):
            operation()
        self.check(self.capture_end(self.stream, C.byref(graph)), "cuStreamEndCapture")
        self.check(
            self.graph_instantiate(C.byref(executable), graph, 0), "cuGraphInstantiate"
        )
        self.graph_destroy(graph)
        return executable

    def graph_us(self, graph, repeats=128):
        start, stop = V(), V()
        self.check(self.cuEventCreate(C.byref(start), 0), "cuEventCreate")
        self.check(self.cuEventCreate(C.byref(stop), 0), "cuEventCreate")
        try:
            self.check(self.cuEventRecord(start, self.stream), "event start")
            for _ in range(8):
                self.check(self.graph_launch(graph, self.stream), "cuGraphLaunch")
            self.check(self.cuEventRecord(stop, self.stream), "event stop")
            self.check(self.cuEventSynchronize(stop), "event synchronize")
            elapsed = F()
            self.check(
                self.cuEventElapsedTime(C.byref(elapsed), start, stop), "event elapsed"
            )
            return elapsed.value * 1000 / (8 * repeats)
        finally:
            self.cuEventDestroy(start)
            self.cuEventDestroy(stop)

    def close(self):
        if getattr(self, "stream", None):
            self.stream_destroy(self.stream)
            self.stream = None
        super().close()


class Buffer:
    def __init__(self, cuda, size):
        self.cuda, self.size = cuda, size
        self.raw = cuda.alloc(size + 2 * GUARD)
        self.ptr = self.raw + GUARD
        self.reset()

    def reset(self):
        self.cuda.memset(self.raw, 0xA5, self.size + 2 * GUARD)

    def read(self):
        return self.cuda.download(self.ptr, self.size)

    def check(self):
        require(
            self.cuda.download(self.raw, GUARD) == bytes([0xA5]) * GUARD,
            "leading guard",
        )
        require(
            self.cuda.download(self.ptr + self.size, GUARD) == bytes([0xA5]) * GUARD,
            "trailing guard",
        )


def run(args):
    cuda = Cuda()
    report = {
        "device": cuda.device_name,
        "pass": False,
        "cases": [],
        "timing": [],
        "scope": "exact dimension2560 RMS+Q8_1 prototype; no production integration",
    }
    try:
        baseline_module = cuda.load_module(args.baseline)
        candidate_module = cuda.load_module(args.candidate)
        names = [
            "termite_rms_norm_f32",
            "termite_rms_norm_add_f32",
            "termite_rms_norm_add_output_scale_f32",
        ]
        canonical = [cuda.function(baseline_module, name) for name in names]
        canonical_bf16 = [
            cuda.function(baseline_module, name + "_bf16") for name in names[:2]
        ]
        quantize = cuda.function(baseline_module, "termite_quantize_f32_q8_1_rows")
        to_bf16 = cuda.function(baseline_module, "termite_f32_to_bf16")
        candidate_names = [
            "antfly_rms_q8_1_2560",
            "antfly_rms_add_q8_1_2560",
            "antfly_rms_add_scale_q8_1_2560",
        ]
        random_source = random.Random(271828)
        for rows in (1, 3, 17):
            count = rows * DIM
            source, weight, residual, scalar = [
                Buffer(cuda, n * 4) for n in (count, DIM, count, 1)
            ]
            f32, q8, mirror = [
                Buffer(cuda, n) for n in (count * 4, rows * 80 * 36, count * 2)
            ]
            ref_f32, ref_q8, ref_mirror = [
                Buffer(cuda, n) for n in (count * 4, rows * 80 * 36, count * 2)
            ]
            for pattern in ("random", "zero", "sparse", "cancellation", "small"):
                values = [random_source.uniform(-2, 2) for _ in range(count)]
                if pattern == "zero":
                    values = [0.0] * count
                if pattern == "sparse":
                    values = [v if i % 79 == 0 else 0.0 for i, v in enumerate(values)]
                if pattern == "cancellation":
                    values = [
                        ((-1) ** i) * (1 + i % 7 * 0.000001) for i in range(count)
                    ]
                if pattern == "small":
                    values = [v * 1e-18 for v in values]
                images = [
                    struct.pack(f"<{len(v)}f", *v)
                    for v in (
                        values,
                        [random_source.uniform(-1, 2) for _ in range(DIM)],
                        [random_source.uniform(-1, 1) for _ in range(count)],
                        [0.713],
                    )
                ]
                for b, data in zip((source, weight, residual, scalar), images):
                    cuda.upload(b.ptr, data)
                for op in range(3):
                    for mirrored in (False, True):

                        def reference():
                            arguments = [P(ref_f32.ptr)]
                            fused_mirror = mirrored and op < 2
                            if fused_mirror:
                                arguments.append(P(ref_mirror.ptr))
                            arguments += [P(source.ptr), P(weight.ptr)]
                            if op:
                                arguments.append(P(residual.ptr))
                            if op == 2:
                                arguments.append(P(scalar.ptr))
                            arguments += [U(rows), U(DIM), F(1e-6)]
                            cuda.launch(
                                canonical_bf16[op] if fused_mirror else canonical[op],
                                rows,
                                256,
                                arguments,
                            )
                            cuda.launch(
                                quantize,
                                (rows * 80 + 7) // 8,
                                256,
                                [P(ref_q8.ptr), P(ref_f32.ptr), U(rows), U(DIM)],
                            )
                            if mirrored and not fused_mirror:
                                cuda.launch(
                                    to_bf16,
                                    (count + 255) // 256,
                                    256,
                                    [P(ref_mirror.ptr), P(ref_f32.ptr), U(count)],
                                )

                        reference()
                        cuda.synchronize()
                        expected = [ref_f32.read(), ref_q8.read()] + (
                            [ref_mirror.read()] if mirrored else []
                        )
                        for tiled in (False, True):
                            function = cuda.function(
                                candidate_module,
                                candidate_names[op] + ("_tile256" if tiled else ""),
                            )

                            def candidate():
                                cuda.launch(
                                    function,
                                    rows * (10 if tiled else 1),
                                    256,
                                    [
                                        P(f32.ptr),
                                        P(q8.ptr),
                                        P(mirror.ptr if mirrored else 0),
                                        P(source.ptr),
                                        P(weight.ptr),
                                        P(residual.ptr),
                                        P(scalar.ptr),
                                        U(rows),
                                        F(1e-6),
                                    ],
                                )

                            for _ in range(2):
                                f32.reset()
                                q8.reset()
                                mirror.reset()
                                candidate()
                                cuda.synchronize()
                                actual = [f32.read(), q8.read()] + (
                                    [mirror.read()] if mirrored else []
                                )
                                for kind, wanted, observed in zip(
                                    ("f32", "q8_1", "bf16"), expected, actual
                                ):
                                    require(wanted == observed, (
                                        rows,
                                        pattern,
                                        op,
                                        mirrored,
                                        tiled,
                                        kind,
                                        sum(a != b for a, b in zip(wanted, observed)),
                                    ))
                            for b, data in zip(
                                (source, weight, residual, scalar), images
                            ):
                                require(b.read() == data, "readonly input changed")
                            for b in (
                                source,
                                weight,
                                residual,
                                scalar,
                                f32,
                                q8,
                                mirror,
                                ref_f32,
                                ref_q8,
                                ref_mirror,
                            ):
                                b.check()
                            report["cases"].append(
                                {
                                    "rows": rows,
                                    "pattern": pattern,
                                    "operation": op,
                                    "bf16": mirrored,
                                    "tiled": tiled,
                                    "bitwise": True,
                                }
                            )
                            if not tiled:
                                cuda.launch(
                                    function,
                                    rows,
                                    256,
                                    [
                                        P(source.ptr),
                                        P(q8.ptr),
                                        P(mirror.ptr if mirrored else 0),
                                        P(source.ptr),
                                        P(weight.ptr),
                                        P(residual.ptr),
                                        P(scalar.ptr),
                                        U(rows),
                                        F(1e-6),
                                    ],
                                )
                                cuda.synchronize()
                                require(source.read() == expected[0], "in-place F32 mismatch")
                                require(q8.read() == expected[1], "in-place Q8 mismatch")
                                if mirrored:
                                    require(mirror.read() == expected[2], "in-place BF16 mismatch")
                                cuda.upload(source.ptr, images[0])
                            if pattern == "random":
                                base_graph, candidate_graph = (
                                    cuda.graph(reference),
                                    cuda.graph(candidate),
                                )
                                try:
                                    cuda.graph_us(base_graph)
                                    cuda.graph_us(candidate_graph)
                                    base_times, candidate_times = [], []
                                    for sample in range(6):
                                        if sample % 2:
                                            candidate_times.append(
                                                cuda.graph_us(candidate_graph)
                                            )
                                            base_times.append(cuda.graph_us(base_graph))
                                        else:
                                            base_times.append(cuda.graph_us(base_graph))
                                            candidate_times.append(
                                                cuda.graph_us(candidate_graph)
                                            )
                                    base_us, candidate_us = (
                                        statistics.median(base_times),
                                        statistics.median(candidate_times),
                                    )
                                    result = {
                                        "rows": rows,
                                        "operation": op,
                                        "bf16": mirrored,
                                        "tiled": tiled,
                                        "baseline_us": base_us,
                                        "candidate_us": candidate_us,
                                        "speedup": base_us / candidate_us,
                                        "baseline_samples": base_times,
                                        "candidate_samples": candidate_times,
                                    }
                                    report["timing"].append(result)
                                    print(json.dumps(result), flush=True)
                                finally:
                                    cuda.graph_exec_destroy(base_graph)
                                    cuda.graph_exec_destroy(candidate_graph)
        report["pass"] = True
    finally:
        cuda.close()
        report["candidate_sha256"] = hashlib.sha256(
            args.candidate.read_bytes()
        ).hexdigest()
        report["baseline_sha256"] = hashlib.sha256(
            args.baseline.read_bytes()
        ).hexdigest()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"PASS: {len(report['cases'])} exact cases; {args.output}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    run(parser.parse_args())
