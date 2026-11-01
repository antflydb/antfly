#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bit-exact, captured AB/BA screen of isolated E4B L2 prefetch candidates.

Opt-in only. No runtime dispatch or build occurs. Inputs retain full production
matrix dimensions; rotating weight banks keep Q4 measurements larger than L2.
The original and hinted kernels have identical arithmetic and launch geometry.
"""
import argparse
import ctypes as C
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
import struct

from benchmark_cuda_rms_q8_1_prototype import Buffer, Cuda, P, U

GEOMETRY = {
    "pair": dict(blocks=80, outputs=10240, block_bytes=18, threads=256, tile=4, weights=2),
    "down": dict(blocks=320, outputs=2560, block_bytes=18, threads=320, tile=4, weights=1),
    "head": dict(blocks=10, outputs=262144, block_bytes=210, threads=160, tile=8, weights=1),
}


def weight_rows(kind, rows, rng, pattern):
    g = GEOMETRY[kind]
    result = bytearray()
    for _ in range(rows * g["blocks"]):
        scale = rng.choice((-0.03125, -0.0078125, 0.0, 0.015625, 0.0625))
        if pattern == "zero":
            scale = 0.0
        if kind == "head":
            block = bytearray(rng.randbytes(210))
            block[208:210] = struct.pack("<e", scale)
            # Q6_K group scales are signed int8. Bound them to keep all logits
            # finite while still exercising both signs and exact argmax ties.
            block[192:208] = bytes(rng.randrange(-8, 9) & 255 for _ in range(16))
        else:
            block = bytearray(struct.pack("<e", scale) + rng.randbytes(16))
        result.extend(block)
    return bytes(result)


def upload_weights(cuda, size, kind, pattern, seed):
    buffer = Buffer(cuda, size)
    row_size = GEOMETRY[kind]["blocks"] * GEOMETRY[kind]["block_bytes"]
    # A bounded host staging buffer avoids allocating a second 550 MB LM head.
    rows_per_chunk = 512
    rng = random.Random(seed)
    digest = hashlib.sha256()
    for row in range(0, GEOMETRY[kind]["outputs"], rows_per_chunk):
        data = weight_rows(kind, min(rows_per_chunk, GEOMETRY[kind]["outputs"] - row), rng, pattern)
        digest.update(data)
        cuda.upload(buffer.ptr + row * row_size, data)
    return buffer, digest.hexdigest()


def run_kind(args, kind):
    cuda = Cuda()
    graphs = []
    report = {"kind": kind, "device": cuda.device_name, "cases": [], "timings": []}
    try:
        major, minor = C.c_int(), C.c_int()
        cuda.check(cuda.cuDeviceGetAttribute(C.byref(major), 75, cuda.device), "major")
        cuda.check(cuda.cuDeviceGetAttribute(C.byref(minor), 76, cuda.device), "minor")
        if (major.value, minor.value) != (12, 1):
            raise RuntimeError("This experiment is scoped to GB10 SM12.1")
        module = cuda.load_module(args.cubin)
        g = GEOMETRY[kind]
        eligible = ["col1", "col2"] + (["k1"] if kind == "down" else [])
        candidates = [name for name in args.candidates if name in eligible]
        if not candidates:
            raise ValueError(f"No eligible candidates for {kind}")
        functions = {name: cuda.function(module, f"antfly_prefetch_{kind}_{name}") for name in ["baseline"] + candidates}
        weight_size = g["blocks"] * g["outputs"] * g["block_bytes"]
        banks = max(1, math.ceil(args.working_set_mb * 1024**2 / (weight_size * g["weights"])))
        report.update(geometry=g, banks=banks, weight_working_set_bytes=banks * weight_size * g["weights"])
        q8_blocks = 80 if kind == "head" else g["blocks"]
        source = Buffer(cuda, q8_blocks * 36)
        outputs = g["outputs"] // g["tile"] if kind == "head" else g["outputs"]
        result = Buffer(cuda, outputs * 4)
        indices = Buffer(cuda, outputs * 4) if kind == "head" else None
        weights = []
        for bank in range(banks):
            weights.append([upload_weights(cuda, weight_size, kind, "random", 100 + bank * 17 + w)[0] for w in range(g["weights"])])
        rng = random.Random(20261031)

        def operation(name, bank=0, activation=0):
            if kind == "head":
                arguments = [P(result.ptr), P(indices.ptr), P(source.ptr), P(weights[bank][0].ptr), P(0), U(1), U(2560), U(262144), U(0)]
            else:
                arguments = [P(result.ptr), P(source.ptr)] + [P(w.ptr) for w in weights[bank]]
                if kind == "pair":
                    arguments.append(U(activation))
            cuda.launch(functions[name], g["outputs"] // g["tile"], g["threads"], arguments)

        def output():
            cuda.synchronize()
            result.check()
            if indices:
                indices.check()
            return result.read() + (indices.read() if indices else b"")

        for pattern in ("random", "zero", "sparse", "small"):
            image = bytearray()
            for block in range(q8_blocks):
                values = [rng.randrange(-127, 128) for _ in range(32)]
                if pattern == "zero":
                    values = [0] * 32
                elif pattern == "sparse":
                    values = [v if i == block % 32 else 0 for i, v in enumerate(values)]
                d = 2**-20 if pattern == "small" else 0.015625
                image.extend(struct.pack("<ee", d, d * sum(values)))
                image.extend(bytes(v & 255 for v in values))
            cuda.upload(source.ptr, image)
            for activation in ((0, 2, 3, 4, 5) if kind == "pair" else (0,)):
                for bank in range(banks):
                    operation("baseline", bank, activation)
                    expected = output()
                    for candidate in candidates:
                        for _ in range(2):
                            result.reset()
                            if indices:
                                indices.reset()
                            operation(candidate, bank, activation)
                            actual = output()
                            if actual != expected:
                                first = next(i for i, (a, b) in enumerate(zip(expected, actual)) if a != b)
                                raise AssertionError(f"{kind}/{candidate}/{pattern}/bank{bank}/act{activation}: byte {first}")
                        report["cases"].append(dict(candidate=candidate, pattern=pattern, bank=bank, activation=activation, sha256=hashlib.sha256(expected).hexdigest(), bit_exact=True))
            if source.read() != bytes(image):
                raise AssertionError("source mutated")
            source.check()
        for bank in weights:
            for weight in bank:
                weight.check()
        # Round up the graph length so every bank occurs equally often.
        repeats = math.ceil(args.graph_kernels / banks) * banks
        for name in functions:
            counter = [0]

            def launch(name=name):
                operation(name, counter[0] % banks)
                counter[0] += 1

            graph = cuda.graph(launch, repeats)
            graphs.append(graph)
            functions[name] = graph
        for candidate in candidates:
            pairs = []
            for order in range(args.pairs):
                names = ("baseline", candidate) if order % 2 == 0 else (candidate, "baseline")
                observed = {name: cuda.graph_us(functions[name], repeats) for name in names}
                pairs.append(observed)
            base = statistics.median(p["baseline"] for p in pairs)
            hinted = statistics.median(p[candidate] for p in pairs)
            report["timings"].append(dict(candidate=candidate, baseline_us=base, candidate_us=hinted, speedup=base / hinted, paired_samples=pairs))
        report["pass"] = True
        return report
    finally:
        cuda.synchronize()
        for graph in graphs:
            cuda.graph_exec_destroy(graph)
        cuda.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cubin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kinds", nargs="+", choices=GEOMETRY, default=list(GEOMETRY))
    parser.add_argument("--candidates", nargs="+", choices=("col1", "col2", "k1"), default=["col1", "col2", "k1"])
    parser.add_argument("--working-set-mb", type=int, default=64)
    parser.add_argument("--graph-kernels", type=int, default=8)
    parser.add_argument("--pairs", type=int, default=6)
    args = parser.parse_args()
    if min(args.working_set_mb, args.graph_kernels, args.pairs) < 1:
        parser.error("positive sizes required")
    report = {"pass": False, "scope": "standalone prefetch-only experiment; no production routing", "cubin_sha256": hashlib.sha256(args.cubin.read_bytes()).hexdigest(), "results": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for kind in args.kinds:
        result = run_kind(args, kind)
        report["results"].append(result)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"kind": kind, "timings": result["timings"]}), flush=True)
    report["pass"] = True
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
