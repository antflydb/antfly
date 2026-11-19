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

"""Differential CUDA head checks; independent Transformer gates remain separate.

Use the pre-change and candidate SM89 bundles with the same Q6_K GGUF. Checks
preserve partial winner bits, indices, vocabulary tails, suppression and full
Q8-head logit bits. Timing alternates baseline/candidate order. This tool does
not establish end-to-end latency or replace the official model oracle.
"""

import argparse
import ctypes as c
import hashlib
import json
import statistics
import subprocess
from pathlib import Path

import numpy as np
from gguf import GGUFReader


class Cuda:
    def __init__(self):
        self.lib = c.CDLL("libcuda.so.1")
        signatures = {
            "cuInit": [c.c_uint],
            "cuDeviceGet": [c.POINTER(c.c_int), c.c_int],
            "cuCtxCreate_v2": [c.POINTER(c.c_void_p), c.c_uint, c.c_int],
            "cuCtxDestroy_v2": [c.c_void_p],
            "cuCtxSynchronize": [],
            "cuMemAlloc_v2": [c.POINTER(c.c_uint64), c.c_size_t],
            "cuMemFree_v2": [c.c_uint64],
            "cuMemcpyHtoD_v2": [c.c_uint64, c.c_void_p, c.c_size_t],
            "cuMemcpyDtoH_v2": [c.c_void_p, c.c_uint64, c.c_size_t],
            "cuModuleLoadDataEx": [
                c.POINTER(c.c_void_p),
                c.c_void_p,
                c.c_uint,
                c.c_void_p,
                c.c_void_p,
            ],
            "cuModuleGetFunction": [c.POINTER(c.c_void_p), c.c_void_p, c.c_char_p],
            "cuLaunchKernel": [
                c.c_void_p,
                *([c.c_uint] * 7),
                c.c_void_p,
                c.POINTER(c.c_void_p),
                c.c_void_p,
            ],
            "cuEventCreate": [c.POINTER(c.c_void_p), c.c_uint],
            "cuEventRecord": [c.c_void_p, c.c_void_p],
            "cuEventSynchronize": [c.c_void_p],
            "cuEventElapsedTime": [c.POINTER(c.c_float), c.c_void_p, c.c_void_p],
            "cuEventDestroy_v2": [c.c_void_p],
        }
        for name, args in signatures.items():
            fn = getattr(self.lib, name)
            fn.argtypes = args
            fn.restype = c.c_int
        self.call("cuInit", 0)
        device = c.c_int()
        self.call("cuDeviceGet", c.byref(device), 0)
        self.ctx = c.c_void_p()
        self.call("cuCtxCreate_v2", c.byref(self.ctx), 0, device)
        self.owned = []
        self.modules = []

    def call(self, name, *args):
        code = getattr(self.lib, name)(*args)
        if code:
            raise RuntimeError(f"{name}: CUDA error {code}")

    def array(self, values):
        values = np.ascontiguousarray(values)
        ptr = c.c_uint64()
        self.call("cuMemAlloc_v2", c.byref(ptr), values.nbytes)
        self.owned.append(ptr)
        self.upload(ptr, values)
        return ptr

    def upload(self, ptr, values):
        values = np.ascontiguousarray(values)
        self.call("cuMemcpyHtoD_v2", ptr, c.c_void_p(values.ctypes.data), values.nbytes)

    def read(self, ptr, count, dtype):
        values = np.empty(count, dtype=dtype)
        self.call("cuMemcpyDtoH_v2", c.c_void_p(values.ctypes.data), ptr, values.nbytes)
        return values

    def function(self, path, name):
        module = c.c_void_p()
        data = c.create_string_buffer(path.read_bytes())
        self.call("cuModuleLoadDataEx", c.byref(module), data, 0, None, None)
        self.modules.append(module)
        fn = c.c_void_p()
        self.call("cuModuleGetFunction", c.byref(fn), module, name.encode())
        return fn

    def launch(self, fn, args, blocks, threads):
        params = (c.c_void_p * len(args))(
            *[c.cast(c.byref(a), c.c_void_p) for a in args]
        )
        self.call(
            "cuLaunchKernel", fn, blocks, 1, 1, threads, 1, 1, 0, None, params, None
        )

    def time(self, fn, args, blocks, threads, iterations):
        for _ in range(4):
            self.launch(fn, args, blocks, threads)
        self.call("cuCtxSynchronize")
        start = c.c_void_p()
        end = c.c_void_p()
        self.call("cuEventCreate", c.byref(start), 0)
        self.call("cuEventCreate", c.byref(end), 0)
        try:
            self.call("cuEventRecord", start, None)
            for _ in range(iterations):
                self.launch(fn, args, blocks, threads)
            self.call("cuEventRecord", end, None)
            self.call("cuEventSynchronize", end)
            ms = c.c_float()
            self.call("cuEventElapsedTime", c.byref(ms), start, end)
            return ms.value / iterations
        finally:
            self.call("cuEventDestroy_v2", start)
            self.call("cuEventDestroy_v2", end)

    def close(self):
        for ptr in reversed(self.owned):
            self.call("cuMemFree_v2", ptr)
        self.call("cuCtxDestroy_v2", self.ctx)


def quantize(values):
    # Preparation is identical for both CUDA arms; this is not a test of the
    # production quantizer. Scale/sum are FP16, followed by 32 signed bytes.
    blocks = values.reshape(-1, 32)
    maximum = np.max(abs(blocks), axis=1)
    scale = maximum / 127
    inverse = np.divide(127, maximum, out=np.zeros_like(maximum), where=maximum != 0)
    integers = np.rint(blocks * inverse[:, None]).astype(np.int8)
    result = np.zeros((len(blocks), 36), np.uint8)
    result[:, :2] = (
        np.ascontiguousarray(scale.astype("<f2")).view(np.uint8).reshape(-1, 2)
    )
    result[:, 2:4] = (
        np.ascontiguousarray((scale * integers.sum(axis=1)).astype("<f2"))
        .view(np.uint8)
        .reshape(-1, 2)
    )
    result[:, 4:] = integers.view(np.uint8)
    return result


def identical(actual, expected, context):
    if not np.array_equal(actual.view(np.uint32), expected.view(np.uint32)):
        raise ValueError("Bitwise mismatch: " + context)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("gguf", "baseline-cubin", "candidate-cubin", "out"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--rounds", type=int, default=4)
    p.add_argument("--iterations", type=int, default=20)
    a = p.parse_args()
    if not 2 <= a.rounds <= 20 or not 1 <= a.iterations <= 100:
        p.error("Invalid timing bounds")
    if a.out.exists():
        p.error("Output must be new")
    if subprocess.check_output(
        ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True
    ).strip():
        raise RuntimeError("GPU busy")
    reader = GGUFReader(a.gguf)
    head = next(t for t in reader.tensors if t.name == "token_embd.weight")
    if int(head.tensor_type) != 14 or list(head.shape) != [2560, 262144]:
        raise ValueError("Expected E4B Q6_K tied head")
    cuda = Cuda()
    try:
        weight = cuda.array(head.data)
        pv = cuda.array(np.zeros(65536, np.float32))
        pi = cuda.array(np.zeros(65536, np.uint32))
        output = cuda.array(np.zeros(262144, np.float32))
        checks = []
        timings = {}
        paths = {"baseline": a.baseline_cubin, "candidate": a.candidate_cubin}
        names = {
            "f32": "termite_linear_q6_k_argmax_rows_stage1_tile16",
            "q8": "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b",
            "q8_generic": "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8",
            "logits": "termite_linear_q6_k_q8_1_f32_tile8_e4b",
        }
        functions = {
            arm: {key: cuda.function(path, name) for key, name in names.items()}
            for arm, path in paths.items()
        }
        rng = np.random.default_rng(17)
        shapes = [(1, 2560, 262144, [])] * 12 + [
            (1, 256, 1, []),
            (2, 512, 19, [-1, 0, 1, 18, 19]),
            (3, 2560, 33, list(range(33))),
            (2, 256, 15, list(range(0, 15, 2))),
        ]
        for trial, (rows, inner, outer, excluded) in enumerate(shapes):
            values = rng.normal(size=(rows, inner)).astype(np.float32)
            if trial == 0:
                values[:] = 0
            elif trial == 1:
                values.reshape(-1)[1:] = 0
            elif trial == 2:
                values *= 1e-4
            elif trial == 3:
                values *= 100
            x = cuda.array(values)
            q8 = cuda.array(quantize(values))
            suppress = cuda.array(np.array(excluded or [0], np.int32))
            for kind, source, columns, threads in [
                ("f32", x, 16, 256),
                (
                    "q8" if trial < 12 else "q8_generic",
                    q8,
                    8,
                    160 if trial < 12 else 256,
                ),
            ]:
                n = rows * ((outer + columns - 1) // columns)
                args = [
                    pv,
                    pi,
                    source,
                    weight,
                    suppress,
                    c.c_uint(rows),
                    c.c_uint(inner),
                    c.c_uint(outer),
                    c.c_uint(len(excluded)),
                ]
                found = {}
                for arm in paths:
                    # Check the first untouched output element after each launch.
                    cuda.upload(
                        c.c_uint64(pv.value + n * 4), np.array([0x7FC0D1FF], np.uint32)
                    )
                    cuda.upload(
                        c.c_uint64(pi.value + n * 4), np.array([0xDEADC0DE], np.uint32)
                    )
                    cuda.launch(functions[arm][kind], args, n, threads)
                    found[arm] = (
                        cuda.read(pv, n, np.float32),
                        cuda.read(pi, n, np.uint32),
                    )
                    if (
                        cuda.read(c.c_uint64(pv.value + n * 4), 1, np.uint32)[0]
                        != 0x7FC0D1FF
                        or cuda.read(c.c_uint64(pi.value + n * 4), 1, np.uint32)[0]
                        != 0xDEADC0DE
                    ):
                        raise ValueError("Output guard changed")
                for index in (0, 1):
                    identical(
                        found["candidate"][index],
                        found["baseline"][index],
                        f"{kind}/{trial}/{index}",
                    )
                checks.append(
                    {
                        "trial": trial,
                        "kind": kind,
                        "rows": rows,
                        "in_dim": inner,
                        "out_dim": outer,
                        "suppressed": excluded,
                        "bitwise_equal": True,
                    }
                )
            if trial < 4:
                args = [output, q8, weight, c.c_uint(outer)]
                found = {}
                for arm in paths:
                    cuda.launch(functions[arm]["logits"], args, outer // 8, 160)
                    found[arm] = cuda.read(output, outer, np.float32)
                identical(found["candidate"], found["baseline"], "all logits")
            print("passed", trial, rows, inner, outer, flush=True)
        values = rng.normal(size=2560).astype(np.float32)
        x = cuda.array(values)
        q8 = cuda.array(quantize(values))
        suppress = cuda.array(np.zeros(1, np.int32))
        for kind, source, columns, threads in [("f32", x, 16, 256), ("q8", q8, 8, 160)]:
            args = [
                pv,
                pi,
                source,
                weight,
                suppress,
                c.c_uint(1),
                c.c_uint(2560),
                c.c_uint(262144),
                c.c_uint(0),
            ]
            samples = {arm: [] for arm in paths}
            for round_index in range(a.rounds):
                order = list(paths) if round_index % 2 == 0 else list(reversed(paths))
                for arm in order:
                    samples[arm].append(
                        cuda.time(
                            functions[arm][kind],
                            args,
                            262144 // columns,
                            threads,
                            a.iterations,
                        )
                    )
            timings[kind] = {
                "stage1_ms": samples,
                "median_speedup": statistics.median(samples["baseline"])
                / statistics.median(samples["candidate"]),
            }
        result = {
            "scope": __doc__,
            "gpu": subprocess.check_output(
                [
                    "nvidia-smi",
                    "--query-gpu=name,driver_version,compute_cap",
                    "--format=csv,noheader",
                ],
                text=True,
            ).strip(),
            "artifacts": {
                arm: hashlib.sha256(path.read_bytes()).hexdigest()
                for arm, path in paths.items()
            },
            "head_tensor_sha256": hashlib.sha256(memoryview(head.data)).hexdigest(),
            "checks": checks,
            "all_logit_bitwise_trials": 4,
            "timing": timings,
        }
        a.out.write_text(json.dumps(result, indent=2) + "\n")
    finally:
        cuda.close()


if __name__ == "__main__":
    main()
