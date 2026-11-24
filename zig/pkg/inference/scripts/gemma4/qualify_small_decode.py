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

"""Check E4B small decode tiles against existing CUDA kernels bit for bit."""

import argparse
import ctypes as c
import hashlib
import json
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


# GGUF dimensions are innermost first. Each Q4_0 block stores 32 weights
# as a two-byte scale and sixteen packed bytes.
E4B_WEIGHT_SHAPES = {
    "ffn_gate": (2560, 10240),
    "ffn_up": (2560, 10240),
    "ffn_down": (10240, 2560),
    "inp_gate": (2560, 256),
    "proj": (256, 2560),
}


def checked_weights(reader):
    tensors = {t.name: t for t in reader.tensors}
    selected = {}
    for name, shape in E4B_WEIGHT_SHAPES.items():
        tensor = tensors[f"blk.0.{name}.weight"]
        if int(tensor.tensor_type) != 2:
            raise ValueError(f"Expected Q4_0: {tensor.name}")
        if tuple(tensor.shape) != shape:
            raise ValueError(
                f"{tensor.name}: expected GGUF dimensions {shape}, got {tuple(tensor.shape)}"
            )
        expected_bytes = shape[0] // 32 * 18 * shape[1]
        if tensor.data.nbytes != expected_bytes:
            raise ValueError(
                f"{tensor.name}: expected {expected_bytes} packed bytes, got {tensor.data.nbytes}"
            )
        selected[name] = tensor
    return selected


def identical(actual, expected, context):
    if not np.array_equal(actual.view(np.uint32), expected.view(np.uint32)):
        raise ValueError("Bitwise mismatch: " + context)


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["gguf", "baseline-cubin", "candidate-cubin", "out"]:
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        parser.error("Output must be new")
    if subprocess.check_output(
        ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True
    ).strip():
        raise RuntimeError("GPU busy")
    selected = checked_weights(GGUFReader(str(args.gguf)))
    gpu = Cuda()
    try:
        weights = {}
        hashes = {}
        for name, tensor in selected.items():
            weights[name] = gpu.array(tensor.data)
            hashes[tensor.name] = hashlib.sha256(tensor.data.tobytes()).hexdigest()
        rng = np.random.default_rng(72)
        checks = []
        for kind, weight_name, threads, oldcols, newcols, oldname in [
            (
                "ffn_pair",
                "ffn_gate",
                160,
                4,
                1,
                "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn",
            ),
            (
                "ffn_down",
                "ffn_down",
                256,
                4,
                1,
                "termite_linear_q4_0_q8_1_f32_tile4_w8",
            ),
            (
                "ple_gate",
                "inp_gate",
                256,
                4,
                1,
                "termite_linear_q4_0_q8_1_f32_tile4_w8",
            ),
            (
                "ple_projection",
                "proj",
                128,
                4,
                8,
                "termite_linear_q4_0_q8_1_f32_tile4",
            ),
        ]:
            indim, outdim = E4B_WEIGHT_SHAPES[weight_name]
            old = gpu.function(args.baseline_cubin, oldname)
            new = gpu.function(
                args.candidate_cubin, f"termite_gemma4_small_{kind}_q8_1_f32"
            )
            dst = gpu.array(np.zeros(outdim, np.float32))
            source = gpu.array(np.zeros(indim // 32 * 36, np.uint8))
            for trial in range(18):
                values = rng.normal(size=indim).astype(np.float32)
                if trial == 0:
                    values[:] = 0
                elif trial == 1:
                    values *= 1e-4
                elif trial == 2:
                    values *= 100
                gpu.upload(source, quantize(values))
                activation = c.c_uint(trial % 6)
                if kind == "ffn_pair":
                    parameters = [
                        dst,
                        source,
                        weights["ffn_gate"],
                        weights["ffn_up"],
                        activation,
                    ]
                else:
                    parameters = [dst, source, weights[weight_name]]
                old_parameters = (
                    parameters
                    if kind == "ffn_pair"
                    else [*parameters, c.c_uint(1), c.c_uint(indim), c.c_uint(outdim)]
                )
                new_parameters = (
                    parameters if kind.startswith("ffn") else old_parameters
                )
                gpu.launch(old, old_parameters, outdim // oldcols, threads)
                expected = gpu.read(dst, outdim, np.float32)
                # Every unwritten lane must differ, even for all-zero reference output.
                gpu.upload(dst, np.bitwise_not(expected.view(np.uint32)))
                gpu.launch(new, new_parameters, outdim // newcols, threads)
                identical(
                    gpu.read(dst, outdim, np.float32), expected, f"{kind}/{trial}"
                )
                checks.append(
                    {
                        "kind": kind,
                        "trial": trial,
                        "activation": activation.value if kind == "ffn_pair" else None,
                        "bitwise_equal": True,
                    }
                )
        args.out.write_text(
            json.dumps(
                {
                    "scope": "Layer-zero E4B Q4 weights and identical prepared Q8 bytes; "
                    "four kernel outputs compared bit for bit; no model or latency claim",
                    "artifacts_sha256": {
                        "baseline": digest(args.baseline_cubin),
                        "candidate": digest(args.candidate_cubin),
                    },
                    "gguf_sha256": digest(args.gguf),
                    "tensor_sha256": hashes,
                    "checks": checks,
                },
                indent=2,
            )
            + "\n"
        )
        print(f"{len(checks)} bitwise checks passed", flush=True)
    finally:
        gpu.close()


if __name__ == "__main__":
    main()
