#!/usr/bin/env python3
"""Qualify standalone SM89 Q4_0 pair-activation decode prototypes.

The checked-in canonical cubin and the standalone candidate cubin are loaded
as separate modules.  The harness checks exact Q8_1 output bytes, guards,
determinism, and read-only inputs before collecting alternating AB/BA timings.
It has no Python package dependencies and never touches runtime dispatch.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import math
import os
import random
import statistics
import struct
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable


IN_DIM = 1536
Q4_VALUES = 32
Q4_BYTES = 18
Q8_VALUES = 32
Q8_BYTES = 36
GUARD_BYTES = 256
CANARY = 0xA5
OUTPUT_POISON = 0xCD

ACTIVATIONS = {
    "gelu_new": 1,
    "silu": 2,
}

VARIANTS = {
    "c4_t384_fixed": 384,
    "c4_t384_scale_broadcast": 384,
    "c4_t192_fixed": 192,
    "c8_t384_fixed": 384,
}

PATTERNS = ("random", "cancellation", "sparse")
SHAPES = (6144, 12288)


class CudaError(RuntimeError):
    pass


class Cuda:
    """Minimal CUDA Driver API binding used only by this experiment."""

    def __init__(self) -> None:
        self.lib = ctypes.CDLL("libcuda.so.1")
        self._bind()
        self.check(self.cuInit(0), "cuInit")
        device = ctypes.c_int()
        self.check(self.cuDeviceGet(ctypes.byref(device), 0), "cuDeviceGet")
        self.device = device.value
        name = ctypes.create_string_buffer(256)
        self.check(
            self.cuDeviceGetName(name, len(name), device), "cuDeviceGetName"
        )
        self.device_name = name.value.decode("utf-8", errors="replace")
        major = ctypes.c_int()
        minor = ctypes.c_int()
        self.check(
            self.cuDeviceGetAttribute(ctypes.byref(major), 75, device),
            "cuDeviceGetAttribute(compute-major)",
        )
        self.check(
            self.cuDeviceGetAttribute(ctypes.byref(minor), 76, device),
            "cuDeviceGetAttribute(compute-minor)",
        )
        self.compute_capability = f"{major.value}.{minor.value}"
        if major.value != 8 or minor.value != 9:
            raise CudaError(
                "prototype qualification requires SM89; found "
                f"{self.compute_capability}"
            )
        driver_version = ctypes.c_int()
        self.check(
            self.cuDriverGetVersion(ctypes.byref(driver_version)),
            "cuDriverGetVersion",
        )
        self.driver_version = driver_version.value
        context = ctypes.c_void_p()
        self.check(
            self.cuCtxCreate(ctypes.byref(context), 0, device),
            "cuCtxCreate_v2",
        )
        self.context = context
        self.allocations: list[int] = []
        self.modules: list[ctypes.c_void_p] = []

    def _bind(self) -> None:
        def bind(name: str, args: list[object], result: object = ctypes.c_int):
            fn = getattr(self.lib, name)
            fn.argtypes = args
            fn.restype = result
            return fn

        self.cuInit = bind("cuInit", [ctypes.c_uint])
        self.cuDriverGetVersion = bind(
            "cuDriverGetVersion", [ctypes.POINTER(ctypes.c_int)]
        )
        self.cuDeviceGet = bind(
            "cuDeviceGet", [ctypes.POINTER(ctypes.c_int), ctypes.c_int]
        )
        self.cuDeviceGetName = bind(
            "cuDeviceGetName", [ctypes.c_char_p, ctypes.c_int, ctypes.c_int]
        )
        self.cuDeviceGetAttribute = bind(
            "cuDeviceGetAttribute",
            [ctypes.POINTER(ctypes.c_int), ctypes.c_int, ctypes.c_int],
        )
        self.cuCtxCreate = bind(
            "cuCtxCreate_v2",
            [ctypes.POINTER(ctypes.c_void_p), ctypes.c_uint, ctypes.c_int],
        )
        self.cuCtxDestroy = bind("cuCtxDestroy_v2", [ctypes.c_void_p])
        self.cuCtxSynchronize = bind("cuCtxSynchronize", [])
        self.cuModuleLoad = bind(
            "cuModuleLoad",
            [ctypes.POINTER(ctypes.c_void_p), ctypes.c_char_p],
        )
        self.cuModuleUnload = bind("cuModuleUnload", [ctypes.c_void_p])
        self.cuModuleGetFunction = bind(
            "cuModuleGetFunction",
            [ctypes.POINTER(ctypes.c_void_p), ctypes.c_void_p, ctypes.c_char_p],
        )
        self.cuMemAlloc = bind(
            "cuMemAlloc_v2",
            [ctypes.POINTER(ctypes.c_uint64), ctypes.c_size_t],
        )
        self.cuMemFree = bind("cuMemFree_v2", [ctypes.c_uint64])
        self.cuMemcpyHtoD = bind(
            "cuMemcpyHtoD_v2",
            [ctypes.c_uint64, ctypes.c_void_p, ctypes.c_size_t],
        )
        self.cuMemcpyDtoH = bind(
            "cuMemcpyDtoH_v2",
            [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_size_t],
        )
        self.cuMemsetD8 = bind(
            "cuMemsetD8_v2",
            [ctypes.c_uint64, ctypes.c_ubyte, ctypes.c_size_t],
        )
        self.cuLaunchKernel = bind(
            "cuLaunchKernel",
            [
                ctypes.c_void_p,
                ctypes.c_uint,
                ctypes.c_uint,
                ctypes.c_uint,
                ctypes.c_uint,
                ctypes.c_uint,
                ctypes.c_uint,
                ctypes.c_uint,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_void_p),
                ctypes.POINTER(ctypes.c_void_p),
            ],
        )
        self.cuEventCreate = bind(
            "cuEventCreate", [ctypes.POINTER(ctypes.c_void_p), ctypes.c_uint]
        )
        self.cuEventRecord = bind(
            "cuEventRecord", [ctypes.c_void_p, ctypes.c_void_p]
        )
        self.cuEventSynchronize = bind(
            "cuEventSynchronize", [ctypes.c_void_p]
        )
        self.cuEventElapsedTime = bind(
            "cuEventElapsedTime",
            [
                ctypes.POINTER(ctypes.c_float),
                ctypes.c_void_p,
                ctypes.c_void_p,
            ],
        )
        self.cuEventDestroy = bind("cuEventDestroy_v2", [ctypes.c_void_p])
        self.cuGetErrorName = bind(
            "cuGetErrorName", [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
        )
        self.cuGetErrorString = bind(
            "cuGetErrorString", [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
        )

    def check(self, code: int, operation: str) -> None:
        if code == 0:
            return
        name = ctypes.c_char_p()
        detail = ctypes.c_char_p()
        self.cuGetErrorName(code, ctypes.byref(name))
        self.cuGetErrorString(code, ctypes.byref(detail))
        error_name = name.value.decode() if name.value else str(code)
        error_detail = (
            detail.value.decode() if detail.value else "unknown CUDA error"
        )
        raise CudaError(f"{operation}: {error_name}: {error_detail}")

    def load_module(self, path: Path) -> ctypes.c_void_p:
        module = ctypes.c_void_p()
        self.check(
            self.cuModuleLoad(ctypes.byref(module), os.fsencode(path)),
            f"cuModuleLoad({path})",
        )
        self.modules.append(module)
        return module

    def function(self, module: ctypes.c_void_p, name: str) -> ctypes.c_void_p:
        function = ctypes.c_void_p()
        self.check(
            self.cuModuleGetFunction(
                ctypes.byref(function), module, name.encode("ascii")
            ),
            f"cuModuleGetFunction({name})",
        )
        return function

    def alloc(self, size: int) -> int:
        pointer = ctypes.c_uint64()
        self.check(self.cuMemAlloc(ctypes.byref(pointer), size), "cuMemAlloc_v2")
        self.allocations.append(pointer.value)
        return pointer.value

    def upload(self, pointer: int, data: bytes | bytearray) -> None:
        image = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
        self.check(
            self.cuMemcpyHtoD(
                pointer, ctypes.cast(image, ctypes.c_void_p), len(data)
            ),
            "cuMemcpyHtoD_v2",
        )

    def download(self, pointer: int, size: int) -> bytes:
        image = (ctypes.c_ubyte * size)()
        self.check(
            self.cuMemcpyDtoH(
                ctypes.cast(image, ctypes.c_void_p), pointer, size
            ),
            "cuMemcpyDtoH_v2",
        )
        return bytes(image)

    def memset(self, pointer: int, value: int, size: int) -> None:
        self.check(
            self.cuMemsetD8(pointer, value, size), "cuMemsetD8_v2"
        )

    def launch(
        self,
        function: ctypes.c_void_p,
        grid_x: int,
        block_x: int,
        arguments: Iterable[ctypes._SimpleCData],
    ) -> None:
        values = list(arguments)
        params = (ctypes.c_void_p * len(values))(
            *(
                ctypes.cast(ctypes.byref(value), ctypes.c_void_p)
                for value in values
            )
        )
        self.check(
            self.cuLaunchKernel(
                function,
                grid_x,
                1,
                1,
                block_x,
                1,
                1,
                0,
                None,
                params,
                None,
            ),
            "cuLaunchKernel",
        )

    def synchronize(self) -> None:
        self.check(self.cuCtxSynchronize(), "cuCtxSynchronize")

    def time(self, launches: int, operation: Callable[[], None]) -> float:
        start = ctypes.c_void_p()
        stop = ctypes.c_void_p()
        self.check(self.cuEventCreate(ctypes.byref(start), 0), "cuEventCreate")
        try:
            self.check(
                self.cuEventCreate(ctypes.byref(stop), 0), "cuEventCreate"
            )
            try:
                self.check(self.cuEventRecord(start, None), "cuEventRecord(start)")
                for _ in range(launches):
                    operation()
                self.check(self.cuEventRecord(stop, None), "cuEventRecord(stop)")
                self.check(
                    self.cuEventSynchronize(stop), "cuEventSynchronize(stop)"
                )
                elapsed = ctypes.c_float()
                self.check(
                    self.cuEventElapsedTime(
                        ctypes.byref(elapsed), start, stop
                    ),
                    "cuEventElapsedTime",
                )
                return float(elapsed.value) * 1000.0 / launches
            finally:
                if stop.value:
                    self.cuEventDestroy(stop)
        finally:
            if start.value:
                self.cuEventDestroy(start)

    def close(self) -> None:
        if not getattr(self, "context", None):
            return
        self.cuCtxSynchronize()
        for pointer in reversed(self.allocations):
            self.cuMemFree(pointer)
        for module in reversed(self.modules):
            self.cuModuleUnload(module)
        self.cuCtxDestroy(self.context)
        self.allocations.clear()
        self.modules.clear()
        self.context = None


@dataclass
class Buffers:
    q8_input: int
    weight_gate: int
    weight_up: int
    reference_output_base: int
    candidate_output_base: int
    output_bytes: int
    weight_bytes: int

    @property
    def reference_output(self) -> int:
        return self.reference_output_base + GUARD_BYTES

    @property
    def candidate_output(self) -> int:
        return self.candidate_output_base + GUARD_BYTES


def sha256_bytes(data: bytes | bytearray) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_csv(
    value: str, allowed: Iterable[str], option_name: str
) -> list[str]:
    allowed_set = set(allowed)
    if value == "all":
        return list(allowed)
    result = [part.strip() for part in value.split(",") if part.strip()]
    if not result or any(part not in allowed_set for part in result):
        raise argparse.ArgumentTypeError(
            f"{option_name} must be all or a comma-separated subset of "
            + ",".join(allowed)
        )
    return result


def half_bytes(value: float) -> bytes:
    return struct.pack("<e", value)


def make_q8_input(pattern: str, seed: int) -> bytes:
    rng = random.Random(seed ^ 0x243F6A8885A308D3)
    result = bytearray((IN_DIM // Q8_VALUES) * Q8_BYTES)
    for block in range(IN_DIM // Q8_VALUES):
        offset = block * Q8_BYTES
        if pattern == "random":
            scale = (1.0 + (block % 11) * 0.125) / 128.0
            values = [rng.randrange(-127, 128) for _ in range(Q8_VALUES)]
        elif pattern == "cancellation":
            scale = (1.0 + (block % 5) * 0.25) / 256.0
            values = [
                (91 - (lane % 13)) * (1 if ((lane + block) & 1) == 0 else -1)
                for lane in range(Q8_VALUES)
            ]
        elif pattern == "sparse":
            scale = 0.0 if block % 7 == 0 else (block % 9 + 1) / 512.0
            values = [
                0 if (lane + block) % 5 else (127 if lane & 1 else -127)
                for lane in range(Q8_VALUES)
            ]
        else:
            raise ValueError(f"unknown pattern: {pattern}")
        result[offset : offset + 2] = half_bytes(scale)
        result[offset + 2 : offset + 4] = b"\x00\x00"
        result[offset + 4 : offset + Q8_BYTES] = bytes(
            value & 0xFF for value in values
        )
    return bytes(result)


def make_q4_weights(out_dim: int, pattern: str, seed: int) -> bytes:
    blocks = out_dim * (IN_DIM // Q4_VALUES)
    rng = random.Random(seed)
    result = bytearray(rng.randbytes(blocks * Q4_BYTES))
    for block in range(blocks):
        if pattern == "random":
            scale = (1.0 + ((block * 13) % 17) * 0.0625) / 256.0
            if block & 1:
                scale = -scale
        elif pattern == "cancellation":
            scale = (1.0 + (block % 3) * 0.5) / 512.0
            if (block // 16) & 1:
                scale = -scale
        elif pattern == "sparse":
            scale = 0.0 if block % 11 == 0 else (block % 7 + 1) / 1024.0
        else:
            raise ValueError(f"unknown pattern: {pattern}")
        struct.pack_into("<e", result, block * Q4_BYTES, scale)
        if pattern == "cancellation":
            payload = 0xF0 if block & 1 else 0x0F
            result[
                block * Q4_BYTES + 2 : (block + 1) * Q4_BYTES
            ] = bytes([payload]) * 16
        elif pattern == "sparse" and block % 5:
            result[
                block * Q4_BYTES + 2 : (block + 1) * Q4_BYTES
            ] = b"\x88" * 16
    return bytes(result)


def output_symbol(out_dim: int, variant: str) -> str:
    return (
        "antfly_q4_0_pair_activation_q8_1_e2b_"
        f"{out_dim}_{variant}_prototype"
    )


def reference_symbol(out_dim: int) -> str:
    return (
        "antfly_q4_0_pair_activation_q8_1_e2b_"
        f"{out_dim}_mmv_v1"
    )


def allocate_buffers(cuda: Cuda, out_dim: int) -> Buffers:
    input_bytes = (IN_DIM // Q8_VALUES) * Q8_BYTES
    weight_bytes = out_dim * (IN_DIM // Q4_VALUES) * Q4_BYTES
    output_bytes = (out_dim // Q8_VALUES) * Q8_BYTES
    guarded_output_bytes = output_bytes + 2 * GUARD_BYTES
    return Buffers(
        q8_input=cuda.alloc(input_bytes),
        weight_gate=cuda.alloc(weight_bytes),
        weight_up=cuda.alloc(weight_bytes),
        reference_output_base=cuda.alloc(guarded_output_bytes),
        candidate_output_base=cuda.alloc(guarded_output_bytes),
        output_bytes=output_bytes,
        weight_bytes=weight_bytes,
    )


def prepare_output(cuda: Cuda, base: int, output_bytes: int) -> None:
    cuda.memset(base, CANARY, output_bytes + 2 * GUARD_BYTES)
    cuda.memset(base + GUARD_BYTES, OUTPUT_POISON, output_bytes)


def launch_arguments(
    output: int,
    buffers: Buffers,
    activation: int,
    out_dim: int,
) -> list[ctypes._SimpleCData]:
    return [
        ctypes.c_uint64(output),
        ctypes.c_uint64(buffers.q8_input),
        ctypes.c_uint64(buffers.weight_gate),
        ctypes.c_uint64(buffers.weight_up),
        ctypes.c_uint32(activation),
        ctypes.c_uint32(1),
        ctypes.c_uint32(IN_DIM),
        ctypes.c_uint32(out_dim),
    ]


def validate_guarded_output(image: bytes, output_bytes: int) -> dict[str, object]:
    prefix = image[:GUARD_BYTES]
    body = image[GUARD_BYTES : GUARD_BYTES + output_bytes]
    suffix = image[GUARD_BYTES + output_bytes :]
    prefix_ok = prefix == bytes([CANARY]) * GUARD_BYTES
    suffix_ok = suffix == bytes([CANARY]) * GUARD_BYTES
    poison_block = bytes([OUTPUT_POISON]) * Q8_BYTES
    unwritten = [
        block
        for block in range(output_bytes // Q8_BYTES)
        if body[block * Q8_BYTES : (block + 1) * Q8_BYTES] == poison_block
    ]
    headers_ok = all(
        body[block * Q8_BYTES + 2 : block * Q8_BYTES + 4] == b"\x00\x00"
        for block in range(output_bytes // Q8_BYTES)
    )
    return {
        "prefix_ok": prefix_ok,
        "suffix_ok": suffix_ok,
        "headers_ok": headers_ok,
        "unwritten_block_count": len(unwritten),
        "first_unwritten_block": unwritten[0] if unwritten else None,
        "ok": prefix_ok and suffix_ok and headers_ok and not unwritten,
        "body": body,
    }


def dequantize_q8(image: bytes) -> list[float]:
    result: list[float] = []
    for offset in range(0, len(image), Q8_BYTES):
        scale = float(struct.unpack_from("<e", image, offset)[0])
        for raw in image[offset + 4 : offset + Q8_BYTES]:
            signed = raw if raw < 128 else raw - 256
            result.append(scale * signed)
    return result


def compare_outputs(reference: bytes, candidate: bytes) -> dict[str, object]:
    mismatches = [
        index
        for index, (expected, actual) in enumerate(zip(reference, candidate))
        if expected != actual
    ]
    reference_f32 = dequantize_q8(reference)
    candidate_f32 = dequantize_q8(candidate)
    errors = [
        abs(expected - actual)
        for expected, actual in zip(reference_f32, candidate_f32)
    ]
    rms = math.sqrt(sum(error * error for error in errors) / len(errors))
    return {
        "byte_mismatch_count": len(mismatches),
        "first_byte_mismatch": mismatches[0] if mismatches else None,
        "reference_byte": reference[mismatches[0]] if mismatches else None,
        "candidate_byte": candidate[mismatches[0]] if mismatches else None,
        "max_abs_dequantized": max(errors, default=0.0),
        "rms_dequantized": rms,
        "exact": not mismatches,
    }


def launch_once(
    cuda: Cuda,
    function: ctypes.c_void_p,
    threads: int,
    output: int,
    buffers: Buffers,
    activation: int,
    out_dim: int,
) -> None:
    cuda.launch(
        function,
        out_dim // Q8_VALUES,
        threads,
        launch_arguments(output, buffers, activation, out_dim),
    )


def run_guarded(
    cuda: Cuda,
    function: ctypes.c_void_p,
    threads: int,
    output_base: int,
    output: int,
    buffers: Buffers,
    activation: int,
    out_dim: int,
) -> tuple[bytes, dict[str, object]]:
    prepare_output(cuda, output_base, buffers.output_bytes)
    launch_once(
        cuda, function, threads, output, buffers, activation, out_dim
    )
    cuda.synchronize()
    image = cuda.download(
        output_base, buffers.output_bytes + 2 * GUARD_BYTES
    )
    validation = validate_guarded_output(image, buffers.output_bytes)
    body = validation.pop("body")
    assert isinstance(body, bytes)
    return body, validation


def coefficient_of_variation(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = statistics.fmean(values)
    if mean == 0.0 or len(values) == 1:
        return 0.0
    return statistics.stdev(values) / mean


def summarize_timings(values: list[float]) -> dict[str, object]:
    ordered = sorted(values)
    return {
        "samples_us": values,
        "median_us": statistics.median(values),
        "mean_us": statistics.fmean(values),
        "min_us": ordered[0],
        "max_us": ordered[-1],
        "cv": coefficient_of_variation(values),
    }


def time_ab_ba(
    cuda: Cuda,
    reference: ctypes.c_void_p,
    candidate: ctypes.c_void_p,
    candidate_threads: int,
    buffers: Buffers,
    activation: int,
    out_dim: int,
    warmup: int,
    iterations: int,
    pairs: int,
) -> dict[str, object]:
    reference_launch = lambda: launch_once(
        cuda,
        reference,
        384,
        buffers.reference_output,
        buffers,
        activation,
        out_dim,
    )
    candidate_launch = lambda: launch_once(
        cuda,
        candidate,
        candidate_threads,
        buffers.candidate_output,
        buffers,
        activation,
        out_dim,
    )
    for _ in range(warmup):
        reference_launch()
        candidate_launch()
    cuda.synchronize()
    reference_times: list[float] = []
    candidate_times: list[float] = []
    orders: list[str] = []
    for pair in range(pairs):
        if pair & 1:
            orders.append("BA")
            candidate_times.append(cuda.time(iterations, candidate_launch))
            reference_times.append(cuda.time(iterations, reference_launch))
        else:
            orders.append("AB")
            reference_times.append(cuda.time(iterations, reference_launch))
            candidate_times.append(cuda.time(iterations, candidate_launch))
    reference_summary = summarize_timings(reference_times)
    candidate_summary = summarize_timings(candidate_times)
    reference_median = float(reference_summary["median_us"])
    candidate_median = float(candidate_summary["median_us"])
    return {
        "orders": orders,
        "warmup_launches": warmup,
        "launches_per_sample": iterations,
        "reference": reference_summary,
        "candidate": candidate_summary,
        "speedup": reference_median / candidate_median,
        "latency_ratio": candidate_median / reference_median,
    }


def self_test() -> None:
    for pattern in PATTERNS:
        q8 = make_q8_input(pattern, 7)
        assert len(q8) == (IN_DIM // Q8_VALUES) * Q8_BYTES
        q4 = make_q4_weights(32, pattern, 11)
        assert len(q4) == (IN_DIM // Q4_VALUES) * Q4_BYTES * 32
    block = half_bytes(0.5) + b"\x00\x00" + bytes(range(32))
    values = dequantize_q8(block)
    assert values[0] == 0.0 and values[-1] == 15.5
    exact = compare_outputs(block, block)
    assert exact["exact"] and exact["rms_dequantized"] == 0.0
    changed = bytearray(block)
    changed[-1] ^= 1
    different = compare_outputs(block, bytes(changed))
    assert not different["exact"] and different["byte_mismatch_count"] == 1
    print("self-test: PASS")


def build_parser() -> argparse.ArgumentParser:
    inference_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate-cubin",
        type=Path,
        default=Path("/tmp/antfly-cuda-q4-pair-prototype/q4_pair_sm89.cubin"),
    )
    parser.add_argument(
        "--baseline-cubin",
        type=Path,
        default=inference_dir
        / "src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin",
    )
    parser.add_argument(
        "--suite", choices=("all", "correctness", "timing"), default="all"
    )
    parser.add_argument(
        "--shapes", default="all", help="all or comma-separated 6144,12288"
    )
    parser.add_argument(
        "--variants",
        default="all",
        help="all or comma-separated prototype variant names",
    )
    parser.add_argument(
        "--patterns",
        default="all",
        help="all or comma-separated random,cancellation,sparse",
    )
    parser.add_argument(
        "--activations",
        default="all",
        help="all or comma-separated gelu_new,silu",
    )
    parser.add_argument("--seed", type=lambda value: int(value, 0), default=0x6A09E667)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--timing-pairs", type=int, default=7)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--self-test", action="store_true")
    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.self_test:
        self_test()
        return 0
    if args.warmup < 0 or args.iterations <= 0 or args.timing_pairs < 3:
        parser.error("warmup must be nonnegative; iterations > 0; timing-pairs >= 3")
    try:
        shapes = [
            int(value)
            for value in parse_csv(args.shapes, ("6144", "12288"), "--shapes")
        ]
        variants = parse_csv(args.variants, VARIANTS, "--variants")
        patterns = parse_csv(args.patterns, PATTERNS, "--patterns")
        activations = parse_csv(
            args.activations, ACTIVATIONS, "--activations"
        )
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))
    for path in (args.candidate_cubin, args.baseline_cubin):
        if not path.is_file():
            parser.error(f"cubin is unavailable: {path}")

    harness_path = Path(__file__).resolve()
    inference_dir = harness_path.parents[1]
    source_path = (
        inference_dir
        / "src/ops/cuda/prototypes/q4_0_pair_activation_q8_1_sm89.cu"
    )
    if not source_path.is_file():
        parser.error(f"prototype source is unavailable: {source_path}")

    evidence: dict[str, object] = {
        "schema_version": 1,
        "experiment": "standalone_sm89_q4_0_pair_activation_q8_1",
        "prototype_source": str(source_path),
        "prototype_source_sha256": sha256_file(source_path),
        "harness": str(harness_path),
        "harness_sha256": sha256_file(harness_path),
        "candidate_cubin": str(args.candidate_cubin.resolve()),
        "candidate_sha256": sha256_file(args.candidate_cubin),
        "baseline_cubin": str(args.baseline_cubin.resolve()),
        "baseline_sha256": sha256_file(args.baseline_cubin),
        "config": {
            "suite": args.suite,
            "shapes": shapes,
            "variants": variants,
            "patterns": patterns,
            "activations": activations,
            "seed": args.seed,
            "warmup": args.warmup,
            "iterations": args.iterations,
            "timing_pairs": args.timing_pairs,
        },
        "correctness": [],
        "timing": [],
    }
    cuda: Cuda | None = None
    try:
        cuda = Cuda()
        evidence["device"] = {
            "name": cuda.device_name,
            "compute_capability": cuda.compute_capability,
            "driver_version": cuda.driver_version,
        }
        baseline_module = cuda.load_module(args.baseline_cubin)
        candidate_module = cuda.load_module(args.candidate_cubin)
        references = {
            out_dim: cuda.function(baseline_module, reference_symbol(out_dim))
            for out_dim in shapes
        }
        candidates = {
            (out_dim, variant): cuda.function(
                candidate_module, output_symbol(out_dim, variant)
            )
            for out_dim in shapes
            for variant in variants
        }

        correctness = evidence["correctness"]
        timings = evidence["timing"]
        assert isinstance(correctness, list) and isinstance(timings, list)
        all_exact = True
        all_integrity = True
        if args.suite in ("all", "correctness"):
            for out_dim in shapes:
                buffers = allocate_buffers(cuda, out_dim)
                for pattern_index, pattern in enumerate(patterns):
                    case_seed = args.seed ^ (out_dim << 17) ^ (pattern_index << 9)
                    q8_input = make_q8_input(pattern, case_seed)
                    gate = make_q4_weights(
                        out_dim, pattern, case_seed ^ 0xBB67AE85
                    )
                    up = make_q4_weights(
                        out_dim, pattern, case_seed ^ 0x3C6EF372
                    )
                    cuda.upload(buffers.q8_input, q8_input)
                    cuda.upload(buffers.weight_gate, gate)
                    cuda.upload(buffers.weight_up, up)
                    for activation_name in activations:
                        activation = ACTIVATIONS[activation_name]
                        reference_body, reference_guard = run_guarded(
                            cuda,
                            references[out_dim],
                            384,
                            buffers.reference_output_base,
                            buffers.reference_output,
                            buffers,
                            activation,
                            out_dim,
                        )
                        for variant in variants:
                            candidate_body, candidate_guard = run_guarded(
                                cuda,
                                candidates[(out_dim, variant)],
                                VARIANTS[variant],
                                buffers.candidate_output_base,
                                buffers.candidate_output,
                                buffers,
                                activation,
                                out_dim,
                            )
                            second_body, second_guard = run_guarded(
                                cuda,
                                candidates[(out_dim, variant)],
                                VARIANTS[variant],
                                buffers.candidate_output_base,
                                buffers.candidate_output,
                                buffers,
                                activation,
                                out_dim,
                            )
                            comparison = compare_outputs(
                                reference_body, candidate_body
                            )
                            deterministic = candidate_body == second_body
                            integrity = bool(reference_guard["ok"]) and bool(
                                candidate_guard["ok"]
                            ) and bool(second_guard["ok"])
                            all_exact = all_exact and bool(comparison["exact"])
                            all_integrity = all_integrity and integrity and deterministic
                            correctness.append(
                                {
                                    "out_dim": out_dim,
                                    "pattern": pattern,
                                    "activation": activation_name,
                                    "variant": variant,
                                    "reference_guard": reference_guard,
                                    "candidate_guard": candidate_guard,
                                    "second_candidate_guard": second_guard,
                                    "deterministic": deterministic,
                                    "comparison": comparison,
                                }
                            )
                    input_read_only = cuda.download(
                        buffers.q8_input, len(q8_input)
                    ) == q8_input
                    gate_read_only = cuda.download(
                        buffers.weight_gate, len(gate)
                    ) == gate
                    up_read_only = cuda.download(
                        buffers.weight_up, len(up)
                    ) == up
                    read_only = input_read_only and gate_read_only and up_read_only
                    all_integrity = all_integrity and read_only
                    for case in correctness:
                        if (
                            case["out_dim"] == out_dim
                            and case["pattern"] == pattern
                            and "inputs_read_only" not in case
                        ):
                            case["inputs_read_only"] = read_only

        if args.suite in ("all", "timing"):
            for out_dim in shapes:
                buffers = allocate_buffers(cuda, out_dim)
                timing_seed = args.seed ^ (out_dim << 17) ^ 0xA54FF53A
                q8_input = make_q8_input("random", timing_seed)
                gate = make_q4_weights(
                    out_dim, "random", timing_seed ^ 0x510E527F
                )
                up = make_q4_weights(
                    out_dim, "random", timing_seed ^ 0x9B05688C
                )
                cuda.upload(buffers.q8_input, q8_input)
                cuda.upload(buffers.weight_gate, gate)
                cuda.upload(buffers.weight_up, up)
                prepare_output(
                    cuda, buffers.reference_output_base, buffers.output_bytes
                )
                prepare_output(
                    cuda, buffers.candidate_output_base, buffers.output_bytes
                )
                for activation_name in activations:
                    for variant in variants:
                        result = time_ab_ba(
                            cuda,
                            references[out_dim],
                            candidates[(out_dim, variant)],
                            VARIANTS[variant],
                            buffers,
                            ACTIVATIONS[activation_name],
                            out_dim,
                            args.warmup,
                            args.iterations,
                            args.timing_pairs,
                        )
                        result.update(
                            {
                                "out_dim": out_dim,
                                "activation": activation_name,
                                "variant": variant,
                            }
                        )
                        timings.append(result)

        correctness_pass = (
            all_exact and all_integrity if correctness else None
        )
        evidence["summary"] = {
            "correctness_case_count": len(correctness),
            "timing_case_count": len(timings),
            "all_exact": all_exact if correctness else None,
            "all_integrity": all_integrity if correctness else None,
            "correctness_pass": correctness_pass,
            # A timing-only run supplements a prior correctness artifact; it
            # is intentionally not labeled a standalone qualification.
            "qualification_pass": correctness_pass is True,
            "run_pass": correctness_pass is not False,
        }
    except Exception as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n"
            )
        raise
    finally:
        if cuda is not None:
            cuda.close()

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n"
        )
    print(json.dumps(evidence["summary"], sort_keys=True))
    timings = evidence["timing"]
    assert isinstance(timings, list)
    for result in timings:
        print(
            f"out={result['out_dim']} activation={result['activation']} "
            f"variant={result['variant']} "
            f"reference={result['reference']['median_us']:.3f}us "
            f"candidate={result['candidate']['median_us']:.3f}us "
            f"speedup={result['speedup']:.4f}x "
            f"candidate_cv={result['candidate']['cv']:.4f}"
        )
    summary = evidence["summary"]
    assert isinstance(summary, dict)
    return 0 if summary["run_pass"] else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except CudaError as error:
        print(f"CUDA error: {error}", file=sys.stderr)
        raise SystemExit(3)
