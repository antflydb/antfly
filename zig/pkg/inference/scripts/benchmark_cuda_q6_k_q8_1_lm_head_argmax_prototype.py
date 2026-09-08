#!/usr/bin/env python3
"""Qualify standalone exact SM89 Q6_K x Q8_1 LM-head argmax kernels.

The checked-in canonical cubin supplies the production eight-row stage and
the production 512-thread pair reducer.  Candidate persistent stages are
loaded from a separate cubin and never enter runtime dispatch.  Correctness is
bitwise: candidate CTA pairs are compared with the equivalent deterministic
fold of canonical stage pairs, and the final token and winning score bits must
match.  The harness also checks guards, poison coverage, input immutability,
determinism, ties, NaNs, vocabulary tails, and AB/BA timing stability.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import math
import os
import statistics
import struct
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable


IN_DIM = 2560
MAX_VOCAB = 262144
Q6_BLOCK_VALUES = 256
Q6_BLOCK_BYTES = 210
Q8_BLOCK_VALUES = 32
Q8_BLOCK_BYTES = 36
PRODUCTION_TILE = 8
PRODUCTION_THREADS = 160
GENERIC_THREADS = 256
REDUCE_THREADS = 512
GUARD_BYTES = 256
CANARY = 0xA5
SCORE_POISON_BITS = 0x7FC0D1FF
INDEX_POISON = 0xDEADC0DE
TOKEN_POISON = 0xC0DEC0DE
INVALID_INDEX = 0xFFFFFFFF
NEG_FLT_MAX_BITS = 0xFF7FFFFF
UPLOAD_CHUNK_ROWS = 4096

BASELINE_FIXED = "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b"
BASELINE_GENERIC = "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8"
REDUCER = "termite_argmax_reduce_rows_pairs_f32_w16"
CANDIDATES = {
    "tile8": (
        "antfly_q6_k_q8_1_lm_head_argmax_persistent_tile8_sm89_prototype",
        8,
        160,
    ),
    "tile16": (
        "antfly_q6_k_q8_1_lm_head_argmax_persistent_tile16_sm89_prototype",
        16,
        160,
    ),
    "tile32": (
        "antfly_q6_k_q8_1_lm_head_argmax_persistent_tile32_sm89_prototype",
        32,
        160,
    ),
    "pipeline8": (
        "antfly_q6_k_q8_1_lm_head_argmax_persistent_pipeline_tile8_sm89_prototype",
        8,
        160,
    ),
    "pipeline16": (
        "antfly_q6_k_q8_1_lm_head_argmax_persistent_pipeline_tile16_sm89_prototype",
        16,
        160,
    ),
    "pipeline8_dedicated": (
        "antfly_q6_k_q8_1_lm_head_argmax_persistent_pipeline_dedicated_tile8_sm89_prototype",
        8,
        192,
    ),
}

# Screening dispositions are retained with the standalone experiment so a
# rejected geometry is not rediscovered and promoted from an isolated lucky
# run.  Only a measured row in the current evidence can become an integration
# candidate; these notes explain why the remaining compiled controls stay
# default-off.
DESIGN_NOTES = {
    "tile8": "lowest-latency persistent control; requires final 1.25x and CV gates",
    "tile16": "48-register wider group; screening did not consistently beat tile8",
    "tile32": "64-register wider group; register pressure erased grid reduction",
    "pipeline8": "double-buffered five-warp pipeline regressed from warp-0 serialization",
    "pipeline16": "wider double-buffered control retained only for rejection evidence",
    "pipeline8_dedicated": "sixth reduction warp was exact but remained below the 20% gate",
}


class CudaError(RuntimeError):
    pass


class Cuda:
    """Dependency-free CUDA Driver API subset used by the prototype harness."""

    def __init__(self) -> None:
        self.lib = ctypes.CDLL("libcuda.so.1")
        self._bind()
        self.check(self.cuInit(0), "cuInit")
        driver_version = ctypes.c_int()
        self.check(
            self.cuDriverGetVersion(ctypes.byref(driver_version)),
            "cuDriverGetVersion",
        )
        self.driver_version = driver_version.value
        device = ctypes.c_int()
        self.check(self.cuDeviceGet(ctypes.byref(device), 0), "cuDeviceGet")
        self.device = device.value
        name = ctypes.create_string_buffer(256)
        self.check(self.cuDeviceGetName(name, len(name), device), "cuDeviceGetName")
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
        if (major.value, minor.value) != (8, 9):
            raise CudaError(
                f"prototype qualification requires SM89, found {self.compute_capability}"
            )
        context = ctypes.c_void_p()
        self.check(self.cuCtxCreate(ctypes.byref(context), 0, device), "cuCtxCreate_v2")
        self.context = context
        self.allocations: list[int] = []
        self.modules: list[ctypes.c_void_p] = []

    def _bind(self) -> None:
        def bind(name: str, args: list[object], result: object = ctypes.c_int):
            function = getattr(self.lib, name)
            function.argtypes = args
            function.restype = result
            return function

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
            "cuModuleLoad", [ctypes.POINTER(ctypes.c_void_p), ctypes.c_char_p]
        )
        self.cuModuleUnload = bind("cuModuleUnload", [ctypes.c_void_p])
        self.cuModuleGetFunction = bind(
            "cuModuleGetFunction",
            [ctypes.POINTER(ctypes.c_void_p), ctypes.c_void_p, ctypes.c_char_p],
        )
        self.cuMemAlloc = bind(
            "cuMemAlloc_v2", [ctypes.POINTER(ctypes.c_uint64), ctypes.c_size_t]
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
        self.cuEventRecord = bind("cuEventRecord", [ctypes.c_void_p, ctypes.c_void_p])
        self.cuEventSynchronize = bind("cuEventSynchronize", [ctypes.c_void_p])
        self.cuEventElapsedTime = bind(
            "cuEventElapsedTime",
            [ctypes.POINTER(ctypes.c_float), ctypes.c_void_p, ctypes.c_void_p],
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
        raise CudaError(
            f"{operation}: {name.value.decode() if name.value else code}: "
            f"{detail.value.decode() if detail.value else 'unknown CUDA error'}"
        )

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
        if not data:
            return
        image = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
        self.check(
            self.cuMemcpyHtoD(pointer, ctypes.cast(image, ctypes.c_void_p), len(data)),
            "cuMemcpyHtoD_v2",
        )

    def download(self, pointer: int, size: int) -> bytes:
        image = (ctypes.c_ubyte * size)()
        self.check(
            self.cuMemcpyDtoH(ctypes.cast(image, ctypes.c_void_p), pointer, size),
            "cuMemcpyDtoH_v2",
        )
        return bytes(image)

    def launch(
        self,
        function: ctypes.c_void_p,
        grid: tuple[int, int, int],
        block: tuple[int, int, int],
        arguments: Iterable[ctypes._SimpleCData],
    ) -> None:
        values = list(arguments)
        params = (ctypes.c_void_p * len(values))(
            *(ctypes.cast(ctypes.byref(value), ctypes.c_void_p) for value in values)
        )
        self.check(
            self.cuLaunchKernel(function, *grid, *block, 0, None, params, None),
            "cuLaunchKernel",
        )

    def synchronize(self) -> None:
        self.check(self.cuCtxSynchronize(), "cuCtxSynchronize")

    def time(self, launches: int, operation: Callable[[], None]) -> float:
        start = ctypes.c_void_p()
        stop = ctypes.c_void_p()
        self.check(self.cuEventCreate(ctypes.byref(start), 0), "cuEventCreate")
        self.check(self.cuEventCreate(ctypes.byref(stop), 0), "cuEventCreate")
        try:
            self.check(self.cuEventRecord(start, None), "cuEventRecord(start)")
            for _ in range(launches):
                operation()
            self.check(self.cuEventRecord(stop, None), "cuEventRecord(stop)")
            self.check(self.cuEventSynchronize(stop), "cuEventSynchronize")
            elapsed = ctypes.c_float()
            self.check(
                self.cuEventElapsedTime(ctypes.byref(elapsed), start, stop),
                "cuEventElapsedTime",
            )
            return elapsed.value * 1000.0 / launches
        finally:
            self.cuEventDestroy(start)
            self.cuEventDestroy(stop)

    def close(self) -> None:
        for pointer in reversed(self.allocations):
            self.cuMemFree(pointer)
        self.allocations.clear()
        for module in reversed(self.modules):
            self.cuModuleUnload(module)
        self.modules.clear()
        if getattr(self, "context", None):
            self.cuCtxDestroy(self.context)
            self.context = None


def ptr(value: int) -> ctypes.c_uint64:
    return ctypes.c_uint64(value)


def u32(value: int) -> ctypes.c_uint:
    return ctypes.c_uint(value)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class Guarded:
    allocation: int
    pointer: int
    payload_bytes: int


def allocate_guarded(cuda: Cuda, payload_bytes: int) -> Guarded:
    allocation = cuda.alloc(payload_bytes + 2 * GUARD_BYTES)
    cuda.upload(allocation, bytes([CANARY]) * GUARD_BYTES)
    cuda.upload(
        allocation + GUARD_BYTES + payload_bytes,
        bytes([CANARY]) * GUARD_BYTES,
    )
    return Guarded(allocation, allocation + GUARD_BYTES, payload_bytes)


def check_guard(cuda: Cuda, buffer: Guarded, label: str) -> None:
    expected = bytes([CANARY]) * GUARD_BYTES
    prefix = cuda.download(buffer.allocation, GUARD_BYTES)
    suffix = cuda.download(buffer.pointer + buffer.payload_bytes, GUARD_BYTES)
    if prefix != expected or suffix != expected:
        side = "prefix" if prefix != expected else "suffix"
        raise AssertionError(f"{label} {side} guard was modified")


def device_sha256(cuda: Cuda, pointer: int, size: int) -> str:
    digest = hashlib.sha256()
    chunk_bytes = 8 * 1024 * 1024
    for offset in range(0, size, chunk_bytes):
        digest.update(cuda.download(pointer + offset, min(chunk_bytes, size - offset)))
    return digest.hexdigest()


def make_q6_row(row: int) -> bytes:
    image = bytearray()
    for block in range(IN_DIM // Q6_BLOCK_VALUES):
        for index in range(128):
            image.append((row * 29 + block * 47 + index * 13 + 17) & 0xFF)
        for index in range(64):
            image.append((row * 11 + block * 31 + index * 23 + 91) & 0xFF)
        for sub in range(16):
            scale = ((row * 7 + block * 5 + sub * 3) % 31) - 15
            if scale == 0:
                scale = 1
            image.append(scale & 0xFF)
        d = 0.0015 + ((row * 3 + block * 5) % 19) * 0.00007
        image.extend(struct.pack("<e", d))
    expected = (IN_DIM // Q6_BLOCK_VALUES) * Q6_BLOCK_BYTES
    if len(image) != expected:
        raise AssertionError(f"invalid synthetic Q6_K row: {len(image)} != {expected}")
    return bytes(image)


def upload_weight(cuda: Cuda, weight: Guarded) -> str:
    unique_rows = b"".join(make_q6_row(row) for row in range(256))
    row_bytes = (IN_DIM // Q6_BLOCK_VALUES) * Q6_BLOCK_BYTES
    chunk = unique_rows * (UPLOAD_CHUNK_ROWS // 256)
    offset = 0
    while offset < weight.payload_bytes:
        size = min(len(chunk), weight.payload_bytes - offset)
        cuda.upload(weight.pointer + offset, chunk[:size])
        offset += size
    return device_sha256(cuda, weight.pointer, weight.payload_bytes)


def make_q8(pattern: str) -> bytes:
    image = bytearray()
    for block in range(IN_DIM // Q8_BLOCK_VALUES):
        if pattern == "all_nan":
            image.extend(struct.pack("<H", 0x7E01))
        else:
            image.extend(struct.pack("<e", 0.012 + (block % 11) * 0.0003))
        image.extend(struct.pack("<e", 0.0))
        for lane in range(Q8_BLOCK_VALUES):
            if pattern == "all_zero":
                value = 0
            else:
                value = ((block * 37 + lane * 19 + 53) % 255) - 127
            image.append(value & 0xFF)
    expected = (IN_DIM // Q8_BLOCK_VALUES) * Q8_BLOCK_BYTES
    if len(image) != expected:
        raise AssertionError(f"invalid synthetic Q8_1 row: {len(image)} != {expected}")
    return bytes(image)


@dataclass(frozen=True)
class CorrectnessCase:
    name: str
    vocab: int
    activation: str


CORRECTNESS_CASES = (
    CorrectnessCase("full_varied", MAX_VOCAB, "varied"),
    CorrectnessCase("tail_minus_1", MAX_VOCAB - 1, "varied"),
    CorrectnessCase("tail_minus_7", MAX_VOCAB - 7, "varied"),
    CorrectnessCase("adversarial_all_tie", MAX_VOCAB, "all_zero"),
    CorrectnessCase("adversarial_all_nan", MAX_VOCAB, "all_nan"),
)


@dataclass
class Buffers:
    weight: Guarded
    q8: Guarded
    baseline_values: Guarded
    baseline_indices: Guarded
    baseline_token: Guarded
    candidate_values: Guarded
    candidate_indices: Guarded
    candidate_token: Guarded


def make_buffers(cuda: Cuda, max_grid: int) -> Buffers:
    row_bytes = (IN_DIM // Q6_BLOCK_VALUES) * Q6_BLOCK_BYTES
    baseline_count = math.ceil(MAX_VOCAB / PRODUCTION_TILE)
    q8_bytes = (IN_DIM // Q8_BLOCK_VALUES) * Q8_BLOCK_BYTES
    return Buffers(
        weight=allocate_guarded(cuda, MAX_VOCAB * row_bytes),
        q8=allocate_guarded(cuda, q8_bytes),
        baseline_values=allocate_guarded(cuda, baseline_count * 4),
        baseline_indices=allocate_guarded(cuda, baseline_count * 4),
        baseline_token=allocate_guarded(cuda, 4),
        candidate_values=allocate_guarded(cuda, max_grid * 4),
        candidate_indices=allocate_guarded(cuda, max_grid * 4),
        candidate_token=allocate_guarded(cuda, 4),
    )


def poison_stage(cuda: Cuda, values: Guarded, indices: Guarded, count: int) -> None:
    cuda.upload(values.pointer, struct.pack("<I", SCORE_POISON_BITS) * count)
    cuda.upload(indices.pointer, struct.pack("<I", INDEX_POISON) * count)


def poison_token(cuda: Cuda, token: Guarded) -> None:
    cuda.upload(token.pointer, struct.pack("<I", TOKEN_POISON))


def launch_stage(
    cuda: Cuda,
    function: ctypes.c_void_p,
    values: Guarded,
    indices: Guarded,
    q8: Guarded,
    weight: Guarded,
    vocab: int,
    grid: int,
    threads: int,
) -> None:
    cuda.launch(
        function,
        (grid, 1, 1),
        (threads, 1, 1),
        (
            ptr(values.pointer),
            ptr(indices.pointer),
            ptr(q8.pointer),
            ptr(weight.pointer),
            ptr(0),
            u32(1),
            u32(IN_DIM),
            u32(vocab),
            u32(0),
        ),
    )


def launch_reduce(
    cuda: Cuda,
    function: ctypes.c_void_p,
    token: Guarded,
    values: Guarded,
    indices: Guarded,
    count: int,
) -> None:
    cuda.launch(
        function,
        (1, 1, 1),
        (REDUCE_THREADS, 1, 1),
        (
            ptr(token.pointer),
            ptr(values.pointer),
            ptr(indices.pointer),
            u32(1),
            u32(count),
        ),
    )


def decode_pairs(
    cuda: Cuda, values: Guarded, indices: Guarded, count: int
) -> list[tuple[int, int]]:
    value_bits = struct.unpack(f"<{count}I", cuda.download(values.pointer, count * 4))
    index_values = struct.unpack(
        f"<{count}I", cuda.download(indices.pointer, count * 4)
    )
    return list(zip(value_bits, index_values))


def pair_wins(value_bits: int, index: int, best_bits: int, best_index: int) -> bool:
    if index == INVALID_INDEX:
        return False
    value = struct.unpack("<f", struct.pack("<I", value_bits))[0]
    best = struct.unpack("<f", struct.pack("<I", best_bits))[0]
    return value > best or (value == best and index < best_index)


def fold_pairs(pairs: Iterable[tuple[int, int]]) -> tuple[int, int]:
    best = (NEG_FLT_MAX_BITS, INVALID_INDEX)
    for value_bits, index in pairs:
        if pair_wins(value_bits, index, best[0], best[1]):
            best = (value_bits, index)
    return best


def aggregate_expected(
    baseline: list[tuple[int, int]],
    vocab: int,
    columns: int,
    grid: int,
) -> list[tuple[int, int]]:
    tiles_per_group = columns // PRODUCTION_TILE
    group_count = math.ceil(vocab / columns)
    expected: list[tuple[int, int]] = []
    for cta in range(grid):
        ordered: list[tuple[int, int]] = []
        for group in range(cta, group_count, grid):
            first_tile = group * tiles_per_group
            for tile_offset in range(tiles_per_group):
                tile = first_tile + tile_offset
                if tile < len(baseline):
                    ordered.append(baseline[tile])
        expected.append(fold_pairs(ordered))
    return expected


def assert_written(pairs: list[tuple[int, int]], label: str) -> None:
    for offset, (value_bits, index) in enumerate(pairs):
        if value_bits == SCORE_POISON_BITS or index == INDEX_POISON:
            raise AssertionError(f"{label} left poison at pair {offset}")


def check_all_guards(cuda: Cuda, buffers: Buffers) -> None:
    for label, buffer in vars(buffers).items():
        check_guard(cuda, buffer, label)


def run_correctness_case(
    cuda: Cuda,
    baseline_fixed: ctypes.c_void_p,
    baseline_generic: ctypes.c_void_p,
    reducer: ctypes.c_void_p,
    candidates: dict[str, ctypes.c_void_p],
    buffers: Buffers,
    case: CorrectnessCase,
    variants: list[str],
    grids: list[int],
) -> dict[str, object]:
    q8_image = make_q8(case.activation)
    cuda.upload(buffers.q8.pointer, q8_image)
    q8_hash_before = device_sha256(cuda, buffers.q8.pointer, len(q8_image))
    baseline_count = math.ceil(case.vocab / PRODUCTION_TILE)
    poison_stage(
        cuda, buffers.baseline_values, buffers.baseline_indices, baseline_count
    )
    poison_token(cuda, buffers.baseline_token)
    fixed = case.vocab == MAX_VOCAB
    baseline_function = baseline_fixed if fixed else baseline_generic
    baseline_threads = PRODUCTION_THREADS if fixed else GENERIC_THREADS
    launch_stage(
        cuda,
        baseline_function,
        buffers.baseline_values,
        buffers.baseline_indices,
        buffers.q8,
        buffers.weight,
        case.vocab,
        baseline_count,
        baseline_threads,
    )
    launch_reduce(
        cuda,
        reducer,
        buffers.baseline_token,
        buffers.baseline_values,
        buffers.baseline_indices,
        baseline_count,
    )
    cuda.synchronize()
    baseline_pairs = decode_pairs(
        cuda, buffers.baseline_values, buffers.baseline_indices, baseline_count
    )
    assert_written(baseline_pairs, f"{case.name} baseline")
    baseline_token = struct.unpack(
        "<I", cuda.download(buffers.baseline_token.pointer, 4)
    )[0]
    baseline_best = fold_pairs(baseline_pairs)
    if baseline_token != (0 if baseline_best[1] == INVALID_INDEX else baseline_best[1]):
        raise AssertionError(
            f"{case.name} canonical reducer disagrees with canonical pairs: "
            f"token={baseline_token} pair={baseline_best}"
        )
    if case.activation in {"all_zero", "all_nan"} and baseline_token != 0:
        raise AssertionError(f"{case.name} expected token 0, found {baseline_token}")

    results: list[dict[str, object]] = []
    for variant in variants:
        _, columns, candidate_threads = CANDIDATES[variant]
        group_count = math.ceil(case.vocab / columns)
        for requested_grid in grids:
            grid = min(requested_grid, group_count)
            poison_stage(
                cuda, buffers.candidate_values, buffers.candidate_indices, grid
            )
            poison_token(cuda, buffers.candidate_token)
            launch_stage(
                cuda,
                candidates[variant],
                buffers.candidate_values,
                buffers.candidate_indices,
                buffers.q8,
                buffers.weight,
                case.vocab,
                grid,
                candidate_threads,
            )
            launch_reduce(
                cuda,
                reducer,
                buffers.candidate_token,
                buffers.candidate_values,
                buffers.candidate_indices,
                grid,
            )
            cuda.synchronize()
            first_pairs = decode_pairs(
                cuda, buffers.candidate_values, buffers.candidate_indices, grid
            )
            first_token = struct.unpack(
                "<I", cuda.download(buffers.candidate_token.pointer, 4)
            )[0]
            assert_written(first_pairs, f"{case.name} {variant} grid={grid}")
            expected = aggregate_expected(baseline_pairs, case.vocab, columns, grid)
            if first_pairs != expected:
                mismatch = next(
                    index
                    for index, (actual, wanted) in enumerate(zip(first_pairs, expected))
                    if actual != wanted
                )
                raise AssertionError(
                    f"{case.name} {variant} grid={grid} pair mismatch at {mismatch}: "
                    f"actual={first_pairs[mismatch]} expected={expected[mismatch]}"
                )
            if first_token != baseline_token:
                raise AssertionError(
                    f"{case.name} {variant} grid={grid} token mismatch: "
                    f"candidate={first_token} baseline={baseline_token}"
                )
            if fold_pairs(first_pairs) != baseline_best:
                raise AssertionError(
                    f"{case.name} {variant} grid={grid} winning score/index mismatch: "
                    f"candidate={fold_pairs(first_pairs)} baseline={baseline_best}"
                )

            # A second launch must reproduce every candidate score bit/index,
            # not merely the final token.
            poison_stage(
                cuda, buffers.candidate_values, buffers.candidate_indices, grid
            )
            poison_token(cuda, buffers.candidate_token)
            launch_stage(
                cuda,
                candidates[variant],
                buffers.candidate_values,
                buffers.candidate_indices,
                buffers.q8,
                buffers.weight,
                case.vocab,
                grid,
                candidate_threads,
            )
            launch_reduce(
                cuda,
                reducer,
                buffers.candidate_token,
                buffers.candidate_values,
                buffers.candidate_indices,
                grid,
            )
            cuda.synchronize()
            second_pairs = decode_pairs(
                cuda, buffers.candidate_values, buffers.candidate_indices, grid
            )
            second_token = struct.unpack(
                "<I", cuda.download(buffers.candidate_token.pointer, 4)
            )[0]
            if second_pairs != first_pairs or second_token != first_token:
                raise AssertionError(
                    f"{case.name} {variant} grid={grid} was nondeterministic"
                )
            results.append(
                {
                    "variant": variant,
                    "columns": columns,
                    "requested_grid": requested_grid,
                    "effective_grid": grid,
                    "token": first_token,
                    "winning_score_bits": f"0x{baseline_best[0]:08x}",
                    "exact_pairs": True,
                    "deterministic": True,
                }
            )

    q8_hash_after = device_sha256(cuda, buffers.q8.pointer, len(q8_image))
    if q8_hash_after != q8_hash_before:
        raise AssertionError(f"{case.name} Q8_1 activation was modified")
    check_all_guards(cuda, buffers)
    return {
        "name": case.name,
        "vocab": case.vocab,
        "activation": case.activation,
        "canonical_stage_symbol": BASELINE_FIXED if fixed else BASELINE_GENERIC,
        "canonical_token": baseline_token,
        "canonical_winning_score_bits": f"0x{baseline_best[0]:08x}",
        "candidate_results": results,
        "q8_read_only_sha256": q8_hash_before,
    }


def timing_summary(samples_us: list[float]) -> dict[str, float | list[float]]:
    mean = statistics.fmean(samples_us)
    return {
        "samples_us": samples_us,
        "median_us": statistics.median(samples_us),
        "mean_us": mean,
        "stddev_us": statistics.pstdev(samples_us),
        "cv": statistics.pstdev(samples_us) / mean if mean else 0.0,
    }


def paired_timing(
    cuda: Cuda,
    baseline: Callable[[], None],
    candidate: Callable[[], None],
    iterations: int,
    pairs: int,
) -> tuple[dict[str, object], dict[str, object]]:
    for _ in range(5):
        baseline()
        candidate()
    cuda.synchronize()
    baseline_samples: list[float] = []
    candidate_samples: list[float] = []
    pair_order: list[str] = []
    for pair in range(pairs):
        if pair % 2 == 0:
            pair_order.append("baseline-candidate")
            baseline_samples.append(cuda.time(iterations, baseline))
            candidate_samples.append(cuda.time(iterations, candidate))
        else:
            pair_order.append("candidate-baseline")
            candidate_samples.append(cuda.time(iterations, candidate))
            baseline_samples.append(cuda.time(iterations, baseline))
    baseline_result = timing_summary(baseline_samples)
    candidate_result = timing_summary(candidate_samples)
    candidate_result["pair_order"] = pair_order
    candidate_result["paired_baseline_over_candidate"] = [
        baseline_us / candidate_us
        for baseline_us, candidate_us in zip(baseline_samples, candidate_samples)
    ]
    return baseline_result, candidate_result


def run_timing(
    cuda: Cuda,
    baseline: ctypes.c_void_p,
    reducer: ctypes.c_void_p,
    candidates: dict[str, ctypes.c_void_p],
    buffers: Buffers,
    variants: list[str],
    grids: list[int],
    iterations: int,
    pairs: int,
) -> list[dict[str, object]]:
    vocab = MAX_VOCAB
    baseline_count = vocab // PRODUCTION_TILE
    cuda.upload(buffers.q8.pointer, make_q8("varied"))

    def baseline_stage() -> None:
        launch_stage(
            cuda,
            baseline,
            buffers.baseline_values,
            buffers.baseline_indices,
            buffers.q8,
            buffers.weight,
            vocab,
            baseline_count,
            PRODUCTION_THREADS,
        )

    def baseline_full() -> None:
        baseline_stage()
        launch_reduce(
            cuda,
            reducer,
            buffers.baseline_token,
            buffers.baseline_values,
            buffers.baseline_indices,
            baseline_count,
        )

    results: list[dict[str, object]] = []
    for variant in variants:
        _, columns, candidate_threads = CANDIDATES[variant]
        group_count = math.ceil(vocab / columns)
        for requested_grid in grids:
            grid = min(requested_grid, group_count)

            def candidate_stage(
                function: ctypes.c_void_p = candidates[variant],
                effective_grid: int = grid,
            ) -> None:
                launch_stage(
                    cuda,
                    function,
                    buffers.candidate_values,
                    buffers.candidate_indices,
                    buffers.q8,
                    buffers.weight,
                    vocab,
                    effective_grid,
                    candidate_threads,
                )

            def candidate_full(
                effective_grid: int = grid,
                stage: Callable[[], None] = candidate_stage,
            ) -> None:
                stage()
                launch_reduce(
                    cuda,
                    reducer,
                    buffers.candidate_token,
                    buffers.candidate_values,
                    buffers.candidate_indices,
                    effective_grid,
                )

            baseline_stage_stats, candidate_stage_stats = paired_timing(
                cuda, baseline_stage, candidate_stage, iterations, pairs
            )
            baseline_full_stats, candidate_full_stats = paired_timing(
                cuda, baseline_full, candidate_full, iterations, pairs
            )
            stage_speedup = statistics.median(
                candidate_stage_stats["paired_baseline_over_candidate"]
            )
            full_speedup = statistics.median(
                candidate_full_stats["paired_baseline_over_candidate"]
            )
            stable = (
                max(
                    float(baseline_stage_stats["cv"]),
                    float(candidate_stage_stats["cv"]),
                    float(baseline_full_stats["cv"]),
                    float(candidate_full_stats["cv"]),
                )
                <= 0.03
            )
            # A 20% latency reduction is candidate/baseline <= 0.80, which
            # is baseline/candidate >= 1.25 (not merely a 1.20x speedup).
            meets_target = stage_speedup >= 1.25 and stable
            results.append(
                {
                    "variant": variant,
                    "columns": columns,
                    "requested_grid": requested_grid,
                    "effective_grid": grid,
                    "baseline_stage": baseline_stage_stats,
                    "candidate_stage": candidate_stage_stats,
                    "stage_speedup": stage_speedup,
                    "baseline_full": baseline_full_stats,
                    "candidate_full": candidate_full_stats,
                    "full_speedup": full_speedup,
                    "stable_cv_le_3pct": stable,
                    "meets_stage_integration_target": meets_target,
                    "decision": (
                        "integration_candidate"
                        if meets_target
                        else (
                            "rejected_unstable"
                            if stage_speedup >= 1.25
                            else "rejected_below_20pct_latency_reduction_gate"
                        )
                    ),
                }
            )
            print(
                f"timing variant={variant} grid={grid} "
                f"stage={candidate_stage_stats['median_us']:.3f}us "
                f"stage_speedup={stage_speedup:.4f} "
                f"full={candidate_full_stats['median_us']:.3f}us "
                f"full_speedup={full_speedup:.4f} "
                f"candidate_stage_cv={candidate_stage_stats['cv']:.4f} "
                f"candidate_full_cv={candidate_full_stats['cv']:.4f}",
                flush=True,
            )
    return results


def parse_csv(raw: str, allowed: set[str] | None = None) -> list[str]:
    values = [value.strip() for value in raw.split(",") if value.strip()]
    if not values:
        raise argparse.ArgumentTypeError("list must not be empty")
    if allowed is not None:
        unknown = sorted(set(values) - allowed)
        if unknown:
            raise argparse.ArgumentTypeError(f"unknown values: {','.join(unknown)}")
    return values


def positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return value


def parse_args() -> argparse.Namespace:
    inference_dir = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-cubin", type=Path, required=True)
    parser.add_argument(
        "--baseline-cubin",
        type=Path,
        default=inference_dir
        / "src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin",
    )
    parser.add_argument(
        "--suite", choices=("smoke", "correctness", "timing", "all"), default="all"
    )
    parser.add_argument(
        "--variants",
        default="tile8,tile16,tile32,pipeline8,pipeline16,pipeline8_dedicated",
        help="comma-separated candidate variants",
    )
    parser.add_argument(
        "--grid-blocks",
        default="240,480,960,1920",
        help="comma-separated persistent stage grid sizes",
    )
    parser.add_argument("--iterations", type=positive_int, default=100)
    parser.add_argument("--timing-pairs", type=positive_int, default=7)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args()
    args.variants = parse_csv(args.variants, set(CANDIDATES))
    try:
        args.grid_blocks = [
            positive_int(value) for value in parse_csv(args.grid_blocks)
        ]
    except (ValueError, argparse.ArgumentTypeError) as error:
        parser.error(str(error))
    for path in (args.candidate_cubin, args.baseline_cubin):
        if not path.is_file():
            parser.error(f"cubin is unavailable: {path}")
    return args


def main() -> int:
    args = parse_args()
    harness_path = Path(__file__).resolve()
    inference_dir = harness_path.parent.parent
    source_path = (
        inference_dir / "src/ops/cuda/prototypes/q6_k_q8_1_lm_head_argmax_sm89.cu"
    )
    build_script_path = (
        inference_dir / "scripts/build_cuda_q6_k_q8_1_lm_head_argmax_prototype.sh"
    )
    cuda: Cuda | None = None
    try:
        cuda = Cuda()
        baseline_module = cuda.load_module(args.baseline_cubin)
        candidate_module = cuda.load_module(args.candidate_cubin)
        baseline_fixed = cuda.function(baseline_module, BASELINE_FIXED)
        baseline_generic = cuda.function(baseline_module, BASELINE_GENERIC)
        reducer = cuda.function(baseline_module, REDUCER)
        candidates = {
            variant: cuda.function(candidate_module, CANDIDATES[variant][0])
            for variant in args.variants
        }
        buffers = make_buffers(cuda, max(args.grid_blocks))
        print(
            f"device={cuda.device_name} cc={cuda.compute_capability} "
            f"driver={cuda.driver_version} weight_mib={buffers.weight.payload_bytes / 2**20:.3f}",
            flush=True,
        )
        weight_hash_before = upload_weight(cuda, buffers.weight)
        print(f"weight_sha256={weight_hash_before}", flush=True)

        correctness_results: list[dict[str, object]] = []
        if args.suite in {"smoke", "correctness", "all"}:
            cases = (
                CORRECTNESS_CASES[:1] if args.suite == "smoke" else CORRECTNESS_CASES
            )
            for case in cases:
                result = run_correctness_case(
                    cuda,
                    baseline_fixed,
                    baseline_generic,
                    reducer,
                    candidates,
                    buffers,
                    case,
                    args.variants,
                    args.grid_blocks,
                )
                correctness_results.append(result)
                print(
                    f"correctness case={case.name} vocab={case.vocab} "
                    f"token={result['canonical_token']} candidates="
                    f"{len(result['candidate_results'])} exact=true",
                    flush=True,
                )

        timing_results: list[dict[str, object]] = []
        if args.suite in {"timing", "all"}:
            timing_results = run_timing(
                cuda,
                baseline_fixed,
                reducer,
                candidates,
                buffers,
                args.variants,
                args.grid_blocks,
                args.iterations,
                args.timing_pairs,
            )

        weight_hash_after = device_sha256(
            cuda, buffers.weight.pointer, buffers.weight.payload_bytes
        )
        if weight_hash_after != weight_hash_before:
            raise AssertionError("Q6_K weights were modified")
        check_all_guards(cuda, buffers)
        evidence: dict[str, object] = {
            "schema_version": 1,
            "experiment": "standalone_sm89_q6_k_q8_1_lm_head_argmax",
            "production_enabled": False,
            "device": {
                "name": cuda.device_name,
                "compute_capability": cuda.compute_capability,
                "driver_version": cuda.driver_version,
            },
            "artifacts": {
                "candidate_cubin": str(args.candidate_cubin.resolve()),
                "candidate_cubin_sha256": sha256_file(args.candidate_cubin),
                "baseline_cubin": str(args.baseline_cubin.resolve()),
                "baseline_cubin_sha256": sha256_file(args.baseline_cubin),
                "prototype_source": str(source_path),
                "prototype_source_sha256": sha256_file(source_path),
                "harness": str(harness_path),
                "harness_sha256": sha256_file(harness_path),
                "build_script": str(build_script_path),
                "build_script_sha256": sha256_file(build_script_path),
            },
            "abi": {
                "rows": 1,
                "in_dim": IN_DIM,
                "max_vocab": MAX_VOCAB,
                "weight": "Q6_K",
                "activation": "Q8_1",
                "canonical_stage": BASELINE_FIXED,
                "canonical_reducer": REDUCER,
                "candidate_threads": {
                    variant: CANDIDATES[variant][2] for variant in args.variants
                },
            },
            "configuration": {
                "suite": args.suite,
                "variants": args.variants,
                "grid_blocks": args.grid_blocks,
                "iterations": args.iterations,
                "timing_pairs": args.timing_pairs,
            },
            "design_catalog": {
                variant: {
                    "symbol": CANDIDATES[variant][0],
                    "columns": CANDIDATES[variant][1],
                    "threads": CANDIDATES[variant][2],
                    "screening_note": DESIGN_NOTES[variant],
                }
                for variant in CANDIDATES
            },
            "correctness": correctness_results,
            "timing": timing_results,
            "integrity": {
                "guards_ok": True,
                "q6_k_read_only": True,
                "q6_k_sha256": weight_hash_before,
                "q8_1_read_only_per_case": True,
            },
            "qualification": {
                "exact": bool(correctness_results),
                "best_stage_speedup": max(
                    (float(item["stage_speedup"]) for item in timing_results),
                    default=None,
                ),
                "best_full_speedup": max(
                    (float(item["full_speedup"]) for item in timing_results),
                    default=None,
                ),
                "has_integration_candidate": any(
                    bool(item["meets_stage_integration_target"])
                    for item in timing_results
                ),
                "integration_gate": (
                    "exact, paired baseline/candidate speedup >=1.25 "
                    "(candidate latency <=80%), and all timing CVs <=3%"
                ),
            },
        }
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n"
            )
        print(
            f"PASS cases={len(correctness_results)} timing_variants={len(timing_results)} "
            f"guards=true read_only=true exact=true",
            flush=True,
        )
        return 0
    except (AssertionError, CudaError) as error:
        print(f"FAIL: {error}", file=sys.stderr, flush=True)
        return 1
    finally:
        if cuda is not None:
            cuda.close()


if __name__ == "__main__":
    raise SystemExit(main())
