#!/usr/bin/env python3
"""Qualify the standalone SM89 split-K online-softmax q=1 GQA prototype.

The candidate cubin and checked-in canonical SM89 cubin are loaded as separate
CUDA modules.  The baseline is the actual canonical score producer followed by
its tiled64 consumer.  The candidate is a two-kernel stage1/stage2 pipeline;
neither module is installed into runtime dispatch by this harness.
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


HEADS = 8
KV_HEADS = 1
PAGE_SIZE = 16
GUARD_BYTES = 256
CANARY = 0xA5
OUTPUT_POISON_BITS = 0x7FC0D1FF
PARTIAL_VALUE_POISON_BITS = 0x7FC0D2FF
PARTIAL_MAX_POISON_BITS = 0x7FC0D3FF
PARTIAL_DENOM_POISON_BITS = 0x7FC0D4FF
SCORE_POISON_BITS = 0x7FC0D5FF
F16_POISON_BITS = 0x7E00
MAX_SPLITS = 64
CHUNK_COUNT = 128

CANDIDATE_STAGE1_HD256 = (
    "antfly_gqa_attention_decode_splitk_online_hd256_swa512_f16_stage1_prototype"
)
CANDIDATE_STAGE1_HD512 = (
    "antfly_gqa_attention_decode_splitk_online_hd512_global_f16_stage1_prototype"
)
CANDIDATE_STAGE2_HD256 = (
    "antfly_gqa_attention_decode_splitk_online_hd256_f16_stage2_prototype"
)
CANDIDATE_STAGE2_HD512 = (
    "antfly_gqa_attention_decode_splitk_online_hd512_f16_stage2_prototype"
)
CANDIDATE_COMPLETE_HD256 = (
    "antfly_gqa_attention_decode_splitk_online_hd256_swa512_f16_complete_prototype"
)
CANDIDATE_COMPLETE_HD512 = (
    "antfly_gqa_attention_decode_splitk_online_hd512_global_f16_complete_prototype"
)
PARITY_EXACT_HD256 = (
    "antfly_gqa_attention_decode_score_lastcta_exact_hd256_swa512_f16_prototype"
)
PARITY_EXACT_HD512 = (
    "antfly_gqa_attention_decode_score_lastcta_exact_hd512_global_f16_prototype"
)
PARITY_SCORE_HD256 = (
    "antfly_gqa_attention_decode_score_parallel_exact_hd256_swa512_f16_prototype"
)
PARITY_SCORE_HD512 = (
    "antfly_gqa_attention_decode_score_parallel_exact_hd512_global_f16_prototype"
)
ACCURATE_MERGE_HD256 = (
    "antfly_gqa_attention_decode_splitk_online_accurate_merge_hd256_swa512_f16_complete_prototype"
)
ACCURATE_MERGE_HD512 = (
    "antfly_gqa_attention_decode_splitk_online_accurate_merge_hd512_global_f16_complete_prototype"
)
SCORE_HD256 = "antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1"
TILED_HD256 = (
    "antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd256_f32_v1"
)
SCORE_HD512 = "antfly_gqa_attention_decode_turboquant_score_prework_hd512_f32_v1"
TILED_HD512 = (
    "antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd512_f32_v1"
)


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
        if major.value != 8 or minor.value != 9:
            raise CudaError(
                f"prototype is qualified only on SM89; found {self.compute_capability}"
            )
        context = ctypes.c_void_p()
        self.check(
            self.cuCtxCreate(ctypes.byref(context), 0, device), "cuCtxCreate_v2"
        )
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
            "cuMemcpyHtoD_v2", [ctypes.c_uint64, ctypes.c_void_p, ctypes.c_size_t]
        )
        self.cuMemcpyDtoH = bind(
            "cuMemcpyDtoH_v2", [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_size_t]
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

    def free(self, pointer: int) -> None:
        if pointer in self.allocations:
            self.check(self.cuMemFree(pointer), "cuMemFree_v2")
            self.allocations.remove(pointer)

    def upload(self, pointer: int, data: bytes | bytearray) -> None:
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
            self.cuLaunchKernel(
                function,
                *grid,
                *block,
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


def u32(value: int) -> ctypes.c_uint:
    return ctypes.c_uint(value)


def ptr(value: int) -> ctypes.c_uint64:
    return ctypes.c_uint64(value)


@dataclass(frozen=True)
class Geometry:
    splits: int
    stage1_threads: int
    stage2_threads: int

    @property
    def label(self) -> str:
        return f"split{self.splits}-s1t{self.stage1_threads}-s2t{self.stage2_threads}"


@dataclass(frozen=True)
class Case:
    name: str
    head_dim: int
    kv_len: int
    query_position: int
    kv_position_offset: int
    sliding_window: int
    layout: str
    pattern: str
    seed: int
    timing_anchor: bool = False
    candidate_only: bool = False

    @property
    def visible_range(self) -> tuple[int, int]:
        if self.query_position < self.kv_position_offset:
            return (0, 0)
        end = min(
            self.query_position - self.kv_position_offset + 1,
            self.kv_len,
        )
        begin = 0
        if self.sliding_window:
            window_start = max(0, self.query_position + 1 - self.sliding_window)
            if window_start > self.kv_position_offset:
                begin = min(window_start - self.kv_position_offset, end)
        return begin, end


@dataclass
class Guarded:
    allocation: int
    pointer: int
    image: bytes
    payload_bytes: int


@dataclass
class HostInputs:
    q: bytes
    k: bytes
    v: bytes
    table: bytes | None
    table_values: list[int] | None
    scalars: bytes
    block_count: int
    physical_capacity: int


@dataclass
class Buffers:
    q: Guarded
    k: Guarded
    v: Guarded
    table: Guarded | None
    scalars: Guarded
    candidate: Guarded
    baseline: Guarded
    partial_values: Guarded
    partial_max: Guarded
    partial_denom: Guarded
    completion_counters: Guarded
    scores: Guarded
    score_capacity: int
    physical_capacity: int
    key_row_bytes: int
    value_row_bytes: int
    block_count: int
    host: HostInputs


@dataclass(frozen=True)
class HeadFunctions:
    stage1: ctypes.c_void_p
    stage2: ctypes.c_void_p
    complete: ctypes.c_void_p
    parity_exact: ctypes.c_void_p
    parity_score: ctypes.c_void_p
    accurate_merge: ctypes.c_void_p
    score: ctypes.c_void_p
    tiled: ctypes.c_void_p


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def guarded_image(payload: bytes | bytearray) -> bytes:
    return bytes([CANARY]) * GUARD_BYTES + bytes(payload) + bytes([CANARY]) * GUARD_BYTES


def upload_guarded(cuda: Cuda, payload: bytes | bytearray) -> Guarded:
    image = guarded_image(payload)
    allocation = cuda.alloc(len(image))
    cuda.upload(allocation, image)
    return Guarded(allocation, allocation + GUARD_BYTES, image, len(payload))


def reset_guarded(cuda: Cuda, guarded: Guarded) -> None:
    cuda.upload(guarded.allocation, guarded.image)


def poison_f32_payload(count: int, bits: int) -> bytes:
    return struct.pack("<I", bits) * count


def deterministic_unit(seed: int, first: int, second: int, third: int = 0) -> float:
    value = (
        seed
        ^ (first * 0x9E3779B97F4A7C15)
        ^ (second * 0xBF58476D1CE4E5B9)
        ^ (third * 0x94D049BB133111EB)
    ) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 30
    value = (value * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 27
    value = (value * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 31
    return (value & 0xFFFF) / 32767.5 - 1.0


def q_value(case: Case, head: int, dimension: int) -> float:
    if case.pattern == "random":
        return deterministic_unit(case.seed, head, dimension) * 0.125
    if case.pattern == "near-tie":
        sign = 1.0 if ((head + dimension) & 1) == 0 else -1.0
        return sign * (0.03125 + ((head * 7 + dimension) % 5) * 0.00003125)
    if case.pattern == "cancellation":
        sign = 1.0 if (dimension & 1) == 0 else -1.0
        return sign * (0.125 + ((head * 13 + dimension) % 7) * 0.00001)
    sign = 1.0 if ((head * 3 + dimension) & 1) == 0 else -1.0
    scale = 2.0 if dimension % 31 == 0 else 0.015625
    return sign * scale * (1.0 + (dimension % 9) * 0.0001)


def k_value(case: Case, token: int, dimension: int) -> float:
    if case.pattern == "random":
        return deterministic_unit(case.seed ^ 0xA5A5A5A5, token, dimension) * 0.125
    if case.pattern == "near-tie":
        sign = 1.0 if (dimension & 1) == 0 else -1.0
        return sign * (0.03125 + ((token * 17 + dimension * 3) % 11) * 0.00000025)
    if case.pattern == "cancellation":
        dimension_sign = 1.0 if (dimension & 1) == 0 else -1.0
        token_sign = 1.0 if (token & 1) == 0 else -1.0
        return dimension_sign * (
            token_sign * 0.125 + ((token * 19 + dimension) % 13) * 0.00001
        )
    sign = 1.0 if ((token + dimension) & 1) == 0 else -1.0
    inverse = 0.0078125 if dimension % 31 == 0 else 0.5
    return sign * inverse * (1.0 + ((token + dimension) % 7) * 0.0001)


def v_value(case: Case, token: int, dimension: int) -> float:
    if case.pattern == "random":
        return deterministic_unit(case.seed ^ 0x5A5A5A5A, token, dimension) * 0.75
    if case.pattern == "near-tie":
        sign = 1.0 if ((token + dimension) & 1) == 0 else -1.0
        return sign * (0.25 + (token % 29) * 0.0005)
    if case.pattern == "cancellation":
        token_sign = 1.0 if (token & 1) == 0 else -1.0
        return token_sign * (0.5 + (dimension % 17) * 0.0001)
    sign = 1.0 if ((token * 5 + dimension * 3) & 1) == 0 else -1.0
    magnitude = 4.0 if token % 257 == 0 else 0.03125
    return sign * magnitude * (1.0 + (dimension % 11) * 0.0001)


def page_table(case: Case, block_count: int) -> list[int] | None:
    if case.layout == "identity-null":
        return None
    if case.layout == "explicit-invalid":
        return [block_count + 7] * block_count
    table = list(range(block_count))
    if case.layout == "explicit-reversed":
        table.reverse()
    elif case.layout == "explicit-permuted" and block_count > 1:
        # A bijection for every block count, unlike a fixed odd multiplier.
        table = [(index + 1) % block_count for index in range(block_count)]
    return table


def physical_token(logical: int, table: list[int] | None) -> int:
    if table is None:
        return logical
    return table[logical // PAGE_SIZE] * PAGE_SIZE + logical % PAGE_SIZE


def make_inputs(case: Case) -> HostInputs:
    head_dim = case.head_dim
    block_count = (case.kv_len + PAGE_SIZE - 1) // PAGE_SIZE
    physical_capacity = max(1, block_count * PAGE_SIZE)
    table_values = page_table(case, block_count)

    q = bytearray(HEADS * head_dim * 4)
    for head in range(HEADS):
        for dimension in range(head_dim):
            struct.pack_into(
                "<f",
                q,
                (head * head_dim + dimension) * 4,
                q_value(case, head, dimension),
            )

    f16_poison = struct.pack("<H", F16_POISON_BITS)
    k = bytearray(f16_poison * (physical_capacity * head_dim))
    v = bytearray(f16_poison * (physical_capacity * head_dim))
    if case.layout != "explicit-invalid":
        for logical in range(case.kv_len):
            physical = physical_token(logical, table_values)
            row = physical * head_dim
            for dimension in range(head_dim):
                struct.pack_into(
                    "<e", k, (row + dimension) * 2, k_value(case, logical, dimension)
                )
                struct.pack_into(
                    "<e", v, (row + dimension) * 2, v_value(case, logical, dimension)
                )

    table = (
        None
        if table_values is None
        else struct.pack(f"<{len(table_values)}I", *table_values)
    )
    total_sequence_len = max(
        case.query_position + 1,
        case.kv_position_offset + case.kv_len,
    )
    scalars = struct.pack(
        "<5I",
        0,
        case.query_position,
        case.kv_len,
        total_sequence_len,
        case.kv_position_offset,
    )
    return HostInputs(
        bytes(q),
        bytes(k),
        bytes(v),
        table,
        table_values,
        scalars,
        0 if table_values is None else block_count,
        physical_capacity,
    )


def prepare_buffers(cuda: Cuda, case: Case) -> Buffers:
    host = make_inputs(case)
    output_count = HEADS * case.head_dim
    score_capacity = 512 if case.head_dim == 256 else 4096
    partial_value_count = HEADS * MAX_SPLITS * case.head_dim
    partial_scalar_count = HEADS * MAX_SPLITS
    return Buffers(
        q=upload_guarded(cuda, host.q),
        k=upload_guarded(cuda, host.k),
        v=upload_guarded(cuda, host.v),
        table=None if host.table is None else upload_guarded(cuda, host.table),
        scalars=upload_guarded(cuda, host.scalars),
        candidate=upload_guarded(
            cuda, poison_f32_payload(output_count, OUTPUT_POISON_BITS)
        ),
        baseline=upload_guarded(
            cuda, poison_f32_payload(output_count, OUTPUT_POISON_BITS)
        ),
        partial_values=upload_guarded(
            cuda,
            poison_f32_payload(partial_value_count, PARTIAL_VALUE_POISON_BITS),
        ),
        partial_max=upload_guarded(
            cuda, poison_f32_payload(partial_scalar_count, PARTIAL_MAX_POISON_BITS)
        ),
        partial_denom=upload_guarded(
            cuda,
            poison_f32_payload(partial_scalar_count, PARTIAL_DENOM_POISON_BITS),
        ),
        completion_counters=upload_guarded(cuda, bytes(HEADS * 4)),
        scores=upload_guarded(
            cuda,
            poison_f32_payload(HEADS * score_capacity, SCORE_POISON_BITS),
        ),
        score_capacity=score_capacity,
        physical_capacity=host.physical_capacity,
        key_row_bytes=case.head_dim * 2,
        value_row_bytes=case.head_dim * 2,
        block_count=host.block_count,
        host=host,
    )


def free_buffers(cuda: Cuda, buffers: Buffers) -> None:
    guarded = [
        buffers.q,
        buffers.k,
        buffers.v,
        buffers.scalars,
        buffers.candidate,
        buffers.baseline,
        buffers.partial_values,
        buffers.partial_max,
        buffers.partial_denom,
        buffers.completion_counters,
        buffers.scores,
    ]
    if buffers.table is not None:
        guarded.append(buffers.table)
    for item in guarded:
        cuda.free(item.allocation)


def common_values(case: Case) -> tuple[int, ...]:
    total = max(
        case.query_position + 1,
        case.kv_position_offset + case.kv_len,
    )
    return (
        1,
        1,
        case.kv_len,
        HEADS,
        KV_HEADS,
        case.head_dim,
        case.query_position,
        case.kv_position_offset,
        case.sliding_window,
        total,
    )


def launch_stage1(
    cuda: Cuda,
    function: ctypes.c_void_p,
    case: Case,
    buffers: Buffers,
    geometry: Geometry,
    *,
    key_format: int = 2,
    base_key_row_bytes: int | None = None,
    page_size: int = PAGE_SIZE,
) -> None:
    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = (
        common_values(case)
    )
    cuda.launch(
        function,
        (HEADS * geometry.splits, 1, 1),
        (geometry.stage1_threads, 1, 1),
        [
            ptr(buffers.partial_values.pointer),
            ptr(buffers.partial_max.pointer),
            ptr(buffers.partial_denom.pointer),
            ptr(buffers.q.pointer),
            ptr(buffers.k.pointer),
            ptr(buffers.v.pointer),
            ptr(0 if buffers.table is None else buffers.table.pointer),
            u32(batch),
            u32(q_len),
            u32(kv_len),
            u32(heads),
            u32(kv_heads),
            u32(hd),
            u32(qpos),
            u32(kvpos),
            u32(window),
            u32(total),
            u32(buffers.key_row_bytes),
            u32(
                buffers.key_row_bytes
                if base_key_row_bytes is None
                else base_key_row_bytes
            ),
            u32(buffers.value_row_bytes),
            u32(buffers.block_count),
            u32(page_size),
            u32(key_format),
            u32(2),
            u32(buffers.physical_capacity),
            u32(buffers.score_capacity),
            ptr(buffers.scalars.pointer),
            u32(geometry.splits),
        ],
    )


def launch_stage2(
    cuda: Cuda,
    function: ctypes.c_void_p,
    case: Case,
    buffers: Buffers,
    geometry: Geometry,
    *,
    head_dim: int | None = None,
) -> None:
    cuda.launch(
        function,
        (HEADS, 1, 1),
        (geometry.stage2_threads, 1, 1),
        [
            ptr(buffers.candidate.pointer),
            ptr(buffers.partial_values.pointer),
            ptr(buffers.partial_max.pointer),
            ptr(buffers.partial_denom.pointer),
            u32(HEADS),
            u32(case.head_dim if head_dim is None else head_dim),
            u32(geometry.splits),
        ],
    )


def launch_candidate(
    cuda: Cuda,
    functions: HeadFunctions,
    case: Case,
    buffers: Buffers,
    geometry: Geometry,
) -> None:
    launch_stage1(cuda, functions.stage1, case, buffers, geometry)
    launch_stage2(cuda, functions.stage2, case, buffers, geometry)


def launch_complete(
    cuda: Cuda,
    function: ctypes.c_void_p,
    case: Case,
    buffers: Buffers,
    geometry: Geometry,
    *,
    key_format: int = 2,
) -> None:
    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = (
        common_values(case)
    )
    cuda.launch(
        function,
        (HEADS * geometry.splits, 1, 1),
        (geometry.stage1_threads, 1, 1),
        [
            ptr(buffers.candidate.pointer),
            ptr(buffers.completion_counters.pointer),
            ptr(buffers.partial_values.pointer),
            ptr(buffers.partial_max.pointer),
            ptr(buffers.partial_denom.pointer),
            ptr(buffers.q.pointer),
            ptr(buffers.k.pointer),
            ptr(buffers.v.pointer),
            ptr(0 if buffers.table is None else buffers.table.pointer),
            u32(batch),
            u32(q_len),
            u32(kv_len),
            u32(heads),
            u32(kv_heads),
            u32(hd),
            u32(qpos),
            u32(kvpos),
            u32(window),
            u32(total),
            u32(buffers.key_row_bytes),
            u32(buffers.key_row_bytes),
            u32(buffers.value_row_bytes),
            u32(buffers.block_count),
            u32(PAGE_SIZE),
            u32(key_format),
            u32(2),
            u32(buffers.physical_capacity),
            u32(buffers.score_capacity),
            ptr(buffers.scalars.pointer),
            u32(geometry.splits),
        ],
    )


def launch_parity_exact(
    cuda: Cuda,
    function: ctypes.c_void_p,
    case: Case,
    buffers: Buffers,
    split_count: int,
    *,
    key_format: int = 2,
) -> None:
    """Launch the baseline-order score/consumer fusion control.

    Its block width is intentionally fixed to HeadDim.  A/B experiments use
    launch_complete with different block widths; this control tests whether
    exact baseline arithmetic can coexist with parallel QK score production.
    """

    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = (
        common_values(case)
    )
    cuda.launch(
        function,
        (HEADS * split_count, 1, 1),
        (case.head_dim, 1, 1),
        [
            ptr(buffers.candidate.pointer),
            ptr(buffers.completion_counters.pointer),
            ptr(buffers.scores.pointer),
            ptr(buffers.q.pointer),
            ptr(buffers.k.pointer),
            ptr(buffers.v.pointer),
            ptr(0 if buffers.table is None else buffers.table.pointer),
            u32(batch),
            u32(q_len),
            u32(kv_len),
            u32(heads),
            u32(kv_heads),
            u32(hd),
            u32(qpos),
            u32(kvpos),
            u32(window),
            u32(total),
            u32(buffers.key_row_bytes),
            u32(buffers.key_row_bytes),
            u32(buffers.value_row_bytes),
            u32(buffers.block_count),
            u32(PAGE_SIZE),
            u32(key_format),
            u32(2),
            u32(buffers.physical_capacity),
            u32(buffers.score_capacity),
            ptr(buffers.scalars.pointer),
            u32(split_count),
        ],
    )


def launch_score(
    cuda: Cuda,
    function: ctypes.c_void_p,
    case: Case,
    buffers: Buffers,
) -> None:
    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = (
        common_values(case)
    )
    chunk_size = (buffers.score_capacity + CHUNK_COUNT - 1) // CHUNK_COUNT
    cuda.launch(
        function,
        (HEADS * CHUNK_COUNT, 1, 1),
        (case.head_dim, 1, 1),
        [
            ptr(buffers.scores.pointer),
            ptr(buffers.q.pointer),
            ptr(buffers.k.pointer),
            ptr(0 if buffers.table is None else buffers.table.pointer),
            u32(batch),
            u32(q_len),
            u32(kv_len),
            u32(heads),
            u32(kv_heads),
            u32(hd),
            u32(qpos),
            u32(kvpos),
            u32(window),
            u32(total),
            u32(buffers.key_row_bytes),
            u32(buffers.key_row_bytes),
            u32(buffers.block_count),
            u32(PAGE_SIZE),
            u32(2),
            u32(buffers.physical_capacity),
            u32(buffers.score_capacity),
            u32(chunk_size),
            u32(CHUNK_COUNT),
            ptr(buffers.scalars.pointer),
        ],
    )


def launch_tiled_to(
    cuda: Cuda,
    function: ctypes.c_void_p,
    case: Case,
    buffers: Buffers,
    destination: Guarded,
) -> None:
    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = (
        common_values(case)
    )
    cuda.launch(
        function,
        (HEADS, case.head_dim // 64, 1),
        (64, 1, 1),
        [
            ptr(destination.pointer),
            ptr(buffers.scores.pointer),
            ptr(buffers.v.pointer),
            ptr(0 if buffers.table is None else buffers.table.pointer),
            u32(batch),
            u32(q_len),
            u32(kv_len),
            u32(heads),
            u32(kv_heads),
            u32(hd),
            u32(qpos),
            u32(kvpos),
            u32(window),
            u32(total),
            u32(buffers.value_row_bytes),
            u32(buffers.block_count),
            u32(PAGE_SIZE),
            u32(2),
            u32(buffers.physical_capacity),
            u32(buffers.score_capacity),
            ptr(buffers.scalars.pointer),
        ],
    )


def launch_tiled(
    cuda: Cuda,
    function: ctypes.c_void_p,
    case: Case,
    buffers: Buffers,
) -> None:
    launch_tiled_to(cuda, function, case, buffers, buffers.baseline)


def launch_baseline(
    cuda: Cuda,
    functions: HeadFunctions,
    case: Case,
    buffers: Buffers,
) -> None:
    launch_score(cuda, functions.score, case, buffers)
    launch_tiled(cuda, functions.tiled, case, buffers)


def payload(image: bytes) -> bytes:
    return image[GUARD_BYTES:-GUARD_BYTES]


def inspect_guarded(
    image: bytes,
    guarded: Guarded,
    *,
    poison_bits: int | None = None,
    used_elements: int | None = None,
    require_unused_poison: bool = False,
) -> dict[str, int]:
    if len(image) != len(guarded.image):
        raise ValueError("guarded image length changed")
    guard_mutations = sum(value != CANARY for value in image[:GUARD_BYTES])
    guard_mutations += sum(value != CANARY for value in image[-GUARD_BYTES:])
    result = {"guard_mutations": guard_mutations}
    if poison_bits is not None:
        body = payload(image)
        count = len(body) // 4
        used = count if used_elements is None else used_elements
        result["used_poison_elements"] = sum(
            struct.unpack_from("<I", body, index * 4)[0] == poison_bits
            for index in range(used)
        )
        result["used_nonfinite_elements"] = sum(
            not math.isfinite(struct.unpack_from("<f", body, index * 4)[0])
            for index in range(used)
        )
        result["unused_mutations"] = (
            sum(
                struct.unpack_from("<I", body, index * 4)[0] != poison_bits
                for index in range(used, count)
            )
            if require_unused_poison
            else 0
        )
    return result


def diff_f32(reference: bytes, candidate: bytes) -> dict[str, object]:
    if len(reference) != len(candidate) or len(reference) % 4:
        raise ValueError("invalid F32 payload lengths")
    count = len(reference) // 4
    bitwise = 0
    nonfinite = 0
    max_abs = 0.0
    max_relative = 0.0
    sum_error_squared = 0.0
    sum_reference_squared = 0.0
    dot = 0.0
    reference_norm = 0.0
    candidate_norm = 0.0
    first: dict[str, object] | None = None
    for index in range(count):
        reference_bits = struct.unpack_from("<I", reference, index * 4)[0]
        candidate_bits = struct.unpack_from("<I", candidate, index * 4)[0]
        reference_value = struct.unpack_from("<f", reference, index * 4)[0]
        candidate_value = struct.unpack_from("<f", candidate, index * 4)[0]
        if reference_bits != candidate_bits:
            bitwise += 1
            if first is None:
                first = {
                    "index": index,
                    "reference_bits": f"0x{reference_bits:08x}",
                    "candidate_bits": f"0x{candidate_bits:08x}",
                    "reference": reference_value,
                    "candidate": candidate_value,
                }
        if not math.isfinite(reference_value) or not math.isfinite(candidate_value):
            nonfinite += 1
            continue
        error = candidate_value - reference_value
        absolute = abs(error)
        max_abs = max(max_abs, absolute)
        max_relative = max(max_relative, absolute / max(abs(reference_value), 1e-8))
        sum_error_squared += error * error
        sum_reference_squared += reference_value * reference_value
        dot += reference_value * candidate_value
        reference_norm += reference_value * reference_value
        candidate_norm += candidate_value * candidate_value
    rms = math.sqrt(sum_error_squared / count) if count else 0.0
    normalized_rms = (
        math.sqrt(sum_error_squared / sum_reference_squared)
        if sum_reference_squared > 0.0
        else (0.0 if sum_error_squared == 0.0 else math.inf)
    )
    cosine = (
        dot / math.sqrt(reference_norm * candidate_norm)
        if reference_norm > 0.0 and candidate_norm > 0.0
        else (1.0 if reference_norm == candidate_norm else 0.0)
    )
    return {
        "element_count": count,
        "bitwise_mismatches": bitwise,
        "nonfinite_pairs": nonfinite,
        "max_abs": max_abs,
        "max_relative": max_relative,
        "rms_error": rms,
        "normalized_rms": normalized_rms,
        "cosine_similarity": cosine,
        "first_bitwise_mismatch": first,
    }


def coefficient_of_variation(samples: list[float]) -> float:
    if len(samples) < 2:
        return 0.0
    mean = statistics.fmean(samples)
    return statistics.pstdev(samples) / mean if mean > 0.0 else 0.0


def paired_timing(
    cuda: Cuda,
    candidate: Callable[[], None],
    baseline: Callable[[], None],
    iterations: int,
    pairs: int,
    max_cv: float,
    max_attempts: int = 3,
) -> dict[str, object]:
    attempts: list[dict[str, object]] = []
    for attempt_index in range(max_attempts):
        # Re-warm both arms before every bounded retry.  A retry is retained in
        # evidence rather than silently discarding an interrupted measurement.
        for _ in range(10):
            candidate()
            baseline()
        cuda.synchronize()
        candidate_samples: list[float] = []
        baseline_samples: list[float] = []
        for pair_index in range(pairs):
            if pair_index % 2 == 0:
                candidate_samples.append(cuda.time(iterations, candidate))
                baseline_samples.append(cuda.time(iterations, baseline))
            else:
                baseline_samples.append(cuda.time(iterations, baseline))
                candidate_samples.append(cuda.time(iterations, candidate))
        candidate_mean = statistics.fmean(candidate_samples)
        baseline_mean = statistics.fmean(baseline_samples)
        attempt = {
            "attempt": attempt_index + 1,
            "candidate_us": candidate_mean,
            "baseline_us": baseline_mean,
            "speedup": baseline_mean / candidate_mean,
            "candidate_cv": coefficient_of_variation(candidate_samples),
            "baseline_cv": coefficient_of_variation(baseline_samples),
            "candidate_samples_us": candidate_samples,
            "baseline_samples_us": baseline_samples,
        }
        attempts.append(attempt)
        if attempt["candidate_cv"] <= max_cv and attempt["baseline_cv"] <= max_cv:
            break
    stable_attempts = [
        attempt
        for attempt in attempts
        if attempt["candidate_cv"] <= max_cv and attempt["baseline_cv"] <= max_cv
    ]
    selected = (
        stable_attempts[0]
        if stable_attempts
        else min(
            attempts,
            key=lambda attempt: max(
                attempt["candidate_cv"], attempt["baseline_cv"]
            ),
        )
    )
    return {
        "pairs": pairs,
        "iterations_per_arm_per_pair": iterations,
        "order": "alternating_AB_BA",
        "max_cv": max_cv,
        "max_attempts": max_attempts,
        "selected_attempt": selected["attempt"],
        "retry_count": len(attempts) - 1,
        "attempts": attempts,
        **{key: value for key, value in selected.items() if key != "attempt"},
    }


def operation_timing(
    cuda: Cuda,
    operation: Callable[[], None],
    iterations: int,
    pairs: int,
) -> dict[str, object]:
    for _ in range(5):
        operation()
    cuda.synchronize()
    samples = [cuda.time(iterations, operation) for _ in range(pairs)]
    return {
        "pairs": pairs,
        "iterations_per_pair": iterations,
        "mean_us": statistics.fmean(samples),
        "cv": coefficient_of_variation(samples),
        "samples_us": samples,
    }


def f32_at(data: bytes, index: int) -> float:
    return struct.unpack_from("<f", data, index * 4)[0]


def f16_at(data: bytes, index: int) -> float:
    return struct.unpack_from("<e", data, index * 2)[0]


def cpu_sample(
    case: Case,
    buffers: Buffers,
    candidate_output: bytes,
    baseline_output: bytes | None,
) -> dict[str, object]:
    """Small independent oracle focused on routing and mask/page semantics.

    Full-output arithmetic metrics use the canonical GPU baseline.  This CPU
    oracle samples two query heads and four value columns while still computing
    each sampled head's complete QK dot.  It therefore catches wrong GQA head
    mapping, visible ranges, page tables, and KV offsets without pretending that
    Python F64 accumulation reproduces CUDA's F32 reduction tree.
    """

    begin, end = case.visible_range
    dimensions = sorted({0, case.head_dim // 3, (2 * case.head_dim) // 3, case.head_dim - 1})
    heads = (0, HEADS - 1)
    candidate_values = [
        struct.unpack_from("<f", candidate_output, index * 4)[0]
        for index in range(len(candidate_output) // 4)
    ]
    baseline_values = (
        None
        if baseline_output is None
        else [
            struct.unpack_from("<f", baseline_output, index * 4)[0]
            for index in range(len(baseline_output) // 4)
        ]
    )
    samples: list[dict[str, object]] = []
    candidate_max_abs = 0.0
    baseline_max_abs = 0.0
    nonfinite = 0
    for head in heads:
        scored: list[tuple[int, float]] = []
        for logical in range(begin, end):
            physical = physical_token(logical, buffers.host.table_values)
            if physical >= buffers.physical_capacity:
                continue
            q_base = head * case.head_dim
            k_base = physical * case.head_dim
            dot = math.fsum(
                f32_at(buffers.host.q, q_base + dimension)
                * f16_at(buffers.host.k, k_base + dimension)
                for dimension in range(case.head_dim)
            )
            scored.append((physical, dot / math.sqrt(case.head_dim)))
        maximum = max((score for _, score in scored), default=-math.inf)
        weighted = [
            (physical, math.exp(score - maximum)) for physical, score in scored
        ]
        denominator = math.fsum(weight for _, weight in weighted)
        for dimension in dimensions:
            expected = (
                math.fsum(
                    weight
                    * f16_at(
                        buffers.host.v,
                        physical * case.head_dim + dimension,
                    )
                    for physical, weight in weighted
                )
                / denominator
                if denominator > 0.0
                else 0.0
            )
            index = head * case.head_dim + dimension
            candidate = candidate_values[index]
            baseline = None if baseline_values is None else baseline_values[index]
            if not math.isfinite(expected) or not math.isfinite(candidate) or (
                baseline is not None and not math.isfinite(baseline)
            ):
                nonfinite += 1
            else:
                candidate_max_abs = max(candidate_max_abs, abs(candidate - expected))
                if baseline is not None:
                    baseline_max_abs = max(baseline_max_abs, abs(baseline - expected))
            samples.append(
                {
                    "head": head,
                    "dimension": dimension,
                    "expected": expected,
                    "candidate": candidate,
                    "baseline": baseline,
                }
            )
    return {
        "sample_count": len(samples),
        "accumulation": "python_f64_fsum_with_exact_f16_storage_values",
        "candidate_max_abs": candidate_max_abs,
        "baseline_max_abs": baseline_max_abs if baseline_values is not None else None,
        "nonfinite": nonfinite,
        "samples": samples,
    }


def input_integrity(cuda: Cuda, buffers: Buffers) -> dict[str, int]:
    result: dict[str, int] = {}
    for name in ("q", "k", "v", "scalars"):
        guarded = getattr(buffers, name)
        current = cuda.download(guarded.allocation, len(guarded.image))
        result[f"{name}_mutations"] = sum(
            left != right for left, right in zip(current, guarded.image)
        )
    if buffers.table is None:
        result["table_mutations"] = 0
    else:
        current = cuda.download(buffers.table.allocation, len(buffers.table.image))
        result["table_mutations"] = sum(
            left != right for left, right in zip(current, buffers.table.image)
        )
    return result


def geometry_space(head_dim: int) -> list[Geometry]:
    stage1_threads = (128, 256) if head_dim == 256 else (128, 256, 512)
    # The headline completion path performs the merge in the last stage-1 CTA,
    # so its merge width equals stage1_threads.  stage2_threads records the
    # independent two-kernel control's fixed diagnostic geometry.
    geometries = {
        Geometry(splits, s1, min(head_dim, 256))
        for splits in (2, 4, 8, 16, 32, 64)
        for s1 in stage1_threads
    }
    return sorted(
        geometries,
        key=lambda item: (item.splits, item.stage1_threads, item.stage2_threads),
    )


def reset_candidate_buffers(cuda: Cuda, buffers: Buffers) -> None:
    for guarded in (
        buffers.candidate,
        buffers.partial_values,
        buffers.partial_max,
        buffers.partial_denom,
        buffers.completion_counters,
    ):
        reset_guarded(cuda, guarded)


def reset_baseline_buffers(cuda: Cuda, buffers: Buffers) -> None:
    reset_guarded(cuda, buffers.baseline)
    reset_guarded(cuda, buffers.scores)


def run_geometry_sweep(
    cuda: Cuda,
    functions: HeadFunctions,
    case: Case,
    iterations: int,
    timing_pairs: int,
    max_abs: float,
    max_normalized_rms: float,
    min_cosine: float,
    max_cv: float,
) -> dict[str, object]:
    buffers = prepare_buffers(cuda, case)
    try:
        reset_baseline_buffers(cuda, buffers)
        launch_baseline(cuda, functions, case, buffers)
        cuda.synchronize()
        baseline_image = cuda.download(
            buffers.baseline.allocation, len(buffers.baseline.image)
        )
        baseline_payload = payload(baseline_image)
        results: list[dict[str, object]] = []
        for geometry in geometry_space(case.head_dim):
            reset_candidate_buffers(cuda, buffers)
            launch_complete(cuda, functions.complete, case, buffers, geometry)
            cuda.synchronize()
            candidate_image = cuda.download(
                buffers.candidate.allocation, len(buffers.candidate.image)
            )
            partial_values = cuda.download(
                buffers.partial_values.allocation,
                len(buffers.partial_values.image),
            )
            partial_max = cuda.download(
                buffers.partial_max.allocation, len(buffers.partial_max.image)
            )
            partial_denom = cuda.download(
                buffers.partial_denom.allocation, len(buffers.partial_denom.image)
            )
            completion_counters = cuda.download(
                buffers.completion_counters.allocation,
                len(buffers.completion_counters.image),
            )
            numeric = diff_f32(baseline_payload, payload(candidate_image))
            used_values = HEADS * geometry.splits * case.head_dim
            used_scalars = HEADS * geometry.splits
            integrity = {
                "output": inspect_guarded(
                    candidate_image,
                    buffers.candidate,
                    poison_bits=OUTPUT_POISON_BITS,
                ),
                "partial_values": inspect_guarded(
                    partial_values,
                    buffers.partial_values,
                    poison_bits=PARTIAL_VALUE_POISON_BITS,
                    used_elements=used_values,
                    require_unused_poison=True,
                ),
                "partial_max": inspect_guarded(
                    partial_max,
                    buffers.partial_max,
                    poison_bits=PARTIAL_MAX_POISON_BITS,
                    used_elements=used_scalars,
                    require_unused_poison=True,
                ),
                "partial_denom": inspect_guarded(
                    partial_denom,
                    buffers.partial_denom,
                    poison_bits=PARTIAL_DENOM_POISON_BITS,
                    used_elements=used_scalars,
                    require_unused_poison=True,
                ),
                "completion_counters_rearmed": {
                    "mutations": sum(
                        left != right
                        for left, right in zip(
                            completion_counters,
                            buffers.completion_counters.image,
                        )
                    )
                },
            }
            numeric_pass = (
                numeric["nonfinite_pairs"] == 0
                and numeric["max_abs"] <= max_abs
                and numeric["normalized_rms"] <= max_normalized_rms
                and numeric["cosine_similarity"] >= min_cosine
            )
            integrity_pass = all(
                value == 0
                for group in integrity.values()
                for value in group.values()
            )
            timing = None
            if iterations:
                timing = paired_timing(
                    cuda,
                    lambda g=geometry: launch_complete(
                        cuda, functions.complete, case, buffers, g
                    ),
                    lambda: launch_baseline(cuda, functions, case, buffers),
                    iterations,
                    timing_pairs,
                    max_cv,
                )
            timing_pass = timing is None or (
                timing["candidate_cv"] <= max_cv
                and timing["baseline_cv"] <= max_cv
            )
            results.append(
                {
                    "geometry": {
                        "label": geometry.label,
                        "splits": geometry.splits,
                        "stage1_threads": geometry.stage1_threads,
                        "stage2_threads": geometry.stage2_threads,
                    },
                    "numeric_vs_canonical": numeric,
                    "integrity": integrity,
                    "timing": timing,
                    "pass": numeric_pass and integrity_pass and timing_pass,
                }
            )
        eligible = [result for result in results if result["pass"]]
        if not eligible:
            raise RuntimeError(f"no split-K geometry passed for {case.name}")
        selected_result = min(
            eligible,
            key=lambda result: (
                result["timing"]["candidate_us"] if result["timing"] else 0.0,
                result["geometry"]["splits"],
                result["geometry"]["stage1_threads"],
                result["geometry"]["stage2_threads"],
            ),
        )
        selected_geometry = Geometry(
            selected_result["geometry"]["splits"],
            selected_result["geometry"]["stage1_threads"],
            selected_result["geometry"]["stage2_threads"],
        )
        return {
            "case": case.name,
            "head_dim": case.head_dim,
            "kv_len": case.kv_len,
            "visible_count": case.visible_range[1] - case.visible_range[0],
            "selection_policy": "lowest_candidate_mean_among_numeric_integrity_cv_passes",
            "selected": selected_result,
            "selected_geometry": selected_geometry,
            "geometries": results,
        }
    finally:
        free_buffers(cuda, buffers)


def run_case(
    cuda: Cuda,
    functions: HeadFunctions,
    case: Case,
    geometry: Geometry,
    iterations: int,
    timing_pairs: int,
    max_abs: float,
    max_normalized_rms: float,
    min_cosine: float,
    max_cpu_sample_abs: float,
    max_cv: float,
) -> dict[str, object]:
    buffers = prepare_buffers(cuda, case)
    try:
        reset_candidate_buffers(cuda, buffers)
        launch_stage1(cuda, functions.stage1, case, buffers, geometry)
        cuda.synchronize()
        partial_values_before = cuda.download(
            buffers.partial_values.allocation, len(buffers.partial_values.image)
        )
        partial_max_before = cuda.download(
            buffers.partial_max.allocation, len(buffers.partial_max.image)
        )
        partial_denom_before = cuda.download(
            buffers.partial_denom.allocation, len(buffers.partial_denom.image)
        )
        launch_stage2(cuda, functions.stage2, case, buffers, geometry)
        cuda.synchronize()
        two_stage_first = cuda.download(
            buffers.candidate.allocation, len(buffers.candidate.image)
        )
        partial_values_after = cuda.download(
            buffers.partial_values.allocation, len(buffers.partial_values.image)
        )
        partial_max_after = cuda.download(
            buffers.partial_max.allocation, len(buffers.partial_max.image)
        )
        partial_denom_after = cuda.download(
            buffers.partial_denom.allocation, len(buffers.partial_denom.image)
        )

        baseline_first: bytes | None = None
        score_before_consumer: bytes | None = None
        score_after_consumer: bytes | None = None
        if not case.candidate_only:
            reset_baseline_buffers(cuda, buffers)
            launch_score(cuda, functions.score, case, buffers)
            cuda.synchronize()
            score_before_consumer = cuda.download(
                buffers.scores.allocation, len(buffers.scores.image)
            )
            launch_tiled(cuda, functions.tiled, case, buffers)
            cuda.synchronize()
            score_after_consumer = cuda.download(
                buffers.scores.allocation, len(buffers.scores.image)
            )
            baseline_first = cuda.download(
                buffers.baseline.allocation, len(buffers.baseline.image)
            )

        reset_candidate_buffers(cuda, buffers)
        launch_complete(cuda, functions.complete, case, buffers, geometry)
        cuda.synchronize()
        candidate_first = cuda.download(
            buffers.candidate.allocation, len(buffers.candidate.image)
        )
        completion_counters_first = cuda.download(
            buffers.completion_counters.allocation,
            len(buffers.completion_counters.image),
        )
        reset_candidate_buffers(cuda, buffers)
        launch_complete(cuda, functions.complete, case, buffers, geometry)
        cuda.synchronize()
        candidate_second = cuda.download(
            buffers.candidate.allocation, len(buffers.candidate.image)
        )
        if not case.candidate_only:
            reset_baseline_buffers(cuda, buffers)
            launch_baseline(cuda, functions, case, buffers)
            cuda.synchronize()
            baseline_second = cuda.download(
                buffers.baseline.allocation, len(buffers.baseline.image)
            )
        else:
            baseline_second = None

        candidate_payload = payload(candidate_first)
        candidate_repeat = diff_f32(candidate_payload, payload(candidate_second))
        two_stage_equivalence = diff_f32(
            payload(two_stage_first), candidate_payload
        )
        if baseline_first is None:
            zero_reference = bytes(len(candidate_payload))
            numeric = diff_f32(zero_reference, candidate_payload)
            baseline_repeat = None
            reference_label = "all-invalid-pages-zero-oracle"
        else:
            numeric = diff_f32(payload(baseline_first), candidate_payload)
            baseline_repeat = diff_f32(
                payload(baseline_first), payload(baseline_second)
            )
            reference_label = "canonical-score-plus-tiled64"

        used_values = HEADS * geometry.splits * case.head_dim
        used_scalars = HEADS * geometry.splits
        integrity = {
            "candidate_output": inspect_guarded(
                candidate_first,
                buffers.candidate,
                poison_bits=OUTPUT_POISON_BITS,
            ),
            "two_stage_output": inspect_guarded(
                two_stage_first,
                buffers.candidate,
                poison_bits=OUTPUT_POISON_BITS,
            ),
            "completion_counters_rearmed": {
                "mutations": sum(
                    left != right
                    for left, right in zip(
                        completion_counters_first,
                        buffers.completion_counters.image,
                    )
                )
            },
            "partial_values": inspect_guarded(
                partial_values_before,
                buffers.partial_values,
                poison_bits=PARTIAL_VALUE_POISON_BITS,
                used_elements=used_values,
                require_unused_poison=True,
            ),
            "partial_max": inspect_guarded(
                partial_max_before,
                buffers.partial_max,
                poison_bits=PARTIAL_MAX_POISON_BITS,
                used_elements=used_scalars,
                require_unused_poison=True,
            ),
            "partial_denom": inspect_guarded(
                partial_denom_before,
                buffers.partial_denom,
                poison_bits=PARTIAL_DENOM_POISON_BITS,
                used_elements=used_scalars,
                require_unused_poison=True,
            ),
            "stage2_readonly": {
                "partial_value_mutations": sum(
                    left != right
                    for left, right in zip(partial_values_before, partial_values_after)
                ),
                "partial_max_mutations": sum(
                    left != right
                    for left, right in zip(partial_max_before, partial_max_after)
                ),
                "partial_denom_mutations": sum(
                    left != right
                    for left, right in zip(partial_denom_before, partial_denom_after)
                ),
            },
            "baseline_output": (
                {"guard_mutations": 0, "used_poison_elements": 0, "used_nonfinite_elements": 0, "unused_mutations": 0}
                if baseline_first is None
                else inspect_guarded(
                    baseline_first,
                    buffers.baseline,
                    poison_bits=OUTPUT_POISON_BITS,
                )
            ),
            "baseline_score_readonly": {
                "mutations": (
                    0
                    if score_before_consumer is None
                    else sum(
                        left != right
                        for left, right in zip(
                            score_before_consumer, score_after_consumer
                        )
                    )
                )
            },
            "inputs": input_integrity(cuda, buffers),
        }
        run_cpu_oracle = (
            case.candidate_only
            or case.pattern == "random"
            or case.visible_range[0] == case.visible_range[1]
        )
        cpu = (
            cpu_sample(
                case,
                buffers,
                candidate_payload,
                None if baseline_first is None else payload(baseline_first),
            )
            if run_cpu_oracle
            else {
                "sample_count": 0,
                "skipped": True,
                "reason": "canonical GPU baseline covers this arithmetic pattern; CPU routing oracle is sampled once per layout/shape family",
                "candidate_max_abs": 0.0,
                "baseline_max_abs": None,
                "nonfinite": 0,
                "samples": [],
            }
        )
        timing = None
        if case.timing_anchor and iterations:
            stage1 = lambda: launch_stage1(
                cuda, functions.stage1, case, buffers, geometry
            )
            stage2 = lambda: launch_stage2(
                cuda, functions.stage2, case, buffers, geometry
            )
            two_stage = lambda: launch_candidate(
                cuda, functions, case, buffers, geometry
            )
            candidate = lambda: launch_complete(
                cuda, functions.complete, case, buffers, geometry
            )
            baseline = lambda: launch_baseline(cuda, functions, case, buffers)
            timing = {
                "stage1": operation_timing(cuda, stage1, iterations, timing_pairs),
                "stage2": operation_timing(cuda, stage2, iterations, timing_pairs),
                "two_kernel_pipeline": operation_timing(
                    cuda, two_stage, iterations, timing_pairs
                ),
                "full_pipeline_vs_canonical": paired_timing(
                    cuda,
                    candidate,
                    baseline,
                    iterations,
                    timing_pairs,
                    max_cv,
                ),
            }

        numeric_pass = (
            numeric["nonfinite_pairs"] == 0
            and numeric["max_abs"] <= max_abs
            and numeric["normalized_rms"] <= max_normalized_rms
            and numeric["cosine_similarity"] >= min_cosine
        )
        determinism_pass = (
            candidate_repeat["bitwise_mismatches"] == 0
            and candidate_repeat["nonfinite_pairs"] == 0
            and (
                baseline_repeat is None
                or (
                    baseline_repeat["bitwise_mismatches"] == 0
                    and baseline_repeat["nonfinite_pairs"] == 0
                )
            )
        )
        equivalence_pass = (
            two_stage_equivalence["bitwise_mismatches"] == 0
            and two_stage_equivalence["nonfinite_pairs"] == 0
        )
        integrity_pass = all(
            value == 0
            for group in integrity.values()
            for value in group.values()
        )
        cpu_pass = (
            cpu["nonfinite"] == 0
            and cpu["candidate_max_abs"] <= max_cpu_sample_abs
            and (
                cpu["baseline_max_abs"] is None
                or cpu["baseline_max_abs"] <= max_cpu_sample_abs
            )
        )
        # Only the AB/BA end-to-end pipeline pair is a stability gate.  The
        # isolated stage timings are diagnostic: at 10--20 us, launch/clock
        # quantization can give them a high CV even while the complete paired
        # measurement is stable to well below one percent.
        timing_pass = timing is None or (
            timing["full_pipeline_vs_canonical"]["candidate_cv"] <= max_cv
            and timing["full_pipeline_vs_canonical"]["baseline_cv"] <= max_cv
        )
        return {
            "name": case.name,
            "head_dim": case.head_dim,
            "kv_len": case.kv_len,
            "query_position": case.query_position,
            "kv_position_offset": case.kv_position_offset,
            "sliding_window": case.sliding_window,
            "visible_begin": case.visible_range[0],
            "visible_end": case.visible_range[1],
            "visible_count": case.visible_range[1] - case.visible_range[0],
            "layout": case.layout,
            "pattern": case.pattern,
            "candidate_only": case.candidate_only,
            "reference": reference_label,
            "geometry": {
                "splits": geometry.splits,
                "stage1_threads": geometry.stage1_threads,
                "stage2_threads": geometry.stage2_threads,
            },
            "numeric_vs_reference": numeric,
            "cpu_routing_sample": cpu,
            "candidate_determinism": candidate_repeat,
            "two_stage_vs_last_cta_completion": two_stage_equivalence,
            "baseline_determinism": baseline_repeat,
            "integrity": integrity,
            "timing": timing,
            "timing_gate_scope": "AB/BA full pipeline; stage timings diagnostic only",
            "gates": {
                "numeric": numeric_pass,
                "determinism": determinism_pass,
                "two_stage_equivalence": equivalence_pass,
                "integrity": integrity_pass,
                "cpu_routing": cpu_pass,
                "timing_stability": timing_pass,
            },
            "pass": numeric_pass
            and determinism_pass
            and equivalence_pass
            and integrity_pass
            and cpu_pass
            and timing_pass,
        }
    finally:
        free_buffers(cuda, buffers)


def abi_rejection_audit(
    cuda: Cuda,
    functions: HeadFunctions,
    head_dim: int,
    geometry: Geometry,
) -> dict[str, object]:
    case = Case(
        name=f"abi-rejection-hd{head_dim}",
        head_dim=head_dim,
        kv_len=17,
        query_position=53,
        kv_position_offset=37,
        sliding_window=512 if head_dim == 256 else 0,
        layout="explicit-permuted",
        pattern="adversarial",
        seed=0x510E527FADE682D1 ^ head_dim,
    )
    buffers = prepare_buffers(cuda, case)
    try:
        tests: dict[str, dict[str, int]] = {}

        def stage1_rejection(
            name: str,
            test_geometry: Geometry = geometry,
            *,
            key_format: int = 2,
            base_key_row_bytes: int | None = None,
            page_size: int = PAGE_SIZE,
        ) -> None:
            for guarded in (
                buffers.partial_values,
                buffers.partial_max,
                buffers.partial_denom,
            ):
                reset_guarded(cuda, guarded)
            launch_stage1(
                cuda,
                functions.stage1,
                case,
                buffers,
                test_geometry,
                key_format=key_format,
                base_key_row_bytes=base_key_row_bytes,
                page_size=page_size,
            )
            cuda.synchronize()
            tests[name] = {}
            for field in ("partial_values", "partial_max", "partial_denom"):
                guarded = getattr(buffers, field)
                current = cuda.download(guarded.allocation, len(guarded.image))
                tests[name][f"{field}_mutations"] = sum(
                    left != right for left, right in zip(current, guarded.image)
                )

        stage1_rejection("unsupported-key-format", key_format=0)
        stage1_rejection(
            "mismatched-base-row-bytes",
            base_key_row_bytes=buffers.key_row_bytes + 2,
        )
        stage1_rejection("zero-page-size", page_size=0)
        stage1_rejection(
            "non-power-of-two-splits",
            Geometry(3, geometry.stage1_threads, geometry.stage2_threads),
        )

        reset_candidate_buffers(cuda, buffers)
        launch_complete(
            cuda,
            functions.complete,
            case,
            buffers,
            geometry,
            key_format=0,
        )
        cuda.synchronize()
        tests["completion-unsupported-key-format"] = {}
        for field in (
            "candidate",
            "partial_values",
            "partial_max",
            "partial_denom",
            "completion_counters",
        ):
            guarded = getattr(buffers, field)
            current = cuda.download(guarded.allocation, len(guarded.image))
            tests["completion-unsupported-key-format"][f"{field}_mutations"] = sum(
                left != right for left, right in zip(current, guarded.image)
            )

        reset_guarded(cuda, buffers.candidate)
        launch_stage2(
            cuda,
            functions.stage2,
            case,
            buffers,
            geometry,
            head_dim=head_dim + 1,
        )
        cuda.synchronize()
        output = cuda.download(buffers.candidate.allocation, len(buffers.candidate.image))
        tests["stage2-head-dim-mismatch"] = {
            "output_mutations": sum(
                left != right for left, right in zip(output, buffers.candidate.image)
            )
        }
        inputs = input_integrity(cuda, buffers)
        passed = all(
            value == 0
            for group in tests.values()
            for value in group.values()
        ) and all(value == 0 for value in inputs.values())
        return {
            "head_dim": head_dim,
            "tests": tests,
            "inputs": inputs,
            "pass": passed,
        }
    finally:
        free_buffers(cuda, buffers)


def locked_cases() -> list[Case]:
    cases: list[Case] = []
    layouts = ("identity-null", "explicit-reversed", "explicit-permuted")
    patterns = ("random", "near-tie", "cancellation", "adversarial")
    ordinal = 0
    for head_dim in (256, 512):
        for layout in layouts:
            for pattern in patterns:
                cases.append(
                    Case(
                        name=f"locked-hd{head_dim}-{layout}-{pattern}",
                        head_dim=head_dim,
                        kv_len=2350,
                        query_position=2349,
                        kv_position_offset=0,
                        sliding_window=512 if head_dim == 256 else 0,
                        layout=layout,
                        pattern=pattern,
                        seed=0x6A09E667F3BCC909 ^ (head_dim << 20) ^ ordinal,
                        timing_anchor=layout == "identity-null" and pattern == "random",
                    )
                )
                ordinal += 1
    return cases


def boundary_cases() -> list[Case]:
    layouts = ("identity-null", "explicit-reversed", "explicit-permuted")
    patterns = ("random", "near-tie", "cancellation", "adversarial")
    shapes = {
        256: (1, 15, 16, 17, 127, 128, 129, 511, 512, 513, 1024, 2350),
        512: (
            1,
            15,
            16,
            17,
            127,
            128,
            129,
            511,
            512,
            513,
            1023,
            1024,
            2048,
            2349,
            2350,
            4095,
            4096,
        ),
    }
    cases: list[Case] = []
    ordinal = 0
    for head_dim, lengths in shapes.items():
        for kv_len in lengths:
            kv_offset = 37 if kv_len in (17, 513, 4095) else 0
            cases.append(
                Case(
                    name=f"boundary-hd{head_dim}-kv{kv_len}-offset{kv_offset}",
                    head_dim=head_dim,
                    kv_len=kv_len,
                    query_position=kv_offset + kv_len - 1,
                    kv_position_offset=kv_offset,
                    sliding_window=512 if head_dim == 256 else 0,
                    layout=layouts[ordinal % len(layouts)],
                    pattern=patterns[ordinal % len(patterns)],
                    seed=0xBB67AE8584CAA73B ^ ordinal,
                    timing_anchor=head_dim == 512 and kv_len == 4096,
                )
            )
            ordinal += 1
    cases.extend(
        [
            Case(
                name="boundary-hd256-partial-visible-window",
                head_dim=256,
                kv_len=2350,
                query_position=511,
                kv_position_offset=0,
                sliding_window=512,
                layout="explicit-permuted",
                pattern="cancellation",
                seed=0x3C6EF372FE94F82B,
            ),
            Case(
                name="boundary-hd512-partial-visible-offset",
                head_dim=512,
                kv_len=2350,
                query_position=2084,
                kv_position_offset=37,
                sliding_window=0,
                layout="explicit-reversed",
                pattern="near-tie",
                seed=0xA54FF53A5F1D36F1,
            ),
            Case(
                name="boundary-hd256-empty-visible",
                head_dim=256,
                kv_len=17,
                query_position=36,
                kv_position_offset=37,
                sliding_window=512,
                layout="explicit-permuted",
                pattern="adversarial",
                seed=0x1F83D9ABFB41BD6B,
            ),
            Case(
                name="boundary-hd512-empty-visible",
                head_dim=512,
                kv_len=17,
                query_position=36,
                kv_position_offset=37,
                sliding_window=0,
                layout="explicit-reversed",
                pattern="random",
                seed=0x5BE0CD19137E2179,
            ),
            Case(
                name="safety-hd256-all-pages-out-of-capacity",
                head_dim=256,
                kv_len=129,
                query_position=128,
                kv_position_offset=0,
                sliding_window=512,
                layout="explicit-invalid",
                pattern="adversarial",
                seed=0x243F6A8885A308D3,
                candidate_only=True,
            ),
            Case(
                name="safety-hd512-all-pages-out-of-capacity",
                head_dim=512,
                kv_len=129,
                query_position=128,
                kv_position_offset=0,
                sliding_window=0,
                layout="explicit-invalid",
                pattern="adversarial",
                seed=0x13198A2E03707344,
                candidate_only=True,
            ),
        ]
    )
    return cases


def visible_score_payload(image: bytes, case: Case, score_capacity: int) -> bytes:
    """Pack only score elements consumed by the canonical tiled64 kernel."""

    visible_count = case.visible_range[1] - case.visible_range[0]
    body = payload(image)
    packed = bytearray()
    for head in range(HEADS):
        begin = (head * score_capacity) * 4
        end = begin + visible_count * 4
        packed.extend(body[begin:end])
    return bytes(packed)


def inspect_visible_scores(
    image: bytes,
    guarded: Guarded,
    case: Case,
    score_capacity: int,
) -> dict[str, int]:
    if len(image) != len(guarded.image):
        raise ValueError("guarded score image length changed")
    guard_mutations = sum(value != CANARY for value in image[:GUARD_BYTES])
    guard_mutations += sum(value != CANARY for value in image[-GUARD_BYTES:])
    visible = visible_score_payload(image, case, score_capacity)
    return {
        "guard_mutations": guard_mutations,
        "used_poison_elements": sum(
            struct.unpack_from("<I", visible, index * 4)[0] == SCORE_POISON_BITS
            for index in range(len(visible) // 4)
        ),
        "used_nonfinite_elements": sum(
            not math.isfinite(struct.unpack_from("<f", visible, index * 4)[0])
            for index in range(len(visible) // 4)
        ),
    }


def reset_parity_candidate(cuda: Cuda, buffers: Buffers, *, exact: bool) -> None:
    reset_candidate_buffers(cuda, buffers)
    if exact:
        reset_guarded(cuda, buffers.scores)


def parity_matrix_cases(
    head_dims: Iterable[int],
    kv_lengths: Iterable[int],
) -> list[Case]:
    cases: list[Case] = []
    for head_dim in head_dims:
        for kv_len in kv_lengths:
            cases.append(
                Case(
                    name=f"parity-hd{head_dim}-kv{kv_len}-random",
                    head_dim=head_dim,
                    kv_len=kv_len,
                    query_position=kv_len - 1,
                    kv_position_offset=0,
                    sliding_window=512 if head_dim == 256 else 0,
                    layout="identity-null",
                    pattern="random",
                    seed=0x9E3779B97F4A7C15 ^ (head_dim << 32) ^ kv_len,
                )
            )
    return cases


def run_parity_matrix_case(
    cuda: Cuda,
    functions: HeadFunctions,
    case: Case,
    iterations: int,
    timing_pairs: int,
    max_cv: float,
) -> dict[str, object]:
    """Run A/B isolation controls and parity architecture E.

    A/B/C/D use the existing online-softmax candidate.  E changes only the
    standalone prototype: parallel score CTAs use the canonical dot tree and
    the last CTA executes the canonical chronological consumer recurrence.
    """

    buffers = prepare_buffers(cuda, case)
    try:
        reset_baseline_buffers(cuda, buffers)
        launch_baseline(cuda, functions, case, buffers)
        cuda.synchronize()
        baseline_image = cuda.download(
            buffers.baseline.allocation, len(buffers.baseline.image)
        )
        baseline_score_image = cuda.download(
            buffers.scores.allocation, len(buffers.scores.image)
        )
        baseline_output = payload(baseline_image)
        baseline_scores = visible_score_payload(
            baseline_score_image, case, buffers.score_capacity
        )

        baseline_threads = case.head_dim
        controls = [
            {
                "id": "A-online-split1-baseline-dot-tree",
                "algorithm": "splitk-online",
                "splits": 1,
                "threads": baseline_threads,
                "isolation": "no split grouping; canonical QK reduction width",
            },
            {
                "id": "B-online-split1-t128-dot-tree",
                "algorithm": "splitk-online",
                "splits": 1,
                "threads": 128,
                "isolation": "no split grouping; production t128 QK reduction",
            },
            {
                "id": "C-online-split64-baseline-dot-tree",
                "algorithm": "splitk-online",
                "splits": 64,
                "threads": baseline_threads,
                "isolation": "64-way softmax grouping with canonical QK reduction width",
            },
            {
                "id": "D-online-split64-t128-production-order",
                "algorithm": "splitk-online",
                "splits": 64,
                "threads": 128,
                "isolation": "production arithmetic order",
            },
            {
                "id": "G-online-split64-t128-f64-partial-merge",
                "algorithm": "splitk-online-accurate-merge",
                "splits": 64,
                "threads": 128,
                "isolation": (
                    "production topology with F64 denominator/value partial merge"
                ),
            },
            {
                "id": "E-score-lastcta-exact-split1",
                "algorithm": "score-lastcta-exact",
                "splits": 1,
                "threads": baseline_threads,
                "isolation": "exact recurrence control without parallel score CTAs",
            },
            {
                "id": "E-score-lastcta-exact-split32",
                "algorithm": "score-lastcta-exact",
                "splits": 32,
                "threads": baseline_threads,
                "isolation": "parallel exact-score production plus exact recurrence",
            },
            {
                "id": "E-score-lastcta-exact-split64",
                "algorithm": "score-lastcta-exact",
                "splits": 64,
                "threads": baseline_threads,
                "isolation": "parallel exact-score production plus exact recurrence",
            },
            {
                "id": "F-score-parallel-exact-tiled64-split32",
                "algorithm": "score-parallel-exact+tiled64",
                "splits": 32,
                "threads": baseline_threads,
                "isolation": (
                    "parallel exact-score production plus canonical tiled64 consumer"
                ),
            },
            {
                "id": "F-score-parallel-exact-tiled64-split64",
                "algorithm": "score-parallel-exact+tiled64",
                "splits": 64,
                "threads": baseline_threads,
                "isolation": (
                    "parallel exact-score production plus canonical tiled64 consumer"
                ),
            },
        ]
        results: list[dict[str, object]] = []
        for control in controls:
            exact = control["algorithm"] in (
                "score-lastcta-exact",
                "score-parallel-exact+tiled64",
            )
            geometry = Geometry(
                int(control["splits"]),
                int(control["threads"]),
                min(case.head_dim, 256),
            )

            def launch_control() -> None:
                if control["algorithm"] == "score-lastcta-exact":
                    launch_parity_exact(
                        cuda,
                        functions.parity_exact,
                        case,
                        buffers,
                        geometry.splits,
                    )
                elif control["algorithm"] == "score-parallel-exact+tiled64":
                    launch_parity_exact(
                        cuda,
                        functions.parity_score,
                        case,
                        buffers,
                        geometry.splits,
                    )
                    launch_tiled_to(
                        cuda,
                        functions.tiled,
                        case,
                        buffers,
                        buffers.candidate,
                    )
                elif control["algorithm"] == "splitk-online-accurate-merge":
                    launch_complete(
                        cuda,
                        functions.accurate_merge,
                        case,
                        buffers,
                        geometry,
                    )
                else:
                    launch_complete(
                        cuda, functions.complete, case, buffers, geometry
                    )

            reset_parity_candidate(cuda, buffers, exact=exact)
            launch_control()
            cuda.synchronize()
            first_image = cuda.download(
                buffers.candidate.allocation, len(buffers.candidate.image)
            )
            first_score_image = (
                cuda.download(buffers.scores.allocation, len(buffers.scores.image))
                if exact
                else None
            )
            counters = cuda.download(
                buffers.completion_counters.allocation,
                len(buffers.completion_counters.image),
            )

            launch_control()
            cuda.synchronize()
            repeat_image = cuda.download(
                buffers.candidate.allocation, len(buffers.candidate.image)
            )
            output_diff = diff_f32(baseline_output, payload(first_image))
            determinism = diff_f32(payload(first_image), payload(repeat_image))
            score_diff = (
                diff_f32(
                    baseline_scores,
                    visible_score_payload(
                        first_score_image, case, buffers.score_capacity
                    ),
                )
                if first_score_image is not None
                else None
            )
            integrity: dict[str, dict[str, int]] = {
                "output": inspect_guarded(
                    first_image,
                    buffers.candidate,
                    poison_bits=OUTPUT_POISON_BITS,
                ),
                "completion_counters_rearmed": {
                    "mutations": sum(
                        left != right
                        for left, right in zip(
                            counters, buffers.completion_counters.image
                        )
                    )
                },
                "inputs": input_integrity(cuda, buffers),
            }
            if first_score_image is not None:
                integrity["scores"] = inspect_visible_scores(
                    first_score_image,
                    buffers.scores,
                    case,
                    buffers.score_capacity,
                )
            integrity_pass = all(
                value == 0
                for group in integrity.values()
                for value in group.values()
            )
            timing = (
                paired_timing(
                    cuda,
                    launch_control,
                    lambda: launch_baseline(cuda, functions, case, buffers),
                    iterations,
                    timing_pairs,
                    max_cv,
                )
                if iterations
                else None
            )
            exact_bitwise_pass = (
                not exact
                or (
                    output_diff["bitwise_mismatches"] == 0
                    and score_diff is not None
                    and score_diff["bitwise_mismatches"] == 0
                )
            )
            result = {
                **control,
                "geometry": {
                    "splits": geometry.splits,
                    "threads": geometry.stage1_threads,
                },
                "output_vs_canonical": output_diff,
                "score_vs_canonical": score_diff,
                "determinism": determinism,
                "integrity": integrity,
                "timing": timing,
                "exact_bitwise_gate": exact_bitwise_pass,
                "pass": (
                    determinism["bitwise_mismatches"] == 0
                    and integrity_pass
                    and exact_bitwise_pass
                ),
            }
            results.append(result)
            timing_text = (
                "untimed"
                if timing is None
                else f"{timing['candidate_us']:.3f}us/{timing['baseline_us']:.3f}us"
            )
            print(
                f"  {control['id']}: output_bits={output_diff['bitwise_mismatches']} "
                f"max_abs={output_diff['max_abs']:.3e} {timing_text}",
                flush=True,
            )

        return {
            "name": case.name,
            "head_dim": case.head_dim,
            "kv_len": case.kv_len,
            "visible_count": case.visible_range[1] - case.visible_range[0],
            "controls": results,
            "exact_candidates_pass": all(
                result["pass"]
                for result in results
                if result["algorithm"] in (
                    "score-lastcta-exact",
                    "score-parallel-exact+tiled64",
                )
            ),
        }
    finally:
        free_buffers(cuda, buffers)


def run_parity_matrix_only(args: argparse.Namespace) -> int:
    head_dims = [args.head_dim] if args.head_dim else [256, 512]
    kv_lengths = args.parity_kv_len or [2188]
    cases = parity_matrix_cases(head_dims, kv_lengths)
    cuda = Cuda()
    results: list[dict[str, object]] = []
    device = {
        "ordinal": 0,
        "name": cuda.device_name,
        "compute_capability": cuda.compute_capability,
        "cuda_driver_version": cuda.driver_version,
    }
    try:
        functions = load_functions(cuda, args.candidate_cubin, args.baseline_cubin)
        for index, case in enumerate(cases, 1):
            print(f"[{index}/{len(cases)}] {case.name}", flush=True)
            results.append(
                run_parity_matrix_case(
                    cuda,
                    functions[case.head_dim],
                    case,
                    args.iterations,
                    args.timing_pairs,
                    args.max_timing_cv,
                )
            )
    finally:
        cuda.close()

    inference_dir = Path(__file__).resolve().parent.parent
    implementation_files = {
        "source": inference_dir
        / "src/ops/cuda/prototypes/gqa_decode_splitk_online_sm89.cu",
        "harness": Path(__file__).resolve(),
        "build_script": inference_dir
        / "scripts/build_cuda_gqa_decode_splitk_online_prototype.sh",
    }
    exact_pass = all(result["exact_candidates_pass"] for result in results)
    evidence = {
        "schema": "antfly.cuda.gqa-decode-splitk-parity-diagnosis.v1",
        "default_off": True,
        "runtime_wiring": False,
        "device": device,
        "implementation": {
            name: {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for name, path in implementation_files.items()
        },
        "candidate_cubin": {
            "path": str(args.candidate_cubin.resolve()),
            "bytes": args.candidate_cubin.stat().st_size,
            "sha256": sha256_file(args.candidate_cubin),
            "parity_symbols": [
                PARITY_EXACT_HD256,
                PARITY_EXACT_HD512,
                PARITY_SCORE_HD256,
                PARITY_SCORE_HD512,
                ACCURATE_MERGE_HD256,
                ACCURATE_MERGE_HD512,
            ],
        },
        "baseline_cubin": {
            "path": str(args.baseline_cubin.resolve()),
            "bytes": args.baseline_cubin.stat().st_size,
            "sha256": sha256_file(args.baseline_cubin),
        },
        "execution_contract": {
            "batch": 1,
            "continuous_batching": False,
            "stream_order": "single serialized CUDA stream",
            "workspace_lifetime": (
                "standalone shared workspace is safe only under serialized launches; "
                "production concurrency requires request- or stream-scoped score and "
                "counter workspace"
            ),
            "fast_math": False,
            "page_size": PAGE_SIZE,
            "heads": HEADS,
            "kv_heads": KV_HEADS,
        },
        "candidate_matrix": {
            "A": "split1 plus canonical dot width isolates recurrence source/order",
            "B": "split1 plus t128 isolates the QK dot tree",
            "C": "split64 plus canonical dot width isolates softmax/value regrouping",
            "D": "split64 plus t128 reproduces production arithmetic order",
            "E": (
                "parallel canonical-order score production plus last-CTA canonical "
                "chronological consumer recurrence"
            ),
            "F": (
                "parallel canonical-order score production plus the canonical "
                "tiled64 consumer"
            ),
            "G": "production split topology with an F64 fixed-order partial merge",
        },
        "cases": results,
        "exact_candidate_bitwise_pass": exact_pass,
        "pass": exact_pass,
    }
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
        print(f"wrote {args.json_out}", flush=True)
    else:
        print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0 if exact_pass else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-cubin", type=Path, required=True)
    parser.add_argument("--baseline-cubin", type=Path, required=True)
    parser.add_argument(
        "--suite", choices=("locked", "boundaries", "all"), default="all"
    )
    parser.add_argument("--head-dim", choices=(256, 512), type=int)
    parser.add_argument(
        "--parity-matrix-only",
        action="store_true",
        help=(
            "run only the A/B split-order diagnosis and exact-score last-CTA "
            "candidate matrix"
        ),
    )
    parser.add_argument(
        "--parity-kv-len",
        action="append",
        type=int,
        help=(
            "KV length for --parity-matrix-only; repeat for multiple lengths "
            "(default: 2188, the approximate first divergent decode position)"
        ),
    )
    parser.add_argument("--iterations", type=int, default=0)
    parser.add_argument("--timing-pairs", type=int, default=7)
    parser.add_argument("--max-timing-cv", type=float, default=0.10)
    parser.add_argument("--max-abs", type=float, default=5e-3)
    parser.add_argument("--max-normalized-rms", type=float, default=1e-3)
    parser.add_argument("--min-cosine", type=float, default=0.999999)
    parser.add_argument("--max-cpu-sample-abs", type=float, default=5e-3)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args()
    if args.iterations < 0 or args.iterations > 100000:
        parser.error("--iterations must be in [0, 100000]")
    if args.timing_pairs < 2 or args.timing_pairs > 31:
        parser.error("--timing-pairs must be in [2, 31]")
    if args.parity_kv_len is not None and not args.parity_matrix_only:
        parser.error("--parity-kv-len requires --parity-matrix-only")
    if args.parity_kv_len is not None and any(
        value < 1 or value > 4096 for value in args.parity_kv_len
    ):
        parser.error("--parity-kv-len values must be in [1, 4096]")
    for name in (
        "max_timing_cv",
        "max_abs",
        "max_normalized_rms",
        "max_cpu_sample_abs",
    ):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0.0:
            parser.error(f"--{name.replace('_', '-')} must be finite and non-negative")
    if (
        not math.isfinite(args.min_cosine)
        or args.min_cosine < -1.0
        or args.min_cosine > 1.0
    ):
        parser.error("--min-cosine must be finite and in [-1, 1]")
    for path in (args.candidate_cubin, args.baseline_cubin):
        if not path.is_file():
            parser.error(f"cubin does not exist: {path}")
    return args


def load_functions(
    cuda: Cuda,
    candidate_path: Path,
    baseline_path: Path,
) -> dict[int, HeadFunctions]:
    candidate_module = cuda.load_module(candidate_path)
    baseline_module = cuda.load_module(baseline_path)
    return {
        256: HeadFunctions(
            cuda.function(candidate_module, CANDIDATE_STAGE1_HD256),
            cuda.function(candidate_module, CANDIDATE_STAGE2_HD256),
            cuda.function(candidate_module, CANDIDATE_COMPLETE_HD256),
            cuda.function(candidate_module, PARITY_EXACT_HD256),
            cuda.function(candidate_module, PARITY_SCORE_HD256),
            cuda.function(candidate_module, ACCURATE_MERGE_HD256),
            cuda.function(baseline_module, SCORE_HD256),
            cuda.function(baseline_module, TILED_HD256),
        ),
        512: HeadFunctions(
            cuda.function(candidate_module, CANDIDATE_STAGE1_HD512),
            cuda.function(candidate_module, CANDIDATE_STAGE2_HD512),
            cuda.function(candidate_module, CANDIDATE_COMPLETE_HD512),
            cuda.function(candidate_module, PARITY_EXACT_HD512),
            cuda.function(candidate_module, PARITY_SCORE_HD512),
            cuda.function(candidate_module, ACCURATE_MERGE_HD512),
            cuda.function(baseline_module, SCORE_HD512),
            cuda.function(baseline_module, TILED_HD512),
        ),
    }


def first_failure_diagnosis(result: dict[str, object]) -> str:
    for gate, passed in result["gates"].items():
        if not passed:
            if gate == "numeric":
                numeric = result["numeric_vs_reference"]
                return (
                    f"numeric drift exceeded gate: max_abs={numeric['max_abs']:.6g}, "
                    f"normalized_rms={numeric['normalized_rms']:.6g}, "
                    f"cosine={numeric['cosine_similarity']:.9g}"
                )
            return f"{gate} gate failed"
    return "unknown qualification failure"


def main() -> int:
    args = parse_args()
    if args.parity_matrix_only:
        return run_parity_matrix_only(args)
    selected_cases = (
        locked_cases()
        if args.suite == "locked"
        else boundary_cases()
        if args.suite == "boundaries"
        else locked_cases() + boundary_cases()
    )
    if args.head_dim:
        selected_cases = [
            case for case in selected_cases if case.head_dim == args.head_dim
        ]

    cuda = Cuda()
    results: list[dict[str, object]] = []
    sweeps: dict[str, dict[str, object]] = {}
    abi_audits: list[dict[str, object]] = []
    failure: str | None = None
    selected_geometries: dict[int, Geometry] = {}
    try:
        functions = load_functions(cuda, args.candidate_cubin, args.baseline_cubin)
        requested_heads = sorted({case.head_dim for case in selected_cases})
        for head_dim in requested_heads:
            sweep_case = Case(
                name=f"geometry-sweep-locked-hd{head_dim}-kv2350",
                head_dim=head_dim,
                kv_len=2350,
                query_position=2349,
                kv_position_offset=0,
                sliding_window=512 if head_dim == 256 else 0,
                layout="identity-null",
                pattern="random",
                seed=0xCBBB9D5DC1059ED8 ^ head_dim,
            )
            sweep = run_geometry_sweep(
                cuda,
                functions[head_dim],
                sweep_case,
                args.iterations,
                args.timing_pairs,
                args.max_abs,
                args.max_normalized_rms,
                args.min_cosine,
                args.max_timing_cv,
            )
            sweeps[f"locked_hd{head_dim}"] = sweep
            selected_geometries[head_dim] = sweep.pop("selected_geometry")
            chosen = selected_geometries[head_dim]
            selected_timing = sweep["selected"]["timing"]
            timing_text = (
                "untimed"
                if selected_timing is None
                else f"{selected_timing['candidate_us']:.3f}us, "
                f"{selected_timing['speedup']:.3f}x"
            )
            print(
                f"geometry hd{head_dim}: {chosen.label} ({timing_text})",
                flush=True,
            )

        if 512 in requested_heads:
            maximum_case = Case(
                name="geometry-sweep-hd512-kv4096",
                head_dim=512,
                kv_len=4096,
                query_position=4095,
                kv_position_offset=0,
                sliding_window=0,
                layout="identity-null",
                pattern="random",
                seed=0x629A292A367CD507,
            )
            maximum_sweep = run_geometry_sweep(
                cuda,
                functions[512],
                maximum_case,
                args.iterations,
                args.timing_pairs,
                args.max_abs,
                args.max_normalized_rms,
                args.min_cosine,
                args.max_timing_cv,
            )
            maximum_sweep.pop("selected_geometry")
            sweeps["maximum_hd512"] = maximum_sweep

        for head_dim in requested_heads:
            audit = abi_rejection_audit(
                cuda,
                functions[head_dim],
                head_dim,
                selected_geometries[head_dim],
            )
            abi_audits.append(audit)
            if not audit["pass"]:
                failure = f"HD{head_dim} invalid-ABI fail-closed audit failed"
                break

        if failure is None:
            for index, case in enumerate(selected_cases, 1):
                result = run_case(
                    cuda,
                    functions[case.head_dim],
                    case,
                    selected_geometries[case.head_dim],
                    args.iterations,
                    args.timing_pairs,
                    args.max_abs,
                    args.max_normalized_rms,
                    args.min_cosine,
                    args.max_cpu_sample_abs,
                    args.max_timing_cv,
                )
                results.append(result)
                numeric = result["numeric_vs_reference"]
                print(
                    f"[{index}/{len(selected_cases)}] {case.name}: "
                    f"max_abs={numeric['max_abs']:.3e} "
                    f"nrms={numeric['normalized_rms']:.3e} "
                    f"cos={numeric['cosine_similarity']:.9f} "
                    f"pass={str(result['pass']).lower()}",
                    flush=True,
                )
                if result["timing"]:
                    full = result["timing"]["full_pipeline_vs_canonical"]
                    print(
                        f"  stage1={result['timing']['stage1']['mean_us']:.3f}us "
                        f"stage2={result['timing']['stage2']['mean_us']:.3f}us "
                        f"full={full['candidate_us']:.3f}us/"
                        f"{full['baseline_us']:.3f}us "
                        f"speedup={full['speedup']:.3f}x",
                        flush=True,
                    )
                if not result["pass"]:
                    failure = first_failure_diagnosis(result)
                    print(f"qualification stopped: {failure}", file=sys.stderr)
                    break
    finally:
        cuda.close()

    inference_dir = Path(__file__).resolve().parent.parent
    implementation_files = {
        "source": inference_dir
        / "src/ops/cuda/prototypes/gqa_decode_splitk_online_sm89.cu",
        "harness": Path(__file__).resolve(),
        "build_script": inference_dir
        / "scripts/build_cuda_gqa_decode_splitk_online_prototype.sh",
    }
    evidence: dict[str, object] = {
        "schema": "antfly.cuda.gqa-decode-splitk-online.prototype.v1",
        "default_off": True,
        "runtime_wiring": False,
        "device": {
            "ordinal": 0,
            "name": cuda.device_name,
            "compute_capability": cuda.compute_capability,
            "cuda_driver_version": cuda.driver_version,
        },
        "implementation": {
            name: {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for name, path in implementation_files.items()
        },
        "candidate": {
            "path": str(args.candidate_cubin.resolve()),
            "bytes": args.candidate_cubin.stat().st_size,
            "sha256": sha256_file(args.candidate_cubin),
            "symbols": [
                CANDIDATE_STAGE1_HD256,
                CANDIDATE_STAGE1_HD512,
                CANDIDATE_STAGE2_HD256,
                CANDIDATE_STAGE2_HD512,
                CANDIDATE_COMPLETE_HD256,
                CANDIDATE_COMPLETE_HD512,
                PARITY_EXACT_HD256,
                PARITY_EXACT_HD512,
                PARITY_SCORE_HD256,
                PARITY_SCORE_HD512,
                ACCURATE_MERGE_HD256,
                ACCURATE_MERGE_HD512,
            ],
        },
        "baseline": {
            "path": str(args.baseline_cubin.resolve()),
            "bytes": args.baseline_cubin.stat().st_size,
            "sha256": sha256_file(args.baseline_cubin),
            "pipeline": "canonical-score-producer-plus-canonical-tiled64-consumer",
            "symbols": {
                "hd256": [SCORE_HD256, TILED_HD256],
                "hd512": [SCORE_HD512, TILED_HD512],
            },
        },
        "contract": {
            "batch": 1,
            "heads": HEADS,
            "kv_heads": KV_HEADS,
            "q_seq_len": 1,
            "q_format": "f32",
            "key_format": "paged-f16",
            "value_format": "paged-f16",
            "output_format": "f32",
            "page_size": PAGE_SIZE,
            "hd256_sliding_window": 512,
            "hd512_sliding_window": 0,
            "max_hd512_kv": 4096,
            "stable_chronological_stage1_and_stage2_merge": True,
            "headline_pipeline": "one launch: split CTAs plus deterministic last-CTA chronological merge",
            "independent_two_kernel_control_retained": True,
            "completion_protocol": "per-thread device fence, per-head atomic arrival counter, last CTA merges fixed split order and rearms counter",
            "fast_math": False,
            "numeric_not_bitwise_parity": True,
            "guards_poison_readonly_determinism": True,
            "timing_order": "alternating_AB_BA_cuda_events",
            "timing_retry_policy": "up to three fully recorded re-warmed attempts when either arm exceeds max CV",
        },
        "thresholds": {
            "max_abs": args.max_abs,
            "max_normalized_rms": args.max_normalized_rms,
            "min_cosine": args.min_cosine,
            "max_cpu_sample_abs": args.max_cpu_sample_abs,
            "max_timing_cv": args.max_timing_cv,
        },
        "suite": args.suite,
        "geometry_sweeps": sweeps,
        "selected_geometries": {
            str(head_dim): {
                "splits": geometry.splits,
                "stage1_threads": geometry.stage1_threads,
                "stage2_threads": geometry.stage2_threads,
            }
            for head_dim, geometry in selected_geometries.items()
        },
        "abi_rejection_audits": abi_audits,
        "requested_case_count": len(selected_cases),
        "completed_case_count": len(results),
        "cases": results,
        "failure_diagnosis": failure,
    }
    locked_timing = {
        result["head_dim"]: result["timing"]["full_pipeline_vs_canonical"]
        for result in results
        if result["name"].endswith("identity-null-random")
        and result["timing"] is not None
    }
    projection = None
    if 256 in locked_timing and 512 in locked_timing:
        candidate_us = (
            28 * locked_timing[256]["candidate_us"]
            + 7 * locked_timing[512]["candidate_us"]
        )
        baseline_us = (
            28 * locked_timing[256]["baseline_us"]
            + 7 * locked_timing[512]["baseline_us"]
        )
        projection = {
            "gemma4_layer_mix": {"hd256": 28, "hd512": 7},
            "candidate_us_per_token": candidate_us,
            "baseline_us_per_token": baseline_us,
            "speedup": baseline_us / candidate_us,
            "target_us_per_token": 2000.0,
            "target_met": candidate_us < 2000.0,
            "note": "kernel-only sequential layer projection; excludes graph and surrounding model ops",
        }
    evidence["locked_35_layer_attention_projection"] = projection
    locked_hd512 = locked_timing.get(512)
    material_speedup = (
        locked_hd512 is not None
        and locked_hd512["speedup"] >= 1.25
        and projection is not None
        and projection["target_met"]
    )
    evidence["performance_target_met"] = bool(material_speedup)
    evidence["pass"] = (
        failure is None
        and len(results) == len(selected_cases)
        and all(result["pass"] for result in results)
        and all(audit["pass"] for audit in abi_audits)
    )
    evidence["production_integration_recommended"] = bool(
        evidence["pass"] and evidence["performance_target_met"]
    )
    evidence["quality_gate_recommendation"] = (
        "Before runtime promotion, require teacher-forced logit/KL/top-k/margin checks "
        "and multi-fixture perplexity in addition to this kernel numeric gate; split-K "
        "changes legal F32 reduction and softmax grouping order."
    )
    rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    return 0 if evidence["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
