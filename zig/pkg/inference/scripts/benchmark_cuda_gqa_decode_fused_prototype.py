#!/usr/bin/env python3
"""Differential/timing harness for default-off exact q=1 GQA prototypes.

The candidate cubin and the checked-in canonical SM89 cubin are loaded as
separate CUDA modules.  The canonical score-prework producer followed by both
its serial and tiled64 consumers are the references.  The current candidate
keeps that score producer, materializes the chronological alpha/beta/denom
stream once per head, then consumes it from 64-column PV CTAs.  No runtime
dispatch or canonical artifact is modified by this harness.
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
SCORE_POISON_BITS = 0x7FC0D2FF
ALPHA_POISON_BITS = 0x7FC0D3FF
BETA_POISON_BITS = 0x7FC0D4FF
DENOM_POISON_BITS = 0x7FC0D5FF
F16_POISON_BITS = 0x7E00
CHUNK_COUNT = 128
COEFFICIENT_GEOMETRIES = (1, 32, 64, 128)

CANDIDATE_HD256 = (
    "antfly_gqa_attention_decode_fused_score_pv_hd256_swa512_f32_prototype"
)
CANDIDATE_HD512 = (
    "antfly_gqa_attention_decode_fused_score_pv_hd512_global_f32_prototype"
)
COEFFICIENT_HD256 = (
    "antfly_gqa_attention_decode_score_coefficients_hd256_swa512_f32_prototype"
)
COEFFICIENT_HD512 = (
    "antfly_gqa_attention_decode_score_coefficients_hd512_global_f32_prototype"
)
COEFFICIENT_PV_HD256 = (
    "antfly_gqa_attention_decode_coefficients_pv_shared_hd256_swa512_f32_prototype"
)
COEFFICIENT_PV_HD512 = (
    "antfly_gqa_attention_decode_coefficients_pv_shared_hd512_global_f32_prototype"
)
SCORE_HD256 = "antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1"
SERIAL_HD256 = (
    "antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1"
)
TILED_HD256 = (
    "antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd256_f32_v1"
)
SCORE_HD512 = "antfly_gqa_attention_decode_turboquant_score_prework_hd512_f32_v1"
SERIAL_HD512 = (
    "antfly_gqa_attention_decode_turboquant_score_prework_serial_hd512_f32_v1"
)
TILED_HD512 = (
    "antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd512_f32_v1"
)


class CudaError(RuntimeError):
    pass


class Cuda:
    """Small CUDA Driver API binding; the harness intentionally has no deps."""

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

    @property
    def visible_range(self) -> tuple[int, int]:
        if self.query_position < self.kv_position_offset:
            return (0, 0)
        end = min(
            self.query_position - self.kv_position_offset + 1, self.kv_len
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
class Buffers:
    q: Guarded
    k: Guarded
    v: Guarded
    table: Guarded | None
    scalars: Guarded
    candidate: Guarded
    serial: Guarded
    tiled: Guarded
    scores: Guarded
    alpha: Guarded
    beta: Guarded
    denom: Guarded
    score_capacity: int
    physical_capacity: int
    key_row_bytes: int
    value_row_bytes: int
    block_count: int


@dataclass(frozen=True)
class Functions:
    fused: ctypes.c_void_p
    coefficient: ctypes.c_void_p
    coefficient_pv: ctypes.c_void_p
    score: ctypes.c_void_p
    serial: ctypes.c_void_p
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


def poison_f32_payload(count: int, bits: int) -> bytes:
    return struct.pack("<I", bits) * count


def half_bytes(value: float) -> bytes:
    return struct.pack("<e", value)


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
    sign = 1.0 if (dimension & 1) == 0 else -1.0
    return sign * (0.125 + ((head * 13 + dimension) % 7) * 0.00001)


def k_value(case: Case, token: int, dimension: int) -> float:
    if case.pattern == "random":
        return deterministic_unit(case.seed ^ 0xA5A5A5A5, token, dimension) * 0.125
    if case.pattern == "near-tie":
        sign = 1.0 if (dimension & 1) == 0 else -1.0
        return sign * (0.03125 + ((token * 17 + dimension * 3) % 11) * 0.00000025)
    dimension_sign = 1.0 if (dimension & 1) == 0 else -1.0
    token_sign = 1.0 if (token & 1) == 0 else -1.0
    return dimension_sign * (
        token_sign * 0.125 + ((token * 19 + dimension) % 13) * 0.00001
    )


def v_value(case: Case, token: int, dimension: int) -> float:
    if case.pattern == "random":
        return deterministic_unit(case.seed ^ 0x5A5A5A5A, token, dimension) * 0.75
    if case.pattern == "near-tie":
        sign = 1.0 if ((token + dimension) & 1) == 0 else -1.0
        return sign * (0.25 + (token % 29) * 0.0005)
    token_sign = 1.0 if (token & 1) == 0 else -1.0
    return token_sign * (0.5 + (dimension % 17) * 0.0001)


def page_table(case: Case, block_count: int) -> list[int]:
    table = list(range(block_count))
    if case.layout == "explicit-reversed":
        table.reverse()
    elif case.layout == "explicit-permuted" and block_count > 1:
        table = [(index + 1) % block_count for index in range(block_count)]
    return table


def physical_token(logical: int, table: list[int] | None) -> int:
    if table is None:
        return logical
    return table[logical // PAGE_SIZE] * PAGE_SIZE + logical % PAGE_SIZE


def make_inputs(case: Case) -> tuple[bytes, bytes, bytes, bytes | None, bytes, int, int]:
    head_dim = case.head_dim
    block_count = (case.kv_len + PAGE_SIZE - 1) // PAGE_SIZE
    physical_capacity = block_count * PAGE_SIZE
    table = None if case.layout == "identity-null" else page_table(case, block_count)

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
    for logical in range(case.kv_len):
        physical = physical_token(logical, table)
        row = physical * head_dim
        for dimension in range(head_dim):
            struct.pack_into(
                "<e", k, (row + dimension) * 2, k_value(case, logical, dimension)
            )
            struct.pack_into(
                "<e", v, (row + dimension) * 2, v_value(case, logical, dimension)
            )

    table_bytes = (
        None
        if table is None
        else struct.pack(f"<{len(table)}I", *table)
    )
    total_sequence_len = max(
        case.query_position + 1, case.kv_position_offset + case.kv_len
    )
    scalars = struct.pack(
        "<5I",
        0,
        case.query_position,
        case.kv_len,
        total_sequence_len,
        case.kv_position_offset,
    )
    return bytes(q), bytes(k), bytes(v), table_bytes, scalars, block_count, physical_capacity


def prepare_buffers(cuda: Cuda, case: Case) -> Buffers:
    q, k, v, table, scalars, page_blocks, physical_capacity = make_inputs(case)
    output_count = HEADS * case.head_dim
    score_capacity = 512 if case.head_dim == 256 else 4096
    output = poison_f32_payload(output_count, OUTPUT_POISON_BITS)
    score = poison_f32_payload(HEADS * score_capacity, SCORE_POISON_BITS)
    alpha = poison_f32_payload(HEADS * score_capacity, ALPHA_POISON_BITS)
    beta = poison_f32_payload(HEADS * score_capacity, BETA_POISON_BITS)
    denom = poison_f32_payload(HEADS, DENOM_POISON_BITS)
    return Buffers(
        q=upload_guarded(cuda, q),
        k=upload_guarded(cuda, k),
        v=upload_guarded(cuda, v),
        table=None if table is None else upload_guarded(cuda, table),
        scalars=upload_guarded(cuda, scalars),
        candidate=upload_guarded(cuda, output),
        serial=upload_guarded(cuda, output),
        tiled=upload_guarded(cuda, output),
        scores=upload_guarded(cuda, score),
        alpha=upload_guarded(cuda, alpha),
        beta=upload_guarded(cuda, beta),
        denom=upload_guarded(cuda, denom),
        score_capacity=score_capacity,
        physical_capacity=physical_capacity,
        key_row_bytes=case.head_dim * 2,
        value_row_bytes=case.head_dim * 2,
        block_count=0 if table is None else page_blocks,
    )


def reset_guarded(cuda: Cuda, guarded: Guarded) -> None:
    cuda.upload(guarded.allocation, guarded.image)


def common_values(case: Case, buffers: Buffers) -> tuple[int, ...]:
    total = max(case.query_position + 1, case.kv_position_offset + case.kv_len)
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


def launch_candidate(cuda: Cuda, fn: ctypes.c_void_p, case: Case, b: Buffers) -> None:
    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = common_values(case, b)
    cuda.launch(
        fn,
        (HEADS, 1, 1),
        (case.head_dim, 1, 1),
        [
            ptr(b.candidate.pointer), ptr(b.q.pointer), ptr(b.k.pointer), ptr(b.v.pointer),
            ptr(0 if b.table is None else b.table.pointer),
            u32(batch), u32(q_len), u32(kv_len), u32(heads), u32(kv_heads), u32(hd),
            u32(qpos), u32(kvpos), u32(window), u32(total),
            u32(b.key_row_bytes), u32(b.key_row_bytes), u32(b.value_row_bytes),
            u32(b.block_count), u32(PAGE_SIZE), u32(2), u32(2),
            u32(b.physical_capacity), u32(b.score_capacity), ptr(b.scalars.pointer),
        ],
    )


def launch_coefficient(
    cuda: Cuda,
    fn: ctypes.c_void_p,
    case: Case,
    b: Buffers,
    block_size: int,
) -> None:
    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = common_values(case, b)
    cuda.launch(
        fn,
        (HEADS, 1, 1),
        (block_size, 1, 1),
        [
            ptr(b.alpha.pointer), ptr(b.beta.pointer), ptr(b.denom.pointer),
            ptr(b.scores.pointer), ptr(0 if b.table is None else b.table.pointer),
            u32(batch), u32(q_len), u32(kv_len), u32(heads), u32(kv_heads), u32(hd),
            u32(qpos), u32(kvpos), u32(window), u32(total), u32(b.block_count),
            u32(PAGE_SIZE), u32(b.physical_capacity), u32(b.score_capacity),
            ptr(b.scalars.pointer),
        ],
    )


def launch_coefficient_pv(
    cuda: Cuda,
    fn: ctypes.c_void_p,
    case: Case,
    b: Buffers,
) -> None:
    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = common_values(case, b)
    cuda.launch(
        fn,
        (HEADS, case.head_dim // 64, 1),
        (64, 1, 1),
        [
            ptr(b.candidate.pointer), ptr(b.alpha.pointer), ptr(b.beta.pointer),
            ptr(b.denom.pointer), ptr(b.v.pointer),
            ptr(0 if b.table is None else b.table.pointer),
            u32(batch), u32(q_len), u32(kv_len), u32(heads), u32(kv_heads), u32(hd),
            u32(qpos), u32(kvpos), u32(window), u32(total), u32(b.value_row_bytes),
            u32(b.block_count), u32(PAGE_SIZE), u32(2), u32(b.physical_capacity),
            u32(b.score_capacity), ptr(b.scalars.pointer),
        ],
    )


def launch_score(cuda: Cuda, fn: ctypes.c_void_p, case: Case, b: Buffers) -> None:
    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = common_values(case, b)
    chunk_size = (b.score_capacity + CHUNK_COUNT - 1) // CHUNK_COUNT
    cuda.launch(
        fn,
        (HEADS * CHUNK_COUNT, 1, 1),
        (case.head_dim, 1, 1),
        [
            ptr(b.scores.pointer), ptr(b.q.pointer), ptr(b.k.pointer),
            ptr(0 if b.table is None else b.table.pointer),
            u32(batch), u32(q_len), u32(kv_len), u32(heads), u32(kv_heads), u32(hd),
            u32(qpos), u32(kvpos), u32(window), u32(total),
            u32(b.key_row_bytes), u32(b.key_row_bytes), u32(b.block_count),
            u32(PAGE_SIZE), u32(2), u32(b.physical_capacity), u32(b.score_capacity),
            u32(chunk_size), u32(CHUNK_COUNT), ptr(b.scalars.pointer),
        ],
    )


def launch_consumer(
    cuda: Cuda,
    fn: ctypes.c_void_p,
    destination: Guarded,
    tiled: bool,
    case: Case,
    b: Buffers,
) -> None:
    batch, q_len, kv_len, heads, kv_heads, hd, qpos, kvpos, window, total = common_values(case, b)
    cuda.launch(
        fn,
        (HEADS, case.head_dim // 64 if tiled else 1, 1),
        (64 if tiled else case.head_dim, 1, 1),
        [
            ptr(destination.pointer), ptr(b.scores.pointer), ptr(b.v.pointer),
            ptr(0 if b.table is None else b.table.pointer),
            u32(batch), u32(q_len), u32(kv_len), u32(heads), u32(kv_heads), u32(hd),
            u32(qpos), u32(kvpos), u32(window), u32(total), u32(b.value_row_bytes),
            u32(b.block_count), u32(PAGE_SIZE), u32(2), u32(b.physical_capacity),
            u32(b.score_capacity), ptr(b.scalars.pointer),
        ],
    )


def launch_serial_pipeline(cuda: Cuda, f: Functions, case: Case, b: Buffers) -> None:
    launch_score(cuda, f.score, case, b)
    launch_consumer(cuda, f.serial, b.serial, False, case, b)


def launch_tiled_pipeline(cuda: Cuda, f: Functions, case: Case, b: Buffers) -> None:
    launch_score(cuda, f.score, case, b)
    launch_consumer(cuda, f.tiled, b.tiled, True, case, b)


def launch_two_stage_post_score(
    cuda: Cuda,
    f: Functions,
    case: Case,
    b: Buffers,
    coefficient_block: int,
) -> None:
    launch_coefficient(cuda, f.coefficient, case, b, coefficient_block)
    launch_coefficient_pv(cuda, f.coefficient_pv, case, b)


def launch_two_stage_pipeline(
    cuda: Cuda,
    f: Functions,
    case: Case,
    b: Buffers,
    coefficient_block: int,
) -> None:
    launch_score(cuda, f.score, case, b)
    launch_two_stage_post_score(cuda, f, case, b, coefficient_block)


def inspect_guarded(
    image: bytes,
    guarded: Guarded,
    poison_bits: int | None = None,
    require_finite: bool = False,
) -> dict[str, int]:
    guard_mutations = sum(value != CANARY for value in image[:GUARD_BYTES])
    guard_mutations += sum(value != CANARY for value in image[-GUARD_BYTES:])
    poison = 0
    if poison_bits is not None:
        payload = image[GUARD_BYTES:-GUARD_BYTES]
        poison = sum(
            struct.unpack_from("<I", payload, offset)[0] == poison_bits
            for offset in range(0, len(payload), 4)
        )
    result = {"guard_mutations": guard_mutations, "poison_elements": poison}
    if require_finite:
        payload_bytes = image[GUARD_BYTES:-GUARD_BYTES]
        result["nonfinite_elements"] = sum(
            not math.isfinite(struct.unpack_from("<f", payload_bytes, offset)[0])
            for offset in range(0, len(payload_bytes), 4)
        )
    return result


def payload(image: bytes) -> bytes:
    return image[GUARD_BYTES:-GUARD_BYTES]


def diff_f32(expected: bytes, actual: bytes) -> dict[str, object]:
    if len(expected) != len(actual) or len(expected) % 4:
        raise ValueError("invalid F32 image lengths")
    mismatches = 0
    nonfinite = 0
    max_abs = 0.0
    first: dict[str, object] | None = None
    for index in range(len(expected) // 4):
        expected_bits = struct.unpack_from("<I", expected, index * 4)[0]
        actual_bits = struct.unpack_from("<I", actual, index * 4)[0]
        if expected_bits != actual_bits:
            mismatches += 1
            expected_value = struct.unpack_from("<f", expected, index * 4)[0]
            actual_value = struct.unpack_from("<f", actual, index * 4)[0]
            if first is None:
                first = {
                    "index": index,
                    "expected_bits": f"0x{expected_bits:08x}",
                    "actual_bits": f"0x{actual_bits:08x}",
                    "expected": expected_value,
                    "actual": actual_value,
                }
            if not math.isfinite(expected_value) or not math.isfinite(actual_value):
                nonfinite += 1
            else:
                max_abs = max(max_abs, abs(expected_value - actual_value))
    return {
        "element_count": len(expected) // 4,
        "bitwise_mismatches": mismatches,
        "nonfinite_mismatches": nonfinite,
        "max_abs": max_abs,
        "first_mismatch": first,
    }


def coefficient_of_variation(samples: list[float]) -> float:
    if len(samples) < 2:
        return 0.0
    mean = statistics.fmean(samples)
    return statistics.pstdev(samples) / mean if mean > 0 else 0.0


def paired_timing(
    cuda: Cuda,
    candidate: Callable[[], None],
    reference: Callable[[], None],
    iterations: int,
    pairs: int,
) -> dict[str, object]:
    for _ in range(5):
        candidate()
        reference()
    cuda.synchronize()
    candidate_samples: list[float] = []
    reference_samples: list[float] = []
    for pair_index in range(pairs):
        if pair_index % 2 == 0:
            candidate_samples.append(cuda.time(iterations, candidate))
            reference_samples.append(cuda.time(iterations, reference))
        else:
            reference_samples.append(cuda.time(iterations, reference))
            candidate_samples.append(cuda.time(iterations, candidate))
    candidate_mean = statistics.fmean(candidate_samples)
    reference_mean = statistics.fmean(reference_samples)
    return {
        "pairs": pairs,
        "iterations_per_arm_per_pair": iterations,
        "order": "alternating_AB_BA",
        "candidate_us": candidate_mean,
        "reference_us": reference_mean,
        "speedup": reference_mean / candidate_mean,
        "candidate_cv": coefficient_of_variation(candidate_samples),
        "reference_cv": coefficient_of_variation(reference_samples),
        "candidate_samples_us": candidate_samples,
        "reference_samples_us": reference_samples,
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


def coefficient_geometry_timing(
    cuda: Cuda,
    function: ctypes.c_void_p,
    case: Case,
    buffers: Buffers,
    iterations: int,
    pairs: int,
    geometries: tuple[int, ...],
) -> tuple[int, dict[str, object]]:
    operations = {
        block: (
            lambda block_size=block: launch_coefficient(
                cuda, function, case, buffers, block_size
            )
        )
        for block in geometries
    }
    for operation in operations.values():
        for _ in range(3):
            operation()
    cuda.synchronize()
    samples: dict[int, list[float]] = {block: [] for block in geometries}
    for pair_index in range(pairs):
        order = geometries if pair_index % 2 == 0 else tuple(reversed(geometries))
        for block in order:
            samples[block].append(cuda.time(iterations, operations[block]))
    results = {
        str(block): {
            "threads_per_cta": block,
            "mean_us": statistics.fmean(samples[block]),
            "cv": coefficient_of_variation(samples[block]),
            "samples_us": samples[block],
        }
        for block in geometries
    }
    best_mean = min(results[str(block)]["mean_us"] for block in geometries)
    equivalence_limit = best_mean * 1.01
    selected = min(
        block
        for block in geometries
        if results[str(block)]["mean_us"] <= equivalence_limit
    )
    return selected, {
        "order": "alternating_forward_reverse",
        "iterations_per_geometry_per_pair": iterations,
        "pairs": pairs,
        "selected_threads_per_cta": selected,
        "selection_policy": "smallest_cta_within_1pct_of_best_mean",
        "best_mean_us": best_mean,
        "equivalence_limit_us": equivalence_limit,
        "geometries": results,
    }


def input_integrity(cuda: Cuda, b: Buffers) -> dict[str, int]:
    result: dict[str, int] = {}
    for name in ("q", "k", "v", "scalars"):
        guarded = getattr(b, name)
        current = cuda.download(guarded.allocation, len(guarded.image))
        result[f"{name}_mutations"] = sum(
            left != right for left, right in zip(current, guarded.image)
        )
    if b.table is not None:
        current = cuda.download(b.table.allocation, len(b.table.image))
        result["table_mutations"] = sum(
            left != right for left, right in zip(current, b.table.image)
        )
    else:
        result["table_mutations"] = 0
    return result


def score_integrity(image: bytes, b: Buffers, case: Case) -> dict[str, int]:
    result = inspect_guarded(image, b.scores)
    scores = payload(image)
    visible = case.visible_range[1] - case.visible_range[0]
    visible_poison = 0
    visible_nonfinite = 0
    unused_mutations = 0
    for head in range(HEADS):
        for index in range(b.score_capacity):
            bits = struct.unpack_from(
                "<I", scores, (head * b.score_capacity + index) * 4
            )[0]
            if index < visible:
                visible_poison += bits == SCORE_POISON_BITS
                visible_nonfinite += not math.isfinite(
                    struct.unpack_from(
                        "<f", scores, (head * b.score_capacity + index) * 4
                    )[0]
                )
            else:
                unused_mutations += bits != SCORE_POISON_BITS
    result["visible_poison_elements"] = visible_poison
    result["visible_nonfinite_elements"] = visible_nonfinite
    result["unused_score_mutations"] = unused_mutations
    return result


def coefficient_integrity(
    image: bytes,
    guarded: Guarded,
    poison_bits: int,
    b: Buffers,
    case: Case,
) -> dict[str, int]:
    result = inspect_guarded(image, guarded)
    values = payload(image)
    visible = case.visible_range[1] - case.visible_range[0]
    visible_poison = 0
    visible_nonfinite = 0
    unused_mutations = 0
    for head in range(HEADS):
        for index in range(b.score_capacity):
            offset = (head * b.score_capacity + index) * 4
            bits = struct.unpack_from("<I", values, offset)[0]
            if index < visible:
                visible_poison += bits == poison_bits
                visible_nonfinite += not math.isfinite(
                    struct.unpack_from("<f", values, offset)[0]
                )
            else:
                unused_mutations += bits != poison_bits
    result["visible_poison_elements"] = visible_poison
    result["visible_nonfinite_elements"] = visible_nonfinite
    result["unused_coefficient_mutations"] = unused_mutations
    return result


def denom_integrity(image: bytes, guarded: Guarded) -> dict[str, int]:
    return inspect_guarded(
        image, guarded, DENOM_POISON_BITS, require_finite=True
    )


def run_case(
    cuda: Cuda,
    functions: Functions,
    case: Case,
    iterations: int,
    timing_pairs: int,
    max_cv: float,
) -> dict[str, object]:
    b = prepare_buffers(cuda, case)

    correctness_coefficient_block = 32

    launch_score(cuda, functions.score, case, b)
    cuda.synchronize()
    score_after_producer = cuda.download(b.scores.allocation, len(b.scores.image))
    launch_consumer(cuda, functions.serial, b.serial, False, case, b)
    launch_consumer(cuda, functions.tiled, b.tiled, True, case, b)
    launch_coefficient(
        cuda,
        functions.coefficient,
        case,
        b,
        correctness_coefficient_block,
    )
    cuda.synchronize()
    alpha_after_precompute = cuda.download(b.alpha.allocation, len(b.alpha.image))
    beta_after_precompute = cuda.download(b.beta.allocation, len(b.beta.image))
    denom_after_precompute = cuda.download(b.denom.allocation, len(b.denom.image))
    launch_coefficient_pv(cuda, functions.coefficient_pv, case, b)
    cuda.synchronize()

    first_score = cuda.download(b.scores.allocation, len(b.scores.image))
    first_alpha = cuda.download(b.alpha.allocation, len(b.alpha.image))
    first_beta = cuda.download(b.beta.allocation, len(b.beta.image))
    first_denom = cuda.download(b.denom.allocation, len(b.denom.image))
    first_candidate = cuda.download(b.candidate.allocation, len(b.candidate.image))
    first_serial = cuda.download(b.serial.allocation, len(b.serial.image))
    first_tiled = cuda.download(b.tiled.allocation, len(b.tiled.image))

    for guarded in (
        b.scores,
        b.alpha,
        b.beta,
        b.denom,
        b.candidate,
        b.serial,
        b.tiled,
    ):
        reset_guarded(cuda, guarded)
    launch_score(cuda, functions.score, case, b)
    launch_consumer(cuda, functions.serial, b.serial, False, case, b)
    launch_consumer(cuda, functions.tiled, b.tiled, True, case, b)
    launch_two_stage_post_score(
        cuda, functions, case, b, correctness_coefficient_block
    )
    cuda.synchronize()

    second_score = cuda.download(b.scores.allocation, len(b.scores.image))
    second_alpha = cuda.download(b.alpha.allocation, len(b.alpha.image))
    second_beta = cuda.download(b.beta.allocation, len(b.beta.image))
    second_denom = cuda.download(b.denom.allocation, len(b.denom.image))
    second_candidate = cuda.download(b.candidate.allocation, len(b.candidate.image))
    second_serial = cuda.download(b.serial.allocation, len(b.serial.image))
    second_tiled = cuda.download(b.tiled.allocation, len(b.tiled.image))

    candidate_payload = payload(first_candidate)
    serial_payload = payload(first_serial)
    tiled_payload = payload(first_tiled)
    diffs = {
        "candidate_vs_serial": diff_f32(serial_payload, candidate_payload),
        "candidate_vs_tiled64": diff_f32(tiled_payload, candidate_payload),
        "serial_vs_tiled64": diff_f32(serial_payload, tiled_payload),
        "candidate_repeat": diff_f32(candidate_payload, payload(second_candidate)),
        "serial_repeat": diff_f32(serial_payload, payload(second_serial)),
        "tiled64_repeat": diff_f32(tiled_payload, payload(second_tiled)),
        "score_repeat": diff_f32(payload(first_score), payload(second_score)),
        "alpha_repeat": diff_f32(payload(first_alpha), payload(second_alpha)),
        "beta_repeat": diff_f32(payload(first_beta), payload(second_beta)),
        "denom_repeat": diff_f32(payload(first_denom), payload(second_denom)),
    }
    integrity = {
        "candidate_output": inspect_guarded(
            first_candidate, b.candidate, OUTPUT_POISON_BITS, require_finite=True
        ),
        "serial_output": inspect_guarded(
            first_serial, b.serial, OUTPUT_POISON_BITS, require_finite=True
        ),
        "tiled64_output": inspect_guarded(
            first_tiled, b.tiled, OUTPUT_POISON_BITS, require_finite=True
        ),
        "score": score_integrity(first_score, b, case),
        "alpha": coefficient_integrity(
            first_alpha, b.alpha, ALPHA_POISON_BITS, b, case
        ),
        "beta": coefficient_integrity(
            first_beta, b.beta, BETA_POISON_BITS, b, case
        ),
        "denom": denom_integrity(first_denom, b.denom),
        "score_consumer_readonly": {
            "mutations": sum(
                left != right
                for left, right in zip(score_after_producer, first_score)
            )
        },
        "coefficient_pv_readonly": {
            "alpha_mutations": sum(
                left != right
                for left, right in zip(alpha_after_precompute, first_alpha)
            ),
            "beta_mutations": sum(
                left != right
                for left, right in zip(beta_after_precompute, first_beta)
            ),
            "denom_mutations": sum(
                left != right
                for left, right in zip(denom_after_precompute, first_denom)
            ),
        },
        "inputs": input_integrity(cuda, b),
    }

    timing: dict[str, object] | None = None
    if case.timing_anchor and iterations:
        selected_block, geometry = coefficient_geometry_timing(
            cuda,
            functions.coefficient,
            case,
            b,
            iterations,
            timing_pairs,
            COEFFICIENT_GEOMETRIES,
        )
        coefficient = lambda: launch_coefficient(
            cuda, functions.coefficient, case, b, selected_block
        )
        pv = lambda: launch_coefficient_pv(
            cuda, functions.coefficient_pv, case, b
        )
        candidate_post_score = lambda: launch_two_stage_post_score(
            cuda, functions, case, b, selected_block
        )
        tiled_post_score = lambda: launch_consumer(
            cuda, functions.tiled, b.tiled, True, case, b
        )
        candidate_full = lambda: launch_two_stage_pipeline(
            cuda, functions, case, b, selected_block
        )
        tiled_full = lambda: launch_tiled_pipeline(cuda, functions, case, b)
        timing = {
            "coefficient_geometry": geometry,
            "coefficient_kernel": operation_timing(
                cuda, coefficient, iterations, timing_pairs
            ),
            "pv_kernel": operation_timing(cuda, pv, iterations, timing_pairs),
            "post_score_vs_tiled64": paired_timing(
                cuda,
                candidate_post_score,
                tiled_post_score,
                iterations,
                timing_pairs,
            ),
            "full_pipeline_vs_tiled64": paired_timing(
                cuda,
                candidate_full,
                tiled_full,
                iterations,
                timing_pairs,
            ),
            "max_cv_limit": max_cv,
            "hd512_consumer_speedup_target": 2.0 if case.head_dim == 512 else None,
        }
        timing["performance_target_met"] = (
            case.head_dim != 512
            or timing["post_score_vs_tiled64"]["speedup"] > 2.0
        )

    diff_pass = all(
        value["bitwise_mismatches"] == 0 for value in diffs.values()
    )
    integrity_pass = all(
        value == 0
        for group in integrity.values()
        for value in group.values()
    )
    timing_pass = True
    if timing is not None:
        paired = (
            timing["post_score_vs_tiled64"],
            timing["full_pipeline_vs_tiled64"],
        )
        timing_pass = all(
            comparison["candidate_cv"] <= max_cv
            and comparison["reference_cv"] <= max_cv
            for comparison in paired
        )
        timing_pass = timing_pass and all(
            kernel["cv"] <= max_cv
            for kernel in (timing["coefficient_kernel"], timing["pv_kernel"])
        )
        timing_pass = timing_pass and all(
            geometry["cv"] <= max_cv
            for geometry in timing["coefficient_geometry"]["geometries"].values()
        )
    result: dict[str, object] = {
        "name": case.name,
        "head_dim": case.head_dim,
        "kv_len": case.kv_len,
        "query_position": case.query_position,
        "kv_position_offset": case.kv_position_offset,
        "sliding_window": case.sliding_window,
        "visible_begin": case.visible_range[0],
        "visible_end": case.visible_range[1],
        "visible_count": case.visible_range[1] - case.visible_range[0],
        "page_size": PAGE_SIZE,
        "layout": case.layout,
        "pattern": case.pattern,
        "key_format": "f16",
        "value_format": "f16",
        "diffs": diffs,
        "integrity": integrity,
        "timing": timing,
        "correctness_coefficient_threads_per_cta": correctness_coefficient_block,
        "pass": diff_pass and integrity_pass and timing_pass,
    }
    return result


def locked_cases() -> list[Case]:
    cases: list[Case] = []
    layouts = ("identity-null", "explicit-reversed", "explicit-permuted")
    patterns = ("random", "near-tie", "cancellation")
    for head_dim in (256, 512):
        for layout_index, layout in enumerate(layouts):
            for pattern_index, pattern in enumerate(patterns):
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
                        seed=0x6A09E667F3BCC909 ^ (head_dim << 20) ^ (layout_index << 8) ^ pattern_index,
                        timing_anchor=layout == "identity-null" and pattern == "random",
                    )
                )
    return cases


def boundary_cases() -> list[Case]:
    layouts = ("identity-null", "explicit-reversed", "explicit-permuted")
    patterns = ("random", "near-tie", "cancellation")
    cases: list[Case] = []
    shapes = {
        256: (1, 15, 16, 17, 511, 512, 513, 1024),
        512: (1, 15, 16, 17, 511, 512, 513, 1024, 2048, 4095, 4096),
    }
    ordinal = 0
    for head_dim, lengths in shapes.items():
        for kv_len in lengths:
            kv_offset = 37 if kv_len in (17, 513, 4095) else 0
            cases.append(
                Case(
                    name=f"boundary-hd{head_dim}-kv{kv_len}",
                    head_dim=head_dim,
                    kv_len=kv_len,
                    query_position=kv_offset + kv_len - 1,
                    kv_position_offset=kv_offset,
                    sliding_window=512 if head_dim == 256 else 0,
                    layout=layouts[ordinal % len(layouts)],
                    pattern=patterns[ordinal % len(patterns)],
                    seed=0xBB67AE8584CAA73B ^ ordinal,
                )
            )
            ordinal += 1
    cases.extend(
        [
            Case(
                name="boundary-hd256-partial-visible-512",
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
                name="boundary-hd512-partial-visible-2048",
                head_dim=512,
                kv_len=2350,
                query_position=2047,
                kv_position_offset=0,
                sliding_window=0,
                layout="explicit-reversed",
                pattern="near-tie",
                seed=0xA54FF53A5F1D36F1,
            ),
        ]
    )
    return cases


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-cubin", type=Path, required=True)
    parser.add_argument("--baseline-cubin", type=Path, required=True)
    parser.add_argument(
        "--suite", choices=("locked", "boundaries", "all"), default="all"
    )
    parser.add_argument("--head-dim", choices=(256, 512), type=int)
    parser.add_argument("--iterations", type=int, default=0)
    parser.add_argument("--timing-pairs", type=int, default=7)
    parser.add_argument("--max-timing-cv", type=float, default=0.10)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args()
    if args.iterations < 0 or args.iterations > 100000:
        parser.error("--iterations must be in [0, 100000]")
    if args.timing_pairs < 2 or args.timing_pairs > 31:
        parser.error("--timing-pairs must be in [2, 31]")
    if not math.isfinite(args.max_timing_cv) or args.max_timing_cv < 0:
        parser.error("--max-timing-cv must be finite and non-negative")
    for path in (args.candidate_cubin, args.baseline_cubin):
        if not path.is_file():
            parser.error(f"cubin does not exist: {path}")
    return args


def load_functions(
    cuda: Cuda, candidate_path: Path, baseline_path: Path, head_dim: int
) -> Functions:
    candidate_module = cuda.load_module(candidate_path)
    baseline_module = cuda.load_module(baseline_path)
    if head_dim == 256:
        names = (
            CANDIDATE_HD256,
            COEFFICIENT_HD256,
            COEFFICIENT_PV_HD256,
            SCORE_HD256,
            SERIAL_HD256,
            TILED_HD256,
        )
    else:
        names = (
            CANDIDATE_HD512,
            COEFFICIENT_HD512,
            COEFFICIENT_PV_HD512,
            SCORE_HD512,
            SERIAL_HD512,
            TILED_HD512,
        )
    return Functions(
        cuda.function(candidate_module, names[0]),
        cuda.function(candidate_module, names[1]),
        cuda.function(candidate_module, names[2]),
        cuda.function(baseline_module, names[3]),
        cuda.function(baseline_module, names[4]),
        cuda.function(baseline_module, names[5]),
    )


def first_failure_diagnosis(result: dict[str, object]) -> str:
    for name, diff in result["diffs"].items():
        if diff["bitwise_mismatches"]:
            first = diff["first_mismatch"]
            return (
                f"{name} first differs at output {first['index']}: "
                f"expected={first['expected_bits']} ({first['expected']!r}), "
                f"candidate={first['actual_bits']} ({first['actual']!r}). "
                "A two-stage promotion is blocked because coefficient "
                "materialization or chronological F32 PV no longer matches production."
            )
    for group, metrics in result["integrity"].items():
        for metric, value in metrics.items():
            if value:
                return f"{group}.{metric}={value}; memory-safety qualification failed"
    return "timing CV exceeded the configured stability gate"


def main() -> int:
    args = parse_args()
    selected = (
        locked_cases()
        if args.suite == "locked"
        else boundary_cases()
        if args.suite == "boundaries"
        else locked_cases() + boundary_cases()
    )
    if args.head_dim:
        selected = [case for case in selected if case.head_dim == args.head_dim]

    cuda = Cuda()
    results: list[dict[str, object]] = []
    failure: str | None = None
    try:
        by_head = {
            head_dim: load_functions(
                cuda, args.candidate_cubin, args.baseline_cubin, head_dim
            )
            for head_dim in sorted({case.head_dim for case in selected})
        }
        for index, case in enumerate(selected, 1):
            result = run_case(
                cuda,
                by_head[case.head_dim],
                case,
                args.iterations,
                args.timing_pairs,
                args.max_timing_cv,
            )
            results.append(result)
            print(
                f"[{index}/{len(selected)}] {case.name}: "
                f"visible={result['visible_count']} bitwise="
                f"{result['diffs']['candidate_vs_tiled64']['bitwise_mismatches']} "
                f"pass={str(result['pass']).lower()}",
                flush=True,
            )
            if result["timing"]:
                post_score = result["timing"]["post_score_vs_tiled64"]
                full = result["timing"]["full_pipeline_vs_tiled64"]
                coefficient = result["timing"]["coefficient_kernel"]
                pv = result["timing"]["pv_kernel"]
                print(
                    f"  coefficient={coefficient['mean_us']:.3f}us "
                    f"pv={pv['mean_us']:.3f}us selected_block="
                    f"{result['timing']['coefficient_geometry']['selected_threads_per_cta']}; "
                    f"post_score={post_score['candidate_us']:.3f}us/"
                    f"{post_score['reference_us']:.3f}us "
                    f"speedup={post_score['speedup']:.4f}x; "
                    f"full={full['candidate_us']:.3f}us/"
                    f"{full['reference_us']:.3f}us "
                    f"speedup={full['speedup']:.4f}x",
                    flush=True,
                )
            if not result["pass"]:
                failure = first_failure_diagnosis(result)
                print(f"qualification stopped: {failure}", file=sys.stderr, flush=True)
                break
    finally:
        cuda.close()

    inference_dir = Path(__file__).resolve().parent.parent
    implementation_files = {
        "source": inference_dir
        / "src/ops/cuda/prototypes/gqa_decode_fused_score_pv_sm89.cu",
        "harness": Path(__file__).resolve(),
        "build_script": inference_dir
        / "scripts/build_cuda_gqa_decode_fused_prototype.sh",
    }
    evidence = {
        "schema": "antfly.cuda.gqa-decode-two-stage-coeff-pv.prototype.v2",
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
                COEFFICIENT_HD256,
                COEFFICIENT_HD512,
                COEFFICIENT_PV_HD256,
                COEFFICIENT_PV_HD512,
            ],
        },
        "baseline": {
            "path": str(args.baseline_cubin.resolve()),
            "bytes": args.baseline_cubin.stat().st_size,
            "sha256": sha256_file(args.baseline_cubin),
            "references": {
                "hd256": [SCORE_HD256, SERIAL_HD256, TILED_HD256],
                "hd512": [SCORE_HD512, SERIAL_HD512, TILED_HD512],
            },
        },
        "contract": {
            "batch": 1,
            "heads": HEADS,
            "kv_heads": KV_HEADS,
            "q_seq_len": 1,
            "q_format": "f32",
            "key_format": "f16",
            "value_format": "f16",
            "output_format": "f32",
            "page_size": PAGE_SIZE,
            "bitwise_required": True,
            "guards_poison_readonly_determinism": True,
            "timing_order": "alternating_AB_BA_cuda_events",
            "coefficient_launch_geometries": list(COEFFICIENT_GEOMETRIES),
            "candidate_pipeline": "canonical_score+coefficient_precompute+shared_coefficient_pv",
            "baseline_pipeline": "canonical_score+canonical_tiled64",
            "max_timing_cv": args.max_timing_cv,
        },
        "suite": args.suite,
        "requested_case_count": len(selected),
        "completed_case_count": len(results),
        "cases": results,
        "failure_diagnosis": failure,
        "pass": failure is None and len(results) == len(selected),
    }
    timed_hd512 = [
        result
        for result in results
        if result["head_dim"] == 512 and result["timing"] is not None
    ]
    evidence["performance_target_met"] = bool(timed_hd512) and all(
        result["timing"]["performance_target_met"] for result in timed_hd512
    )
    evidence["promotion_recommended"] = (
        evidence["pass"] and evidence["performance_target_met"]
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
