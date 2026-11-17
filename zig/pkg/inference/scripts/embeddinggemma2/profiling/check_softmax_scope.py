#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Check historical vision softmax against its pre-change compiled CUDA image."""

import argparse
import ctypes as C
import json
from pathlib import Path

import ml_dtypes
import numpy as np


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-cubin", type=Path, required=True)
    parser.add_argument("--candidate-cubin", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    lib = C.CDLL("libcuda.so.1")
    pointer, device_pointer = C.c_void_p, C.c_uint64

    def fn(name, signature):
        method = getattr(lib, name)
        method.argtypes, method.restype = signature, C.c_int
        return method

    def ok(result):
        if result:
            raise RuntimeError(f"CUDA status {result}")

    init = fn("cuInit", [C.c_uint])
    retain = fn("cuDevicePrimaryCtxRetain", [C.POINTER(pointer), C.c_int])
    release = fn("cuDevicePrimaryCtxRelease_v2", [C.c_int])
    current = fn("cuCtxSetCurrent", [pointer])
    load = fn("cuModuleLoad", [C.POINTER(pointer), C.c_char_p])
    unload = fn("cuModuleUnload", [pointer])
    get = fn("cuModuleGetFunction", [C.POINTER(pointer), pointer, C.c_char_p])
    allocate = fn("cuMemAlloc_v2", [C.POINTER(device_pointer), C.c_size_t])
    free = fn("cuMemFree_v2", [device_pointer])
    h2d = fn("cuMemcpyHtoD_v2", [device_pointer, pointer, C.c_size_t])
    d2h = fn("cuMemcpyDtoH_v2", [pointer, device_pointer, C.c_size_t])
    synchronize = fn("cuCtxSynchronize", [])
    launch = fn(
        "cuLaunchKernel",
        [
            pointer,
            C.c_uint,
            C.c_uint,
            C.c_uint,
            C.c_uint,
            C.c_uint,
            C.c_uint,
            C.c_uint,
            pointer,
            C.POINTER(pointer),
            pointer,
        ],
    )
    ok(init(0))
    context = pointer()
    ok(retain(C.byref(context), 0))
    modules = []
    try:
        ok(current(context))
        functions = []
        for path in (args.baseline_cubin, args.candidate_cubin):
            module, function = pointer(), pointer()
            ok(load(C.byref(module), str(path.resolve()).encode()))
            modules.append(module)
            ok(
                get(
                    C.byref(function),
                    module,
                    b"termite_embedding_gemma2_attention_softmax_bf16",
                )
            )
            functions.append(function)
        rng = np.random.default_rng(93)
        results = []
        for length in (17, 513, 8192):
            rows, capacity, heads = min(length, 37), 64, 12
            scores = rng.normal(0, 3, (heads, capacity, length)).astype(np.float32)
            for mask_kind in ("none", "holes", "all"):
                mask = np.ones(length, dtype=np.int64)
                if mask_kind == "holes":
                    mask[::7] = 0
                elif mask_kind == "all":
                    mask[:] = 0
                arrays = [
                    scores,
                    mask,
                    np.zeros(scores.shape, dtype=ml_dtypes.bfloat16),
                    np.zeros(scores.shape, dtype=ml_dtypes.bfloat16),
                ]
                buffers = []
                try:
                    for array in arrays:
                        buffer = device_pointer()
                        ok(allocate(C.byref(buffer), array.nbytes))
                        buffers.append(buffer)
                        ok(h2d(buffer, array.ctypes.data, array.nbytes))
                    for function, output in zip(functions, buffers[2:]):
                        values = [
                            output,
                            buffers[0],
                            device_pointer(
                                0 if mask_kind == "none" else buffers[1].value
                            ),
                            C.c_uint(length),
                            C.c_uint(0),
                            C.c_uint(rows),
                            C.c_uint(0),
                            C.c_uint(capacity),
                        ]
                        params = (pointer * len(values))(
                            *(C.cast(C.byref(value), pointer) for value in values)
                        )
                        ok(
                            launch(
                                function,
                                rows * heads,
                                1,
                                1,
                                256,
                                1,
                                1,
                                1024,
                                None,
                                params,
                                None,
                            )
                        )
                    ok(synchronize())
                    for buffer, array in zip(buffers[2:], arrays[2:]):
                        ok(d2h(array.ctypes.data, buffer, array.nbytes))
                    if not np.array_equal(
                        arrays[2].view(np.uint16), arrays[3].view(np.uint16)
                    ):
                        raise RuntimeError(
                            f"Historical softmax changed: {length=} {mask_kind=}"
                        )
                    results.append(
                        {
                            "sequence": length,
                            "heads": heads,
                            "rows": rows,
                            "mask": mask_kind,
                            "bitwise_equal": True,
                        }
                    )
                finally:
                    for buffer in reversed(buffers):
                        ok(free(buffer))
        args.out.write_text(
            json.dumps({"cases": results, "passed": True}, indent=2) + "\n"
        )
    finally:
        for module in reversed(modules):
            ok(unload(module))
        ok(release(0))


if __name__ == "__main__":
    main()
