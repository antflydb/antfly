#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Check the bundled Decide FP32 attention against the existing FP32 kernel.

Requires CUDA and PyTorch for tensor allocation, events and numerical checks.
Includes ragged tails, full/empty/partial masks and amplified inputs. Neither
kernel uses reduced precision. Run under compute-sanitizer for memory checks.
"""
from __future__ import annotations

import argparse
import ctypes as C
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cubin", type=Path, default=Path(__file__).resolve().parents[2] / "src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    import torch

    torch.set_num_threads(2)
    torch.manual_seed(9)
    # Allocation makes the primary CUDA context current for the driver API.
    context_anchor = torch.empty(1, device="cuda")
    pointer, uint = C.c_void_p, C.c_uint
    driver = C.CDLL("libcuda.so.1")

    def checked(name, values, types):
        function = getattr(driver, name)
        function.argtypes, function.restype = types, C.c_int
        status = function(*values)
        if status:
            raise RuntimeError(f"{name}: CUDA status {status}")

    module = pointer()
    checked("cuModuleLoad", [C.byref(module), str(args.cubin.resolve()).encode()], [C.POINTER(pointer), C.c_char_p])
    try:
        functions = []
        for name in (b"termite_deberta_attention_fused_f32", b"termite_deberta_attention_stream_f32"):
            function = pointer()
            checked("cuModuleGetFunction", [C.byref(function), module, name], [C.POINTER(pointer), pointer, C.c_char_p])
            functions.append(function)
        launch = driver.cuLaunchKernel
        launch.argtypes = [pointer, uint, uint, uint, uint, uint, uint, uint, pointer, pointer, pointer]
        launch.restype = C.c_int
        results = []
        for batch, sequence in ((1, 1), (1, 31), (1, 32), (1, 33), (1, 71), (8, 107), (8, 111), (2, 127), (2, 128), (2, 129), (2, 257), (1, 511), (1, 512)):
            for mask_kind in ("full", "empty", "prefix", "holes"):
                for scale in (1.0, 8.0):
                    heads, width = 16, 64
                    q, k, v = [scale * torch.randn(batch, sequence, heads * width, device="cuda") for _ in range(3)]
                    qr, kr = [scale * torch.randn(2 * sequence - 1, heads * width, device="cuda") for _ in range(2)]
                    mask = torch.ones(batch, sequence, dtype=torch.int64, device="cuda")
                    if mask_kind == "empty":
                        mask.zero_()
                    elif mask_kind == "prefix":
                        for row in range(batch):
                            mask[row, max(1, sequence * (row + 1) // (batch + 1)):] = 0
                    elif mask_kind == "holes":
                        mask[:, 1::3] = 0
                    outputs = [torch.empty_like(q) for _ in functions]
                    timings = []
                    for index, (function, output) in enumerate(zip(functions, outputs, strict=True)):
                        values = [pointer(t.data_ptr()) for t in (output, q, k, v, qr, kr, mask)] + [uint(batch), uint(sequence), uint(heads), uint(width)]
                        parameters = (pointer * len(values))(*(C.cast(C.byref(value), pointer) for value in values))
                        blocks = batch * heads * (sequence if index == 0 else (sequence + 7) // 8)

                        def run():
                            status = launch(function, blocks, 1, 1, 256, 1, 1, 0, pointer(torch.cuda.current_stream().cuda_stream), parameters, None)
                            if status:
                                raise RuntimeError(f"cuLaunchKernel: CUDA status {status}")

                        run()
                        start, end = torch.cuda.Event(enable_timing=True), torch.cuda.Event(enable_timing=True)
                        start.record()
                        for _ in range(3):
                            run()
                        end.record()
                        end.synchronize()
                        timings.append(start.elapsed_time(end) / 3)
                    torch.testing.assert_close(outputs[1], outputs[0], atol=3e-4, rtol=3e-4)
                    error = (outputs[0] - outputs[1]).abs().max().item()
                    results.append(dict(batch=batch, sequence=sequence, mask=mask_kind, scale=scale,
                                        max_error=error, reference_ms=timings[0], streaming_ms=timings[1]))
        args.out.write_text(json.dumps(dict(gpu=torch.cuda.get_device_name(), cases=results), indent=2) + "\n")
        print(f"Passed {len(results)} FP32 attention cases; maximum absolute error {max(row['max_error'] for row in results):.8g}")
    finally:
        checked("cuModuleUnload", [module], [pointer])
        del context_anchor


if __name__ == "__main__":
    main()
