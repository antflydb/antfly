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

"""Measure MLX OCR matrix GPU times, using the native checker's weight population.

Compile metal_command_timing.m as a dylib inside the worktree, then pass it via
--timing-library. Run separately from native GPU benchmarks. Outputs are BF16;
Antfly's default projection inputs/accumulation/outputs are F32 with BF16 weights.
The observer is benchmark-only. Invalid/missing GPU timestamps fail the report.
"""

import argparse
import ctypes
import hashlib
import json
import statistics
import time
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path


def weights(in_dim, out_dim):
    import numpy as np

    # Advance 8192 independent lanes of the same LCG used by the C checker.
    # This keeps setup fast while matching its BF16 weight bits exactly.
    width = 8192
    seeds = np.empty(width, dtype=np.uint32)
    state = 123
    step_a, step_c = 1, 0
    for i in range(width):
        state = (state * 1664525 + 1013904223) & 0xFFFFFFFF
        seeds[i] = state
        step_a = (step_a * 1664525) & 0xFFFFFFFF
        step_c = (step_c * 1664525 + 1013904223) & 0xFFFFFFFF
    result = np.empty(in_dim * out_dim, dtype=np.uint16)
    for start in range(0, result.size, width):
        size = min(width, result.size - start)
        values = ((seeds[:size] >> 16).astype(np.int32) - 32768).astype(np.float32)
        bits = (values * np.float32(0.000001)).view(np.uint32)
        result[start : start + size] = (
            (bits + np.uint32(0x7FFF) + ((bits >> 16) & 1)) >> 16
        ).astype(np.uint16)
        seeds = seeds * np.uint32(step_a) + np.uint32(step_c)
    return (result.astype(np.uint32) << 16).view(np.float32).reshape(out_dim, in_dim)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timing-library", type=Path, required=True)
    parser.add_argument("--rows", type=int, nargs="+", default=[1, 4, 8, 128, 512])
    parser.add_argument(
        "--shapes",
        nargs="+",
        default=[
            "1024x6144",
            "1024x3584",
            "3584x1024",
            "2560x8192",
            "2560x9216",
            "9216x2560",
        ],
    )
    parser.add_argument("--warmup", type=int, default=8)
    parser.add_argument("--trials", type=int, default=30)
    parser.add_argument(
        "--wire-weights",
        action="store_true",
        help="Use vLLM Metal's recommended-working-set wired limit",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.warmup < 1 or args.trials < 2 or any(row < 1 for row in args.rows):
        parser.error("positive rows/warmup and at least two trials are required")
    try:
        shapes = [tuple(map(int, shape.split("x"))) for shape in args.shapes]
        if any(len(shape) != 2 or min(shape) < 1 for shape in shapes):
            raise ValueError
    except ValueError:
        parser.error("shapes must be positive INxOUT pairs")
    library = args.timing_library.resolve()
    timer = ctypes.CDLL(str(library))
    timer.ocr_timing_end.argtypes = [ctypes.POINTER(ctypes.c_uint64)]
    timer.ocr_timing_end.restype = ctypes.c_uint64
    timer.ocr_timing_empty_count.restype = ctypes.c_uint64
    if timer.ocr_timing_init() != 0:
        raise RuntimeError("Metal command-buffer observer initialization failed")
    if timer.ocr_timing_self_test() != 0:
        raise RuntimeError("Metal timing observer self-test failed")
    import mlx.core as mx

    wired_limit = (
        int(mx.device_info()["max_recommended_working_set_size"])
        if args.wire_weights
        else 0
    )
    mx.set_wired_limit(wired_limit)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        "mlx": version("mlx"),
        "device": mx.device_info(),
        "wired_limit_bytes": wired_limit,
        "observer_self_test": "passed",
        "observer_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
        "scope": "Isolated random-weight matrices, not checkpoint or page throughput; one GPU process at a time. GPU times sum committed command buffers. Native checker uses the same LCG/BF16 weight bits; inputs/outputs differ in precision.",
        "activation": "bf16",
        "weights": "bf16",
        "accumulation": "f32",
        "output": "bf16",
        "warmup": args.warmup,
        "trials": args.trials,
        "status": "running",
        "results": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        for in_dim, out_dim in shapes:
            values = weights(in_dim, out_dim)
            weight_hash = hashlib.sha256(values.tobytes()).hexdigest()
            w = mx.array(values, dtype=mx.bfloat16)
            del values
            mx.eval(w)
            for rows in args.rows:
                x = mx.sin(mx.arange(rows * in_dim, dtype=mx.float32) * 0.01)
                x = x.reshape(rows, in_dim).astype(mx.bfloat16)
                mx.eval(x)
                for _ in range(args.warmup):
                    mx.eval(x @ w.T)
                gpu, wall, command_counts, empty_counts = [], [], [], []
                for _ in range(args.trials):
                    timer.ocr_timing_begin()
                    begin = time.perf_counter_ns()
                    y = x @ w.T
                    mx.eval(y)
                    wall.append((time.perf_counter_ns() - begin) / 1000)
                    count = ctypes.c_uint64()
                    nanos = timer.ocr_timing_end(ctypes.byref(count))
                    if nanos == 0 or count.value == 0:
                        raise RuntimeError(
                            "Missing/invalid GPU timestamps; refusing a zero-time result"
                        )
                    gpu.append(nanos / 1000)
                    command_counts.append(count.value)
                    empty_counts.append(timer.ocr_timing_empty_count())
                    del y
                row = {
                    "rows": rows,
                    "in": in_dim,
                    "out": out_dim,
                    "expanded_weight_sha256": weight_hash,
                    "gpu_mean_us": statistics.mean(gpu),
                    "gpu_median_us": statistics.median(gpu),
                    "gpu_min_us": min(gpu),
                    "gpu_max_us": max(gpu),
                    "wall_mean_us": statistics.mean(wall),
                    "command_counts": sorted(set(command_counts)),
                    "empty_command_counts": sorted(set(empty_counts)),
                }
                report["results"].append(row)
                print(json.dumps(row), flush=True)
                del x
            del w
            mx.clear_cache()
        report["status"] = "ok"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
