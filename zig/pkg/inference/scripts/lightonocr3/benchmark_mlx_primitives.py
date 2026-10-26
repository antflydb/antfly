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

"""Time MLX OCR normalization and recurrent prefill separately from full readers.

Use a freshly compiled metal_command_timing.m observer. Both F32 and BF16
activation cases retain F32 recurrent state. This measures installed MLX-LM's
primitive, not Antfly's interleaved QKV layout or complete recurrent core.
"""

import argparse
import ctypes
import hashlib
import json
import statistics
import time
from datetime import datetime, timezone
from importlib.metadata import version
from functools import partial
from pathlib import Path


def measure(mx, timer, function, warmup, trials):
    def evaluate():
        result = function()
        result = result if isinstance(result, (list, tuple)) else (result,)
        mx.eval(*result)
        return result

    for _ in range(warmup):
        evaluate()
    gpu, wall, counts, empty = [], [], [], []
    for _ in range(trials):
        timer.ocr_timing_begin()
        begin = time.perf_counter_ns()
        result = evaluate()
        wall.append((time.perf_counter_ns() - begin) / 1000)
        count = ctypes.c_uint64()
        nanos = timer.ocr_timing_end(ctypes.byref(count))
        if nanos == 0 or count.value == 0:
            raise RuntimeError("Missing GPU work timestamps")
        gpu.append(nanos / 1000)
        counts.append(count.value)
        empty.append(timer.ocr_timing_empty_count())
    if not all(mx.all(mx.isfinite(value)).item() for value in result):
        raise RuntimeError("Nonfinite primitive output")
    return {
        "gpu_mean_us": statistics.mean(gpu),
        "gpu_median_us": statistics.median(gpu),
        "gpu_min_us": min(gpu),
        "gpu_max_us": max(gpu),
        "wall_mean_us": statistics.mean(wall),
        "command_counts": sorted(set(counts)),
        "empty_command_counts": sorted(set(empty)),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timing-library", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sequences", type=int, nargs="+", default=[432, 554, 662])
    parser.add_argument("--warmup", type=int, default=8)
    parser.add_argument("--trials", type=int, default=20)
    args = parser.parse_args()
    if min(args.sequences) < 1 or args.warmup < 1 or args.trials < 2:
        parser.error("positive sequences/warmup and at least two trials required")
    library = args.timing_library.resolve()
    timer = ctypes.CDLL(str(library))
    timer.ocr_timing_end.argtypes = [ctypes.POINTER(ctypes.c_uint64)]
    timer.ocr_timing_end.restype = ctypes.c_uint64
    timer.ocr_timing_empty_count.restype = ctypes.c_uint64
    if timer.ocr_timing_init() or timer.ocr_timing_self_test():
        raise RuntimeError("GPU observer initialization/self-test failed")
    import mlx.core as mx
    from mlx_lm.models.gated_delta import gated_delta_kernel

    wired_limit = int(mx.device_info()["max_recommended_working_set_size"])
    mx.set_wired_limit(wired_limit)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        "mlx": version("mlx"),
        "mlx_lm": version("mlx-lm"),
        "device": mx.device_info(),
        "observer_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
        "observer_self_test": "passed",
        "wired_limit_bytes": wired_limit,
        "warmup": args.warmup,
        "trials": args.trials,
        "scope": "Isolated primitives; recurrent state/gates F32, inputs/outputs as specified. Recurrent Q/K/V are contiguous separate arrays, unlike native interleaved storage. No page-throughput claim.",
        "status": "running",
        "results": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        for dtype_name, dtype in [("f32", mx.float32), ("bf16", mx.bfloat16)]:
            for seq in args.sequences:
                for hidden in [1024, 2560]:
                    x = (
                        (mx.sin(mx.arange(seq * hidden) * 0.013) * 0.5)
                        .reshape(seq, hidden)
                        .astype(dtype)
                    )
                    weight = mx.cos(mx.arange(hidden) * 0.01) * 0.5
                    mx.eval(x, weight)
                    row = {
                        "operation": "rmsnorm",
                        "rows": seq,
                        "hidden": hidden,
                        "dtype": dtype_name,
                    }
                    row.update(
                        measure(
                            mx,
                            timer,
                            partial(mx.fast.rms_norm, x, weight, 1e-6),
                            args.warmup,
                            args.trials,
                        )
                    )
                    report["results"].append(row)
                    print(json.dumps(row), flush=True)
                    del x, weight
                for heads in [16, 32]:
                    channels = (32 + heads) * 128
                    mixed = (mx.sin(mx.arange(seq * channels) * 0.013) * 0.1).reshape(
                        1, seq, channels
                    )
                    q = mx.contiguous(
                        mixed[..., :2048].reshape(1, seq, 16, 128).astype(dtype)
                    )
                    k = mx.contiguous(
                        mixed[..., 2048:4096].reshape(1, seq, 16, 128).astype(dtype)
                    )
                    v = mx.contiguous(
                        mixed[..., 4096:].reshape(1, seq, heads, 128).astype(dtype)
                    )
                    ids = mx.arange(seq * heads).reshape(1, seq, heads)
                    decay = 0.97 + mx.cos(ids * 0.1) * 0.02
                    beta = 0.3 + mx.sin(ids * 0.02) * 0.1
                    state = mx.zeros((1, heads, 128, 128), dtype=mx.float32)
                    mx.eval(q, k, v, decay, beta, state)
                    row = {
                        "operation": "recurrent",
                        "sequence": seq,
                        "key_heads": 16,
                        "value_heads": heads,
                        "dimension": 128,
                        "dtype": dtype_name,
                    }
                    row.update(
                        measure(
                            mx,
                            timer,
                            partial(gated_delta_kernel, q, k, v, decay, beta, state),
                            args.warmup,
                            args.trials,
                        )
                    )
                    report["results"].append(row)
                    print(json.dumps(row), flush=True)
                    del q, k, v, decay, beta, state, mixed, ids
                mx.clear_cache()
        report["status"] = "ok"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
