#!/usr/bin/env python3
"""Synchronized upstream CUDA benchmark using the exact captured GLiDE prompts.

Run each model/dtype in a fresh process. Timings include padding, H2D, model,
and CPU logits materialization, matching the native glide-bench boundary.
"""

import argparse
import importlib.metadata
import json
import math
import statistics
import time
from pathlib import Path
from capture_reference import import_reference


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--model", type=Path, required=True)
    p.add_argument("--fixture", type=Path, required=True)
    p.add_argument("--dtype", choices=["bf16", "fp16"], default="bf16")
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--measure", type=int, default=5)
    p.add_argument(
        "--budget",
        type=int,
        default=16384,
        help="Padded tokens per upstream microbatch",
    )
    p.add_argument("--only", help="Comma-separated fixture indices")
    p.add_argument("--mode", choices=["single", "batched", "both"], default="both")
    p.add_argument("--max-dlogit", type=float, default=math.inf)
    p.add_argument("--max-dprob", type=float, default=math.inf)
    p.add_argument(
        "--max-flips",
        type=int,
        default=0,
        help="Allowed upstream argmax drift versus FP32; always reported",
    )
    args = p.parse_args()
    import torch

    if args.warmup < 1 or args.measure < 1 or args.budget < 1:
        p.error("warmup, measure, and budget must be positive")
    fixture = json.loads(args.fixture.read_text())
    temperature = json.loads((args.model / "decision_config.json").read_text())[
        "temperature"
    ]
    chosen = (
        list(map(int, args.only.split(",")))
        if args.only
        else list(range(len(fixture["prompts"])))
    )
    if not chosen or any(i < 0 or i >= len(fixture["prompts"]) for i in chosen):
        p.error("select at least one valid fixture index")
    rows = [fixture["prompts"][i] for i in chosen]
    glide, _ = import_reference(args.model)
    t0 = time.perf_counter()
    model = glide.Glide.from_pretrained(
        str(args.model),
        device="cuda",
        dtype={"bf16": torch.bfloat16, "fp16": torch.float16}[args.dtype],
        token_budget=args.budget,
        vision=False,
    )
    torch.cuda.synchronize()
    versions = {
        name: importlib.metadata.version(name)
        for name in ("torch", "transformers", "flash-linear-attention", "causal-conv1d")
    }
    print(
        json.dumps(
            {
                "event": "load",
                "model": str(args.model),
                "dtype": args.dtype,
                "load_s": time.perf_counter() - t0,
                "versions": versions,
                "token_budget": args.budget,
                "gpu": torch.cuda.get_device_name(),
                "tf32": torch.backends.cuda.matmul.allow_tf32,
            }
        ),
        flush=True,
    )

    def cell(name, selected):
        seqs = [r["ids"] for r in selected]
        for _ in range(args.warmup):
            model._logits(seqs, [None] * len(seqs))
        torch.cuda.synchronize()
        torch.cuda.reset_peak_memory_stats()
        times = []
        for _ in range(args.measure):
            t0 = time.perf_counter()
            actual = model._logits(seqs, [None] * len(seqs))
            torch.cuda.synchronize()
            times.append((time.perf_counter() - t0) * 1000)
        if len(actual) != len(selected):
            raise AssertionError("Missing or extra output rows")
        max_error = 0.0
        max_probability_error = 0.0
        flips = 0
        flip_prompts = []
        for got, row in zip(actual, selected):
            n = row["n"]
            if len(got) != len(row["logits"]) or not all(map(math.isfinite, got)):
                raise AssertionError("Invalid or nonfinite logits")
            max_error = max(
                max_error, max(abs(x - y) for x, y in zip(got[:n], row["logits"][:n]))
            )

            def softmax(values):
                maximum = max(values)
                scores = [math.exp((value - maximum) / temperature) for value in values]
                return [value / sum(scores) for value in scores]

            max_probability_error = max(
                max_probability_error,
                max(
                    abs(x - y)
                    for x, y in zip(softmax(got[:n]), softmax(row["logits"][:n]))
                ),
            )
            if max(range(n), key=lambda i: got[i]) != max(
                range(n), key=lambda i: row["logits"][i]
            ):
                flips += 1
                flip_prompts.append(fixture["prompts"].index(row))
        print(
            json.dumps(
                {
                    "event": "cell",
                    "cell": name,
                    "ms": times,
                    "median_ms": statistics.median(times),
                    "prompts": len(seqs),
                    "tokens": sum(map(len, seqs)),
                    "max_dlogit": max_error,
                    "max_dprob": max_probability_error,
                    "flips": flips,
                    "flip_prompts": flip_prompts,
                    "peak_allocated": torch.cuda.max_memory_allocated(),
                    "peak_reserved": torch.cuda.max_memory_reserved(),
                }
            ),
            flush=True,
        )
        if (
            flips > args.max_flips
            or max_error > args.max_dlogit
            or max_probability_error > args.max_dprob
        ):
            raise AssertionError("GLiDE reference parity failed")

    if args.mode in ("single", "both"):
        for index, row in zip(chosen, rows):
            cell(f"single_{index}", [row])
    if args.mode in ("batched", "both"):
        cell("batched", rows)


if __name__ == "__main__":
    main()
