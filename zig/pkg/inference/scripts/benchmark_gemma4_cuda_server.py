#!/usr/bin/env python3
"""Benchmark the batching-off server with the shared Gemma 4 QAT CUDA profile."""

from __future__ import annotations

import argparse
import json
import os
import pathlib

from benchmark_gemma4_cuda_batching import Server, measure_mode


def parse_args() -> argparse.Namespace:
    repo = pathlib.Path(__file__).resolve().parents[4]
    parser = argparse.ArgumentParser()
    parser.add_argument("--antfly-bin", type=pathlib.Path, default=repo / "zig/pkg/inference/zig-out/bin/antfly-inference")
    parser.add_argument("--model", type=pathlib.Path, default=repo / ".models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf")
    parser.add_argument("--models-dir", type=pathlib.Path, default=repo / ".models")
    parser.add_argument("--output-dir", type=pathlib.Path, default=pathlib.Path("/tmp/antfly-gemma4-cuda-server"))
    parser.add_argument("--prompt", default="Write one sentence about ants.")
    parser.add_argument("--tokens", type=int, default=256)
    parser.add_argument("--cache-dtype", default="f32")
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--startup-timeout", type=float, default=600.0)
    parser.add_argument(
        "--server-prefix",
        default=str(repo / "zig/pkg/inference/scripts/with_gemma4_qat_cuda_tuning.sh"),
    )
    args = parser.parse_args()
    args.max_step_items = 1
    args.decode_wait_us = 0
    return args


def main() -> None:
    args = parse_args()
    if not args.antfly_bin.exists() or not args.model.exists():
        raise SystemExit("antfly binary or model does not exist")
    if args.tokens < 1 or args.warmups < 0 or args.repeats < 1:
        raise SystemExit("tokens/repeats must be positive and warmups non-negative")

    capacity = ((len(args.prompt.encode("utf-8")) + 3) // 4 + args.tokens + 64 + 31) // 32 * 32
    os.environ["ANTFLY_CAPTURE_FORCE_KV_CAPACITY"] = str(max(capacity, 544))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    body = {
        "model": str(args.model.resolve()),
        "backend": "cuda",
        "messages": [{"role": "user", "content": args.prompt}],
        "max_tokens": args.tokens,
        "temperature": 0,
        "stream": False,
        "cache_dtype": args.cache_dtype,
    }

    with Server(args, "off", args.output_dir) as server:
        result = measure_mode(server, [("default", body)], [1], args.warmups, args.repeats)

    measurement = result["measurements"]["1"]
    deterministic = len(measurement["fingerprints"]) == 1
    summary = {
        "config": {
            "model": str(args.model.resolve()),
            "tokens": args.tokens,
            "cache_dtype": args.cache_dtype,
            "warmups": args.warmups,
            "repeats": args.repeats,
            "graph_kv_capacity": int(os.environ["ANTFLY_CAPTURE_FORCE_KV_CAPACITY"]),
            "server_prefix": args.server_prefix,
        },
        "deterministic_fingerprint": deterministic,
        "measurement": measurement,
        "metrics": result["metrics"],
    }
    output = args.output_dir / "server_summary.json"
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(
        f"cuda_server median_tok_s={measurement['aggregate_tok_s']['median']:.3f} "
        f"p95_ms={measurement['request_latency_ms']['p95']:.3f} "
        f"deterministic={str(deterministic).lower()} output={output}"
    )
    if not deterministic:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
