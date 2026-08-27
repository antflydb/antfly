#!/usr/bin/env python3
"""Run and gate paired Gemma 4 CUDA benchmarks across output lengths."""

from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import statistics
import subprocess


def parse_args() -> argparse.Namespace:
    repo = pathlib.Path(__file__).resolve().parents[5]
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-script", type=pathlib.Path, default=repo / "zig/pkg/inference/scripts/gemma4/gemma4_qat_llamacpp_pair_benchmark.sh")
    parser.add_argument("--output-dir", type=pathlib.Path, default=pathlib.Path("/tmp/antfly-gemma4-cuda-matrix"))
    parser.add_argument("--prompt", default="Write one sentence about ants.")
    parser.add_argument("--lengths", type=int, nargs="+", default=[64, 128, 256, 512])
    parser.add_argument("--target-length", type=int, default=256)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--min-antfly-tok-s", type=float, default=120.0)
    parser.add_argument("--min-comparable-ratio", type=float, default=0.95)
    parser.add_argument("--max-cv", type=float, default=0.02)
    parser.add_argument("--require-graph-replay", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--require-generated-attention", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--require-generated-q6-lm-head-argmax", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--collect-only", action="store_true")
    return parser.parse_args()


def summarize_entry(output_tokens: int, summary: dict, pair_exit_code: int = 0) -> dict:
    antfly_tps = float(summary["antfly_decode_tok_s"]["median"])
    comparable_tps = float(summary["llama_comparable_tok_s"]["median"])
    pair_summary_ok = bool(summary["ok"])
    return {
        "output_tokens": output_tokens,
        "pair_exit_code": pair_exit_code,
        "pair_summary_ok": pair_summary_ok,
        "pair_ok": pair_exit_code == 0 and pair_summary_ok,
        "antfly_eval_tokens": int(summary["comparison"]["antfly_tokens"]),
        "antfly_tok_s": antfly_tps,
        "antfly_ms_per_token": 1000.0 / antfly_tps,
        "antfly_cv": float(summary["antfly_tok_s_cv"]),
        "llama_comparable_tok_s": comparable_tps,
        "comparable_ratio": antfly_tps / comparable_tps,
        "graph_replay_ok": bool(summary["ok_graph_replay"]),
        "generated_attention_ok": bool(summary["ok_generated_attention"]),
        "generated_q6_lm_head_argmax_ok": bool(summary.get("ok_generated_q6_lm_head_argmax", False)),
    }


def evaluate(entries: list[dict], args: argparse.Namespace) -> dict:
    by_length = {entry["output_tokens"]: entry for entry in entries}
    target = by_length.get(args.target_length)
    if target is None:
        raise ValueError(f"target length {args.target_length} was not measured")

    ordered = sorted(entries, key=lambda entry: entry["output_tokens"])
    slopes = []
    for left, right in zip(ordered, ordered[1:]):
        slopes.append({
            "from_tokens": left["output_tokens"],
            "to_tokens": right["output_tokens"],
            "additional_ms_per_token": right["antfly_ms_per_token"] - left["antfly_ms_per_token"],
        })

    checks = {
        "pair_benchmarks": all(entry["pair_ok"] for entry in entries),
        "target_throughput": target["antfly_tok_s"] >= args.min_antfly_tok_s,
        "comparable_ratio": target["comparable_ratio"] >= args.min_comparable_ratio,
        "stability": target["antfly_cv"] <= args.max_cv,
        "graph_replay": (not args.require_graph_replay) or all(entry["graph_replay_ok"] for entry in entries),
        "generated_attention": (not args.require_generated_attention) or all(entry["generated_attention_ok"] for entry in entries),
        "generated_q6_lm_head_argmax": (not args.require_generated_q6_lm_head_argmax) or all(
            entry["generated_q6_lm_head_argmax_ok"] for entry in entries
        ),
    }
    return {
        "config": {
            "lengths": [entry["output_tokens"] for entry in ordered],
            "target_length": args.target_length,
            "min_antfly_tok_s": args.min_antfly_tok_s,
            "min_comparable_ratio": args.min_comparable_ratio,
            "max_cv": args.max_cv,
            "require_graph_replay": args.require_graph_replay,
            "require_generated_attention": args.require_generated_attention,
            "require_generated_q6_lm_head_argmax": args.require_generated_q6_lm_head_argmax,
            "warmups": args.warmups,
            "repeats": args.repeats,
            "prompt": args.prompt,
        },
        "entries": ordered,
        "context_slopes": slopes,
        "median_antfly_tok_s": statistics.median(entry["antfly_tok_s"] for entry in entries),
        "checks": checks,
        "passed": all(checks.values()),
    }


def run_pair_case(pair_script: pathlib.Path, run_dir: pathlib.Path, env: dict[str, str]) -> tuple[int, dict]:
    summary_path = run_dir / "paired_summary.json"
    summary_path.unlink(missing_ok=True)
    completed = subprocess.run([str(pair_script)], check=False, env=env)
    try:
        summary = json.loads(summary_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"paired benchmark exited {completed.returncode} without a readable summary at {summary_path}: {exc}"
        ) from exc
    if not isinstance(summary, dict):
        raise RuntimeError(
            f"paired benchmark exited {completed.returncode} with a non-object summary at {summary_path}"
        )
    return completed.returncode, summary


def collect_entries(args: argparse.Namespace) -> list[dict]:
    entries = []
    for length in args.lengths:
        run_dir = args.output_dir / f"tokens-{length}"
        env = os.environ.copy()
        env.update({
            "OUT_DIR": str(run_dir),
            "WARMUPS": str(args.warmups),
            "REPEATS": str(args.repeats),
            "PROMPT": args.prompt,
            "ANTFLY_TOKENS": str(length - 1),
            "LLAMA_TOKENS": str(length),
            "MIN_LLAMA_THROUGHPUT_RATIO": "0",
            "MIN_COMPARABLE_THROUGHPUT_RATIO": "0",
            "MIN_ANTFLY_TOK_S": "0",
            "MAX_ANTFLY_TOK_S_CV": "1",
            "REQUIRE_GRAPH_REPLAY": "1" if args.require_graph_replay else "0",
            "REQUIRE_GENERATED_ATTENTION": "1" if args.require_generated_attention else "0",
            "ANTFLY_GENERATED_ATTENTION_DECODE": "1" if args.require_generated_attention else "0",
            "REQUIRE_GENERATED_Q6_LM_HEAD_ARGMAX": "1" if args.require_generated_q6_lm_head_argmax else "0",
            "ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX": "1" if args.require_generated_q6_lm_head_argmax else "0",
        })
        pair_exit_code, summary = run_pair_case(args.pair_script, run_dir, env)
        entries.append(summarize_entry(length, summary, pair_exit_code))
    return entries


def main() -> None:
    args = parse_args()
    if not args.pair_script.exists():
        raise SystemExit(f"pair script does not exist: {args.pair_script}")
    if len(set(args.lengths)) != len(args.lengths) or any(length < 2 for length in args.lengths):
        raise SystemExit("lengths must be unique integers >= 2")
    if args.target_length not in args.lengths:
        raise SystemExit("target length must be included in --lengths")
    if args.warmups < 0 or args.repeats < 1:
        raise SystemExit("warmups must be non-negative and repeats positive")
    thresholds = {
        "min-antfly-tok-s": args.min_antfly_tok_s,
        "min-comparable-ratio": args.min_comparable_ratio,
        "max-cv": args.max_cv,
    }
    for name, value in thresholds.items():
        if not math.isfinite(value) or value < 0:
            raise SystemExit(f"{name} must be a finite non-negative number")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    try:
        entries = collect_entries(args)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    result = evaluate(entries, args)
    output = args.output_dir / "matrix_summary.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    target = next(entry for entry in entries if entry["output_tokens"] == args.target_length)
    print(
        f"cuda_matrix target_tok_s={target['antfly_tok_s']:.3f} "
        f"comparable_ratio={target['comparable_ratio']:.3f} cv={target['antfly_cv']:.4f} "
        f"passed={str(result['passed']).lower()} output={output}"
    )
    if not args.collect_only and not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
