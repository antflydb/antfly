#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Benchmark native Qwen3-VL BF16 and Q4 Metal against Transformers MPS.

This is a benchmark/qualification tool, not a serving promotion.  The BF16
bundle must have been built by ``convert_qwen3vl_high_precision.py`` and is
loaded only through Antfly's offline native CLI.  It keeps high-precision and
Q4 results as distinct profiles, with the same image, prompt, token cap, and
frozen Transformers MPS reference bound into one report.  The high-precision
lane is BF16-to-BF16 by default; it does not silently compare a BF16 native
bundle with an FP16 MPS reference.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import sys
import time
from types import SimpleNamespace
from typing import Any

import benchmark_qwen3vl_transformers_mps as mps_benchmark
import convert_qwen3vl_high_precision as high_precision
from qualify_qwen3vl_metal import (
    EXPECTED_SOURCE,
    QualificationError,
    git_provenance,
    logit_metrics,
    parity_gates,
    patch_metrics,
    run_metal,
    run_oracle,
    sha256_file,
    validate_managed_bundle,
    write_json_atomic,
)


SCHEMA = "antfly.qwen3vl.native_precision_benchmark.v1"
HIGH_PRECISION_LIMITS = {
    "min_cosine_similarity": 0.995,
    "min_pearson_correlation": 0.995,
    "max_mean_abs": 0.25,
    "max_rmse": 0.40,
    "max_max_abs": 2.0,
    "min_top_10_overlap": 9,
}


class NativePrecisionError(RuntimeError):
    pass


def high_precision_logit_pass(metrics: dict[str, Any]) -> bool:
    return (
        metrics.get("size_match") is True
        and metrics.get("finite") is True
        and metrics.get("cosine_similarity", -math.inf)
        >= HIGH_PRECISION_LIMITS["min_cosine_similarity"]
        and metrics.get("pearson_correlation", -math.inf)
        >= HIGH_PRECISION_LIMITS["min_pearson_correlation"]
        and metrics.get("mean_abs", math.inf) <= HIGH_PRECISION_LIMITS["max_mean_abs"]
        and metrics.get("rmse", math.inf) <= HIGH_PRECISION_LIMITS["max_rmse"]
        and metrics.get("max_abs", math.inf) <= HIGH_PRECISION_LIMITS["max_max_abs"]
        and metrics.get("top_k_overlap", -1) >= HIGH_PRECISION_LIMITS["min_top_10_overlap"]
    )


def validate_high_precision_bundle(model_dir: Path) -> dict[str, Any]:
    evidence = validate_managed_bundle(model_dir, high_precision.OUTPUT_IDENTITY)
    root = Path(evidence["model_dir"])
    report_path = root / "conversion-report.json"
    try:
        conversion_report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NativePrecisionError(f"invalid high-precision conversion report: {exc}") from exc
    if (
        conversion_report.get("schema") != high_precision.SCHEMA
        or conversion_report.get("pass") is not True
        or conversion_report.get("reproducible") is not True
        or conversion_report.get("benchmark_only") is not True
        or conversion_report.get("output_identity") != high_precision.OUTPUT_IDENTITY
    ):
        raise NativePrecisionError("high-precision bundle lacks a passing benchmark-only conversion report")
    evidence["conversion_report_sha256"] = sha256_file(report_path)
    evidence["conversion"] = {
        "source": conversion_report.get("source"),
        "reproduction_digests": conversion_report.get("reproduction_digests"),
        "contracts": conversion_report.get("contracts"),
    }
    return evidence


def validate_args(args: argparse.Namespace) -> None:
    if platform.system() != "Darwin":
        raise NativePrecisionError("native Qwen3-VL precision benchmark requires macOS")
    if args.output.exists():
        raise NativePrecisionError(f"refusing to overwrite output: {args.output}")
    if not args.antfly_bin.is_file() or not args.antfly_bin.stat().st_mode & 0o111:
        raise NativePrecisionError(f"Antfly binary is not executable: {args.antfly_bin}")
    if not args.image.is_file() or args.image.is_symlink():
        raise NativePrecisionError(f"image must be a regular file: {args.image}")
    if not 2 <= args.native_runs <= 10:
        raise NativePrecisionError("native runs must be in [2, 10]")
    if not 1 <= args.mps_timed_runs <= 20:
        raise NativePrecisionError("MPS timed runs must be in [1, 20]")
    if not 1 <= args.max_tokens <= 8:
        raise NativePrecisionError("max tokens must be in [1, 8]")
    if args.max_tokens != 1:
        raise NativePrecisionError(
            "this prefill-parity benchmark currently requires a one-token cap"
        )
    if args.max_rss_mib <= 0 or not 0 <= args.min_free_percent <= 100:
        raise NativePrecisionError("invalid native resource envelope")
    if args.max_swap_growth_mib < 0 or args.timeout_seconds <= 0:
        raise NativePrecisionError("invalid native resource limits")


def _native_args(args: argparse.Namespace, model_dir: Path) -> SimpleNamespace:
    return SimpleNamespace(
        model_dir=model_dir,
        image=args.image,
        antfly_bin=args.antfly_bin,
        prompt=args.prompt,
        max_tokens=args.max_tokens,
        host_budget_mb=args.host_budget_mb,
        backend_budget_mb=args.backend_budget_mb,
        combined_budget_mb=args.combined_budget_mb,
        kv_budget_mb=args.kv_budget_mb,
        scratch_budget_mb=args.scratch_budget_mb,
        vision_trace_layer=None,
        # Reuse the established qualification gate set, but bind the expected
        # token against the independently captured MPS logit below rather
        # than pre-seeding it before that reference exists.
        expected_token_id=None,
        timeout_seconds=args.timeout_seconds,
        max_rss_mib=args.max_rss_mib,
        min_free_percent=args.min_free_percent,
        max_swap_growth_mib=args.max_swap_growth_mib,
        sample_interval_seconds=args.sample_interval_seconds,
    )


def _summarize_native(
    profile: str,
    args: argparse.Namespace,
    model_dir: Path,
    oracle: dict[str, Any],
    oracle_patches: Path,
    work_dir: Path,
) -> dict[str, Any]:
    model_args = _native_args(args, model_dir)
    samples: list[dict[str, Any]] = []
    for run_index in range(1, args.native_runs + 1):
        run_dir = work_dir / f"{profile}_run_{run_index}"
        run_dir.mkdir()
        parity, timing, execution = run_metal(model_args, run_dir)
        preprocess = patch_metrics(oracle_patches, Path(execution["patches"]))
        gates = parity_gates(model_args, oracle, parity, timing, execution, preprocess)
        failed = sorted(name for name, gate in gates.items() if not gate["pass"])
        if failed:
            raise NativePrecisionError(
                f"{profile} native run {run_index} failed acceptance gates: {failed}"
            )
        timing_ms = timing.get("timing_ms")
        if not isinstance(timing_ms, dict):
            raise NativePrecisionError(f"{profile} native run {run_index} omitted timing_ms")
        try:
            core_seconds = float(timing_ms["generate"]) / 1000.0
            vision_seconds = float(timing_ms["multimodal_prepare_inner"]) / 1000.0
            prefill_seconds = float(timing_ms["prefill_inner"]) / 1000.0
        except (KeyError, TypeError, ValueError) as exc:
            raise NativePrecisionError(f"{profile} native timing is incomplete") from exc
        samples.append(
            {
                "run": run_index,
                "resources": execution["resources"],
                "timing_ms": timing_ms,
                "core_seconds": core_seconds,
                "vision_seconds": vision_seconds,
                "prefill_seconds": prefill_seconds,
                "token_ids": timing.get("token_ids"),
                "prefill_logits": execution["logits"],
                "prefill_logits_sha256": sha256_file(Path(execution["logits"])),
                "parity": parity,
            }
        )
    token_sequences = [tuple(sample["token_ids"] or []) for sample in samples]
    logit_hashes = [sample["prefill_logits_sha256"] for sample in samples]
    if len(set(token_sequences)) != 1 or len(token_sequences[0]) != args.max_tokens:
        raise NativePrecisionError(f"{profile} native token sequences are not deterministic")
    if len(set(logit_hashes)) != 1:
        raise NativePrecisionError(f"{profile} native prefill logits are not bitwise deterministic")
    return {
        "profile": profile,
        "timing_boundary": "fresh process; native timing_ms.generate excludes model load",
        "runs": samples,
        "median": {
            "core_seconds": statistics.median(sample["core_seconds"] for sample in samples),
            "vision_seconds": statistics.median(sample["vision_seconds"] for sample in samples),
            "prefill_seconds": statistics.median(sample["prefill_seconds"] for sample in samples),
            "process_seconds": statistics.median(
                sample["resources"]["elapsed_seconds"] for sample in samples
            ),
            "max_rss_mib": max(sample["resources"]["max_rss_mib"] for sample in samples),
        },
        "determinism": {
            "token_sequence_exact": True,
            "prefill_logits_bitwise_exact": True,
            "generated_token_ids": list(token_sequences[0]),
        },
    }


def run_mps(args: argparse.Namespace, work_dir: Path) -> dict[str, Any]:
    output = work_dir / "transformers_mps.json"
    result = mps_benchmark.main(
        [
            "--weights-dir",
            str(args.weights_dir),
            "--processor-dir",
            str(args.processor_dir),
            "--image",
            str(args.image),
            "--prompt",
            args.prompt,
            "--output",
            str(output),
            "--work-dir",
            str(work_dir / "transformers_mps.artifacts"),
            "--dtype",
            "bfloat16",
            "--attn-implementation",
            "sdpa",
            "--load-strategy",
            "device_map",
            "--logit-transfer",
            "clone",
            "--warmup-runs",
            str(args.mps_warmup_runs),
            "--timed-runs",
            str(args.mps_timed_runs),
            "--max-merged-tokens",
            "576",
            "--logits-to-keep",
            "1",
            "--profile-stages",
            "--timeout-seconds",
            str(args.mps_timeout_seconds),
            "--max-rss-mib",
            str(args.mps_max_rss_mib),
            "--min-free-percent",
            str(args.min_free_percent),
            "--max-swap-growth-mib",
            str(args.max_swap_growth_mib),
        ]
    )
    payload = json.loads(output.read_text(encoding="utf-8"))
    if result != 0 or payload.get("pass") is not True:
        raise NativePrecisionError(f"Transformers MPS benchmark failed: {payload.get('failure')}")
    logits = work_dir / "transformers_mps.artifacts" / "mps_last_logits.f32le"
    if not logits.is_file():
        raise NativePrecisionError("Transformers MPS benchmark omitted final logits")
    return {"report": payload, "report_path": str(output), "logits": str(logits)}


def parse_args(argv: list[str]) -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--high-precision-model-dir", type=Path, required=True)
    parser.add_argument("--q4-model-dir", type=Path, required=True)
    parser.add_argument("--weights-dir", type=Path, required=True)
    parser.add_argument("--processor-dir", type=Path, required=True)
    parser.add_argument("--image", type=Path, required=True)
    parser.add_argument("--antfly-bin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--oracle-script", type=Path, default=here / "transformers_oracle.py")
    parser.add_argument("--prompt", default="Describe the image briefly.")
    parser.add_argument("--max-tokens", type=int, default=1)
    parser.add_argument("--native-runs", type=int, default=5)
    parser.add_argument("--mps-warmup-runs", type=int, default=1)
    parser.add_argument("--mps-timed-runs", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--mps-timeout-seconds", type=float, default=300.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.25)
    parser.add_argument("--max-rss-mib", type=float, default=8192.0)
    parser.add_argument("--mps-max-rss-mib", type=float, default=8192.0)
    parser.add_argument("--min-free-percent", type=int, default=15)
    parser.add_argument("--max-swap-growth-mib", type=float, default=0.0)
    parser.add_argument("--host-budget-mb", type=int, default=4096)
    parser.add_argument("--backend-budget-mb", type=int, default=6144)
    parser.add_argument("--combined-budget-mb", type=int, default=8192)
    parser.add_argument("--kv-budget-mb", type=int, default=256)
    parser.add_argument("--scratch-budget-mb", type=int, default=1024)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    # Never turn a failing retry into an overwrite of existing benchmark
    # evidence.  The output is an attestation, not a mutable status file.
    if args.output.exists():
        print(json.dumps({"pass": False, "failure": f"refusing to overwrite output: {args.output}"}))
        return 2
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "pass": False,
        "release_ready": False,
        "created_unix_seconds": int(time.time()),
        "host": {"platform": platform.platform(), "python": platform.python_version()},
    }
    try:
        validate_args(args)
        args.image = args.image.resolve(strict=True)
        args.antfly_bin = args.antfly_bin.resolve(strict=True)
        args.weights_dir = args.weights_dir.resolve(strict=True)
        args.processor_dir = args.processor_dir.resolve(strict=True)
        args.oracle_script = args.oracle_script.resolve(strict=True)
        output = args.output.resolve()
        work_dir = (args.work_dir or output.with_suffix(".artifacts")).resolve()
        work_dir.mkdir(parents=True, exist_ok=False)
        high = validate_high_precision_bundle(args.high_precision_model_dir)
        q4 = validate_managed_bundle(args.q4_model_dir, EXPECTED_SOURCE)
        oracle_args = SimpleNamespace(
            model_dir=args.processor_dir,
            image=args.image,
            prompt=args.prompt,
            oracle_script=args.oracle_script,
            oracle_timeout_seconds=args.mps_timeout_seconds,
        )
        preprocessing_oracle, preprocessing_execution = run_oracle(oracle_args, work_dir)
        mps = run_mps(args, work_dir)
        high_native = _summarize_native(
            "bf16", args, Path(high["model_dir"]), preprocessing_oracle,
            Path(preprocessing_execution["patches"]), work_dir,
        )
        q4_native = _summarize_native(
            "q4", args, Path(q4["model_dir"]), preprocessing_oracle,
            Path(preprocessing_execution["patches"]), work_dir,
        )
        mps_logits = Path(mps["logits"])
        high_metrics = logit_metrics(mps_logits, Path(high_native["runs"][0]["prefill_logits"]))
        q4_metrics = logit_metrics(mps_logits, Path(q4_native["runs"][0]["prefill_logits"]))
        mps_argmax = mps["report"]["oracle"]["last_logits"]["argmax_token_id"]
        gates = {
            "transformers_mps_pass": mps["report"].get("pass") is True,
            "high_precision_logit_quality": high_precision_logit_pass(high_metrics),
            "high_precision_argmax_exact": high_metrics.get("actual_argmax") == mps_argmax,
            "high_precision_token_matches_mps": high_native["determinism"]["generated_token_ids"] == [mps_argmax],
            "q4_argmax_exact": q4_metrics.get("actual_argmax") == mps_argmax,
            "q4_token_matches_mps": q4_native["determinism"]["generated_token_ids"] == [mps_argmax],
        }
        report.update(
            {
                "work_dir": str(work_dir),
                "runtime_build": git_provenance(args.antfly_bin),
                "request": {
                    "prompt": args.prompt,
                    "image": str(args.image),
                    "image_sha256": sha256_file(args.image),
                    "max_tokens": args.max_tokens,
                    "max_merged_tokens": 576,
                },
                "precision_contract": {
                    "high_precision_native": "bf16",
                    "high_precision_transformers_mps": "bfloat16",
                    "q4_native": "q4_k_m decoder with q8_0 projector",
                },
                "preprocessing_oracle": preprocessing_execution,
                "transformers_mps": mps,
                "models": {"bf16": high, "q4": q4},
                "native": {"bf16": high_native, "q4": q4_native},
                "logit_metrics": {"bf16_vs_transformers_mps": high_metrics, "q4_vs_transformers_mps": q4_metrics},
                "gates": gates,
            }
        )
        report["pass"] = all(gates.values())
        if not report["pass"]:
            report["failure"] = "one or more native precision benchmark gates failed"
    except (NativePrecisionError, QualificationError, OSError, ValueError) as exc:
        report["failure"] = str(exc)
    write_json_atomic(args.output.resolve(), report)
    print(json.dumps({"pass": report["pass"], "report": str(args.output.resolve())}))
    return 0 if report["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
