#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Run a fail-closed, resident-prefill Qwen3-VL Transformers MPS benchmark."""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import platform
import sys
import time
from typing import Any

from qualify_qwen3vl_metal import (
    QualificationError,
    ResourceViolation,
    load_json,
    logit_metrics,
    run_resource_monitored,
    sha256_file,
    write_json_atomic,
)
from transformers_weights_oracle import MODEL_SHA256


SCHEMA = "antfly.qwen3vl.transformers_mps_benchmark.v1"
ORACLE_SCHEMA = "antfly.qwen3vl.transformers_weights_oracle.v1"
MPS_PARITY_LIMITS = {
    "min_cosine_similarity": 0.995,
    "min_pearson_correlation": 0.995,
    "max_mean_abs": 0.25,
    "max_rmse": 0.40,
    "max_max_abs": 2.0,
    "min_top_10_overlap": 9,
}


def validate_args(args: argparse.Namespace) -> None:
    if platform.system() != "Darwin":
        raise QualificationError("Transformers MPS benchmark requires macOS")
    if args.output.exists():
        raise QualificationError(f"refusing to overwrite output: {args.output}")
    for label, path in (
        ("oracle script", args.oracle_script),
        ("requirements file", args.requirements_file),
    ):
        if not path.is_file():
            raise QualificationError(f"missing {label}: {path}")
    if (args.reference_json is None) != (args.reference_logits is None):
        raise QualificationError("reference JSON and logits must be supplied together")
    if args.warmup_runs < 0 or args.warmup_runs > 10:
        raise QualificationError("warmup runs must be in [0, 10]")
    if args.timed_runs < 1 or args.timed_runs > 20:
        raise QualificationError("timed runs must be in [1, 20]")
    if args.max_merged_tokens < 1 or args.max_merged_tokens > 576:
        raise QualificationError("max merged tokens must be in [1, 576]")
    if args.logits_to_keep < 1:
        raise QualificationError("MPS benchmark logits to keep must be positive")
    if not 0.0 < args.mps_high_watermark_ratio <= 1.0:
        raise QualificationError("MPS high watermark ratio must be in (0, 1]")
    if not 0.0 <= args.mps_low_watermark_ratio <= args.mps_high_watermark_ratio:
        raise QualificationError(
            "MPS low watermark ratio must be in [0, high watermark]"
        )
    if args.timeout_seconds <= 0 or args.sample_interval_seconds <= 0:
        raise QualificationError("timeouts and sample intervals must be positive")
    if args.max_rss_mib <= 0 or not 0 <= args.min_free_percent <= 100:
        raise QualificationError("invalid resource envelope")
    if args.max_swap_growth_mib < 0:
        raise QualificationError("maximum swap growth must be non-negative")


def build_environment(args: argparse.Namespace) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "HF_HUB_DISABLE_PROGRESS_BARS": "1",
            "PYTORCH_ENABLE_MPS_FALLBACK": "0",
            "PYTORCH_MPS_FAST_MATH": "0",
            "PYTORCH_MPS_HIGH_WATERMARK_RATIO": str(args.mps_high_watermark_ratio),
            "PYTORCH_MPS_LOW_WATERMARK_RATIO": str(args.mps_low_watermark_ratio),
            "PYTORCH_MPS_PREFER_METAL": "1" if args.mps_prefer_metal else "0",
        }
    )
    return env


def oracle_command(args: argparse.Namespace, work_dir: Path) -> list[str]:
    command = [
        sys.executable,
        str(args.oracle_script),
        "--weights-dir",
        str(args.weights_dir),
        "--processor-dir",
        str(args.processor_dir),
        "--image",
        str(args.image),
        "--prompt",
        args.prompt,
        "--device",
        "mps",
        "--dtype",
        args.dtype,
        "--attn-implementation",
        args.attn_implementation,
        "--load-strategy",
        args.load_strategy,
        "--logit-transfer",
        args.logit_transfer,
        "--warmup-runs",
        str(args.warmup_runs),
        "--timed-runs",
        str(args.timed_runs),
        "--max-merged-tokens",
        str(args.max_merged_tokens),
        "--top-k",
        "20",
        "--logits-to-keep",
        str(args.logits_to_keep),
        "--logits-output",
        str(work_dir / "mps_last_logits.f32le"),
        "--output",
        str(work_dir / "mps_oracle.json"),
    ]
    if args.profile_stages:
        command.append("--profile-stages")
    return command


def parity_quality_pass(metrics: dict[str, Any]) -> bool:
    return (
        metrics.get("size_match") is True
        and metrics.get("finite") is True
        and metrics.get("cosine_similarity", -math.inf)
        >= MPS_PARITY_LIMITS["min_cosine_similarity"]
        and metrics.get("pearson_correlation", -math.inf)
        >= MPS_PARITY_LIMITS["min_pearson_correlation"]
        and metrics.get("mean_abs", math.inf) <= MPS_PARITY_LIMITS["max_mean_abs"]
        and metrics.get("rmse", math.inf) <= MPS_PARITY_LIMITS["max_rmse"]
        and metrics.get("max_abs", math.inf) <= MPS_PARITY_LIMITS["max_max_abs"]
        and metrics.get("top_k_overlap", -1) >= MPS_PARITY_LIMITS["min_top_10_overlap"]
    )


def reference_contract(
    reference: dict[str, Any], candidate: dict[str, Any], reference_logits: Path
) -> dict[str, Any]:
    reference_request = reference.get("request", {})
    candidate_request = candidate.get("request", {})
    reference_model_sha = reference.get("model", {}).get("sha256")
    candidate_model_sha = candidate.get("model", {}).get("sha256")
    expected_logits_sha = reference.get("last_logits", {}).get("f32le_sha256")
    checks = {
        "schema": reference.get("schema") == ORACLE_SCHEMA,
        "reference_model_sha256": reference_model_sha == MODEL_SHA256,
        "candidate_model_sha256": candidate_model_sha == MODEL_SHA256,
        "model_sha256_exact": reference_model_sha == candidate_model_sha,
        "reference_logits_sha256": expected_logits_sha == sha256_file(reference_logits),
        "prompt": reference_request.get("prompt") == candidate_request.get("prompt"),
        "image_sha256": reference_request.get("image_sha256")
        == candidate_request.get("image_sha256"),
        "input_ids": reference_request.get("input_ids")
        == candidate_request.get("input_ids"),
        "image_grid_thw": reference_request.get("image_grid_thw")
        == candidate_request.get("image_grid_thw"),
    }
    return {"pass": all(checks.values()), "checks": checks}


def parse_args(argv: list[str]) -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weights-dir", type=Path, required=True)
    parser.add_argument("--processor-dir", type=Path, required=True)
    parser.add_argument("--image", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument(
        "--oracle-script",
        type=Path,
        default=here / "transformers_weights_oracle.py",
    )
    parser.add_argument(
        "--requirements-file",
        type=Path,
        default=here / "requirements-qwen3vl-oracle.txt",
    )
    parser.add_argument("--reference-json", type=Path)
    parser.add_argument("--reference-logits", type=Path)
    parser.add_argument("--prompt", default="Describe the image briefly.")
    parser.add_argument("--dtype", choices=("float16", "bfloat16"), default="float16")
    parser.add_argument(
        "--attn-implementation",
        choices=("sdpa", "eager"),
        default="sdpa",
    )
    parser.add_argument(
        "--load-strategy",
        choices=("cpu_then_move", "device_map"),
        default="device_map",
    )
    parser.add_argument("--logit-transfer", choices=("clone", "view"), default="clone")
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--timed-runs", type=int, default=3)
    parser.add_argument("--max-merged-tokens", type=int, default=576)
    parser.add_argument(
        "--logits-to-keep",
        type=int,
        default=1,
        help="compute only final-token logits for parity with native prefill",
    )
    parser.add_argument("--profile-stages", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.25)
    parser.add_argument("--max-rss-mib", type=float, default=8192.0)
    parser.add_argument("--min-free-percent", type=int, default=15)
    parser.add_argument("--max-swap-growth-mib", type=float, default=0.0)
    parser.add_argument("--mps-high-watermark-ratio", type=float, default=0.80)
    parser.add_argument("--mps-low-watermark-ratio", type=float, default=0.70)
    parser.add_argument("--mps-prefer-metal", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "pass": False,
        "release_ready": False,
        "created_unix_seconds": int(time.time()),
        "host": {"platform": platform.platform(), "python": platform.python_version()},
    }
    try:
        validate_args(args)
        output = args.output.resolve()
        work_dir = (args.work_dir or output.with_suffix(".artifacts")).resolve()
        work_dir.mkdir(parents=True, exist_ok=False)
        command = oracle_command(args, work_dir)
        environment = build_environment(args)
        report["configuration"] = {
            "device": "mps",
            "dtype": args.dtype,
            "attention_implementation": args.attn_implementation,
            "load_strategy": args.load_strategy,
            "logit_transfer": args.logit_transfer,
            "warmup_runs": args.warmup_runs,
            "timed_runs": args.timed_runs,
            "max_merged_tokens": args.max_merged_tokens,
            "logits_to_keep": args.logits_to_keep,
            "profile_stages": args.profile_stages,
            "mps_high_watermark_ratio": args.mps_high_watermark_ratio,
            "mps_low_watermark_ratio": args.mps_low_watermark_ratio,
            "mps_prefer_metal": args.mps_prefer_metal,
        }
        report["harness"] = {
            "python_executable": sys.executable,
            "script": {
                "path": str(Path(__file__).resolve()),
                "sha256": sha256_file(Path(__file__)),
            },
            "oracle_script": {
                "path": str(args.oracle_script.resolve(strict=True)),
                "sha256": sha256_file(args.oracle_script),
            },
            "requirements": {
                "path": str(args.requirements_file.resolve(strict=True)),
                "sha256": sha256_file(args.requirements_file),
            },
        }
        report["request"] = {
            "prompt": args.prompt,
            "image": str(args.image.resolve(strict=True)),
            "image_sha256": sha256_file(args.image),
        }
        try:
            execution = run_resource_monitored(
                command,
                work_dir / "mps_oracle.stdout.log",
                work_dir / "mps_oracle.stderr.log",
                timeout_seconds=args.timeout_seconds,
                max_rss_mib=args.max_rss_mib,
                min_free_percent=args.min_free_percent,
                max_swap_growth_mib=args.max_swap_growth_mib,
                sample_interval_seconds=args.sample_interval_seconds,
                env=environment,
                label="Transformers MPS benchmark",
            )
        except ResourceViolation as exc:
            report["execution"] = exc.execution
            raise
        report["execution"] = execution
        if execution["returncode"] != 0:
            raise QualificationError(
                f"MPS oracle exited {execution['returncode']}: {execution['stderr'].strip()[-2000:]}"
            )
        candidate_path = work_dir / "mps_oracle.json"
        candidate_logits = work_dir / "mps_last_logits.f32le"
        candidate = load_json(candidate_path)
        if candidate.get("schema") != ORACLE_SCHEMA:
            raise QualificationError(
                f"unexpected oracle schema: {candidate.get('schema')}"
            )
        report["oracle"] = candidate
        deterministic = candidate.get("benchmark", {}).get(
            "timed_logits_bitwise_deterministic"
        )
        report["gates"] = {"timed_logits_bitwise_deterministic": deterministic is True}
        if args.reference_json is not None and args.reference_logits is not None:
            reference = load_json(args.reference_json)
            contract = reference_contract(reference, candidate, args.reference_logits)
            metrics = logit_metrics(args.reference_logits, candidate_logits)
            parity_pass = (
                contract["pass"]
                and parity_quality_pass(metrics)
                and metrics.get("reference_argmax") == metrics.get("actual_argmax")
            )
            report["reference"] = {
                "json": str(args.reference_json.resolve(strict=True)),
                "json_sha256": sha256_file(args.reference_json),
                "logits": str(args.reference_logits.resolve(strict=True)),
                "logits_sha256": sha256_file(args.reference_logits),
                "contract": contract,
                "metrics": metrics,
                "limits": MPS_PARITY_LIMITS,
            }
            report["gates"]["reference_contract_exact"] = contract["pass"]
            report["gates"]["reference_logit_quality"] = parity_quality_pass(metrics)
            report["gates"]["reference_argmax_exact"] = metrics.get(
                "reference_argmax"
            ) == metrics.get("actual_argmax")
            report["gates"]["reference_parity"] = parity_pass
        report["pass"] = all(report["gates"].values())
        if not report["pass"]:
            report["failure"] = "one or more MPS benchmark gates failed"
    except (QualificationError, OSError, ValueError) as exc:
        report["failure"] = str(exc)
    write_json_atomic(args.output.resolve(), report)
    print(json.dumps({"pass": report["pass"], "report": str(args.output.resolve())}))
    return 0 if report["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
