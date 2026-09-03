#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Compare native-Qwen3-VL and MLX-VLM reports without mixing precision.

The report has two intentionally independent rows: BF16-to-BF16 and Q4-to-Q4.
It rejects a cross-profile comparison and requires the same image, prompt, and
token cap.  Framework-specific timing boundaries remain in the output rather
than being silently treated as serving-throughput measurements.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any

import benchmark_qwen3vl_mlx_vlm as mlx_benchmark
import benchmark_qwen3vl_native_precision as native_benchmark


SCHEMA = "antfly.qwen3vl.precision_backend_comparison.v1"


class ComparisonError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_passing_report(path: Path, schema: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ComparisonError(f"invalid report {path}: {exc}") from exc
    if value.get("schema") != schema or value.get("pass") is not True:
        raise ComparisonError(f"report is not a passing {schema}: {path}")
    return value


def _native_profile(report: dict[str, Any], profile: str) -> dict[str, Any]:
    profile_report = report.get("native", {}).get(profile)
    if not isinstance(profile_report, dict):
        raise ComparisonError(f"native report is missing {profile} evidence")
    return profile_report


def validate_request_match(
    native: dict[str, Any], mlx: dict[str, Any], native_profile: dict[str, Any]
) -> dict[str, bool]:
    native_request = native.get("request", {})
    mlx_request = mlx.get("request", {})
    native_runs = native_profile.get("runs")
    native_visual_tokens = None
    if isinstance(native_runs, list) and native_runs:
        native_visual_tokens = native_runs[0].get("parity", {}).get("visual_token_count")
    checks = {
        "prompt": native_request.get("prompt") == mlx_request.get("prompt"),
        "image_sha256": native_request.get("image_sha256") == mlx_request.get("image_sha256"),
        "max_tokens": native_request.get("max_tokens") == mlx_request.get("max_tokens"),
        "max_merged_tokens": (
            native_request.get("max_merged_tokens") == mlx_request.get("max_merged_tokens")
        ),
        "visual_token_count": native_visual_tokens == mlx_request.get("visual_token_count"),
    }
    if not all(checks.values()):
        raise ComparisonError(f"benchmark requests do not match: {checks}")
    return checks


def profile_comparison(
    native: dict[str, Any], mlx: dict[str, Any], profile: str
) -> dict[str, Any]:
    if mlx.get("profile") != profile:
        raise ComparisonError(
            f"MLX report profile mismatch: expected {profile}, got {mlx.get('profile')!r}"
        )
    native_profile = _native_profile(native, profile)
    request_checks = validate_request_match(native, mlx, native_profile)
    native_precision = native.get("precision_contract", {})
    if profile == "bf16":
        expected_native = "bf16"
        expected_transformers = "bfloat16"
    else:
        expected_native = "q4_k_m decoder with q8_0 projector"
        expected_transformers = None
    precision_checks = {
        "native_profile": native_precision.get(
            "high_precision_native" if profile == "bf16" else "q4_native"
        ) == expected_native,
        "mlx_profile": mlx.get("precision_contract", {}).get("profile") == profile,
    }
    if expected_transformers is not None:
        precision_checks["declared_transformers_mps_profile"] = (
            native_precision.get("high_precision_transformers_mps") == expected_transformers
        )
        precision_checks["measured_transformers_mps_profile"] = (
            native.get("transformers_mps", {})
            .get("report", {})
            .get("configuration", {})
            .get("dtype")
            == expected_transformers
        )
    if not all(precision_checks.values()):
        raise ComparisonError(f"benchmark precision contracts do not match: {precision_checks}")
    native_median = native_profile.get("median", {})
    mlx_median = mlx.get("benchmark", {}).get("median", {})
    native_seconds = native_median.get("core_seconds")
    mlx_seconds = mlx_median.get("end_to_end_seconds")
    if not isinstance(native_seconds, (int, float)) or native_seconds <= 0:
        raise ComparisonError(f"native {profile} report has no positive core timing")
    if not isinstance(mlx_seconds, (int, float)) or mlx_seconds <= 0:
        raise ComparisonError(f"MLX {profile} report has no positive warmed request timing")
    native_tokens = native_profile.get("determinism", {}).get("generated_token_ids")
    mlx_runs = mlx.get("benchmark", {}).get("timed", [])
    mlx_tokens = mlx_runs[0].get("generated_token_ids") if isinstance(mlx_runs, list) and mlx_runs else None
    if not isinstance(native_tokens, list) or not isinstance(mlx_tokens, list):
        raise ComparisonError(f"{profile} report lacks generated token evidence")
    return {
        "profile": profile,
        "precision_contract": precision_checks,
        "request_contract": request_checks,
        "token_sequence_exact": native_tokens == mlx_tokens,
        "native": {
            "median_core_seconds": native_seconds,
            "timing_boundary": native_profile.get("timing_boundary"),
            "generated_token_ids": native_tokens,
        },
        "mlx_vlm": {
            "median_warmed_request_seconds": mlx_seconds,
            "timing_boundary": (
                "warmed resident MLX-VLM stream_generate request, including image "
                "preparation, prompt processing, and the bounded decode"
            ),
            "generated_token_ids": mlx_tokens,
        },
        "mlx_warmed_request_over_native_core_ratio": mlx_seconds / native_seconds,
        "comparison_scope": (
            "same request and precision profile; this is a request-latency signal with "
            "framework timing boundaries retained, not a serving-throughput claim"
        ),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-report", type=Path, required=True)
    parser.add_argument("--mlx-bf16-report", type=Path, required=True)
    parser.add_argument("--mlx-q4-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    # Comparison reports are immutable evidence, just like the source
    # benchmark reports they bind.  Refuse before emitting a failure report.
    if args.output.exists():
        print(json.dumps({"pass": False, "failure": f"refusing to overwrite output: {args.output}"}))
        return 2
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "pass": False,
        "release_ready": False,
        "created_unix_seconds": int(time.time()),
    }
    try:
        native = load_passing_report(args.native_report, native_benchmark.SCHEMA)
        mlx_bf16 = load_passing_report(args.mlx_bf16_report, mlx_benchmark.SCHEMA)
        mlx_q4 = load_passing_report(args.mlx_q4_report, mlx_benchmark.SCHEMA)
        high = profile_comparison(native, mlx_bf16, "bf16")
        q4 = profile_comparison(native, mlx_q4, "q4")
        report.update(
            {
                "native_report": {
                    "path": str(args.native_report.resolve(strict=True)),
                    "sha256": sha256_file(args.native_report),
                },
                "mlx_bf16_report": {
                    "path": str(args.mlx_bf16_report.resolve(strict=True)),
                    "sha256": sha256_file(args.mlx_bf16_report),
                },
                "mlx_q4_report": {
                    "path": str(args.mlx_q4_report.resolve(strict=True)),
                    "sha256": sha256_file(args.mlx_q4_report),
                },
                "comparisons": {"high_precision": high, "q4": q4},
                "gates": {
                    "high_precision_request_exact": all(high["request_contract"].values()),
                    "q4_request_exact": all(q4["request_contract"].values()),
                    "high_precision_precision_contract_exact": all(high["precision_contract"].values()),
                    "q4_precision_contract_exact": all(q4["precision_contract"].values()),
                    "high_precision_token_sequence_exact": high["token_sequence_exact"],
                    "q4_token_sequence_exact": q4["token_sequence_exact"],
                },
            }
        )
        report["pass"] = all(report["gates"].values())
        if not report["pass"]:
            report["failure"] = "one or more cross-framework parity gates failed"
    except (ComparisonError, OSError, ValueError) as exc:
        report["failure"] = str(exc)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"pass": report["pass"], "report": str(args.output.resolve())}))
    return 0 if report["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
