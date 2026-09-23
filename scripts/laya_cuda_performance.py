#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Paired, fail-closed Laya FP32 pipeline performance gate on an idle L4."""

import argparse
import json
import math
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from laya_cuda_qualify import ROOT, run, sha256, validate_fixtures

VARIANTS = {
    "baseline": ("0", "0", "0"),
    "attention": ("1", "0", "0"),
    "fused": ("1", "1", "0"),
    "bucketed": ("1", "1", "1"),
    "candidate": (None, None, None),
}


def index_measurements(measurements):
    indexed = {}
    for row in measurements:
        key = (row["profile"], row["scope"], row["batch"])
        if key in indexed:
            raise ValueError(f"Duplicate benchmark measurement: {key}")
        for name in ("p50_ms", "p95_ms"):
            if not math.isfinite(row[name]) or row[name] <= 0:
                raise ValueError("Benchmark latency must be finite and positive")
        indexed[key] = row
    required = {
        (profile, scope, batch)
        for profile in ("fixed", "mixed")
        for scope in ("pipeline", "prepared")
        for batch in (1, 8)
    }
    if not required.issubset(indexed):
        raise ValueError("Missing required benchmark measurements")
    return indexed


def compare(baseline, candidate, pytorch):
    base, actual, torch = map(index_measurements, (baseline, candidate, pytorch))
    checks = []
    for profile in ("fixed", "mixed"):
        for batch, reference, limit in ((1, base, 1.05), (8, torch, 1.10)):
            key = (profile, "pipeline", batch)
            for metric in ("p50_ms", "p95_ms"):
                ratio = actual[key][metric] / reference[key][metric]
                checks.append(
                    {
                        "profile": profile,
                        "batch": batch,
                        "metric": metric,
                        "ratio": ratio,
                        "limit": limit,
                        "passed": ratio <= limit,
                    }
                )
    return checks


def validate_pytorch(report, samples, warmups):
    if (
        report["dtype"] != "float32"
        or report["tf32"] is not False
        or report["torch_version"] != "2.6.0+cu124"
        or report["transformers_version"] != "4.57.6"
        or report["samples"] != samples
        or report["warmups"] != warmups
    ):
        raise ValueError("Unexpected PyTorch benchmark configuration")
    index_measurements(report["measurements"])
    for row in report["measurements"]:
        if row["samples"] != samples or row["warmups"] != warmups:
            raise ValueError("Incorrect PyTorch iteration count")


def compare_bucketing(fused, candidate):
    base, actual = map(index_measurements, (fused, candidate))
    key = ("mixed", "pipeline", 8)
    return [
        {
            "profile": "mixed",
            "batch": 8,
            "metric": metric,
            "reference": "fused_without_bucketing",
            "limit": 0.95,
            "ratio": actual[key][metric] / base[key][metric],
            "passed": actual[key][metric] <= 0.95 * base[key][metric],
        }
        for metric in ("p50_ms", "p95_ms")
    ]


def parse_native(output, variant, samples, warmups):
    if re.findall(r"(\d+) selected; (\d+) passed; (\d+) skipped", output) != [
        ("1", "1", "0")
    ]:
        raise ValueError(
            "Benchmark must execute exactly one successful test with no skips"
        )
    if "Laya qualification backend=cuda " not in output:
        raise ValueError("Benchmark did not select CUDA")
    if (
        "Laya benchmark build mode=ReleaseSafe cpu=x86_64_v3 artifacts=fatbin"
        not in output
    ):
        raise ValueError("Benchmark requires ReleaseSafe x86_64_v3 fatbin")
    rows = [
        json.loads(line.removeprefix("Laya benchmark "))
        for line in output.splitlines()
        if line.startswith("Laya benchmark {")
    ]
    index_measurements(rows)
    for row in rows:
        if row["samples"] != samples or row["warmups"] != warmups:
            raise ValueError("Incorrect benchmark iteration count")
        if variant == "baseline" and (row["warp_attention"] or row["packed_geglu"]):
            raise ValueError("Baseline exercised optimized kernels")
        if variant != "baseline" and row["batch"] == 8 and row["warp_attention"] <= 0:
            raise ValueError("Candidate did not exercise optimized attention")
        if variant in ("fused", "bucketed", "candidate") and row["packed_geglu"] <= 0:
            raise ValueError("Candidate did not exercise packed exact GELU")
        if (
            variant in ("bucketed", "candidate")
            and row["profile"] == "mixed"
            and row["scope"] == "pipeline"
            and row["batch"] == 8
            and (row["chunks"] != 3 or row["padded_tokens"] != 820)
        ):
            raise ValueError("Candidate did not exercise the expected length buckets")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tests", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--uv", default="uv")
    parser.add_argument("--nvidia-smi", default="nvidia-smi")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument(
        "--variants",
        nargs="+",
        choices=VARIANTS,
        default=["baseline", "fused", "candidate"],
    )
    args = parser.parse_args()
    if (
        args.rounds < 1
        or args.samples < 2
        or args.warmups < 0
        or "baseline" not in args.variants
        or len(set(args.variants)) != len(args.variants)
    ):
        parser.error(
            "Positive rounds, >=2 samples, nonnegative warmups and distinct variants including baseline are required"
        )
    args.tests = args.tests.resolve()
    args.work_dir = args.work_dir.resolve()
    output_dir = args.report.resolve().parent
    output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "schema": "antfly.laya.cuda_performance.v1",
        "status": "failed",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "rounds": [],
    }
    try:
        validate_fixtures(args.work_dir)
        report["gpu"] = run(
            [
                args.nvidia_smi,
                "--query-gpu=name,uuid,driver_version,compute_cap,memory.total",
                "--format=csv,noheader",
            ]
        )
        if "NVIDIA L4" not in report["gpu"]:
            raise ValueError("This gate requires an L4")
        report["tests_sha256"] = sha256(args.tests)
        report["checkpoint_sha256"] = sha256(
            args.work_dir / "released/model/model.safetensors"
        )
        report["fixture_sha256"] = sha256(args.work_dir / "released/qualification.json")
        report["git_commit"] = run(["git", "-C", ROOT, "rev-parse", "HEAD"]).strip()
        report["working_tree_dirty"] = bool(
            run(["git", "-C", ROOT, "status", "--porcelain"]).strip()
        )
        report["cuda_fatbin_sha256"] = sha256(
            ROOT
            / "zig/pkg/inference/src/ops/cuda/artifacts/inference_cuda_kernels.fatbin"
        )
        report["variants"] = args.variants
        report["samples"], report["warmups"] = args.samples, args.warmups
        env = {
            **os.environ,
            "ANTFLY_LAYA_BACKEND": "cuda",
            "ANTFLY_LAYA_REQUIRE_TESTS": "1",
            "ANTFLY_LAYA_QUALIFICATION": str(args.work_dir / "released"),
            "ANTFLY_LAYA_PERFORMANCE_ONLY": "1",
            "ANTFLY_LAYA_BENCH_SAMPLES": str(args.samples),
            "ANTFLY_LAYA_BENCH_WARMUPS": str(args.warmups),
            "ANTFLY_CUDA_ALLOW_HOST_ATTENTION_FALLBACK": "0",
            "NVIDIA_TF32_OVERRIDE": "0",
        }
        for key in (
            "ANTFLY_LAYA_METAL",
            "ANTFLY_INFERENCE_TEST_RUNTIME_OFFSET",
            "ANTFLY_INFERENCE_TEST_RUNTIME_LIMIT",
            "ANTFLY_INFERENCE_TEST_LIST_FILE",
        ):
            env.pop(key, None)
        for iteration in range(args.rounds):
            order = ["pytorch", *args.variants]
            if iteration % 2:
                order.reverse()
            result = {"order": order}
            for variant in order:
                print(
                    f"Laya performance round {iteration + 1}/{args.rounds}: {variant}",
                    flush=True,
                )
                log = output_dir / f"round-{iteration + 1}-{variant}.log"
                if variant == "pytorch":
                    destination = output_dir / f"round-{iteration + 1}-pytorch.json"
                    run(
                        [
                            args.uv,
                            "run",
                            "--index",
                            "https://download.pytorch.org/whl/cu124",
                            "--index-strategy",
                            "unsafe-best-match",
                            ROOT / "scripts/laya_cuda_benchmark.py",
                            "--work-dir",
                            args.work_dir,
                            "--output",
                            destination,
                            "--samples",
                            args.samples,
                            "--warmups",
                            args.warmups,
                            "--scopes",
                            "pipeline",
                            "prepared",
                        ],
                        env=env,
                        log=log,
                    )
                    result[variant] = json.loads(destination.read_text())
                    validate_pytorch(result[variant], args.samples, args.warmups)
                else:
                    variant_env = dict(env)
                    for name, value in zip(
                        (
                            "ANTFLY_CUDA_LAYA_OPTIMIZATIONS",
                            "ANTFLY_CUDA_LAYA_FUSION",
                            "ANTFLY_CUDA_LAYA_BUCKETING",
                        ),
                        VARIANTS[variant],
                        strict=True,
                    ):
                        variant_env.pop(name, None)
                        if value is not None:
                            variant_env[name] = value
                    output = run(
                        [args.tests, "--test-filter", "laya released"],
                        env=variant_env,
                        log=log,
                    )
                    result[variant] = {
                        "measurements": parse_native(
                            output, variant, args.samples, args.warmups
                        )
                    }
            result["checks"] = {
                variant: compare(
                    result["baseline"]["measurements"],
                    result[variant]["measurements"],
                    result["pytorch"]["measurements"],
                )
                for variant in args.variants
                if variant != "baseline"
            }
            if "candidate" in result and "fused" in result:
                result["checks"]["candidate"] += compare_bucketing(
                    result["fused"]["measurements"], result["candidate"]["measurements"]
                )
            report["rounds"].append(result)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        qualifying = (
            args.rounds >= 3
            and args.samples >= 100
            and args.warmups >= 10
            and "candidate" in args.variants
            and "fused" in args.variants
        )
        if qualifying:
            report["status"] = (
                "passed"
                if all(
                    check["passed"]
                    for result in report["rounds"]
                    for check in result["checks"]["candidate"]
                )
                else "performance_gate_failed"
            )
        else:
            report["status"] = "exploratory"
    except (
        OSError,
        ValueError,
        RuntimeError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
    ) as error:
        report["error"] = str(error)
    report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "report": str(args.report)}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
