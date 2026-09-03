#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Measure Qwen3-VL Metal image-request batching under qualification guards.

This measures one native request containing N image placeholders, not N
independent requests scheduled concurrently.  It is therefore an input-ingest
throughput measurement: model loading, command launch, vision preparation,
and prefill are all included in their appropriate timing fields.  The default
one-token cap deliberately isolates the image-input path from variable OCR
output length.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import statistics
import sys
from types import SimpleNamespace
from typing import Any

from qualify_qwen3vl_metal import (
    EXPECTED_SOURCE,
    QualificationError,
    git_provenance,
    run_metal,
    sha256_file,
    validate_managed_bundle,
    write_json_atomic,
)


SCHEMA = "antfly.qwen3vl.native_image_batch_benchmark.v1"
QWEN3VL_SPATIAL_MERGE_SIZE = 2

# Keep the attestation reproducible without serializing the entire caller
# environment.  These gates select every documented Qwen3-VL performance path
# used by the image-ingest benchmark (and their explicit rollbacks).
PERFORMANCE_ENVIRONMENT_KEYS = (
    "TERMITE_METAL_ENABLE_Q4_K_HIGH_ROW_MM",
    "TERMITE_METAL_DISABLE_Q4_K_HIGH_ROW_MM",
    "TERMITE_METAL_ENABLE_Q6_K_HIGH_ROW_MM",
    "TERMITE_METAL_DISABLE_Q6_K_HIGH_ROW_MM",
    "TERMITE_METAL_ENABLE_VISION_SDPA_HD64_FLASH_Q32",
    "TERMITE_METAL_DISABLE_VISION_SDPA_HD64_FLASH_Q32",
    "TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_SG_ATTENTION",
    "TERMITE_METAL_DISABLE_QWEN3VL_PREFILL_SG_ATTENTION",
    "TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_SLOTS",
    "TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_FRAME",
    "TERMITE_METAL_DISABLE_QWEN3VL_PREFILL_FAST_PATH",
    "TERMITE_METAL_ENABLE_QWEN3VL_FORWARD_FRAME",
    "TERMITE_METAL_DISABLE_QWEN3VL_FORWARD_FRAME",
    "TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_FFN",
    "TERMITE_METAL_DISABLE_QWEN3VL_PREPARED_FFN",
    "TERMITE_METAL_ENABLE_QWEN3VL_DECODE_FRAME",
    "TERMITE_METAL_DISABLE_QWEN3VL_DECODE_FRAME",
)

# Counter sampling changes timing characteristics, so retain it separately
# from performance gates. A diagnostic report must say exactly which bounded
# sample policy produced its stage breakdown.
STAGE_TIMING_ENVIRONMENT_KEYS = (
    "TERMITE_METAL_STAGE_TIMING",
    "TERMITE_METAL_STAGE_TIMING_PREFILL_MAX",
    "TERMITE_METAL_STAGE_TIMING_DECODE_START",
    "TERMITE_METAL_STAGE_TIMING_DECODE_STRIDE",
    "TERMITE_METAL_STAGE_TIMING_DECODE_MAX",
)


class ImageBatchBenchmarkError(RuntimeError):
    pass


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--antfly-bin", type=Path, required=True)
    parser.add_argument("--image", type=Path, required=True, help="single-image baseline input")
    parser.add_argument(
        "--batch-image",
        type=Path,
        action="append",
        help="image for the multi-image request; repeat to use distinct images",
    )
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--prompt", default="Read all visible text.")
    parser.add_argument("--max-tokens", type=int, default=1)
    parser.add_argument(
        "--require-stage-timing",
        action="store_true",
        help=(
            "require complete Metal stage-timing evidence in every run; set "
            "TERMITE_METAL_STAGE_TIMING=1 and its bounded sample policy explicitly"
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.25)
    parser.add_argument("--max-rss-mib", type=float, default=8192.0)
    parser.add_argument("--min-free-percent", type=int, default=15)
    parser.add_argument("--max-swap-growth-mib", type=float, default=0.0)
    parser.add_argument("--host-budget-mb", type=int, default=4096)
    parser.add_argument("--backend-budget-mb", type=int, default=6144)
    parser.add_argument("--combined-budget-mb", type=int, default=8192)
    parser.add_argument("--kv-budget-mb", type=int, default=256)
    parser.add_argument("--scratch-budget-mb", type=int, default=1024)
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> list[Path]:
    if platform.system() != "Darwin":
        raise ImageBatchBenchmarkError("Qwen3-VL Metal image batching requires macOS")
    if args.output.exists() or args.work_dir.exists():
        raise ImageBatchBenchmarkError("refusing to overwrite output or work directory")
    if not args.antfly_bin.is_file() or not args.antfly_bin.stat().st_mode & 0o111:
        raise ImageBatchBenchmarkError(f"Antfly binary is not executable: {args.antfly_bin}")
    if not 2 <= args.runs <= 10:
        raise ImageBatchBenchmarkError("runs must be in [2, 10]")
    if not 2 <= args.batch_size <= 8:
        raise ImageBatchBenchmarkError("batch-size must be in [2, 8]")
    if args.max_tokens != 1:
        raise ImageBatchBenchmarkError("this input-ingest benchmark requires --max-tokens 1")
    if args.timeout_seconds <= 0 or args.max_rss_mib <= 0:
        raise ImageBatchBenchmarkError("invalid timeout or RSS limit")
    if not 0 <= args.min_free_percent <= 100 or args.max_swap_growth_mib < 0:
        raise ImageBatchBenchmarkError("invalid system resource limits")

    batch_images = list(args.batch_image or [args.image] * args.batch_size)
    if len(batch_images) != args.batch_size:
        raise ImageBatchBenchmarkError("batch-image count must exactly equal batch-size")
    for image in [args.image, *batch_images]:
        if not image.is_file() or image.is_symlink():
            raise ImageBatchBenchmarkError(f"image must be a regular file: {image}")
    return batch_images


def native_args(args: argparse.Namespace, images: list[Path]) -> SimpleNamespace:
    return SimpleNamespace(
        model_dir=args.model_dir,
        image=images[0],
        images=images,
        antfly_bin=args.antfly_bin,
        prompt=args.prompt,
        max_tokens=args.max_tokens,
        host_budget_mb=args.host_budget_mb,
        backend_budget_mb=args.backend_budget_mb,
        combined_budget_mb=args.combined_budget_mb,
        kv_budget_mb=args.kv_budget_mb,
        scratch_budget_mb=args.scratch_budget_mb,
        vision_trace_layer=None,
        timeout_seconds=args.timeout_seconds,
        max_rss_mib=args.max_rss_mib,
        min_free_percent=args.min_free_percent,
        max_swap_growth_mib=args.max_swap_growth_mib,
        sample_interval_seconds=args.sample_interval_seconds,
    )


def required_timing(timing: dict[str, Any], profile: str, run: int) -> dict[str, float]:
    values = timing.get("timing_ms")
    if not isinstance(values, dict):
        raise ImageBatchBenchmarkError(f"{profile} run {run} omitted timing_ms")
    try:
        return {
            "generate_seconds": float(values["generate"]) / 1000.0,
            "vision_seconds": float(values["multimodal_prepare_inner"]) / 1000.0,
            "prefill_seconds": float(values["prefill_inner"]) / 1000.0,
            "load_seconds": float(values["load_model"]) / 1000.0,
            "process_seconds": float(values["total"]) / 1000.0,
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise ImageBatchBenchmarkError(f"{profile} run {run} has incomplete timing") from exc


def _positive_int(value: Any, field: str, profile: str, run: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ImageBatchBenchmarkError(
            f"{profile} run {run} has invalid positive integer {field}: {value!r}"
        )
    return value


def image_geometry_evidence(
    parity: dict[str, Any], *, profile: str, run: int, expected_count: int
) -> tuple[int, list[dict[str, Any]]]:
    """Validate and normalize exact per-image Qwen3-VL preprocessing evidence.

    The runtime reports grid dimensions before the fixed 2x2 spatial merge and
    the aggregate post-merge visual-token count. Binding both prevents a batch
    timing report from silently comparing a different visual workload.
    """

    images = parity.get("images")
    if not isinstance(images, list) or len(images) != expected_count:
        raise ImageBatchBenchmarkError(
            f"{profile} run {run} reported {len(images or [])} images, expected {expected_count}"
        )
    aggregate_visual_tokens = _positive_int(
        parity.get("visual_token_count"), "visual_token_count", profile, run
    )
    normalized: list[dict[str, Any]] = []
    pre_merge_tokens = 0
    for index, image in enumerate(images):
        if not isinstance(image, dict):
            raise ImageBatchBenchmarkError(f"{profile} run {run} image {index} is not an object")
        grid = image.get("grid_thw")
        if not isinstance(grid, list) or len(grid) != 3:
            raise ImageBatchBenchmarkError(
                f"{profile} run {run} image {index} has invalid grid_thw"
            )
        temporal, rows, columns = (
            _positive_int(value, f"images[{index}].grid_thw[{axis}]", profile, run)
            for axis, value in enumerate(grid)
        )
        pre_merge_tokens += temporal * rows * columns
        normalized.append(
            {
                "source_width": _positive_int(
                    image.get("source_width"), f"images[{index}].source_width", profile, run
                ),
                "source_height": _positive_int(
                    image.get("source_height"), f"images[{index}].source_height", profile, run
                ),
                "resized_width": _positive_int(
                    image.get("resized_width"), f"images[{index}].resized_width", profile, run
                ),
                "resized_height": _positive_int(
                    image.get("resized_height"), f"images[{index}].resized_height", profile, run
                ),
                "grid_thw": [temporal, rows, columns],
                "patch_rows": _positive_int(
                    image.get("patch_rows"), f"images[{index}].patch_rows", profile, run
                ),
                "patch_columns": _positive_int(
                    image.get("patch_columns"), f"images[{index}].patch_columns", profile, run
                ),
            }
        )
    merge_area = QWEN3VL_SPATIAL_MERGE_SIZE**2
    if pre_merge_tokens % merge_area or pre_merge_tokens // merge_area != aggregate_visual_tokens:
        raise ImageBatchBenchmarkError(
            f"{profile} run {run} visual_token_count does not match the image grids"
        )
    return aggregate_visual_tokens, normalized


def stage_timing_evidence(timing: dict[str, Any], *, profile: str, run: int) -> dict[str, Any]:
    """Validate the bounded Metal timing snapshot emitted with a request."""

    metal = timing.get("metal")
    if not isinstance(metal, dict):
        raise ImageBatchBenchmarkError(f"{profile} run {run} omitted Metal timing evidence")
    stage = metal.get("stage_timing_ns")
    if not isinstance(stage, dict):
        raise ImageBatchBenchmarkError(f"{profile} run {run} omitted stage_timing_ns")

    def nonnegative(name: str) -> int:
        value = stage.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ImageBatchBenchmarkError(
                f"{profile} run {run} has invalid stage_timing_ns.{name}: {value!r}"
            )
        return value

    enabled = nonnegative("enabled")
    supported = nonnegative("supported")
    complete = nonnegative("complete")
    samples = nonnegative("samples")
    failures = nonnegative("failures")
    if enabled != 1 or supported != 1 or complete != 1 or samples == 0 or failures != 0:
        raise ImageBatchBenchmarkError(
            f"{profile} run {run} did not produce complete stage timing "
            f"(enabled={enabled} supported={supported} complete={complete} "
            f"samples={samples} failures={failures})"
        )

    normalized: dict[str, dict[str, int]] = {}
    for regime in ("prefill", "decode"):
        raw = stage.get(regime)
        if not isinstance(raw, dict):
            raise ImageBatchBenchmarkError(f"{profile} run {run} omitted stage_timing_ns.{regime}")
        values: dict[str, int] = {}
        for bucket in ("frames", "gpu", "attention", "ffn", "ple", "tail", "embedding", "other"):
            value = raw.get(bucket)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ImageBatchBenchmarkError(
                    f"{profile} run {run} has invalid stage_timing_ns.{regime}.{bucket}: {value!r}"
                )
            values[bucket] = value
        normalized[regime] = values
    return {
        "scope": stage.get("scope"),
        "samples": samples,
        "prefill": normalized["prefill"],
        "decode": normalized["decode"],
    }


def run_profile(
    args: argparse.Namespace,
    *,
    profile: str,
    images: list[Path],
    work_dir: Path,
) -> dict[str, Any]:
    request_args = native_args(args, images)
    samples: list[dict[str, Any]] = []
    for run in range(1, args.runs + 1):
        run_dir = work_dir / f"{profile}_run_{run}"
        run_dir.mkdir()
        parity, timing, execution = run_metal(request_args, run_dir)
        visual_token_count, image_geometry = image_geometry_evidence(
            parity, profile=profile, run=run, expected_count=len(images)
        )
        token_ids = timing.get("token_ids")
        if not isinstance(token_ids, list) or len(token_ids) != args.max_tokens:
            raise ImageBatchBenchmarkError(f"{profile} run {run} omitted exactly one generated token")
        sample = {
            "run": run,
            "input_count": len(images),
            "input_sha256": [sha256_file(image) for image in images],
            "timing": required_timing(timing, profile, run),
            "resources": execution["resources"],
            "token_ids": token_ids,
            "visual_token_count": visual_token_count,
            "image_geometry": image_geometry,
            "parity_artifact": str(run_dir / "antfly_parity.json"),
        }
        if args.require_stage_timing:
            sample["stage_timing"] = stage_timing_evidence(timing, profile=profile, run=run)
        samples.append(sample)

    token_sequences = [tuple(sample["token_ids"]) for sample in samples]
    if len(set(token_sequences)) != 1:
        raise ImageBatchBenchmarkError(f"{profile} generated token sequence is not deterministic")
    geometry_snapshots = {
        json.dumps(sample["image_geometry"], sort_keys=True, separators=(",", ":"))
        for sample in samples
    }
    if len(geometry_snapshots) != 1:
        raise ImageBatchBenchmarkError(f"{profile} image geometry changed across identical runs")
    return {
        "input_count": len(images),
        "input_sha256": [sha256_file(image) for image in images],
        "runs": samples,
        "determinism": {
            "generated_token_ids": list(token_sequences[0]),
            "image_geometry_exact": True,
            "exact": True,
        },
        "median": {
            key: statistics.median(sample["timing"][key] for sample in samples)
            for key in (
                "generate_seconds",
                "vision_seconds",
                "prefill_seconds",
                "load_seconds",
                "process_seconds",
            )
        }
        | {
            "max_rss_mib": max(sample["resources"]["max_rss_mib"] for sample in samples),
            "max_swapout_growth_mib": max(
                sample["resources"]["swapout_growth_mib"] for sample in samples
            ),
            "min_free_percent": min(
                sample["resources"]["min_free_percent"] for sample in samples
            ),
        },
    }


def comparison(single: dict[str, Any], batch: dict[str, Any]) -> dict[str, float]:
    single_timing = single["median"]
    batch_timing = batch["median"]
    count = int(batch["input_count"])
    return {
        "batch_size": count,
        "single_images_per_core_second": 1.0 / single_timing["generate_seconds"],
        "batch_images_per_core_second": count / batch_timing["generate_seconds"],
        "core_throughput_gain": (
            count * single_timing["generate_seconds"] / batch_timing["generate_seconds"]
        ),
        "vision_throughput_gain": (
            count * single_timing["vision_seconds"] / batch_timing["vision_seconds"]
        ),
        "prefill_throughput_gain": (
            count * single_timing["prefill_seconds"] / batch_timing["prefill_seconds"]
        ),
    }


def performance_environment() -> dict[str, str | None]:
    """Return the complete allowlisted performance configuration.

    Recording absent gates as ``null`` distinguishes a deliberately unset
    optimization from a reporter omission while keeping unrelated process
    values (including credentials) out of the immutable benchmark report.
    """

    return {key: os.environ.get(key) for key in PERFORMANCE_ENVIRONMENT_KEYS}


def stage_timing_environment() -> dict[str, str | None]:
    return {key: os.environ.get(key) for key in STAGE_TIMING_ENVIRONMENT_KEYS}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    require_stage_timing = bool(getattr(args, "require_stage_timing", False))
    args.require_stage_timing = require_stage_timing
    report: dict[str, Any] = {"schema": SCHEMA, "pass": False}
    try:
        batch_images = validate_args(args)
        bundle = validate_managed_bundle(args.model_dir, EXPECTED_SOURCE)
        args.work_dir.mkdir(parents=True)
        single = run_profile(
            args, profile="single", images=[args.image], work_dir=args.work_dir
        )
        batch = run_profile(
            args, profile="batch", images=batch_images, work_dir=args.work_dir
        )
        report = {
            "schema": SCHEMA,
            "pass": True,
            "timing_boundary": (
                "fresh process per request; timing.generate excludes model load; "
                "one-token cap isolates image ingestion rather than full OCR output"
            ),
            "provenance": git_provenance(args.antfly_bin),
            "performance_environment": performance_environment(),
            "stage_timing": {
                "required": require_stage_timing,
                "environment": stage_timing_environment() if require_stage_timing else None,
            },
            "model_bundle": bundle,
            "input": {
                "prompt": args.prompt,
                "max_tokens": args.max_tokens,
                "single_image": str(args.image),
                "batch_images": [str(image) for image in batch_images],
                "batch_reuses_single_image": batch_images == [args.image] * args.batch_size,
            },
            "resource_limits": {
                "max_rss_mib": args.max_rss_mib,
                "min_free_percent": args.min_free_percent,
                "max_swap_growth_mib": args.max_swap_growth_mib,
                "timeout_seconds": args.timeout_seconds,
            },
            "single": single,
            "batch": batch,
            "comparison": comparison(single, batch),
        }
    except (ImageBatchBenchmarkError, QualificationError, OSError, ValueError) as exc:
        report["failure"] = str(exc)
    write_json_atomic(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
