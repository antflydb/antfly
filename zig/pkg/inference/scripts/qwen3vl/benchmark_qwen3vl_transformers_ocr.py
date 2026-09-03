#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Benchmark bounded Qwen3-VL OCR generation with pinned Transformers.

This runner measures the complete ``model.generate`` OCR request on either
CPU or MPS.  It deliberately keeps model loading outside the timed request,
uses the same image-token cap as the native Qwen3-VL qualification lane, and
records generated token IDs and decoded text for every timed run.  The parent
process enforces the resource envelope; the worker process owns the model so a
limit violation can be terminated without leaving a resident model behind.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import sys
import time
from typing import Any

from qualify_qwen3vl_metal import (
    QualificationError,
    ResourceViolation,
    load_json,
    run_resource_monitored,
    sha256_file,
    write_json_atomic,
)
from transformers_oracle import REQUIRED_SIDECARS, verify_environment
from transformers_weights_oracle import (
    ATTENTION_IMPLEMENTATIONS,
    DTYPES,
    LOAD_STRATEGIES,
    MODEL_REVISION,
    MODEL_SHA256,
    MODEL_SIZE,
    make_inputs,
    mps_memory_snapshot,
    percentile,
    synchronize,
    torch_dtype,
)


SCHEMA = "antfly.qwen3vl.transformers_ocr_benchmark.v1"
WORKER_SCHEMA = "antfly.qwen3vl.transformers_ocr_worker.v1"
DEVICES = ("cpu", "mps")


def validate_args(args: argparse.Namespace) -> None:
    if platform.system() != "Darwin":
        raise QualificationError("Qwen3-VL Transformers OCR benchmark requires macOS")
    if args.device not in DEVICES:
        raise QualificationError(f"unsupported device: {args.device}")
    if args.dtype not in DTYPES:
        raise QualificationError(f"unsupported dtype: {args.dtype}")
    if args.device == "cpu" and args.dtype != "bfloat16":
        raise QualificationError("the canonical CPU OCR lane requires bfloat16")
    if args.attn_implementation not in ATTENTION_IMPLEMENTATIONS:
        raise QualificationError(f"unsupported attention implementation: {args.attn_implementation}")
    if args.load_strategy not in LOAD_STRATEGIES:
        raise QualificationError(f"unsupported load strategy: {args.load_strategy}")
    if not args.prompt:
        raise QualificationError("prompt must not be empty")
    if not 1 <= args.max_tokens <= 256:
        raise QualificationError("max tokens must be in [1, 256]")
    if not 1 <= args.max_merged_tokens <= 576:
        raise QualificationError("max merged tokens must be in [1, 576]")
    if not 0 <= args.warmup_runs <= 10 or not 1 <= args.timed_runs <= 20:
        raise QualificationError("warmup runs must be in [0, 10] and timed runs in [1, 20]")
    if args.timeout_seconds <= 0 or args.sample_interval_seconds <= 0:
        raise QualificationError("timeouts and sample intervals must be positive")
    if args.max_rss_mib <= 0 or not 0 <= args.min_free_percent <= 100:
        raise QualificationError("invalid resource envelope")
    if args.max_swap_growth_mib < 0:
        raise QualificationError("maximum swap growth must be non-negative")
    if not 0.0 < args.mps_high_watermark_ratio <= 1.0:
        raise QualificationError("MPS high watermark ratio must be in (0, 1]")
    if not 0.0 <= args.mps_low_watermark_ratio <= args.mps_high_watermark_ratio:
        raise QualificationError("MPS low watermark ratio must be in [0, high watermark]")


def benchmark_environment(args: argparse.Namespace) -> dict[str, str]:
    """Return the offline, deterministic child environment."""

    environment = os.environ.copy()
    environment.update(
        {
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "HF_HUB_DISABLE_PROGRESS_BARS": "1",
        }
    )
    if args.device == "mps":
        environment.update(
            {
                "PYTORCH_ENABLE_MPS_FALLBACK": "0",
                "PYTORCH_MPS_FAST_MATH": "0",
                "PYTORCH_MPS_HIGH_WATERMARK_RATIO": str(args.mps_high_watermark_ratio),
                "PYTORCH_MPS_LOW_WATERMARK_RATIO": str(args.mps_low_watermark_ratio),
                "PYTORCH_MPS_PREFER_METAL": "1" if args.mps_prefer_metal else "0",
            }
        )
    return environment


def worker_command(args: argparse.Namespace, worker_output: Path) -> list[str]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--worker-output",
        str(worker_output),
        "--weights-dir",
        str(args.weights_dir),
        "--processor-dir",
        str(args.processor_dir),
        "--image",
        str(args.image),
        "--prompt",
        args.prompt,
        "--device",
        args.device,
        "--dtype",
        args.dtype,
        "--attn-implementation",
        args.attn_implementation,
        "--load-strategy",
        args.load_strategy,
        "--warmup-runs",
        str(args.warmup_runs),
        "--timed-runs",
        str(args.timed_runs),
        "--max-tokens",
        str(args.max_tokens),
        "--max-merged-tokens",
        str(args.max_merged_tokens),
    ]
    return command


def _validate_model_bundle(args: argparse.Namespace) -> tuple[Path, Path, str]:
    processor_dir = args.processor_dir.resolve(strict=True)
    weights_dir = args.weights_dir.resolve(strict=True)
    model_path = weights_dir / "model.safetensors"
    if not model_path.is_file():
        raise QualificationError(f"missing BF16 weights: {model_path}")
    if model_path.stat().st_size != MODEL_SIZE:
        raise QualificationError(
            f"BF16 weight size mismatch: expected {MODEL_SIZE}, got {model_path.stat().st_size}"
        )
    model_sha = sha256_file(model_path)
    if model_sha != MODEL_SHA256:
        raise QualificationError(f"BF16 weight SHA-256 mismatch: {model_sha}")
    for name in REQUIRED_SIDECARS:
        processor_sidecar = processor_dir / name
        weight_sidecar = weights_dir / name
        if not processor_sidecar.is_file() or not weight_sidecar.is_file():
            raise QualificationError(f"missing required sidecar: {name}")
        if sha256_file(processor_sidecar) != sha256_file(weight_sidecar):
            raise QualificationError(f"weights and processor sidecar mismatch: {name}")
    return weights_dir, processor_dir, model_sha


def _finish_reason(token_ids: list[int], max_tokens: int, eos_token_id: Any) -> str:
    if len(token_ids) == max_tokens:
        return "length"
    if isinstance(eos_token_id, int) and token_ids and token_ids[-1] == eos_token_id:
        return "eos"
    if isinstance(eos_token_id, list) and token_ids and token_ids[-1] in eos_token_id:
        return "eos"
    return "generation_stop"


def merged_visual_token_count(image_grid_thw: list[list[int]], merge_size: int) -> int:
    """Calculate post-merge visual tokens from the processor's image grid."""

    if merge_size <= 0:
        raise QualificationError(f"invalid spatial merge size: {merge_size}")
    patch_tokens = 0
    for row in image_grid_thw:
        if len(row) != 3 or any(value <= 0 for value in row):
            raise QualificationError(f"invalid image_grid_thw row: {row!r}")
        patch_tokens += row[0] * row[1] * row[2]
    divisor = merge_size * merge_size
    if patch_tokens <= 0 or patch_tokens % divisor != 0:
        raise QualificationError(
            f"image patch count {patch_tokens} is not divisible by merge area {divisor}"
        )
    return patch_tokens // divisor


def run_worker(args: argparse.Namespace) -> dict[str, Any]:
    """Run the resident-model OCR lane once the parent has admitted resources."""

    versions = verify_environment()
    import torch
    from transformers import AutoProcessor, Qwen3VLForConditionalGeneration
    from transformers.utils import logging as transformers_logging

    transformers_logging.disable_progress_bar()
    if args.device == "mps" and not torch.backends.mps.is_available():
        raise QualificationError("MPS is not available")
    weights_dir, processor_dir, model_sha = _validate_model_bundle(args)
    processor = AutoProcessor.from_pretrained(
        processor_dir,
        local_files_only=True,
        trust_remote_code=False,
        use_fast=True,
    )

    load_started = time.monotonic()
    load_options = {
        "local_files_only": True,
        "trust_remote_code": False,
        "dtype": torch_dtype(args.dtype, torch),
        "attn_implementation": args.attn_implementation,
        "low_cpu_mem_usage": True,
    }
    if args.load_strategy == "device_map":
        model = Qwen3VLForConditionalGeneration.from_pretrained(
            weights_dir,
            **load_options,
            device_map={"": args.device},
        )
    else:
        model = Qwen3VLForConditionalGeneration.from_pretrained(weights_dir, **load_options)
        model.to(args.device)
    model.eval()
    synchronize(args.device)
    loaded_at = time.monotonic()

    snapshots: list[dict[str, Any]] = []

    def capture_memory(stage: str, run: int | None = None) -> None:
        snapshot = mps_memory_snapshot(args.device, torch)
        if snapshot is not None:
            snapshots.append({"stage": stage, "run": run, **snapshot})

    capture_memory("model_loaded")
    # Prepare one untimed contract input for report evidence. Every warmup and
    # timed request below repeats this work inside its timing boundary.
    rendered, evidence_inputs = make_inputs(args, processor)
    input_ids = [int(token) for token in evidence_inputs["input_ids"].reshape(-1).tolist()]
    image_grid_thw = [
        [int(item) for item in row] for row in evidence_inputs["image_grid_thw"].tolist()
    ]
    prompt_tokens = len(input_ids)
    visual_token_count = merged_visual_token_count(
        image_grid_thw, int(processor.image_processor.merge_size)
    )

    def generate_once() -> dict[str, Any]:
        synchronize(args.device)
        started = time.monotonic()
        current_rendered, inputs = make_inputs(args, processor)
        if current_rendered != rendered:
            raise QualificationError("rendered prompt changed during one OCR benchmark")
        current_input_ids = [int(token) for token in inputs["input_ids"].reshape(-1).tolist()]
        current_grid = [
            [int(item) for item in row] for row in inputs["image_grid_thw"].tolist()
        ]
        if current_input_ids != input_ids or current_grid != image_grid_thw:
            raise QualificationError("OCR preprocessing changed during one benchmark")
        preprocessed_at = time.monotonic()
        model_inputs = {
            name: value.to(args.device)
            for name, value in inputs.items()
            if hasattr(value, "to")
        }
        synchronize(args.device)
        inputs_moved_at = time.monotonic()
        sequences = model.generate(
            **model_inputs,
            do_sample=False,
            num_beams=1,
            use_cache=True,
            repetition_penalty=1.0,
            max_new_tokens=args.max_tokens,
            return_dict_in_generate=False,
        )
        synchronize(args.device)
        elapsed = time.monotonic() - started
        if elapsed <= 0.0:
            raise QualificationError("non-positive OCR request timing")
        if sequences.ndim != 2 or sequences.shape[0] != 1:
            raise QualificationError(f"unexpected generation sequence shape: {tuple(sequences.shape)}")
        if sequences.shape[1] <= prompt_tokens:
            raise QualificationError("Transformers generated no OCR tokens")
        token_ids = [int(token) for token in sequences[0, prompt_tokens:].tolist()]
        if len(token_ids) > args.max_tokens:
            raise QualificationError(
                f"Transformers generated {len(token_ids)} tokens above cap {args.max_tokens}"
            )
        text = processor.decode(
            token_ids,
            skip_special_tokens=True,
            clean_up_tokenization_spaces=False,
        )
        return {
            "end_to_end_seconds": elapsed,
            "preprocess_seconds": preprocessed_at - started,
            "input_move_seconds": inputs_moved_at - preprocessed_at,
            "model_generate_seconds": elapsed - (inputs_moved_at - started),
            "generated_token_ids": token_ids,
            "generated_text": text,
            "generated_token_count": len(token_ids),
            "finish_reason": _finish_reason(
                token_ids, args.max_tokens, model.generation_config.eos_token_id
            ),
            "end_to_end_tokens_per_second": len(token_ids) / elapsed,
        }

    warmups: list[dict[str, Any]] = []
    timed: list[dict[str, Any]] = []
    benchmark_started = time.monotonic()
    with torch.inference_mode():
        for run_index in range(args.warmup_runs):
            warmups.append(generate_once())
            capture_memory("warmup_complete", run_index + 1)
        for run_index in range(args.timed_runs):
            timed.append(generate_once())
            capture_memory("timed_complete", run_index + 1)
    if not timed:
        raise QualificationError("no timed OCR samples")
    token_sequences = [tuple(sample["generated_token_ids"]) for sample in timed]
    if len(set(token_sequences)) != 1:
        raise QualificationError("timed OCR token sequences are not deterministic")
    generated = timed[0]
    finished_at = time.monotonic()
    return {
        "schema": WORKER_SCHEMA,
        "model": {
            "repository": "Qwen/Qwen3-VL-2B-Instruct",
            "revision": MODEL_REVISION,
            "path": str(weights_dir / "model.safetensors"),
            "size": MODEL_SIZE,
            "sha256": model_sha,
            "dtype": args.dtype,
        },
        "runtime": {
            "python": platform.python_version(),
            "packages": versions,
            "device": args.device,
            "torch_mps_available": bool(torch.backends.mps.is_available()),
            "attention_implementation": args.attn_implementation,
            "load_strategy": args.load_strategy,
            "mps_environment": {
                name: os.environ.get(name)
                for name in (
                    "PYTORCH_ENABLE_MPS_FALLBACK",
                    "PYTORCH_MPS_FAST_MATH",
                    "PYTORCH_MPS_HIGH_WATERMARK_RATIO",
                    "PYTORCH_MPS_LOW_WATERMARK_RATIO",
                    "PYTORCH_MPS_PREFER_METAL",
                )
            }
            if args.device == "mps"
            else None,
        },
        "request": {
            "prompt": args.prompt,
            "rendered_prompt_sha256": hashlib.sha256(rendered.encode()).hexdigest(),
            "image": str(args.image.resolve(strict=True)),
            "image_sha256": sha256_file(args.image),
            "input_ids": input_ids,
            "prompt_token_count": prompt_tokens,
            "image_grid_thw": image_grid_thw,
            "visual_token_count": visual_token_count,
            "max_merged_tokens": args.max_merged_tokens,
            "max_tokens": args.max_tokens,
        },
        "timing_seconds": {
            "load_and_move": loaded_at - load_started,
            "resident_benchmark_total": finished_at - benchmark_started,
            "warmup_samples": warmups,
            "timed_samples": timed,
            "timed_preprocess_median": statistics.median(
                sample["preprocess_seconds"] for sample in timed
            ),
            "timed_input_move_median": statistics.median(
                sample["input_move_seconds"] for sample in timed
            ),
            "timed_model_generate_median": statistics.median(
                sample["model_generate_seconds"] for sample in timed
            ),
            "timed_request_median": statistics.median(
                sample["end_to_end_seconds"] for sample in timed
            ),
            "timed_request_p95": percentile(
                [sample["end_to_end_seconds"] for sample in timed], 0.95
            ),
            "timed_end_to_end_tokens_per_second_median": statistics.median(
                sample["end_to_end_tokens_per_second"] for sample in timed
            ),
        },
        "benchmark": {
            "warmup_runs": args.warmup_runs,
            "timed_runs": args.timed_runs,
            "timed_token_sequences_deterministic": True,
            "generated_token_ids": generated["generated_token_ids"],
            "generated_text": generated["generated_text"],
            "generated_token_count": generated["generated_token_count"],
            "finish_reason": generated["finish_reason"],
            "mps_memory_snapshots": snapshots,
            "timing_boundary": (
                "resident model.generate request, including image preprocessing, visual "
                "encoding, prefill, greedy decode, and MPS synchronization"
            ),
        },
    }


def validate_worker_report(worker: dict[str, Any], args: argparse.Namespace) -> dict[str, bool]:
    benchmark = worker.get("benchmark")
    request = worker.get("request")
    model = worker.get("model")
    runtime = worker.get("runtime")
    token_ids = benchmark.get("generated_token_ids") if isinstance(benchmark, dict) else None
    token_count = benchmark.get("generated_token_count") if isinstance(benchmark, dict) else None
    checks = {
        "schema": worker.get("schema") == WORKER_SCHEMA,
        "model_sha256": isinstance(model, dict) and model.get("sha256") == MODEL_SHA256,
        "device": isinstance(runtime, dict) and runtime.get("device") == args.device,
        "request_max_tokens": isinstance(request, dict) and request.get("max_tokens") == args.max_tokens,
        "request_max_merged_tokens": isinstance(request, dict)
        and request.get("max_merged_tokens") == args.max_merged_tokens,
        "timed_tokens_deterministic": isinstance(benchmark, dict)
        and benchmark.get("timed_token_sequences_deterministic") is True,
        "generated_token_ids": isinstance(token_ids, list)
        and all(isinstance(token, int) and not isinstance(token, bool) for token in token_ids),
        "generated_token_count": isinstance(token_count, int)
        and not isinstance(token_count, bool)
        and 1 <= token_count <= args.max_tokens
        and isinstance(token_ids, list)
        and len(token_ids) == token_count,
    }
    return checks


def parse_args(argv: list[str]) -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weights-dir", type=Path, required=True)
    parser.add_argument("--processor-dir", type=Path, required=True)
    parser.add_argument("--image", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--prompt", default="Read all visible text.")
    parser.add_argument("--device", choices=DEVICES, required=True)
    parser.add_argument("--dtype", choices=DTYPES, default="bfloat16")
    parser.add_argument("--attn-implementation", choices=ATTENTION_IMPLEMENTATIONS, default="sdpa")
    parser.add_argument("--load-strategy", choices=LOAD_STRATEGIES, default="device_map")
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--timed-runs", type=int, default=3)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--max-merged-tokens", type=int, default=576)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.25)
    parser.add_argument("--max-rss-mib", type=float, default=8192.0)
    parser.add_argument("--min-free-percent", type=int, default=15)
    parser.add_argument("--max-swap-growth-mib", type=float, default=0.0)
    parser.add_argument("--mps-high-watermark-ratio", type=float, default=0.80)
    parser.add_argument("--mps-low-watermark-ratio", type=float, default=0.70)
    parser.add_argument("--mps-prefer-metal", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker-output", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--requirements-file",
        type=Path,
        default=here / "requirements-qwen3vl-oracle.txt",
    )
    return parser.parse_args(argv)


def run_parent(args: argparse.Namespace) -> dict[str, Any]:
    if args.output is None:
        raise QualificationError("--output is required outside worker mode")
    if args.output.exists():
        raise QualificationError(f"refusing to overwrite output: {args.output}")
    if not args.requirements_file.is_file():
        raise QualificationError(f"missing requirements file: {args.requirements_file}")
    work_dir = (args.work_dir or args.output.with_suffix(".artifacts")).resolve()
    if work_dir.exists():
        raise QualificationError(f"refusing to reuse work directory: {work_dir}")
    work_dir.mkdir(parents=True)
    worker_output = work_dir / "worker.json"
    execution = run_resource_monitored(
        worker_command(args, worker_output),
        work_dir / "worker.stdout.log",
        work_dir / "worker.stderr.log",
        timeout_seconds=args.timeout_seconds,
        max_rss_mib=args.max_rss_mib,
        min_free_percent=args.min_free_percent,
        max_swap_growth_mib=args.max_swap_growth_mib,
        sample_interval_seconds=args.sample_interval_seconds,
        env=benchmark_environment(args),
        label=f"Transformers {args.device} OCR benchmark",
    )
    if execution["returncode"] != 0:
        raise QualificationError(
            f"Transformers OCR worker exited {execution['returncode']}: "
            f"{execution['stderr'].strip()[-2000:]}"
        )
    worker = load_json(worker_output)
    gates = validate_worker_report(worker, args)
    if not all(gates.values()):
        raise QualificationError(f"one or more OCR benchmark gates failed: {gates}")
    return {
        "schema": SCHEMA,
        "pass": True,
        "release_ready": False,
        "created_unix_seconds": int(time.time()),
        "host": {"platform": platform.platform(), "python": platform.python_version()},
        "configuration": {
            "device": args.device,
            "dtype": args.dtype,
            "attention_implementation": args.attn_implementation,
            "load_strategy": args.load_strategy,
            "warmup_runs": args.warmup_runs,
            "timed_runs": args.timed_runs,
            "max_tokens": args.max_tokens,
            "max_merged_tokens": args.max_merged_tokens,
            "mps_high_watermark_ratio": args.mps_high_watermark_ratio
            if args.device == "mps"
            else None,
            "mps_low_watermark_ratio": args.mps_low_watermark_ratio
            if args.device == "mps"
            else None,
            "mps_prefer_metal": args.mps_prefer_metal if args.device == "mps" else None,
        },
        "harness": {
            "script": {"path": str(Path(__file__).resolve()), "sha256": sha256_file(Path(__file__))},
            "requirements": {
                "path": str(args.requirements_file.resolve(strict=True)),
                "sha256": sha256_file(args.requirements_file),
            },
            "work_dir": str(work_dir),
        },
        "execution": execution,
        "gates": gates,
        "worker": worker,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        validate_args(args)
        if args.worker:
            if args.worker_output is None:
                raise QualificationError("--worker-output is required in worker mode")
            if args.output is not None or args.work_dir is not None:
                raise QualificationError("worker mode cannot accept --output or --work-dir")
            if args.worker_output.exists():
                raise QualificationError(f"refusing to overwrite worker output: {args.worker_output}")
            worker = run_worker(args)
            write_json_atomic(args.worker_output.resolve(), worker)
            return 0

        report: dict[str, Any] = {
            "schema": SCHEMA,
            "pass": False,
            "release_ready": False,
            "created_unix_seconds": int(time.time()),
            "host": {"platform": platform.platform(), "python": platform.python_version()},
        }
        try:
            report = run_parent(args)
        except ResourceViolation as exc:
            report["execution"] = exc.execution
            report["failure"] = str(exc)
        except (QualificationError, OSError, RuntimeError, ValueError) as exc:
            report["failure"] = str(exc)
        if args.output is None:
            raise QualificationError("--output is required outside worker mode")
        write_json_atomic(args.output.resolve(), report)
        print(json.dumps({"pass": report["pass"], "report": str(args.output.resolve())}))
        return 0 if report["pass"] else 2
    except (QualificationError, OSError, RuntimeError, ValueError) as exc:
        print(f"Qwen3-VL Transformers OCR benchmark failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
