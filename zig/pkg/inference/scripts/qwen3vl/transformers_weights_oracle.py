#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Run one bounded, pinned BF16 Qwen3-VL Transformers prefill oracle."""

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

from transformers_oracle import (
    OracleError,
    REQUIRED_SIDECARS,
    make_messages,
    sha256_file,
    tensor_f32le_bytes,
    verify_environment,
)


MODEL_REVISION = "89644892e4d85e24eaac8bacfd4f463576704203"
MODEL_SIZE = 4_255_140_312
MODEL_SHA256 = "7de1838c87a5349b016c26a1c3f7d2bc400a3d485f95ef39a7059ffd734977a0"
DTYPES = ("bfloat16", "float16")
ATTENTION_IMPLEMENTATIONS = ("eager", "sdpa")
LOAD_STRATEGIES = ("device_map", "cpu_then_move")
LOGIT_TRANSFER_STRATEGIES = ("view", "clone")
PROFILE_STAGE_NAMES = ("vision", "decoder", "lm_head")


def synchronize(device: str) -> None:
    import torch

    if device == "mps":
        torch.mps.synchronize()


def percentile(samples: list[float], percentile_value: float) -> float:
    if not samples:
        raise OracleError("cannot calculate a percentile without samples")
    ordered = sorted(samples)
    rank = max(1, int((percentile_value * len(ordered)) + 0.999999999))
    return ordered[min(rank, len(ordered)) - 1]


def summarize_stage_samples(
    samples: list[dict[str, float]],
) -> dict[str, Any]:
    """Summarize synchronized module timings without hiding per-run variance."""
    stages: dict[str, Any] = {}
    for name in (*PROFILE_STAGE_NAMES, "unattributed"):
        values = [sample[name] for sample in samples]
        stages[name] = {
            "samples": values,
            "median": statistics.median(values),
            "p95": percentile(values, 0.95),
        }
    return {
        "synchronization": "device synchronization at selected module boundaries",
        "samples": samples,
        "stages": stages,
    }


class SynchronizedStageProfiler:
    """Measure coarse model stages while accounting for asynchronous MPS work."""

    def __init__(self, device: str, clock: Any = time.monotonic) -> None:
        self.device = device
        self.clock = clock
        self.current: dict[str, float] | None = None
        self.starts: dict[str, float] = {}
        self.handles: list[Any] = []

    def attach(self, model: Any) -> None:
        modules = {
            "vision": model.model.visual,
            "decoder": model.model.language_model,
            "lm_head": model.lm_head,
        }
        for name, module in modules.items():
            self.handles.append(
                module.register_forward_pre_hook(self._make_pre_hook(name))
            )
            self.handles.append(
                module.register_forward_hook(self._make_post_hook(name))
            )

    def _make_pre_hook(self, name: str) -> Any:
        def pre_hook(_module: Any, _inputs: Any) -> None:
            if self.current is None:
                return
            synchronize(self.device)
            self.starts[name] = self.clock()

        return pre_hook

    def _make_post_hook(self, name: str) -> Any:
        def post_hook(_module: Any, _inputs: Any, _output: Any) -> None:
            if self.current is None:
                return
            started = self.starts.pop(name, None)
            if started is None:
                raise OracleError(f"missing stage start for {name}")
            synchronize(self.device)
            self.current[name] = self.clock() - started

        return post_hook

    def begin(self) -> None:
        if self.current is not None:
            raise OracleError("stage profiler run already active")
        self.current = {}
        self.starts.clear()

    def finish(self, model_forward_seconds: float) -> dict[str, float]:
        if self.current is None:
            raise OracleError("stage profiler run is not active")
        missing = [name for name in PROFILE_STAGE_NAMES if name not in self.current]
        if missing:
            raise OracleError(f"missing stage timings: {', '.join(missing)}")
        sample = dict(self.current)
        accounted = sum(sample[name] for name in PROFILE_STAGE_NAMES)
        sample["unattributed"] = max(0.0, model_forward_seconds - accounted)
        self.current = None
        self.starts.clear()
        return sample

    def cancel(self) -> None:
        self.current = None
        self.starts.clear()

    def close(self) -> None:
        self.cancel()
        for handle in self.handles:
            handle.remove()
        self.handles.clear()


def mps_memory_snapshot(device: str, torch: Any) -> dict[str, int] | None:
    if device != "mps":
        return None
    snapshot: dict[str, int] = {}
    for function_name, field_name in (
        ("current_allocated_memory", "current_allocated_bytes"),
        ("driver_allocated_memory", "driver_allocated_bytes"),
        ("recommended_max_memory", "recommended_max_bytes"),
    ):
        function = getattr(torch.mps, function_name, None)
        if callable(function):
            snapshot[field_name] = int(function())
    return snapshot


def torch_dtype(name: str, torch: Any) -> Any:
    if name == "bfloat16":
        return torch.bfloat16
    if name == "float16":
        return torch.float16
    raise OracleError(f"unsupported dtype: {name}")


def make_inputs(args: argparse.Namespace, processor: Any) -> tuple[str, dict[str, Any]]:
    from PIL import Image

    messages = make_messages(args.prompt, True)
    rendered = processor.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
    )
    patch_size = int(processor.image_processor.patch_size)
    merge_size = int(processor.image_processor.merge_size)
    factor_area = (patch_size * merge_size) ** 2
    official_min_pixels = int(processor.image_processor.size["shortest_edge"])
    with Image.open(args.image) as opened:
        rgb = opened.convert("RGB")
        inputs = processor(
            text=[rendered],
            images=[rgb],
            images_kwargs={
                "size": {
                    "longest_edge": args.max_merged_tokens * factor_area,
                    "shortest_edge": official_min_pixels,
                }
            },
            return_tensors="pt",
        )
    return rendered, inputs


def run(args: argparse.Namespace) -> dict[str, Any]:
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    versions = verify_environment()

    import numpy as np
    import torch
    from transformers import AutoProcessor, Qwen3VLForConditionalGeneration
    from transformers.utils import logging as transformers_logging

    transformers_logging.disable_progress_bar()

    if args.device == "mps" and not torch.backends.mps.is_available():
        raise OracleError("MPS is not available")
    if args.device == "cpu" and args.dtype != "bfloat16":
        raise OracleError("the canonical CPU oracle requires bfloat16")
    if args.warmup_runs < 0:
        raise OracleError("warmup runs must be non-negative")
    if args.timed_runs < 1:
        raise OracleError("timed runs must be positive")
    if args.max_merged_tokens < 1:
        raise OracleError("max merged tokens must be positive")
    if args.logits_to_keep < 0:
        raise OracleError("logits to keep must be non-negative")
    processor_dir = args.processor_dir.resolve(strict=True)
    weights_dir = args.weights_dir.resolve(strict=True)
    model_path = weights_dir / "model.safetensors"
    if not model_path.is_file():
        raise OracleError(f"missing BF16 weights: {model_path}")
    if model_path.stat().st_size != MODEL_SIZE:
        raise OracleError(
            f"BF16 weight size mismatch: expected {MODEL_SIZE}, got {model_path.stat().st_size}"
        )
    model_sha = sha256_file(model_path)
    if model_sha != MODEL_SHA256:
        raise OracleError(f"BF16 weight SHA-256 mismatch: {model_sha}")
    for name in REQUIRED_SIDECARS:
        processor_sidecar = processor_dir / name
        weight_sidecar = weights_dir / name
        if not weight_sidecar.is_file() or sha256_file(weight_sidecar) != sha256_file(
            processor_sidecar
        ):
            raise OracleError(f"weights oracle sidecar mismatch: {name}")

    processor = AutoProcessor.from_pretrained(
        processor_dir,
        local_files_only=True,
        trust_remote_code=False,
        use_fast=True,
    )
    rendered, inputs = make_inputs(args, processor)

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
        model = Qwen3VLForConditionalGeneration.from_pretrained(
            weights_dir,
            **load_options,
        )
        model.to(args.device)
    model.eval()
    synchronize(args.device)
    loaded_at = time.monotonic()
    memory_snapshots: list[dict[str, Any]] = []

    def capture_memory(stage: str, run_index: int | None = None) -> None:
        snapshot = mps_memory_snapshot(args.device, torch)
        if snapshot is not None:
            memory_snapshots.append({"stage": stage, "run": run_index, **snapshot})

    capture_memory("model_loaded")

    input_move_started = time.monotonic()
    model_inputs = {
        name: value.to(args.device)
        for name, value in inputs.items()
        if hasattr(value, "to")
    }
    synchronize(args.device)
    inputs_moved_at = time.monotonic()
    capture_memory("inputs_moved")

    device_logit_diagnostics: list[dict[str, float]] = []
    stage_profiler = (
        SynchronizedStageProfiler(args.device) if args.profile_stages else None
    )
    if stage_profiler is not None:
        stage_profiler.attach(model)

    def forward_once() -> tuple[Any, float, float, dict[str, float] | None]:
        if stage_profiler is not None:
            stage_profiler.begin()
        model_started = time.monotonic()
        try:
            output = model(
                **model_inputs,
                use_cache=False,
                return_dict=True,
                logits_to_keep=args.logits_to_keep,
            )
            synchronize(args.device)
        except BaseException:
            if stage_profiler is not None:
                stage_profiler.cancel()
            raise
        model_forward_seconds = time.monotonic() - model_started
        stage_sample = (
            stage_profiler.finish(model_forward_seconds)
            if stage_profiler is not None
            else None
        )
        postprocess_started = time.monotonic()
        device_logits = output.logits[0, -1]
        device_logit_diagnostics.append(
            {
                "min": float(device_logits.min().item()),
                "max": float(device_logits.max().item()),
                "max_abs": float(device_logits.abs().max().item()),
            }
        )
        if args.logit_transfer == "clone":
            device_logits = device_logits.clone()
        result = device_logits.to(device="cpu", dtype=torch.float32).contiguous()
        del output
        synchronize(args.device)
        return (
            result,
            model_forward_seconds,
            time.monotonic() - postprocess_started,
            stage_sample,
        )

    warmup_samples: list[float] = []
    timed_samples: list[float] = []
    warmup_model_forward_samples: list[float] = []
    timed_model_forward_samples: list[float] = []
    warmup_logit_postprocess_samples: list[float] = []
    timed_logit_postprocess_samples: list[float] = []
    timed_stage_samples: list[dict[str, float]] = []
    timed_logit_sha256: list[str] = []
    logits = None
    with torch.inference_mode():
        for run_index in range(args.warmup_runs):
            started = time.monotonic()
            (
                logits,
                model_forward_seconds,
                logit_postprocess_seconds,
                _stage_sample,
            ) = forward_once()
            warmup_samples.append(time.monotonic() - started)
            warmup_model_forward_samples.append(model_forward_seconds)
            warmup_logit_postprocess_samples.append(logit_postprocess_seconds)
            capture_memory("warmup_complete", run_index + 1)
        for run_index in range(args.timed_runs):
            started = time.monotonic()
            (
                logits,
                model_forward_seconds,
                logit_postprocess_seconds,
                stage_sample,
            ) = forward_once()
            timed_samples.append(time.monotonic() - started)
            timed_model_forward_samples.append(model_forward_seconds)
            timed_logit_postprocess_samples.append(logit_postprocess_seconds)
            if stage_sample is not None:
                timed_stage_samples.append(stage_sample)
            timed_logit_sha256.append(
                hashlib.sha256(tensor_f32le_bytes(logits)).hexdigest()
            )
            capture_memory("timed_forward_complete", run_index + 1)
    finished_at = time.monotonic()
    if stage_profiler is not None:
        stage_profiler.close()
    if logits is None:
        raise OracleError("no timed forward result")
    if logits.ndim != 1 or logits.numel() != int(model.config.text_config.vocab_size):
        raise OracleError(f"unexpected last-logit shape: {tuple(logits.shape)}")
    if not bool(torch.isfinite(logits).all().item()):
        raise OracleError("Transformers produced non-finite logits")
    max_abs_logit = float(logits.abs().max().item())
    if max_abs_logit > 10_000.0:
        device_max_abs = device_logit_diagnostics[-1]["max_abs"]
        raise OracleError(
            "Transformers produced implausible logits "
            f"(host_max_abs={max_abs_logit}, device_max_abs={device_max_abs}); "
            "rejecting a numerically unstable reference"
        )

    raw_logits = tensor_f32le_bytes(logits)
    args.logits_output.write_bytes(raw_logits)
    top_values, top_indices = torch.topk(logits, k=min(args.top_k, logits.numel()))
    token_id = int(top_indices[0].item())
    canonical = np.frombuffer(raw_logits, dtype="<f4")
    return {
        "schema": "antfly.qwen3vl.transformers_weights_oracle.v1",
        "model": {
            "repository": "Qwen/Qwen3-VL-2B-Instruct",
            "revision": MODEL_REVISION,
            "path": str(model_path),
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
            "logit_transfer": args.logit_transfer,
            "logits_to_keep": args.logits_to_keep,
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
            "image": str(args.image.resolve(strict=True)),
            "image_sha256": sha256_file(args.image),
            "rendered_prompt": rendered,
            "input_ids": [
                int(item) for item in inputs["input_ids"].reshape(-1).tolist()
            ],
            "image_grid_thw": [
                [int(item) for item in row] for row in inputs["image_grid_thw"].tolist()
            ],
        },
        "last_logits": {
            "path": str(args.logits_output.resolve()),
            "value_count": int(logits.numel()),
            "f32le_sha256": hashlib.sha256(raw_logits).hexdigest(),
            "min": float(canonical.min()),
            "max": float(canonical.max()),
            "mean": float(canonical.mean()),
            "argmax_token_id": token_id,
            "argmax_text": processor.decode([token_id], skip_special_tokens=False),
            "top_k": [
                {"token_id": int(index), "logit": float(value)}
                for index, value in zip(
                    top_indices.tolist(), top_values.tolist(), strict=True
                )
            ],
        },
        "timing_seconds": {
            "load_and_move": loaded_at - load_started,
            "forward": finished_at - loaded_at,
            "total": finished_at - load_started,
            "input_move": inputs_moved_at - input_move_started,
            "warmup_samples": warmup_samples,
            "timed_forward_samples": timed_samples,
            "timed_forward_median": statistics.median(timed_samples),
            "timed_forward_p95": percentile(timed_samples, 0.95),
            "warmup_model_forward_samples": warmup_model_forward_samples,
            "timed_model_forward_samples": timed_model_forward_samples,
            "timed_model_forward_median": statistics.median(
                timed_model_forward_samples
            ),
            "timed_model_forward_p95": percentile(
                timed_model_forward_samples, 0.95
            ),
            "warmup_logit_postprocess_samples": warmup_logit_postprocess_samples,
            "timed_logit_postprocess_samples": timed_logit_postprocess_samples,
        },
        "benchmark": {
            "warmup_runs": args.warmup_runs,
            "timed_runs": args.timed_runs,
            "timed_logit_sha256": timed_logit_sha256,
            "timed_logits_bitwise_deterministic": len(set(timed_logit_sha256)) == 1,
            "mps_memory_snapshots": memory_snapshots,
            "device_logit_diagnostics": device_logit_diagnostics,
            "stage_profile": summarize_stage_samples(timed_stage_samples)
            if timed_stage_samples
            else None,
        },
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weights-dir", type=Path, required=True)
    parser.add_argument("--processor-dir", type=Path, required=True)
    parser.add_argument("--image", type=Path, required=True)
    parser.add_argument("--prompt", default="Describe the image briefly.")
    parser.add_argument("--device", choices=("cpu", "mps"), default="cpu")
    parser.add_argument("--dtype", choices=DTYPES, default="bfloat16")
    parser.add_argument(
        "--attn-implementation",
        choices=ATTENTION_IMPLEMENTATIONS,
        default="eager",
    )
    parser.add_argument(
        "--load-strategy", choices=LOAD_STRATEGIES, default="device_map"
    )
    parser.add_argument(
        "--logit-transfer",
        choices=LOGIT_TRANSFER_STRATEGIES,
        default="view",
    )
    parser.add_argument("--warmup-runs", type=int, default=0)
    parser.add_argument("--timed-runs", type=int, default=1)
    parser.add_argument("--max-merged-tokens", type=int, default=576)
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument(
        "--logits-to-keep",
        type=int,
        default=0,
        help="only compute logits for the final N positions; zero preserves all positions",
    )
    parser.add_argument(
        "--profile-stages",
        action="store_true",
        help="synchronize and time vision, decoder, and LM-head module boundaries",
    )
    parser.add_argument("--logits-output", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        payload = run(args)
        args.output.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return 0
    except (OracleError, OSError, RuntimeError, ValueError) as exc:
        print(f"Qwen3-VL Transformers weights oracle failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
