#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Run a bounded, offline Qwen3-VL benchmark with MLX-VLM.

MLX-VLM is an independent Apple-Silicon reference for this benchmark suite;
it is never used by Antfly serving.  This runner deliberately records a
complete local model-tree fingerprint, the formatted request, per-run timing,
and deterministic token evidence.  Numeric logits are not claimed because
MLX-VLM's public generation API exposes sampled-token statistics rather than a
stable public last-logit transfer contract.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path, PurePosixPath
import platform
import statistics
import sys
import time
from typing import Any


SCHEMA = "antfly.qwen3vl.mlx_vlm_benchmark.v1"
PROFILES = ("bf16", "q4")
# Antfly and the pinned Transformers MPS lane cap a generation image to 576
# merged visual tokens. Qwen3-VL's merge factor is 16 * 2 = 32 pixels, so this
# is the exact processor max-pixels value used for cross-framework timing.
QWEN3VL_MERGE_FACTOR = 32
MAX_MERGED_VISUAL_TOKENS = 576
COMPARISON_MAX_PIXELS = MAX_MERGED_VISUAL_TOKENS * QWEN3VL_MERGE_FACTOR**2


class MlxVlmBenchmarkError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_relative_path(path: Path) -> str:
    relative = PurePosixPath(path.as_posix())
    if relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts):
        raise MlxVlmBenchmarkError(f"unsafe model artifact path: {path}")
    return str(relative)


def fingerprint_model_dir(model_dir: Path) -> dict[str, Any]:
    """Hash every model file once before load; symlinked artifacts are rejected."""
    root = model_dir.resolve(strict=True)
    if not root.is_dir():
        raise MlxVlmBenchmarkError(f"MLX model is not a directory: {root}")
    artifacts: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        relative = safe_relative_path(path.relative_to(root))
        metadata = path.lstat()
        if not path.is_file() or path.is_symlink():
            raise MlxVlmBenchmarkError(
                f"MLX benchmark artifacts must be regular non-symlink files: {relative}"
            )
        artifacts.append(
            {"path": relative, "size": metadata.st_size, "sha256": sha256_file(path)}
        )
    if not artifacts:
        raise MlxVlmBenchmarkError("MLX model directory contains no artifacts")
    config_path = root / "config.json"
    if not config_path.is_file():
        raise MlxVlmBenchmarkError("MLX model is missing config.json")
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MlxVlmBenchmarkError(f"invalid MLX config.json: {exc}") from exc
    if config.get("model_type") != "qwen3_vl":
        raise MlxVlmBenchmarkError(
            f"MLX model_type must be qwen3_vl, got {config.get('model_type')!r}"
        )
    fingerprint = hashlib.sha256(
        json.dumps(artifacts, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {"path": str(root), "artifacts": artifacts, "tree_sha256": fingerprint}


def precision_contract(profile: str, config: dict[str, Any]) -> dict[str, str | int]:
    """Bind the advertised MLX precision class to its local model config."""
    quantization = config.get("quantization")
    if profile == "bf16":
        if quantization not in (None, {}):
            raise MlxVlmBenchmarkError(
                "BF16 MLX profile must not declare quantization in config.json"
            )
        return {"profile": "bf16", "weight_precision": "bf16", "quantization_bits": 0}
    if not isinstance(quantization, dict) or quantization.get("bits") != 4:
        raise MlxVlmBenchmarkError(
            "Q4 MLX profile requires config.json quantization.bits == 4"
        )
    return {"profile": "q4", "weight_precision": "q4", "quantization_bits": 4}


def validate_args(args: argparse.Namespace) -> None:
    if platform.system() != "Darwin":
        raise MlxVlmBenchmarkError("MLX-VLM benchmark requires macOS")
    if args.output.exists():
        raise MlxVlmBenchmarkError(f"refusing to overwrite output: {args.output}")
    if args.profile not in PROFILES:
        raise MlxVlmBenchmarkError(f"unsupported precision profile: {args.profile}")
    if not args.requirements_file.is_file() or args.requirements_file.is_symlink():
        raise MlxVlmBenchmarkError(
            f"MLX benchmark requirements file must be a regular file: {args.requirements_file}"
        )
    if not 0 <= args.warmup_runs <= 10:
        raise MlxVlmBenchmarkError("warmup runs must be in [0, 10]")
    if not 1 <= args.timed_runs <= 20:
        raise MlxVlmBenchmarkError("timed runs must be in [1, 20]")
    if not 1 <= args.max_tokens <= 8:
        raise MlxVlmBenchmarkError("max tokens must be in [1, 8]")
    if args.max_pixels < QWEN3VL_MERGE_FACTOR**2:
        raise MlxVlmBenchmarkError("max-pixels is below one merged visual token")
    if not args.image.is_file() or args.image.is_symlink():
        raise MlxVlmBenchmarkError(f"image must be a regular file: {args.image}")
    if not args.prompt:
        raise MlxVlmBenchmarkError("prompt must not be empty")


def median(values: list[float]) -> float:
    if not values:
        raise MlxVlmBenchmarkError("cannot summarize an empty benchmark")
    return float(statistics.median(values))


def _token_trace(responses: list[Any], max_tokens: int) -> tuple[list[int], Any]:
    if not responses:
        raise MlxVlmBenchmarkError("MLX-VLM generated no response records")
    final = responses[-1]
    # stream_generate emits one intermediate record per token then a final
    # summary record.  The final token duplicates the last intermediate token.
    tokens = [
        int(response.token)
        for response in responses
        if getattr(response, "token", None) is not None
        and getattr(response, "finish_reason", None) is None
    ]
    if not tokens and getattr(final, "token", None) is not None:
        tokens = [int(final.token)]
    if len(tokens) != max_tokens:
        raise MlxVlmBenchmarkError(
            f"MLX-VLM generated {len(tokens)} tokens; expected exactly {max_tokens}"
        )
    if int(getattr(final, "generation_tokens", 0)) != max_tokens:
        raise MlxVlmBenchmarkError("MLX-VLM final generation token count is inconsistent")
    return tokens, final


def run_once(
    stream_generate: Any,
    model: Any,
    processor: Any,
    formatted_prompt: str,
    image: Path,
    max_tokens: int,
    max_pixels: int,
    mx: Any,
) -> dict[str, Any]:
    started = time.monotonic()
    responses = list(
        stream_generate(
            model,
            processor,
            formatted_prompt,
            image=[str(image)],
            max_tokens=max_tokens,
            temperature=0.0,
            top_p=1.0,
            top_k=1,
            max_pixels=max_pixels,
            verbose=False,
        )
    )
    mx.synchronize()
    elapsed = time.monotonic() - started
    tokens, final = _token_trace(responses, max_tokens)
    prompt_tokens = int(getattr(final, "prompt_tokens", 0))
    prompt_tps = float(getattr(final, "prompt_tps", 0.0))
    generation_tps = float(getattr(final, "generation_tps", 0.0))
    if prompt_tokens <= 0 or prompt_tps <= 0.0 or generation_tps <= 0.0:
        raise MlxVlmBenchmarkError("MLX-VLM omitted finite prompt/generation timing")
    return {
        "end_to_end_seconds": elapsed,
        "prefill_seconds": prompt_tokens / prompt_tps,
        "decode_seconds": max_tokens / generation_tps,
        "prompt_tokens": prompt_tokens,
        "generated_token_ids": tokens,
        "finish_reason": getattr(final, "finish_reason", None),
        "peak_memory_gib": float(getattr(final, "peak_memory", 0.0)),
    }


def visual_token_evidence(processor: Any, image: Path, max_pixels: int) -> dict[str, int]:
    """Bind MLX preprocessing to the native 576-merged-token request contract."""
    try:
        from PIL import Image

        with Image.open(image) as source:
            width, height = source.size
        image_processor = getattr(processor, "image_processor", processor)
        merged_tokens = int(
            image_processor.num_image_tokens(height, width, max_pixels=max_pixels)
        )
        resized_height, resized_width = image_processor._resolved_size(
            height, width, max_pixels=max_pixels
        )
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        raise MlxVlmBenchmarkError(
            "MLX-VLM processor cannot attest Qwen3-VL visual-token geometry"
        ) from exc
    if not 0 < merged_tokens <= MAX_MERGED_VISUAL_TOKENS:
        raise MlxVlmBenchmarkError(
            f"MLX-VLM visual token count {merged_tokens} exceeds the "
            f"{MAX_MERGED_VISUAL_TOKENS}-token comparison contract"
        )
    return {
        "source_width": int(width),
        "source_height": int(height),
        "resized_width": int(resized_width),
        "resized_height": int(resized_height),
        "merged_visual_tokens": merged_tokens,
    }


def run(args: argparse.Namespace, model_evidence: dict[str, Any]) -> dict[str, Any]:
    # Keep the benchmark offline even when callers have a Hub token configured.
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    try:
        import mlx.core as mx
        from mlx_vlm import load
        from mlx_vlm.generate import stream_generate
        from mlx_vlm.prompt_utils import apply_chat_template
        from mlx_vlm.utils import load_config
    except ImportError as exc:
        raise MlxVlmBenchmarkError(
            "MLX-VLM is unavailable; install the pinned benchmark environment"
        ) from exc

    try:
        versions = {
            "mlx": importlib.metadata.version("mlx"),
            "mlx-vlm": importlib.metadata.version("mlx-vlm"),
        }
    except importlib.metadata.PackageNotFoundError as exc:
        raise MlxVlmBenchmarkError("MLX-VLM distribution metadata is unavailable") from exc
    if not mx.metal.is_available():
        raise MlxVlmBenchmarkError("MLX Metal backend is unavailable")

    model_path = model_evidence["path"]
    try:
        local_config = json.loads((Path(model_path) / "config.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MlxVlmBenchmarkError(f"invalid local MLX config: {exc}") from exc
    profile_contract = precision_contract(args.profile, local_config)
    load_started = time.monotonic()
    model, processor = load(model_path)
    mx.synchronize()
    load_seconds = time.monotonic() - load_started
    config = load_config(model_path)
    visual = visual_token_evidence(processor, args.image, args.max_pixels)
    formatted_prompt = apply_chat_template(processor, config, args.prompt, num_images=1)
    if not isinstance(formatted_prompt, str) or not formatted_prompt:
        raise MlxVlmBenchmarkError("MLX-VLM produced an empty formatted prompt")

    warmups = [
        run_once(
            stream_generate, model, processor, formatted_prompt, args.image,
            args.max_tokens, args.max_pixels, mx,
        )
        for _ in range(args.warmup_runs)
    ]
    timed = [
        run_once(
            stream_generate, model, processor, formatted_prompt, args.image,
            args.max_tokens, args.max_pixels, mx,
        )
        for _ in range(args.timed_runs)
    ]
    token_sequences = [sample["generated_token_ids"] for sample in timed]
    if len({tuple(tokens) for tokens in token_sequences}) != 1:
        raise MlxVlmBenchmarkError("MLX-VLM timed token sequences are not deterministic")
    prompt_counts = {sample["prompt_tokens"] for sample in timed}
    if len(prompt_counts) != 1:
        raise MlxVlmBenchmarkError("MLX-VLM prompt token counts changed across runs")
    return {
        "schema": SCHEMA,
        "pass": True,
        "release_ready": False,
        "backend": "mlx_vlm",
        "profile": args.profile,
        "precision_contract": profile_contract,
        "model": model_evidence,
        "runtime": {
            "packages": versions,
            "device": str(mx.default_device()),
            "metal_available": bool(mx.metal.is_available()),
            "load_seconds": load_seconds,
        },
        "harness": {
            "script": {
                "path": str(Path(__file__).resolve()),
                "sha256": sha256_file(Path(__file__)),
            },
            "requirements": {
                "path": str(args.requirements_file.resolve(strict=True)),
                "sha256": sha256_file(args.requirements_file),
            },
        },
        "request": {
            "prompt": args.prompt,
            "formatted_prompt_sha256": hashlib.sha256(formatted_prompt.encode()).hexdigest(),
            "image": str(args.image.resolve(strict=True)),
            "image_sha256": sha256_file(args.image),
            "max_tokens": args.max_tokens,
            "max_pixels": args.max_pixels,
            "max_merged_tokens": MAX_MERGED_VISUAL_TOKENS,
            "visual_token_count": visual["merged_visual_tokens"],
            "image_geometry": visual,
        },
        "benchmark": {
            "warmup_runs": args.warmup_runs,
            "timed_runs": args.timed_runs,
            "timed_token_sequences_deterministic": True,
            "warmups": warmups,
            "timed": timed,
            "median": {
                "end_to_end_seconds": median([sample["end_to_end_seconds"] for sample in timed]),
                "prefill_seconds": median([sample["prefill_seconds"] for sample in timed]),
                "decode_seconds": median([sample["decode_seconds"] for sample in timed]),
                "peak_memory_gib": max(sample["peak_memory_gib"] for sample in timed),
            },
        },
        "parity_scope": {
            "exact": ["image_sha256", "formatted_prompt_sha256", "token_count", "timed_token_sequence"],
            "not_claimed": ["last_logits", "intermediate_activations"],
        },
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--image", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=PROFILES, required=True)
    parser.add_argument("--prompt", default="Describe the image briefly.")
    parser.add_argument("--max-tokens", type=int, default=1)
    parser.add_argument(
        "--max-pixels",
        type=int,
        default=COMPARISON_MAX_PIXELS,
        help=(
            "Qwen3-VL image preprocessing cap; default matches Antfly and "
            "Transformers' 576 merged visual-token lane"
        ),
    )
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--timed-runs", type=int, default=5)
    parser.add_argument(
        "--requirements-file",
        type=Path,
        default=here / "requirements-qwen3vl-mlx-vlm.txt",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    # A failed invocation must not replace an earlier attestation.  Check this
    # before constructing or serializing a failure report.
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
        model_evidence = fingerprint_model_dir(args.model_dir)
        report = {**report, **run(args, model_evidence)}
    except (ImportError, MlxVlmBenchmarkError, OSError, RuntimeError, ValueError) as exc:
        report["failure"] = str(exc)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"pass": report["pass"], "report": str(args.output.resolve())}))
    return 0 if report["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
