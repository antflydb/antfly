#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0

"""Emit deterministic, weights-free Qwen3-VL Transformers parity evidence.

The oracle loads only processor/tokenizer sidecars from a managed Antfly model
directory. It never downloads a model or executes the language model. Image
evidence covers the exact float patch rows consumed by the reference vision
patch embedding, including smart resize, normalization, temporal duplication,
and merge-major patch ordering.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import sys
from types import SimpleNamespace
from typing import Any


EXPECTED_VERSIONS = {
    "accelerate": "1.14.0",
    "numpy": "2.4.2",
    "pillow": "12.1.0",
    "torch": "2.10.0",
    "torchvision": "0.25.0",
    "transformers": "5.1.0",
}
REQUIRED_SIDECARS = (
    "config.json",
    "chat_template.json",
    "preprocessor_config.json",
    "video_preprocessor_config.json",
    "tokenizer.json",
    "tokenizer_config.json",
)


class OracleError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def installed_versions() -> dict[str, str]:
    return {name: importlib.metadata.version(name) for name in EXPECTED_VERSIONS}


def verify_environment() -> dict[str, str]:
    actual = installed_versions()
    mismatches = {
        name: {"expected": expected, "actual": actual[name]}
        for name, expected in EXPECTED_VERSIONS.items()
        if actual[name] != expected
    }
    if mismatches:
        raise OracleError(f"oracle dependency mismatch: {json.dumps(mismatches, sort_keys=True)}")
    return actual


def tensor_f32le_bytes(tensor: Any) -> bytes:
    import numpy as np
    import torch

    canonical = tensor.detach().to(device="cpu", dtype=torch.float32).contiguous().numpy()
    canonical = np.asarray(canonical, dtype="<f4", order="C")
    return canonical.tobytes(order="C")


def tensor_f32le_sha256(tensor: Any) -> str:
    return hashlib.sha256(tensor_f32le_bytes(tensor)).hexdigest()


def flat_ints(value: Any) -> list[int]:
    return [int(item) for item in value.detach().cpu().reshape(-1).tolist()]


def mrope_evidence(config: Any, inputs: dict[str, Any]) -> dict[str, Any]:
    """Run the pinned Transformers M-RoPE planner without allocating weights."""

    from transformers.models.qwen3_vl.modeling_qwen3_vl import Qwen3VLModel

    position_ids, deltas = Qwen3VLModel.get_rope_index(
        SimpleNamespace(config=config),
        input_ids=inputs["input_ids"],
        image_grid_thw=inputs.get("image_grid_thw"),
        video_grid_thw=inputs.get("video_grid_thw"),
        attention_mask=inputs.get("attention_mask"),
    )
    if position_ids.ndim != 3 or position_ids.shape[0] != 3:
        raise OracleError(f"unexpected M-RoPE shape: {tuple(position_ids.shape)}")
    if deltas.numel() != position_ids.shape[1]:
        raise OracleError(
            f"unexpected M-RoPE delta shape: {tuple(deltas.shape)} for "
            f"batch {position_ids.shape[1]}"
        )
    return {
        "position_ids_shape": [int(dim) for dim in position_ids.shape],
        "position_ids": flat_ints(position_ids),
        "position_deltas": flat_ints(deltas),
    }


def make_messages(prompt: str, with_image: bool) -> list[dict[str, Any]]:
    content: list[dict[str, str]] = []
    if with_image:
        content.append({"type": "image"})
    content.append({"type": "text", "text": prompt})
    return [{"role": "user", "content": content}]


def image_evidence(
    pixel_values: Any,
    grid_thw: Any,
    processor: Any,
    patch_output: Path | None,
) -> dict[str, Any]:
    import torch

    if pixel_values.ndim != 2 or grid_thw.ndim != 2 or grid_thw.shape[1] != 3:
        raise OracleError(
            f"unexpected Qwen image tensor shapes: pixels={tuple(pixel_values.shape)} "
            f"grid={tuple(grid_thw.shape)}"
        )
    patch_size = int(processor.image_processor.patch_size)
    temporal_patch_size = int(processor.image_processor.temporal_patch_size)
    channels = 3
    patch_columns = channels * temporal_patch_size * patch_size * patch_size
    if pixel_values.shape[1] != patch_columns:
        raise OracleError(
            f"unexpected patch width {pixel_values.shape[1]} != {patch_columns}"
        )
    # Qwen flattens each row as [channel, temporal, patch_y, patch_x]. For a
    # still image the temporal frames are exact duplicates. Antfly applies the
    # two Conv3D temporal weight slices to one canonical spatial row, so this
    # digest is the direct cross-runtime input contract.
    shaped = pixel_values.detach().to(device="cpu", dtype=torch.float32).reshape(
        pixel_values.shape[0], channels, temporal_patch_size, patch_size, patch_size
    )
    spatial = shaped[:, :, 0, :, :].contiguous()
    temporal_delta = float((shaped - shaped[:, :, :1, :, :]).abs().max().item())
    if temporal_delta != 0.0:
        raise OracleError(f"static-image temporal patches differ: max_abs={temporal_delta}")
    if patch_output is not None:
        with patch_output.open("wb") as stream:
            stream.write(tensor_f32le_bytes(spatial))
    grid = [[int(item) for item in row] for row in grid_thw.detach().cpu().tolist()]
    expected_rows = sum(t * h * w for t, h, w in grid)
    if expected_rows != pixel_values.shape[0]:
        raise OracleError(f"grid rows {expected_rows} != pixel rows {pixel_values.shape[0]}")
    return {
        "grid_thw": grid,
        "resized_sizes": [[w * patch_size, h * patch_size] for _, h, w in grid],
        "pixel_values_shape": [int(dim) for dim in pixel_values.shape],
        "pixel_values_f32le_sha256": tensor_f32le_sha256(pixel_values),
        "spatial_patch_shape": [int(dim) for dim in spatial.shape],
        "spatial_patch_f32le_sha256": tensor_f32le_sha256(spatial),
        "spatial_patch_f32le_path": str(patch_output) if patch_output is not None else None,
        "temporal_duplicate_max_abs": temporal_delta,
        "pixel_values_stats": {
            "min": float(pixel_values.min().item()),
            "max": float(pixel_values.max().item()),
            "mean": float(pixel_values.mean().item()),
        },
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    versions = verify_environment()

    from PIL import Image
    from transformers import AutoConfig, AutoProcessor

    model_dir = args.model_dir.resolve(strict=True)
    missing = [name for name in REQUIRED_SIDECARS if not (model_dir / name).is_file()]
    if missing:
        raise OracleError(f"managed bundle lacks processor sidecars: {missing}")

    processor = AutoProcessor.from_pretrained(
        model_dir,
        local_files_only=True,
        trust_remote_code=False,
        use_fast=True,
    )
    if type(processor).__name__ != "Qwen3VLProcessor":
        raise OracleError(f"unexpected processor: {type(processor).__name__}")
    config = AutoConfig.from_pretrained(
        model_dir,
        local_files_only=True,
        trust_remote_code=False,
    )
    if type(config).__name__ != "Qwen3VLConfig":
        raise OracleError(f"unexpected config: {type(config).__name__}")

    image_path = args.image.resolve(strict=True) if args.image else None
    messages = make_messages(args.prompt, image_path is not None)
    rendered = processor.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
    )
    placeholder_ids = processor.tokenizer(
        rendered,
        add_special_tokens=False,
        return_attention_mask=False,
    )["input_ids"]

    if image_path is None:
        inputs = processor(text=[rendered], return_tensors="pt")
    else:
        patch_size = int(processor.image_processor.patch_size)
        merge_size = int(processor.image_processor.merge_size)
        factor_area = (patch_size * merge_size) ** 2
        max_pixels = args.max_merged_tokens * factor_area
        official_min_pixels = int(processor.image_processor.size["shortest_edge"])
        if max_pixels < official_min_pixels:
            raise OracleError(
                f"max merged tokens {args.max_merged_tokens} cannot satisfy official "
                f"minimum {official_min_pixels // factor_area}"
            )
        with Image.open(image_path) as opened:
            rgb = opened.convert("RGB")
            source_size = [int(rgb.width), int(rgb.height)]
            inputs = processor(
                text=[rendered],
                images=[rgb],
                images_kwargs={
                    "size": {
                        "longest_edge": max_pixels,
                        "shortest_edge": official_min_pixels,
                    }
                },
                return_tensors="pt",
            )

    input_ids = flat_ints(inputs["input_ids"])
    image_token_id = int(config.image_token_id)
    visual_token_count = sum(token_id == image_token_id for token_id in input_ids)
    deepstack_indexes = [int(item) for item in config.vision_config.deepstack_visual_indexes]
    payload: dict[str, Any] = {
        "schema": "antfly.qwen3vl.transformers_oracle.v1",
        "runtime": {
            "python": platform.python_version(),
            "packages": versions,
            "processor": type(processor).__name__,
            "image_processor": type(processor.image_processor).__name__,
            "video_processor": type(processor.video_processor).__name__,
            "config": type(config).__name__,
        },
        "model_dir": str(model_dir),
        "sidecar_sha256": {name: sha256_file(model_dir / name) for name in REQUIRED_SIDECARS},
        "prompt": args.prompt,
        "rendered_prompt": rendered,
        "placeholder_token_ids": [int(item) for item in placeholder_ids],
        "input_ids": input_ids,
        "attention_mask": flat_ints(inputs["attention_mask"]),
        "decoded_input": processor.decode(input_ids, skip_special_tokens=False),
        "architecture": {
            "image_token_id": image_token_id,
            "vision_start_token_id": int(config.vision_start_token_id),
            "text_hidden_size": int(config.text_config.hidden_size),
            "spatial_merge_size": int(config.vision_config.spatial_merge_size),
            "deepstack_visual_indexes": deepstack_indexes,
            "visual_token_count": visual_token_count,
        },
        "mrope": mrope_evidence(config, inputs),
    }
    if "token_type_ids" in inputs:
        payload["token_type_ids"] = flat_ints(inputs["token_type_ids"])
    if image_path is not None:
        payload["image"] = {
            "path": str(image_path),
            "sha256": sha256_file(image_path),
            "source_size": source_size,
            "max_merged_tokens": args.max_merged_tokens,
            "max_pixels": max_pixels,
            **image_evidence(
                inputs["pixel_values"],
                inputs["image_grid_thw"],
                processor,
                args.patch_output,
            ),
        }
    return payload


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--prompt", default="Describe the image briefly.")
    parser.add_argument("--image", type=Path)
    parser.add_argument("--max-merged-tokens", type=int, default=576)
    parser.add_argument("--patch-output", type=Path)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        payload = run(args)
        encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.write_text(encoded)
        else:
            sys.stdout.write(encoded)
        return 0
    except (OracleError, OSError, ValueError) as exc:
        print(f"qwen3vl Transformers oracle failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
