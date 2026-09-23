#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# /// script
# requires-python = ">=3.11"
# dependencies = ["huggingface-hub>=0.34,<2"]
# ///
"""Prepare an upstream Laya checkpoint for Antfly's native extractor.

uv run scripts/laya/prepare_laya.py convaiinnovations/laya --revision <commit> \
    --output ./models/extractors/laya

Weights are copied unchanged; encoder and decision metadata are combined in
config.json. Local source directories work with Python's standard library alone.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import struct
from pathlib import Path


def safetensor_shapes(path: Path) -> dict[str, list[int]]:
    with path.open("rb") as f:
        length_bytes = f.read(8)
        if len(length_bytes) != 8:
            raise ValueError("Missing safetensors header")
        length = struct.unpack("<Q", length_bytes)[0]
        if length > 16 * 1024 * 1024:
            raise ValueError("Safetensors header exceeds 16 MiB")
        header = json.loads(f.read(length))
    return {
        key: value["shape"] for key, value in header.items() if key != "__metadata__"
    }


def prepare(source: Path, output: Path, origin: str, revision: str) -> None:
    if output.exists():
        raise ValueError(f"Output already exists: {output}")
    cfg = json.loads((source / "rl_agent_config.json").read_text())
    encoder = json.loads((source / "encoder/config.json").read_text())
    if encoder.get("model_type") != "modernbert":
        raise ValueError("Only ModernBERT-backed Laya checkpoints are supported")
    for key in ("attention_bias", "mlp_bias", "norm_bias"):
        if encoder.get(key, False):
            raise ValueError(f"Unsupported encoder configuration: {key}")
    dim = encoder["hidden_size"]
    if (
        dim < 64
        or dim % 64
        or encoder["num_attention_heads"] <= 0
        or dim % encoder["num_attention_heads"]
        or encoder["num_hidden_layers"] <= 0
    ):
        raise ValueError("Invalid encoder attention geometry")
    layers = cfg.get("head_layers", 2)
    max_len = cfg.get("max_len", 512)
    head_len = cfg.get("head_max_len", 192)
    if not 0 <= layers <= 16 or not 16 <= head_len < max_len <= min(
        8192, encoder["max_position_embeddings"]
    ):
        raise ValueError("Unsupported Laya head or token budget")
    temperatures = cfg.get("temperature", [1, 1, 1])
    if len(temperatures) != 3 or any(
        not math.isfinite(t) or t <= 0
        for t in [*temperatures, *cfg.get("temperature_by_options", {}).values()]
    ):
        raise ValueError("Calibration temperatures must be finite and positive")
    n_act = len(cfg.get("act_costs", {"escalate": 0.5})) + 1
    if n_act > 33:
        raise ValueError("Too many action classes")
    shapes = safetensor_shapes(source / "model.safetensors")
    expected = {
        "type_emb.weight": [3, dim],
        "scorer.0.weight": [dim],
        "scorer.0.bias": [dim],
        "scorer.1.weight": [dim, dim],
        "scorer.1.bias": [dim],
        "scorer.3.weight": [1, dim],
        "scorer.3.bias": [1],
        "act_head.0.weight": [256, dim + 4],
        "act_head.0.bias": [256],
        "act_head.2.weight": [n_act, 256],
        "act_head.2.bias": [n_act],
        "encoder.embeddings.tok_embeddings.weight": [encoder["vocab_size"], dim],
    }
    for layer in range(layers):
        prefix = f"head.layers.{layer}."
        for name, shape in {
            "self_attn.in_proj_weight": [3 * dim, dim],
            "self_attn.in_proj_bias": [3 * dim],
            "self_attn.out_proj.weight": [dim, dim],
            "self_attn.out_proj.bias": [dim],
            "linear1.weight": [4 * dim, dim],
            "linear1.bias": [4 * dim],
            "linear2.weight": [dim, 4 * dim],
            "linear2.bias": [dim],
            "norm1.weight": [dim],
            "norm1.bias": [dim],
            "norm2.weight": [dim],
            "norm2.bias": [dim],
        }.items():
            expected[prefix + name] = shape
    for name, shape in expected.items():
        if shapes.get(name) != shape:
            raise ValueError(
                f"Missing or incompatible tensor {name}: expected {shape}, got {shapes.get(name)}"
            )
    tokenizer = source / "tokenizer"
    for name in ("tokenizer.json", "tokenizer_config.json"):
        if not (tokenizer / name).is_file():
            raise ValueError(f"Missing tokenizer/{name}")
    # Transformers 5 stores theta values under rope_parameters; Antfly reads
    # the explicit scalar encoder fields. mmBERT uses 160000 for both modes.
    for kind, key in (
        ("full_attention", "global_rope_theta"),
        ("sliding_attention", "local_rope_theta"),
    ):
        params = encoder.get("rope_parameters", {}).get(kind)
        if params:
            if params.get("rope_type", "default") != "default":
                raise ValueError("Unsupported RoPE scaling")
            encoder[key] = params["rope_theta"]
    token_config = json.loads((tokenizer / "tokenizer_config.json").read_text())
    mask_token = token_config.get("mask_token", "[MASK]")
    if isinstance(mask_token, dict):
        mask_token = mask_token["content"]
    if not isinstance(mask_token, str) or not 1 <= len(mask_token.encode()) <= 128:
        raise ValueError("Invalid tokenizer mask token")
    cfg["mask_token"] = mask_token
    encoder["architectures"] = ["ModernBertModel"]
    encoder["laya"] = cfg
    output.mkdir(parents=True)
    try:
        shutil.copy2(source / "model.safetensors", output / "model.safetensors")
        for path in tokenizer.iterdir():
            if path.is_file():
                shutil.copy2(path, output / path.name)
        (output / "config.json").write_text(json.dumps(encoder, indent=2) + "\n")
        (output / "rl_agent_config.json").write_text(json.dumps(cfg, indent=2) + "\n")
        (output / "model_manifest.json").write_text(
            json.dumps(
                {
                    "type": "classifier",
                    "tasks": ["extract"],
                    "capabilities": ["classification", "typed_decisions"],
                    "inputs": ["text"],
                    "source": {"repository": origin, "revision": revision},
                },
                indent=2,
            )
            + "\n"
        )
    except BaseException:
        shutil.rmtree(output)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source", help="Local upstream checkpoint or Hugging Face repository"
    )
    parser.add_argument(
        "--revision", help="Hugging Face commit SHA (required for remote sources)"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = Path(args.source)
    if not source.is_dir():
        if (
            not args.revision
            or len(args.revision) != 40
            or any(c not in "0123456789abcdef" for c in args.revision)
        ):
            parser.error("Remote sources require --revision with a full commit SHA")
        from huggingface_hub import snapshot_download

        source = Path(
            snapshot_download(
                args.source,
                revision=args.revision,
                allow_patterns=[
                    "rl_agent_config.json",
                    "encoder/config.json",
                    "model.safetensors",
                    "tokenizer/*",
                ],
            )
        )
    prepare(source, args.output, args.source, args.revision or "local")
    print(f"Prepared Laya extractor at {args.output}")


if __name__ == "__main__":
    main()
