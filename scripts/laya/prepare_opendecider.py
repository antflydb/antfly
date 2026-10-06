#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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
"""Prepare OpenDecider-nano for Antfly's typed-decision runtime.

uv run scripts/laya/prepare_opendecider.py manjunathshiva/opendecider-nano \
    --revision <commit> --output ./models/extractors/opendecider-nano

OpenDecider-nano is an Ettin (ModernBERT) encoder plus a marker MLP stored in
head.safetensors. Antfly serves it through the Laya pipeline with
`laya.format: "opendecider"`, which needs one safetensors file: the encoder
tensors are renamed to `encoder.*` and the head's to `scorer.*`, unchanged
otherwise. Local source directories work with Python's standard library alone.
"""

from __future__ import annotations

import argparse
import json
import shutil
import struct
from pathlib import Path

# Antfly's prompt switch reproduces opendecider/prompt.py `nano_ids`, and the
# head Linear-GELU-LayerNorm-Linear, as of this package version.
OPENDECIDER_HEAD = "mlp: Linear-GELU-LayerNorm-Linear"


def read_header(path: Path) -> tuple[dict, int]:
    with path.open("rb") as f:
        length_bytes = f.read(8)
        if len(length_bytes) != 8:
            raise ValueError(f"Missing safetensors header: {path}")
        length = struct.unpack("<Q", length_bytes)[0]
        if length > 16 * 1024 * 1024:
            raise ValueError("Safetensors header exceeds 16 MiB")
        header = json.loads(f.read(length))
    header.pop("__metadata__", None)
    return header, 8 + length


def merge(
    parts: list[tuple[Path, dict[str, str]]], output: Path
) -> dict[str, list[int]]:
    """Write one safetensors file holding every tensor of `parts`, each
    renamed by its part's mapping. Tensor bytes are copied unchanged."""
    entries = []
    for path, names in parts:
        header, data_start = read_header(path)
        if set(header) != set(names):
            raise ValueError(
                f"Unexpected tensors in {path.name}: {sorted(set(header) ^ set(names))}"
            )
        for old, meta in header.items():
            begin, end = meta["data_offsets"]
            entries.append((names[old], meta, path, data_start + begin, end - begin))
    entries.sort(key=lambda e: e[0])
    header, offset = {}, 0
    for name, meta, _, _, size in entries:
        header[name] = {
            "dtype": meta["dtype"],
            "shape": meta["shape"],
            "data_offsets": [offset, offset + size],
        }
        offset += size
    blob = json.dumps(header, separators=(",", ":")).encode()
    blob += b" " * (-len(blob) % 8)
    with output.open("wb") as out:
        out.write(struct.pack("<Q", len(blob)) + blob)
        for _, _, path, start, size in entries:
            with path.open("rb") as f:
                f.seek(start)
                while size:
                    chunk = f.read(min(size, 1 << 24))
                    if not chunk:
                        raise ValueError(f"Truncated tensor data in {path.name}")
                    out.write(chunk)
                    size -= len(chunk)
    return {name: meta["shape"] for name, meta, _, _, _ in entries}


def prepare(source: Path, output: Path, origin: str, revision: str) -> None:
    if output.exists():
        raise ValueError(f"Output already exists: {output}")
    info = json.loads((source / "opendecider.json").read_text())
    if info.get("kind") != "nano" or info.get("head") != OPENDECIDER_HEAD:
        raise ValueError(
            f"Only OpenDecider-nano with the {OPENDECIDER_HEAD!r} head is supported"
        )
    encoder = json.loads((source / "config.json").read_text())
    if encoder.get("model_type") != "modernbert":
        raise ValueError("OpenDecider-nano must be ModernBERT-backed")
    for key in ("attention_bias", "mlp_bias", "norm_bias"):
        if encoder.get(key):
            raise ValueError(f"Unsupported ModernBERT {key}")
    for kind in ("full_attention", "sliding_attention"):
        if (
            encoder.get("rope_parameters", {}).get(kind, {}).get("rope_type", "default")
            != "default"
        ):
            raise ValueError("Unsupported RoPE scaling")
    dim = encoder["hidden_size"]
    max_len = int(info.get("max_len", 2048))
    if max_len > encoder["max_position_embeddings"]:
        raise ValueError("max_len exceeds the encoder's positions")
    token_config = json.loads((source / "tokenizer_config.json").read_text())
    mask_token = token_config.get("mask_token", "[MASK]")
    if isinstance(mask_token, dict):
        mask_token = mask_token["content"]
    if not isinstance(mask_token, str) or not 1 <= len(mask_token.encode()) <= 128:
        raise ValueError("Invalid tokenizer mask token")
    encoder_names, _ = read_header(source / "model.safetensors")
    head_names, _ = read_header(source / "head.safetensors")
    head_map = {
        f"{i}.{p}": f"scorer.{i}.{p}" for i in (0, 2, 3) for p in ("weight", "bias")
    }
    output.mkdir(parents=True)
    try:
        shapes = merge(
            [
                (
                    source / "model.safetensors",
                    {n: "encoder." + n.removeprefix("model.") for n in encoder_names},
                ),
                (
                    source / "head.safetensors",
                    {n: head_map.get(n, n) for n in head_names},
                ),
            ],
            output / "model.safetensors",
        )
        for name, shape in {
            "scorer.0.weight": [dim, dim],
            "scorer.0.bias": [dim],
            "scorer.2.weight": [dim],
            "scorer.2.bias": [dim],
            "scorer.3.weight": [1, dim],
            "scorer.3.bias": [1],
            "encoder.embeddings.tok_embeddings.weight": [encoder["vocab_size"], dim],
        }.items():
            if shapes.get(name) != shape:
                raise ValueError(
                    f"Missing or incompatible tensor {name}: expected {shape}, got {shapes.get(name)}"
                )
        for name in ("tokenizer.json", "tokenizer_config.json"):
            shutil.copy2(source / name, output / name)
        encoder["architectures"] = ["ModernBertModel"]
        encoder["laya"] = {
            "format": "opendecider",
            "head_layers": 0,
            "max_len": max_len,
            "mask_token": mask_token,
        }
        (output / "config.json").write_text(json.dumps(encoder, indent=2) + "\n")
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
    parser.add_argument("source", help="Local checkpoint or Hugging Face repository")
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
                    "config.json",
                    "opendecider.json",
                    "model.safetensors",
                    "head.safetensors",
                    "tokenizer.json",
                    "tokenizer_config.json",
                ],
            )
        )
    prepare(source, args.output, args.source, args.revision or "local")


if __name__ == "__main__":
    main()
