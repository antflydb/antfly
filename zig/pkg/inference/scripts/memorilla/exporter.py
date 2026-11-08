#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Strict, offline Memorilla memory.pt to native SafeTensors conversion.

Only the explicit acquire command accesses the network. Conversion never imports
upstream code or deserializes arbitrary Python objects. Output directories are
published atomically after a bitwise round-trip check.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import tempfile
import urllib.request

SCHEMA = "antfly.memorilla.bundle.v1"
UPSTREAM_COMMIT = "d8f18891a50bf33c18f04fe5167c22f51e58c9cd"
UPSTREAM_MEMORY_SHA256 = (
    "09acdf9ec6b8be550bd90d01cd825365de5dde6d6e19eccf4ece436ed94626a7"
)
MODEL = "memorilla/Memorilla-Qwen3-8B"
REVISION = "f6b15af56aa687861c8d2d03b1cd8f7979881361"
CHECKPOINT_SHA256 = {
    "stage1": "14044002c5cb9f14de15fac5b96f01c4044d1c238151c7dcf71cfd3deea022ed",
    "stage2": "5e8476db360b1326fb6026e892e9e42c19bcf96a79208c7022c20dba8d2a5fca",
    "stage3": "782d4a84ac13a2a4e1b72be3284e078e752c2a595b96301741028539aa48305a",
    "pv4": "9c343c7e7b1c143c54118796bcdb96823b8d2c8461098a2afe521947d13c37da",
    "pmv2": "04bc3f82989dc3c756bc2dc1f9a3e06941dc88e796cfe6f0aa2e06ae726ac047",
}
# Canonical split-QKV SafeTensors v1, generated from the independently verified
# original releases with safetensors 0.5.3. These bind converted tensor bytes to
# release identity even if a bundle's mutable manifest is rewritten.
CONVERTED_SHA256 = {
    "stage1": "32497c063bc1a6667925f5ed7b7acdb08d0b1b32e96241c3f650a5a6ccd163d8",
    "stage2": "a3f3e1d42f53ccc108005587bfd39718b913b2c727128e02e254b63665c32dbd",
    "stage3": "3a7ec610dc29818c2aa29c3867d3be3e69908a191a94d0d6f0f416d564d3470e",
    "pv4": "1d054e866b01fd4b3345ba90787f7bfafa915dcdff4065bd9341cd44f0c03445",
    "pmv2": "c5a6a671d8223189e1015889715e310cf912254ebfbfd698e3d14064000ce301",
}
RELEASE_CONFIG = {
    "embedding_dim": 2560,
    "output_dim": 4096,
    "num_memories": 16,
    "num_heads": 8,
    "num_self_attn_layers": 0,
    "num_cross_attn_layers": 2,
    "dropout": 0.1,
    "retrieval_init": False,
}
BASE_MODELS = {
    "encoder": {
        "model": "Qwen/Qwen3-Embedding-4B",
        "revision": "5cf2132abc99cad020ac570b19d031efec650f2b",
    },
    "decoder": {
        "model": "Qwen/Qwen3-8B",
        "revision": "b968826d9c46dd6066d109eabc6255188de91218",
    },
}
EXECUTION = {
    "layer_norm_epsilon": 1e-5,
    "gelu": "erf",
    "attention": "noncausal-mha",
    "padding_true_means": "masked",
    "projection_layout": "out_features,in_features",
    "qkv_layout": "split-q-k-v",
    "encoder_pooling": "last-token-l2-normalized",
    "document_prefix": "",
    "question_prefix": "",
    "cached_embedding_dtype": "float16",
    "memory_input_dtype": "bfloat16",
    "enable_thinking": False,
    "soft_tokens": "input-embedding-rows",
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path: Path) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    value = json.loads(Path(path).read_text(), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def validate_config(config: dict, release: bool = True) -> dict:
    if set(config) != set(RELEASE_CONFIG):
        raise ValueError("unsupported memory config fields")
    for key in (
        "embedding_dim",
        "output_dim",
        "num_memories",
        "num_heads",
        "num_cross_attn_layers",
    ):
        if type(config[key]) is not int or not 1 <= config[key] <= 16384:
            raise ValueError(f"invalid {key}")
    if (
        type(config["num_self_attn_layers"]) is not int
        or config["num_self_attn_layers"] != 0
    ):
        raise ValueError("document self-attention is unsupported")
    if config["retrieval_init"] is not False:
        raise ValueError("retrieval initialization is unsupported")
    if (
        type(config["dropout"]) not in (int, float)
        or not math.isfinite(config["dropout"])
        or not 0 <= config["dropout"] < 1
    ):
        raise ValueError("invalid dropout")
    if config["embedding_dim"] % config["num_heads"]:
        raise ValueError("embedding width must divide into attention heads")
    if config["num_cross_attn_layers"] > 8:
        raise ValueError("unsupported readout depth")
    if release and config != RELEASE_CONFIG:
        raise ValueError("config differs from the released Qwen architecture")
    return dict(config)


def expected_shapes(config: dict, split: bool = True) -> dict[str, tuple[int, ...]]:
    d, h, k = (config[x] for x in ("embedding_dim", "output_dim", "num_memories"))
    result = {"memory_queries": (1, k, d)}

    def linear(name, out, inp):
        result[f"{name}.weight"] = (out, inp)
        result[f"{name}.bias"] = (out,)

    def norm(name):
        result[f"{name}.weight"] = (d,)
        result[f"{name}.bias"] = (d,)

    def attention(name):
        if split:
            for part in ("q", "k", "v"):
                linear(f"{name}.{part}_proj", d, d)
        else:
            result[f"{name}.in_proj_weight"] = (3 * d, d)
            result[f"{name}.in_proj_bias"] = (3 * d,)
        linear(f"{name}.out_proj", d, d)

    attention("query_cond_attn")
    norm("query_cond_norm")
    norm("doc_norm")
    for i in range(config["num_cross_attn_layers"]):
        norm(f"cross_attn_norms.{i}")
        attention(f"cross_attn_layers.{i}")
        norm(f"ffn_norms.{i}")
        linear(f"ffn_layers.{i}.0", 4 * d, d)
        linear(f"ffn_layers.{i}.3", d, 4 * d)
    norm("layer_norm")
    linear("memory_projection", h, d)
    return result


def validate_tensors(weights, config, *, split, release):
    import torch

    shapes = expected_shapes(config, split)
    if not isinstance(weights, dict) or set(weights) != set(shapes):
        missing = (
            sorted(set(shapes) - set(weights))
            if isinstance(weights, dict)
            else list(shapes)
        )
        extra = sorted(set(weights) - set(shapes)) if isinstance(weights, dict) else []
        raise ValueError(f"tensor inventory mismatch; missing={missing}, extra={extra}")
    dtype = None
    for name, shape in shapes.items():
        tensor = weights[name]
        if (
            not isinstance(tensor, torch.Tensor)
            or tensor.layout != torch.strided
            or tuple(tensor.shape) != shape
        ):
            raise ValueError(f"invalid tensor shape/layout: {name}, expected {shape}")
        if tensor.dtype not in (torch.float32, torch.float16, torch.bfloat16):
            raise ValueError(f"unsupported tensor dtype: {name}")
        if release and tensor.dtype != torch.bfloat16:
            raise ValueError(f"released tensor is not BF16: {name}")
        if dtype is not None and tensor.dtype != dtype:
            raise ValueError("mixed tensor precision is unsupported")
        dtype = tensor.dtype
        if not torch.isfinite(tensor).all().item():
            raise ValueError(f"nonfinite tensor: {name}")
    return str(dtype).removeprefix("torch.")


def split_projections(weights: dict) -> dict:
    result = {}
    for name, tensor in weights.items():
        suffix = next(
            (s for s in ("in_proj_weight", "in_proj_bias") if name.endswith(f".{s}")),
            None,
        )
        if suffix:
            base = name[: -(len(suffix) + 1)]
            kind = "weight" if suffix.endswith("weight") else "bias"
            for part, value in zip(
                ("q", "k", "v"), tensor.chunk(3, dim=0), strict=True
            ):
                result[f"{base}.{part}_proj.{kind}"] = value.contiguous().clone()
        else:
            result[name] = tensor.contiguous().clone()
    return dict(sorted(result.items()))


def load_bundle(bundle_dir: Path):
    from safetensors.torch import load_file

    bundle_dir = Path(bundle_dir)
    manifest = read_json(bundle_dir / "manifest.json")
    if manifest.get("schema") != SCHEMA or type(manifest.get("release")) is not bool:
        raise ValueError("unsupported bundle manifest")
    if manifest.get("execution") != EXECUTION:
        raise ValueError("unsupported execution contract")
    for filename in ("config.json", "memory.safetensors"):
        info = manifest.get("files", {}).get(filename, {})
        path = bundle_dir / filename
        if info.get("size_bytes") != path.stat().st_size or info.get(
            "sha256"
        ) != sha256(path):
            raise ValueError(f"bundle checksum mismatch: {filename}")
    config = validate_config(read_json(bundle_dir / "config.json"), manifest["release"])
    if manifest.get("config") != config:
        raise ValueError("manifest config mismatch")
    weights = load_file(str(bundle_dir / "memory.safetensors"), device="cpu")
    dtype = validate_tensors(weights, config, split=True, release=manifest["release"])
    inventory = {
        name: {"shape": list(t.shape), "dtype": dtype}
        for name, t in sorted(weights.items())
    }
    count = sum(t.numel() for t in weights.values())
    if (
        manifest.get("tensors") != inventory
        or manifest.get("parameter_count") != count
        or manifest.get("dtype") != dtype
    ):
        raise ValueError("manifest tensor inventory mismatch")
    if manifest["release"]:
        source = manifest.get("source", {})
        checkpoint = source.get("checkpoint")
        if (
            not isinstance(checkpoint, str)
            or checkpoint not in CHECKPOINT_SHA256
            or source.get("model") != MODEL
            or source.get("revision") != REVISION
            or source.get("sha256") != CHECKPOINT_SHA256.get(checkpoint)
        ):
            raise ValueError("unrecognized release provenance")
        if manifest.get("base_models") != BASE_MODELS:
            raise ValueError("unsupported base model pair")
        implementation = manifest.get("implementation", {})
        if not isinstance(implementation, dict) or any(
            implementation.get(key) != expected
            for key, expected in {
                "repository": "snap-stanford/memorilla",
                "revision": UPSTREAM_COMMIT,
                "memory_py_sha256": UPSTREAM_MEMORY_SHA256,
            }.items()
        ):
            raise ValueError("unsupported memory implementation provenance")
        if (
            manifest["files"]["memory.safetensors"]["sha256"]
            != CONVERTED_SHA256[checkpoint]
        ):
            raise ValueError("converted weights differ from the pinned release")
    return config, weights, manifest


def export_checkpoint(
    checkpoint_dir: Path,
    output_dir: Path,
    *,
    expected_sha256: str | None = None,
    release: bool = True,
) -> dict:
    import torch
    from safetensors.torch import load_file, save_file

    checkpoint_dir, output_dir = Path(checkpoint_dir), Path(output_dir)
    if output_dir.exists():
        raise ValueError("output already exists; choose a new bundle directory")
    config = validate_config(read_json(checkpoint_dir / "config.json"), release)
    source = checkpoint_dir / "memory.pt"
    source_digest = sha256(source)
    if expected_sha256 is not None and source_digest != expected_sha256:
        raise ValueError("checkpoint SHA-256 mismatch")
    checkpoint = next(
        (key for key, value in CHECKPOINT_SHA256.items() if value == source_digest),
        None,
    )
    if release and checkpoint is None:
        raise ValueError("checkpoint is not a pinned published release")
    weights = torch.load(source, map_location="cpu", weights_only=True, mmap=True)
    validate_tensors(weights, config, split=False, release=release)
    converted = split_projections(weights)
    dtype = validate_tensors(converted, config, split=True, release=release)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{output_dir.name}-", dir=output_dir.parent
    ) as temporary:
        staging = Path(temporary) / "bundle"
        staging.mkdir()
        (staging / "config.json").write_text(json.dumps(config, indent=2) + "\n")
        save_file(
            converted, str(staging / "memory.safetensors"), metadata={"format": SCHEMA}
        )
        restored = load_file(str(staging / "memory.safetensors"), device="cpu")
        for name, tensor in converted.items():
            # Byte equality also distinguishes signed zeros and preserves BF16.
            if not torch.equal(
                tensor.view(torch.uint8), restored[name].view(torch.uint8)
            ):
                raise ValueError(f"round-trip mismatch: {name}")
        manifest = {
            "schema": SCHEMA,
            "release": release,
            "config": config,
            "execution": EXECUTION,
            "base_models": BASE_MODELS if release else None,
            "source": {
                "model": MODEL if release else None,
                "revision": REVISION if release else None,
                "checkpoint": checkpoint,
                "sha256": source_digest,
                "config_sha256": sha256(checkpoint_dir / "config.json"),
            },
            "implementation": {
                "repository": "snap-stanford/memorilla",
                "revision": UPSTREAM_COMMIT,
                "memory_py_sha256": UPSTREAM_MEMORY_SHA256,
                "exporter_sha256": sha256(Path(__file__)),
            },
            "dtype": dtype,
            "parameter_count": sum(t.numel() for t in converted.values()),
            "tensors": {
                name: {"shape": list(t.shape), "dtype": dtype}
                for name, t in converted.items()
            },
            "files": {
                name: {
                    "sha256": sha256(staging / name),
                    "size_bytes": (staging / name).stat().st_size,
                }
                for name in ("config.json", "memory.safetensors")
            },
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )
        load_bundle(staging)
        staging.rename(output_dir)
    return manifest


def acquire(output_root: Path, checkpoints: list[str]):
    """Fetch only pinned memory weights and config, checking Git/LFS hashes."""
    output_root = Path(output_root)
    with urllib.request.urlopen(
        f"https://huggingface.co/api/models/{MODEL}/revision/{REVISION}?blobs=true",
        timeout=60,
    ) as response:
        metadata = json.load(response)
    if metadata.get("sha") != REVISION:
        raise ValueError("upstream revision mismatch")
    files = {entry["rfilename"]: entry for entry in metadata["siblings"]}
    for checkpoint in checkpoints:
        if checkpoint not in CHECKPOINT_SHA256:
            raise ValueError(f"unknown checkpoint: {checkpoint}")
        destination = output_root / checkpoint
        if destination.exists():
            validate_config(read_json(destination / "config.json"))
            if sha256(destination / "memory.pt") != CHECKPOINT_SHA256[checkpoint]:
                raise ValueError(f"existing checkpoint checksum mismatch: {checkpoint}")
            continue
        output_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=f".{checkpoint}-", dir=output_root
        ) as temporary:
            staging = Path(temporary) / "checkpoint"
            staging.mkdir()
            for filename in ("config.json", "memory.pt"):
                entry = files[f"{checkpoint}/{filename}"]
                target = staging / filename
                with (
                    urllib.request.urlopen(
                        f"https://huggingface.co/{MODEL}/resolve/{REVISION}/{checkpoint}/{filename}",
                        timeout=120,
                    ) as response,
                    target.open("xb") as output,
                ):
                    for chunk in iter(lambda: response.read(4 * 1024 * 1024), b""):
                        output.write(chunk)
                if target.stat().st_size != entry["size"]:
                    raise ValueError(f"download size mismatch: {filename}")
                if filename == "memory.pt":
                    expected = CHECKPOINT_SHA256[checkpoint]
                    if (
                        entry.get("lfs", {}).get("sha256") != expected
                        or sha256(target) != expected
                    ):
                        raise ValueError("download SHA-256 mismatch")
                else:
                    data = target.read_bytes()
                    git_blob = hashlib.sha1(
                        f"blob {len(data)}\0".encode() + data
                    ).hexdigest()
                    if git_blob != entry["blobId"]:
                        raise ValueError("config Git blob mismatch")
                    validate_config(read_json(target))
            staging.rename(destination)
        print(f"verified {checkpoint}: {CHECKPOINT_SHA256[checkpoint]}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    download = sub.add_parser("acquire")
    download.add_argument("--output-root", type=Path, required=True)
    download.add_argument(
        "--checkpoint", choices=[*CHECKPOINT_SHA256, "all"], default="stage3"
    )
    export = sub.add_parser("export")
    export.add_argument("--checkpoint-dir", type=Path, required=True)
    export.add_argument("--output-dir", type=Path, required=True)
    export.add_argument("--expected-sha256")
    check = sub.add_parser("verify")
    check.add_argument("--bundle", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "acquire":
        acquire(
            args.output_root,
            list(CHECKPOINT_SHA256) if args.checkpoint == "all" else [args.checkpoint],
        )
    elif args.command == "export":
        manifest = export_checkpoint(
            args.checkpoint_dir, args.output_dir, expected_sha256=args.expected_sha256
        )
        print(
            json.dumps(
                {
                    "parameter_count": manifest["parameter_count"],
                    "dtype": manifest["dtype"],
                    "checkpoint": manifest["source"]["checkpoint"],
                }
            )
        )
    else:
        _, _, manifest = load_bundle(args.bundle)
        print(
            json.dumps(
                {"verified": True, "parameter_count": manifest["parameter_count"]}
            )
        )


if __name__ == "__main__":
    main()
