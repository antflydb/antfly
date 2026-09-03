#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Build a reproducible, benchmark-only BF16 Qwen3-VL 2B GGUF bundle.

The official Qwen checkpoint is BF16 SafeTensors.  Antfly's Qwen3-VL runtime
uses a split GGUF decoder/projector bundle, so this tool is the explicit,
auditable bridge between the pinned source checkpoint and the native Metal
high-precision benchmark lane.  It is deliberately *not* a production
promotion mechanism: the receipt identity produced here remains blocked by
the serving compatibility policy.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import sys
import tempfile
from typing import Any

from convert_qwen3vl_reranker import (
    ConversionError,
    DEFAULT_CONVERTER_SHA256,
    artifact_record,
    json_bytes,
    regular_contained_file,
    run_logged,
    sha256_file,
    validate_tool,
    write_bytes,
)
from transformers_oracle import REQUIRED_SIDECARS
from transformers_weights_oracle import MODEL_REVISION, MODEL_SHA256, MODEL_SIZE


SCHEMA = "antfly.qwen3vl.high_precision_conversion.v1"
RECEIPT_NAME = ".antfly-download-complete.json"
BLOCKING_MARKERS = (".antfly-download-in-progress", ".antfly-download-plan.json")
SOURCE_IDENTITY = {
    "owner": "Qwen",
    "name": "Qwen3-VL-2B-Instruct",
    "revision": MODEL_REVISION,
    "weight_format": "bf16-safetensors",
}
OUTPUT_IDENTITY = {
    "owner": "Qwen",
    "name": "Qwen3-VL-2B-Instruct-GGUF",
    "variant": "bf16-reference-bundle-v1",
}
DECODER_NAME = "Qwen3VL-2B-Instruct-BF16.gguf"
PROJECTOR_NAME = "mmproj-Qwen3VL-2B-Instruct-BF16.gguf"


def safe_relative_path(raw: object) -> str:
    if not isinstance(raw, str) or not raw or "\\" in raw or ":" in raw or "\x00" in raw:
        raise ConversionError(f"unsafe artifact path: {raw!r}")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise ConversionError(f"unsafe artifact path: {raw!r}")
    return raw


def validate_source(source_dir: Path) -> dict[str, Any]:
    """Validate the exact official BF16 source without trusting a path name."""
    root = source_dir.resolve(strict=True)
    if not root.is_dir():
        raise ConversionError(f"source is not a directory: {root}")
    for marker in BLOCKING_MARKERS:
        if (root / marker).exists():
            raise ConversionError(f"source publication is incomplete: {marker}")

    model = regular_contained_file(root, "model.safetensors")
    if model.stat().st_size != MODEL_SIZE:
        raise ConversionError(
            f"BF16 model size mismatch: expected {MODEL_SIZE}, got {model.stat().st_size}"
        )
    model_sha256 = sha256_file(model)
    if model_sha256 != MODEL_SHA256:
        raise ConversionError(
            f"BF16 model SHA-256 mismatch: expected {MODEL_SHA256}, got {model_sha256}"
        )

    sidecars: dict[str, dict[str, Any]] = {}
    for relative in REQUIRED_SIDECARS:
        safe_relative_path(relative)
        path = regular_contained_file(root, relative)
        sidecars[relative] = artifact_record(path, relative)

    config_path = root / "config.json"
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConversionError(f"invalid source config: {exc}") from exc
    if config.get("model_type") != "qwen3_vl":
        raise ConversionError(
            f"source model_type is not qwen3_vl: {config.get('model_type')!r}"
        )
    return {
        "root": root,
        "identity": SOURCE_IDENTITY,
        "model": artifact_record(model, "model.safetensors"),
        "sidecars": sidecars,
    }


def open_gguf(path: Path) -> Any:
    try:
        from gguf import GGUFReader
    except ImportError as exc:
        raise ConversionError(
            "GGUF validation requires requirements-qwen3vl-conversion.txt"
        ) from exc
    return GGUFReader(path)


def _contents(reader: Any, field: str) -> Any:
    value = reader.get_field(field)
    if value is None:
        raise ConversionError(f"GGUF is missing {field}")
    return value.contents()


def validate_decoder(path: Path) -> dict[str, Any]:
    reader = open_gguf(path)
    architecture = _contents(reader, "general.architecture")
    blocks = _contents(reader, "qwen3vl.block_count")
    if architecture != "qwen3vl" or blocks != 28:
        raise ConversionError(
            "BF16 decoder metadata does not match Qwen3-VL-2B "
            f"(architecture={architecture!r}, block_count={blocks!r})"
        )
    tensors = {tensor.name: tensor for tensor in reader.tensors}
    required = {
        "token_embd.weight",
        "output_norm.weight",
        "blk.0.attn_q.weight",
        "blk.0.attn_k.weight",
        "blk.0.attn_v.weight",
        "blk.0.ffn_gate.weight",
        "blk.27.attn_output.weight",
        "blk.27.ffn_down.weight",
    }
    missing = sorted(required - tensors.keys())
    if missing:
        raise ConversionError(f"BF16 decoder is missing required tensors: {missing}")
    types = sorted({tensor.tensor_type.name for tensor in reader.tensors})
    if any(name.startswith("Q") for name in types):
        raise ConversionError(f"high-precision decoder unexpectedly contains quantized tensors: {types}")
    # The official 2B checkpoint has tied input/output embeddings, so the
    # valid llama.cpp export intentionally omits ``output.weight``.  Bind the
    # shared output matrix to token_embd instead (as the qualified Q4 receipt
    # does) instead of accidentally rejecting a correct conversion.
    if tensors["token_embd.weight"].tensor_type.name != "BF16":
        raise ConversionError(
            "BF16 decoder tied token/output embedding was not emitted as BF16: "
            f"{tensors['token_embd.weight'].tensor_type.name}"
        )
    return {
        "architecture": architecture,
        "block_count": blocks,
        "tensor_count": len(tensors),
        "tensor_types": types,
    }


def validate_projector(path: Path) -> dict[str, Any]:
    reader = open_gguf(path)
    architecture = _contents(reader, "general.architecture")
    artifact_type = _contents(reader, "general.type")
    blocks = _contents(reader, "clip.vision.block_count")
    if architecture != "clip" or artifact_type != "mmproj" or blocks != 24:
        raise ConversionError(
            "BF16 projector metadata does not match Qwen3-VL-2B "
            f"(architecture={architecture!r}, type={artifact_type!r}, blocks={blocks!r})"
        )
    tensors = {tensor.name: tensor for tensor in reader.tensors}
    required = {
        "v.patch_embd.weight",
        "v.position_embd.weight",
        "v.post_ln.weight",
        "mm.0.weight",
        "mm.2.weight",
        "v.deepstack.5.fc1.weight",
        "v.deepstack.11.fc1.weight",
        "v.deepstack.17.fc1.weight",
    }
    missing = sorted(required - tensors.keys())
    if missing:
        raise ConversionError(f"BF16 projector is missing required tensors: {missing}")
    types = sorted({tensor.tensor_type.name for tensor in reader.tensors})
    if any(name.startswith("Q") for name in types):
        raise ConversionError(f"high-precision projector unexpectedly contains quantized tensors: {types}")
    # llama.cpp deliberately forces the vision patch/merger matrices to F32
    # for a BF16 mmproj (see its tensor_force_quant policy).  That is a
    # higher-precision, architecture-required exception—not a quantized
    # fallback—and must be pinned explicitly in the reference contract.
    if tensors["v.patch_embd.weight"].tensor_type.name != "F32":
        raise ConversionError(
            "BF16 projector patch embedding was not emitted as F32: "
            f"{tensors['v.patch_embd.weight'].tensor_type.name}"
        )
    return {
        "architecture": architecture,
        "artifact_type": artifact_type,
        "block_count": blocks,
        "tensor_count": len(tensors),
        "tensor_types": types,
    }


def convert_artifact(
    source: Path,
    output: Path,
    converter: Path,
    log_path: Path,
    *,
    projector: bool,
) -> None:
    """Run one deterministic conversion without retaining a whole second bundle."""
    logs = output.parent / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    command = [sys.executable, str(converter), str(source)]
    if projector:
        command.append("--mmproj")
    command.extend(("--outtype", "bf16", "--outfile", str(output)))
    run_logged(command, logs / log_path)


def copy_sidecars(source: Path, destination: Path) -> None:
    for relative in REQUIRED_SIDECARS:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(regular_contained_file(source, relative), target)


def build_bundle(
    source_dir: Path,
    output_dir: Path,
    converter: Path,
    converter_sha256: str,
) -> dict[str, Any]:
    source = validate_source(source_dir)
    converter_info = validate_tool(converter, converter_sha256, "converter")
    destination = output_dir.absolute()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise ConversionError(f"destination already exists: {destination}")

    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent))
    published = False
    try:
        # Retain the validated first artifact as the published candidate, then
        # create and discard one independent duplicate at a time.  This keeps
        # the reproducibility proof while avoiding two full BF16 bundles on a
        # single local volume.
        first_digests: dict[str, str] = {}
        second_digests: dict[str, str] = {}
        first_decoder = staging / "decoder-a-bf16.gguf"
        convert_artifact(
            source["root"], first_decoder, converter, Path("decoder-a.log"), projector=False
        )
        first_digests["decoder"] = sha256_file(first_decoder)
        decoder_contract = validate_decoder(first_decoder)
        shutil.move(first_decoder, staging / DECODER_NAME)

        first_projector = staging / "projector-a-bf16.gguf"
        convert_artifact(
            source["root"], first_projector, converter, Path("projector-a.log"), projector=True
        )
        first_digests["projector"] = sha256_file(first_projector)
        projector_contract = validate_projector(first_projector)
        shutil.move(first_projector, staging / PROJECTOR_NAME)

        second_decoder = staging / "decoder-b-bf16.gguf"
        convert_artifact(
            source["root"], second_decoder, converter, Path("decoder-b.log"), projector=False
        )
        second_digests["decoder"] = sha256_file(second_decoder)
        validate_decoder(second_decoder)
        second_decoder.unlink()

        second_projector = staging / "projector-b-bf16.gguf"
        convert_artifact(
            source["root"], second_projector, converter, Path("projector-b.log"), projector=True
        )
        second_digests["projector"] = sha256_file(second_projector)
        validate_projector(second_projector)
        second_projector.unlink()
        if first_digests != second_digests:
            raise ConversionError(
                f"BF16 conversion is not reproducible: first={first_digests}, second={second_digests}"
            )

        copy_sidecars(source["root"], staging)
        write_bytes(
            staging / "antfly_inference_bundle.json",
            json_bytes(
                {
                    "family": "qwen3_vl_gguf_bundle/v1",
                    "decoder": DECODER_NAME,
                    "projector": PROJECTOR_NAME,
                }
            ),
        )
        write_bytes(
            staging / "model_manifest.json",
            json_bytes(
                {
                    "type": "generative",
                    "inputs": ["text", "image"],
                    "capabilities": ["qwen3vl", "multimodal", "high_precision_reference"],
                    "benchmark_only": True,
                }
            ),
        )
        report = {
            "schema": SCHEMA,
            "pass": True,
            "release_ready": False,
            "benchmark_only": True,
            # ``source["root"]`` is an execution-only Path.  Keep the receipt
            # JSON portable and serialization-safe while preserving every
            # provenance-bearing source artifact.
            "source": {
                "path": str(source["root"]),
                "identity": source["identity"],
                "model": source["model"],
                "sidecars": source["sidecars"],
            },
            "output_identity": OUTPUT_IDENTITY,
            "tools": {"converter": converter_info},
            "reproducible": True,
            "reproduction_digests": {"first": first_digests, "second": second_digests},
            "contracts": {"decoder": decoder_contract, "projector": projector_contract},
            "outputs": {
                "decoder": artifact_record(staging / DECODER_NAME, DECODER_NAME),
                "projector": artifact_record(staging / PROJECTOR_NAME, PROJECTOR_NAME),
            },
        }
        write_bytes(staging / "conversion-report.json", json_bytes(report))
        artifacts = [
            artifact_record(path, str(path.relative_to(staging)))
            for path in sorted(staging.rglob("*"))
            if path.is_file()
        ]
        write_bytes(
            staging / RECEIPT_NAME,
            json_bytes({"version": 2, "source": OUTPUT_IDENTITY, "artifacts": artifacts}),
        )
        os.rename(staging, destination)
        published = True
        report["managed_receipt_sha256"] = sha256_file(destination / RECEIPT_NAME)
        return report
    finally:
        if not published:
            shutil.rmtree(staging, ignore_errors=True)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--converter", type=Path, required=True)
    parser.add_argument("--converter-sha256", default=DEFAULT_CONVERTER_SHA256)
    parser.add_argument("--report", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.report.exists():
        print(f"refusing to overwrite report: {args.report}", file=sys.stderr)
        return 2
    try:
        report = build_bundle(
            args.source_dir,
            args.output_dir,
            args.converter,
            args.converter_sha256,
        )
        write_bytes(args.report, json_bytes(report))
        print(json.dumps({"pass": True, "report": str(args.report.resolve())}))
        return 0
    except (ConversionError, OSError, ValueError) as exc:
        print(f"Qwen3-VL high-precision conversion failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
