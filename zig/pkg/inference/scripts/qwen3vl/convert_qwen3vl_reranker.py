#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Build a reproducible, managed Qwen3-VL reranker GGUF bundle.

This tool deliberately treats conversion as a release operation. It validates
the complete managed BF16 source, pins the executable converter and quantizer,
runs two independent conversion passes, compares their bytes, validates the
GGUF metadata/tensor catalogs, and atomically publishes a version-2 managed
bundle. A partial or mismatched build is never visible at the destination.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from typing import Any

SCHEMA = "antfly.qwen3vl.reranker_conversion.v1"
RECEIPT_NAME = ".antfly-download-complete.json"
BLOCKING_MARKERS = (".antfly-download-in-progress", ".antfly-download-plan.json")
SOURCE_IDENTITY = {
    "owner": "Qwen",
    "name": "Qwen3-VL-Reranker-2B",
    "variant": "bf16-safetensors-bundle-v1",
}
DECODER_QUANTIZATIONS = ("Q8_0", "Q4_K_M")
DEFAULT_CONVERTER_SHA256 = "3ff05b62f65c16c1864ee3439b692aa6f184f5e18356f94ae2ebe1c7427644d4"
DEFAULT_QUANTIZER_SHA256 = "3949db9ef1577e482d852185c169b4d68bc8865022a1380cd82ef78026867260"
HEX_64 = re.compile(r"[0-9a-f]{64}")
ASSETS = (
    "config.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "preprocessor_config.json",
    "video_preprocessor_config.json",
    "chat_template.jinja",
    "additional_chat_templates/reranker.jinja",
)
PROJECTOR_NAME = "mmproj-Qwen3-VL-Reranker-2B-Q8_0.gguf"


class ConversionError(RuntimeError):
    pass


def open_gguf(path: Path) -> Any:
    try:
        from gguf import GGUFReader
    except ImportError as exc:
        raise ConversionError(
            "GGUF validation requires requirements-qwen3vl-conversion.txt"
        ) from exc
    return GGUFReader(path)


def decoder_name(quantization: str) -> str:
    if quantization not in DECODER_QUANTIZATIONS:
        raise ConversionError(f"unsupported decoder quantization: {quantization}")
    return f"Qwen3-VL-Reranker-2B-{quantization}.gguf"


def output_identity(quantization: str) -> dict[str, str]:
    if quantization not in DECODER_QUANTIZATIONS:
        raise ConversionError(f"unsupported decoder quantization: {quantization}")
    return {
        "owner": "Qwen",
        "name": "Qwen3-VL-Reranker-2B-GGUF",
        "variant": f"{quantization.lower().replace('_', '-')}-q8-0-bundle-v1",
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def load_object(path: Path) -> dict[str, Any]:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ConversionError(f"duplicate JSON key {key!r} in {path}")
            result[key] = value
        return result

    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys)
    except (OSError, json.JSONDecodeError) as exc:
        raise ConversionError(f"invalid JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ConversionError(f"JSON root is not an object: {path}")
    return value


def safe_relative_path(raw: object) -> str:
    if not isinstance(raw, str) or not raw or "\\" in raw or ":" in raw or "\x00" in raw:
        raise ConversionError(f"unsafe managed artifact path: {raw!r}")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise ConversionError(f"unsafe managed artifact path: {raw!r}")
    return raw


def regular_contained_file(root: Path, relative: str) -> Path:
    path = root / relative
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ConversionError(f"missing managed artifact {relative}: {exc}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise ConversionError(f"managed artifact is not a regular file: {relative}")
    if not path.resolve(strict=True).is_relative_to(root):
        raise ConversionError(f"managed artifact escapes model root: {relative}")
    return path


def validate_source(source_dir: Path) -> dict[str, Any]:
    root = source_dir.resolve(strict=True)
    if not root.is_dir():
        raise ConversionError(f"source is not a directory: {root}")
    for marker in BLOCKING_MARKERS:
        if (root / marker).exists():
            raise ConversionError(f"managed source publication is incomplete: {marker}")
    receipt_path = regular_contained_file(root, RECEIPT_NAME)
    receipt = load_object(receipt_path)
    if receipt.get("version") != 2:
        raise ConversionError("source must have a version-2 managed receipt")
    if receipt.get("source") != SOURCE_IDENTITY:
        raise ConversionError(f"managed source identity mismatch: {receipt.get('source')!r}")
    items = receipt.get("artifacts")
    if not isinstance(items, list) or not items:
        raise ConversionError("managed source receipt has no artifacts")

    seen: set[str] = set()
    artifacts: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            raise ConversionError("managed source artifact is not an object")
        relative = safe_relative_path(item.get("path"))
        if relative in seen:
            raise ConversionError(f"duplicate managed source artifact: {relative}")
        seen.add(relative)
        path = regular_contained_file(root, relative)
        expected_size = item.get("size")
        if not isinstance(expected_size, int) or isinstance(expected_size, bool) or expected_size < 0:
            raise ConversionError(f"invalid managed source size for {relative}")
        if path.stat().st_size != expected_size:
            raise ConversionError(f"managed source size mismatch for {relative}")
        digest = sha256_file(path)
        expected_digest = item.get("sha256")
        if expected_digest is not None:
            if not isinstance(expected_digest, str) or HEX_64.fullmatch(expected_digest) is None:
                raise ConversionError(f"invalid managed source SHA-256 for {relative}")
            if digest != expected_digest:
                raise ConversionError(f"managed source SHA-256 mismatch for {relative}")
        artifacts[relative] = {"size": expected_size, "sha256": digest}

    actual = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and path.name != RECEIPT_NAME
    }
    if actual != seen:
        raise ConversionError(
            f"managed source receipt/file set mismatch: missing={sorted(seen - actual)} "
            f"unexpected={sorted(actual - seen)}"
        )
    for required in ("model.safetensors", *ASSETS):
        if required not in artifacts:
            raise ConversionError(f"managed source omits required artifact {required}")
    return {
        "root": root,
        "receipt_sha256": sha256_file(receipt_path),
        "artifacts": artifacts,
    }


def validate_tool(path: Path, expected_sha256: str, label: str) -> dict[str, Any]:
    if HEX_64.fullmatch(expected_sha256) is None:
        raise ConversionError(f"invalid expected SHA-256 for {label}")
    resolved = path.resolve(strict=True)
    metadata = resolved.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise ConversionError(f"{label} is not a regular file: {resolved}")
    actual = sha256_file(resolved)
    if actual != expected_sha256:
        raise ConversionError(f"{label} SHA-256 mismatch: expected {expected_sha256}, got {actual}")
    return {"path": str(resolved), "sha256": actual, "size": metadata.st_size}


def run_logged(command: list[str], log_path: Path) -> None:
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    write_bytes(log_path, result.stdout.encode(errors="replace"))
    if result.returncode != 0:
        raise ConversionError(f"command failed ({result.returncode}); see {log_path.name}")


def required_decoder_tensors(block_count: int) -> set[str]:
    names = {"cls.output.weight", "output_norm.weight", "token_embd.weight"}
    suffixes = (
        "attn_k.weight",
        "attn_k_norm.weight",
        "attn_norm.weight",
        "attn_output.weight",
        "attn_q.weight",
        "attn_q_norm.weight",
        "attn_v.weight",
        "ffn_down.weight",
        "ffn_gate.weight",
        "ffn_norm.weight",
        "ffn_up.weight",
    )
    names.update(f"blk.{index}.{suffix}" for index in range(block_count) for suffix in suffixes)
    return names


def validate_decoder(path: Path, quantization: str) -> dict[str, Any]:
    reader = open_gguf(path)
    architecture = reader.get_field("general.architecture").contents()
    block_count = reader.get_field("qwen3vl.block_count").contents()
    labels = reader.get_field("qwen3vl.classifier.output_labels").contents()
    if architecture != "qwen3vl" or block_count != 28 or labels != ["yes", "no"]:
        raise ConversionError("decoder GGUF metadata does not match Qwen3-VL-Reranker-2B")
    tensors = {tensor.name: tensor for tensor in reader.tensors}
    if len(tensors) != 311:
        raise ConversionError(f"decoder tensor count mismatch: expected 311, got {len(tensors)}")
    missing = required_decoder_tensors(block_count) - tensors.keys()
    if missing:
        raise ConversionError(f"decoder is missing required tensors: {sorted(missing)}")
    classifier_shape = [int(value) for value in tensors["cls.output.weight"].shape]
    if classifier_shape != [2048, 2]:
        raise ConversionError(f"classifier tensor shape mismatch: {classifier_shape}")
    classifier_type = tensors["cls.output.weight"].tensor_type.name
    if classifier_type != "F16":
        raise ConversionError(
            f"classifier tensor must remain F16, got {classifier_type}"
        )
    tensor_types = sorted({tensor.tensor_type.name for tensor in reader.tensors})
    if quantization == "Q4_K_M":
        if "Q4_K" not in tensor_types or "Q6_K" not in tensor_types:
            raise ConversionError(f"decoder is not a Q4_K_M artifact: {tensor_types}")
    elif quantization == "Q8_0":
        if "Q8_0" not in tensor_types or "Q4_K" in tensor_types or "Q6_K" in tensor_types:
            raise ConversionError(f"decoder is not a Q8_0 artifact: {tensor_types}")
    else:
        raise ConversionError(f"unsupported decoder quantization: {quantization}")
    return {
        "architecture": architecture,
        "block_count": block_count,
        "tensor_count": len(tensors),
        "tensor_types": tensor_types,
        "classifier_shape": classifier_shape,
        "classifier_type": classifier_type,
        "classifier_labels": labels,
        "decoder_quantization": quantization,
    }


def validate_projector(path: Path) -> dict[str, Any]:
    reader = open_gguf(path)
    architecture = reader.get_field("general.architecture").contents()
    artifact_type = reader.get_field("general.type").contents()
    block_count = reader.get_field("clip.vision.block_count").contents()
    tensors = {tensor.name: tensor for tensor in reader.tensors}
    if architecture != "clip" or artifact_type != "mmproj" or block_count != 24:
        raise ConversionError("projector GGUF metadata does not match Qwen3-VL-Reranker-2B")
    if len(tensors) != 316:
        raise ConversionError(f"projector tensor count mismatch: expected 316, got {len(tensors)}")
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
    missing = required - tensors.keys()
    if missing:
        raise ConversionError(f"projector is missing required tensors: {sorted(missing)}")
    tensor_types = sorted({tensor.tensor_type.name for tensor in reader.tensors})
    if "Q8_0" not in tensor_types:
        raise ConversionError(f"projector is not Q8_0: {tensor_types}")
    return {
        "architecture": architecture,
        "artifact_type": artifact_type,
        "block_count": block_count,
        "tensor_count": len(tensors),
        "tensor_types": tensor_types,
    }


def convert_pass(
    source: Path,
    work: Path,
    label: str,
    converter: Path,
    quantizer: Path,
    quantization: str,
) -> dict[str, Path]:
    bf16 = work / f"decoder-{label}-bf16.gguf"
    decoder = work / f"decoder-{label}-{quantization.lower()}.gguf"
    projector = work / f"projector-{label}-q8_0.gguf"
    logs = work / "logs"
    run_logged(
        [sys.executable, str(converter), str(source), "--outtype", "bf16", "--outfile", str(bf16)],
        logs / f"decoder-{label}-conversion.log",
    )
    run_logged(
        [
            str(quantizer),
            "--tensor-type",
            "cls.output.weight=F16",
            str(bf16),
            str(decoder),
            quantization,
        ],
        logs / f"decoder-{label}-quantization.log",
    )
    run_logged(
        [
            sys.executable,
            str(converter),
            str(source),
            "--mmproj",
            "--outtype",
            "q8_0",
            "--outfile",
            str(projector),
        ],
        logs / f"projector-{label}-conversion.log",
    )
    return {"bf16": bf16, "decoder": decoder, "projector": projector}


def artifact_record(path: Path, relative: str) -> dict[str, Any]:
    return {"path": relative, "size": path.stat().st_size, "sha256": sha256_file(path)}


def build_bundle(
    source_dir: Path,
    output_dir: Path,
    converter: Path,
    quantizer: Path,
    converter_sha256: str,
    quantizer_sha256: str,
    quantization: str = "Q8_0",
) -> dict[str, Any]:
    selected_decoder_name = decoder_name(quantization)
    selected_output_identity = output_identity(quantization)
    source = validate_source(source_dir)
    converter_info = validate_tool(converter, converter_sha256, "converter")
    quantizer_info = validate_tool(quantizer, quantizer_sha256, "quantizer")
    destination = output_dir.absolute()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise ConversionError(f"destination already exists: {destination}")

    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent))
    published = False
    try:
        first = convert_pass(source["root"], staging, "a", converter, quantizer, quantization)
        first_digests = {name: sha256_file(path) for name, path in first.items()}
        decoder_contract = validate_decoder(first["decoder"], quantization)
        projector_contract = validate_projector(first["projector"])
        shutil.move(first["decoder"], staging / selected_decoder_name)
        shutil.move(first["projector"], staging / PROJECTOR_NAME)
        first["bf16"].unlink()

        second = convert_pass(source["root"], staging, "b", converter, quantizer, quantization)
        second_digests = {name: sha256_file(path) for name, path in second.items()}
        if first_digests != second_digests:
            raise ConversionError(
                f"conversion is not reproducible: first={first_digests}, second={second_digests}"
            )
        validate_decoder(second["decoder"], quantization)
        validate_projector(second["projector"])
        for path in second.values():
            path.unlink()

        for relative in ASSETS:
            source_path = regular_contained_file(source["root"], relative)
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, target)

        write_bytes(
            staging / "antfly_inference_bundle.json",
            json_bytes(
                {
                    "family": "qwen3_vl_reranker_gguf_bundle/v1",
                    "model": selected_decoder_name,
                    "mmproj": PROJECTOR_NAME,
                }
            ),
        )
        write_bytes(
            staging / "model_manifest.json",
            json_bytes(
                {
                    "type": "reranker",
                    "inputs": ["text", "image"],
                    "capabilities": ["qwen3vl", "multimodal", "generative_yes_no"],
                }
            ),
        )
        report = {
            "schema": SCHEMA,
            "pass": True,
            "reproducible": True,
            "decoder_quantization": quantization,
            "source": {
                "identity": SOURCE_IDENTITY,
                "receipt_sha256": source["receipt_sha256"],
                "model_safetensors": source["artifacts"]["model.safetensors"],
            },
            "tools": {"converter": converter_info, "quantizer": quantizer_info},
            "outputs": {
                "decoder": artifact_record(staging / selected_decoder_name, selected_decoder_name),
                "projector": artifact_record(staging / PROJECTOR_NAME, PROJECTOR_NAME),
            },
            "reproduction_digests": {"first": first_digests, "second": second_digests},
            "contracts": {"decoder": decoder_contract, "projector": projector_contract},
        }
        write_bytes(staging / "conversion-report.json", json_bytes(report))

        receipt_artifacts = []
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                relative = str(path.relative_to(staging))
                receipt_artifacts.append(artifact_record(path, relative))
        write_bytes(
            staging / RECEIPT_NAME,
            json_bytes({"version": 2, "source": selected_output_identity, "artifacts": receipt_artifacts}),
        )
        os.rename(staging, destination)
        published = True
        report["managed_receipt_sha256"] = sha256_file(destination / RECEIPT_NAME)
        return report
    finally:
        if not published:
            shutil.rmtree(staging, ignore_errors=True)


def validate_published_bundle(model_dir: Path, quantization: str = "Q8_0") -> dict[str, Any]:
    """Revalidate a published conversion without trusting its report alone."""

    root = model_dir.resolve(strict=True)
    if not root.is_dir():
        raise ConversionError(f"published bundle is not a directory: {root}")
    for marker in BLOCKING_MARKERS:
        if (root / marker).exists():
            raise ConversionError(f"published bundle is incomplete: {marker}")

    receipt_path = regular_contained_file(root, RECEIPT_NAME)
    receipt = load_object(receipt_path)
    if receipt.get("version") != 2 or receipt.get("source") != output_identity(quantization):
        raise ConversionError("published bundle receipt identity mismatch")
    raw_artifacts = receipt.get("artifacts")
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise ConversionError("published bundle receipt has no artifacts")

    artifacts: dict[str, dict[str, Any]] = {}
    for item in raw_artifacts:
        if not isinstance(item, dict):
            raise ConversionError("published bundle artifact is not an object")
        relative = safe_relative_path(item.get("path"))
        if relative in artifacts:
            raise ConversionError(f"duplicate published artifact: {relative}")
        path = regular_contained_file(root, relative)
        expected_size = item.get("size")
        expected_sha = item.get("sha256")
        if (
            not isinstance(expected_size, int)
            or isinstance(expected_size, bool)
            or expected_size < 0
            or path.stat().st_size != expected_size
        ):
            raise ConversionError(f"published artifact size mismatch for {relative}")
        if not isinstance(expected_sha, str) or HEX_64.fullmatch(expected_sha) is None:
            raise ConversionError(f"published artifact lacks a valid SHA-256: {relative}")
        actual_sha = sha256_file(path)
        if actual_sha != expected_sha:
            raise ConversionError(f"published artifact SHA-256 mismatch for {relative}")
        artifacts[relative] = {"size": expected_size, "sha256": actual_sha}

    actual = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and path.name != RECEIPT_NAME
    }
    if actual != set(artifacts):
        raise ConversionError(
            f"published receipt/file set mismatch: missing={sorted(set(artifacts) - actual)} "
            f"unexpected={sorted(actual - set(artifacts))}"
        )

    selected_decoder = decoder_name(quantization)
    required = {
        selected_decoder,
        PROJECTOR_NAME,
        "conversion-report.json",
        "antfly_inference_bundle.json",
        "model_manifest.json",
        *ASSETS,
    }
    if missing := required - artifacts.keys():
        raise ConversionError(f"published bundle omits required artifacts: {sorted(missing)}")

    bundle = load_object(root / "antfly_inference_bundle.json")
    if bundle != {
        "family": "qwen3_vl_reranker_gguf_bundle/v1",
        "model": selected_decoder,
        "mmproj": PROJECTOR_NAME,
    }:
        raise ConversionError(f"published inference bundle contract mismatch: {bundle!r}")
    manifest = load_object(root / "model_manifest.json")
    if (
        manifest.get("type") != "reranker"
        or manifest.get("inputs") != ["text", "image"]
        or manifest.get("capabilities") != ["qwen3vl", "multimodal", "generative_yes_no"]
    ):
        raise ConversionError(f"published model manifest contract mismatch: {manifest!r}")

    report = load_object(root / "conversion-report.json")
    if (
        report.get("schema") != SCHEMA
        or report.get("pass") is not True
        or report.get("reproducible") is not True
        or report.get("decoder_quantization") != quantization
    ):
        raise ConversionError("published conversion report contract mismatch")
    if report.get("source", {}).get("identity") != SOURCE_IDENTITY:
        raise ConversionError("published conversion source identity mismatch")
    tools = report.get("tools", {})
    if (
        tools.get("converter", {}).get("sha256") != DEFAULT_CONVERTER_SHA256
        or tools.get("quantizer", {}).get("sha256") != DEFAULT_QUANTIZER_SHA256
    ):
        raise ConversionError("published conversion tool identity mismatch")
    outputs = report.get("outputs", {})
    for kind, relative in (("decoder", selected_decoder), ("projector", PROJECTOR_NAME)):
        if outputs.get(kind) != {"path": relative, **artifacts[relative]}:
            raise ConversionError(f"published conversion output identity mismatch for {kind}")
    reproduction = report.get("reproduction_digests", {})
    if reproduction.get("first") != reproduction.get("second"):
        raise ConversionError("published conversion digest passes do not match")

    decoder_contract = validate_decoder(root / selected_decoder, quantization)
    projector_contract = validate_projector(root / PROJECTOR_NAME)
    if report.get("contracts") != {
        "decoder": decoder_contract,
        "projector": projector_contract,
    }:
        raise ConversionError("published tensor contracts do not match live GGUF inspection")
    return {
        "model_dir": str(root),
        "managed_receipt_sha256": sha256_file(receipt_path),
        "source": receipt["source"],
        "decoder_quantization": quantization,
        "decoder": {"path": str(root / selected_decoder), **artifacts[selected_decoder]},
        "projector": {"path": str(root / PROJECTOR_NAME), **artifacts[PROJECTOR_NAME]},
        "conversion_report_sha256": artifacts["conversion-report.json"]["sha256"],
        "contracts": report["contracts"],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--converter", type=Path, required=True)
    parser.add_argument("--quantizer", type=Path, required=True)
    parser.add_argument("--converter-sha256", default=DEFAULT_CONVERTER_SHA256)
    parser.add_argument("--quantizer-sha256", default=DEFAULT_QUANTIZER_SHA256)
    parser.add_argument(
        "--decoder-quantization",
        choices=DECODER_QUANTIZATIONS,
        default="Q8_0",
        help="Q8_0 is the calibrated production tier; Q4_K_M is ranking-only",
    )
    parser.add_argument("--report", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = build_bundle(
            args.source_dir,
            args.output_dir,
            args.converter,
            args.quantizer,
            args.converter_sha256,
            args.quantizer_sha256,
            args.decoder_quantization,
        )
        if args.report is not None:
            temporary = args.report.with_name(f".{args.report.name}.{os.getpid()}.tmp")
            write_bytes(temporary, json_bytes(report))
            os.replace(temporary, args.report)
        print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except (ConversionError, OSError, subprocess.SubprocessError) as exc:
        print(f"Qwen3-VL reranker conversion failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
