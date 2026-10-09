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

"""Verify a local GLiNER2.5 release against a pinned architecture contract.

The default check reads only JSON and the safetensors header.  Pass
``--verify-model-sha256`` when a release/promotion job must also stream and
hash the complete weights file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import struct
from pathlib import Path
from typing import Any


CONTRACT_PATH = Path(__file__).with_name("family_contract.json")
MAX_HEADER_BYTES = 16 * 1024 * 1024
MAX_SIDECAR_BYTES = 32 * 1024 * 1024


class ContractError(ValueError):
    pass


def strict_json(path: Path) -> Any:
    def duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ContractError(f"{path}: duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=duplicate,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ContractError(f"{path}: invalid JSON constant {value}")
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ContractError(f"could not read {path}: {exc}") from exc


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_sidecar(path: Path, expected: dict[str, Any]) -> dict[str, Any]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ContractError(f"could not stat {path}: {exc}") from exc
    expected_size = expected.get("size_bytes")
    expected_sha256 = expected.get("sha256")
    if (
        type(expected_size) is not int
        or not 0 < expected_size <= MAX_SIDECAR_BYTES
        or not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
    ):
        raise ContractError(f"invalid sidecar contract for {path.name}")
    if size != expected_size:
        raise ContractError(f"sidecar size differs: {path}")
    digest = sha256_file(path)
    if digest != expected_sha256:
        raise ContractError(f"sidecar SHA-256 differs: {path}")
    return {"size_bytes": size, "sha256": digest}


def safetensors_header(path: Path) -> tuple[dict[str, Any], bytes, int]:
    try:
        size = path.stat().st_size
        with path.open("rb") as source:
            prefix = source.read(8)
            if len(prefix) != 8:
                raise ContractError(f"{path}: truncated safetensors length")
            length = struct.unpack("<Q", prefix)[0]
            if not 2 <= length <= MAX_HEADER_BYTES or 8 + length > size:
                raise ContractError(
                    f"{path}: invalid safetensors header length {length}"
                )
            raw = source.read(length)
    except OSError as exc:
        raise ContractError(f"could not read {path}: {exc}") from exc
    if len(raw) != length:
        raise ContractError(f"{path}: truncated safetensors header")
    try:
        header = json.loads(raw, object_pairs_hook=lambda pairs: _unique(pairs, path))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ContractError(f"{path}: invalid safetensors JSON: {exc}") from exc
    if not isinstance(header, dict):
        raise ContractError(f"{path}: safetensors header is not an object")
    header.pop("__metadata__", None)
    return header, raw, size


def _unique(pairs: list[tuple[str, Any]], path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError(f"{path}: duplicate safetensors key {key!r}")
        result[key] = value
    return result


def _require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{name} must be an object")
    return value


def _same_json(actual: Any, expected: Any) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _same_json(actual[key], value) for key, value in expected.items()
        )
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _same_json(left, right) for left, right in zip(actual, expected)
        )
    return actual == expected


def _require_fields(
    actual: dict[str, Any], expected: dict[str, Any], name: str
) -> None:
    for key, value in expected.items():
        if key not in actual:
            raise ContractError(f"{name}.{key} is missing")
        if not _same_json(actual[key], value):
            raise ContractError(f"{name}.{key}={actual[key]!r}, expected {value!r}")


def verify_model(
    profile: str,
    directory: Path,
    *,
    contract_path: Path = CONTRACT_PATH,
    verify_model_sha256: bool = False,
) -> dict[str, Any]:
    contract = _require_mapping(strict_json(contract_path), "contract")
    if contract.get("format_version") != 1:
        raise ContractError("unsupported family contract version")
    models = _require_mapping(contract.get("models"), "contract.models")
    if profile not in models:
        raise ContractError(
            f"unknown profile {profile!r}; choose one of {sorted(models)}"
        )
    expected = _require_mapping(models[profile], f"contract.models.{profile}")

    directory = directory.expanduser().resolve()
    sidecars = _require_mapping(expected.get("sidecars"), "model.sidecars")
    required_sidecars = {
        "config.json",
        "encoder_config/config.json",
        "tokenizer.json",
        "tokenizer_config.json",
    }
    if set(sidecars) != required_sidecars:
        raise ContractError("model sidecar contract is incomplete")
    verified_sidecars = {
        relative: verify_sidecar(directory / relative, _require_mapping(spec, relative))
        for relative, spec in sidecars.items()
    }
    config = _require_mapping(strict_json(directory / "config.json"), "config")
    encoder = _require_mapping(
        strict_json(directory / "encoder_config" / "config.json"), "encoder_config"
    )
    tokenizer = _require_mapping(
        strict_json(directory / "tokenizer_config.json"), "tokenizer_config"
    )
    _require_fields(
        config,
        {
            "architecture": expected["architecture"],
            "architectures": [expected["architecture_class"]],
            "config_version": 3,
            "model_name": expected["model_name"],
            "model_type": "extractor",
            **expected["top_level"],
        },
        "config",
    )
    _require_fields(encoder, expected["encoder"], "encoder_config")
    _require_fields(tokenizer, expected["tokenizer"], "tokenizer_config")

    weights = directory / "model.safetensors"
    tensors, raw_header, model_size = safetensors_header(weights)
    if model_size != expected["model_size_bytes"]:
        raise ContractError(
            f"model.safetensors size={model_size}, expected {expected['model_size_bytes']}"
        )
    if len(raw_header) != expected["tensor_header_length"]:
        raise ContractError("safetensors header length differs from pinned release")
    header_sha256 = hashlib.sha256(raw_header).hexdigest()
    if header_sha256 != expected["tensor_header_sha256"]:
        raise ContractError("safetensors tensor schema differs from pinned release")
    if len(tensors) != expected["tensor_count"]:
        raise ContractError("safetensors tensor count differs from pinned release")

    dtype_counts: dict[str, int] = {}
    parameter_count = 0
    for name, descriptor in tensors.items():
        if not isinstance(name, str) or not isinstance(descriptor, dict):
            raise ContractError("invalid safetensors tensor descriptor")
        dtype = descriptor.get("dtype")
        shape = descriptor.get("shape")
        offsets = descriptor.get("data_offsets")
        if (
            not isinstance(dtype, str)
            or not isinstance(shape, list)
            or not shape
            or any(type(dim) is not int or dim <= 0 for dim in shape)
            or not isinstance(offsets, list)
            or len(offsets) != 2
            or any(type(offset) is not int or offset < 0 for offset in offsets)
            or offsets[0] > offsets[1]
        ):
            raise ContractError(f"invalid safetensors descriptor for {name}")
        dtype_counts[dtype] = dtype_counts.get(dtype, 0) + 1
        parameter_count += math.prod(shape)
    if dtype_counts != {expected["dtype"]: expected["tensor_count"]}:
        raise ContractError(f"safetensors dtypes differ: {dtype_counts}")
    if parameter_count != expected["parameter_count"]:
        raise ContractError(
            f"parameter count={parameter_count}, expected {expected['parameter_count']}"
        )
    for name, shape in expected["critical_tensors"].items():
        if name not in tensors or tensors[name].get("shape") != shape:
            raise ContractError(f"critical tensor geometry differs: {name}")

    model_sha256 = None
    if verify_model_sha256:
        model_sha256 = sha256_file(weights)
        if model_sha256 != expected["model_sha256"]:
            raise ContractError("model.safetensors SHA-256 differs from pinned release")
    return {
        "status": "verified",
        "qualification": False,
        "profile": profile,
        "repo": expected["repo"],
        "revision": expected["revision"],
        "architecture": expected["architecture"],
        "encoder_family": expected["encoder"]["model_type"],
        "tensor_count": len(tensors),
        "parameter_count": parameter_count,
        "model_size_bytes": model_size,
        "tensor_header_sha256": header_sha256,
        "model_sha256": model_sha256,
        "sidecars": verified_sidecars,
        "compatibility": expected["compatibility"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--model-dir", required=True, type=Path)
    parser.add_argument("--contract", type=Path, default=CONTRACT_PATH)
    parser.add_argument("--verify-model-sha256", action="store_true")
    args = parser.parse_args()
    result = verify_model(
        args.profile,
        args.model_dir,
        contract_path=args.contract,
        verify_model_sha256=args.verify_model_sha256,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
