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

"""Bind real, pinned encoder reference features to a memory/decoder input.

No model or source downloads occur. The selected features come from the
independent encoder reference, not an optional native comparison tensor.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import torch
from safetensors.torch import load_file

from assets import lock_model
from encoder_reference import EOS, SCHEMA, WIDTH
from exporter import read_json, sha256
from reference import Inputs, validate_inputs


FEATURE_SCHEMA = "antfly.memorilla.feature-inputs.v1"
RECIPE = {
    "document_prefix": "",
    "question_prefix": "",
    "question_whitespace": "strip-for-dataset-cache",
    "trailing_eos_id": EOS,
    "pooling": "last",
    "normalize": "l2",
    "dimensions": WIDTH,
    "storage_dtype": "float16",
    "memory_dtype": "bfloat16",
}


def _bit_equal(actual: torch.Tensor, expected: torch.Tensor) -> bool:
    return (
        actual.dtype == expected.dtype
        and actual.shape == expected.shape
        and torch.equal(
            actual.contiguous().view(torch.uint8),
            expected.contiguous().view(torch.uint8),
        )
    )


def _validate_cases(value: Any) -> tuple[list[dict], list[int], int]:
    if not isinstance(value, list) or not 1 <= len(value) <= 64:
        raise ValueError("encoder reference must contain between one and 64 cases")
    seen: set[str] = set()
    documents: list[int] = []
    questions: list[int] = []
    for index, case in enumerate(value):
        if not isinstance(case, dict) or set(case) != {
            "id",
            "role",
            "text",
            "rendered",
            "token_ids",
        }:
            raise ValueError("invalid encoder case metadata")
        identity, role, text, rendered, ids = (
            case[key] for key in ("id", "role", "text", "rendered", "token_ids")
        )
        if (
            not isinstance(identity, str)
            or not identity
            or len(identity) > 256
            or identity in seen
        ):
            raise ValueError("encoder case IDs must be nonempty and unique")
        seen.add(identity)
        if (
            role not in ("document", "question")
            or not isinstance(text, str)
            or not isinstance(rendered, str)
        ):
            raise ValueError("encoder cases require document/question roles and text")
        expected = text if role == "document" else text.strip()
        if rendered != expected:
            raise ValueError(
                "encoder rendered text differs from its role's prefix/whitespace recipe"
            )
        if (
            not isinstance(ids, list)
            or not 1 <= len(ids) <= 256
            or any(type(token) is not int or token < 0 for token in ids)
            or ids[-1] != EOS
        ):
            raise ValueError(
                "encoder token IDs must be bounded integer rows ending in the pinned EOS"
            )
        if role == "question":
            questions.append(index)
        elif rendered.strip():
            documents.append(index)
    if len(questions) != 1 or not value[questions[0]]["rendered"]:
        raise ValueError(
            "feature chain requires exactly one nonempty rendered question"
        )
    if not documents:
        raise ValueError("feature chain requires at least one nonempty document")
    return value, documents, questions[0]


def load_encoder_features(
    reference_dir: Path, config: dict
) -> tuple[Inputs, str, dict]:
    """Return F32 views of exact BF16-valued document/question features.

    The returned question is the encoder's rendered text and must also be used
    for the decoder prompt. Provenance binds its source row and the artifact
    bytes, avoiding a silently unrelated question/memory pairing.
    """
    if type(config.get("embedding_dim")) is not int or config["embedding_dim"] != WIDTH:
        raise ValueError(f"real Qwen encoder features require memory width {WIDTH}")
    reference_dir = Path(reference_dir)
    receipt_path = reference_dir / "encoder.json"
    if receipt_path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError("encoder reference receipt exceeds the input budget")
    receipt = read_json(receipt_path)
    lock = lock_model("encoder")
    if (
        receipt.get("schema") != SCHEMA
        or receipt.get("model") != lock["model"]
        or receipt.get("revision") != lock["revision"]
        or receipt.get("verified_artifacts") != lock["files"]
    ):
        raise ValueError(
            "encoder reference differs from the pinned model/artifact inventory"
        )
    if receipt.get("recipe") != RECIPE:
        raise ValueError("encoder reference differs from the pinned memory recipe")
    reference = receipt.get("reference")
    if not isinstance(reference, dict) or (
        reference.get("engine") != "transformers-cpu"
        or reference.get("dtype") not in ("float32", "bfloat16")
        or reference.get("normalization_dtype") != reference.get("dtype")
        or reference.get("checkpoint") != "bfloat16"
        or reference.get("attention") != "eager"
        or reference.get("upstream_vllm_gpu_attested") is not False
    ):
        raise ValueError("unsupported encoder reference execution recipe")
    batch_comparison = receipt.get("reference_batch_singleton")
    if (
        receipt.get("passed") is not True
        or not isinstance(batch_comparison, dict)
        or batch_comparison.get("passed") is not True
    ):
        raise ValueError("encoder reference numerical checks did not pass")
    cases, document_rows, question_row = _validate_cases(receipt.get("cases"))
    tensor_file = receipt.get("tensor_file")
    if (
        not isinstance(tensor_file, dict)
        or set(tensor_file) != {"path", "sha256"}
        or tensor_file.get("path") != "encoder.safetensors"
    ):
        raise ValueError("encoder reference must bind the fixed tensor artifact")
    expected_sha = tensor_file.get("sha256")
    if (
        not isinstance(expected_sha, str)
        or len(expected_sha) != 64
        or any(c not in "0123456789abcdef" for c in expected_sha)
    ):
        raise ValueError("invalid encoder tensor SHA-256")
    tensor_path = reference_dir / "encoder.safetensors"
    if (
        tensor_path.stat().st_size > 64 * 1024 * 1024
        or sha256(tensor_path) != expected_sha
    ):
        raise ValueError("encoder tensor artifact SHA-256 or size mismatch")
    tensors = load_file(tensor_path, device="cpu")
    shape = (len(cases), WIDTH)
    expected_dtypes = {
        "reference.normalized": torch.float32,
        "stored.fp16": torch.float16,
        "memory.bf16": torch.bfloat16,
    }
    for name, dtype in expected_dtypes.items():
        value = tensors.get(name)
        if (
            not isinstance(value, torch.Tensor)
            or value.dtype != dtype
            or tuple(value.shape) != shape
        ):
            raise ValueError(
                f"encoder tensor {name} must have dtype {dtype} and shape {shape}"
            )
        if not torch.isfinite(value).all().item():
            raise ValueError(f"encoder tensor {name} must be finite")
    normalized, stored, memory = (tensors[name] for name in expected_dtypes)
    norms = normalized.norm(dim=1)
    norm_tolerance = 1e-5 if reference["dtype"] == "float32" else 0.01
    if (
        torch.any(norms == 0).item()
        or torch.any((norms - 1).abs() > norm_tolerance).item()
    ):
        raise ValueError("encoder features must preserve the declared L2 normalization")
    if reference["dtype"] == "bfloat16" and not _bit_equal(
        normalized, normalized.bfloat16().float()
    ):
        raise ValueError(
            "encoder normalized features differ from their BF16 reference precision"
        )
    if not _bit_equal(stored, normalized.to(torch.float16)) or not _bit_equal(
        memory, stored.to(torch.bfloat16)
    ):
        raise ValueError(
            "encoder features do not preserve FP16 storage to BF16 memory casts"
        )
    documents = memory[document_rows].float().unsqueeze(0).contiguous().clone()
    question = memory[question_row : question_row + 1].float().contiguous().clone()
    inputs = Inputs(
        documents, torch.zeros((1, len(document_rows)), dtype=torch.bool), question
    )
    validate_inputs(inputs, WIDTH)
    rendered_question = cases[question_row]["rendered"]
    provenance = {
        "schema": FEATURE_SCHEMA,
        "feature_source": "encoder-reference",
        "encoder_reference_sha256": sha256(receipt_path),
        "encoder_tensor_sha256": expected_sha,
        "encoder_model": lock["model"],
        "encoder_revision": lock["revision"],
        "verified_artifacts": lock["files"],
        "recipe": dict(RECIPE),
        "reference_execution": dict(reference),
        "feature_tensor": "memory.bf16",
        "feature_precision": "FP16 storage -> BF16 module input -> widened F32",
        "native_encoder_features_used": False,
        "selected_document_rows": document_rows,
        "selected_question_row": question_row,
        "documents": [dict(cases[index]) for index in document_rows],
        "question": dict(cases[question_row]),
        "omitted_document_case_ids": [
            case["id"]
            for index, case in enumerate(cases)
            if case["role"] == "document" and index not in document_rows
        ],
        "rendered_question": rendered_question,
    }
    return inputs, rendered_question, provenance
