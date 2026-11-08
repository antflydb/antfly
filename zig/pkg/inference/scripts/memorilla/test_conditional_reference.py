# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Conditional provenance/cast gates without model downloads or GPU work."""

import hashlib
import json
from pathlib import Path

import pytest
import torch

from assets import LOCK_PATH
from conditional_reference import (
    ENCODER_MANIFEST,
    ENCODER_PROFILE,
    conditional_inputs,
    execution_identity,
    load_native_query,
    verify_decoder,
)
from feature_inputs import load_encoder_features
import test_feature_inputs

encoder_reference = test_feature_inputs.encoder_reference


@pytest.fixture
def native_query(tmp_path: Path, encoder_reference: Path) -> Path:
    receipt = json.loads((encoder_reference / "encoder.json").read_text())
    question = next(
        case["rendered"] for case in receipt["cases"] if case["role"] == "question"
    )
    normalized = torch.zeros(2560)
    normalized[0], normalized[1] = 0.6, 0.8
    stored = normalized.half()
    path = tmp_path / "native-query.json"
    path.write_text(
        json.dumps(
            {
                "schema": "antfly.memorilla.native-query.v1",
                "question_text": question,
                "stored_fp16": stored.float().tolist(),
                "memory_bf16_f32": stored.bfloat16().float().tolist(),
                "producer": {
                    "profile": ENCODER_PROFILE,
                    "publication": "1" * 64,
                    "identity": execution_identity(),
                    "source_lock_sha256": hashlib.sha256(
                        LOCK_PATH.read_bytes()
                    ).hexdigest(),
                    "recipe_sha256": hashlib.sha256(ENCODER_MANIFEST).hexdigest(),
                },
            }
        )
    )
    return path


def test_conditional_query_changes_only_the_exact_query_row(
    encoder_reference, native_query
):
    original, question, _ = load_encoder_features(
        encoder_reference, {"embedding_dim": 2560}
    )
    inputs, actual_question, provenance = conditional_inputs(
        {"embedding_dim": 2560}, encoder_reference, native_query
    )
    expected, binding = load_native_query(native_query, expected_question=question)
    assert actual_question == question
    assert torch.equal(
        inputs.documents.view(torch.int32), original.documents.view(torch.int32)
    )
    assert torch.equal(inputs.padding_mask, original.padding_mask)
    assert torch.equal(inputs.question.view(torch.int32), expected.view(torch.int32))
    assert not torch.equal(inputs.question, original.question)
    assert provenance["native_encoder_features_used"] is True
    assert provenance["native_document_features_used"] is False
    assert binding["encoder_independently_qualified_here"] is False
    assert binding["cuda_execution_attested_here"] is False


@pytest.mark.parametrize(
    "name", ["model.safetensors", "pytorch_model.bin", "adapter_config.json"]
)
def test_unlocked_shadow_decoder_weights_are_rejected_before_loading(tmp_path, name):
    (tmp_path / name).write_bytes(b"unlocked")
    with pytest.raises(ValueError, match="shadow"):
        verify_decoder(tmp_path)


def test_production_identity_matches_the_frozen_native_profile():
    assert (
        execution_identity()
        == "e3d6de85a3204759c8d575bfe109aba3ec1a49318ae01a88c71515415e0ce91a"
    )
    assert (
        hashlib.sha256(ENCODER_MANIFEST).hexdigest()
        == "494f7e6793aef818a4f24c2fd37867f1ce50c50e69f79e8ca92036d221926735"
    )


@pytest.mark.parametrize(
    "field",
    ["identity", "source_lock_sha256", "recipe_sha256", "publication", "profile"],
)
def test_mismatched_native_producer_is_rejected(native_query, field):
    receipt = json.loads(native_query.read_text())
    question = receipt["question_text"]
    receipt["producer"][field] = "0" * 64
    native_query.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="producer"):
        load_native_query(native_query, expected_question=question)


@pytest.mark.parametrize(
    "mutation", ["shape", "bool", "nan", "nonhalf", "wrong_bf16", "zero_norm", "extra"]
)
def test_native_query_values_are_not_silently_recast_or_repaired(
    native_query, mutation
):
    receipt = json.loads(native_query.read_text())
    question = receipt["question_text"]
    if mutation == "shape":
        receipt["stored_fp16"].pop()
    elif mutation == "bool":
        receipt["stored_fp16"][0] = True
    elif mutation == "nan":
        receipt["memory_bf16_f32"][0] = float("nan")
    elif mutation == "nonhalf":
        receipt["stored_fp16"][0] = 0.601
    elif mutation == "wrong_bf16":
        receipt["memory_bf16_f32"][0] = 0.5
    elif mutation == "zero_norm":
        receipt["stored_fp16"] = receipt["memory_bf16_f32"] = [0.0] * 2560
    else:
        receipt["extra"] = "unbound metadata"
    native_query.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):
        load_native_query(native_query, expected_question=question)


@pytest.mark.parametrize(
    "question", ["different question", "\u3000padded\u00a0", "", "<|memory|>"]
)
def test_conditional_question_is_bound_and_already_canonical(native_query, question):
    receipt = json.loads(native_query.read_text())
    expected = receipt["question_text"]
    receipt["question_text"] = question
    native_query.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="canonical question"):
        load_native_query(native_query, expected_question=expected)
