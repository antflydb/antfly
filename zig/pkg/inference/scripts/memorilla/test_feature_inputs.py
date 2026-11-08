# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Integrity and feature-chain checks without encoder weights or downloads."""

from dataclasses import asdict
import json
from pathlib import Path

import pytest
import torch
from safetensors.torch import load_file, save_file

from assets import lock_model
from encoder_reference import SCHEMA, WIDTH, default_cases
from exporter import sha256
from feature_inputs import FEATURE_SCHEMA, RECIPE, load_encoder_features


CONFIG = {"embedding_dim": WIDTH}


@pytest.fixture
def encoder_reference(tmp_path: Path) -> Path:
    directory = tmp_path / "encoder-reference"
    directory.mkdir()
    source = lock_model("encoder")
    tokenizer_fixture = json.loads(
        Path(__file__)
        .with_name("fixtures")
        .joinpath("encoder-tokenizer.json")
        .read_text()
    )
    cases = [
        dict(asdict(case), rendered=case.rendered(), token_ids=fixture["token_ids"])
        for case, fixture in zip(
            default_cases(), tokenizer_fixture["cases"], strict=True
        )
    ]
    values = torch.arange(1, len(cases) * WIDTH + 1, dtype=torch.float32).reshape(
        len(cases), WIDTH
    )
    normalized = torch.nn.functional.normalize(values, dim=1)
    stored = normalized.half()
    memory = stored.bfloat16()
    tensor_path = directory / "encoder.safetensors"
    save_file(
        {
            "reference.normalized": normalized,
            "reference.singleton": normalized.clone(),
            "stored.fp16": stored,
            "memory.bf16": memory,
        },
        tensor_path,
    )
    receipt = {
        "schema": SCHEMA,
        "model": source["model"],
        "revision": source["revision"],
        "verified_artifacts": source["files"],
        "reference": {
            "engine": "transformers-cpu",
            "dtype": "float32",
            "normalization_dtype": "float32",
            "checkpoint": "bfloat16",
            "attention": "eager",
            "threads": 1,
            "upstream_vllm_gpu_attested": False,
        },
        "recipe": dict(RECIPE),
        "cases": cases,
        "reference_batch_singleton": {"passed": True},
        "passed": True,
        "tensor_file": {"path": tensor_path.name, "sha256": sha256(tensor_path)},
    }
    (directory / "encoder.json").write_text(json.dumps(receipt))
    return directory


def modify_receipt(directory: Path, mutate) -> None:
    path = directory / "encoder.json"
    receipt = json.loads(path.read_text())
    mutate(receipt)
    path.write_text(json.dumps(receipt))


def modify_tensors(directory: Path, mutate) -> None:
    path = directory / "encoder.safetensors"
    tensors = {key: value.clone() for key, value in load_file(path).items()}
    mutate(tensors)
    save_file({key: value.contiguous() for key, value in tensors.items()}, path)
    modify_receipt(
        directory,
        lambda value: value["tensor_file"].__setitem__("sha256", sha256(path)),
    )


def test_real_feature_rows_and_prompt_question_are_bound(
    encoder_reference: Path,
) -> None:
    inputs, question, provenance = load_encoder_features(encoder_reference, CONFIG)
    original = load_file(encoder_reference / "encoder.safetensors")["memory.bf16"]
    assert inputs.documents.shape == (1, 2, WIDTH)
    assert inputs.question.shape == (1, WIDTH)
    assert inputs.documents.dtype == inputs.question.dtype == torch.float32
    assert inputs.padding_mask.dtype == torch.bool
    assert inputs.padding_mask.shape == (1, 2)
    assert not inputs.padding_mask.any().item()
    torch.testing.assert_close(
        inputs.documents[0], original[[1, 2]].float(), rtol=0, atol=0
    )
    torch.testing.assert_close(inputs.question, original[3:4].float(), rtol=0, atol=0)
    assert question == default_cases()[-1].text.strip()
    assert provenance["rendered_question"] == question
    assert provenance["question"]["rendered"] == question
    assert provenance["selected_document_rows"] == [1, 2]
    assert provenance["selected_question_row"] == 3
    assert provenance["omitted_document_case_ids"] == ["empty_document"]
    assert [item["id"] for item in provenance["documents"]] == [
        "short_document",
        "unicode_document",
    ]
    assert provenance["schema"] == FEATURE_SCHEMA
    assert provenance["feature_source"] == "encoder-reference"
    assert provenance["native_encoder_features_used"] is False
    assert provenance["encoder_reference_sha256"] == sha256(
        encoder_reference / "encoder.json"
    )
    assert provenance["encoder_tensor_sha256"] == sha256(
        encoder_reference / "encoder.safetensors"
    )
    inputs.documents.zero_()
    assert (
        sha256(encoder_reference / "encoder.safetensors")
        == provenance["encoder_tensor_sha256"]
    )


@pytest.mark.parametrize(
    "config", ({}, {"embedding_dim": 1024}, {"embedding_dim": True})
)
def test_memory_width_must_match_the_encoder(
    encoder_reference: Path, config: dict
) -> None:
    with pytest.raises(ValueError, match="memory width"):
        load_encoder_features(encoder_reference, config)


@pytest.mark.parametrize("field", ("schema", "model", "revision"))
def test_model_identity_is_pinned(encoder_reference: Path, field: str) -> None:
    modify_receipt(encoder_reference, lambda value: value.__setitem__(field, "changed"))
    with pytest.raises(ValueError, match="pinned model"):
        load_encoder_features(encoder_reference, CONFIG)


def test_all_artifact_hashes_are_pinned(encoder_reference: Path) -> None:
    modify_receipt(
        encoder_reference,
        lambda value: value["verified_artifacts"][0].__setitem__("size_bytes", 1),
    )
    with pytest.raises(ValueError, match="pinned model"):
        load_encoder_features(encoder_reference, CONFIG)


@pytest.mark.parametrize(
    "field,value",
    (
        ("question_prefix", "Instruct: retrieve"),
        ("document_prefix", "Document:"),
        ("pooling", "mean"),
        ("normalize", False),
        ("dimensions", 1024),
        ("trailing_eos_id", 151645),
        ("storage_dtype", "float32"),
        ("question_whitespace", "preserve"),
    ),
)
def test_encoder_recipe_drift_is_rejected(
    encoder_reference: Path, field: str, value
) -> None:
    modify_receipt(
        encoder_reference, lambda receipt: receipt["recipe"].__setitem__(field, value)
    )
    with pytest.raises(ValueError, match="pinned memory recipe"):
        load_encoder_features(encoder_reference, CONFIG)


@pytest.mark.parametrize(
    "field,value",
    (
        ("engine", "remote"),
        ("dtype", "float16"),
        ("normalization_dtype", "bfloat16"),
        ("attention", "unknown"),
        ("checkpoint", "float32"),
        ("upstream_vllm_gpu_attested", True),
    ),
)
def test_reference_execution_scope_is_checked(
    encoder_reference: Path, field: str, value
) -> None:
    modify_receipt(
        encoder_reference,
        lambda receipt: receipt["reference"].__setitem__(field, value),
    )
    with pytest.raises(ValueError, match="reference execution"):
        load_encoder_features(encoder_reference, CONFIG)


@pytest.mark.parametrize("target", ("passed", "reference_batch_singleton"))
def test_failed_reference_cannot_enter_the_feature_chain(
    encoder_reference: Path, target: str
) -> None:
    def alter(receipt):
        if target == "passed":
            receipt[target] = False
        else:
            receipt[target]["passed"] = False

    modify_receipt(encoder_reference, alter)
    with pytest.raises(ValueError, match="did not pass"):
        load_encoder_features(encoder_reference, CONFIG)


def test_tensor_bytes_are_verified_before_loading(encoder_reference: Path) -> None:
    path = encoder_reference / "encoder.safetensors"
    with path.open("ab") as stream:
        stream.write(b"altered")
    with pytest.raises(ValueError, match="SHA-256"):
        load_encoder_features(encoder_reference, CONFIG)


@pytest.mark.parametrize(
    "path",
    ("../encoder.safetensors", "/tmp/encoder.safetensors", "./encoder.safetensors"),
)
def test_tensor_reference_is_a_fixed_local_file(
    encoder_reference: Path, path: str
) -> None:
    modify_receipt(
        encoder_reference, lambda value: value["tensor_file"].__setitem__("path", path)
    )
    with pytest.raises(ValueError, match="fixed tensor artifact"):
        load_encoder_features(encoder_reference, CONFIG)


@pytest.mark.parametrize(
    "change",
    (
        "unknown_role",
        "duplicate_id",
        "missing_question",
        "extra_question",
        "empty_question",
        "empty_documents",
    ),
)
def test_case_roles_and_nonempty_collection_are_validated(
    encoder_reference: Path, change: str
) -> None:
    def alter(receipt):
        cases = receipt["cases"]
        if change == "unknown_role":
            cases[0]["role"] = "query"
        elif change == "duplicate_id":
            cases[1]["id"] = cases[0]["id"]
        elif change == "missing_question":
            cases[3]["role"] = "document"
            cases[3]["rendered"] = cases[3]["text"]
        elif change == "extra_question":
            cases[1]["role"] = "question"
        elif change == "empty_question":
            cases[3]["text"], cases[3]["rendered"] = "  ", ""
        elif change == "empty_documents":
            for case in cases:
                if case["role"] == "document":
                    case["text"] = case["rendered"] = ""

    modify_receipt(encoder_reference, alter)
    with pytest.raises(ValueError):
        load_encoder_features(encoder_reference, CONFIG)


@pytest.mark.parametrize("row", (1, 3))
def test_rendered_question_or_document_cannot_diverge_from_source(
    encoder_reference: Path, row: int
) -> None:
    modify_receipt(
        encoder_reference,
        lambda receipt: receipt["cases"][row].__setitem__(
            "rendered", "unrelated prompt"
        ),
    )
    with pytest.raises(ValueError, match="rendered text"):
        load_encoder_features(encoder_reference, CONFIG)


@pytest.mark.parametrize(
    "ids", ([], [True, 151643], [-1, 151643], [1, 151645], [1] * 256 + [151643])
)
def test_token_rows_must_keep_the_pinned_eos_recipe(
    encoder_reference: Path, ids: list
) -> None:
    modify_receipt(
        encoder_reference,
        lambda receipt: receipt["cases"][1].__setitem__("token_ids", ids),
    )
    with pytest.raises(ValueError, match="token IDs"):
        load_encoder_features(encoder_reference, CONFIG)


@pytest.mark.parametrize("name", ("reference.normalized", "stored.fp16", "memory.bf16"))
def test_nonfinite_features_are_rejected_even_for_omitted_rows(
    encoder_reference: Path, name: str
) -> None:
    def alter(tensors):
        tensors[name][0, 0] = float("nan")

    modify_tensors(encoder_reference, alter)
    with pytest.raises(ValueError, match="finite"):
        load_encoder_features(encoder_reference, CONFIG)


@pytest.mark.parametrize("change", ("width", "rows", "dtype", "missing"))
def test_geometry_and_tensor_types_are_checked(
    encoder_reference: Path, change: str
) -> None:
    def alter(tensors):
        if change == "width":
            tensors["memory.bf16"] = tensors["memory.bf16"][:, :1024]
        elif change == "rows":
            tensors["memory.bf16"] = tensors["memory.bf16"][:3]
        elif change == "dtype":
            tensors["memory.bf16"] = tensors["memory.bf16"].float()
        else:
            del tensors["memory.bf16"]

    modify_tensors(encoder_reference, alter)
    with pytest.raises(ValueError, match="must have dtype"):
        load_encoder_features(encoder_reference, CONFIG)


def test_storage_cast_cannot_be_shortcut(encoder_reference: Path) -> None:
    def alter(tensors):
        direct = tensors["reference.normalized"].bfloat16()
        assert not torch.equal(direct, tensors["memory.bf16"])
        tensors["memory.bf16"] = direct

    modify_tensors(encoder_reference, alter)
    with pytest.raises(ValueError, match="storage to BF16"):
        load_encoder_features(encoder_reference, CONFIG)


def test_bf16_reference_features_keep_the_same_storage_contract(
    encoder_reference: Path,
) -> None:
    def alter(tensors):
        normalized = tensors["reference.normalized"].bfloat16().float()
        tensors["reference.normalized"] = normalized
        tensors["stored.fp16"] = normalized.half()
        tensors["memory.bf16"] = normalized.half().bfloat16()

    modify_tensors(encoder_reference, alter)
    modify_receipt(
        encoder_reference,
        lambda receipt: receipt["reference"].update(
            dtype="bfloat16", normalization_dtype="bfloat16"
        ),
    )
    inputs, _, provenance = load_encoder_features(encoder_reference, CONFIG)
    assert inputs.documents.dtype == torch.float32
    assert provenance["reference_execution"]["dtype"] == "bfloat16"


def test_zero_or_unnormalized_features_are_rejected(encoder_reference: Path) -> None:
    def alter(tensors):
        tensors["reference.normalized"][0].zero_()

    modify_tensors(encoder_reference, alter)
    with pytest.raises(ValueError, match="L2 normalization"):
        load_encoder_features(encoder_reference, CONFIG)
