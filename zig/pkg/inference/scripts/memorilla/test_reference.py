# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Numerical and provenance checks for the CPU module reference oracle."""

import json
import os
from pathlib import Path

import pytest
import torch
from safetensors.torch import load_file

from exporter import export_checkpoint, load_bundle
from reference import (
    Inputs,
    ReferenceMemory,
    deterministic_cpu,
    fixture_inputs,
    generate_fixtures,
    load_upstream,
    pack_attention_weights,
    sha256,
    validate_inputs,
)


CONFIG = {
    "embedding_dim": 8,
    "output_dim": 12,
    "num_memories": 16,
    "num_heads": 2,
    "num_self_attn_layers": 0,
    "num_cross_attn_layers": 2,
    "dropout": 0.1,
    "retrieval_init": False,
}


def synthetic_state() -> dict[str, torch.Tensor]:
    generator = torch.Generator().manual_seed(781)
    model = ReferenceMemory(CONFIG)
    with torch.no_grad():
        for name, value in model.named_parameters():
            value.copy_(torch.randn(value.shape, generator=generator) * 0.2)
            if "norm" in name and name.endswith("weight"):
                value.add_(1.0)
    return model.state_dict()


@pytest.fixture
def bundle(tmp_path: Path) -> Path:
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir()
    (checkpoint / "config.json").write_text(json.dumps(CONFIG))
    torch.save(synthetic_state(), checkpoint / "memory.pt")
    output = tmp_path / "bundle"
    export_checkpoint(checkpoint, output, release=False)
    return output


def model_from_bundle(bundle: Path) -> ReferenceMemory:
    config, weights, _ = load_bundle(bundle)
    model = ReferenceMemory(config)
    model.load_state_dict(pack_attention_weights(weights), strict=True)
    return model.eval()


def test_split_qkv_round_trip(bundle: Path) -> None:
    config, weights, _ = load_bundle(bundle)
    packed = pack_attention_weights(weights)
    model = ReferenceMemory(config)
    model.load_state_dict(packed, strict=True)
    for prefix in ("query_cond_attn", "cross_attn_layers.0", "cross_attn_layers.1"):
        for index, part in enumerate(("q", "k", "v")):
            for kind in ("weight", "bias"):
                expected = weights[f"{prefix}.{part}_proj.{kind}"]
                actual = packed[f"{prefix}.in_proj_{kind}"].chunk(3, dim=0)[index]
                torch.testing.assert_close(actual, expected, rtol=0, atol=0)


def test_packed_attention_rejects_missing_bias(bundle: Path) -> None:
    _, weights, _ = load_bundle(bundle)
    del weights["cross_attn_layers.0.v_proj.bias"]
    with pytest.raises(ValueError, match="incomplete split attention"):
        pack_attention_weights(weights)


def test_padding_and_permutation_do_not_change_memory(bundle: Path) -> None:
    model = model_from_bundle(bundle)
    cases = fixture_inputs(8, torch.float32, 23)
    with deterministic_cpu(), torch.inference_mode():
        baseline = model.trace(cases["ragged"])["memory_tokens"]
        for case in ("masked_padding_changed", "document_permutation"):
            torch.testing.assert_close(
                model.trace(cases[case])["memory_tokens"],
                baseline,
                rtol=1e-5,
                atol=1e-5,
            )


def test_distinct_duplicate_and_question_changes_affect_readout(bundle: Path) -> None:
    model = model_from_bundle(bundle)
    cases = fixture_inputs(8, torch.float32, 23)
    with deterministic_cpu(), torch.inference_mode():
        baseline = model.trace(cases["ragged"])["memory_tokens"]
        duplicate = model.trace(cases["duplicate_document"])["memory_tokens"]
        changed_query = model.trace(cases["question_changed"])["memory_tokens"]
    assert (duplicate - baseline[:1]).abs().max().item() > 1e-5
    assert (changed_query - baseline).abs().max().item() > 1e-5


def test_empty_collections_rejected_before_attention(bundle: Path) -> None:
    model = model_from_bundle(bundle)
    cases = fixture_inputs(8, torch.float32, 23)
    for documents, mask in (
        (torch.empty(1, 0, 8), torch.empty(1, 0, dtype=torch.bool)),
        (torch.zeros(1, 1, 8), torch.ones(1, 1, dtype=torch.bool)),
        (cases["ragged"].documents, torch.tensor([[False] * 5, [True] * 5])),
    ):
        question = cases["ragged"].question[: documents.shape[0]]
        with pytest.raises(ValueError, match="closed-book fallback"):
            model.trace(Inputs(documents, mask, question))


@pytest.mark.parametrize("field", ("documents", "question"))
def test_nonfinite_values_rejected_even_when_masked(field: str) -> None:
    inputs = fixture_inputs(8, torch.float32, 23)["ragged"]
    documents, question = inputs.documents.clone(), inputs.question.clone()
    if field == "documents":
        documents[1, 4, 0] = float("nan")
    else:
        question[0, 0] = float("inf")
    with pytest.raises(ValueError, match="must be finite"):
        validate_inputs(Inputs(documents, inputs.padding_mask, question), 8)


def test_training_mode_rejected(bundle: Path) -> None:
    model = model_from_bundle(bundle).train()
    with pytest.raises(ValueError, match="eval mode"):
        model.trace(fixture_inputs(8, torch.float32, 23)["single_document"])


def test_inputs_use_private_rng_and_storage_cast() -> None:
    before = torch.get_rng_state().clone()
    inputs = fixture_inputs(8, torch.float32, 23)
    assert torch.equal(torch.get_rng_state(), before)
    repeated = fixture_inputs(8, torch.float32, 23)
    for name, values in inputs.items():
        for key, value in values.tensors().items():
            torch.testing.assert_close(
                value, repeated[name].tensors()[key], rtol=0, atol=0
            )
        torch.testing.assert_close(
            values.documents, values.documents.half().float(), rtol=0, atol=0
        )
        torch.testing.assert_close(
            values.documents, values.documents.bfloat16().float(), rtol=0, atol=0
        )
    bf16_inputs = fixture_inputs(8, torch.bfloat16, 23)
    for name, values in inputs.items():
        for key, value in values.tensors().items():
            torch.testing.assert_close(
                value, bf16_inputs[name].tensors()[key].to(value.dtype), rtol=0, atol=0
            )


@pytest.mark.parametrize("dtype", ("float32", "bfloat16"))
def test_fixture_receipt_and_tensors_are_reproducible(
    bundle: Path, tmp_path: Path, dtype: str
) -> None:
    first, second = tmp_path / "first", tmp_path / "second"
    before_threads = torch.get_num_threads()
    before_fastpath = torch.backends.mha.get_fastpath_enabled()
    receipt = generate_fixtures(bundle, first, dtype=dtype)
    repeated = generate_fixtures(bundle, second, dtype=dtype)
    assert receipt == repeated
    assert (first / "reference.safetensors").read_bytes() == (
        second / "reference.safetensors"
    ).read_bytes()
    assert (first / "reference.json").read_bytes() == (
        second / "reference.json"
    ).read_bytes()
    assert receipt["native_parity_verified"] is False
    assert receipt["upstream_forward_compared"] is False
    assert receipt["empty_collection"]["attention_executed"] is False
    assert receipt["decoder"]["status"] == "not_run"
    assert receipt["bundle_manifest_sha256"] == sha256(bundle / "manifest.json")
    assert receipt["tensors"]["sha256"] == sha256(first / "reference.safetensors")
    tensors = load_file(first / "reference.safetensors")
    assert set(tensors) == set(receipt["tensors"]["inventory"])
    for name, tensor in tensors.items():
        assert list(tensor.shape) == receipt["tensors"]["inventory"][name]["shape"]
        assert str(tensor.dtype) == receipt["tensors"]["inventory"][name]["dtype"]
    assert tensors["single_document.memory_tokens"].shape == (1, 16, 12)
    assert tensors["ragged.block1.ffn.activated"].shape == (2, 16, 32)
    assert torch.get_num_threads() == before_threads
    assert torch.backends.mha.get_fastpath_enabled() == before_fastpath


def test_nonempty_output_rejected(bundle: Path, tmp_path: Path) -> None:
    output = tmp_path / "occupied"
    output.mkdir()
    marker = output / "keep"
    marker.write_text("existing fixture")
    with pytest.raises(ValueError, match="must not already exist"):
        generate_fixtures(bundle, output)
    assert marker.read_text() == "existing fixture"


def test_bundle_tampering_rejected_before_fixture_publication(
    bundle: Path, tmp_path: Path
) -> None:
    with (bundle / "memory.safetensors").open("ab") as stream:
        stream.write(b"tampered")
    output = tmp_path / "not_published"
    with pytest.raises(ValueError):
        generate_fixtures(bundle, output)
    assert not output.exists()


def test_pinned_source_change_rejected(tmp_path: Path) -> None:
    source = tmp_path / "memorilla"
    source.mkdir()
    (source / "memory.py").write_text("raise RuntimeError('must not execute')")
    with pytest.raises(ValueError, match="pinned source"):
        load_upstream(tmp_path, CONFIG, synthetic_state())


@pytest.mark.skipif(
    not os.environ.get("MEMORILLA_SOURCE_ROOT"),
    reason="optional pinned source checkout",
)
@pytest.mark.parametrize("dtype", ("float32", "bfloat16"))
def test_against_unmodified_pinned_upstream_forward(
    bundle: Path, tmp_path: Path, dtype: str
) -> None:
    receipt = generate_fixtures(
        bundle,
        tmp_path / dtype,
        dtype=dtype,
        upstream_source=Path(os.environ["MEMORILLA_SOURCE_ROOT"]),
    )
    assert receipt["upstream_forward_compared"] is True
