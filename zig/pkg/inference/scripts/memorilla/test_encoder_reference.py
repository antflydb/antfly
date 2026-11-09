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

"""Bounded numerical and recipe checks without model or network dependencies."""

import hashlib
import json
from pathlib import Path

import pytest
import torch

from encoder_reference import (
    EOS,
    NATIVE_MANIFEST,
    NATIVE_MANIFEST_SHA256,
    WIDTH,
    Case,
    active_ids,
    compare_vectors,
    default_cases,
    generate_reference,
    install_native_manifest,
    last_token_pool,
    normalize_features,
    storage_features,
    validate_native_response,
    verify_memory_recipe,
    verify_tokenizer_cases,
)


def unit_vectors(rows: int = 2) -> torch.Tensor:
    values = torch.arange(1, rows * WIDTH + 1, dtype=torch.float32).reshape(rows, WIDTH)
    return torch.nn.functional.normalize(values, dim=1)


def response() -> dict:
    return {
        "schema": "antfly.embedding.diagnostics.v1",
        "backend": "cuda",
        "text_prefix": "",
        "trailing_eos_id": EOS,
        "pooling": "last",
        "normalize": True,
        "token_ids": [[3, EOS], [4, 5, EOS]],
        "embeddings": unit_vectors().tolist(),
        "cuda_execution": {
            "scope": "text-forward-after-asset-init",
            "host_output_readback_allowed": True,
            "resident_weight_bytes_before": 1024,
            "resident_weight_bytes_after": 1024,
            "counters": {
                "kernel_launches": 120,
                "linear_kernel_launches": 40,
                "linear_qkv_kernel_launches": 0,
                "bf16_cublaslt_linear_calls": 0,
                "bf16_cublaslt_qkv_calls": 0,
                "f16_cublaslt_linear_calls": 0,
                "f16_cublaslt_qkv_calls": 0,
                "attention_kernel_launches": 20,
                "d2h_bytes": 20480,
                "host_attention_fallbacks": 0,
                "paged_attention_host_fallbacks": 0,
                "rope_host_fallbacks": 0,
                "cross_backend_sync_fallbacks": 0,
            },
        },
    }


def test_no_prefix_rendering_preserves_documents_and_strips_cached_questions() -> None:
    assert Case("doc", "document", "  source  ").rendered() == "  source  "
    assert Case("q", "question", "  question  ").rendered() == "question"
    with pytest.raises(ValueError, match="role"):
        Case("bad", "query", "text").rendered()
    assert len({case.id for case in default_cases()}) == len(default_cases())


def test_last_active_pooling_handles_left_and_right_padding() -> None:
    hidden = torch.arange(2 * 4 * 3).reshape(2, 4, 3).float()
    mask = torch.tensor([[0, 0, 1, 1], [1, 1, 0, 0]])
    pooled = last_token_pool(hidden, mask)
    torch.testing.assert_close(pooled, torch.stack([hidden[0, 3], hidden[1, 1]]))


@pytest.mark.parametrize(
    "mask", [torch.tensor([[0, 0]]), torch.tensor([[2, 1]]), torch.tensor([[-1, 1]])]
)
def test_invalid_pooling_masks_fail_closed(mask: torch.Tensor) -> None:
    with pytest.raises(ValueError, match="binary mask"):
        last_token_pool(torch.ones(1, 2, WIDTH), mask)


def test_native_token_ids_compare_only_active_positions() -> None:
    encoded = {
        "input_ids": torch.tensor([[EOS, 3, EOS], [4, 5, EOS]]),
        "attention_mask": torch.tensor([[0, 1, 1], [1, 1, 1]]),
    }
    assert active_ids(encoded) == [[3, EOS], [4, 5, EOS]]
    encoded["input_ids"][1, -1] = 151645
    with pytest.raises(ValueError, match="tokenizer EOS"):
        active_ids(encoded)


def test_normalization_uses_full_width_and_original_dtype() -> None:
    hidden = torch.ones(1, 3, WIDTH, dtype=torch.bfloat16)
    normalized = normalize_features(hidden, torch.tensor([[0, 1, 1]]))
    assert normalized.shape == (1, WIDTH)
    assert normalized.dtype == torch.bfloat16
    assert abs(normalized.float().norm().item() - 1) < 0.005
    with pytest.raises(ValueError, match="full-width"):
        normalize_features(hidden[:, :, :1024], torch.tensor([[0, 1, 1]]))
    with pytest.raises(ValueError, match="zero pooled"):
        normalize_features(torch.zeros_like(hidden), torch.tensor([[0, 1, 1]]))


def test_fp16_storage_cast_is_not_shortcut_to_bf16() -> None:
    vector = torch.full((1, WIDTH), 1.0039064)
    stored, memory = storage_features(vector)
    assert stored.dtype == torch.float16
    assert memory.dtype == torch.bfloat16
    assert memory[0, 0].item() == 1.0
    assert vector.to(torch.bfloat16)[0, 0].item() != memory[0, 0].item()


def test_numeric_gate_checks_component_error_norm_and_cosine() -> None:
    expected = unit_vectors()
    assert compare_vectors(expected, expected, atol=1e-5, min_cosine=0.99999)["passed"]
    # Positive rescaling has identical cosine, but still violates fidelity.
    assert not compare_vectors(expected * 2, expected, atol=1e-5, min_cosine=0.99999)[
        "passed"
    ]
    with pytest.raises(ValueError, match="zero comparison"):
        compare_vectors(
            torch.zeros_like(expected), expected, atol=1e-5, min_cosine=0.99999
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("backend", "native"),
        ("text_prefix", "Instruct: retrieve\nQuery:"),
        ("trailing_eos_id", 151645),
        ("pooling", "mean"),
        ("normalize", False),
    ],
)
def test_native_diagnostics_reject_fallback_and_recipe_drift(field: str, value) -> None:
    payload = response()
    payload[field] = value
    with pytest.raises(ValueError):
        validate_native_response(payload, [[3, EOS], [4, 5, EOS]], "cuda")


def test_native_diagnostics_reject_token_order_and_width_changes() -> None:
    payload = response()
    assert validate_native_response(payload, [[3, EOS], [4, 5, EOS]], "cuda").shape == (
        2,
        WIDTH,
    )
    with pytest.raises(ValueError, match="token IDs"):
        validate_native_response(payload, [[4, 5, EOS], [3, EOS]], "cuda")
    payload["embeddings"] = unit_vectors()[:, :1024].tolist()
    with pytest.raises(ValueError, match="full-width"):
        validate_native_response(payload, [[3, EOS], [4, 5, EOS]], "cuda")


@pytest.mark.parametrize(
    "field,value",
    [
        ("kernel_launches", 0),
        ("attention_kernel_launches", 0),
        ("linear_kernel_launches", 0),
        ("host_attention_fallbacks", 1),
        ("paged_attention_host_fallbacks", 1),
        ("rope_host_fallbacks", 1),
        ("cross_backend_sync_fallbacks", 1),
        ("host_attention_fallbacks", False),
        ("d2h_bytes", -1),
    ],
)
def test_cuda_diagnostics_require_forward_work_and_zero_fallback(field, value):
    payload = response()
    payload["cuda_execution"]["counters"][field] = value
    with pytest.raises(ValueError, match="CUDA execution evidence"):
        validate_native_response(payload, [[3, EOS], [4, 5, EOS]], "cuda")


@pytest.mark.parametrize(
    "mutation", ("missing", "counter_missing", "scope", "resident")
)
def test_cuda_backend_label_alone_cannot_qualify_execution(mutation):
    payload = response()
    if mutation == "missing":
        del payload["cuda_execution"]
    elif mutation == "counter_missing":
        del payload["cuda_execution"]["counters"]["rope_host_fallbacks"]
    elif mutation == "scope":
        payload["cuda_execution"]["scope"] = "model-initialization-and-forward"
    else:
        payload["cuda_execution"]["resident_weight_bytes_before"] = 0
    with pytest.raises(ValueError, match="CUDA execution evidence"):
        validate_native_response(payload, [[3, EOS], [4, 5, EOS]], "cuda")


def test_cuda_diagnostics_allow_instrumented_cublas_linear_and_output_readback():
    payload = response()
    payload["cuda_execution"]["counters"].update(
        linear_kernel_launches=0, bf16_cublaslt_linear_calls=10
    )
    assert validate_native_response(payload, [[3, EOS], [4, 5, EOS]], "cuda").shape == (
        2,
        WIDTH,
    )


def strict_response():
    from assets import lock_model, LOCK_PATH

    payload = response()
    lock = lock_model("encoder")
    payload.update(
        scope="offline-strict-f32-encoder-reference",
        arithmetic="BF16-source-weights-widened-F32; F32-activations",
        model=lock["model"],
        revision=lock["revision"],
        base_model_lock_sha256=hashlib.sha256(LOCK_PATH.read_bytes()).hexdigest(),
    )
    payload["cuda_execution"].update(
        strict_f32_weights=True,
        resident_weights=398,
        low_precision_activation_staging_calls=0,
        low_precision_matmul_calls=0,
    )
    return payload


def test_strict_f32_encoder_scope_accepts_verified_precision_and_identity():
    assert validate_native_response(
        strict_response(), [[3, EOS], [4, 5, EOS]], "cuda"
    ).shape == (2, WIDTH)


@pytest.mark.parametrize(
    "field,value",
    [
        ("arithmetic", "mixed"),
        ("revision", "changed"),
        ("model", "Qwen/Qwen3-8B"),
        ("base_model_lock_sha256", "0" * 64),
    ],
)
def test_strict_f32_encoder_scope_rejects_identity_drift(field, value):
    payload = strict_response()
    payload[field] = value
    with pytest.raises(ValueError, match="strict-F32 encoder"):
        validate_native_response(payload, [[3, EOS], [4, 5, EOS]], "cuda")


@pytest.mark.parametrize(
    "field,value",
    [
        ("strict_f32_weights", False),
        ("resident_weights", 0),
        ("low_precision_activation_staging_calls", 1),
        ("low_precision_matmul_calls", 1),
    ],
)
def test_strict_f32_encoder_scope_rejects_low_precision_execution(field, value):
    payload = strict_response()
    payload["cuda_execution"][field] = value
    with pytest.raises(ValueError, match="strict-F32 encoder"):
        validate_native_response(payload, [[3, EOS], [4, 5, EOS]], "cuda")


def test_native_recipe_publication_matches_catalog_and_rejects_replacement(
    tmp_path: Path,
) -> None:
    assert hashlib.sha256(NATIVE_MANIFEST).hexdigest() == NATIVE_MANIFEST_SHA256
    install_native_manifest(tmp_path)
    verify_memory_recipe(tmp_path)
    install_native_manifest(tmp_path)
    manifest = tmp_path / "model_manifest.json"
    altered = json.loads(manifest.read_text())
    altered["embedding_profile"]["query"]["prefix"] = "wrong"
    manifest.write_text(json.dumps(altered))
    with pytest.raises(ValueError, match="existing native"):
        install_native_manifest(tmp_path)
    with pytest.raises(ValueError, match="pinned Memorilla"):
        verify_memory_recipe(tmp_path)
    assert json.loads(manifest.read_text()) == altered


def test_reference_cases_require_the_hash_pinned_tokenizer_fixture() -> None:
    fixture = json.loads(
        Path(__file__)
        .with_name("fixtures")
        .joinpath("encoder-tokenizer.json")
        .read_text()
    )
    ids = [case["token_ids"] for case in fixture["cases"]]
    assert [len(row) for row in ids] == [1, 8, 31, 11]
    verify_tokenizer_cases(default_cases(), ids)
    ids[-1][-1] = 151645
    with pytest.raises(ValueError, match="pinned tokenizer fixture"):
        verify_tokenizer_cases(default_cases(), ids)


def test_cuda_comparison_requires_an_explicit_native_executable(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires --native-binary"):
        generate_reference(tmp_path / "missing-model", tmp_path / "out", backend="cuda")
