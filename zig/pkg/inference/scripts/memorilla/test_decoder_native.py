# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Optional real decoder boundary checks with a tiny independent Torch oracle.

Set MEMORILLA_QWEN_VERIFY_BIN to the compiled Qwen decoder verifier. These
checks need no downloaded weights and do not admit the tiny model on CUDA.
"""

import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest
import torch
from safetensors.torch import load_file, save_file
from transformers import Qwen3Config, Qwen3ForCausalLM

from decoder_reference import teacher_force


pytestmark = pytest.mark.skipif(
    not os.environ.get("MEMORILLA_QWEN_VERIFY_BIN"),
    reason="optional compiled soft-prompt decoder verifier",
)


def bind_fixture(directory: Path, *, kv_dtype="f32") -> None:
    path = directory / "reference.safetensors"
    receipt = {
        "schema": "antfly.memorilla.decoder-reference.v1",
        "synthetic": True,
        "precision": (
            "F32-source-weights; F32-activations-and-KV"
            if kv_dtype == "f32"
            else "F32-source-weights; F32-activations; FP16-KV-roundtrip-F32"
        ),
        "files": {
            path.name: {
                "size_bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        },
    }
    (directory / "reference.json").write_text(json.dumps(receipt))


@pytest.fixture
def invocation(tmp_path: Path) -> list[str]:
    torch.manual_seed(71)
    torch.set_num_threads(2)
    config = Qwen3Config(
        vocab_size=64,
        hidden_size=32,
        intermediate_size=64,
        num_hidden_layers=2,
        num_attention_heads=4,
        num_key_value_heads=2,
        head_dim=8,
        max_position_embeddings=128,
        rope_theta=10000.0,
        attention_dropout=0.0,
        attention_bias=False,
        tie_word_embeddings=False,
        bos_token_id=1,
        eos_token_id=2,
        pad_token_id=0,
    )
    config._attn_implementation = "eager"
    model = Qwen3ForCausalLM(config).float().eval()
    model.save_pretrained(tmp_path / "model", safe_serialization=True)
    (tmp_path / "model" / "model_manifest.json").write_text('{"type":"generator"}')
    ids = torch.tensor([1, 4, 9, 30, 11], dtype=torch.int64)
    with torch.inference_mode():
        embeddings = model.get_input_embeddings()(ids).clone()
        embeddings[1:3] = torch.randn_like(embeddings[1:3]) * 0.2
        prefill, decode_ids, decode_logits = teacher_force(model, embeddings, 3)
    save_file(
        {
            "input_ids": ids,
            "input_embeddings": embeddings.contiguous(),
            "prefill_logits": prefill,
            "decode_input_ids": decode_ids,
            "decode_logits": decode_logits,
        },
        tmp_path / "reference.safetensors",
    )
    bind_fixture(tmp_path)
    return [
        str(Path(os.environ["MEMORILLA_QWEN_VERIFY_BIN"]).resolve()),
        "--model",
        str(tmp_path / "model"),
        "--fixture",
        str(tmp_path / "reference.safetensors"),
        "--output",
        str(tmp_path / "report.json"),
        "--backend",
        "native",
        "--allow-synthetic",
    ]


def execute(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args, capture_output=True, text=True, timeout=120, check=False
    )


def test_real_qwen_soft_prefill_and_cached_decode_match_torch(invocation):
    result = execute(invocation)
    assert result.returncode == 0, result.stderr
    report = json.loads(Path(invocation[invocation.index("--output") + 1]).read_text())
    assert report["passed"] is True
    assert report["backend"] == "native"
    assert report["synthetic_model"] is True
    assert report["source_tokens"] == 5
    assert report["decode_steps"] == 3
    assert report["prefill"]["failed"] == 0
    assert all(step["failed"] == 0 for step in report["decode"])


def test_changed_fixture_checksum_rejected_before_model_execution(invocation):
    path = Path(invocation[invocation.index("--fixture") + 1])
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    result = execute(invocation)
    assert result.returncode != 0
    assert "DecoderReferenceArtifactMismatch" in result.stderr


def test_bf16_activation_receipt_cannot_claim_f32_parity(invocation):
    path = Path(invocation[invocation.index("--fixture") + 1]).parent / "reference.json"
    receipt = json.loads(path.read_text())
    receipt["precision"] = "bfloat16"
    path.write_text(json.dumps(receipt))
    result = execute(invocation)
    assert result.returncode != 0
    assert "InvalidDecoderReferencePrecision" in result.stderr


def test_wrong_expected_logits_fail_numeric_conformance(invocation):
    path = Path(invocation[invocation.index("--fixture") + 1])
    tensors = {name: value.clone() for name, value in load_file(path).items()}
    tensors["prefill_logits"][0] += 0.1
    save_file(tensors, path)
    bind_fixture(path.parent)
    result = execute(invocation)
    assert result.returncode != 0
    assert "DecoderConformanceFailed" in result.stderr


def test_synthetic_model_requires_explicit_opt_in(invocation):
    invocation.remove("--allow-synthetic")
    result = execute(invocation)
    assert result.returncode != 0
    assert "SyntheticDecoderReferenceNotAllowed" in result.stderr


def release_receipt(invocation):
    path = Path(invocation[invocation.index("--fixture") + 1]).parent / "reference.json"
    receipt = json.loads(path.read_text())
    lock_path = Path(__file__).with_name("base_models.json")
    receipt["synthetic"] = False
    receipt["precision"] = "BF16-source-weights-widened-F32; F32-activations-and-KV"
    receipt["model"] = json.loads(lock_path.read_text())["models"]["decoder"]
    receipt["base_model_lock_sha256"] = hashlib.sha256(
        lock_path.read_bytes()
    ).hexdigest()
    return path, receipt


def test_allow_synthetic_flag_does_not_bypass_release_revision(invocation):
    path, receipt = release_receipt(invocation)
    receipt["model"]["revision"] = "0" * 40
    path.write_text(json.dumps(receipt))
    result = execute(invocation)
    assert result.returncode != 0
    assert "DecoderModelIdentityMismatch" in result.stderr


def test_same_family_model_cannot_claim_released_weight_identity(invocation):
    path, receipt = release_receipt(invocation)
    path.write_text(json.dumps(receipt))
    result = execute(invocation)
    assert result.returncode != 0
    assert "DecoderModelArtifactMismatch" in result.stderr


@pytest.fixture
def f16_invocation(invocation):
    path = Path(invocation[invocation.index("--fixture") + 1])
    tensors = {name: value.clone() for name, value in load_file(path).items()}
    model = Qwen3ForCausalLM.from_pretrained(
        invocation[invocation.index("--model") + 1],
        local_files_only=True,
        torch_dtype=torch.float32,
        attn_implementation="eager",
    ).eval()
    prefill, ids, logits = teacher_force(
        model, tensors["input_embeddings"], 3, kv_dtype="f16"
    )
    tensors.update(prefill_logits=prefill, decode_input_ids=ids, decode_logits=logits)
    save_file(tensors, path)
    bind_fixture(path.parent, kv_dtype="f16")
    return invocation


def test_f16_reference_rejected_for_f32_native_cache(f16_invocation):
    result = execute(f16_invocation)
    assert result.returncode != 0
    assert "InvalidDecoderReferencePrecision" in result.stderr


def test_f16_reference_matches_native_f16_cache(f16_invocation):
    result = execute([*f16_invocation, "--kv-dtype", "f16"])
    assert result.returncode == 0, result.stderr
    path = Path(f16_invocation[f16_invocation.index("--output") + 1])
    report = json.loads(path.read_text())
    assert report["passed"] is True
    assert report["kv_dtype"] == "f16"
    assert report["reference_precision"] == (
        "F32-source-weights; F32-activations; FP16-KV-roundtrip-F32"
    )
