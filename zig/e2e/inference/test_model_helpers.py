# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

import json

from . import models
from .models import (
    DEFAULT_GENERATOR_MODEL,
    DEFAULT_MULTIMODAL_GENERATOR_MODEL,
    DEFAULT_TOOL_GENERATOR_MODEL,
    _env_model_specs,
    _looks_like_model_dir,
    bootstrap_models_for_listing,
)


def test_partial_model_directory_is_not_available(tmp_path):
    model_dir = tmp_path / "BAAI" / "bge-small-en-v1.5"
    (model_dir / "onnx").mkdir(parents=True)
    (model_dir / "config.json").write_text("{}")
    (model_dir / "onnx" / "model.onnx.part").write_bytes(b"incomplete")

    assert not _looks_like_model_dir(model_dir)


def test_completed_nested_model_payload_is_available(tmp_path):
    model_dir = tmp_path / "BAAI" / "bge-small-en-v1.5"
    (model_dir / "onnx").mkdir(parents=True)
    (model_dir / "config.json").write_text("{}")
    (model_dir / "onnx" / "model.onnx").write_bytes(b"complete")

    assert _looks_like_model_dir(model_dir)


def test_unsupported_framework_bin_is_not_a_completed_model(tmp_path):
    model_dir = tmp_path / "owner" / "framework-only"
    model_dir.mkdir(parents=True)
    (model_dir / "config.json").write_text("{}")
    (model_dir / "pytorch_model.bin").write_bytes(b"unsupported")

    assert not _looks_like_model_dir(model_dir)


def test_partial_file_invalidates_an_otherwise_complete_model(tmp_path):
    model_dir = tmp_path / "owner" / "interrupted"
    model_dir.mkdir(parents=True)
    (model_dir / "model.safetensors").write_bytes(b"complete")
    (model_dir / "adapter_model.safetensors.part").write_bytes(b"incomplete")

    assert not _looks_like_model_dir(model_dir)


def test_managed_download_in_progress_invalidates_completed_first_shard(tmp_path):
    model_dir = tmp_path / "owner" / "interrupted-between-files"
    model_dir.mkdir(parents=True)
    (model_dir / "model-00001-of-00002.safetensors").write_bytes(b"first")
    (model_dir / ".antfly-download-in-progress").write_text('{"version":1,"state":"in_progress"}')

    assert not _looks_like_model_dir(model_dir)


def test_managed_completion_receipt_requires_every_artifact(tmp_path):
    model_dir = tmp_path / "owner" / "missing-shard"
    model_dir.mkdir(parents=True)
    (model_dir / "model-00001-of-00002.safetensors").write_bytes(b"first")
    receipt = {
        "version": 1,
        "artifacts": [
            {"path": "model-00001-of-00002.safetensors", "size": 5},
            {"path": "model-00002-of-00002.safetensors", "size": 6},
        ],
    }
    (model_dir / ".antfly-download-complete.json").write_text(json.dumps(receipt))

    assert not _looks_like_model_dir(model_dir)


def test_managed_completion_receipt_accepts_complete_artifact_set(tmp_path):
    model_dir = tmp_path / "owner" / "complete-shards"
    model_dir.mkdir(parents=True)
    (model_dir / "model-00001-of-00002.safetensors").write_bytes(b"first")
    (model_dir / "model-00002-of-00002.safetensors").write_bytes(b"second")
    receipt = {
        "version": 1,
        "artifacts": [
            {"path": "model-00001-of-00002.safetensors", "size": 5},
            {"path": "model-00002-of-00002.safetensors", "size": 6},
        ],
    }
    (model_dir / ".antfly-download-complete.json").write_text(json.dumps(receipt))

    assert _looks_like_model_dir(model_dir)


def test_managed_completion_receipt_rejects_boolean_numeric_fields(tmp_path):
    model_dir = tmp_path / "owner" / "invalid-numeric-types"
    model_dir.mkdir(parents=True)
    (model_dir / "model.onnx").write_bytes(b"x")
    receipt = {
        "version": True,
        "artifacts": [{"path": "model.onnx", "size": True}],
    }
    (model_dir / ".antfly-download-complete.json").write_text(json.dumps(receipt))

    assert not _looks_like_model_dir(model_dir)


def test_reader_environment_override_preserves_curated_variant(monkeypatch):
    monkeypatch.setenv("ANTFLY_INFERENCE_FLORENCE_MODEL", "antflydb/florence-2-base")

    specs = _env_model_specs()
    florence = next(spec for spec in specs if spec.repo == "antflydb/florence-2-base")

    assert florence.pull_ref == "hf:antflydb/florence-2-base:gguf:Q4_K"


def test_generator_environment_override_preserves_curated_variant(monkeypatch):
    monkeypatch.setenv("ANTFLY_INFERENCE_DEFAULT_GENERATOR_MODEL", "ggml-org/gemma-4-e2b-it-gguf")

    specs = _env_model_specs()
    gemma = next(spec for spec in specs if spec.repo == "ggml-org/gemma-4-e2b-it-gguf")

    assert gemma.pull_ref == "hf:ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0"

    monkeypatch.setenv("ANTFLY_INFERENCE_DEFAULT_GENERATOR_MODEL", "unsloth/Qwen3-1.7B-GGUF")
    specs = _env_model_specs()
    qwen = next(spec for spec in specs if spec.repo == "unsloth/Qwen3-1.7B-GGUF")
    assert qwen.pull_ref == "hf:unsloth/Qwen3-1.7B-GGUF:gguf:Q4_K_M"


def test_generation_defaults_share_one_model():
    assert DEFAULT_GENERATOR_MODEL == DEFAULT_TOOL_GENERATOR_MODEL
    assert DEFAULT_GENERATOR_MODEL == DEFAULT_MULTIMODAL_GENERATOR_MODEL


def test_explicit_large_generator_is_bootstrapped(monkeypatch):
    for env_name in (*models.GENERATOR_ENV_VARS, *models.READER_ENV_VARS):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setenv("ANTFLY_INFERENCE_DOWNLOAD", "1")
    monkeypatch.setenv("ANTFLY_INFERENCE_DEFAULT_GENERATOR_MODEL", DEFAULT_GENERATOR_MODEL)
    monkeypatch.setattr(models, "model_available", lambda spec: False)
    pulled = []
    monkeypatch.setattr(models, "ensure_model", pulled.append)
    listing = {
        category: [{"name": "already-present"}] for category in models.LISTING_BOOTSTRAP
    }

    assert bootstrap_models_for_listing(listing)
    assert [spec.repo for spec in pulled] == [DEFAULT_GENERATOR_MODEL]
    assert pulled[0].pull_ref == "hf:unsloth/Qwen3-1.7B-GGUF:gguf:Q4_K_M"
