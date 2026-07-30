# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

import json

from .models import _looks_like_model_dir


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
