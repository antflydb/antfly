# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

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
