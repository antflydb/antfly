# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

import json
import tempfile
import threading
from types import SimpleNamespace

import pytest
import requests

from . import models
from .conftest import InferenceServer, capacity_retry_delay, retry_transient_capacity
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
    (model_dir / ".antfly-download-in-progress").write_text(
        '{"version":1,"state":"in_progress"}'
    )

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
    monkeypatch.setenv(
        "ANTFLY_INFERENCE_DEFAULT_GENERATOR_MODEL", "ggml-org/gemma-4-e2b-it-gguf"
    )

    specs = _env_model_specs()
    gemma = next(spec for spec in specs if spec.repo == "ggml-org/gemma-4-e2b-it-gguf")

    assert gemma.pull_ref == "hf:ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0"

    monkeypatch.setenv(
        "ANTFLY_INFERENCE_DEFAULT_GENERATOR_MODEL", "unsloth/Qwen3-1.7B-GGUF"
    )
    specs = _env_model_specs()
    qwen = next(spec for spec in specs if spec.repo == "unsloth/Qwen3-1.7B-GGUF")
    assert qwen.pull_ref == "hf:unsloth/Qwen3-1.7B-GGUF:gguf:Q4_K_M"


def test_generation_defaults_share_one_model():
    assert DEFAULT_GENERATOR_MODEL == DEFAULT_TOOL_GENERATOR_MODEL
    assert DEFAULT_GENERATOR_MODEL == DEFAULT_MULTIMODAL_GENERATOR_MODEL


def test_multimodal_model_selection_uses_shared_gemma_default(monkeypatch, tmp_path):
    monkeypatch.delenv("ANTFLY_INFERENCE_MULTIMODAL_GENERATOR_MODEL", raising=False)
    monkeypatch.setenv("ANTFLY_INFERENCE_MODELS_DIR", str(tmp_path))
    model_dir = tmp_path / DEFAULT_GENERATOR_MODEL
    model_dir.mkdir(parents=True)
    (model_dir / "config.json").write_text(
        json.dumps(
            {
                "architectures": ["Gemma4ForConditionalGeneration"],
                "vision_config": {},
            }
        )
    )

    assert (
        models.find_multimodal_generator_model_name({DEFAULT_GENERATOR_MODEL})
        == DEFAULT_GENERATOR_MODEL
    )


def test_explicit_large_generator_is_bootstrapped(monkeypatch):
    for env_name in (*models.GENERATOR_ENV_VARS, *models.READER_ENV_VARS):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setenv("ANTFLY_INFERENCE_DOWNLOAD", "1")
    monkeypatch.setenv(
        "ANTFLY_INFERENCE_DEFAULT_GENERATOR_MODEL", DEFAULT_GENERATOR_MODEL
    )
    monkeypatch.setattr(models, "model_available", lambda spec: False)
    pulled = []
    monkeypatch.setattr(models, "ensure_model", pulled.append)
    listing = {
        category: [{"name": "already-present"}] for category in models.LISTING_BOOTSTRAP
    }

    assert bootstrap_models_for_listing(listing)
    assert [spec.repo for spec in pulled] == [DEFAULT_GENERATOR_MODEL]
    assert pulled[0].pull_ref == "hf:ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0"
    assert pulled[0].projector == "Q8_0"


def test_gemma_pull_repairs_managed_cache_missing_q8_projector(monkeypatch, tmp_path):
    spec = models.spec_for_name(DEFAULT_GENERATOR_MODEL, "generators")
    assert spec is not None
    monkeypatch.setenv("ANTFLY_INFERENCE_MODELS_DIR", str(tmp_path))
    monkeypatch.setattr(models, "inference_command", lambda: ["antfly", "inference"])

    model_dir = tmp_path / DEFAULT_GENERATOR_MODEL
    model_dir.mkdir(parents=True)
    decoder = model_dir / "gemma-4-e2b-it-Q4_0.gguf"
    decoder.write_bytes(b"decoder")
    completion_path = model_dir / models.MANAGED_DOWNLOAD_COMPLETE
    completion_path.write_text(
        json.dumps(
            {
                "version": 1,
                "artifacts": [{"path": decoder.name, "size": decoder.stat().st_size}],
            }
        )
    )

    calls: list[list[str]] = []

    def repair(command, **_kwargs):
        calls.append(command)
        projector = model_dir / "mmproj-gemma-4-e2b-it-Q8_0.gguf"
        projector.write_bytes(b"projector")
        completion_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "artifacts": [
                        {"path": decoder.name, "size": decoder.stat().st_size},
                        {
                            "path": projector.name,
                            "size": projector.stat().st_size,
                        },
                    ],
                }
            )
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(
        models.subprocess,
        "run",
        repair,
    )

    assert not models.model_available(spec)
    assert models.ensure_model(spec) == model_dir

    assert calls
    projector_index = calls[0].index("--projector")
    assert calls[0][projector_index + 1] == "Q8_0"
    assert models.model_available(spec)


def test_gemma_matching_managed_projector_skips_pull(monkeypatch, tmp_path):
    spec = models.spec_for_name(DEFAULT_GENERATOR_MODEL, "generators")
    assert spec is not None
    monkeypatch.setenv("ANTFLY_INFERENCE_MODELS_DIR", str(tmp_path))

    model_dir = tmp_path / DEFAULT_GENERATOR_MODEL
    model_dir.mkdir(parents=True)
    decoder = model_dir / "gemma-4-e2b-it-Q4_0.gguf"
    projector = model_dir / "nested" / "mmproj-gemma-4-e2b-it-Q8_0.gguf"
    projector.parent.mkdir()
    decoder.write_bytes(b"decoder")
    projector.write_bytes(b"projector")
    (model_dir / models.MANAGED_DOWNLOAD_COMPLETE).write_text(
        json.dumps(
            {
                "version": 1,
                "artifacts": [
                    {"path": decoder.name, "size": decoder.stat().st_size},
                    {
                        "path": projector.relative_to(model_dir).as_posix(),
                        "size": projector.stat().st_size,
                    },
                ],
            }
        )
    )
    monkeypatch.setattr(
        models.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("complete model must not be pulled again")
        ),
    )

    assert models.model_available(spec)
    assert models.ensure_model(spec) == model_dir


def test_managed_model_probe_validates_receipt_once(monkeypatch, tmp_path):
    spec = models.spec_for_name(DEFAULT_GENERATOR_MODEL, "generators")
    assert spec is not None
    monkeypatch.setenv("ANTFLY_INFERENCE_MODELS_DIR", str(tmp_path))

    model_dir = tmp_path / DEFAULT_GENERATOR_MODEL
    model_dir.mkdir(parents=True)
    decoder = model_dir / "gemma-4-e2b-it-Q4_0.gguf"
    projector = model_dir / "mmproj-gemma-4-e2b-it-Q8_0.gguf"
    decoder.write_bytes(b"decoder")
    projector.write_bytes(b"projector")
    (model_dir / models.MANAGED_DOWNLOAD_COMPLETE).write_text(
        json.dumps(
            {
                "version": 1,
                "artifacts": [
                    {"path": decoder.name, "size": decoder.stat().st_size},
                    {"path": projector.name, "size": projector.stat().st_size},
                ],
            }
        )
    )

    validations = 0
    validate = models._validated_managed_artifacts

    def count_validations(path):
        nonlocal validations
        validations += 1
        return validate(path)

    monkeypatch.setattr(models, "_validated_managed_artifacts", count_validations)

    assert models.model_available(spec)
    assert validations == 1


def test_projector_filename_matching_mirrors_runtime_case_contract():
    assert models._is_projector_gguf("nested/mmproj-model-Q8_0.gguf")
    assert not models._is_projector_gguf("nested/MMPROJ-model-Q8_0.gguf")
    assert not models._is_projector_gguf("nested/mmproj-model-Q8_0.GGUF")


def test_gemma_wrong_projector_quant_does_not_satisfy_spec(monkeypatch, tmp_path):
    spec = models.spec_for_name(DEFAULT_GENERATOR_MODEL, "generators")
    assert spec is not None
    monkeypatch.setenv("ANTFLY_INFERENCE_MODELS_DIR", str(tmp_path))

    model_dir = tmp_path / DEFAULT_GENERATOR_MODEL
    model_dir.mkdir(parents=True)
    decoder = model_dir / "gemma-4-e2b-it-Q4_0.gguf"
    projector = model_dir / "mmproj-gemma-4-e2b-it-BF16.gguf"
    decoder.write_bytes(b"decoder")
    projector.write_bytes(b"projector")
    (model_dir / models.MANAGED_DOWNLOAD_COMPLETE).write_text(
        json.dumps(
            {
                "version": 1,
                "artifacts": [
                    {"path": decoder.name, "size": decoder.stat().st_size},
                    {"path": projector.name, "size": projector.stat().st_size},
                ],
            }
        )
    )

    assert not models.model_available(spec)


def test_gemma_pull_fails_closed_when_requested_projector_is_not_published(
    monkeypatch, tmp_path
):
    spec = models.spec_for_name(DEFAULT_GENERATOR_MODEL, "generators")
    assert spec is not None
    monkeypatch.setenv("ANTFLY_INFERENCE_MODELS_DIR", str(tmp_path))
    monkeypatch.setattr(models, "inference_command", lambda: ["antfly", "inference"])

    model_dir = tmp_path / DEFAULT_GENERATOR_MODEL

    def incomplete_pull(*_args, **_kwargs):
        model_dir.mkdir(parents=True)
        decoder = model_dir / "gemma-4-e2b-it-Q4_0.gguf"
        decoder.write_bytes(b"decoder")
        (model_dir / models.MANAGED_DOWNLOAD_COMPLETE).write_text(
            json.dumps(
                {
                    "version": 1,
                    "artifacts": [
                        {"path": decoder.name, "size": decoder.stat().st_size}
                    ],
                }
            )
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(models.subprocess, "run", incomplete_pull)

    with pytest.raises(RuntimeError, match="missing projector Q8_0"):
        models.ensure_model(spec)


def _response(status: int, body: dict) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.headers["content-type"] = "application/json"
    response._content = json.dumps(body).encode()
    return response


def test_capacity_retry_requires_explicit_retry_contract():
    retryable = _response(
        503,
        {
            "error": "MODEL_RESOURCE_BUSY",
            "retryable": True,
            "retry_after_ms": 1250,
        },
    )
    assert capacity_retry_delay(retryable, 0.25) == 1.25
    assert (
        capacity_retry_delay(_response(503, {"error": "MODEL_RESOURCE_BUSY"}), 0.25)
        is None
    )
    assert capacity_retry_delay(_response(500, {"retryable": True}), 0.25) is None


def test_capacity_retry_uses_header_fallback_and_sanitizes_delay():
    retryable = _response(
        503,
        {"error": "MODEL_RESOURCE_BUSY", "retryable": True},
    )
    retryable.headers["Retry-After"] = "2"
    assert capacity_retry_delay(retryable, 0.25) == 2

    retryable._content = json.dumps(
        {
            "error": "MODEL_RESOURCE_BUSY",
            "retryable": True,
            "retry_after_ms": float("nan"),
        }
    ).encode()
    assert capacity_retry_delay(retryable, 0.25) == 0.25


def test_capacity_retry_closes_rejection_and_replays_after_delay():
    busy = _response(
        503,
        {"error": "MODEL_RESOURCE_BUSY", "retryable": True, "retry_after_ms": 1000},
    )
    success = _response(200, {"ok": True})
    now = [10.0]
    sends = []

    def sleep(delay):
        now[0] += delay

    result = retry_transient_capacity(
        busy,
        lambda: sends.append(True) or success,
        timeout=5,
        clock=lambda: now[0],
        sleeper=sleep,
    )

    assert result is success
    assert sends == [True]
    assert busy.raw is None or busy._content_consumed
    assert now[0] == 11.0


def test_capacity_retry_preserves_last_response_at_deadline():
    busy = _response(
        503,
        {"error": "MODEL_RESOURCE_BUSY", "retryable": True, "retry_after_ms": 1000},
    )
    result = retry_transient_capacity(
        busy,
        lambda: (_ for _ in ()).throw(AssertionError("must not replay")),
        timeout=0.5,
        clock=lambda: 1.0,
        sleeper=lambda _delay: None,
    )
    assert result is busy


def test_capacity_retry_bounds_attempts_under_zero_delay():
    responses = [
        _response(
            503,
            {"error": "MODEL_RESOURCE_BUSY", "retryable": True, "retry_after_ms": 0},
        )
        for _ in range(4)
    ]
    sends = []
    result = retry_transient_capacity(
        responses[0],
        lambda: sends.append(True) or responses[len(sends)],
        timeout=30,
        max_attempts=2,
        clock=lambda: 0,
        sleeper=lambda _delay: None,
    )
    assert result is responses[2]
    assert len(sends) == 2


def test_live_server_tail_read_does_not_move_shared_output_offset():
    server = InferenceServer.__new__(InferenceServer)
    server.output = tempfile.TemporaryFile(mode="w+b")
    try:
        server.output.write(b"prefix\nactionable backend error\n")
        server.output.seek(3)
        position = server.output.tell()

        assert "actionable backend error" in server.read_output()
        assert server.output.tell() == position
    finally:
        server.output.close()


def test_live_server_http_diagnostic_is_reported_once(capsys):
    server = InferenceServer.__new__(InferenceServer)
    server.output = tempfile.TemporaryFile(mode="w+b")
    server.output.write(b"backend failed to load tensor\n")
    server.proc = SimpleNamespace(poll=lambda: None)
    server.http_failure_reported = False
    server._diagnostic_lock = threading.Lock()
    try:
        server.report_http_failure_once()
        server.report_http_failure_once()
        captured = capsys.readouterr()
        assert captured.err.count("backend failed to load tensor") == 1
        assert "still running" in captured.err
    finally:
        server.output.close()
