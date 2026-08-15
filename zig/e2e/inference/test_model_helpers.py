# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

import hashlib
import json
import tempfile
import threading
from types import SimpleNamespace

import pytest
import requests

from . import models
from .conftest import InferenceServer, capacity_retry_delay, retry_transient_capacity
from .models import (
    DEFAULT_EXTRACTOR_MODEL,
    DEFAULT_EXTRACTOR_VARIANT,
    DEFAULT_GENERATOR_MODEL,
    DEFAULT_MULTIMODAL_GENERATOR_MODEL,
    DEFAULT_TOOL_GENERATOR_MODEL,
    _env_model_specs,
    _looks_like_model_dir,
    _model_path,
    bootstrap_models_for_listing,
    find_local_model_path,
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


def test_explicit_variant_uses_registry_install_path(monkeypatch, tmp_path):
    monkeypatch.setenv("ANTFLY_INFERENCE_MODELS_DIR", str(tmp_path))
    spec = models.ModelSpec(
        name="model",
        repo="owner/model",
        task="generators",
        variant="gguf:Q4_K_M",
    )
    variant_hash = hashlib.sha256(spec.variant.encode()).hexdigest()[:16]

    assert _model_path(spec) == tmp_path / "owner" / f"model--antfly-{variant_hash}"


def test_bare_model_name_finds_completed_explicit_variant(monkeypatch, tmp_path):
    monkeypatch.setenv("ANTFLY_INFERENCE_MODELS_DIR", str(tmp_path))
    model_dir = tmp_path / "owner" / "model--antfly-0123456789abcdef"
    model_dir.mkdir(parents=True)
    (model_dir / "model.gguf").write_bytes(b"complete")

    assert find_local_model_path("owner/model") == model_dir


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


def test_managed_completion_receipt_accepts_v1_complete_artifact_set(tmp_path):
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


def test_managed_completion_receipt_accepts_v2_complete_artifact_set(tmp_path):
    model_dir = tmp_path / "owner" / "complete-v2"
    model_dir.mkdir(parents=True)
    (model_dir / "model.gguf").write_bytes(b"complete")
    receipt = {
        "version": 2,
        "source": {"owner": "owner", "name": "complete-v2", "variant": "auto"},
        "artifacts": [{"path": "model.gguf", "size": 8, "sha256": None}],
    }
    (model_dir / ".antfly-download-complete.json").write_text(json.dumps(receipt))

    assert _looks_like_model_dir(model_dir)


def test_managed_completion_receipt_rejects_unknown_version(tmp_path):
    model_dir = tmp_path / "owner" / "future-version"
    model_dir.mkdir(parents=True)
    (model_dir / "model.gguf").write_bytes(b"complete")
    receipt = {
        "version": 3,
        "artifacts": [{"path": "model.gguf", "size": 8}],
    }
    (model_dir / ".antfly-download-complete.json").write_text(json.dumps(receipt))

    assert not _looks_like_model_dir(model_dir)


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


@pytest.mark.parametrize(
    "artifact_path",
    (
        "nested//model.onnx",
        "nested/./model.onnx",
        "C:model.onnx",
        "nested\\model.onnx",
        "model\x00.onnx",
    ),
)
def test_managed_completion_receipt_rejects_runtime_unsafe_paths(
    tmp_path, artifact_path
):
    model_dir = tmp_path / "owner" / "unsafe-path"
    model_dir.mkdir(parents=True)
    (model_dir / models.MANAGED_DOWNLOAD_COMPLETE).write_text(
        json.dumps(
            {
                "version": 1,
                "artifacts": [{"path": artifact_path, "size": 1}],
            }
        )
    )

    assert not _looks_like_model_dir(model_dir)


def test_managed_completion_receipt_rejects_duplicate_artifact_paths(tmp_path):
    model_dir = tmp_path / "owner" / "duplicate-path"
    model_dir.mkdir(parents=True)
    (model_dir / "model.onnx").write_bytes(b"x")
    artifact = {"path": "model.onnx", "size": 1}
    (model_dir / models.MANAGED_DOWNLOAD_COMPLETE).write_text(
        json.dumps({"version": 1, "artifacts": [artifact, artifact]})
    )

    assert not _looks_like_model_dir(model_dir)


def test_managed_completion_receipt_bounds_artifact_count(monkeypatch, tmp_path):
    monkeypatch.setattr(models, "MAX_MANAGED_DOWNLOAD_ARTIFACTS", 1)
    model_dir = tmp_path / "owner" / "too-many-artifacts"
    model_dir.mkdir(parents=True)
    (model_dir / "model.onnx").write_bytes(b"x")
    (model_dir / "config.json").write_bytes(b"{}")
    (model_dir / models.MANAGED_DOWNLOAD_COMPLETE).write_text(
        json.dumps(
            {
                "version": 1,
                "artifacts": [
                    {"path": "model.onnx", "size": 1},
                    {"path": "config.json", "size": 2},
                ],
            }
        )
    )

    assert not _looks_like_model_dir(model_dir)


def test_managed_completion_receipt_limit_is_measured_in_bytes(monkeypatch, tmp_path):
    model_dir = tmp_path / "owner" / "receipt-byte-limit"
    model_dir.mkdir(parents=True)
    (model_dir / "model.onnx").write_bytes(b"x")
    serialized = json.dumps(
        {
            "version": 1,
            "artifacts": [{"path": "model.onnx", "size": 1}],
            "note": "é" * 32,
        },
        ensure_ascii=False,
    )
    assert len(serialized) < len(serialized.encode("utf-8"))
    monkeypatch.setattr(models, "MAX_MANAGED_DOWNLOAD_RECEIPT_BYTES", len(serialized))
    (model_dir / models.MANAGED_DOWNLOAD_COMPLETE).write_text(
        serialized, encoding="utf-8"
    )

    assert not _looks_like_model_dir(model_dir)


def test_reader_environment_override_preserves_curated_variant(monkeypatch):
    monkeypatch.setenv("ANTFLY_INFERENCE_FLORENCE_MODEL", "antflydb/florence-2-base")

    specs = _env_model_specs()
    florence = next(spec for spec in specs if spec.repo == "antflydb/florence-2-base")

    assert florence.pull_ref == "hf:antflydb/florence-2-base:gguf:Q4_K"


def test_default_extractor_uses_complete_antfly_gguf_bundle(tmp_path):
    spec = models.spec_for_name(DEFAULT_EXTRACTOR_MODEL, "extractors")
    assert spec is not None
    assert spec.variant == DEFAULT_EXTRACTOR_VARIANT
    assert spec.pull_ref == "hf:antflydb/gliner2-base-v1:gguf:Q4_K"
    assert models.DEFAULT_MODEL_BY_PATH["/ai/v1/extract"] == (
        DEFAULT_EXTRACTOR_MODEL,
        "extractors",
    )

    model_dir = tmp_path / DEFAULT_EXTRACTOR_MODEL
    model_dir.mkdir(parents=True)
    encoder = model_dir / "gliner2-encoder.Q4_K.gguf"
    head = model_dir / "gliner2-head.Q4_K.gguf"
    encoder.write_bytes(b"encoder")

    partial = models._probe_model_dir(model_dir)
    assert partial is not None
    assert not models._model_satisfies_spec(partial, spec)

    head.write_bytes(b"head")
    complete = models._probe_model_dir(model_dir)
    assert complete is not None
    assert models._model_satisfies_spec(complete, spec)


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


def test_draft_generator_environment_override_is_bootstrapped(monkeypatch):
    for env_name in (*models.GENERATOR_ENV_VARS, *models.READER_ENV_VARS):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setenv("ANTFLY_INFERENCE_DRAFT_MODEL", "unsloth/Qwen3-1.7B-GGUF")

    specs = _env_model_specs()

    assert [spec.repo for spec in specs] == ["unsloth/Qwen3-1.7B-GGUF"]
    assert specs[0].pull_ref == "hf:unsloth/Qwen3-1.7B-GGUF:gguf:Q4_K_M"


def test_generation_defaults_share_one_model():
    assert DEFAULT_GENERATOR_MODEL == DEFAULT_TOOL_GENERATOR_MODEL
    assert DEFAULT_GENERATOR_MODEL == DEFAULT_MULTIMODAL_GENERATOR_MODEL


def test_targeted_multimodal_generator_gate(monkeypatch):
    monkeypatch.delenv("RUN_LARGE_MODEL_TESTS", raising=False)
    monkeypatch.delenv("RUN_MULTIMODAL_GENERATOR_TESTS", raising=False)
    assert not models.run_multimodal_generator_tests()

    monkeypatch.setenv("RUN_MULTIMODAL_GENERATOR_TESTS", "1")
    assert models.run_multimodal_generator_tests()

    monkeypatch.setenv("RUN_MULTIMODAL_GENERATOR_TESTS", "false")
    monkeypatch.setenv("RUN_LARGE_MODEL_TESTS", "1")
    assert models.run_multimodal_generator_tests()


def test_targeted_clipclap_contract_gate(monkeypatch):
    monkeypatch.delenv("RUN_LARGE_MODEL_TESTS", raising=False)
    monkeypatch.delenv("RUN_CLIPCLAP_CONTRACT_TESTS", raising=False)
    assert not models.run_clipclap_contract_tests()

    monkeypatch.setenv("RUN_CLIPCLAP_CONTRACT_TESTS", "1")
    assert models.run_clipclap_contract_tests()

    monkeypatch.setenv("RUN_CLIPCLAP_CONTRACT_TESTS", "false")
    monkeypatch.setenv("RUN_LARGE_MODEL_TESTS", "1")
    assert models.run_clipclap_contract_tests()


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
