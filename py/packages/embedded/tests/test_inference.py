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

"""Tests for antfly_embedded.Inference (embedded inference without a database),
mirroring the C ABI's "Embedded inference without a database" contract (see
zig/CAPI.md "Inference" and antfly.h).
"""

from __future__ import annotations

import base64
import glob
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import antfly_embedded
from antfly_embedded import errors

pytestmark = pytest.mark.usefixtures("require_native")


def _has_qwen_embedding_model() -> bool:
    pattern = os.path.expanduser("~/.antfly/inference/models/Qwen/Qwen3-Embedding-0.6B-GGUF*")
    return len(glob.glob(pattern)) > 0


PULL_TEST_MODEL = os.environ.get("ANTFLY_INFERENCE_PULL_TEST_MODEL")


@pytest.mark.parametrize("backend", ["whisper", "gemma"])
def test_qualified_audio_read_and_retained_extraction(backend: str) -> None:
    """Opt-in installed-model qualification; never download model weights."""
    models = os.environ.get("ANTFLY_AUDIO_READ_QUALIFICATION_MODELS_DIR")
    model = os.environ.get(f"ANTFLY_AUDIO_READ_QUALIFICATION_{backend.upper()}")
    extractor = os.environ.get("ANTFLY_AUDIO_READ_QUALIFICATION_EXTRACTOR")
    if not models or not model or not extractor:
        pytest.skip("audio read qualification models were not selected")
    fixture = Path(__file__).resolve().parents[4] / "zig/e2e/inference/testdata/whisper_quality.wav"
    audio = "data:audio/wav;base64," + base64.b64encode(fixture.read_bytes()).decode()
    with antfly_embedded.Inference.open(
        models_dir=models,
        process_memory_budget_mb=24576,
        call_timeout_ms=120_000,
    ) as inference:
        unsupported = (
            {"audio_options": {"timestamps": True}} if backend == "gemma" else {"prompt": "Rewrite the transcript"}
        )
        with pytest.raises(errors.AntflyError, match="UnsupportedAudioReadCapability"):
            inference.read({"model": model, "audio": "https://example.invalid/must-not-fetch.wav", **unsupported})
        with pytest.raises(errors.AntflyError):
            inference.read({"model": model, "audio": audio, "images": []})
        options = {"audio_options": {"language": "en", "timestamps": True}} if backend == "whisper" else {}
        reading = inference.read({"model": model, "audio": audio, "retain_artifact": True, **options})
        item = reading["data"][0]
        assert item["status"] == "complete"
        assert item["kind"] == "audio" and item["coordinate_space"] == "none"
        assert item["origin"] == "transcription"
        assert all(word in item["text"].lower() for word in ("fox", "dog"))
        assert item["audio"]["duration_ms"] == 2496
        if backend == "whisper":
            assert item["audio"]["language_source"] == "forced"
            assert item["audio"]["segments"]
        else:
            assert item["audio"]["segments"] is None
            assert item["audio"]["speakers"] is None
        handle = reading["document"]["artifact_id"]
        assert len(handle) == 64
        extracted = inference.extract(
            {
                "model": extractor,
                "schema_version": 2,
                "schema": {"entities": ["animal"]},
                "options": {"include_spans": True},
                "read_artifact": handle,
            }
        )
        assert extracted["data"][0]["entities"]
        assert extracted["data"][0]["entities"][0]["evidence"][0]["origin"] == "transcription"
        assert extracted["usage"]["physical_model_calls"] == 1
        if backend == "gemma":
            partial = inference.read({"model": model, "audio": audio, "max_tokens": 1})
            assert partial["data"][0]["status"] == "truncated"


# -- open / close -------------------------------------------------------------


def test_open_default_and_close() -> None:
    inf = antfly_embedded.Inference.open()
    try:
        assert isinstance(inf, antfly_embedded.Inference)
    finally:
        inf.close()
    # Double close is a no-op.
    inf.close()


def test_open_with_options(tmp_path: Path) -> None:
    with antfly_embedded.Inference.open(
        models_dir=tmp_path,
        host_budget_mb=64,
        backend_budget_mb=64,
        process_memory_budget_mb=64,
        combined_budget_mb=64,
        kv_budget_mb=16,
        scratch_budget_mb=16,
        call_timeout_ms=30_000,
    ) as inf:
        models = inf.list_models()
        assert models["data"] == []


def test_double_close_ok() -> None:
    inf = antfly_embedded.Inference.open()
    inf.close()
    inf.close()
    inf.close()


def test_calls_after_close_raise() -> None:
    inf = antfly_embedded.Inference.open()
    inf.close()
    with pytest.raises(errors.InvalidArgumentError):
        inf.chunk({"input": "hi"})
    with pytest.raises(errors.InvalidArgumentError):
        inf.list_models()


# -- calls that need no model --------------------------------------------------


def test_chunk_returns_data(tmp_path: Path) -> None:
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        result = inf.chunk({"input": "Ants live in colonies. Workers gather food."})
        assert "data" in result
        assert isinstance(result["data"], list)
        assert len(result["data"]) > 0


def test_list_models_empty_dir_returns_no_data(tmp_path: Path) -> None:
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        result = inf.list_models()
        assert result["data"] == []


def test_embed_missing_model_raises_not_found(tmp_path: Path) -> None:
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        with pytest.raises(errors.NotFoundError) as exc_info:
            inf.embed({"model": "nonexistent/does-not-exist", "input": "hello"})
        assert "MODEL_NOT_FOUND" in str(exc_info.value)


def test_decide_errors_and_closed_handle(tmp_path: Path) -> None:
    inf = antfly_embedded.Inference.open(models_dir=tmp_path)
    try:
        with pytest.raises(errors.InvalidArgumentError, match="INVALID_REQUEST"):
            inf.decide({})
        with pytest.raises(errors.NotFoundError, match="MODEL_NOT_FOUND"):
            inf.decide(
                {
                    "model": "no/such-model",
                    "input": "refund",
                    "questions": [{"name": "refund", "type": "predicate", "instructions": "Refund?"}],
                }
            )
    finally:
        inf.close()
    with pytest.raises(errors.InvalidArgumentError):
        inf.decide({})


def test_decide_real_runtime_returns_all_answer_types_and_raw_json(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[4] / "scripts/testing/create_decision_fixture.py"
    model = subprocess.check_output([sys.executable, str(script), str(tmp_path)], text=True).strip()
    request = {
        "model": model,
        "input": "Refund the duplicate charge.",
        "questions": [
            {
                "name": "route",
                "type": "choice",
                "instructions": "Which team?",
                "choices": [
                    {"value": "billing", "description": "Charges"},
                    {"value": "support", "description": "Product"},
                ],
            },
            {
                "name": "urgency",
                "type": "score",
                "instructions": "How urgent?",
                "levels": [{"label": "Routine"}, {"label": "Soon"}, {"label": "Immediate"}],
            },
            {"name": "refund", "type": "predicate", "instructions": "Refund requested?"},
        ],
    }
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        result = inf.decide(request)
        assert result["model"] == model
        route, urgency, refund = result["answers"]
        assert route["name"] == "route" and route["choice"] == "billing"
        assert route["probabilities"] == [
            {"value": "billing", "probability": 0.5},
            {"value": "support", "probability": 0.5},
        ]
        assert urgency["score"] == pytest.approx(1.0)
        assert [level["label"] for level in urgency["probabilities"]] == ["Routine", "Soon", "Immediate"]
        assert sum(level["probability"] for level in urgency["probabilities"]) == pytest.approx(1.0)
        assert refund["type"] == "predicate" and refund["probability"] == pytest.approx(0.5)
        assert result["usage"]["input_tokens"] > 0
        assert result["usage"]["output_tokens"] == 0
        raw = inf.decide(json.dumps(request), raw=True)
        assert isinstance(raw, bytes)
        assert json.loads(raw) == result


def test_pull_missing_model_field_raises_invalid_argument(tmp_path: Path) -> None:
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        with pytest.raises(errors.InvalidArgumentError) as exc_info:
            inf.pull({})
        # The C API contract guarantees a JSON error body on failure; make
        # sure this binding actually surfaces it rather than raising a bare
        # generic error.
        assert str(exc_info.value)


def test_generate_with_stream_true_raises_invalid_argument(tmp_path: Path) -> None:
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        with pytest.raises(errors.InvalidArgumentError):
            inf.generate({"input": "hello", "stream": True})


def test_generate_stream_missing_model_raises_not_found(tmp_path: Path) -> None:
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        chunks: list[object] = []
        with pytest.raises(errors.NotFoundError) as exc_info:
            inf.generate_stream(
                {"model": "nonexistent/does-not-exist", "messages": [{"role": "user", "content": "hi"}]},
                chunks.append,
            )
        assert "MODEL_NOT_FOUND" in str(exc_info.value)
        assert chunks == []


def test_generate_stream_invalid_json_rejected(tmp_path: Path) -> None:
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        with pytest.raises(errors.InvalidArgumentError):
            # A str request is sent as raw bytes (see JSON conventions), so
            # this exercises the C API's own JSON parsing, not ours.
            inf.generate_stream("not valid json", lambda _chunk: None)


# -- optional: real model, skipped unless installed ----------------------------


@pytest.mark.skipif(not _has_qwen_embedding_model(), reason="Qwen3-Embedding-0.6B-GGUF model not installed locally")
def test_embed_real_model_returns_vectors() -> None:
    with antfly_embedded.Inference.open() as inf:
        result = inf.embed({"model": "Qwen/Qwen3-Embedding-0.6B-GGUF", "input": ["a", "b"]})
        assert len(result["data"]) == 2


def _has_gemma_model() -> bool:
    pattern = os.path.expanduser("~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf*")
    return len(glob.glob(pattern)) > 0


GEMMA_MODEL = "ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0"


@pytest.mark.skipif(not _has_gemma_model(), reason="ggml-org/gemma-4-e2b-it-gguf model not installed locally")
def test_generate_stream_yields_multiple_chunks() -> None:
    with antfly_embedded.Inference.open() as inf:
        chunks: list[dict] = []
        inf.generate_stream(
            {
                "model": GEMMA_MODEL,
                "messages": [{"role": "user", "content": "Count from one to twenty in words."}],
                "max_tokens": 48,
            },
            chunks.append,
        )
        assert len(chunks) > 2
        for chunk in chunks:
            assert chunk["object"] == "chat.completion.chunk"


@pytest.mark.skipif(not _has_gemma_model(), reason="ggml-org/gemma-4-e2b-it-gguf model not installed locally")
def test_generate_stream_stopping_after_two_cancels() -> None:
    with antfly_embedded.Inference.open() as inf:
        chunks: list[dict] = []

        def on_chunk(chunk: dict) -> bool:
            chunks.append(chunk)
            return len(chunks) < 2

        with pytest.raises(errors.CancelledError):
            inf.generate_stream(
                {
                    "model": GEMMA_MODEL,
                    "messages": [{"role": "user", "content": "Count from one to twenty in words."}],
                    "max_tokens": 48,
                },
                on_chunk,
            )
        assert len(chunks) == 2


# -- optional: network pull, skipped unless explicitly requested ---------------


@pytest.mark.skipif(not PULL_TEST_MODEL, reason="set ANTFLY_INFERENCE_PULL_TEST_MODEL to run")
def test_pull_downloads_model_with_progress(tmp_path: Path) -> None:
    assert PULL_TEST_MODEL
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        events: list[antfly_embedded.PullProgress] = []

        def on_progress(p: antfly_embedded.PullProgress) -> None:
            events.append(p)

        result = inf.pull({"model": PULL_TEST_MODEL}, progress=on_progress)
        assert events, "expected at least one progress callback"
        for event in events:
            assert isinstance(event, antfly_embedded.PullProgress)
        assert "models" in result

        models = inf.list_models()
        names = [m.get("id") or m.get("model") for m in models["data"]]
        assert any(PULL_TEST_MODEL in str(n) for n in names) or models["data"]


@pytest.mark.skipif(not PULL_TEST_MODEL, reason="set ANTFLY_INFERENCE_PULL_TEST_MODEL to run")
def test_pull_progress_callback_exception_propagates(tmp_path: Path) -> None:
    assert PULL_TEST_MODEL
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:

        def on_progress(_p: antfly_embedded.PullProgress) -> None:
            raise ValueError("boom from progress callback")

        with pytest.raises(ValueError, match="boom from progress callback"):
            inf.pull({"model": PULL_TEST_MODEL}, progress=on_progress)


@pytest.mark.skipif(not PULL_TEST_MODEL, reason="set ANTFLY_INFERENCE_PULL_TEST_MODEL to run")
def test_pull_cancel_on_first_report_then_full_pull_succeeds(tmp_path: Path) -> None:
    assert PULL_TEST_MODEL
    with antfly_embedded.Inference.open(models_dir=tmp_path) as inf:
        reports = {"n": 0}

        def cancel_on_first_report(_p: antfly_embedded.PullProgress) -> bool:
            reports["n"] += 1
            return False

        with pytest.raises(errors.CancelledError):
            inf.pull({"model": PULL_TEST_MODEL}, progress=cancel_on_first_report)
        assert reports["n"] == 1

        models = inf.list_models()
        names = [m.get("id") or m.get("model") for m in models["data"]]
        assert not any(PULL_TEST_MODEL in str(n) for n in names)

        # A later, uncancelled pull resumes from whatever stayed staged and
        # completes successfully.
        result = inf.pull({"model": PULL_TEST_MODEL})
        assert "models" in result


def test_native_database_needs_no_worker_across_writes_and_reopen(
    aflite_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ANTFLY_INFERENCE_WORKER", str(tmp_path / "missing-worker"))
    options = antfly_embedded.OpenOptions(
        local_runtime_configured=True,
        generated_enrichment_replay=True,
        models_dir=tmp_path / "models",
        busy_timeout=5.0,
    )
    with antfly_embedded.create_with_options(aflite_path, options) as db:
        if not db.capabilities()["local_inference_runtime"]:
            pytest.skip("local inference is not advertised by this build")
        for i in range(20):
            db.batch_json({"inserts": {f"doc:{i}": {"body": f"synthetic row {i}"}}})
            admission = db.status()["inference"]["memory_admission"]
            assert admission["pressure"] == "none"
            assert admission["denied_count"] == 0
        db.run_until_idle()
        assert db.lookup("doc:19") is not None
    with antfly_embedded.open_with_options(aflite_path, options) as db:
        assert db.lookup("doc:19") is not None
        assert db.status()["inference"]["memory_admission"]["denied_count"] == 0
