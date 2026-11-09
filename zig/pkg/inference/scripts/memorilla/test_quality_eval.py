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

"""Transport-mocked runner checks; these do not execute a native model."""

import argparse
import copy
import json

import pytest

import quality_eval as qe


def arguments(tmp_path):
    return argparse.Namespace(
        output=tmp_path / "result.json",
        suite=qe.HERE / "fixtures/quality_v1.json",
        case_id=None,
        max_new_tokens=48,
        module_sha256=qe.STAGE3_SHA256,
        reuse_features=None,
        profile="memorilla-stage3",
        encoder="encoder",
        base_url="http://127.0.0.1:18091",
        timeout_ms=300000,
    )


class FakeNativeClient:
    def __init__(self, suite):
        self.suite = suite
        self.calls = []
        self.responses = []

    def post(self, path, body):
        self.calls.append((path, copy.deepcopy(body)))
        if path == "embed":
            assert set(body) == {"model", "input", "task_type"}
            assert body["task_type"] == "RETRIEVAL_DOCUMENT"
            rows = []
            for text in body["input"]:
                row = [0.0] * 2560
                row[int(qe.document_key(text), 16) % 2560] = 1.0
                rows.append(row)
            return {"model_identity": qe.IDENTITY, "embeddings": rows}
        assert path == "memorilla/generate"
        assert set(body) == {
            "model",
            "question",
            "document_embeddings",
            "encoder_execution_identity",
            "max_new_tokens",
        }
        value = {
            "model": body["model"],
            "text": "An intentionally incorrect mocked answer.",
            "token_ids": [1, 2],
            "finish_reason": "eos",
            "input_documents": len(body["document_embeddings"]),
            "memory_tokens": 16,
            "context_source": "supplied_embeddings",
            "encoder_execution_identity": qe.IDENTITY,
            "decoder_execution_identity": qe.DECODER_IDENTITY,
            "memory_module_sha256": qe.STAGE3_SHA256,
            "usage": {
                "encoder_query_tokens": 10,
                "prompt_tokens": 60,
                "completion_tokens": 2,
            },
        }
        self.responses.append(value)
        return value


def test_runner_persists_raw_document_features_and_separates_quality(tmp_path):
    args = arguments(tmp_path)
    suite = qe.load_suite(args.suite)
    client = FakeNativeClient(suite)
    result = qe.run(args, client)
    assert result["completed"] and result["execution_passed"]
    assert result["rollout_qualified"] is False
    assert result["selected_case_accuracy"] == 0
    assert result["checks"]["first_case_after_all_cases_response_equal"]
    assert len(result["results"]) == len(suite["cases"])
    assert len(client.responses) == len(suite["cases"]) + 1
    for path, body in client.calls:
        if path == "memorilla/generate":
            # Neither gold aliases, evidence labels nor raw context is submitted.
            assert "answer" not in body and "documents" not in body
    store = json.loads((tmp_path / "result-features.json").read_text())
    documents = list(
        dict.fromkeys(doc for c in suite["cases"] for doc in c["documents"])
    )
    rows = qe.validate_store(store, documents)
    first = next(b for p, b in client.calls if p == "memorilla/generate")
    assert first["document_embeddings"] == [
        rows[qe.document_key(t)] for t in suite["cases"][0]["documents"]
    ]
    assert json.loads(args.output.read_text()) == result


@pytest.mark.parametrize(
    "bad", ["identity", "row-count", "nonfinite", "dimensions", "normalization"]
)
def test_invalid_producer_cannot_reach_generation(tmp_path, bad):
    args = arguments(tmp_path)

    class Client(FakeNativeClient):
        def post(self, path, body):
            value = super().post(path, body)
            assert path == "embed", "Generation must not run after invalid production"
            if bad == "identity":
                value["model_identity"] = "0" * 64
            elif bad == "row-count":
                value["embeddings"] = []
            elif bad == "nonfinite":
                value["embeddings"][0][0] = float("nan")
            elif bad == "dimensions":
                value["embeddings"][0] = [1.0]
            else:
                value["embeddings"][0] = [0.0] * 2560
            return value

    with pytest.raises(ValueError):
        qe.run(args, Client(qe.load_suite(args.suite)))
    receipt = json.loads(args.output.read_text())
    assert receipt["completed"] is False and receipt["execution_passed"] is False
    assert "error" in receipt
    assert receipt["quality"]["primary"]["correct"] == 0
    assert receipt["quality"]["primary"]["total"] == 22
    assert len(receipt["quality"]["missing_or_invalid_answers"]) == 24


def test_feature_store_tamper_detected(tmp_path):
    args = arguments(tmp_path)
    qe.run(args, FakeNativeClient(qe.load_suite(args.suite)))
    store = json.loads((tmp_path / "result-features.json").read_text())
    store["documents"][0]["text"] += " altered"
    with pytest.raises(ValueError, match="modified"):
        qe.validate_store(store, [])
    store = json.loads((tmp_path / "result-features.json").read_text())
    row = store["documents"][0]["embedding"]
    i = row.index(1.0)
    row[i], row[(i + 1) % 2560] = 0.0, 1.0
    with pytest.raises(ValueError, match="digest"):
        qe.validate_store(store, [])


def test_reuse_requires_coverage_and_skips_embedding(tmp_path):
    args = arguments(tmp_path)
    suite = qe.load_suite(args.suite)
    qe.run(args, FakeNativeClient(suite))
    args.reuse_features = tmp_path / "result-features.json"
    args.output = tmp_path / "reused.json"
    client = FakeNativeClient(suite)
    result = qe.run(args, client)
    assert all(path != "embed" for path, _ in client.calls)
    assert result["execution_passed"]
    assert "reused" in result["feature_provenance"]
    store = json.loads(args.reuse_features.read_text())
    store["documents"] = []
    args.reuse_features.write_text(json.dumps(store))
    args.output = tmp_path / "uncovered.json"
    with pytest.raises(ValueError, match="cover"):
        qe.run(args, FakeNativeClient(suite))


def test_changed_response_after_other_collections_fails_execution(tmp_path):
    args = arguments(tmp_path)
    suite = qe.load_suite(args.suite)

    class Client(FakeNativeClient):
        def post(self, path, body):
            value = super().post(path, body)
            if len(self.responses) > len(suite["cases"]):
                value["token_ids"] = [3, 4]
            return value

    with pytest.raises(ValueError, match="changed"):
        qe.run(args, Client(suite))
    assert json.loads(args.output.read_text())["execution_passed"] is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("decoder_execution_identity", "wrong"),
        ("memory_tokens", 15),
        ("memory_module_sha256", "wrong"),
        ("input_documents", 64),
    ],
)
def test_response_identity_and_geometry_are_verified(tmp_path, field, value):
    args = arguments(tmp_path)
    client = FakeNativeClient(qe.load_suite(args.suite))
    response = client.post(
        "memorilla/generate",
        {
            "model": args.profile,
            "question": "q",
            "document_embeddings": [[1.0]],
            "encoder_execution_identity": qe.IDENTITY,
            "max_new_tokens": 48,
        },
    )
    response[field] = value
    with pytest.raises(ValueError):
        qe.validate_response(
            response,
            profile=args.profile,
            module_sha256=args.module_sha256,
            documents=1,
            max_new_tokens=48,
        )


@pytest.mark.parametrize("budget", [0, -1, 129])
def test_invalid_budget_rejected_before_requests(tmp_path, budget):
    args = arguments(tmp_path)
    args.max_new_tokens = budget
    client = FakeNativeClient(qe.load_suite(args.suite))
    with pytest.raises(ValueError):
        qe.run(args, client)
    assert not client.calls


def test_existing_evidence_not_overwritten(tmp_path):
    args = arguments(tmp_path)
    args.output.write_text("original")
    with pytest.raises(ValueError, match="Preserve"):
        qe.run(args)
    assert args.output.read_text() == "original"
