# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Mocked native transport/provenance tests; no GPU or model execution."""

import argparse
import copy
import json

import pytest

import verified_quality_eval as vq


def arguments(tmp_path, label="dev"):
    return argparse.Namespace(
        output=tmp_path / (label + ".json"),
        base_url="http://127.0.0.1:18091",
        profile="memorilla-stage3",
        encoder="encoder",
        module_sha256=vq.STAGE3_SHA256,
        split="dev",
        development_receipt=None,
        native_binary=None,
        max_new_tokens=48,
        timeout_ms=300000,
    )


def response(documents, *, answer="Alice", path="memory_verified"):
    abstain = path == "abstained"
    return {
        "model": "memorilla-stage3",
        "context_source": "supplied_embeddings_and_documents",
        "source_alignment": "caller_supplied",
        "input_documents": len(documents),
        "document_text_sha256s": [vq.document_key(text) for text in documents],
        "encoder_execution_identity": vq.IDENTITY,
        "decoder_execution_identity": vq.DECODER_IDENTITY,
        "memory_module_sha256": vq.STAGE3_SHA256,
        "answer_path": path,
        "verification_status": "abstained" if abstain else "model_supported",
        "memory_tokens": 16 if path == "memory_verified" else 0,
        "text": "UNKNOWN" if abstain else answer,
        "token_ids": [] if abstain else [42],
        "finish_reason": "abstained" if abstain else "eos",
        "usage": {
            "encoder_query_tokens": 8,
            "prompt_tokens": 50,
            "completion_tokens": 0 if abstain else 1,
            "total_prompt_tokens": 100,
            "total_completion_tokens": 3,
            "verification_calls": 1,
            "source_fallback_used": path != "memory_verified",
        },
    }


def validate(value, documents=None):
    vq.validate_response(
        value,
        profile="memorilla-stage3",
        module_sha256=vq.STAGE3_SHA256,
        documents=documents or ["raw document"],
        maximum=48,
    )


class NativeMock:
    def __init__(self, fail_first=False):
        suites = [
            vq.quality_cases.load_suite(vq.quality_cases.DEFAULT_SUITE),
            vq.factual_cases.load_suite(),
        ]
        self.index = {
            (c["question"], tuple(c["documents"])): c
            for suite in suites
            for c in suite["cases"]
        }
        self.calls, self.generations = [], 0
        self.fail_first = fail_first

    def post(self, path, body):
        self.calls.append((path, copy.deepcopy(body)))
        if path == "embed":
            assert set(body) == {"model", "input", "task_type"}
            assert body["task_type"] == "RETRIEVAL_DOCUMENT" and len(body["input"]) <= 4
            vectors = []
            for text in body["input"]:
                vector = [0.0] * 2560
                vector[int(vq.document_key(text), 16) % 2560] = 1.0
                vectors.append(vector)
            return {"model_identity": vq.IDENTITY, "embeddings": vectors}
        assert path == "memorilla/generate"
        assert set(body) == {
            "model",
            "question",
            "document_embeddings",
            "encoder_execution_identity",
            "max_new_tokens",
            "context_mode",
            "document_texts",
        }
        assert body["context_mode"] == "source_verified"
        assert len(body["document_embeddings"]) == len(body["document_texts"])
        self.generations += 1
        if self.fail_first and self.generations == 1:
            raise RuntimeError("mocked HTTP 503 request error")
        case = self.index[(body["question"], tuple(body["document_texts"]))]
        gold = case["answer"]["aliases"][0]
        path = (
            "abstained"
            if gold == "UNKNOWN"
            else "source_fallback_verified"
            if self.generations % 2
            else "memory_verified"
        )
        return response(body["document_texts"], answer=gold, path=path)


def test_raw_docs_persist_reload_and_separate_suites(tmp_path):
    args = arguments(tmp_path)
    mock = NativeMock()
    result = vq.run(args, mock)
    assert result["completed"] and result["execution_passed"]
    assert not result["rollout_qualified"]
    assert result["attempted_cases"] == {"v1": 24, "v2": 16}
    assert result["quality_v1"]["primary"]["correct"] == 22
    assert result["quality_v1"]["auxiliary_diagnostic"]["correct"] == 2
    assert result["quality_v2"]["all"] == {"correct": 16, "total": 16, "missing": 0}
    assert set(result["answer_paths"]) == {
        "memory_verified",
        "source_fallback_verified",
        "abstained",
    }
    stored = json.loads(args.output.with_name("dev-features.json").read_text())
    rows = vq.feature_rows(stored)
    for path, body in mock.calls:
        if path == "memorilla/generate":
            assert body["document_embeddings"] == [
                rows[vq.document_key(t)] for t in body["document_texts"]
            ]
            assert not {
                "answer",
                "support_document_indices",
                "source_records",
                "expected_as_of",
                "aliases",
            } & set(body)
            assert vq.factual_cases.ANSWER_INSTRUCTION not in body["question"]


def test_request_errors_continue_with_full_denominators(tmp_path):
    mock = NativeMock(fail_first=True)
    result = vq.run(arguments(tmp_path), mock)
    assert result["completed"] and not result["execution_passed"]
    assert mock.generations == 40
    assert result["quality_v1"]["primary"]["total"] == 22
    assert result["quality_v1"]["primary"]["correct"] == 21
    assert result["quality_v2"]["all"]["total"] == 16
    assert result["results"][0]["error"]["type"] == "RuntimeError"


def test_heldout_requires_complete_matching_development(tmp_path):
    args = arguments(tmp_path, "heldout")
    args.split = "test"
    mock = NativeMock()
    with pytest.raises(ValueError, match="development-receipt"):
        vq.run(args, mock)
    assert not mock.calls and not args.output.exists()
    dev_args = arguments(tmp_path)
    vq.run(dev_args, NativeMock())
    args.development_receipt = dev_args.output
    result = vq.run(args, mock)
    assert result["attempted_cases"] == {"v1": 24, "v2": 48}
    assert result["quality_v2"]["all"] == {"correct": 48, "total": 48, "missing": 0}


def test_heldout_rejects_changed_protocol_before_requests(tmp_path):
    dev = arguments(tmp_path)
    vq.run(dev, NativeMock())
    args = arguments(tmp_path, "test")
    args.split, args.development_receipt = "test", dev.output
    args.max_new_tokens = 49
    mock = NativeMock()
    with pytest.raises(ValueError, match="protocol changed"):
        vq.run(args, mock)
    assert not mock.calls


@pytest.mark.parametrize(
    "failure",
    ["wrong_fact", "answerable_abstention", "negative_wrong", "failed_request"],
)
def test_heldout_rejects_failed_quality_despite_cached_passing_scores(
    tmp_path, failure
):
    dev = arguments(tmp_path)
    vq.run(dev, NativeMock())
    receipt = json.loads(dev.output.read_text())
    if failure == "negative_wrong":
        result = next(
            r for r in receipt["results"] if r["case_id"] == "missing_bowl_shelf_a"
        )
        result["answer"] = "third shelf"
    else:
        result = receipt["results"][0]
        if failure == "failed_request":
            result["execution_passed"] = False
        else:
            result["answer"] = (
                "UNKNOWN"
                if failure == "answerable_abstention"
                else "wrong source entity"
            )
    # Cached score/summaries intentionally remain passing: the gate must rescore.
    vq.save(dev.output, receipt)
    args = arguments(tmp_path, "heldout")
    args.split, args.development_receipt = "test", dev.output
    mock = NativeMock()
    with pytest.raises(ValueError, match="quality target|failed requests"):
        vq.run(args, mock)
    assert not mock.calls and not args.output.exists()


@pytest.mark.parametrize(
    "path", ["memory_verified", "source_fallback_verified", "abstained"]
)
def test_valid_response_paths_are_consistent(path):
    validate(response(["raw document"], path=path))


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_alignment", "verified"),
        ("context_source", "supplied_embeddings"),
        ("document_text_sha256s", ["0" * 64]),
        ("memory_tokens", 0),
        ("finish_reason", "length"),
        ("verification_status", "unverified"),
        ("answer_path", "some_fallback"),
        ("token_ids", [True]),
        ("encoder_execution_identity", "wrong"),
        ("text", "UNKNOWN"),
    ],
)
def test_response_contract_rejects_invalid_supported_metadata(field, value):
    actual = response(["raw document"])
    actual[field] = value
    with pytest.raises(ValueError):
        validate(actual)


@pytest.mark.parametrize(
    "field,value",
    [
        ("text", "Alice"),
        ("token_ids", [42]),
        ("finish_reason", "eos"),
        ("memory_tokens", 16),
    ],
)
def test_abstention_cannot_publish_an_answer_or_tokens(field, value):
    actual = response(["raw document"], path="abstained")
    actual[field] = value
    with pytest.raises(ValueError):
        validate(actual)


@pytest.mark.parametrize(
    "field,value",
    [
        ("verification_calls", 3),
        ("verification_calls", 2),
        ("verification_calls", 0),
        ("source_fallback_used", True),
        ("total_completion_tokens", 0),
        ("completion_tokens", 2),
        ("total_prompt_tokens", 1),
    ],
)
def test_inconsistent_usage_rejected(field, value):
    actual = response(["raw document"])
    actual["usage"][field] = value
    with pytest.raises(ValueError):
        validate(actual)


def test_feature_hash_tamper_and_previous_receipt_rejected(tmp_path):
    args = arguments(tmp_path)
    vq.run(args, NativeMock())
    with pytest.raises(ValueError, match="previous receipts"):
        vq.run(args, NativeMock())
    store = json.loads(args.output.with_name("dev-features.json").read_text())
    store["documents"][0]["embedding_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="identity mismatch"):
        vq.feature_rows(store)


def test_memory_publication_still_accounts_for_independent_source_work():
    actual = response(["raw document"], path="memory_verified")
    validate(actual)
    assert actual["usage"]["source_fallback_used"] is False
    for key, public_key in [
        ("total_prompt_tokens", "prompt_tokens"),
        ("total_completion_tokens", "completion_tokens"),
    ]:
        invalid = copy.deepcopy(actual)
        invalid["usage"][key] = invalid["usage"][public_key]
        with pytest.raises(ValueError, match="usage"):
            validate(invalid)


def test_policy_abstention_selects_source_path_flag():
    actual = response(["raw document"], path="abstained")
    actual["usage"]["source_fallback_used"] = False
    with pytest.raises(ValueError, match="usage"):
        validate(actual)


def test_actual_native_openai_embedding_response_and_direct_array_shape():
    saved = json.loads(
        (vq.HERE / "fixtures/native-embedding-response.json").read_text()
    )
    actual = saved["response"]
    assert actual["object"] == "list"
    rows = vq.producer_vectors(actual, 2)
    assert rows == [item["embedding"] for item in actual["data"]]
    assert (
        vq.producer_vectors({"model_identity": vq.IDENTITY, "embeddings": rows}, 2)
        == rows
    )
    invalid = copy.deepcopy(actual)
    invalid["data"].reverse()
    with pytest.raises(ValueError, match="indices"):
        vq.producer_vectors(invalid, 2)


def test_entire_runner_handles_openai_embedding_producer(tmp_path):
    class OpenAIProducer(NativeMock):
        def post(self, path, body):
            value = super().post(path, body)
            if path == "embed":
                return {
                    "model_identity": value["model_identity"],
                    "object": "list",
                    "data": [
                        {"object": "embedding", "embedding": row, "index": i}
                        for i, row in enumerate(value["embeddings"])
                    ],
                }
            return value

    result = vq.run(arguments(tmp_path), OpenAIProducer())
    assert result["completed"] and result["execution_passed"]
    assert result["attempted_cases"] == {"v1": 24, "v2": 16}
