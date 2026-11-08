# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Mocked protocol/transport tests; no model is loaded."""

import argparse
import copy
import json

import pytest

import causal_probe as cp


def row(index):
    values = [0.0] * 2560
    values[index] = 1.0
    return values


def arguments(tmp_path):
    _, cases = cp.protocol(cp.HERE / "fixtures/quality_v1.json")
    documents = list(dict.fromkeys(t for c in cases for t in c["documents"]))
    store = {
        "schema": cp.qe.FEATURE_SCHEMA,
        "encoder_execution_identity": cp.qe.IDENTITY,
        "documents": [],
    }
    for index, text in enumerate(documents):
        values = row(index)
        store["documents"].append(
            {
                "text": text,
                "text_sha256": cp.qe.document_key(text),
                "embedding": values,
                "embedding_sha256": cp.qe.sha256(cp.qe.json_bytes(values)),
            }
        )
    feature_path = tmp_path / "source.json"
    cp.qe.save_json(feature_path, store)
    return argparse.Namespace(
        output=tmp_path / "probe",
        features=feature_path,
        suite=cp.HERE / "fixtures/quality_v1.json",
        protocol_only=False,
        profile="memorilla-stage3",
        encoder="encoder",
        base_url="http://127.0.0.1:18092",
        timeout_ms=300000,
    )


class Client:
    def __init__(self):
        self.calls = []

    def post(self, path, body):
        self.calls.append((path, copy.deepcopy(body)))
        if path == "embed":
            assert set(body) == {"model", "input", "task_type"}
            assert body["task_type"] == "RETRIEVAL_QUERY"
            return {"model_identity": cp.qe.IDENTITY, "embeddings": [row(0)]}
        assert path == "memorilla/generate"
        return {
            "model": body["model"],
            "text": "Unknown",
            "token_ids": [1],
            "finish_reason": "eos",
            "input_documents": len(body["document_embeddings"]),
            "memory_tokens": 16,
            "context_source": "supplied_embeddings",
            "encoder_execution_identity": cp.qe.IDENTITY,
            "decoder_execution_identity": cp.qe.DECODER_IDENTITY,
            "memory_module_sha256": cp.qe.STAGE3_SHA256,
            "usage": {
                "completion_tokens": 1,
                "encoder_query_tokens": 12,
                "prompt_tokens": 50,
            },
        }


def test_fixed_factorial_records_exact_questions_and_full_evidence(tmp_path):
    args = arguments(tmp_path)
    client = Client()
    result = cp.run(args, client)
    assert result["completed"] and result["execution_passed"]
    assert result["rollout_qualified"] is False
    assert len(result["results"]) == 36
    assert sum(path == "embed" for path, _ in client.calls) == 6
    assert sum(path == "memorilla/generate" for path, _ in client.calls) == 36
    assert len(list(args.output.glob("http-*.response.json"))) == 42
    assert result["feature_provenance"]["json_round_trip_equal"]
    assert all(v["total"] == v["completed"] == 6 for v in result["summary"].values())
    assert all(v["correct"] == 0 for v in result["summary"].values())
    locked = json.loads((args.output / "protocol.json").read_text())
    assert "not an isolated" in locked["question_intervention"]
    assert not locked["selection_uses_answers"]
    for case in locked["cases"]:
        rows = [r for r in result["results"] if r["case_id"] == case["case_id"]]
        assert len(rows) == 6
        for mode, question in [
            ("authored", case["authored_question"]),
            ("bare_semantic", case["bare_semantic_question"]),
        ]:
            assert {r["question"] for r in rows if r["question_mode"] == mode} == {
                question
            }
        oracle = [r for r in rows if r["collection_mode"] == "oracle_support"]
        assert all(r["oracle_diagnostic"] for r in oracle)
        assert all(
            r["selected_document_indices"] == case["oracle_support_indices"]
            for r in oracle
        )
        retrieved = [r for r in rows if r["collection_mode"] == "cosine_topk"]
        assert (
            retrieved[0]["selected_document_indices"]
            == retrieved[1]["selected_document_indices"]
        )
        assert len(retrieved[0]["selected_document_indices"]) == case["retrieval_k"]
    for path, body in client.calls:
        if path == "memorilla/generate":
            assert set(body) == {
                "model",
                "question",
                "document_embeddings",
                "encoder_execution_identity",
                "max_new_tokens",
            }
            assert body["max_new_tokens"] == 48
    assert json.loads((args.output / "result.json").read_text()) == result


def test_retrieval_uses_only_vectors_ties_stably_and_preserves_source_order():
    result = cp.cosine_topk([row(1), row(0), row(0)], row(0), 2)
    assert [v["index"] for v in result["ranking"]] == [1, 2, 0]
    assert result["selected_indices"] == [1, 2]
    result = cp.cosine_topk([row(0), row(1)], row(2), 1)
    assert result["selected_indices"] == [0]
    assert cp.fp16_stored([0.123456])[0] != 0.123456
    with pytest.raises(ValueError):
        cp.cosine_topk([row(0)], row(0), 2)


def test_actual_openai_embedding_wire_shape_is_supported(tmp_path):
    args = arguments(tmp_path)

    class OpenAI(Client):
        def post(self, path, body):
            value = super().post(path, body)
            if path == "embed":
                value["data"] = [
                    {
                        "object": "embedding",
                        "index": 0,
                        "embedding": value.pop("embeddings")[0],
                    }
                ]
            return value

    result = cp.run(args, OpenAI())
    assert result["completed"] and len(result["results"]) == 36


@pytest.mark.parametrize("mutation", ["identity", "dimensions", "nan"])
def test_bad_query_producer_blocks_all_generation_and_keeps_denominators(
    tmp_path, mutation
):
    args = arguments(tmp_path)

    class Bad(Client):
        def post(self, path, body):
            value = super().post(path, body)
            if mutation == "identity":
                value["model_identity"] = "0" * 64
            elif mutation == "dimensions":
                value["embeddings"] = [[1.0]]
            else:
                value["embeddings"][0][0] = float("nan")
            return value

    client = Bad()
    with pytest.raises(ValueError):
        cp.run(args, client)
    assert all(path == "embed" for path, _ in client.calls)
    result = json.loads((args.output / "result.json").read_text())
    assert not result["completed"] and not result["execution_passed"]
    assert all(
        v["total"] == 6 and v["completed"] == 0 for v in result["summary"].values()
    )


def test_partial_transport_failure_retains_success_and_missing_cases(tmp_path):
    args = arguments(tmp_path)

    class Fails(Client):
        def post(self, path, body):
            if (
                path == "memorilla/generate"
                and sum(p == path for p, _ in self.calls) == 1
            ):
                raise RuntimeError("declared transport failure")
            return super().post(path, body)

    with pytest.raises(RuntimeError):
        cp.run(args, Fails())
    result = json.loads((args.output / "result.json").read_text())
    assert len(result["results"]) == 1
    assert result["summary"]["authored/full_collection"]["completed"] == 1
    assert len(result["summary"]["authored/full_collection"]["missing_case_ids"]) == 5
    assert all(v["total"] == 6 for v in result["summary"].values())
    assert len(list(args.output.glob("http-*.error.json"))) == 1


def test_tampered_document_store_fails_before_http(tmp_path):
    args = arguments(tmp_path)
    store = json.loads(args.features.read_text())
    store["documents"][0]["embedding"] = row(2559)
    cp.qe.save_json(args.features, store)
    client = Client()
    with pytest.raises(ValueError, match="digest"):
        cp.run(args, client)
    assert client.calls == []
    result = json.loads((args.output / "result.json").read_text())
    assert all(v["total"] == 6 for v in result["summary"].values())


def test_protocol_only_never_calls_network_and_existing_output_is_preserved(tmp_path):
    args = arguments(tmp_path)
    args.protocol_only = True
    args.features = None
    client = Client()
    value = cp.run(args, client)
    assert value["expected_generation_calls"] == 36 and client.calls == []
    before = (args.output / "protocol.json").read_bytes()
    with pytest.raises(ValueError, match="Preserve"):
        cp.run(args, client)
    assert (args.output / "protocol.json").read_bytes() == before


def test_changed_suite_is_rejected_before_loading_its_cases(tmp_path):
    path = tmp_path / "modified.json"
    path.write_text("{}")
    with pytest.raises(ValueError, match="frozen v1"):
        cp.protocol(path)
