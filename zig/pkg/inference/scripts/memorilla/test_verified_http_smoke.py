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

"""Transport-only source-mode smoke contracts; never executes a native model."""

import argparse
import copy
import json

import pytest

import verified_http_smoke as smoke


def args_and_store(tmp_path):
    suite = smoke.quality_cases.load_suite(smoke.quality_cases.DEFAULT_SUITE)
    cases = [
        c for c in suite["cases"] if c["id"] in {"copper_badge_a", "copper_badge_b"}
    ]
    docs = list(dict.fromkeys(t for c in cases for t in c["documents"]))
    store = {
        "schema": smoke.quality_eval.FEATURE_SCHEMA,
        "encoder_execution_identity": smoke.IDENTITY,
        "documents": [],
    }
    for i, text in enumerate(docs):
        row = [0.0] * 2560
        row[i] = 1.0
        store["documents"].append(
            {
                "text": text,
                "text_sha256": smoke.quality_eval.document_key(text),
                "embedding": row,
                "embedding_sha256": smoke.hashlib.sha256(
                    smoke.quality_eval.json_bytes(row)
                ).hexdigest(),
            }
        )
    features = tmp_path / "native-features.json"
    features.write_text(json.dumps(store))
    return argparse.Namespace(
        output=tmp_path / "smoke.json",
        features=features,
        base_url="http://127.0.0.1:18091",
        profile="memorilla-stage3",
        module_sha256=smoke.STAGE3_SHA256,
        native_binary=None,
        max_new_tokens=48,
        timeout_ms=300000,
    )


def verified_response(body, answer):
    return {
        "model": body["model"],
        "text": answer,
        "token_ids": [42],
        "finish_reason": "eos",
        "input_documents": len(body["document_texts"]),
        "memory_tokens": 0,
        "context_source": "supplied_embeddings_and_documents",
        "answer_path": "source_fallback_verified",
        "verification_status": "model_supported",
        "source_alignment": "caller_supplied",
        "document_text_sha256s": [
            smoke.quality_eval.document_key(t) for t in body["document_texts"]
        ],
        "encoder_execution_identity": smoke.IDENTITY,
        "decoder_execution_identity": smoke.verified.DECODER_IDENTITY,
        "memory_module_sha256": smoke.STAGE3_SHA256,
        "usage": {
            "encoder_query_tokens": 10,
            "prompt_tokens": 100,
            "completion_tokens": 1,
            "total_prompt_tokens": 300,
            "total_completion_tokens": 5,
            "verification_calls": 1,
            "source_fallback_used": True,
        },
    }


class MockClient:
    def __init__(self, args, *, wrong=False, change_recovery=False, extra_legacy=False):
        self.calls = []
        self.cases, self.bodies = smoke.load_inputs(args.features, args.profile, 48)
        self.wrong, self.change_recovery, self.extra_legacy = (
            wrong,
            change_recovery,
            extra_legacy,
        )

    def post(self, body, expected, *, label, timeout_ms=None):
        self.calls.append((copy.deepcopy(body), expected, label, timeout_ms))
        if expected != {200}:
            return {"error": "mocked expected rejection"}
        if label == "legacy_latent":
            response = verified_response(self.bodies[0], "no factual quality claim")
            response["context_source"] = "supplied_embeddings"
            response["memory_tokens"] = 16
            for key in (
                "answer_path",
                "verification_status",
                "source_alignment",
                "document_text_sha256s",
            ):
                response.pop(key)
            response["usage"] = {
                "encoder_query_tokens": 10,
                "prompt_tokens": 59,
                "completion_tokens": 1,
            }
            if self.extra_legacy:
                response["verification_status"] = "model_supported"
            return response
        index = 0 if body["document_texts"] == self.bodies[0]["document_texts"] else 1
        answer = self.cases[index]["answer"]["aliases"][0]
        if self.wrong:
            answer = "wrong mocked answer"
        response = verified_response(body, answer)
        if self.change_recovery and label == "A_after_deadline":
            response["usage"]["total_completion_tokens"] += 1
        return response


def test_full_smoke_sequence_uses_saved_valid_features_and_checks_answers(tmp_path):
    args = args_and_store(tmp_path)
    client = MockClient(args)
    result = smoke.run(args, client)
    assert result["passed"] and not result["rollout_qualified"]
    assert all(result["checks"].values())
    labels = [call[2] for call in client.calls]
    assert labels[-8:-5] == ["A_first", "B", "A_after_B"]
    assert set(labels[-5:-3]) == {"A_concurrent", "B_concurrent"}
    assert labels[-3:] == ["deadline_1ms", "A_after_deadline", "legacy_latent"]
    assert next(c for c in client.calls if c[2] == "deadline_1ms")[1:] == (
        {408, 504},
        "deadline_1ms",
        1,
    )
    for body, _, _, _ in client.calls:
        assert not {"answer", "aliases", "support_document_indices"} & body.keys()
        for row in body["document_embeddings"]:
            smoke.quality_eval.validate_row(row)
    legacy = client.calls[-1][0]
    assert "context_mode" not in legacy and "document_texts" not in legacy
    with pytest.raises(ValueError, match="previous"):
        smoke.run(args, client)


def test_negatives_have_valid_features_and_independent_source_bounds(tmp_path):
    args = args_and_store(tmp_path)
    _, bodies = smoke.load_inputs(args.features, args.profile, 48)
    cases = {
        label: (body, expected)
        for label, body, expected in smoke.negative_cases(bodies[0])
    }
    for label, (body, _) in cases.items():
        if label != "producer_identity":
            assert body["encoder_execution_identity"] == smoke.IDENTITY
        for row in body["document_embeddings"]:
            smoke.quality_eval.validate_row(row)
    assert (
        len(cases["individual_source_bytes"][0]["document_texts"][0].encode()) == 8193
    )
    aggregate = cases["aggregate_source_bytes"][0]
    assert max(len(t.encode()) for t in aggregate["document_texts"]) <= 8192
    assert sum(len(t.encode()) for t in aggregate["document_texts"]) > 32768
    assert (
        len(aggregate["document_texts"]) == len(aggregate["document_embeddings"]) == 5
    )
    framing = cases["source_framing_tokens"][0]
    assert max(len(t.encode()) for t in framing["document_texts"]) <= 8192
    assert sum(len(t.encode()) for t in framing["document_texts"]) < 32768
    assert framing["document_texts"][0].split() == ["source"] * 1100
    assert cases["unknown_profile"][1] == {404}
    assert cases["producer_identity"][1] == {409}
    assert cases["explicit_latent_with_sources"][1] == {400}


@pytest.mark.parametrize(
    "option,match",
    [
        ("wrong", "factually wrong"),
        ("change_recovery", "changed after deadline"),
        ("extra_legacy", "source-mode-only"),
    ],
)
def test_failed_quality_recovery_or_legacy_contract_preserved(tmp_path, option, match):
    args = args_and_store(tmp_path)
    with pytest.raises(ValueError, match=match):
        smoke.run(args, MockClient(args, **{option: True}))
    result = json.loads(args.output.read_text())
    assert not result["passed"] and result["error"]["type"] == "ValueError"


def test_tampered_feature_provenance_rejected_before_http(tmp_path):
    args = args_and_store(tmp_path)
    store = json.loads(args.features.read_text())
    store["encoder_execution_identity"] = "wrong"
    args.features.write_text(json.dumps(store))
    with pytest.raises(ValueError, match="identity"):
        smoke.run(args)
    assert not args.output.exists()


def test_http_transport_checks_status_and_records_deadline(tmp_path, monkeypatch):
    args = args_and_store(tmp_path)
    _, bodies = smoke.load_inputs(args.features, args.profile, 48)
    events, checkpoints = [], []

    class Reply:
        status = 404

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def read(self):
            return b'{"error":"MEMORILLA_DISABLED"}'

    def transport(request, **kwargs):
        assert request.full_url.endswith("/ai/v1/memorilla/generate")
        assert request.get_header("X-antfly-inference-timeout-ms") == "1"
        assert json.loads(request.data) == bodies[0]
        return Reply()

    monkeypatch.setattr(smoke, "urlopen", transport)
    client = smoke.Client(args, events, lambda: checkpoints.append(True))
    assert (
        client.post(bodies[0], {404}, label="mock_disabled", timeout_ms=1)["error"]
        == "MEMORILLA_DISABLED"
    )
    assert events[0]["status"] == 404 and events[0]["timeout_ms"] == 1 and checkpoints
    with pytest.raises(ValueError, match="received 404"):
        client.post(bodies[0], {200}, label="must_not_ignore_status", timeout_ms=1)
