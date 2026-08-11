# Copyright 2026 Antfly, Inc.
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

"""Tests for canonical /ai/v1/extract entity and relation modes.

Matches Go antfly's gliner_test.go patterns.
GLiNER models use zero-shot NER with user-specified entity labels.
"""

import pytest

from .helpers import assert_extraction_response
from .models import DEFAULT_EXTRACTOR_MODEL, run_large_model_tests

pytestmark = pytest.mark.model_integration

GLINER_MODEL = DEFAULT_EXTRACTOR_MODEL
REBEL_MODEL = "Babelscape/rebel-large"
NATIVE_BERT_NER_MODEL = "dslim/bert-base-NER"
NATIVE_DEBERTA_NER_MODEL = "mukuls9971/pii-deberta-v3-xsmall"


def _entities(resp):
    return [item["entities"] for item in resp["data"]]


def _relations(resp):
    return [item.get("relations", []) for item in resp["data"]]


def test_extract_entities(api):
    """Should extract entities from text using default labels."""
    resp = api.extract_entities(
        text=["John Smith works at Google in Mountain View."],
        labels=["person", "organization", "location"],
        model=GLINER_MODEL,
    )
    assert_extraction_response(resp, expected_len=1)
    entities = _entities(resp)
    assert len(entities) == 1
    assert len(entities[0]) > 0, "Should find at least one entity"

    for ent in entities[0]:
        assert "text" in ent
        assert "label" in ent


def test_extract_entities_with_custom_labels(api):
    """Custom labels should restrict entity types."""
    resp = api.extract_entities(
        text=["I bought a Tesla Model 3 on January 15th."],
        labels=["product", "company", "date", "vehicle"],
        model=GLINER_MODEL,
    )
    assert_extraction_response(resp, expected_len=1)
    entities = _entities(resp)
    assert len(entities) == 1

    valid_labels = {"product", "company", "date", "vehicle"}
    for ent in entities[0]:
        assert ent["label"] in valid_labels, f"Unexpected label: {ent['label']}"


def test_extract_relations(api):
    """Relation requests should include a relations array when supported."""
    resp = api.extract_entities(
        text=["John Smith works at Google in Mountain View."],
        labels=["person", "organization", "location"],
        relations=["works_for", "located_in"],
        model=GLINER_MODEL,
        include_confidence=True,
    )
    assert_extraction_response(resp, expected_len=1)
    relations = _relations(resp)
    assert len(relations) == 1
    if relations[0]:
        rel = relations[0][0]
        assert "source" in rel
        assert "target" in rel
        assert "type" in rel
        assert "score" in rel


def test_extract_relations_with_resolver(api):
    """Multi-input entity extraction should preserve one response object per input text."""
    texts = ["Elon Musk founded SpaceX.", "Musk also runs Tesla."]
    resp = api.extract_entities(
        text=texts,
        labels=["person", "organization"],
        relations=["founded", "runs"],
        resolver={"similarity_threshold": 0.85},
        model=GLINER_MODEL,
        include_confidence=True,
        include_spans=True,
    )
    assert_extraction_response(resp, expected_len=2)
    entities = _entities(resp)
    assert len(entities) == 2
    for input_index, batch in enumerate(entities):
        for entity in batch:
            assert "label" in entity
            assert "score" in entity
            assert "start" in entity
            assert "end" in entity
            assert texts[input_index][entity["start"]:entity["end"]] == entity["text"]


def test_extract_rebel_relations(api):
    """REBEL-style extractors should return relation edges through /ai/v1/extract."""
    if not run_large_model_tests():
        pytest.skip("REBEL relation extraction uses a large model; set RUN_LARGE_MODEL_TESTS=1 to run it")
    resp = api.extract_entities(
        text=["Barack Obama was born in Hawaii and worked for the United States government."],
        labels=["person", "location", "organization"],
        relations=["born in", "worked for"],
        model=REBEL_MODEL,
    )
    assert_extraction_response(resp, expected_len=1)
    assert len(_entities(resp)) == 1
    assert len(_relations(resp)) == 1


def test_retired_recognize_route_remains_absent(api):
    """The removed recognition endpoint must not silently return or skip as a model miss."""
    response = api.post(
        "/recognize",
        json={"model": GLINER_MODEL, "texts": ["John Smith works at Google."]},
    )
    assert response.status_code == 404
    if response.headers.get("content-type", "").startswith("application/json"):
        assert response.json().get("error") != "MODEL_NOT_FOUND"


def test_extract_native_safetensors_bert_token_classifier(api):
    """Native WordPiece extractors should return non-empty entity spans."""
    extractors = api.models().get("extractors", {})
    if NATIVE_BERT_NER_MODEL not in extractors:
        pytest.skip(f"No local extractor model is available for {NATIVE_BERT_NER_MODEL}")

    resp = api.extract_entities(
        text=["John Smith works at Google in Mountain View."],
        model=NATIVE_BERT_NER_MODEL,
        include_spans=True,
    )
    entities = _entities(resp)
    assert len(entities) == 1
    assert len(entities[0]) > 0

    for ent in entities[0]:
        assert ent["text"]
        assert ent["end"] > ent["start"]


def test_extract_native_safetensors_deberta_token_classifier(api):
    """Native DeBERTa extractors should merge fragmented subword spans."""
    extractors = api.models().get("extractors", {})
    if NATIVE_DEBERTA_NER_MODEL not in extractors:
        pytest.skip(f"No local extractor model is available for {NATIVE_DEBERTA_NER_MODEL}")

    resp = api.extract_entities(
        text=["Reach Jane at jane.smith@example.org or 203.0.113.42."],
        model=NATIVE_DEBERTA_NER_MODEL,
        include_spans=True,
    )
    entities = _entities(resp)
    assert len(entities) == 1
    assert len(entities[0]) > 0

    texts = {ent["text"] for ent in entities[0]}
    assert "jane.smith@example.org" in texts
    assert "203.0.113.42" in texts

    for ent in entities[0]:
        assert ent["text"]
        assert ent["end"] > ent["start"]
