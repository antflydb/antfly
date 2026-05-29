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

"""Tests for canonical /api/extract entity and relation extraction.

Matches Go antfly's gliner_test.go patterns.
GLiNER models use zero-shot NER with user-specified entity labels.
"""

import pytest

from .helpers import assert_openai_list_response

pytestmark = pytest.mark.model_integration

GLINER_MODEL = "fastino/gliner2-base-v1"
REBEL_MODEL = "Babelscape/rebel-large"
NATIVE_BERT_NER_MODEL = "dslim/bert-base-NER"
NATIVE_DEBERTA_NER_MODEL = "mukuls9971/pii-deberta-v3-xsmall"


def _entities(resp):
    return [item["entities"] for item in resp["data"]]


def _relations(resp):
    return [item.get("relations", []) for item in resp["data"]]


def test_recognize_entities(api):
    """Should extract entities from text using default labels."""
    resp = api.recognize(
        text=["John Smith works at Google in Mountain View."],
        labels=["person", "organization", "location"],
        model=GLINER_MODEL,
    )
    assert_openai_list_response(resp, expected_len=1)
    entities = _entities(resp)
    assert len(entities) == 1
    assert len(entities[0]) > 0, "Should find at least one entity"

    for ent in entities[0]:
        assert "text" in ent
        assert "label" in ent
        assert "start" in ent
        assert "end" in ent


def test_recognize_with_custom_labels(api):
    """Custom labels should restrict entity types."""
    resp = api.recognize(
        text=["I bought a Tesla Model 3 on January 15th."],
        labels=["product", "company", "date", "vehicle"],
        model=GLINER_MODEL,
    )
    entities = _entities(resp)
    assert len(entities) == 1

    valid_labels = {"product", "company", "date", "vehicle"}
    for ent in entities[0]:
        assert ent["label"] in valid_labels, f"Unexpected label: {ent['label']}"


def test_recognize_with_relation_labels(api):
    """Relation requests should include a relations array when supported."""
    resp = api.recognize(
        text=["John Smith works at Google in Mountain View."],
        labels=["person", "organization", "location"],
        relation_labels=["works_for", "located_in"],
        model=GLINER_MODEL,
    )
    relations = _relations(resp)
    assert len(relations) == 1
    if relations[0]:
        rel = relations[0][0]
        assert "head" in rel
        assert "tail" in rel
        assert "label" in rel
        assert "score" in rel


def test_recognize_with_resolver(api):
    """Multi-input entity extraction should preserve one response object per input text."""
    resp = api.recognize(
        text=["Elon Musk founded SpaceX.", "Musk also runs Tesla."],
        labels=["person", "organization"],
        relation_labels=["founded", "runs"],
        model=GLINER_MODEL,
    )
    assert_openai_list_response(resp, expected_len=2)
    entities = _entities(resp)
    assert len(entities) == 2
    if entities[0]:
        entity = entities[0][0]
        assert "text" in entity
        assert "label" in entity
        assert "score" in entity
        assert "start" in entity
        assert "end" in entity


def test_recognize_rebel_relations(api):
    """REBEL-style recognizers should return relation edges through /api/extract."""
    try:
        resp = api.recognize(
            text=["Barack Obama was born in Hawaii and worked for the United States government."],
            labels=["person", "location", "organization"],
            relation_labels=["born in", "worked for"],
            model=REBEL_MODEL,
        )
    except Exception as exc:
        pytest.skip(f"REBEL model {REBEL_MODEL} is present but not loadable in this build: {exc}")
    assert len(_entities(resp)) == 1
    assert len(_relations(resp)) == 1


def test_recognize_native_safetensors_bert_token_classifier(api):
    """Native WordPiece recognizers should return non-empty spans through /api/extract."""
    recognizers = api.models().get("recognizers", {})
    if NATIVE_BERT_NER_MODEL not in recognizers:
        pytest.skip(f"No local recognizer model is available for {NATIVE_BERT_NER_MODEL}")

    resp = api.recognize(
        text=["John Smith works at Google in Mountain View."],
        model=NATIVE_BERT_NER_MODEL,
    )
    entities = _entities(resp)
    assert len(entities) == 1
    assert len(entities[0]) > 0

    for ent in entities[0]:
        assert ent["text"]
        assert ent["end"] > ent["start"]


def test_recognize_native_safetensors_deberta_token_classifier(api):
    """Native DeBERTa recognizers should merge fragmented subword spans."""
    recognizers = api.models().get("recognizers", {})
    if NATIVE_DEBERTA_NER_MODEL not in recognizers:
        pytest.skip(f"No local recognizer model is available for {NATIVE_DEBERTA_NER_MODEL}")

    resp = api.recognize(
        text=["Reach Jane at jane.smith@example.org or 203.0.113.42."],
        model=NATIVE_DEBERTA_NER_MODEL,
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
