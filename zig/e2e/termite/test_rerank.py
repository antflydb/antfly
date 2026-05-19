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

"""Tests for /api/rerank endpoint.

Matches Go termite's rerankers_test.go patterns.
"""

import pytest

from .helpers import assert_openai_list_response

pytestmark = pytest.mark.model_integration


def test_rerank_score_ordering(api):
    """Relevant documents should score higher than irrelevant ones."""
    query = "What is machine learning?"
    documents = [
        "Machine learning is a subset of artificial intelligence that enables systems to learn from data.",
        "The weather today is sunny with a chance of rain.",
        "Deep learning uses neural networks to learn representations.",
        "Cooking pasta requires boiling water.",
    ]

    resp = api.rerank(query, documents)
    assert_openai_list_response(resp, expected_len=len(documents))
    scores = [item["score"] for item in resp["data"]]
    assert len(scores) == len(documents)

    # ML doc should beat cooking (strong signal)
    assert scores[0] > scores[3], f"ML ({scores[0]:.4f}) should beat cooking ({scores[3]:.4f})"
    # At least one ML-related doc should be in the top 2
    ml_scores = [scores[0], scores[2]]
    other_scores = [scores[1], scores[3]]
    assert max(ml_scores) > min(other_scores), (
        f"ML-related docs ({ml_scores}) should outscore at least one irrelevant ({other_scores})"
    )


def test_rerank_returns_all_scores(api):
    query = "test query"
    docs = ["doc one", "doc two", "doc three"]
    resp = api.rerank(query, docs)
    assert len(resp["data"]) == 3
