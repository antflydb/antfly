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

"""Tests for canonical /api/extract classification extraction.

Matches Go antfly's classifier_test.go patterns.
"""

import pytest

from .helpers import assert_openai_list_response

pytestmark = pytest.mark.model_integration


def test_classify_native_safetensors_classifier_smoke(api):
    """Explicit safetensors classifier model should return a valid classification batch."""
    resp = api.classify(
        model="cross-encoder/nli-distilroberta-base",
        text=["The new iPhone has an impressive camera system with advanced AI features."],
        labels=["technology", "sports", "politics", "entertainment"],
    )
    assert resp["model"] == "cross-encoder/nli-distilroberta-base"
    assert_openai_list_response(resp, expected_len=1)
    classifications = [item["classifications"] for item in resp["data"]]
    assert len(classifications) == 1
    assert len(classifications[0]) == 4

    total = 0.0
    for item in classifications[0]:
        assert item["label"] in {"technology", "sports", "politics", "entertainment"}
        assert 0.0 <= item["score"] <= 1.0
        total += item["score"]
    assert abs(total - 1.0) < 1e-3


def test_classify_native_safetensors_deberta_smoke(api):
    """Explicit native DeBERTa classifier model should return a valid classification batch."""
    resp = api.classify(
        model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli",
        text=["The new iPhone has an impressive camera system with advanced AI features."],
        labels=["technology", "sports", "politics", "entertainment"],
    )
    assert resp["model"] == "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli"
    classifications = [item["classifications"] for item in resp["data"]]
    assert len(classifications) == 1
    assert len(classifications[0]) == 4

    total = 0.0
    for item in classifications[0]:
        assert item["label"] in {"technology", "sports", "politics", "entertainment"}
        assert 0.0 <= item["score"] <= 1.0
        total += item["score"]
    assert abs(total - 1.0) < 1e-3


def test_classify_single_text(api):
    """iPhone text should classify as technology."""
    resp = api.classify(
        text=["The new iPhone 15 Pro has an impressive camera system with advanced AI features."],
        labels=["technology", "sports", "politics", "entertainment"],
    )
    classifications = [item["classifications"] for item in resp["data"]]
    assert len(classifications) == 1
    assert len(classifications[0]) > 0

    top = max(classifications[0], key=lambda x: x["score"])
    assert top["label"] == "technology", f"Expected 'technology', got '{top['label']}'"


def test_classify_multiple_texts(api):
    """Each text should match its expected top label."""
    texts = [
        "The Lakers won the championship last night with an amazing comeback.",
        "The new climate bill passed the Senate with bipartisan support.",
        "Taylor Swift announced her new world tour dates for 2025.",
    ]
    labels = ["sports", "politics", "entertainment", "business"]
    expected = ["sports", "politics", "entertainment"]

    resp = api.classify(text=texts, labels=labels)
    classifications = [item["classifications"] for item in resp["data"]]
    assert len(classifications) == len(texts)

    for i, (cls, exp) in enumerate(zip(classifications, expected)):
        top = max(cls, key=lambda x: x["score"])
        assert top["label"] == exp, f"Text {i}: expected '{exp}', got '{top['label']}'"


def test_classify_multi_label(api):
    """Tech company stock news should score high for both 'technology' and 'business'."""
    resp = api.classify(
        text=["The tech company's stock surged after announcing record quarterly earnings."],
        labels=["technology", "business", "sports", "politics"],
        multi_label=True,
    )
    classifications = [item["classifications"] for item in resp["data"]]
    scores = {c["label"]: c["score"] for c in classifications[0]}
    assert scores.get("technology", 0) > 0.3, f"Technology score too low: {scores}"
    assert scores.get("business", 0) > 0.3, f"Business score too low: {scores}"
