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

"""Tests for /api/rewrite (seq2seq) endpoint.

Matches Go antfly's rewriter_test.go patterns.
"""

import pytest

from .helpers import assert_openai_list_response

pytestmark = pytest.mark.model_integration


def test_rewrite_text(api):
    """Seq2seq rewriting should return a result with text field."""
    resp = api.rewrite(
        text=["generate question: Machine learning is a subset of artificial intelligence."],
    )
    assert_openai_list_response(resp, expected_len=1)
    results = resp["data"]
    assert len(results) == 1
    assert results[0]["texts"], "Result should contain rewritten text"


def test_rewrite_multiple_texts(api):
    texts = [
        "generate question: The Great Wall of China is one of the most famous landmarks in the world.",
        "generate question: DNA carries genetic information in living organisms.",
    ]
    resp = api.rewrite(text=texts)
    results = resp["data"]
    assert len(results) == len(texts)
    for r in results:
        assert r["texts"], "Each result should contain rewritten text"
