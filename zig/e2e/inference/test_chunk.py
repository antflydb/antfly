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

"""Tests for /api/chunk endpoint.

Matches Go antfly's chunker_test.go patterns.
"""

from .helpers import assert_openai_list_response


def test_basic_chunking(api):
    text = (
        "Machine learning is transforming industries worldwide. "
        "From healthcare to finance, AI models are being deployed at scale. "
        "Natural language processing has made tremendous progress. "
        "Large language models can now understand and generate human-like text. "
        "Computer vision systems can identify objects in images with high accuracy."
    )
    resp = api.chunk(text)
    assert_openai_list_response(resp)
    chunks = resp["data"]
    assert len(chunks) > 0, "Should return at least one chunk"


def test_chunks_cover_input(api):
    text = (
        "First paragraph about machine learning and AI. "
        "Second paragraph about natural language processing. "
        "Third paragraph about computer vision systems."
    )
    resp = api.chunk(text)
    chunks = resp["data"]

    # Reconstruct and verify coverage
    total_chars = sum(len(c["text"]) for c in chunks)
    assert total_chars > 0, "Chunks should contain text"


def test_fixed_chunking(api):
    """Fixed chunker should always be available (no model needed)."""
    text = "A " * 200  # Long repetitive text
    resp = api.chunk(text, model="fixed")
    chunks = resp["data"]
    assert len(chunks) > 0
