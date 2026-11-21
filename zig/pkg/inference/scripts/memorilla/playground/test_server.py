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

"""CPU tests of the playground's passage splitter, read plan and highlights."""

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

import server


def words(text: str) -> int:
    """Stand-in tokenizer: one token per whitespace-separated word."""
    return len(text.split())


def test_split_passages_mirrors_the_zig_chunker_rule():
    # The Zig test case (chunkDocument, bound 5 with EOS = 4 words here).
    text = "One two three. Four five.\nSix seven eight nine ten eleven. Twelve!"
    got = [text[a:b] for a, b in server.split_passages(text, words, 4)]
    assert got == [
        "One two three.",
        "Four five.",
        "Six seven eight nine",
        "ten eleven. Twelve!",
    ]
    assert all(words(p) <= 4 for p in got)
    # Trimmed slices of the original text, in order.
    padded = "  short text  "
    assert [padded[a:b] for a, b in server.split_passages(padded, words, 4)] == [
        "short text"
    ]
    assert server.split_passages(" \n ", words, 4) == []


def test_split_passages_keeps_decimal_points_and_codes_inside_sentences():
    text = "The ramp has a 7.5 tonne limit. Code Hq4-007315 opens it."
    assert [text[a:b] for a, b in server.split_passages(text, words, 7)] == [
        "The ramp has a 7.5 tonne limit.",
        "Code Hq4-007315 opens it.",
    ]


def test_wide_read_takes_leading_documents_within_the_budget():
    # memorilla_span.readBudget: the first always, stop at the first overflow.
    assert server.wide_read([60, 60, 60], 128) == (2, 120)
    assert server.wide_read([600, 10], 512) == (1, 600)
    assert server.wide_read([64] * 10, 512) == (8, 512)
    assert server.wide_read([10, 600, 10], 512) == (1, 10)


def test_segments_mark_the_window_and_the_span():
    text = "Gate code Kq9-0012 opens the yard."
    window = (0, 24)
    span = (10, 18)
    got = server.segments(text, window, span)
    assert got == [
        {"kind": "window", "text": "Gate code "},
        {"kind": "span", "text": "Kq9-0012"},
        {"kind": "window", "text": " opens"},
        {"kind": "plain", "text": " the yard."},
    ]
    assert "".join(s["text"] for s in got) == text
    assert server.segments(text, None, None) == [{"kind": "plain", "text": text}]


def test_byte_offsets_map_to_characters():
    text = "Zürich gate Kq9"
    start = len("Zürich gate ".encode())
    assert server.byte_to_char(text, start) == len("Zürich gate ")
    assert text[server.byte_to_char(text, start) :] == "Kq9"


def test_bm25_prefers_the_matching_passage():
    passages = [
        "The tide rises twice a day.",
        "Gate code for Harrow Quay Depot is Hq4-007315.",
    ]
    scores = server.bm25_scores(
        passages, "What is the gate code for Harrow Quay Depot?"
    )
    assert scores[1] > scores[0]


def test_cosine_order():
    scores = server.cosine_order([[1.0, 0.0], [0.6, 0.8], [0.0, 2.0]], [0.0, 1.0])
    assert [round(s, 3) for s in scores] == [0.0, 0.8, 1.0]


def test_rank_disambiguates_key_holders_and_preserves_citable_source_text():
    playground = server.Playground.__new__(server.Playground)
    playground.args = SimpleNamespace(passage_tokens=6, ranking="bm25")
    playground.count = words
    chunks = [
        "Site record: Oak Yard. The key holder is Pat.",
        "Site record: Willow Yard. The key holder is Jo.",
    ]
    passages = playground.passages(chunks)
    question = "Who is the key holder for Willow Yard?"
    scores = server.bm25_scores([p["text"] for p in passages], question)
    assert scores[1] == scores[3]  # Both holder passages lost their subject.
    ranked, _ = playground.rank(passages, question)
    assert ranked[0]["text"] == "The key holder is Jo."
    for p in ranked:
        assert chunks[p["chunk"]][p["start"] : p["end"]] == p["text"]
        assert p["tokens"] == words(p["text"])
