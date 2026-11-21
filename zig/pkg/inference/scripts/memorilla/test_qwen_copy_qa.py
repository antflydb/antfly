#!/usr/bin/env python3
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

import re
from collections import Counter

import pytest
import torch
from encoder_lora import complete_copied_words, token_continues_word
from qwen_copy_qa import (
    NEGATIVE,
    POSITIVE,
    SKIP,
    SLOTS,
    answer_char_ranges,
    answer_in_documents,
    check_record,
    covering_tokens,
    document_layout,
    example_stream,
    gate_confusion,
    heldout,
    pack_chunks,
    paired_bootstrap,
    parse_args,
    parse_samples,
    parse_weights,
    qa_record,
    resume_partial,
    select_read_set,
    select_top_chunks,
    select_top_packs,
    span_occurrences,
    word_complete,
)
from qwen_hybrid_eval import (
    QUESTION_PREFIX_TEMPLATES,
    learned_gate_open,
    parse_args as parse_hybrid_args,
    parse_paper_args,
    question_prefix_text,
    question_prefixed_documents,
    record_gate_decision,
)

PIECE = re.compile(r" ?[A-Za-z]+|[0-9]| ?[^\sA-Za-z0-9]|\s")


class Tokenizer:
    """BPE stand-in with Qwen's habits: a word carries its leading space,
    digits are single tokens with the space before them a token of its own,
    punctuation stands alone. Ids are assigned on first sight; " " is the pad."""

    is_fast = True

    def __init__(self):
        self.vocab, self.pieces = {}, []
        self.id(" ")

    def id(self, piece):
        if piece not in self.vocab:
            self.vocab[piece] = len(self.pieces)
            self.pieces.append(piece)
        return self.vocab[piece]

    def __call__(self, text, add_special_tokens=False, return_offsets_mapping=False):
        assert not add_special_tokens
        matches = list(PIECE.finditer(text))
        assert "".join(m.group(0) for m in matches) == text
        out = {"input_ids": [self.id(m.group(0)) for m in matches]}
        if return_offsets_mapping:
            out["offset_mapping"] = [(m.start(), m.end()) for m in matches]
        return out

    def encode(self, text, add_special_tokens=False):
        return self(text, add_special_tokens=add_special_tokens)["input_ids"]

    def decode(self, ids):
        return "".join(self.pieces[int(i)] for i in ids)


@pytest.fixture
def tokenizer():
    return Tokenizer()


def build(tokenizer, documents, answer, label=" answer", **kwargs):
    layout = document_layout(tokenizer, documents)
    record = qa_record(
        answer,
        layout,
        decode=tokenizer.decode,
        continues=lambda i: token_continues_word(tokenizer, i),
        label_ids=tokenizer.encode(label),
        pad_id=tokenizer.encode(" ")[0],
        **kwargs,
    )
    return layout, record


# --------------------------------------------------------------- records


def test_answer_char_ranges_normalize_case_articles_and_punctuation():
    text = " Beyonce became popular in the late 1990s (as lead singer)."
    (start, end), = answer_char_ranges(text, "The Late 1990s")
    assert text[start:end] == "late 1990s"
    (start, end), = answer_char_ranges(text, "lead singer")
    assert text[start:end] == "lead singer"
    assert answer_char_ranges(text, "early 1990s") == []
    assert answer_char_ranges(text, "...") == []


def test_positive_record_has_label_span_and_pointer(tokenizer):
    documents = ["Paris is the capital of France.", "The Eiffel Tower is in Paris."]
    layout, record = build(tokenizer, documents, "france")
    pad = tokenizer.encode(" ")[0]
    assert record["kind"] == POSITIVE
    assert tokenizer.decode(record["target_ids"]).rstrip() == " answer France"
    assert record["target_ids"][2:] == [pad] * (SLOTS - 2)
    assert record["gate"] == [0, 1] + [0] * (SLOTS - 2)
    (slot, positions), = record["pointer"]
    assert slot == 1 and [layout["ids"][p] for p in positions] == [record["target_ids"][1]]
    assert record["span_text"] == " France" and record["document"] == 0
    check_record(record, layout["ids"])


def test_span_in_second_document_points_at_every_occurrence(tokenizer):
    documents = ["Nothing here.", "Paris, and Paris again."]
    layout, record = build(tokenizer, documents, "Paris")
    assert record["kind"] == POSITIVE and record["document"] == 1
    (slot, positions), = record["pointer"]
    assert record["span_occurrences"] == 2 and len(positions) == 2
    assert all(layout["ids"][p] == record["target_ids"][slot] for p in positions)


def test_number_span_takes_its_space_by_word_completion(tokenizer):
    # " 1835" tokenizes as " ", "1", "8", "3", "5": the copied span starts at
    # the source space, exactly what complete_copied_words would emit.
    layout, record = build(tokenizer, ["He moved in 1835 to Paris."], "1835")
    assert record["kind"] == POSITIVE
    assert tokenizer.decode(record["target_ids"]).rstrip() == " answer 1835"
    assert record["gate"][:6] == [0, 1, 1, 1, 1, 1] and sum(record["gate"]) == 5
    check_record(record, layout["ids"])


def test_span_completion_keeps_attached_punctuation(tokenizer):
    # " (" + "Radiohead": the word continues back to the bracket token, as
    # complete_copied_words would widen a copied span at inference.
    _, record = build(tokenizer, ["The band (Radiohead) formed."], "Radiohead")
    assert record["kind"] == POSITIVE
    assert tokenizer.decode(record["target_ids"]).rstrip() == " answer (Radiohead"
    assert record["gate"][:3] == [0, 1, 1]


def test_span_without_leading_space_gets_a_generated_space(tokenizer):
    # After a newline the span starts with "\n": one generated space slot
    # (gate 0) keeps the label and the span apart.
    layout, record = build(tokenizer, ["Title:\nRadiohead formed."], "Radiohead")
    pad = tokenizer.encode(" ")[0]
    assert record["kind"] == POSITIVE
    assert record["target_ids"][1] == pad and record["gate"][1] == 0
    assert record["gate"][2:4] == [1, 1]
    assert tokenizer.decode(record["target_ids"]).rstrip() == " answer \nRadiohead"
    check_record(record, layout["ids"])


def test_missing_answer_is_all_pads_with_closed_gate(tokenizer):
    layout, record = build(tokenizer, ["Paris is in France."], "Berlin")
    pad = tokenizer.encode(" ")[0]
    assert record["kind"] == NEGATIVE and record["reason"] == "absent"
    assert record["target_ids"] == [pad] * SLOTS and record["gate"] == [0] * SLOTS
    assert record["pointer"] == []
    check_record(record, layout["ids"])


def test_polar_short_numeric_and_long_answers(tokenizer):
    _, polar = build(tokenizer, ["yes it is."], "Yes")
    assert polar["kind"] == NEGATIVE and polar["reason"] == "polar"
    _, short = build(tokenizer, ["a b c"], "b")
    assert short["kind"] == SKIP and short["reason"] == "short-answer"
    _, numeric = build(tokenizer, ["They scored 21 points."], "21", skip_numeric=True)
    assert numeric["kind"] == SKIP and numeric["reason"] == "numeric"
    _, kept = build(tokenizer, ["They scored 21 points."], "21")
    assert kept["kind"] == POSITIVE
    words = " ".join(f"w{chr(97 + i)}" for i in range(20))
    _, long = build(tokenizer, ["Start " + words + " end."], words)
    assert long["kind"] == SKIP and long["reason"] == "long-span"


def test_multi_token_label_shifts_the_span(tokenizer):
    _, record = build(tokenizer, ["Paris is in France."], "France", label=" the answer is")
    assert tokenizer.decode(record["target_ids"]).rstrip() == " the answer is France"
    assert record["gate"].index(1) == 3 and record["pointer"][0][0] == 3


def test_check_record_rejects_inconsistent_pointers(tokenizer):
    layout, record = build(tokenizer, ["Paris is in France."], "France")
    broken = dict(record, pointer=[[1, [0]]])
    with pytest.raises(ValueError):
        check_record(broken, layout["ids"])
    with pytest.raises(ValueError):
        check_record(dict(record, gate=[1] * SLOTS), layout["ids"])


def test_word_complete_matches_complete_copied_words(tokenizer):
    ids = tokenizer.encode(" Mozeba at Daboco Depot")
    continues = lambda i: token_continues_word(tokenizer, i)  # noqa: E731
    # A fake piece split: pretend the copy started inside a word.
    first, last = word_complete(ids, 1, 2, continues)
    pad = tokenizer.encode(" ")[0]
    completed, _ = complete_copied_words(
        [pad] * 4, [(0, 1, 1)], ids, tokenizer, pad, overflow="truncate"
    )
    assert completed[: last - first] == ids[first:last]


def test_covering_tokens_and_occurrences():
    offsets = [(0, 3), (3, 7), (7, 8), (8, 12)]
    assert covering_tokens(offsets, 4, 8) == (1, 3)
    assert covering_tokens(offsets, 12, 14) is None
    assert span_occurrences([1, 2, 1, 2, 3], [1, 2]) == [0, 2]
    assert span_occurrences([1, 2], []) == []


# ------------------------------------------------------- top-2 selection


def test_select_top_chunks_by_stored_cosine():
    question = torch.tensor([1.0, 0.0, 0.0])
    vectors = torch.tensor(
        [[0.0, 1.0, 0.0], [0.8, 0.6, 0.0], [10.0, 0.0, 1.0], [0.5, 0.5, 0.5]],
        dtype=torch.float16,
    )
    chunks = ["far", "close", "closest despite its norm", "middle"]
    texts, indices, scores = select_top_chunks(chunks, vectors, question, 2)
    assert indices == [2, 1] and texts == ["closest despite its norm", "close"]
    assert scores[0] >= scores[1]
    with pytest.raises(ValueError):
        select_top_chunks(chunks[:2], vectors, question, 2)


def test_pack_chunks_packs_greedily_splits_long_chunks_and_maps_chunks(tokenizer):
    # Word tokens carry their space; a newline join is one token.
    chunks = [" alpha beta ", "gamma", "", "one two three four five six seven", "delta epsilon"]
    packs, chunk_pack = pack_chunks(chunks, tokenizer, max_tokens=4)
    assert packs == [
        "alpha beta\ngamma",
        "one two three four",
        "five six seven",
        "delta epsilon",
    ]
    for text in packs:
        assert len(tokenizer(text)["input_ids"]) <= 4
    # The over-long chunk maps to the pack of its first piece; empty -> None.
    assert chunk_pack == [0, 0, None, 1, 3]


def test_pack_chunks_without_splits_keeps_every_chunk_in_one_pack(tokenizer):
    chunks = ["a b", "c d", "e f", "g"]
    packs, chunk_pack = pack_chunks(chunks, tokenizer, max_tokens=6)
    assert packs == ["a b\nc d", "e f\ng"]
    assert chunk_pack == [0, 0, 1, 1]
    for index, pack in enumerate(chunk_pack):
        assert chunks[index] in packs[pack]


def test_select_top_packs_takes_distinct_packs_in_chunk_order():
    question = torch.tensor([1.0, 0.0])
    # Chunk cosines: 2 > 0 > 3 > 1; chunks 2 and 0 share pack 0.
    vectors = torch.tensor([[0.9, 0.1], [0.0, 1.0], [1.0, 0.0], [0.5, 0.5]])
    chunks = ["c0", "c1", "c2", "c3"]
    packing = (["p0", "p1", "p2"], [0, 2, 0, 1])
    texts, packs, scores, selecting = select_top_packs(chunks, vectors, question, packing, 2)
    assert packs == [0, 1] and texts == ["p0", "p1"] and selecting == [2, 3]
    assert scores[0] >= scores[1]
    texts, packs, _, selecting = select_top_packs(chunks, vectors, question, packing, 3)
    assert packs == [0, 1, 2] and selecting == [2, 3, 1]
    with pytest.raises(ValueError):
        select_top_packs(chunks[:3], vectors, question, packing, 2)


def test_select_read_set_reads_stored_cosine_order_within_the_budget():
    question = torch.tensor([1.0, 0.0])
    # Chunk cosines: 2 > 0 > 3 > 1; word counts stand in for tokens.
    vectors = torch.tensor([[0.9, 0.1], [0.0, 1.0], [1.0, 0.0], [0.5, 0.5]])
    chunks = ["a b c", "d", "e f", "g h i j"]
    words = lambda text: len(text.split())
    texts, indices, scores = select_read_set(chunks, vectors, question, words, 5)
    assert indices == [2, 0] and texts == ["e f", "a b c"] and scores[0] >= scores[1]
    # Reading stops at the first chunk past the budget, never skips ahead.
    texts, indices, _ = select_read_set(chunks, vectors, question, words, 6)
    assert indices == [2, 0]
    texts, indices, _ = select_read_set(chunks, vectors, question, words, 100)
    assert indices == [2, 0, 3, 1]
    # The first chunk is always read; the cap bounds the ranked list.
    assert select_read_set(chunks, vectors, question, words, 1)[1] == [2]
    assert select_read_set(chunks, vectors, question, words, 100, cap=2)[1] == [2, 0]
    with pytest.raises(ValueError):
        select_read_set(chunks[:3], vectors, question, words, 5)


def test_prep_read_tokens_is_opt_in_and_parses():
    base = ["prep", "--data-root", "d", "--tokenizer", "t", "--output", "o", "--samples", "triviaqa=1"]
    assert parse_args(base).read_tokens == 0
    assert parse_args([*base, "--read-tokens", "512"]).read_tokens == 512


def test_resume_partial_keeps_whole_rows_and_cuts_a_torn_tail(tmp_path):
    path = tmp_path / "triviaqa.partial"
    path.write_text('{"name": "a"}\n{"name": "b"}\n{"name": "c", "kin')
    assert resume_partial(path) == {"a", "b"}
    assert path.read_text() == '{"name": "a"}\n{"name": "b"}\n'
    assert resume_partial(path) == {"a", "b"}
    empty = tmp_path / "empty.partial"
    empty.write_text("")
    assert resume_partial(empty) == set()


def test_answer_in_documents_uses_squad_normalization():
    assert answer_in_documents("The Pacific Ocean", ["across the pacific  ocean."])
    assert not answer_in_documents("Atlantic", ["across the pacific ocean"])
    assert not answer_in_documents("", ["anything"])


# --------------------------------------------------------------- gating


def test_learned_gate_needs_a_span_and_a_high_gate():
    assert learned_gate_open([(1, 3, 7)], [0.1, 0.9, 0.8])
    assert not learned_gate_open([], [0.9, 0.9])
    assert not learned_gate_open([(1, 3, 7)], [0.1, 0.4])
    assert learned_gate_open([(1, 2, 0)], [0.5])
    with pytest.raises(ValueError):
        learned_gate_open([(0, 1, 0)], [0.9], threshold=1.5)


def test_record_gate_decision_modes():
    spans, gates, cosines = [(0, 2, 5)], [0.9, 0.9, 0.1], [0.5, 0.5, 1.0]
    assert record_gate_decision("learned", spans, gates, cosines)
    assert not record_gate_decision("cosine", spans, gates, cosines, 0.85)
    with pytest.raises(ValueError):
        record_gate_decision("other", spans, gates, cosines)


def test_hybrid_gate_mode_flags_default_to_cosine(tmp_path):
    base = [
        "--assets", str(tmp_path), "--candidate", str(tmp_path),
        "--depot-dev", "d", "--depot-fresh", "f", "--parquet", "p",
        "--output", str(tmp_path / "o"),
    ]
    assert parse_hybrid_args(base).record_gate_mode == "cosine"
    assert parse_hybrid_args(base + ["--record-gate-mode", "learned"]).record_gate_mode == "learned"
    paper = [
        "paper-run", "--target", "triviaqa", "--data-root", "d", "--assets", "a",
        "--candidate", "c", "--cases", "x", "--output", "o", "--arms", "hybrid",
    ]
    assert parse_paper_args(paper).record_gate_mode == "cosine"
    assert parse_paper_args(paper + ["--record-gate-mode", "learned"]).record_gate_mode == "learned"


def test_gate_confusion_counts():
    rows = [(POSITIVE, True), (POSITIVE, False), (NEGATIVE, True), (NEGATIVE, False), (NEGATIVE, False)]
    result = gate_confusion(rows)
    assert result["positives"] == 2 and result["negatives"] == 3
    assert result["recall"] == 0.5 and result["false_open_rate"] == pytest.approx(1 / 3)
    assert result["precision"] == 0.5 and result["accuracy"] == pytest.approx(3 / 5)


# ------------------------------------------------------------ bootstrap


def test_paired_bootstrap_identical_and_shifted():
    left = [0.0, 1.0, 0.5, 1.0, 0.0]
    same = paired_bootstrap(left, left, resamples=500, seed=1)
    assert same["mean_delta"] == 0 and same["ci_low"] == 0 and same["ci_high"] == 0
    assert not same["excludes_zero"]
    shifted = paired_bootstrap(left, [v + 0.25 for v in left], resamples=500, seed=1)
    assert shifted["mean_delta"] == pytest.approx(0.25)
    assert shifted["ci_low"] == pytest.approx(0.25) and shifted["ci_high"] == pytest.approx(0.25)
    assert shifted["excludes_zero"] and shifted["right_better"] == 5


def test_paired_bootstrap_interval_covers_mean_and_is_seeded():
    left = [float(i % 2) for i in range(200)]
    right = [float((i % 3) == 0) for i in range(200)]
    first = paired_bootstrap(left, right, resamples=2000, seed=7)
    again = paired_bootstrap(left, right, resamples=2000, seed=7)
    assert first == again
    assert first["ci_low"] <= first["mean_delta"] <= first["ci_high"]
    assert first["ci_high"] - first["ci_low"] > 0.05
    with pytest.raises(ValueError):
        paired_bootstrap([1.0], [1.0, 0.0])
    with pytest.raises(ValueError):
        paired_bootstrap([], [])


# ---------------------------------------------------------------- data flow


def test_example_stream_is_deterministic_and_mixes_depot():
    pools = {"squad_v2": list(range(50)), "triviaqa": list(range(30))}
    weights = {"squad_v2": 1.0, "triviaqa": 1.0, "drop": 1.0}
    first = example_stream(pools, 20, weights, 0.15, 4000, seed=3)
    assert first == example_stream(pools, 20, weights, 0.15, 4000, seed=3)
    counts = Counter(s for s, _ in first)
    assert set(counts) == {"depot", "squad_v2", "triviaqa"}
    assert 0.11 < counts["depot"] / 4000 < 0.19
    assert all(0 <= i < 50 for s, i in first if s == "squad_v2")
    # Every source cycles through a full shuffle before repeating.
    squad = [i for s, i in first if s == "squad_v2"]
    assert sorted(squad[:50]) == list(range(50))


def test_heldout_is_stable_and_rare():
    names = [f"squad_v2-{i:06d}-1" for i in range(4000)]
    chosen = [n for n in names if heldout(n)]
    assert chosen == [n for n in names if heldout(n)]
    assert 5 <= len(chosen) <= 40


def test_parsers():
    assert parse_samples("squad_v2=10,triviaqa=0") == {"squad_v2": 10, "triviaqa": 0}
    with pytest.raises(ValueError):
        parse_samples("unknown=3")
    assert parse_weights("squad_v2=0.5,drop=1") == {"squad_v2": 0.5, "drop": 1.0}
    with pytest.raises(ValueError):
        parse_weights("squad_v2=-1")
    args = parse_args(
        [
            "train", "--assets", "a", "--init", "i", "--examples", "e",
            "--depot-data", "d", "--data-root", "r", "--reference-rows", "h",
            "--output", "o", "--weights", "squad_v2=1",
        ]
    )
    assert args.gate_weight == 3.0 and args.depot_fraction == 0.15 and args.steps == 4096
    prep = parse_args(["prep", "--data-root", "r", "--tokenizer", "t", "--output", "o", "--samples", "drop=1"])
    assert prep.chunking == "none"
    packed = parse_args(
        ["prep", "--data-root", "r", "--tokenizer", "t", "--output", "o", "--samples", "drop=1", "--chunking", "pack256"]
    )
    assert packed.chunking == "pack256"
    with pytest.raises(SystemExit):
        parse_args(
            [
                "train", "--assets", "a", "--init", "i", "--examples", "e",
                "--depot-data", "d", "--data-root", "r", "--reference-rows", "h",
                "--output", "o", "--weights", "squad_v2=1", "--depot-fraction", "1.5",
            ]
        )


def test_question_prefix_keeps_the_second_copy(tokenizer):
    layout = document_layout(tokenizer, ["The Normans lived in Normandy.", "Rollo led them."])
    question = "Who was  the Norse\nleader?"
    texts = question_prefixed_documents(
        tokenizer, layout["doubled"], layout["counts"], question, "question-passage-v1"
    )
    prefix = question_prefix_text(question, "question-passage-v1")
    assert prefix == "Question: Who was the Norse leader?\nPassage:"
    for text, plain, count in zip(texts, layout["doubled"], layout["counts"], strict=True):
        assert text == prefix + plain
        ids = tokenizer(text)["input_ids"]
        assert ids[-count:] == tokenizer(plain)["input_ids"][-count:]
    assert question_prefixed_documents(
        tokenizer, layout["doubled"], layout["counts"], question, None
    ) == layout["doubled"]
    with pytest.raises(ValueError):
        question_prefix_text(question, "nope")
    with pytest.raises(ValueError):
        question_prefixed_documents(tokenizer, layout["doubled"], [0, 1], question, "question-passage-v1")


def test_question_prefix_with_the_real_encoder_tokenizer():
    from pathlib import Path

    path = Path(
        "/home/tim/antfly/.worktrees/plan-memorilla-native-memory/.benchmark-assets/memorilla/models/qwen3-embedding-4b"
    )
    if not path.exists():
        pytest.skip("Qwen3-Embedding-4B tokenizer not present")
    from transformers import AutoTokenizer

    real = AutoTokenizer.from_pretrained(path, local_files_only=True)
    layout = document_layout(real, ["Rollo, the Norse leader, agreed in 911.", "Hello:world 42"])
    for name in QUESTION_PREFIX_TEMPLATES:
        texts = question_prefixed_documents(
            real, layout["doubled"], layout["counts"], "Who led the Norse?", name
        )
        assert all(t.startswith("Question: Who led the Norse?") for t in texts)


def test_train_parses_question_prefix():
    base = [
        "train", "--assets", "a", "--init", "i", "--examples", "e", "--depot-data", "d",
        "--data-root", "r", "--reference-rows", "h", "--output", "o", "--weights", "squad_v2=1",
    ]
    assert parse_args(base).question_prefix == "none"
    assert parse_args(base + ["--question-prefix", "question-passage-v1"]).question_prefix == (
        "question-passage-v1"
    )
