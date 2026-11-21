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

import json

import pytest
import torch
from qwen_hybrid_eval import (
    EXACT_PROMPT,
    HELPFUL_PROMPT,
    QUESTION_TYPES,
    StoredEmbeddings,
    answer_alternatives,
    baseline_user_turn,
    bucket_rows,
    canonicalize_answer,
    compose_memory_rows,
    depot_metrics,
    hybrid_prompt,
    is_unknown,
    left_pad,
    memory_user_turn,
    merged_system_prompt,
    noise_documents,
    noise_seed,
    normalize_answer,
    append_row,
    existing_names,
    generate_greedy_batch,
    paired_summary,
    read_rows,
    CHOICE_TABLE,
    PAPER_TARGETS,
    choice_accuracy_from_correct,
    choice_embedding_texts,
    choice_row_correct,
    clean_generation_text,
    embed_pooled_texts,
    file_sha256,
    load_choice_table,
    nonempty_choices,
    paired_bootstrap_ci,
    paper_arm_files,
    case_name,
    CUSTOM_TARGETS,
    cases_split,
    records_split,
    paper_released_checkpoint,
    per_row_scores,
    save_choice_table,
    score_arm,
    text_key,
    truncate_prompt_right,
    paper_arm_spec,
    paper_generation_limit,
    paper_memory_system,
    paper_protocol,
    paper_text_messages,
    paper_truncations,
    parse_args,
    parse_paper_args,
    qa_metrics,
    question_type,
    record_gate_open,
    retrieval_order,
    rouge_l,
    sample_rows,
    span_record_ids,
    squad_exact,
    squad_f1,
    summarize_rows,
    system_prompt_text,
    tensor_digest,
    trim_generation,
)


class Tokenizer:
    """Chat template stand-in: the rendered prompt is the system text, every
    ``<|memory|>`` becomes id 5 between a start (2) and an end (3) token."""

    pad_token = None
    eos_token = "<eos>"

    def __init__(self):
        self.messages = None

    def add_special_tokens(self, value):
        assert value == {"additional_special_tokens": ["<|memory|>"]}

    def convert_tokens_to_ids(self, value):
        assert value == "<|memory|>"
        return 5

    def apply_chat_template(self, messages, **kwargs):
        assert kwargs == {
            "tokenize": False,
            "add_generation_prompt": True,
            "enable_thinking": False,
        }
        self.messages = messages
        return messages[0]["content"]

    def __call__(self, text, **kwargs):
        assert kwargs == {"return_tensors": "pt", "add_special_tokens": False}
        return {"input_ids": torch.tensor([[2] + [5] * text.count("<|memory|>") + [3]])}


class Decoder:
    """Token id -> text for the word-completion rule."""

    def __init__(self, pieces):
        self.pieces = pieces

    def decode(self, ids):
        return "".join(self.pieces[int(i)] for i in ids)


def test_system_prompt_choices():
    assert system_prompt_text("helpful") == HELPFUL_PROMPT
    assert system_prompt_text("exact") == EXACT_PROMPT
    assert system_prompt_text("merged") == HELPFUL_PROMPT + " " + EXACT_PROMPT
    assert system_prompt_text("Custom text.") == "Custom text."


def test_compose_puts_soft_rows_before_record_rows():
    soft = torch.full((16, 8), 1.0, dtype=torch.bfloat16)
    record = torch.full((16, 8), 2.0)
    rows = compose_memory_rows(soft, record)
    assert rows.shape == (32, 8) and rows.dtype == torch.bfloat16
    assert torch.equal(rows[:16], soft) and torch.all(rows[16:] == 2)
    with pytest.raises(ValueError, match="16-slot"):
        compose_memory_rows(soft[:8], record)
    with pytest.raises(ValueError, match="one width"):
        compose_memory_rows(soft, torch.zeros(16, 4))


def test_compose_row_order():
    soft = torch.full((16, 8), 1.0)
    record = torch.full((16, 8), 2.0)
    rows = compose_memory_rows(soft, record, "record-first")
    assert torch.all(rows[:16] == 2) and torch.all(rows[16:] == 1)
    assert torch.equal(
        compose_memory_rows(soft, record, "soft-first"),
        compose_memory_rows(soft, record),
    )
    with pytest.raises(ValueError, match="row order"):
        compose_memory_rows(soft, record, "interleaved")


def test_record_gate_trusts_a_clean_copied_span():
    cosines = [1.0] * 16
    # No copied span: the soft rows go alone.
    assert record_gate_open([], cosines) is False
    # A clean span opens the gate.
    assert record_gate_open([(2, 6, 0)], cosines) is True
    # A fence-sitting slot inside the only span closes it.
    fence = list(cosines)
    fence[4] = 0.7
    assert record_gate_open([(2, 6, 0)], fence) is False
    # Another clean span is enough.
    assert record_gate_open([(2, 6, 0), (8, 10, 20)], fence) is True
    # Low cosines outside the spans (generated label slots) do not matter.
    noisy = [0.3] * 16
    noisy[2:6] = [0.85, 0.9, 1.0, 0.86]
    assert record_gate_open([(2, 6, 0)], noisy) is True
    assert record_gate_open([(2, 6, 0)], noisy, threshold=0.9) is False
    with pytest.raises(ValueError, match="does not fit"):
        record_gate_open([(10, 17, 0)], cosines)
    with pytest.raises(ValueError, match="gate cosine"):
        record_gate_open([(2, 6, 0)], cosines, threshold=1.5)


def test_hybrid_prompt_injects_thirty_two_rows_in_order():
    tokenizer = Tokenizer()
    embedding = torch.nn.Embedding(10, 6)
    soft = torch.arange(96, dtype=torch.float32).reshape(16, 6)
    record = -torch.arange(96, dtype=torch.float32).reshape(16, 6)
    ids, values, info = hybrid_prompt(
        tokenizer,
        embedding,
        soft,
        record,
        system_prompt=system_prompt_text("merged"),
        question=" Which code? ",
    )
    assert ids.tolist() == [2] + [5] * 32 + [3]
    assert info["memory_positions"] == list(range(1, 33))
    assert info["injection"] == "scatter-add-delta"
    system = tokenizer.messages[0]["content"]
    assert system.startswith(HELPFUL_PROMPT + " " + EXACT_PROMPT + "\nMemory")
    assert system.count("<|memory|>") == 32 and system.endswith("<|memory|>Memory")
    assert tokenizer.messages[1] == {"role": "user", "content": "Which code?"}
    original = embedding(ids)
    torch.testing.assert_close(values[[0, 33]], original[[0, 33]], rtol=0, atol=0)
    torch.testing.assert_close(values[1:17], original[1:17] + (soft - original[1:17]))
    torch.testing.assert_close(
        values[17:33], original[17:33] + (record - original[17:33])
    )


def test_span_record_keeps_only_word_completed_copies():
    pieces = {
        1: " code",
        2: " Kg",
        3: "9",
        4: "-",
        5: "009",
        6: "369",
        7: " at",
        8: " Pip",
        9: "ofo",
        10: " Depot",
        11: ".",
        220: " ",
    }
    tokenizer = Decoder(pieces)
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    # Slots 3..6 copied " Kg", "9", "-", "009" (source 1..4): the run stopped
    # before "369", which continues the code; slots 9..10 copied "ofo" from the
    # middle of " Pipofo" and extend back to the word start.
    ids, kept = span_record_ids([(9, 10, 8), (3, 7, 1)], source, tokenizer, 220)
    assert ids == [2, 3, 4, 5, 6, 8, 9] + [220] * 9
    assert kept == 7
    assert tokenizer.decode(ids).rstrip() == " Kg9-009369 Pipofo"
    # No copied span: an all-pad record.
    assert span_record_ids([], source, tokenizer, 220) == ([220] * 16, 0)
    # Overflow truncates to the slot count but reports the full span length.
    ids, kept = span_record_ids([(0, 11, 0)], source, tokenizer, 220, slots=4)
    assert ids == [1, 2, 3, 4] and kept == 11
    with pytest.raises(ValueError, match="does not fit"):
        span_record_ids([(0, 3, 10)], source, tokenizer, 220)


@pytest.mark.parametrize(
    "answer,record,expected",
    [
        # (a) case flips take the record's casing.
        ("WZ3-003545", " code Wz3-003545 at Dociru Depot", "Wz3-003545"),
        (
            "hidehi hokifu",
            " key holder Hidehi Hokifu at Kopene Depot ",
            "Hidehi Hokifu",
        ),
        # (b) prefix drops grow back to the record word (hyphen ignored).
        ("3005601", " code Te3-005601 at Zorutu ", "Te3-005601"),
        ("4672", " code Xd4-004672 at Kuruli Depot ", "Xd4-004672"),
        ("001618", " code Pp7-001618 at Soceso Depot ", "Pp7-001618"),
        # A whole-word match is left as the record spells it, never extended.
        ("Hokifu", " key holder Hidehi Hokifu at Kopene Depot ", "Hokifu"),
        # Bare digits never grow into a record word (the tail rule needs three
        # characters); the shortest real prefix drop still completes.
        ("4", " code holder4 10 and 50 centavos ", "4"),
        ("0", " code UNKNOWN at 12-10 with under 2 ", "0"),
        ("10", " code UNKNOWN at 12-10 with under 2 ", "10"),
        ("672", " code Xd4-004672 at Kuruli Depot ", "Xd4-004672"),
        # Multi-word tails complete to the word sequence.
        (
            "Fihunu at Kopene",
            " key holder Faketa Fihunu at Kopene Depot ",
            "Fihunu at Kopene",
        ),
        ("Tuloco", " key holder Komode Tuloco ", "Tuloco"),
        # Free-form words never grow into longer words.
        ("yes", " code holder Hayes at 1 or 0 ", "yes"),
        ("France", " key holder region in France ", "France"),
        # No relation to the record: unchanged (whitespace stripped).
        ("JESAVO DEGEBE", " key holder Jesavo Degebo at Gihera ", "JESAVO DEGEBE"),
        (" Rome ", " code holder at 1060s ", "Rome"),
        ("UNKNOWN", " code Te3-005601 ", "UNKNOWN"),
        ("", " code Te3-005601 ", ""),
        ("-", " code Te3-005601 ", "-"),
    ],
)
def test_canonicalize_answer(answer, record, expected):
    assert canonicalize_answer(answer, record) == expected


def test_squad_normalization_exact_and_f1():
    assert normalize_answer("The  Normans, in France!") == "normans in france"
    assert squad_exact("france.", ["France"]) == 1.0
    assert squad_exact("in France", ["France"]) == 0.0
    assert squad_f1("in France", ["France"]) == pytest.approx(2 / 3)
    assert squad_f1(
        "Iceland and Norway", ["Denmark, Iceland and Norway"]
    ) == pytest.approx(2 * (1.0 * 0.75) / 1.75)
    assert squad_f1("", ["France"]) == 0.0
    assert squad_f1("Rome", ["France"]) == 0.0
    # Unanswerable gold: only an abstention scores.
    assert squad_exact("UNKNOWN", [""]) == 1.0
    assert squad_exact("France", [""]) == 0.0
    assert squad_f1("unanswerable", ["unanswerable"]) == 1.0
    # Max over alternatives.
    assert squad_exact("Raiders", ["the Raiders", "Oakland"]) == 1.0


def test_drop_alternatives_and_qa_metrics():
    assert answer_alternatives("2, 3, 3, 3, 3", True) == ["2, 3, 3, 3, 3", "2", "3"]
    assert answer_alternatives("Denmark, Iceland and Norway", False) == [
        "Denmark, Iceland and Norway"
    ]
    row = qa_metrics("3", "2, 3, 3, 3, 3", split_commas=True)
    assert row["em"] == 0.0 and row["em_any"] == 1.0 and row["f1_any"] == 1.0
    assert not row["abstained"] and not row["gold_abstain"]
    row = qa_metrics("France", "France", split_commas=False)
    assert row == {"em": 1.0, "f1": 1.0, "abstained": False, "gold_abstain": False}


def test_rouge_l_lcs():
    score = rouge_l("the cat sat on the mat", "the cat lay on the mat")
    assert score["lcs"] == 5
    assert score["precision"] == pytest.approx(5 / 6)
    assert score["recall"] == pytest.approx(5 / 6)
    assert score["f1"] == pytest.approx(5 / 6)
    assert rouge_l("", "anything")["f1"] == 0.0
    assert rouge_l("x y z", "a b c")["f1"] == 0.0
    assert rouge_l("Hello, World!", "hello world")["f1"] == 1.0
    # Order matters: a reversed sequence shares only one token with itself.
    assert rouge_l("a b c", "c b a")["lcs"] == 1


def test_depot_metrics_and_summary():
    assert depot_metrics(" Kg9-009369 ", "Kg9-009369") == {
        "exact": True,
        "contains": True,
        "abstained": False,
        "gold_missing": False,
    }
    assert depot_metrics("UNKNOWN", "UNKNOWN")["exact"] is True
    assert depot_metrics("UNKNOWN", "Kg9-009369")["abstained"] is True
    verbose = depot_metrics("The code is Kg9-009369.", "Kg9-009369")
    assert not verbose["exact"] and verbose["contains"]
    rows = [
        {
            "metrics": depot_metrics(a, g),
            "generation_seconds": 0.5,
            "generated_tokens": 4,
            "finish_reason": "eos",
        }
        for a, g in [
            ("Kg9-009369", "Kg9-009369"),
            ("UNKNOWN", "Jz6-001863"),
            ("UNKNOWN", "UNKNOWN"),
            ("code Kg9-009369", "Kg9-009369"),
        ]
    ] + [{"error": "encoder bound exceeded"}]
    summary = summarize_rows("depot-dev", rows)
    assert summary["cases"] == 5 and summary["errors"] == 1
    assert summary["exact"] == 2 and summary["present_exact"] == 1
    assert summary["present_total"] == 3 and summary["missing_abstained"] == 1
    assert summary["false_abstentions"] == 1
    assert summary["contains_rate"] == pytest.approx(2 / 3)
    qa = summarize_rows(
        "drop",
        [
            {
                "metrics": qa_metrics("3", "2, 3", True),
                "generation_seconds": 1.0,
                "generated_tokens": 1,
                "finish_reason": "eos",
            }
        ],
    )
    assert qa["em"] == 0.0 and qa["em_any"] == 1.0 and qa["f1"] == pytest.approx(2 / 3)
    enwiki = summarize_rows(
        "enwiki",
        [
            {
                "metrics": rouge_l("a b", "a b c d"),
                "generation_seconds": 1.0,
                "generated_tokens": 2,
                "finish_reason": "length",
            }
        ],
    )
    assert enwiki["rouge_l_recall"] == 0.5 and enwiki["length_stops"] == 1
    gated = summarize_rows(
        "squad",
        [
            {
                "metrics": qa_metrics("x", "x", False),
                "generation_seconds": 1.0,
                "generated_tokens": 1,
                "finish_reason": "eos",
                "record_gate": gate,
            }
            for gate in (True, False, True)
        ],
    )
    assert gated["record_gate_total"] == 3 and gated["record_gate_fired"] == 2
    canon = summarize_rows(
        "depot-dev",
        [
            {
                "metrics": depot_metrics("Wz3-003545", "Wz3-003545"),
                "generation_seconds": 1.0,
                "generated_tokens": 1,
                "finish_reason": "eos",
                "canonicalized": flag,
            }
            for flag in (True, False)
        ],
    )
    assert canon["canonicalized"] == 1
    assert "record_gate_total" not in qa
    with pytest.raises(ValueError, match="unknown evaluation set"):
        summarize_rows("other", [])


def test_argument_parsing(tmp_path):
    common = [
        "--assets",
        str(tmp_path),
        "--candidate",
        str(tmp_path),
        "--depot-dev",
        str(tmp_path / "dev.json"),
        "--depot-fresh",
        str(tmp_path / "fresh.json"),
        "--parquet",
        str(tmp_path),
        "--output",
        str(tmp_path / "out"),
    ]
    args = parse_args(common)
    assert args.arms == ["soft", "record", "hybrid"]
    assert args.sets == ["depot-dev", "depot-fresh", "squad", "drop", "enwiki"]
    assert args.system_prompt == "merged" and args.record_rows == "full"
    assert args.row_order == "soft-first" and not args.record_gate
    assert args.record_gate_cosine == 0.85
    gated = parse_args(common + ["--row-order", "record-first", "--record-gate"])
    assert gated.row_order == "record-first" and gated.record_gate
    with pytest.raises(SystemExit):
        parse_args(common + ["--record-gate-cosine", "1.5"])
    extra = parse_args(
        common + ["--answer-canonicalize", "--released-checkpoint", "/ckpt/stage1"]
    )
    assert (
        extra.answer_canonicalize and str(extra.released_checkpoint) == "/ckpt/stage1"
    )
    assert not args.answer_canonicalize and args.released_checkpoint is None
    args = parse_args(
        common + ["--arm", "record", "--arms", "hybrid,record", "--sets", "squad"]
    )
    assert args.arms == ["record", "hybrid"] and args.sets == ["squad"]
    with pytest.raises(SystemExit):
        parse_args(common + ["--sets", "triviaqa"])
    with pytest.raises(SystemExit):
        parse_args(common + ["--arms", "oracle"])
    with pytest.raises(SystemExit):
        parse_args(common + ["--soft-system-prompt", "x <|memory|> y"])
    assert json.dumps(vars(parse_args(common)), default=str)


# ----------------------------------------------------------------------------
# paper protocol helpers


TRIVIA_SYSTEM = "You are a trivia assistant."
TRIVIA_SUFFIX = "\n\nAnswer with a short phrase.\n\nYour answer:"


class Benchmarks:
    """Stand-in for upstream ``memorilla.benchmarks`` (registry + constants)."""

    HELPFUL_PROMPT = "You are a helpful assistant."
    GENERATION = "generation"
    ROUGE1, ROUGEL = "rouge1", "rougeL"

    class _Baseline:
        def __init__(self, system, suffix, max_new_tokens):
            self.system_prompt, self.suffix = system, suffix
            self.max_new_tokens = max_new_tokens

    class _Benchmark:
        def __init__(
            self, system, suffix, max_new_tokens, scoring, metrics, baseline, length
        ):
            self.system_prompt, self.suffix = system, suffix
            self.max_new_tokens, self.scoring = max_new_tokens, scoring
            self.metrics, self.baseline = metrics, baseline
            self.max_length = length

    BENCHMARKS = {
        "triviaqa": _Benchmark(
            TRIVIA_SYSTEM,
            TRIVIA_SUFFIX,
            64,
            "exact_match",
            ("exact_match", "f1"),
            _Baseline("You are a helpful assistant.", TRIVIA_SUFFIX, 64),
            2048,
        ),
        "narrativeqa": _Benchmark(
            "You are a personalized AI assistant.",
            "",
            32,
            "generation",
            ("rougeL", "f1"),
            _Baseline("Answer briefly.", "", 64),
            8192,
        ),
        "pv4": _Benchmark(
            "You are a personalized AI assistant.",
            "",
            256,
            "choice",
            ("accuracy", "accuracy_hard"),
            _Baseline("You are a helpful assistant.", "", 256),
            8192,
        ),
        "factkg": _Benchmark(
            "You are a personalized AI assistant.",
            "",
            8,
            "choice",
            ("accuracy",),
            _Baseline("Fact check.", "\n\nA:\n", 8),
            2048,
        ),
        "lamp7": _Benchmark(
            "You are a personalized AI assistant.",
            "",
            128,
            "generation",
            ("rouge1", "rougeL"),
            _Baseline("Style.", "", 128),
            1024,
        ),
    }


def write_store(directory, collections, questions):
    """Upstream ``EmbeddingWriter`` layout: ascending collection ids, rows
    sliced from shards by (shard, start, length), row-aligned questions."""
    from safetensors.torch import save_file

    directory.mkdir(parents=True)
    ids, shards, starts, lengths, buffers = [], [], [], [], {0: [], 1: []}
    for index, (cid, rows) in enumerate(sorted(collections.items())):
        shard = index % 2
        start = sum(len(b) for b in buffers[shard])
        ids.append(cid), shards.append(shard), starts.append(start)
        lengths.append(len(rows))
        buffers[shard].append(rows)
    for shard, parts in buffers.items():
        save_file(
            {"embeddings": torch.cat(parts).half().contiguous()},
            str(directory / f"documents-{shard:05d}.safetensors"),
        )
    save_file(
        {
            "collection_ids": torch.tensor(ids, dtype=torch.int64),
            "shard": torch.tensor(shards, dtype=torch.int32),
            "start": torch.tensor(starts, dtype=torch.int64),
            "length": torch.tensor(lengths, dtype=torch.int64),
        },
        str(directory / "index.safetensors"),
    )
    save_file(
        {"embeddings": questions.half().contiguous()},
        str(directory / "questions.safetensors"),
    )


def test_stored_embeddings_mirror_the_upstream_layout(tmp_path):
    collections = {
        7: torch.randn(3, 4),
        2: torch.randn(5, 4),
        11: torch.randn(1, 4),
    }
    questions = torch.randn(6, 4)
    write_store(tmp_path / "store", collections, questions)
    store = StoredEmbeddings(tmp_path / "store")
    assert len(store) == 3 and store.num_questions == 6
    for cid, rows in collections.items():
        got = store.documents(cid)
        assert got.dtype == torch.float16 and torch.equal(got, rows.half())
    assert torch.equal(store.question(4), questions[4].half())
    with pytest.raises(KeyError):
        store.documents(5)
    with pytest.raises(KeyError):
        store.documents(99)


def test_retrieval_order_matches_upstream_cosine_ranking():
    documents = torch.tensor(
        [[1.0, 0.0], [0.0, 3.0], [2.0, 2.0], [-1.0, 0.0]], dtype=torch.float16
    )
    question = torch.tensor([0.1, 1.0], dtype=torch.float16)
    indices, scores = retrieval_order(documents, question, 2)
    assert indices == [1, 2]
    assert scores[0] == pytest.approx(0.995, abs=1e-3)
    assert scores[0] >= scores[1]
    # Norms do not matter, only direction; k beyond n returns every chunk.
    assert retrieval_order(documents * 4, question, 10)[0] == [1, 2, 0, 3]
    assert retrieval_order(documents, question, 0) == ([], [])
    with pytest.raises(ValueError):
        retrieval_order(documents[0], question, 1)


@pytest.mark.parametrize(
    "question, expected",
    [
        ("Who is Mark Hunter?", "who"),
        ("  'Which ship sank first?", "which"),
        ("WHY does she leave?", "why"),
        ("How many years pass?", "how"),
        ("Where does the story begin?", "where"),
        ("When was it written?", "when"),
        ("What is the name of the dog?", "what"),
        ("Whose house burns down?", "other"),
        ("In which city do they meet?", "other"),
        ("Mark's father is a ...?", "other"),
        ("", "other"),
    ],
)
def test_question_type_is_the_first_word(question, expected):
    assert question_type(question) == expected


def test_bucket_rows_keeps_every_type_in_order():
    rows = [
        {"name": "a", "question_type": "who"},
        {"name": "b", "question_type": "why"},
        {"name": "c", "question_type": "who"},
    ]
    buckets = bucket_rows(rows)
    assert list(buckets) == list(QUESTION_TYPES)
    assert [r["name"] for r in buckets["who"]] == ["a", "c"]
    assert buckets["which"] == [] and len(buckets["why"]) == 1


def test_noise_rows_match_each_stored_norm():
    documents = (torch.randn(6, 64) * torch.arange(1, 7)[:, None]).half()
    documents[2] = 0
    noise = noise_documents(documents, 5)
    assert noise.shape == documents.shape and noise.dtype == torch.float16
    assert torch.allclose(
        noise.float().norm(dim=-1), documents.float().norm(dim=-1), rtol=2e-3
    )
    assert torch.all(noise[2] == 0)
    assert torch.equal(noise, noise_documents(documents, 5))
    assert not torch.equal(noise, noise_documents(documents, 6))
    # Direction is unrelated to the stored row.
    cosine = torch.nn.functional.cosine_similarity(
        noise[0].float(), documents[0].float(), dim=0
    )
    assert abs(float(cosine)) < 0.5
    assert noise_seed(23, 4) != noise_seed(23, 5) != noise_seed(24, 4)


def test_paper_protocol_reads_the_upstream_registry():
    trivia = paper_protocol("triviaqa", Benchmarks)
    assert trivia["system"] == TRIVIA_SYSTEM and trivia["suffix"] == TRIVIA_SUFFIX
    assert trivia["max_new_tokens"] == 64 and trivia["paper_max_new_tokens"] == 64
    assert trivia["baseline"] == {
        "system": "You are a helpful assistant.",
        "suffix": TRIVIA_SUFFIX,
        "max_new_tokens": 64,
    }
    narrative = paper_protocol("narrativeqa", Benchmarks)
    assert narrative["max_new_tokens"] == 32 and narrative["paper_max_new_tokens"] == 64
    enwiki = paper_protocol("enwiki", Benchmarks)
    assert enwiki["system"] == Benchmarks.HELPFUL_PROMPT
    assert enwiki["max_new_tokens"] == 512 and enwiki["baseline"] is None
    assert enwiki["max_length"] == 2048 and trivia["max_length"] == 2048
    with pytest.raises(ValueError):
        paper_protocol("squad", Benchmarks)


def test_prompt_merging_with_benchmark_prompts():
    trivia = paper_protocol("triviaqa", Benchmarks)
    assert merged_system_prompt(TRIVIA_SYSTEM) == TRIVIA_SYSTEM + " " + EXACT_PROMPT
    soft, hybrid = paper_arm_spec("soft"), paper_arm_spec("hybrid")
    bench, noisy = paper_arm_spec("hybrid-bench"), paper_arm_spec("hybrid-noise")
    assert paper_memory_system(trivia, soft, None) == TRIVIA_SYSTEM
    assert paper_memory_system(trivia, hybrid, True) == merged_system_prompt(
        TRIVIA_SYSTEM
    )
    assert paper_memory_system(trivia, noisy, True) == merged_system_prompt(
        TRIVIA_SYSTEM
    )
    # A closed record gate leaves the soft rows under the benchmark prompt.
    assert paper_memory_system(trivia, hybrid, False) == TRIVIA_SYSTEM
    assert paper_memory_system(trivia, bench, True) == TRIVIA_SYSTEM
    with pytest.raises(ValueError):
        paper_memory_system(trivia, paper_arm_spec("rag-5"), True)
    # Upstream build_messages: stripped question + suffix in the user turn.
    assert memory_user_turn("  Who wrote Emma? ", TRIVIA_SUFFIX) == (
        "Who wrote Emma?" + TRIVIA_SUFFIX
    )
    with pytest.raises(ValueError, match="stripped"):
        memory_user_turn("Q?", "\nA:\n")


def test_hybrid_prompt_carries_the_benchmark_suffix():
    tokenizer = Tokenizer()
    embedding = torch.nn.Embedding(10, 6)
    question = memory_user_turn("Who wrote Emma?", TRIVIA_SUFFIX)
    _, embeds, _ = hybrid_prompt(
        tokenizer,
        embedding,
        torch.zeros(16, 6),
        torch.ones(16, 6),
        system_prompt=merged_system_prompt(TRIVIA_SYSTEM),
        question=question,
    )
    assert embeds.shape[0] == 34
    assert tokenizer.messages[1]["content"] == "Who wrote Emma?" + TRIVIA_SUFFIX
    assert tokenizer.messages[0]["content"].startswith(
        TRIVIA_SYSTEM + " " + EXACT_PROMPT + "\nMemory"
    )


def test_baseline_turn_mirrors_upstream_user_message():
    protocol = paper_protocol("triviaqa", Benchmarks)
    messages = paper_text_messages(protocol, ["chunk one", "chunk two"], " Q? ")
    assert messages[0] == {"role": "system", "content": "You are a helpful assistant."}
    assert messages[1]["content"] == "chunk one\n\nchunk two\n\n Q? " + TRIVIA_SUFFIX
    assert baseline_user_turn([], "Q?", "") == "Q?"
    with pytest.raises(ValueError, match="baseline"):
        paper_text_messages(paper_protocol("enwiki", Benchmarks), [], "Q?")


def test_paper_arms_budgets_and_truncations():
    narrative = paper_protocol("narrativeqa", Benchmarks)
    assert paper_arm_spec("closed-book") == {
        "arm": "closed-book",
        "kind": "text",
        "top_k": 0,
    }
    assert paper_arm_spec("rag-50")["top_k"] == 50
    for bad in ("rag-0", "rag", "oracle"):
        with pytest.raises(ValueError):
            paper_arm_spec(bad)
    soft = paper_arm_spec("soft")
    assert not soft["record"] and not soft["noise"]
    assert paper_arm_spec("soft-noise")["noise"]
    assert paper_arm_spec("hybrid-noise")["record"]
    rag = paper_arm_spec("rag-5")
    assert paper_generation_limit(narrative, soft) == 64
    assert paper_generation_limit(narrative, rag) == 64
    assert paper_generation_limit(narrative, soft, override=512) == 512
    assert paper_truncations(narrative, soft, 64) == [32]
    assert paper_truncations(narrative, rag, 64) == []
    assert paper_truncations(narrative, soft, 512, (160, 600, 0)) == [32, 160]


def test_generation_trimming_and_left_padding():
    assert trim_generation([5, 6, 151645, 151643, 151643]) == ([5, 6, 151645], "eos")
    assert trim_generation([5, 6, 7]) == ([5, 6, 7], "length")
    assert trim_generation([151643]) == ([151643], "eos")
    batch, mask = left_pad([torch.ones(2, 3), torch.full((4, 3), 2.0)])
    assert batch.shape == (2, 4, 3)
    assert mask.tolist() == [[0, 0, 1, 1], [1, 1, 1, 1]]
    assert torch.all(batch[0, :2] == 0) and torch.all(batch[0, 2:] == 1)
    with pytest.raises(ValueError):
        left_pad([])


def test_sampling_digests_and_abstention():
    first = sample_rows(10557, 1000, 23)
    assert first == sample_rows(10557, 1000, 23) and len(set(first)) == 1000
    assert first == sorted(first) and first != sample_rows(10557, 1000, 24)
    assert sample_rows(5, 0, 1) == [0, 1, 2, 3, 4]
    assert sample_rows(5, 9, 1) == [0, 1, 2, 3, 4]
    tensor = torch.arange(6, dtype=torch.float16).reshape(2, 3)
    assert tensor_digest(tensor) == tensor_digest(tensor.clone())
    assert tensor_digest(tensor) != tensor_digest(tensor.reshape(3, 2))
    assert tensor_digest(tensor) != tensor_digest(tensor.float())
    assert is_unknown("UNKNOWN") and is_unknown(" unknown. ")
    assert not is_unknown("Unknown Soldier")


def test_paired_summary_counts_the_union():
    left = {"a": 1.0, "b": 0.0, "c": 0.5, "d": 0.1}
    right = {"a": 1.0, "b": 0.3, "c": 0.0, "e": 1.0}
    summary = paired_summary(left, right, 0.2)
    assert summary["rows"] == 3
    assert summary["right_better"] == 1 and summary["left_better"] == 1
    assert summary["answered_left"] == 2 and summary["answered_right"] == 2
    assert summary["answered_union"] == 3
    assert summary["answered_left_only"] == 1 and summary["answered_right_only"] == 1
    assert paired_summary({}, right, 0.2) == {"rows": 0}


def test_paper_argument_parsing(tmp_path):
    run = parse_paper_args(
        [
            "paper-run",
            "--target",
            "narrativeqa",
            "--data-root",
            str(tmp_path),
            "--assets",
            str(tmp_path),
            "--candidate",
            str(tmp_path),
            "--cases",
            str(tmp_path / "c.jsonl"),
            "--output",
            str(tmp_path / "out"),
            "--arms",
            "soft,hybrid,rag-1,rag-5",
            "--truncations",
            "16",
        ]
    )
    assert run.arms == ["soft", "hybrid", "rag-1", "rag-5"]
    assert run.truncations == [16] and run.record_top_k == 2
    assert run.record_gate_cosine == 0.85 and run.encoder_max_length == 2048
    assert run.record_extractor == "candidate" and run.record_stride == 8
    base = ["paper-run", "--target", "triviaqa", "--data-root", "x", "--assets", "x",
            "--candidate", "x", "--cases", "x", "--output", "x", "--arms", "hybrid-bench"]
    cheap = parse_paper_args([*base, "--record-extractor", "embed-window", "--record-stride", "4"])
    assert cheap.record_extractor == "embed-window" and cheap.record_stride == 4
    for bad in (["--record-extractor", "bm25"], ["--record-stride", "0"]):
        with pytest.raises(SystemExit):
            parse_paper_args([*base, *bad])
    with pytest.raises(ValueError):
        parse_paper_args(
            [
                "paper-run",
                "--target",
                "triviaqa",
                "--data-root",
                "x",
                "--assets",
                "x",
                "--candidate",
                "x",
                "--cases",
                "x",
                "--output",
                "x",
                "--arms",
                "oracle",
            ]
        )
    score = parse_paper_args(
        [
            "paper-score",
            "--target",
            "triviaqa",
            "--data-root",
            "x",
            "--output",
            "x",
            "--pairs",
            "soft:hybrid,rag-1:hybrid",
        ]
    )
    assert score.pairs == [("soft", "hybrid"), ("rag-1", "hybrid")]
    with pytest.raises(SystemExit):
        parse_paper_args(
            [
                "paper-prep",
                "--target",
                "narrativeqa",
                "--data-root",
                "x",
                "--output",
                "x",
                "--datasets-cache",
                "x",
                "--sample",
                "5",
                "--first",
                "5",
            ]
        )


def test_jsonl_rows_keep_unicode_line_separators(tmp_path):
    path = tmp_path / "rows.jsonl"
    append_row(path, {"name": "a", "text": "one\u2028two\x85three"})
    append_row(path, {"name": "b", "text": "plain"})
    rows = read_rows(path)
    assert [r["name"] for r in rows] == ["a", "b"]
    assert rows[0]["text"] == "one\u2028two\x85three"
    assert existing_names(path) == {"a", "b"}


def tiny_sampling_qwen():
    """A tiny random Qwen3 whose generation config samples like Qwen3-8B's."""
    transformers = pytest.importorskip("transformers")
    torch.manual_seed(0)
    config = transformers.Qwen3Config(
        vocab_size=151936,
        hidden_size=32,
        intermediate_size=64,
        num_hidden_layers=2,
        num_attention_heads=4,
        num_key_value_heads=2,
        head_dim=8,
        max_position_embeddings=128,
        tie_word_embeddings=False,
    )
    model = transformers.Qwen3ForCausalLM(config).eval()
    model.config._attn_implementation = "sdpa"
    model.generation_config = transformers.GenerationConfig(
        do_sample=True,
        temperature=0.6,
        top_k=20,
        top_p=0.95,
        eos_token_id=[151645, 151643],
        pad_token_id=151643,
    )
    model.generation_config.transformers_version = "4.51.0"
    return model


def argmax_loop(model, embeds, steps):
    ids = []
    with torch.no_grad():
        for _ in range(steps):
            inputs = torch.cat(
                [
                    embeds,
                    model.get_input_embeddings()(torch.tensor(ids, dtype=torch.long)),
                ]
            )
            token = int(model(inputs_embeds=inputs[None]).logits[0, -1].argmax())
            ids.append(token)
            if token in (151645, 151643):
                break
    return ids


def test_batched_generation_is_greedy_and_padding_free():
    model = tiny_sampling_qwen()
    embedding = model.get_input_embeddings()
    prompts = [
        embedding(torch.tensor([11, 12, 13])).detach(),
        embedding(torch.tensor([21, 22, 23, 24, 25, 26, 27])).detach(),
        embedding(torch.tensor([31])).detach(),
    ]
    outputs, seconds = generate_greedy_batch(model, prompts, 6)
    assert seconds >= 0 and len(outputs) == 3
    for prompt, (ids, reason) in zip(prompts, outputs, strict=True):
        expected = argmax_loop(model, prompt, 6)
        assert ids == expected
        assert reason == ("eos" if expected[-1] in (151645, 151643) else "length")
    # Repeated runs agree (no sampling).
    assert generate_greedy_batch(model, prompts, 6)[0] == outputs


# --------------------------------------------------- eight-benchmark protocol


class Metrics:
    """Stand-in for the upstream ``memorilla.metrics`` functions the choice
    and polarity paths call (same semantics, numpy cosine over FP16)."""

    @staticmethod
    def clean_generation(text):
        return clean_generation_text(text)

    @staticmethod
    def choice_predictions(prediction_embeddings, choice_embeddings, choices):
        import numpy as np

        selected = []
        for vector, candidates, texts in zip(
            prediction_embeddings, choice_embeddings, choices, strict=True
        ):
            vector = vector / (np.linalg.norm(vector) + 1e-8)
            matrix = candidates / (
                np.linalg.norm(candidates, axis=-1, keepdims=True) + 1e-8
            )
            sims = np.full(len(texts), -np.inf)
            sims[[i for i, t in enumerate(texts) if t.strip()]] = vector @ matrix.T
            selected.append(int(np.argmax(sims)))
        return selected

    @staticmethod
    def choice_index(choices, reference):
        stripped = [c.strip() for c in choices]
        return stripped.index(reference.strip())

    @staticmethod
    def first_polarity(text):
        head = text.strip().split()[0].rstrip(".,!?:;\"'").lower() if text else ""
        return head if head in {"yes", "no", "maybe"} else None

    @staticmethod
    def normalize_answer(text):
        return normalize_answer(text)

    @staticmethod
    def rouge(predictions, references):
        return {"rougeL": float(predictions[0] == references[0])}


def test_case_split_names_and_runs():
    # The registered split keeps its names; another split names itself.
    assert case_name("narrativeqa", "test", 7, 42) == "narrativeqa-00007-42"
    assert case_name("narrativeqa", "validation", 7, 42) == "narrativeqa-validation-00007-42"
    assert case_name("enwiki", "validation", 3, 1) == "enwiki-00003-1"
    # paper-run reads the split from the cases (absent = registered).
    assert cases_split("pv4", [{"row": 1}, {"row": 2}]) == "test"
    fresh = [{"row": 1, "split": "validation"}, {"row": 2, "split": "validation"}]
    assert cases_split("pv4", fresh) == "validation"
    with pytest.raises(ValueError):
        cases_split("pv4", [{"row": 1}, {"row": 2, "split": "validation"}])
    # paper-choice-embed reads it back from the record names.
    assert records_split("pv4", [{"name": "pv4-00001-6"}]) == "test"
    assert records_split("pv4", [{"name": "pv4-validation-00001-6"}]) == "validation"
    assert records_split("narrativeqa", [{"name": case_name("narrativeqa", "train", 1, 2)}]) == "train"
    with pytest.raises(ValueError):
        records_split("pv4", [{"name": "pv4-00001-6"}, {"name": "pv4-validation-00002-6"}])
    base =["paper-prep", "--target", "factkg", "--data-root", "x", "--output", "x",
            "--datasets-cache", "x"]
    assert parse_paper_args(base).split == ""
    assert parse_paper_args([*base, "--split", "validation"]).split == "validation"
    with pytest.raises(SystemExit):
        parse_paper_args([*base, "--split", "dev"])


def test_eight_targets_budgets_and_lengths():
    assert set(PAPER_TARGETS) == {
        "pmv2",
        "pv4",
        "factkg",
        "triviaqa",
        "narrativeqa",
        "pubmedqa",
        "lamp4",
        "lamp7",
        "enwiki",
        *CUSTOM_TARGETS,
    }
    # Plan 8.17's unseen task types: validation split, custom-config defaults.
    for target in CUSTOM_TARGETS:
        assert PAPER_TARGETS[target] == (target, "validation")
        custom = paper_protocol(target, Benchmarks)
        assert custom["system"] == Benchmarks.HELPFUL_PROMPT and custom["suffix"] == ""
        assert custom["scoring"] == Benchmarks.GENERATION and custom["baseline"] is None
        assert custom["max_new_tokens"] == custom["paper_max_new_tokens"] == 128
        assert custom["max_length"] == 2048
    pv4 = paper_protocol("pv4", Benchmarks)
    assert pv4["scoring"] == "choice"
    assert pv4["metrics"] == ("accuracy", "accuracy_hard")
    assert pv4["max_new_tokens"] == pv4["paper_max_new_tokens"] == 256
    assert pv4["max_length"] == 8192
    factkg = paper_protocol("factkg", Benchmarks)
    assert factkg["paper_max_new_tokens"] == 8 and factkg["suffix"] == ""
    soft = paper_arm_spec("soft")
    assert paper_generation_limit(factkg, soft) == 8
    assert paper_truncations(factkg, soft, 8) == []
    lamp7 = paper_protocol("lamp7", Benchmarks)
    assert paper_generation_limit(lamp7, soft) == 128 and lamp7["max_length"] == 1024
    # The memory turn of every new target is the stripped question alone.
    assert memory_user_turn(" Claim? ", factkg["suffix"]) == "Claim?"


def test_released_checkpoint_per_target(tmp_path):
    stage3 = tmp_path / "checkpoints" / "stage3"
    assert paper_released_checkpoint("pmv2", tmp_path, stage3) == (
        tmp_path / "checkpoints" / "pmv2"
    )
    assert paper_released_checkpoint("pv4", tmp_path, stage3) == (
        tmp_path / "checkpoints" / "pv4"
    )
    for target in ("factkg", "triviaqa", "narrativeqa", "pubmedqa", "lamp4", "lamp7"):
        assert paper_released_checkpoint(target, tmp_path, stage3) == stage3
    other = tmp_path / "x"
    assert paper_released_checkpoint("pmv2", tmp_path, stage3, other) == other
    (tmp_path / "f.bin").write_bytes(b"abc")
    assert file_sha256(tmp_path / "f.bin", block=2) == (
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_prompt_right_truncation_mirrors_the_collator():
    embeds = torch.arange(10, dtype=torch.float32).reshape(5, 2)
    same, untruncated = truncate_prompt_right(embeds, 5)
    assert untruncated is None and torch.equal(same, embeds)
    assert truncate_prompt_right(embeds, None)[1] is None
    cut, untruncated = truncate_prompt_right(embeds, 3)
    assert untruncated == 5 and torch.equal(cut, embeds[:3])


def test_choice_texts_follow_upstream_embedding_inputs():
    assert clean_generation_text("  True<|im_end|> ") == "True"
    assert clean_generation_text("<|im_start|>a<|endoftext|>b") == "ab"
    assert nonempty_choices(["A", " ", "", " B "]) == ["A", " B "]
    rows = [
        {
            "predictions": {"full": " True<|im_end|>", "4": "Tr"},
            "choices": ["True", "False"],
        },
        {"predictions": {"full": "True"}, "choices": ["True", "False", "  "]},
        {"predictions": {"full": ""}, "choices": [" Maybe "]},
    ]
    assert choice_embedding_texts(rows) == ["True", "Tr", "False", "", " Maybe "]
    assert text_key("a") != text_key("a ")


def test_choice_accuracy_aggregation():
    scores = choice_accuracy_from_correct([True, False, True, True])
    assert scores == {"accuracy": 0.75}
    scores = choice_accuracy_from_correct(
        [True, False, True], [False, True, True], want_hard=True
    )
    assert scores == {"accuracy": 2 / 3, "accuracy_hard": 0.5}
    assert choice_accuracy_from_correct([True], [False], True)["accuracy_hard"] == 0.0
    with pytest.raises(ValueError):
        choice_accuracy_from_correct([True, False], [True], True)
    with pytest.raises(ValueError):
        choice_accuracy_from_correct([])


def choice_rows_and_table():
    import numpy as np

    vectors = {
        "True": [1.0, 0.0],
        "False": [0.0, 1.0],
        "true it is": [0.9, 0.1],
        "no": [0.2, 0.8],
        "": [0.6, 0.4],
    }
    keys = [text_key(t) for t in vectors]
    table = (
        {k: i for i, k in enumerate(keys)},
        np.array(list(vectors.values()), dtype=np.float16),
    )
    rows = [
        {
            "name": "a",
            "answer": "True",
            "choices": ["True", "False"],
            "hard": False,
            "predictions": {"full": "true it is<|im_end|>"},
        },
        {
            "name": "b",
            "answer": "True",
            "choices": ["True", "", "False"],
            "hard": True,
            "predictions": {"full": "no"},
        },
        {
            "name": "c",
            "answer": " False",
            "choices": ["True", "False"],
            "hard": True,
            "predictions": {"full": " "},
        },
    ]
    for row in rows:
        row.update(generated_tokens=2, finish_reason="eos")
    return rows, table, keys, vectors


def test_choice_rows_use_upstream_selection_and_fail_closed():
    rows, table, _, _ = choice_rows_and_table()
    assert choice_row_correct(rows, "full", table, Metrics) == [True, False, False]
    protocol = {
        "target": "pv4",
        "scoring": "choice",
        "metrics": ("accuracy", "accuracy_hard"),
    }
    entry = score_arm(protocol, rows, "full", Metrics, table)
    assert entry["metrics"] == {"accuracy": 33.33, "accuracy_hard": 0.0}
    assert per_row_scores(protocol, rows, "full", Metrics, table) == {
        "a": 1.0,
        "b": 0.0,
        "c": 0.0,
    }
    with pytest.raises(ValueError, match="paper-choice-embed"):
        score_arm(protocol, rows, "full", Metrics, None)
    missing = [dict(rows[0], predictions={"full": "unseen"})]
    with pytest.raises(ValueError, match="paper-choice-embed"):
        choice_row_correct(missing, "full", table, Metrics)
    with pytest.raises(ValueError, match="no choices"):
        choice_row_correct([dict(rows[0], choices=[])], "full", table, Metrics)


def test_polarity_rows_skip_references_without_polarity():
    protocol = {"target": "pubmedqa", "scoring": "polarity", "metrics": ("accuracy",)}
    rows = [
        {"name": "a", "answer": "Yes. Because.", "predictions": {"full": "yes, it"}},
        {"name": "b", "answer": "No. Not.", "predictions": {"full": "Maybe."}},
        {"name": "c", "answer": "Unclear.", "predictions": {"full": "Yes."}},
    ]
    assert per_row_scores(protocol, rows, "full", Metrics) == {"a": 1.0, "b": 0.0}


def test_choice_table_round_trip_and_tamper_check(tmp_path):
    _, table, keys, _ = choice_rows_and_table()
    assert load_choice_table(tmp_path) == ({}, None, {})
    save_choice_table(tmp_path, keys, table[1], {"encoder": "e"})
    index, vectors, meta = load_choice_table(tmp_path)
    assert index == table[0] and (vectors == table[1]).all()
    assert meta["texts"] == 5 and meta["encoder"] == "e"
    with pytest.raises(ValueError):
        save_choice_table(tmp_path, keys[:2], table[1], {})
    meta["vectors_sha256"] = "0" * 64
    (tmp_path / f"{CHOICE_TABLE}.json").write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="changed"):
        load_choice_table(tmp_path)


def test_paired_bootstrap_interval():
    left = {str(i): float(i % 2) for i in range(200)}
    same = paired_bootstrap_ci(left, left, resamples=500, seed=1)
    assert same["mean_delta"] == same["ci_low"] == same["ci_high"] == 0.0
    better = {k: 1.0 for k in left}
    result = paired_bootstrap_ci(left, better, resamples=2000, seed=1)
    assert result["rows"] == 200 and result["mean_delta"] == 50.0
    assert 40.0 < result["ci_low"] < 50.0 < result["ci_high"] < 60.0
    assert result == paired_bootstrap_ci(left, better, resamples=2000, seed=1)
    assert paired_bootstrap_ci({}, better) == {"rows": 0}
    with pytest.raises(ValueError):
        paired_bootstrap_ci(left, better, resamples=0)


def test_arm_files_skip_record_cache(tmp_path):
    for name in ("soft", "hybrid", "records", "run-1"):
        (tmp_path / f"{name}.jsonl").write_text("")
    (tmp_path / "scores.json").write_text("{}")
    assert paper_arm_files(tmp_path) == ["hybrid", "soft"]


def test_pooled_text_embedding_takes_each_last_real_token():
    class Tok:
        def __call__(self, texts, padding=False, return_tensors=None):
            if isinstance(texts, str):
                return {"input_ids": [1] * (len(texts) + 1)}
            longest = max(len(t) for t in texts) + 1
            ids = [
                [len(t) + 1] * (len(t) + 1) + [0] * (longest - len(t) - 1)
                for t in texts
            ]
            mask = [[1] * (len(t) + 1) + [0] * (longest - len(t) - 1) for t in texts]
            return _Batch(
                input_ids=torch.tensor(ids), attention_mask=torch.tensor(mask)
            )

    class _Batch(dict):
        def to(self, device):
            return self

    class Enc:
        def __call__(self, input_ids, attention_mask, use_cache):
            hidden = torch.zeros(*input_ids.shape, 2560)
            hidden[..., 0] = input_ids.float()
            hidden[..., 1] = 1.0
            return type("Out", (), {"last_hidden_state": hidden.bfloat16()})()

    vectors = embed_pooled_texts(Enc(), Tok(), ["abc", "a", "ab"], 2, 16, device="cpu")
    assert vectors.dtype == torch.float16 and vectors.shape == (3, 2560)
    # Last real token carries id len+1 in channel 0 (padding id 0 never pooled).
    ratios = (vectors[:, 0].float() / vectors[:, 1].float()).round().tolist()
    assert ratios == [4.0, 2.0, 3.0]
    assert torch.allclose(vectors.float().norm(dim=1), torch.ones(3), atol=1e-2)
    with pytest.raises(ValueError, match="tokens"):
        embed_pooled_texts(Enc(), Tok(), ["x" * 20], 2, 16, device="cpu")


def test_choice_embed_and_score_argument_parsing():
    embed = parse_paper_args(
        [
            "paper-choice-embed",
            "--target",
            "pv4",
            "--data-root",
            "x",
            "--output",
            "o",
            "--assets",
            "a",
            "--arms",
            "soft,hybrid",
        ]
    )
    assert embed.arms == ["soft", "hybrid"] and embed.batch_size == 32
    assert embed.min_verify_cosine == 0.999
    score = parse_paper_args(
        ["paper-score", "--target", "factkg", "--data-root", "x", "--output", "o"]
    )
    assert score.bootstrap_resamples == 10_000 and score.bootstrap_seed == 23
    with pytest.raises(SystemExit):
        parse_paper_args(
            [
                "paper-score",
                "--target",
                "factkg",
                "--data-root",
                "x",
                "--output",
                "o",
                "--bootstrap-resamples",
                "0",
            ]
        )
