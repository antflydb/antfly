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

"""Qwen hybrid memory: released soft slots composed with copy17's exact record.

Three arms read by the stock frozen Qwen3-8B decoder (section 8 of
``docs/plans/memorilla-copy17-native.md``):

- ``soft``: the released Memorilla stage3 reader over pooled Qwen3-Embedding-4B
  document vectors (16 soft rows, the released helpful prompt).
- ``record``: the copy17 research candidate exactly as ``encoder_lora_eval.py``
  decodes it (``--snap-memory all --copy-contiguous-runs --copy-complete-words``):
  16 snapped token rows under copy17's exact-answer prompt.
- ``hybrid``: 32 memory markers; rows 1-16 are the soft rows, rows 17-32 the
  record rows (or only its copied spans), under a chosen system prompt.

Evaluation sets: the depot dev/fresh cases (exact match, UNKNOWN as abstain),
Memorilla-Data squad_v2 and drop validation subsets (SQuAD EM / token F1) and
an enwiki restatement sample (LCS ROUGE-L against the passage). Research
diagnostic only; nothing here qualifies a serving profile.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import re
import string
import time
from collections import Counter
from pathlib import Path

import torch
from decoder_reference import prepare_prompt
from encoder_lora import (
    RELEASED_MEMORY_SLOTS,
    LoRALinear,
    assert_shared_token_ids,
    complete_copied_words,
    copy_with_contiguous_runs,
    document_features_dual,
    encoder_special_suffix,
    encoder_states_pair,
    looped_read,
    prefixed_documents,
    self_context_documents,
    self_context_states,
    token_continues_word,
    token_ids_embeddings,
)
from encoder_lora import (
    SYSTEM as EXACT_PROMPT,
)
from encoder_lora_export import (
    DEFAULT_ASSETS,
    MIN_COPY_GATE,
    load_candidate,
    load_models,
    read_cases,
    wait_for_gpu,
)
from encoder_reference import normalize_features
from gpu_quality_reference import generate
from reference import load_upstream
from released_reference import SYSTEM as HELPFUL_PROMPT

ARMS = ("soft", "record", "hybrid")
SYSTEM_PROMPTS = {
    "helpful": HELPFUL_PROMPT,
    "exact": EXACT_PROMPT,
    "merged": HELPFUL_PROMPT + " " + EXACT_PROMPT,
}
DEPOT_SETS = ("depot-dev", "depot-fresh")
QA_SETS = ("squad", "drop")
RESTATE_SETS = ("enwiki",)
SETS = DEPOT_SETS + QA_SETS + RESTATE_SETS
PARQUET_CONFIG = {"squad": "squad_v2", "drop": "drop", "enwiki": "enwiki"}
ABSTAIN_MARKERS = {"", "unknown", "unanswerable", "no answer"}
DEPOT_MISSING = "UNKNOWN"
ARTICLES = re.compile(r"\b(a|an|the)\b")
WORDS = re.compile(r"[a-z0-9]+")
DECODER_WIDTH = 4096
ROW_ORDERS = ("soft-first", "record-first")
RECORD_WORD = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]*")
RECORD_WORD_CHARS = set(string.ascii_letters + string.digits + "-")
TAIL_MIN_CHARS = 3
RECORD_GATE_COSINE = 0.85
# ``--record-gate-mode learned`` (copy halves trained with a relevance gate,
# ``qwen_copy_qa.py``): the record opens when the copy head copied a span and
# its largest slot gate reaches this threshold. ``cosine`` (the default) is
# the section 7.4 snap-cosine rule above.
RECORD_GATE_MODES = ("cosine", "learned", "always")
LEARNED_GATE_THRESHOLD = 0.5
# Question-conditioned record encoding (copy halves trained with
# ``qwen_copy_qa.py --question-prefix``, protocol key
# ``document_question_prefix``): the question text precedes every
# self-context document in the encoder input, so the causal encoder's document
# states see the question. The kept rows (the second copy) keep their count
# and ids; only their states change.
QUESTION_PREFIX_TEMPLATES = {"question-passage-v1": "Question: {question}\nPassage:"}


def question_prefix_text(question, name: str) -> str:
    """The encoder-side prefix of one question (whitespace collapsed)."""
    if name not in QUESTION_PREFIX_TEMPLATES:
        raise ValueError("unknown question prefix: " + str(name))
    return QUESTION_PREFIX_TEMPLATES[name].format(question=" ".join(str(question).split()))


def question_prefixed_documents(tokenizer, doubled, counts, question, name) -> list[str]:
    """Self-context doubled texts with the question prefix in front (``name``
    None: unchanged). Each text's last ``counts[i]`` own ids must equal the
    unprefixed text's, so the kept rows still align with the copy sources."""
    if name is None:
        return list(doubled)
    prefix = question_prefix_text(question, name)
    texts = []
    for text, count in zip(doubled, counts, strict=True):
        plain = tokenizer(text, add_special_tokens=False)["input_ids"]
        joined = prefix + text
        full = tokenizer(joined, add_special_tokens=False)["input_ids"]
        if not 1 <= count <= len(plain) or list(full[-count:]) != list(plain[-count:]):
            raise ValueError("question prefix changed the kept document ids")
        texts.append(joined)
    return texts


# ----------------------------------------------------------------------------
# prompt and memory composition


def system_prompt_text(choice: str) -> str:
    """``helpful`` (released), ``exact`` (copy17) or ``merged`` (both, in that
    order, space separated); anything else is taken as literal prompt text."""
    return SYSTEM_PROMPTS.get(choice, choice)


def compose_memory_rows(
    soft: torch.Tensor, record: torch.Tensor, order: str = "soft-first"
) -> torch.Tensor:
    """The 16 soft rows and the record rows (16 for copy17, the evidence
    window's width for a span-window candidate) -> [16 + R, width] in the soft
    dtype; ``order`` puts the soft rows first (default) or the record first."""
    if soft.ndim != 2 or record.ndim != 2 or soft.shape[1] != record.shape[1]:
        raise ValueError("memory rows must be [slots, width] with one width")
    if soft.shape[0] != RELEASED_MEMORY_SLOTS or record.shape[0] < 1:
        raise ValueError("hybrid needs the 16-slot soft memory and a nonempty record")
    if order not in ROW_ORDERS:
        raise ValueError("row order must be one of " + ", ".join(ROW_ORDERS))
    record = record.to(soft.dtype)
    rows = [soft, record] if order == "soft-first" else [record, soft]
    return torch.cat(rows, dim=0)


def record_gate_open(spans, snap_top1_cosines, threshold=RECORD_GATE_COSINE) -> bool:
    """Plan section 7.4 routing: the record is trusted when at least one copied
    span's slots all snapped at cosine >= ``threshold``; otherwise (no copied
    span, or every span carries a fence-sitting slot) only the soft rows go to
    the decoder."""
    if not 0 <= threshold <= 1:
        raise ValueError("gate cosine must lie in [0, 1]")
    cosines = [float(c) for c in snap_top1_cosines]
    for begin, end, _start in spans:
        if not 0 <= begin < end <= len(cosines):
            raise ValueError("copied span does not fit the record")
        if min(cosines[begin:end]) >= threshold:
            return True
    return False


def learned_gate_open(spans, gates, threshold=LEARNED_GATE_THRESHOLD) -> bool:
    """A copy half trained with a relevance gate: the record is trusted when
    the copy head copied at least one span and the largest per-slot gate is at
    least ``threshold``; otherwise only the soft rows go to the decoder."""
    if not 0 <= threshold <= 1:
        raise ValueError("learned gate threshold must lie in [0, 1]")
    gates = [float(g) for g in gates]
    return bool(spans) and bool(gates) and max(gates) >= threshold


def record_gate_decision(
    mode, spans, gates, snap_top1_cosines, cosine_threshold=RECORD_GATE_COSINE
) -> bool:
    """``cosine``: ``record_gate_open`` on the snap cosines (the default);
    ``learned``: ``learned_gate_open`` on the copy head's slot gates;
    ``always``: the record is always used (a span-window record still carries
    the binding context the decoder needs to abstain)."""
    if mode == "always":
        return True
    if mode == "cosine":
        return record_gate_open(spans, snap_top1_cosines, cosine_threshold)
    if mode == "learned":
        return learned_gate_open(spans, gates)
    raise ValueError("record gate mode must be one of " + ", ".join(RECORD_GATE_MODES))


def hybrid_prompt(
    tokenizer, embedding, soft, record, *, system_prompt, question, device=None
):
    """``prepare_prompt`` over the 32 composed rows: one marker per row, the
    same scatter-add delta injection for soft and token rows alike."""
    return prepare_prompt(
        tokenizer,
        embedding,
        compose_memory_rows(soft, record),
        system_prompt=system_prompt,
        question=question,
        device=device,
    )


def squash(text: str) -> str:
    """Lower case without hyphens or whitespace, for prefix-drop comparisons
    ("3005601" against "Te3-005601")."""
    return re.sub(r"[-\s]", "", text.lower())


def canonicalize_answer(answer: str, record_text: str) -> str:
    """Re-spell a decoded answer from the record it was read from.

    (a) An answer that occurs in ``record_text`` case-insensitively, aligned to
    record word boundaries, takes the record's casing ("WZ3-003545" ->
    "Wz3-003545"). (b) Otherwise, an answer that is the tail of one to three
    consecutive record words (hyphens and spaces ignored) becomes that word
    sequence ("3005601" -> "Te3-005601", "4672" -> "Xd4-004672"); the tail
    rule needs at least three characters (a bare "4" or "0" never grows into
    "holder4" or "12-10"), and a tail that starts inside a word is accepted
    only for answers carrying a digit, so a free-form word is never grown into
    a longer one ("yes" stays "yes" next to "Hayes"). UNKNOWN and empty
    answers pass through.
    """
    raw = answer.strip()
    if not raw or raw == DEPOT_MISSING:
        return raw
    lowered, needle = record_text.lower(), raw.lower()
    start = lowered.find(needle)
    while start >= 0:
        end = start + len(raw)
        before = record_text[start - 1] if start > 0 else ""
        after = record_text[end] if end < len(record_text) else ""
        if before not in RECORD_WORD_CHARS and after not in RECORD_WORD_CHARS:
            return record_text[start:end]
        start = lowered.find(needle, start + 1)
    target = squash(raw)
    if len(target) < TAIL_MIN_CHARS:
        return raw
    words = RECORD_WORD.findall(record_text)
    has_digit = any(c.isdigit() for c in raw)
    for size in (1, 2, 3):
        for index in range(len(words) - size + 1):
            sequence = words[index : index + size]
            joined = squash(" ".join(sequence))
            if not joined.endswith(target):
                continue
            # The tail must start at a word start unless the answer is a code.
            suffix_starts = {
                len(joined) - len(squash(" ".join(sequence[k:]))) for k in range(size)
            }
            if has_digit or len(joined) - len(target) in suffix_starts:
                return " ".join(sequence)
    return raw


def span_record_ids(spans, source_ids, tokenizer, pad_id, slots=RELEASED_MEMORY_SLOTS):
    """Only the copied spans of a record, word-completed, in order -> (ids
    [slots], span tokens kept).

    ``spans`` are ``(begin, end, start)`` from ``copy_with_contiguous_runs``;
    each span widens to word boundaries by the ``complete_copied_words`` rule
    (``token_continues_word`` backward from its first and forward from its
    last token). Generated (non-copied) slots are dropped, the kept source ids
    are packed from slot 0, truncated to ``slots`` and padded with ``pad_id``.
    """
    source_ids = [int(i) for i in source_ids]
    kept = []
    for begin, end, start in sorted(spans):
        length = end - begin
        if length <= 0 or not 0 <= start <= len(source_ids) - length:
            raise ValueError("copied span does not fit the source")
        first, last = start, start + length
        while first > 0 and token_continues_word(tokenizer, source_ids[first]):
            first -= 1
        while last < len(source_ids) and token_continues_word(
            tokenizer, source_ids[last]
        ):
            last += 1
        kept.extend(source_ids[first:last])
    total = len(kept)
    kept = kept[:slots]
    return kept + [int(pad_id)] * (slots - len(kept)), total


# ----------------------------------------------------------------------------
# metrics


def normalize_answer(text: str) -> str:
    """SQuAD normalization: lower case, drop punctuation and articles, collapse
    whitespace."""
    lowered = "".join(c for c in str(text).lower() if c not in set(string.punctuation))
    return " ".join(ARTICLES.sub(" ", lowered).split())


def is_abstain(text: str) -> bool:
    return normalize_answer(text) in ABSTAIN_MARKERS


def answer_alternatives(answer: str, split_commas: bool) -> list[str]:
    """Gold alternatives: the whole string, plus its comma-separated pieces for
    datasets that join several annotations with ", " (drop)."""
    alternatives = [answer]
    if split_commas:
        alternatives += [p.strip() for p in answer.split(",") if p.strip()]
    return list(dict.fromkeys(alternatives))


def squad_exact(prediction: str, golds: list[str]) -> float:
    if all(is_abstain(g) for g in golds):
        return float(is_abstain(prediction))
    predicted = normalize_answer(prediction)
    return float(any(predicted == normalize_answer(g) for g in golds))


def squad_f1(prediction: str, golds: list[str]) -> float:
    if all(is_abstain(g) for g in golds):
        return float(is_abstain(prediction))
    predicted = normalize_answer(prediction).split()
    best = 0.0
    for gold in golds:
        tokens = normalize_answer(gold).split()
        common = sum((Counter(predicted) & Counter(tokens)).values())
        if not predicted or not tokens or common == 0:
            continue
        precision, recall = common / len(predicted), common / len(tokens)
        best = max(best, 2 * precision * recall / (precision + recall))
    return best


def rouge_l(prediction: str, reference: str) -> dict:
    """LCS-based ROUGE-L over lower-cased alphanumeric tokens."""
    hyp, ref = WORDS.findall(prediction.lower()), WORDS.findall(reference.lower())
    if not hyp or not ref:
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0, "lcs": 0}
    previous = [0] * (len(ref) + 1)
    for h in hyp:
        current = [0] * (len(ref) + 1)
        for j, r in enumerate(ref, 1):
            current[j] = (
                previous[j - 1] + 1 if h == r else max(previous[j], current[j - 1])
            )
        previous = current
    lcs = previous[-1]
    if lcs == 0:
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0, "lcs": 0}
    precision, recall = lcs / len(hyp), lcs / len(ref)
    return {
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall),
        "lcs": lcs,
    }


def depot_metrics(answer: str, gold: str) -> dict:
    """Exact (case-sensitive, stripped) match; ``UNKNOWN`` is the abstention."""
    answer = answer.strip()
    return {
        "exact": answer == gold,
        "contains": bool(gold) and gold != DEPOT_MISSING and gold in answer,
        "abstained": answer == DEPOT_MISSING,
        "gold_missing": gold == DEPOT_MISSING,
    }


def qa_metrics(answer: str, gold: str, split_commas: bool) -> dict:
    golds = answer_alternatives(gold, False)
    row = {
        "em": squad_exact(answer, golds),
        "f1": squad_f1(answer, golds),
        "abstained": is_abstain(answer),
        "gold_abstain": all(is_abstain(g) for g in golds),
    }
    if split_commas:
        pieces = answer_alternatives(gold, True)
        row["em_any"] = squad_exact(answer, pieces)
        row["f1_any"] = squad_f1(answer, pieces)
    return row


def set_kind(name: str) -> str:
    if name in DEPOT_SETS:
        return "depot"
    if name in QA_SETS:
        return "qa"
    if name in RESTATE_SETS:
        return "restate"
    raise ValueError("unknown evaluation set: " + name)


def case_metrics(name: str, answer: str, case: dict) -> dict:
    kind = set_kind(name)
    if kind == "depot":
        return depot_metrics(answer, case["answer"])
    if kind == "qa":
        return qa_metrics(answer, case["answer"], split_commas=(name == "drop"))
    return rouge_l(answer, case["answer"])


def mean(values) -> float | None:
    values = list(values)
    return sum(values) / len(values) if values else None


def summarize_rows(name: str, rows: list[dict]) -> dict:
    """Per-arm metrics of one set from its JSONL rows."""
    kind = set_kind(name)
    scored = [r for r in rows if "error" not in r]
    summary = {
        "set": name,
        "kind": kind,
        "cases": len(rows),
        "errors": len(rows) - len(scored),
        "mean_generation_seconds": mean(r["generation_seconds"] for r in scored),
        "mean_generated_tokens": mean(r["generated_tokens"] for r in scored),
        "length_stops": sum(r["finish_reason"] == "length" for r in scored),
    }
    if any("canonicalized" in r for r in scored):
        summary["canonicalized"] = sum(bool(r.get("canonicalized")) for r in scored)
    gated = [r for r in scored if r.get("record_gate") is not None]
    if gated:
        summary["record_gate_total"] = len(gated)
        summary["record_gate_fired"] = sum(bool(r["record_gate"]) for r in gated)
    if not scored:
        return summary
    metrics = [r["metrics"] for r in scored]
    if kind == "depot":
        present = [m for m in metrics if not m["gold_missing"]]
        missing = [m for m in metrics if m["gold_missing"]]
        summary.update(
            {
                "exact": sum(m["exact"] for m in metrics),
                "exact_rate": mean(float(m["exact"]) for m in metrics),
                "contains_rate": mean(float(m["contains"]) for m in present),
                "present_exact": sum(m["exact"] for m in present),
                "present_total": len(present),
                "missing_abstained": sum(m["abstained"] for m in missing),
                "missing_total": len(missing),
                "false_abstentions": sum(m["abstained"] for m in present),
            }
        )
    elif kind == "qa":
        summary.update(
            {
                "em": mean(m["em"] for m in metrics),
                "f1": mean(m["f1"] for m in metrics),
                "abstain_rate": mean(float(m["abstained"]) for m in metrics),
            }
        )
        if "em_any" in metrics[0]:
            summary["em_any"] = mean(m["em_any"] for m in metrics)
            summary["f1_any"] = mean(m["f1_any"] for m in metrics)
    else:
        summary.update(
            {
                "rouge_l_f1": mean(m["f1"] for m in metrics),
                "rouge_l_precision": mean(m["precision"] for m in metrics),
                "rouge_l_recall": mean(m["recall"] for m in metrics),
            }
        )
    return summary


# ----------------------------------------------------------------------------
# evaluation sets


def parquet_cases(root: Path, name: str, limit: int) -> list[dict]:
    """First ``limit`` validation rows of a Memorilla-Data config as cases."""
    import pyarrow.parquet as pq

    config = PARQUET_CONFIG[name]
    files = sorted((root / config).glob("validation-*.parquet"))
    if not files:
        raise FileNotFoundError(f"no validation parquet under {root / config}")
    table = pq.read_table(
        files[0], columns=["collection_id", "question", "answer", "documents"]
    ).slice(0, limit)
    cases = []
    for index, row in enumerate(table.to_pylist()):
        cases.append(
            {
                "name": f"{name}-{index:04d}-{row['collection_id']}",
                "documents": list(row["documents"]),
                "question": row["question"],
                "answer": row["answer"],
            }
        )
    return cases


def load_set(args, name: str) -> list[dict]:
    if name == "depot-dev":
        cases = read_cases(args.depot_dev)
    elif name == "depot-fresh":
        cases = read_cases(args.depot_fresh)
    elif name in QA_SETS:
        cases = parquet_cases(args.parquet, name, args.qa_limit)
    else:
        cases = parquet_cases(args.parquet, name, args.enwiki_limit)
    if args.cases_limit:
        cases = cases[: args.cases_limit]
    return cases


# ----------------------------------------------------------------------------
# model stack


@contextlib.contextmanager
def adapters_disabled(encoder):
    """Swap every ``LoRALinear`` for its frozen base projection, so the shared
    BF16 encoder computes the released (adapter-free) pooled vectors."""
    swapped = []
    for name, module in list(encoder.named_modules()):
        if isinstance(module, LoRALinear):
            parent_name, leaf = name.rsplit(".", 1)
            parent = encoder.get_submodule(parent_name)
            setattr(parent, leaf, module.base)
            swapped.append((parent, leaf, module))
    if not swapped:
        raise ValueError("encoder carries no adapters to disable")
    try:
        yield
    finally:
        for parent, leaf, module in swapped:
            setattr(parent, leaf, module)


def load_released_reader(paths: dict, config: dict, checkpoint: Path | None = None):
    """The released reader from ``checkpoint`` (default: the stage3 directory
    the copy17 stack was verified against); another released stage uses its
    own ``config.json``."""
    checkpoint = Path(checkpoint or paths["checkpoint"])
    if checkpoint.resolve() != Path(paths["checkpoint"]).resolve():
        config = json.loads((checkpoint / "config.json").read_text())
    weights = torch.load(
        checkpoint / "memory.pt",
        map_location="cpu",
        weights_only=True,
        mmap=True,
    )
    reader = (
        load_upstream(paths["upstream_source"], config, weights)
        .to(device="cuda", dtype=torch.bfloat16)
        .requires_grad_(False)
        .eval()
    )
    if reader.num_memories != RELEASED_MEMORY_SLOTS:
        raise ValueError("released reader is not the 16-slot geometry")
    return reader


class SoftMemory:
    """Released path: pooled last-token L2-normalized BF16 encoder vectors
    through the FP16 -> BF16 storage channel, read by the stage3 reader."""

    def __init__(self, models: dict, reader):
        self.models, self.reader, self.cache = models, reader, {}

    def pooled(self, text: str) -> torch.Tensor:
        if text in self.cache:
            return self.cache[text]
        encoder, tokenizer = self.models["encoder"], self.models["enc_tok"]
        tokens = tokenizer([text], return_tensors="pt").to("cuda")
        with torch.inference_mode(), adapters_disabled(encoder):
            normalized = normalize_features(
                encoder(**tokens, use_cache=False).last_hidden_state,
                tokens["attention_mask"],
            )
        vector = normalized[0].half().bfloat16()
        self.cache[text] = vector
        return vector

    def rows(self, case: dict) -> torch.Tensor:
        with torch.inference_mode():
            docs = torch.stack([self.pooled(d) for d in case["documents"]])[None]
            mask = torch.zeros((1, docs.shape[1]), dtype=torch.bool, device="cuda")
            query = self.pooled(case["question"].strip())[None]
            memory = self.reader(docs, mask, query)[0]
        if tuple(memory.shape) != (RELEASED_MEMORY_SLOTS, DECODER_WIDTH):
            raise ValueError("released reader changed memory geometry")
        return memory


def attach_span_head(models, candidate_dir, protocol) -> None:
    """A span-window candidate (``qwen_span_copy.py``) loads its span head."""
    if protocol.get("record_kind") == "span-window":
        from qwen_span_copy import attach_span_head as attach

        attach(models, candidate_dir, protocol)


def record_memory(models, protocol, case, *, suffix, pad_id, max_length, window_policy=None):
    """copy17's snapped record for one case (the evaluator's path under
    ``--snap-memory all --copy-contiguous-runs --copy-complete-words``, as
    ``encoder_lora_export.run_case`` mirrors it) without the decoder step.

    Returns the final BF16 token rows [16, width] and the record diagnostics.
    ``max_length`` bounds the padded encoder batch (the evaluator's bound is
    128; squad/drop passages need more under self-context doubling).
    ``window_policy`` (span-window candidates only, research): see
    ``qwen_span_copy.span_record_memory``.
    """
    if protocol.get("record_kind") == "span-window":
        from qwen_span_copy import span_record_memory

        if window_policy is None:
            return span_record_memory(
                models, protocol, case, suffix=suffix, pad_id=pad_id, max_length=max_length
            )
        return span_record_memory(
            models,
            protocol,
            case,
            suffix=suffix,
            pad_id=pad_id,
            max_length=max_length,
            window_policy=window_policy,
        )
    if window_policy is not None:
        raise ValueError("window policies need a span-window candidate")
    enc_tok, dec_tok = models["enc_tok"], models["dec_tok"]
    encoder, reader = models["encoder"], models["reader"]
    copy_head, snapper = models["copy_head"], models["snapper"]
    embedding = models["decoder"].get_input_embeddings()
    with torch.inference_mode(), torch.autocast("cuda", dtype=torch.bfloat16):
        documents = prefixed_documents(case["documents"], True)
        doubled, counts, single_ids, fallbacks = self_context_documents(
            enc_tok, documents
        )
        doubled = question_prefixed_documents(
            enc_tok,
            doubled,
            counts,
            case["question"],
            protocol.get("document_question_prefix"),
        )
        copy_ids = torch.cat(single_ids)
        tokens = enc_tok(
            doubled + [case["question"]], padding=True, return_tensors="pt"
        )
        if tokens.input_ids.shape[1] > max_length:
            raise ValueError(
                f"encoder bound exceeded: {tokens.input_ids.shape[1]} > {max_length}"
            )
        tokens = tokens.to("cuda")
        mask = tokens.attention_mask
        states = encoder_states_pair(encoder, tokens, protocol["dual_layer"])
        states, mask = self_context_states(states, mask, counts, suffix)
        final, layer = states
        docs, query = document_features_dual(final, layer, mask)
        memories = looped_read(
            reader,
            None,
            docs,
            torch.zeros((1, docs.shape[1]), dtype=torch.bool, device="cuda"),
            query,
            1,
        )[0]
        if tuple(memories.shape) != (RELEASED_MEMORY_SLOTS, DECODER_WIDTH):
            raise ValueError("candidate changed memory geometry")
        sources = token_ids_embeddings(embedding, copy_ids)
        copy_output, gate, moved, spans = copy_with_contiguous_runs(
            copy_head,
            memories,
            sources,
            None,
            min_gate=MIN_COPY_GATE,
            return_spans=True,
        )
        snapped, snap_ids, snap_top1 = snapper(copy_output)
        snapped_ids = snap_ids.tolist()
        inserted = 0
        if spans:
            completed, inserted = complete_copied_words(
                snapped_ids,
                spans,
                copy_ids.tolist(),
                dec_tok,
                pad_id,
                overflow="truncate",
            )
            rows = (
                embedding.weight[torch.tensor(completed, device=snap_ids.device)]
                .detach()
                .to(snapped.dtype)
            )
        else:
            completed = list(snapped_ids)
            rows = snapped
    return rows, {
        "copy_ids": copy_ids.tolist(),
        "spans": [[int(b), int(e), int(s)] for b, e, s in spans],
        "gate": [round(float(g), 4) for g in gate],
        "copy_moved_slots": int(moved),
        "copy_completed_tokens": int(inserted),
        "self_context_fallbacks": int(fallbacks),
        "encoder_tokens": int(tokens.input_ids.shape[1]),
        "snap_min_top1_cosine": round(float(snap_top1.min()), 4),
        "snap_top1_cosines": [round(float(c), 4) for c in snap_top1],
        "record_ids": [int(i) for i in completed],
        "record_text": "".join(dec_tok.decode([int(i)]) for i in completed),
    }


def record_rows_for_hybrid(models, record_info, mode, pad_id):
    """Rows 17-32 of the hybrid: the full record, or only its copied spans."""
    embedding = models["decoder"].get_input_embeddings()
    if mode == "full":
        ids, kept = record_info["record_ids"], None
    else:
        ids, kept = span_record_ids(
            [tuple(s) for s in record_info["spans"]],
            record_info["copy_ids"],
            models["dec_tok"],
            pad_id,
        )
    with torch.inference_mode():
        rows = embedding.weight[torch.tensor(ids, device="cuda")].detach()
    text = "".join(models["dec_tok"].decode([int(i)]) for i in ids)
    return rows, {
        "hybrid_record_ids": ids,
        "hybrid_record_text": text,
        "span_tokens": kept,
    }


def answer_with(models, memories, *, system_prompt, question, max_new_tokens):
    """Prompt (one marker per row, delta injection) and greedy generation under
    the evaluator's BF16 autocast."""
    decoder, dec_tok = models["decoder"], models["dec_tok"]
    with torch.inference_mode(), torch.autocast("cuda", dtype=torch.bfloat16):
        _, embeds, info = prepare_prompt(
            dec_tok,
            decoder.get_input_embeddings(),
            memories,
            system_prompt=system_prompt,
            question=question,
            device="cuda",
        )
        output = generate(decoder, dec_tok, embeds, max_new_tokens)
    return output, {**info, "prompt_tokens": int(embeds.shape[0])}


# ----------------------------------------------------------------------------
# driver


def paths_note(args) -> Path:
    return Path(args.checkpoint or (args.assets / DEFAULT_ASSETS["checkpoint"]))


def existing_names(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {row["name"] for row in read_rows(path)}


def read_rows(path: Path) -> list[dict]:
    """JSONL rows; split on "\n" only (``str.splitlines`` also breaks on the
    U+2028/U+0085 separators that chunk texts carry inside JSON strings)."""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().split("\n") if line.strip()]


def append_row(path: Path, row: dict) -> None:
    with path.open("a") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def save_json(path: Path, value) -> None:
    temporary = path.with_suffix(".partial")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(path)


def sanity_reference(path: Path) -> dict[str, str]:
    """``name -> answer`` from an ``encoder_lora_export.py reference`` receipt."""
    receipt = json.loads(Path(path).read_text())
    return {c["name"]: c["answer"] for c in receipt["cases"]}


def run_set(args, models, protocol, soft, name, cases, *, suffix, pad_id, reference):
    """All requested arms over one set; rows go to ``<arm>-<set>.jsonl``."""
    output = args.output
    paths = {arm: output / f"{arm}-{name}.jsonl" for arm in args.arms}
    done = {
        arm: existing_names(p) if args.resume else set() for arm, p in paths.items()
    }
    for arm, path in paths.items():
        if not args.resume and path.exists():
            raise FileExistsError(f"{path} exists; pass --resume or a new --output")
    max_new = (
        args.enwiki_max_new_tokens if name in RESTATE_SETS else args.max_new_tokens
    )
    hybrid_system = system_prompt_text(args.system_prompt)
    mismatches = []
    started = time.perf_counter()
    first = True
    for index, case in enumerate(cases):
        pending = [arm for arm in args.arms if case["name"] not in done[arm]]
        if not pending:
            continue
        if first and any(arm != "soft" for arm in pending):
            assert_shared_token_ids(
                models["enc_tok"],
                models["dec_tok"],
                prefixed_documents(case["documents"], True),
                True,
            )
            first = False
        base = {
            "name": case["name"],
            "set": name,
            "question": case["question"],
            "gold": case["answer"],
            "documents": len(case["documents"]),
        }
        try:
            soft_rows = soft.rows(case) if {"soft", "hybrid"} & set(pending) else None
            record = None
            if {"record", "hybrid"} & set(pending):
                record = record_memory(
                    models,
                    protocol,
                    case,
                    suffix=suffix,
                    pad_id=pad_id,
                    max_length=args.encoder_max_length,
                )
        except ValueError as error:
            for arm in pending:
                append_row(paths[arm], {**base, "arm": arm, "error": str(error)})
            print(json.dumps({"case": case["name"], "error": str(error)}), flush=True)
            continue
        for arm in pending:
            extra = {}
            if arm == "soft":
                memories, system = soft_rows, args.soft_system_prompt
            elif arm == "record":
                memories, system = record[0], EXACT_PROMPT
                extra = {k: v for k, v in record[1].items() if k != "copy_ids"}
            else:
                gate = None
                if args.record_gate:
                    gate = record_gate_decision(
                        getattr(args, "record_gate_mode", "cosine"),
                        [tuple(s) for s in record[1]["spans"]],
                        record[1]["gate"],
                        record[1]["snap_top1_cosines"],
                        args.record_gate_cosine,
                    )
                extra = {
                    "record_text": record[1]["record_text"],
                    "spans": record[1]["spans"],
                    "snap_min_top1_cosine": record[1]["snap_min_top1_cosine"],
                    "record_gate": gate,
                }
                if gate is False:
                    # Gate closed: the soft memory alone, under the soft prompt.
                    memories, system = soft_rows, args.soft_system_prompt
                else:
                    rows, hybrid_info = record_rows_for_hybrid(
                        models, record[1], args.record_rows, pad_id
                    )
                    memories = compose_memory_rows(soft_rows, rows, args.row_order)
                    system = hybrid_system
                    extra.update(hybrid_info)
            output_row, info = answer_with(
                models,
                memories,
                system_prompt=system,
                question=case["question"],
                max_new_tokens=max_new,
            )
            raw_answer = output_row["answer"]
            answer = raw_answer
            canonical_source = None
            if args.answer_canonicalize:
                canonical_source = (
                    extra.get("hybrid_record_text")
                    if arm == "hybrid"
                    else extra.get("record_text")
                    if arm == "record"
                    else None
                )
                if canonical_source is not None:
                    answer = canonicalize_answer(raw_answer, canonical_source)
            metrics = case_metrics(name, answer, case)
            row = {
                **base,
                "arm": arm,
                "system_prompt": system,
                "memory_rows": int(memories.shape[0]),
                "answer": answer,
                **(
                    {
                        "answer_raw": raw_answer,
                        "canonicalized": answer != raw_answer.strip(),
                    }
                    if args.answer_canonicalize
                    else {}
                ),
                "metrics": metrics,
                "finish_reason": output_row["finish_reason"],
                "generated_tokens": len(output_row["token_ids"]),
                "generation_seconds": round(output_row["decoder_seconds"], 4),
                "prompt_tokens": info["prompt_tokens"],
                **extra,
            }
            if arm == "record" and reference is not None:
                expected = reference.get(case["name"])
                row["reference_answer"] = expected
                row["matches_reference"] = answer == expected
                if answer != expected:
                    mismatches.append((case["name"], answer, expected))
            append_row(paths[arm], row)
            print(
                json.dumps(
                    {
                        "set": name,
                        "index": index,
                        "arm": arm,
                        "name": case["name"],
                        "answer": answer[:120],
                        "gold": case["answer"][:80],
                        "metrics": {
                            k: (round(v, 3) if isinstance(v, float) else v)
                            for k, v in metrics.items()
                        },
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
    summary = {
        "set": name,
        "cases": len(cases),
        "elapsed_seconds": round(time.perf_counter() - started, 1),
        "arms": {arm: summarize_rows(name, read_rows(paths[arm])) for arm in args.arms},
        "config": {
            "system_prompt": args.system_prompt,
            "hybrid_system_text": hybrid_system,
            "soft_system_text": args.soft_system_prompt,
            "record_rows": args.record_rows,
            "row_order": args.row_order,
            "record_gate": args.record_gate,
            "record_gate_cosine": args.record_gate_cosine,
            **(
                {"record_gate_mode": args.record_gate_mode}
                if args.record_gate_mode != "cosine"
                else {}
            ),
            "answer_canonicalize": args.answer_canonicalize,
            "released_checkpoint": str(args.released_checkpoint or paths_note(args)),
            "max_new_tokens": max_new,
            "encoder_max_length": args.encoder_max_length,
        },
    }
    if reference is not None and "record" in args.arms:
        summary["record_reference_mismatches"] = [
            {"name": n, "answer": a, "reference": e} for n, a, e in mismatches
        ]
    save_json(output / f"summary-{name}.json", summary)
    return summary


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--assets", type=Path, required=True, help="Memorilla asset root")
    for key in DEFAULT_ASSETS:
        p.add_argument("--" + key.replace("_", "-"), type=Path)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--depot-dev", type=Path, required=True)
    p.add_argument("--depot-fresh", type=Path, required=True)
    p.add_argument(
        "--parquet", type=Path, required=True, help="Memorilla-Data data/ dir"
    )
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--arm", choices=ARMS, action="append", default=[])
    p.add_argument("--arms", default=None, help="comma-separated arms")
    p.add_argument("--sets", default=",".join(SETS))
    p.add_argument(
        "--system-prompt", default="merged", help="hybrid: helpful|exact|merged|text"
    )
    p.add_argument("--soft-system-prompt", default=HELPFUL_PROMPT)
    p.add_argument("--record-rows", choices=("full", "spans"), default="full")
    p.add_argument("--row-order", choices=ROW_ORDERS, default="soft-first")
    p.add_argument("--record-gate", action="store_true")
    p.add_argument("--record-gate-cosine", type=float, default=RECORD_GATE_COSINE)
    p.add_argument(
        "--record-gate-mode",
        choices=RECORD_GATE_MODES,
        default="cosine",
        help="with --record-gate: snap-cosine rule or the copy head's learned gate",
    )
    p.add_argument("--answer-canonicalize", action="store_true")
    p.add_argument("--released-checkpoint", type=Path, help="released reader dir")
    p.add_argument("--max-new-tokens", type=int, default=48)
    p.add_argument("--enwiki-max-new-tokens", type=int, default=160)
    p.add_argument("--encoder-max-length", type=int, default=1024)
    p.add_argument("--qa-limit", type=int, default=200)
    p.add_argument("--enwiki-limit", type=int, default=50)
    p.add_argument("--cases-limit", type=int, default=0)
    p.add_argument(
        "--sanity-reference", type=Path, help="reference.json of lora-ref-dev"
    )
    p.add_argument("--resume", action="store_true")
    p.add_argument("--cotenant-limit-mib", type=int, default=40960)
    args = p.parse_args(argv)
    arms = list(args.arm)
    if args.arms:
        arms += [a.strip() for a in args.arms.split(",") if a.strip()]
    args.arms = list(dict.fromkeys(arms)) or list(ARMS)
    if any(a not in ARMS for a in args.arms):
        p.error("arms must be among " + ",".join(ARMS))
    args.sets = [s.strip() for s in args.sets.split(",") if s.strip()]
    if any(s not in SETS for s in args.sets):
        p.error("sets must be among " + ",".join(SETS))
    if args.system_prompt == "<|memory|>" or "<|memory|>" in args.soft_system_prompt:
        p.error("system prompts cannot carry memory placeholders")
    if not 0 <= args.record_gate_cosine <= 1:
        p.error("record gate cosine must lie in [0, 1]")
    return args


def main(argv=None):
    args = parse_args(argv)
    paths = {
        key: Path(getattr(args, key) or (args.assets / relative))
        for key, relative in DEFAULT_ASSETS.items()
    }
    candidate, protocol = load_candidate(args.candidate)
    sets = {name: load_set(args, name) for name in args.sets}
    args.output.mkdir(parents=True, exist_ok=True)
    cotenant = wait_for_gpu(args.cotenant_limit_mib)
    loading = time.perf_counter()
    models = load_models(args.candidate, protocol, paths)
    attach_span_head(models, args.candidate, protocol)
    released = load_released_reader(
        paths, models["reader_config"], args.released_checkpoint
    )
    soft = SoftMemory(models, released)
    load_seconds = round(time.perf_counter() - loading, 1)
    suffix = encoder_special_suffix(models["enc_tok"])
    pad = models["dec_tok"].encode(" ", add_special_tokens=False)
    if len(pad) != 1:
        raise ValueError("record pad must be a single token")
    reference = (
        sanity_reference(args.sanity_reference) if args.sanity_reference else None
    )
    result = {
        "schema": "antfly.memorilla.qwen-hybrid-eval.v1",
        "completed": False,
        "rollout_qualified": False,
        "candidate": str(args.candidate.resolve()),
        "candidate_sha256": candidate.get("protocol_sha256"),
        "arms": args.arms,
        "sets": args.sets,
        "system_prompt": args.system_prompt,
        "record_rows": args.record_rows,
        "row_order": args.row_order,
        "record_gate": args.record_gate,
        "record_gate_cosine": args.record_gate_cosine,
        **(
            {"record_gate_mode": args.record_gate_mode}
            if args.record_gate_mode != "cosine"
            else {}
        ),
        "answer_canonicalize": args.answer_canonicalize,
        "released_checkpoint": str(args.released_checkpoint or paths["checkpoint"]),
        "gpu_cotenant_mib_at_start": cotenant,
        "model_load_seconds": load_seconds,
        "cuda_free_bytes_after_load": torch.cuda.mem_get_info()[0],
        "torch_version": torch.__version__,
        "summaries": {},
    }
    save_json(args.output / "summary.json", result)
    for name, cases in sets.items():
        result["summaries"][name] = run_set(
            args,
            models,
            protocol,
            soft,
            name,
            cases,
            suffix=suffix,
            pad_id=pad[0],
            reference=reference,
        )
        save_json(args.output / "summary.json", result)
    result["completed"] = True
    result["cuda_peak_allocated_bytes"] = torch.cuda.max_memory_allocated()
    save_json(args.output / "summary.json", result)
    table = {
        name: {
            arm: {k: v for k, v in s["arms"][arm].items() if k not in ("set", "kind")}
            for arm in args.arms
        }
        for name, s in result["summaries"].items()
    }
    print(json.dumps({"completed": True, "table": table}, indent=1), flush=True)
    if reference is not None and any(
        s.get("record_reference_mismatches") for s in result["summaries"].values()
    ):
        raise SystemExit("record arm differs from the sanity reference")


# ----------------------------------------------------------------------------
# paper protocol: the Memorilla benchmarks (the eight registered upstream
# benchmarks pmv2, pv4, factkg, triviaqa, narrativeqa, pubmedqa, lamp4, lamp7,
# plus EnWiki reconstruction)
#
# ``paper-prep`` (CPU, a venv with the upstream package's ``datasets`` /
# ``rouge_score`` / ``nltk`` dependencies) selects the evaluated rows through
# upstream ``load_split_with_embeddings`` and writes one case per row with its
# stored-embedding digests, the cosine top-k chunk order of upstream
# ``evaluate_baselines.retrieve`` and the top-k chunk texts. ``paper-run`` (GPU,
# the CUDA venv) generates every arm in left-padded greedy batches from those
# cases and the stored Qwen3-Embedding-4B vectors. ``paper-choice-embed`` (GPU,
# the CUDA venv; choice-scored pmv2/pv4/factkg only) embeds every generation
# and choice with the encoder for upstream's nearest-choice rule.
# ``paper-score`` (CPU, the upstream venv) scores the rows with upstream
# ``memorilla.metrics`` and adds paired bootstrap intervals for arm deltas.

PAPER_COMMANDS = ("paper-prep", "paper-run", "paper-choice-embed", "paper-score")
# The eight registered upstream benchmarks (``memorilla.benchmarks``, test
# splits) plus EnWiki reconstruction (not registered).
PAPER_TARGETS = {
    "pmv2": ("pmv2", "test"),
    "pv4": ("pv4", "test"),
    "factkg": ("factkg", "test"),
    "triviaqa": ("triviaqa", "test"),
    "narrativeqa": ("narrativeqa", "test"),
    "pubmedqa": ("pubmedqa", "test"),
    "lamp4": ("lamp4", "test"),
    "lamp7": ("lamp7", "test"),
    "enwiki": ("enwiki", "validation"),
}
# Unregistered Memorilla-Data configs (validation split), prompted and scored
# with evaluate.py's custom-config defaults like EnWiki: helpful prompt, no
# suffix, generation scoring, 128 new tokens (CUSTOM_MAX_NEW_TOKENS). Plan
# 8.17 uses them as task types that have no gate profile.
CUSTOM_TARGETS = (
    "squad_v2",
    "drop",
    "coqa",
    "quail",
    "msmarco",
    "samsum",
    "dialogsum",
    "cnn_dailymail",
    "pwc",
)
CUSTOM_MAX_NEW_TOKENS = 128
PAPER_TARGETS.update({t: (t, "validation") for t in CUSTOM_TARGETS})
# Paper Table 3: 64 new tokens for QA, 512 for EnWiki reconstruction. The
# released ``benchmarks.py`` gives NarrativeQA 32; the run generates the larger
# budget and keeps the smaller one as a prefix (greedy decoding). The other
# registered benchmarks use their registered budget.
PAPER_TABLE3_LIMITS = {
    "pmv2": 256,
    "pv4": 256,
    "factkg": 8,
    "triviaqa": 64,
    "narrativeqa": 64,
    "pubmedqa": 64,
    "lamp4": 128,
    "lamp7": 128,
    "enwiki": 512,
    **dict.fromkeys(CUSTOM_TARGETS, CUSTOM_MAX_NEW_TOKENS),
}
# Paper section 2.6: PersonaMem-v2 and PersonalizationV4 are evaluated with
# their benchmark-specific fine-tunes; every other target with stage3.
PAPER_TARGET_CHECKPOINTS = {"pmv2": "checkpoints/pmv2", "pv4": "checkpoints/pv4"}
# evaluate.py's prompt token limit for a custom (unregistered) config.
CUSTOM_MAX_LENGTH = 2048
# Optional per-row columns that choice scoring reads (upstream EVAL_COLUMNS).
CHOICES_COLUMN, HARD_COLUMN = "choices", "hard"
CHOICE_SCORING, POLARITY_SCORING = "choice", "polarity"
CHOICE_TABLE = "choice-embeddings"
# Mirror of upstream ``metrics.CHAT_TOKEN_PATTERN`` for the CUDA venv (which
# cannot import ``memorilla.metrics``); ``paper-score`` re-derives every text
# with upstream ``clean_generation`` and fails on any lookup mismatch.
CHAT_TOKEN_PATTERN = re.compile(r"<\|(?:im_end|im_start|endoftext|im_sep)\|>")
BOOTSTRAP_RESAMPLES = 10_000
QUESTION_TYPES = ("who", "which", "what", "why", "how", "where", "when", "other")
ENCODER_SLUG = "qwen3-embedding-4b"
STORE_INDEX = "index.safetensors"
STORE_QUESTIONS = "questions.safetensors"
STORE_SHARD = "documents-{:05d}.safetensors"
STORE_TENSOR = "embeddings"
DOCUMENT_SEPARATOR = "\n\n"
RETRIEVAL_CONTEXT_LENGTH = 32_768
GENERATION_RESERVE = 32
PAPER_EOS_IDS = (151645, 151643)
PAPER_PAD_ID = 151643
RECORD_TOP_K = 2
CLOSED_BOOK = "closed-book"
RAG_ARM = re.compile(r"^rag-(\d+)$")
PAPER_MEMORY_ARMS = ("soft", "soft-noise", "hybrid", "hybrid-bench", "hybrid-noise")
FIRST_WORD = re.compile(r"[A-Za-z]+")


def upstream_benchmarks():
    """``memorilla.benchmarks`` of the pinned upstream package (on PYTHONPATH)."""
    from memorilla import benchmarks

    return benchmarks


def paper_protocol(target: str, benchmarks=None) -> dict:
    """Memorilla and baseline prompting for one target from upstream
    ``BENCHMARKS``. EnWiki and ``CUSTOM_TARGETS`` are not registered: they
    take ``evaluate.py``'s custom-config defaults (helpful prompt, no suffix,
    generation scoring), EnWiki with the paper's 512-token reconstruction
    budget and the others with the custom default of 128."""
    if target not in PAPER_TARGETS:
        raise ValueError("unknown paper target: " + target)
    benchmarks = benchmarks or upstream_benchmarks()
    if target == "enwiki" or target in CUSTOM_TARGETS:
        return {
            "target": target,
            "system": benchmarks.HELPFUL_PROMPT,
            "suffix": "",
            "max_new_tokens": PAPER_TABLE3_LIMITS[target],
            "paper_max_new_tokens": PAPER_TABLE3_LIMITS[target],
            "max_length": CUSTOM_MAX_LENGTH,
            "scoring": benchmarks.GENERATION,
            "metrics": (benchmarks.ROUGE1, benchmarks.ROUGEL),
            "baseline": None,
        }
    benchmark = benchmarks.BENCHMARKS[target]
    return {
        "target": target,
        "system": benchmark.system_prompt,
        "suffix": benchmark.suffix,
        "max_new_tokens": benchmark.max_new_tokens,
        "paper_max_new_tokens": PAPER_TABLE3_LIMITS[target],
        "max_length": benchmark.max_length,
        "scoring": benchmark.scoring,
        "metrics": tuple(benchmark.metrics),
        "baseline": {
            "system": benchmark.baseline.system_prompt,
            "suffix": benchmark.baseline.suffix,
            "max_new_tokens": benchmark.baseline.max_new_tokens,
        },
    }


def paper_released_checkpoint(
    target: str, assets: Path, default: Path, override: Path | None = None
) -> Path:
    """Released reader for a target: ``override`` when given, else the
    benchmark-specific fine-tune (pmv2, pv4; paper section 2.6), else
    ``default`` (stage3, or ``--checkpoint``)."""
    if override:
        return Path(override)
    relative = PAPER_TARGET_CHECKPOINTS.get(target)
    return Path(assets) / relative if relative else Path(default)


def file_sha256(path: Path, block: int = 1 << 24) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(block):
            digest.update(chunk)
    return digest.hexdigest()


def truncate_prompt_right(embeds: torch.Tensor, max_length: int | None):
    """Upstream ``MemoryCollator`` tokenizes the rendered prompt with
    ``truncation=True, max_length=benchmark.max_length`` and the decoder
    tokenizer's default right truncation side; the same cut on prompt
    embeddings -> (embeds, untruncated length or None)."""
    if not max_length or embeds.shape[0] <= int(max_length):
        return embeds, None
    return embeds[: int(max_length)], int(embeds.shape[0])


def clean_generation_text(text: str) -> str:
    """Upstream ``metrics.clean_generation`` (chat tokens removed, stripped)."""
    return CHAT_TOKEN_PATTERN.sub("", str(text)).strip()


def nonempty_choices(choices) -> list[str]:
    """The choice texts upstream ``choice_accuracy`` embeds: every choice whose
    stripped text is non-empty, unstripped, in order."""
    return [choice for choice in choices if choice.strip()]


def choice_embedding_texts(rows: list[dict]) -> list[str]:
    """Distinct texts the choice scorer embeds for ``rows``: each prediction
    (every budget key) cleaned and stripped as upstream embeds
    ``prediction.strip()``, and every non-empty choice; first-seen order."""
    texts = {}
    for row in rows:
        for prediction in row["predictions"].values():
            texts.setdefault(clean_generation_text(prediction).strip(), None)
        for choice in nonempty_choices(row.get(CHOICES_COLUMN) or []):
            texts.setdefault(choice, None)
    return list(texts)


def text_key(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def choice_accuracy_from_correct(correct, hard=None, want_hard: bool = False) -> dict:
    """Upstream ``choice_accuracy`` aggregation over per-row correctness:
    ``accuracy`` and, when requested, ``accuracy_hard`` over the hard rows (0
    when no row is hard)."""
    correct = [bool(c) for c in correct]
    if not correct:
        raise ValueError("choice accuracy needs at least one row")
    scores = {"accuracy": sum(correct) / len(correct)}
    if want_hard:
        flags = [bool(h) for h in (hard or [])]
        if len(flags) != len(correct):
            raise ValueError("hard flags must align with the rows")
        picked = [c for c, h in zip(correct, flags, strict=True) if h]
        scores["accuracy_hard"] = sum(picked) / len(picked) if picked else 0.0
    return scores


def paired_bootstrap_ci(
    left: dict,
    right: dict,
    resamples: int = BOOTSTRAP_RESAMPLES,
    seed: int = 23,
    confidence: float = 0.95,
) -> dict:
    """Paired bootstrap over the common rows of two per-row score maps: the
    mean of ``right - left`` and its percentile interval (scores x 100)."""
    import numpy as np

    if resamples < 1 or not 0 < confidence < 1:
        raise ValueError("bootstrap needs resamples >= 1 and 0 < confidence < 1")
    common = sorted(set(left) & set(right))
    if not common:
        return {"rows": 0}
    deltas = np.array([right[n] - left[n] for n in common], dtype=np.float64)
    rng = np.random.default_rng(int(seed))
    means = np.empty(int(resamples), dtype=np.float64)
    for start in range(0, int(resamples), 1000):
        stop = min(int(resamples), start + 1000)
        picks = rng.integers(0, len(deltas), size=(stop - start, len(deltas)))
        means[start:stop] = deltas[picks].mean(axis=1)
    tail = (1 - confidence) / 2
    low, high = np.quantile(means, [tail, 1 - tail])
    return {
        "rows": len(common),
        "mean_delta": round(100 * float(deltas.mean()), 2),
        "ci_low": round(100 * float(low), 2),
        "ci_high": round(100 * float(high), 2),
        "confidence": confidence,
        "resamples": int(resamples),
        "seed": int(seed),
    }


def merged_system_prompt(benchmark_system: str) -> str:
    """The hybrid's merged prompt for a benchmark: its Memorilla system prompt,
    a space, then copy17's exact-answer prompt (the order of ``merged``)."""
    return benchmark_system + " " + EXACT_PROMPT


def memory_user_turn(question, suffix: str) -> str:
    """Upstream ``build_messages`` user turn: the stripped question + suffix.
    ``prepare_prompt`` strips the turn again, so a suffix with trailing
    whitespace would change the prompt; reject it."""
    turn = str(question).strip() + suffix
    if turn != turn.strip():
        raise ValueError("suffix with surrounding whitespace would be stripped")
    return turn


def baseline_user_turn(documents, question, suffix: str) -> str:
    """Upstream ``evaluate_baselines.user_message``: chunks most similar first,
    blank-line separated, then the unstripped question and the suffix."""
    if documents:
        return (
            DOCUMENT_SEPARATOR.join(documents)
            + DOCUMENT_SEPARATOR
            + str(question)
            + suffix
        )
    return str(question) + suffix


def question_type(question: str) -> str:
    """Paper Figure 4a bucket: the question's first word (first alphabetic run,
    lower-cased) when it is one of who/which/what/why/how/where/when, else
    ``other``."""
    match = FIRST_WORD.search(str(question))
    word = match.group(0).lower() if match else ""
    return word if word in QUESTION_TYPES[:-1] else "other"


def sample_rows(total: int, count: int, seed: int) -> list[int]:
    """``count`` distinct row indices of ``range(total)`` drawn with ``seed``,
    ascending; every row when ``count`` is 0 or covers the split."""
    import random

    if total < 0 or count < 0:
        raise ValueError("row counts must be nonnegative")
    if count == 0 or count >= total:
        return list(range(total))
    return sorted(random.Random(seed).sample(range(total), count))


def retrieval_order(documents: torch.Tensor, question: torch.Tensor, top_k: int):
    """Upstream ``evaluate_baselines.retrieve`` for one row: cosine between the
    stored chunk and question vectors (FP32, L2-normalized), most similar
    first -> (indices, scores) of the first ``min(top_k, n)`` chunks."""
    if documents.ndim != 2 or question.ndim != 1:
        raise ValueError("retrieval needs [n, d] chunks and a [d] question")
    if top_k < 0:
        raise ValueError("top_k must be nonnegative")
    docs = torch.nn.functional.normalize(documents.float(), dim=1)
    query = torch.nn.functional.normalize(question.float().unsqueeze(0), dim=1)
    similarities = (docs @ query.T).squeeze(1)
    order = similarities.argsort(descending=True)[: min(top_k, len(docs))]
    return order.tolist(), [round(float(similarities[i]), 6) for i in order]


def noise_seed(base: int, row: int) -> int:
    return int(base) * 1_000_003 + int(row)


def noise_documents(documents: torch.Tensor, seed: int) -> torch.Tensor:
    """Paper sanity check: every stored chunk vector replaced by Gaussian noise
    with the same per-row L2 norm, returned in the stored dtype (FP16)."""
    if documents.ndim != 2:
        raise ValueError("noise needs [n, d] chunk vectors")
    generator = torch.Generator().manual_seed(int(seed))
    noise = torch.randn(documents.shape, generator=generator, dtype=torch.float32)
    norms = documents.float().norm(dim=-1, keepdim=True)
    return (noise / noise.norm(dim=-1, keepdim=True) * norms).to(documents.dtype)


def tensor_digest(tensor: torch.Tensor) -> str:
    """sha256 of a tensor's dtype, shape and raw bytes (alignment receipts)."""
    flat = tensor.detach().cpu().contiguous()
    digest = hashlib.sha256(f"{flat.dtype}:{tuple(flat.shape)}:".encode())
    digest.update(flat.view(torch.uint8).numpy().tobytes() if flat.numel() else b"")
    return digest.hexdigest()


def embedding_directory(data_root: Path, config: str, split: str) -> Path:
    return Path(data_root) / "embeddings" / ENCODER_SLUG / config / split


class StoredEmbeddings:
    """Read-only mirror of upstream ``EmbeddingStore`` (same index, shards and
    slicing) that runs without ``datasets``; ``paper-prep`` checks that both
    readers return identical tensors for every evaluated row and records
    digests that ``paper-run`` re-checks."""

    def __init__(self, directory: Path):
        from safetensors.torch import load_file

        self.directory = Path(directory)
        index = load_file(str(self.directory / STORE_INDEX))
        self.collection_ids = index["collection_ids"]
        self._shard, self._start = index["shard"], index["start"]
        self._length = index["length"]
        self._handles = {}

    def _open(self, name: str):
        from safetensors import safe_open

        if name not in self._handles:
            self._handles[name] = safe_open(str(self.directory / name), framework="pt")
        return self._handles[name]

    def __len__(self) -> int:
        return int(self.collection_ids.numel())

    @property
    def num_questions(self) -> int:
        return int(self._open(STORE_QUESTIONS).get_slice(STORE_TENSOR).get_shape()[0])

    def documents(self, collection_id: int) -> torch.Tensor:
        target = torch.tensor(int(collection_id))
        position = int(torch.searchsorted(self.collection_ids, target))
        if position >= len(self) or int(self.collection_ids[position]) != int(
            collection_id
        ):
            raise KeyError(f"collection {collection_id} not in {self.directory}")
        start, length = int(self._start[position]), int(self._length[position])
        handle = self._open(STORE_SHARD.format(int(self._shard[position])))
        return handle.get_slice(STORE_TENSOR)[start : start + length]

    def question(self, row: int) -> torch.Tensor:
        handle = self._open(STORE_QUESTIONS)
        return handle.get_slice(STORE_TENSOR)[row : row + 1][0]


def paper_arm_spec(arm: str) -> dict:
    """``closed-book`` / ``rag-<k>``: chunk texts in the user turn under the
    benchmark's baseline protocol. Memory arms: ``soft`` (released reader over
    every stored chunk vector, benchmark prompt), ``hybrid`` (soft rows + the
    gated copy17 record of the top-2 chunks, merged prompt), ``hybrid-bench``
    (the same under the benchmark prompt alone) and the ``-noise`` variants
    (soft stream from norm-matched Gaussian chunk vectors)."""
    if arm == CLOSED_BOOK:
        return {"arm": arm, "kind": "text", "top_k": 0}
    match = RAG_ARM.match(arm)
    if match:
        top_k = int(match.group(1))
        if top_k < 1:
            raise ValueError("rag arms need k >= 1")
        return {"arm": arm, "kind": "text", "top_k": top_k}
    if arm not in PAPER_MEMORY_ARMS:
        raise ValueError("unknown paper arm: " + arm)
    return {
        "arm": arm,
        "kind": "memory",
        "noise": arm.endswith("-noise"),
        "record": arm.startswith("hybrid"),
        "merged": arm in ("hybrid", "hybrid-noise"),
    }


def paper_memory_system(protocol: dict, spec: dict, gate_open: bool | None) -> str:
    """System text of a memory arm: the benchmark prompt, merged with copy17's
    exact-answer prompt only when the arm merges and the record gate opened
    (a closed gate leaves the soft rows alone under the benchmark prompt)."""
    if spec["kind"] != "memory":
        raise ValueError("not a memory arm")
    if spec["record"] and spec["merged"] and gate_open:
        return merged_system_prompt(protocol["system"])
    return protocol["system"]


def paper_text_messages(protocol: dict, documents, question) -> list[dict]:
    baseline = protocol["baseline"]
    if baseline is None:
        raise ValueError(protocol["target"] + " has no baseline protocol")
    return [
        {"role": "system", "content": baseline["system"]},
        {
            "role": "user",
            "content": baseline_user_turn(documents, question, baseline["suffix"]),
        },
    ]


def paper_generation_limit(protocol: dict, spec: dict, override: int = 0) -> int:
    """Tokens generated per row: the baseline budget for text arms; for memory
    arms the larger of the released and the paper budget (or ``override``)."""
    if override:
        return int(override)
    if spec["kind"] == "text":
        return int(protocol["baseline"]["max_new_tokens"])
    return max(int(protocol["max_new_tokens"]), int(protocol["paper_max_new_tokens"]))


def paper_truncations(protocol: dict, spec: dict, limit: int, extra=()) -> list[int]:
    """Prefix budgets also scored: the released memory budget when generation
    ran longer (NarrativeQA 32 of 64) and any ``extra`` below ``limit``."""
    points = set(int(e) for e in extra if 0 < int(e) < limit)
    if spec["kind"] == "memory" and int(protocol["max_new_tokens"]) < limit:
        points.add(int(protocol["max_new_tokens"]))
    return sorted(points)


def trim_generation(ids, eos_ids=PAPER_EOS_IDS) -> tuple[list[int], str]:
    """Generated ids up to and including the first EOS -> (ids, reason)."""
    ids = [int(i) for i in ids]
    for index, token in enumerate(ids):
        if token in eos_ids:
            return ids[: index + 1], "eos"
    return ids, "length"


def left_pad(prompts: list[torch.Tensor]) -> tuple[torch.Tensor, torch.Tensor]:
    """[L_i, width] prompt embeddings -> left-padded [B, L, width] (zeros) and
    the [B, L] attention mask."""
    if not prompts or any(p.ndim != 2 for p in prompts):
        raise ValueError("left_pad needs [length, width] prompts")
    longest = max(p.shape[0] for p in prompts)
    batch = prompts[0].new_zeros((len(prompts), longest, prompts[0].shape[1]))
    mask = torch.zeros(
        (len(prompts), longest), dtype=torch.long, device=prompts[0].device
    )
    for index, prompt in enumerate(prompts):
        batch[index, longest - prompt.shape[0] :] = prompt
        mask[index, longest - prompt.shape[0] :] = 1
    return batch, mask


def bucket_rows(rows: list[dict], key: str = "question_type") -> dict[str, list]:
    """Rows grouped by ``key`` in QUESTION_TYPES order (empty buckets kept)."""
    buckets = {name: [] for name in QUESTION_TYPES}
    for row in rows:
        buckets.setdefault(row[key], []).append(row)
    return buckets


def is_unknown(text: str) -> bool:
    """copy17's abstention literal, after SQuAD normalization."""
    return normalize_answer(text) == "unknown"


# ---------------------------------------------------------------- paper-prep


def parquet_rows(files, rows, columns=("question", "documents")) -> dict[int, dict]:
    """``columns`` of the given global rows (row order of the sorted shard
    files, as upstream ``load_split`` concatenates them), reading one row
    group at a time so long-story splits stay out of memory."""
    import pyarrow.parquet as pq

    wanted = sorted(set(int(r) for r in rows))
    found, offset, cursor = {}, 0, 0
    for path in files:
        handle = pq.ParquetFile(path)
        for group in range(handle.metadata.num_row_groups):
            count = handle.metadata.row_group(group).num_rows
            local = []
            while cursor < len(wanted) and wanted[cursor] < offset + count:
                local.append(wanted[cursor] - offset)
                cursor += 1
            if local:
                table = handle.read_row_group(group, columns=list(columns))
                for index in local:
                    found[offset + index] = {
                        c: table.column(c)[index].as_py() for c in columns
                    }
                del table
            offset += count
    if cursor != len(wanted):
        raise IndexError("requested rows beyond the split")
    return found


def paper_scoring_check(target: str, dataset) -> dict:
    """Upstream ``evaluate.load_target`` on the whole split: choice scoring
    reports ``accuracy_hard`` exactly when the split flags hard rows, then
    ``metrics.check_scoring_inputs`` (every reference among its row's choices;
    the NLTK data METEOR needs). EnWiki and the custom targets are not
    registered: no check."""
    if target == "enwiki" or target in CUSTOM_TARGETS:
        return {"scoring": "generation", "metrics": ["rouge1", "rougeL"]}
    from dataclasses import replace

    from memorilla import benchmarks
    from memorilla.metrics import check_scoring_inputs

    benchmark = benchmarks.BENCHMARKS[target]
    if benchmark.scoring == benchmarks.CHOICE:
        has_hard = HARD_COLUMN in dataset.column_names and any(dataset[HARD_COLUMN])
        metrics = (
            (benchmarks.ACCURACY, benchmarks.ACCURACY_HARD)
            if has_hard
            else (benchmarks.ACCURACY,)
        )
        benchmark = replace(benchmark, metrics=metrics)
    check_scoring_inputs(benchmark, dataset)
    return {"scoring": benchmark.scoring, "metrics": list(benchmark.metrics)}


def case_name(target: str, split: str, row: int, collection_id) -> str:
    """Case names of the registered split keep their original form; another
    split names itself, so its rows never collide with the registered ones."""
    if split == PAPER_TARGETS[target][1]:
        return f"{target}-{row:05d}-{collection_id}"
    return f"{target}-{split}-{row:05d}-{collection_id}"


def cases_split(target: str, cases) -> str:
    """The one split a cases file was drawn from (absent = registered)."""
    splits = {c.get("split", PAPER_TARGETS[target][1]) for c in cases}
    if len(splits) != 1:
        raise ValueError(f"cases mix splits: {sorted(splits)}")
    return splits.pop()


def records_split(target: str, records) -> str:
    """The one split paper-run records came from, read back from case_name
    (records do not carry the split field)."""
    registered = PAPER_TARGETS[target][1]
    others = [s for s in ("train", "validation", "test") if s != registered]
    splits = set()
    for record in records:
        name = str(record["name"])
        found = [s for s in others if name.startswith(f"{target}-{s}-")]
        splits.add(found[0] if found else registered)
    if len(splits) != 1:
        raise ValueError(f"records mix splits: {sorted(splits)}")
    return splits.pop()


def paper_prep(args) -> dict:
    import os

    os.environ.setdefault("HF_DATASETS_CACHE", str(args.datasets_cache))
    from memorilla.data import split_files
    from memorilla.embeddings import load_split_with_embeddings

    config, registered = PAPER_TARGETS[args.target]
    split = args.split or registered
    # Upstream EVAL_COLUMNS; load_split skips the ones a split lacks.
    dataset, store = load_split_with_embeddings(
        config,
        split,
        args.data_root,
        ["collection_id", "question", "answer", CHOICES_COLUMN, HARD_COLUMN],
    )
    scoring = paper_scoring_check(args.target, dataset)
    extra_columns = [
        c for c in (CHOICES_COLUMN, HARD_COLUMN) if c in dataset.column_names
    ]
    mirror = StoredEmbeddings(store.directory)
    if mirror.num_questions != store.num_questions or len(mirror) != len(store):
        raise ValueError("mirror store disagrees with upstream EmbeddingStore")
    total = len(dataset)
    rows = (
        list(range(min(args.first, total)))
        if args.first
        else sample_rows(total, args.sample, args.seed)
    )
    texts = parquet_rows(split_files(config, split, args.data_root), rows)
    cases, mismatches, counts = [], [], Counter()
    for row in rows:
        item = dataset[row]
        documents = store.documents(item["collection_id"])
        query = store.question(row)
        if not torch.equal(documents, mirror.documents(item["collection_id"])):
            raise ValueError(f"mirror documents differ at row {row}")
        if not torch.equal(query, mirror.question(row)):
            raise ValueError(f"mirror question differs at row {row}")
        # Pop each row's chunk texts once used: long personalization
        # collections stay resident only until their case is written.
        text = texts.pop(row)
        chunks = text["documents"] or []
        if text["question"] != item["question"]:
            raise ValueError(f"parquet question differs from the dataset at row {row}")
        if len(chunks) != documents.shape[0]:
            mismatches.append(
                {"row": row, "chunks": len(chunks), "vectors": documents.shape[0]}
            )
            continue
        indices, scores = retrieval_order(documents, query, args.top_k)
        kind = question_type(item["question"])
        counts[kind] += 1
        cases.append(
            {
                "name": case_name(args.target, split, row, item["collection_id"]),
                "target": args.target,
                **({} if split == registered else {"split": split}),
                "row": row,
                "collection_id": int(item["collection_id"]),
                "question": item["question"],
                "answer": item["answer"],
                "question_type": kind,
                "documents": int(documents.shape[0]),
                "document_tokens_hint": sum(len(c.split()) for c in chunks),
                "documents_sha256": tensor_digest(documents),
                "question_sha256": tensor_digest(query),
                "top_indices": indices,
                "top_scores": scores,
                "top_documents": [chunks[i] for i in indices],
                **{c: item[c] for c in extra_columns},
            }
        )
        del chunks, text
    if mismatches:
        raise ValueError(f"chunk/vector count mismatches: {mismatches[:5]}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w") as handle:
        for case in cases:
            handle.write(json.dumps(case, ensure_ascii=False) + "\n")
    receipt = {
        "schema": "antfly.memorilla.qwen-hybrid-paper-cases.v1",
        "target": args.target,
        "config": config,
        "split": split,
        "split_rows": total,
        "rows": len(cases),
        "selection": {"first": args.first}
        if args.first
        else {"sample": args.sample, "seed": args.seed},
        "collections": len({c["collection_id"] for c in cases}),
        "top_k": args.top_k,
        "question_types": {k: counts.get(k, 0) for k in QUESTION_TYPES},
        "scoring": scoring,
        "case_columns": extra_columns,
        **(
            {"hard_rows": sum(bool(c[HARD_COLUMN]) for c in cases)}
            if HARD_COLUMN in extra_columns
            else {}
        ),
        "mean_chunks_per_question": mean(c["documents"] for c in cases),
        "alignment": {
            "upstream_rows_equal_question_vectors": True,
            "chunk_count_mismatches": 0,
            "mirror_store_identical": True,
            "parquet_questions_equal": True,
        },
        "embedding_directory": str(store.directory),
        "cases_file": str(args.output),
    }
    save_json(args.output.with_suffix(".prep.json"), receipt)
    print(json.dumps(receipt, indent=1), flush=True)
    return receipt


# ----------------------------------------------------------------- paper-run


def greedy_generation_config(max_new_tokens: int):
    """An explicit greedy config. Qwen3's ``generation_config.json`` samples
    (temperature 0.6, top-k 20, top-p 0.95) and transformers >= 4.50 copies
    model defaults over every field a passed config leaves at the global
    default, ``do_sample=False`` included; ``generate`` must therefore get
    ``use_model_defaults=False`` with this config."""
    from transformers import GenerationConfig

    config = GenerationConfig(
        max_new_tokens=int(max_new_tokens),
        do_sample=False,
        num_beams=1,
        temperature=None,
        top_k=None,
        top_p=None,
        eos_token_id=list(PAPER_EOS_IDS),
        pad_token_id=PAPER_PAD_ID,
    )
    if config.do_sample or config.num_beams != 1:
        raise ValueError("generation config is not greedy")
    return config


def generate_greedy_batch(decoder, prompts: list[torch.Tensor], max_new_tokens: int):
    """Greedy generation over left-padded prompt embeddings (BF16 decoder, no
    autocast) -> ([(ids, reason)], seconds)."""
    embeds, mask = left_pad(prompts)
    config = greedy_generation_config(max_new_tokens)
    synchronize = torch.cuda.synchronize if embeds.is_cuda else (lambda: None)
    synchronize()
    started = time.perf_counter()
    with torch.inference_mode():
        output = decoder.generate(
            inputs_embeds=embeds,
            attention_mask=mask,
            generation_config=config,
            use_model_defaults=False,
        )
    synchronize()
    seconds = time.perf_counter() - started
    if output.shape[1] > max_new_tokens:
        raise ValueError("generate returned prompt positions; expected new tokens")
    return [trim_generation(row.tolist()) for row in output], seconds


class PaperRun:
    """Model stack + stored vectors + per-case caches for ``paper-run``."""

    def __init__(self, args, models, protocol, released, store, cases):
        self.args, self.models, self.protocol = args, models, protocol
        self.released, self.store, self.cases = released, store, cases
        self.embedding = models["decoder"].get_input_embeddings()
        self.dec_tok = models["dec_tok"]
        self._documents = (None, None)
        self.records = {}
        self.record_path = args.output / "records.jsonl"
        for row in read_rows(self.record_path):
            self.records[row["name"]] = row

    def documents(self, case) -> torch.Tensor:
        """Stored FP16 chunk vectors of the case's collection, digest-checked."""
        key, cached = self._documents
        if key == case["collection_id"]:
            return cached
        documents = self.store.documents(case["collection_id"])
        if tensor_digest(documents) != case["documents_sha256"]:
            raise ValueError(f"stored chunk vectors changed for {case['name']}")
        self._documents = (case["collection_id"], documents)
        return documents

    def question_vector(self, case) -> torch.Tensor:
        query = self.store.question(case["row"])
        if tensor_digest(query) != case["question_sha256"]:
            raise ValueError(f"stored question vector changed for {case['name']}")
        return query

    def soft_rows(self, case, noise: bool) -> torch.Tensor:
        """The released reader over every stored chunk vector of the
        collection (FP16 -> BF16, as upstream ``MemoryCollator``)."""
        documents = self.documents(case)
        if noise:
            documents = noise_documents(
                documents, noise_seed(self.args.noise_seed, case["row"])
            )
        docs = documents.to("cuda").bfloat16()[None]
        query = self.question_vector(case).to("cuda").bfloat16()[None]
        mask = torch.zeros((1, docs.shape[1]), dtype=torch.bool, device="cuda")
        with torch.inference_mode():
            memory = self.released(docs, mask, query)[0]
        if tuple(memory.shape) != (RELEASED_MEMORY_SLOTS, DECODER_WIDTH):
            raise ValueError("released reader changed memory geometry")
        return memory

    def record(self, case) -> dict:
        """copy17's snapped record of the top-2 chunks (cached per case and
        per input digest in ``records.jsonl``)."""
        documents = case["top_documents"][: self.args.record_top_k]
        read_tokens = getattr(self.args, "record_read_tokens", 0)
        window_policy = getattr(self.args, "record_window_policy", None)
        if read_tokens:
            # Budget study B: top-ranked chunks until the token budget.
            from qwen_span_copy import read_budget_documents

            tokenizer = self.models["enc_tok"]
            documents = read_budget_documents(
                lambda text: len(tokenizer(text, add_special_tokens=False)["input_ids"]),
                case["top_documents"],
                read_tokens,
            )
        extractor = getattr(self.args, "record_extractor", "candidate")
        inputs = [documents, case["question"]]
        if extractor != "candidate":
            inputs += [extractor, self.args.record_stride]
        if read_tokens or window_policy:
            inputs += ["budget-study-b", read_tokens, window_policy]
        key = hashlib.sha256(json.dumps(inputs, ensure_ascii=False).encode()).hexdigest()
        cached = self.records.get(case["name"])
        if cached is not None and cached.get("inputs_sha256") == key:
            if self.gate_mode != "cosine" and not cached.get("error"):
                cached["gate_open"] = self.gate_open(cached)
            return cached
        started = time.perf_counter()
        try:
            if extractor == "candidate":
                _, info = record_memory(
                    self.models,
                    self.args.record_protocol,
                    {"documents": documents, "question": case["question"]},
                    suffix=self.args.encoder_suffix,
                    pad_id=self.args.record_pad_id,
                    max_length=self.args.encoder_max_length,
                    window_policy=window_policy,
                )
            else:
                from qwen_span_copy import DEFAULT_WINDOW, cheap_record_memory

                _, info = cheap_record_memory(
                    self.models,
                    {"documents": documents, "question": case["question"]},
                    extractor=extractor,
                    query=self.question_vector(case),
                    width=int(
                        self.args.record_protocol.get("span_window_tokens", DEFAULT_WINDOW)
                    ),
                    stride=self.args.record_stride,
                    pad_id=self.args.record_pad_id,
                )
            info.pop("copy_ids", None)
            record = {"name": case["name"], "inputs_sha256": key, **info}
            record["gate_open"] = self.gate_open(record)
        except ValueError as error:
            record = {
                "name": case["name"],
                "inputs_sha256": key,
                "error": str(error),
                "gate_open": False,
            }
        record["seconds"] = round(time.perf_counter() - started, 4)
        self.records[case["name"]] = record
        append_row(self.record_path, record)
        return record

    @property
    def gate_mode(self) -> str:
        return getattr(self.args, "record_gate_mode", "cosine")

    def gate_open(self, record: dict) -> bool:
        """The record gate of one computed record under ``--record-gate-mode``."""
        return record_gate_decision(
            self.gate_mode,
            [tuple(s) for s in record["spans"]],
            record["gate"],
            record["snap_top1_cosines"],
            self.args.record_gate_cosine,
        )

    def prompt(self, case, spec) -> tuple[torch.Tensor, dict]:
        """Prompt embeddings [L, 4096] BF16 of one case under one arm."""
        if spec["kind"] == "text":
            documents = case["top_documents"][: spec["top_k"]]
            # Re-chunked cases (``chunk_count``) retrieve over their own chunks.
            available = case.get("chunk_count", case["documents"])
            if spec["top_k"] and len(documents) < min(spec["top_k"], available):
                raise ValueError("case carries fewer chunk texts than the arm needs")
            messages = paper_text_messages(self.protocol, documents, case["question"])
            rendered = self.dec_tok.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True,
                enable_thinking=False,
            )
            ids = self.dec_tok.encode(rendered, add_special_tokens=False)
            budget = max(
                RETRIEVAL_CONTEXT_LENGTH
                - self.protocol["baseline"]["max_new_tokens"]
                - GENERATION_RESERVE,
                1,
            )
            truncated = len(ids) > budget
            ids = torch.tensor(ids[-budget:], device="cuda")
            with torch.inference_mode():
                embeds = self.embedding(ids)
            return embeds, {
                "prompt_tokens": int(ids.numel()),
                "prompt_truncated": truncated,
                "chunks_in_prompt": len(documents),
            }
        extra = {}
        soft = self.soft_rows(case, spec["noise"])
        memories, gate = soft, None
        if spec["record"]:
            record = self.record(case)
            gate = bool(record["gate_open"])
            extra = {
                "record_gate": gate,
                "record_error": record.get("error"),
                "record_text": record.get("record_text"),
                "snap_min_top1_cosine": record.get("snap_min_top1_cosine"),
                "spans": record.get("spans"),
            }
            if gate:
                with torch.inference_mode():
                    rows = self.embedding.weight[
                        torch.tensor(record["record_ids"], device="cuda")
                    ].detach()
                memories = compose_memory_rows(soft, rows, "soft-first")
        system = paper_memory_system(self.protocol, spec, gate)
        with torch.inference_mode():
            _, embeds, _ = prepare_prompt(
                self.dec_tok,
                self.embedding,
                memories,
                system_prompt=system,
                question=memory_user_turn(case["question"], self.protocol["suffix"]),
                device="cuda",
            )
        embeds, untruncated = truncate_prompt_right(
            embeds, self.protocol.get("max_length")
        )
        return embeds, {
            "prompt_tokens": int(embeds.shape[0]),
            "memory_rows": int(memories.shape[0]),
            "system_prompt": system,
            **({"prompt_truncated_from": untruncated} if untruncated else {}),
            **extra,
        }

    def decode(self, ids) -> str:
        return self.dec_tok.decode(ids, skip_special_tokens=True).strip()

    def finish_row(self, case, spec, ids, reason, meta, limit, truncations) -> dict:
        predictions = {"full": self.decode(ids)}
        for point in truncations:
            predictions[str(point)] = self.decode(trim_generation(ids[:point])[0])
        raw = dict(predictions)
        canonicalized = False
        if spec["kind"] == "memory" and spec["record"] and meta.get("record_gate"):
            for key, text in raw.items():
                predictions[key] = canonicalize_answer(text, meta["record_text"])
                canonicalized |= predictions[key] != text
        row = {
            "name": case["name"],
            "target": case["target"],
            "row": case["row"],
            "arm": spec["arm"],
            "question": case["question"],
            "answer": case["answer"],
            "question_type": case["question_type"],
            "predictions": predictions,
            "generated_tokens": len(ids),
            "finish_reason": reason,
            "max_new_tokens": limit,
            "token_ids": ids,
            **{c: case[c] for c in (CHOICES_COLUMN, HARD_COLUMN) if c in case},
            **meta,
        }
        if canonicalized:
            row["predictions_raw"] = raw
        row["canonicalized"] = canonicalized
        return row

    def run_arm(self, arm: str) -> dict:
        spec = paper_arm_spec(arm)
        if spec["kind"] == "text" and self.protocol["baseline"] is None:
            raise ValueError(f"{arm} needs a baseline protocol")
        path = self.args.output / f"{arm}.jsonl"
        done = existing_names(path)
        pending = [c for c in self.cases if c["name"] not in done]
        if spec["kind"] == "text":
            # Similar lengths share a batch (less padding).
            pending.sort(
                key=lambda c: sum(len(d) for d in c["top_documents"][: spec["top_k"]])
            )
        limit = paper_generation_limit(self.protocol, spec, self.args.max_new_tokens)
        truncations = paper_truncations(
            self.protocol, spec, limit, self.args.truncations
        )
        started, generated, batches = time.perf_counter(), 0, 0
        batch_size = self.args.batch_size
        if spec["kind"] == "text" and spec["top_k"] >= 20:
            batch_size = max(1, min(batch_size, self.args.long_batch_size))
        for start in range(0, len(pending), batch_size):
            chunk = pending[start : start + batch_size]
            prepared_at = time.perf_counter()
            prompts, metas, ready = [], [], []
            for case in chunk:
                embeds, meta = self.prompt(case, spec)
                prompts.append(embeds)
                metas.append(meta)
                ready.append(case)
            prepare_seconds = time.perf_counter() - prepared_at
            outputs, seconds = generate_greedy_batch(
                self.models["decoder"], prompts, limit
            )
            batches += 1
            for case, meta, (ids, reason) in zip(ready, metas, outputs, strict=True):
                row = self.finish_row(case, spec, ids, reason, meta, limit, truncations)
                row["batch_size"] = len(ready)
                row["batch_generation_seconds"] = round(seconds, 4)
                row["batch_prepare_seconds"] = round(prepare_seconds, 4)
                append_row(path, row)
                generated += 1
            print(
                json.dumps(
                    {
                        "arm": arm,
                        "done": len(done) + generated,
                        "of": len(self.cases),
                        "batch_seconds": round(seconds, 2),
                        "prepare_seconds": round(prepare_seconds, 2),
                        "sample": {
                            "q": ready[0]["question"][:80],
                            "gold": str(ready[0]["answer"])[:60],
                            "pred": self.decode(outputs[0][0])[:100],
                        },
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
        return {
            "arm": arm,
            "rows_generated": generated,
            "rows_total": len(read_rows(path)),
            "batches": batches,
            "max_new_tokens": limit,
            "truncations": truncations,
            "elapsed_seconds": round(time.perf_counter() - started, 1),
        }

    def verify_stored_vectors(self, soft: SoftMemory, count: int) -> list[dict]:
        """Re-embed the question and (when the case carries chunk texts) the
        top-1 chunk of the first ``count`` cases with the frozen encoder
        (adapters off, last-token pooling) and compare to the stored vectors:
        confirms row alignment and pooling."""

        def cosine(left, right) -> float:
            value = torch.nn.functional.cosine_similarity(left, right, dim=0)
            return round(float(value), 5)

        checks = []
        for case in self.cases[:count]:
            stored = self.question_vector(case).float().cuda()
            check = {
                "name": case["name"],
                "question_cosine": cosine(
                    soft.pooled(case["question"].strip()).float(), stored
                ),
            }
            if case["top_indices"]:
                stored = self.documents(case)[case["top_indices"][0]].float().cuda()
                ours = soft.pooled(case["top_documents"][0]).float()
                check["top1_chunk_cosine"] = cosine(ours, stored)
            checks.append(check)
        soft.cache.clear()
        return checks


def paper_run(args) -> dict:
    protocol = paper_protocol(args.target)
    cases = read_rows(args.cases)
    if not cases or any(c["target"] != args.target for c in cases):
        raise ValueError("cases file does not hold this target")
    config, split = PAPER_TARGETS[args.target][0], cases_split(args.target, cases)
    if args.limit:
        cases = cases[: args.limit]
    cases.sort(key=lambda c: (c["collection_id"], c["row"]))
    store = StoredEmbeddings(embedding_directory(args.data_root, config, split))
    args.output.mkdir(parents=True, exist_ok=True)
    paths = {
        key: Path(getattr(args, key) or (args.assets / relative))
        for key, relative in DEFAULT_ASSETS.items()
    }
    candidate, record_protocol = load_candidate(args.candidate)
    if getattr(args, "record_read_tokens", 0) < 0:
        raise ValueError("--record-read-tokens must be >= 0")
    if getattr(args, "record_window_policy", None) and (
        record_protocol.get("record_kind") != "span-window"
    ):
        raise ValueError("--record-window-policy needs a span-window candidate")
    checkpoint = paper_released_checkpoint(
        args.target, args.assets, paths["checkpoint"], args.released_checkpoint
    )
    checkpoint_sha256 = file_sha256(checkpoint / "memory.pt")
    cotenant = wait_for_gpu(args.cotenant_limit_mib)
    loading = time.perf_counter()
    models = load_models(args.candidate, record_protocol, paths)
    attach_span_head(models, args.candidate, record_protocol)
    released = load_released_reader(paths, models["reader_config"], checkpoint)
    load_seconds = round(time.perf_counter() - loading, 1)
    args.record_protocol = record_protocol
    args.encoder_suffix = encoder_special_suffix(models["enc_tok"])
    pad = models["dec_tok"].encode(" ", add_special_tokens=False)
    if len(pad) != 1:
        raise ValueError("record pad must be a single token")
    args.record_pad_id = pad[0]
    runner = PaperRun(args, models, protocol, released, store, cases)
    if any(paper_arm_spec(a).get("record") for a in args.arms):
        assert_shared_token_ids(
            models["enc_tok"],
            models["dec_tok"],
            prefixed_documents(cases[0]["top_documents"][: args.record_top_k], True),
            True,
        )
    result = {
        "schema": "antfly.memorilla.qwen-hybrid-paper-run.v1",
        "target": args.target,
        "cases_file": str(args.cases),
        "cases": len(cases),
        "arms": args.arms,
        "protocol": {k: v for k, v in protocol.items()},
        "released_checkpoint": str(checkpoint),
        "released_checkpoint_memory_sha256": checkpoint_sha256,
        "released_checkpoint_source": (
            "override"
            if args.released_checkpoint
            else "target"
            if args.target in PAPER_TARGET_CHECKPOINTS
            else "default"
        ),
        "candidate": str(args.candidate.resolve()),
        "candidate_sha256": candidate.get("protocol_sha256"),
        "record_top_k": args.record_top_k,
        **(
            {"record_extractor": args.record_extractor, "record_stride": args.record_stride}
            if args.record_extractor != "candidate"
            else {}
        ),
        **(
            {
                "record_read_tokens": args.record_read_tokens,
                "record_window_policy": args.record_window_policy,
            }
            if args.record_read_tokens or args.record_window_policy
            else {}
        ),
        "record_gate_cosine": args.record_gate_cosine,
        **(
            {"record_gate_mode": args.record_gate_mode}
            if args.record_gate_mode != "cosine"
            else {}
        ),
        "encoder_max_length": args.encoder_max_length,
        "noise_seed": args.noise_seed,
        "batch_size": args.batch_size,
        "gpu_cotenant_mib_at_start": cotenant,
        "model_load_seconds": load_seconds,
        "torch_version": torch.__version__,
        "stored_vector_checks": runner.verify_stored_vectors(
            SoftMemory(models, released), args.verify_cases
        ),
        "arm_runs": {},
    }
    stamp = time.strftime("%Y%m%dT%H%M%S")
    receipt = args.output / f"run-{stamp}.json"
    save_json(receipt, result)
    print(
        json.dumps({"stored_vector_checks": result["stored_vector_checks"]}), flush=True
    )
    for arm in args.arms:
        result["arm_runs"][arm] = runner.run_arm(arm)
        save_json(receipt, result)
    result["completed"] = True
    result["cuda_peak_allocated_bytes"] = torch.cuda.max_memory_allocated()
    save_json(receipt, result)
    print(json.dumps({"completed": True, "arm_runs": result["arm_runs"]}), flush=True)
    return result


# -------------------------------------------------------- paper-choice-embed
#
# Upstream scores pmv2/pv4/factkg by embedding each generation and every
# non-empty choice with Qwen3-Embedding-4B (vLLM ``task="embed"``: BF16,
# tokenizer special tokens on, last-token pooling, L2 normalization, FP16
# arrays) and picking the most cosine-similar choice. ``paper-choice-embed``
# (GPU, CUDA venv) computes the same vectors with transformers through the
# path that reproduces the stored question vectors (``SoftMemory.pooled``)
# and stores them per distinct text; ``paper-score`` (upstream venv) then runs
# upstream ``choice_predictions`` / ``choice_index`` on them.


def paper_arm_files(output: Path) -> list[str]:
    """Arm names of a paper-run directory (every ``<arm>.jsonl`` except the
    record cache)."""
    return [
        p.stem
        for p in sorted(Path(output).glob("*.jsonl"))
        if p.stem != "records" and not p.stem.startswith("run-")
    ]


def load_choice_table(output: Path):
    """``(sha256 -> row, FP16 numpy vectors [U, D], metadata)`` of a
    ``paper-choice-embed`` table, or ``({}, None, {})`` before the first run."""
    from safetensors.numpy import load_file

    meta_path = Path(output) / f"{CHOICE_TABLE}.json"
    if not meta_path.exists():
        return {}, None, {}
    meta = json.loads(meta_path.read_text())
    vectors = load_file(str(Path(output) / f"{CHOICE_TABLE}.safetensors"))["vectors"]
    keys = meta["text_sha256"]
    if len(keys) != vectors.shape[0] or len(set(keys)) != len(keys):
        raise ValueError("choice table index and vectors disagree")
    if meta.get("vectors_sha256") != hashlib.sha256(vectors.tobytes()).hexdigest():
        raise ValueError("choice table vectors changed since they were written")
    return {k: i for i, k in enumerate(keys)}, vectors, meta


def save_choice_table(output: Path, keys: list[str], vectors, meta: dict) -> None:
    """Write the table atomically (vectors first, index last)."""
    import os

    from safetensors.numpy import save_file

    if len(keys) != vectors.shape[0]:
        raise ValueError("choice table index and vectors disagree")
    output = Path(output)
    tensor_path = output / f"{CHOICE_TABLE}.safetensors"
    save_file({"vectors": vectors}, str(tensor_path) + ".partial")
    os.replace(str(tensor_path) + ".partial", tensor_path)
    meta = {
        **meta,
        "text_sha256": list(keys),
        "texts": len(keys),
        "vectors_sha256": hashlib.sha256(vectors.tobytes()).hexdigest(),
    }
    save_json(output / f"{CHOICE_TABLE}.json.partial", meta)
    os.replace(output / f"{CHOICE_TABLE}.json.partial", output / f"{CHOICE_TABLE}.json")


def embed_pooled_texts(
    encoder, tokenizer, texts, batch_size: int, max_tokens: int, device="cuda"
):
    """Last-token pooled, L2-normalized BF16 vectors -> FP16 CPU ``[N, D]``
    (texts batched by length with right padding; the attention mask keeps
    each text's last real token)."""
    if not texts:
        return torch.zeros((0, 0), dtype=torch.float16)
    lengths = [len(tokenizer(t)["input_ids"]) for t in texts]
    if max(lengths) > max_tokens:
        raise ValueError(f"choice-scorer text of {max(lengths)} tokens > {max_tokens}")
    order = sorted(range(len(texts)), key=lambda i: lengths[i])
    out = [None] * len(texts)
    for start in range(0, len(order), batch_size):
        picks = order[start : start + batch_size]
        tokens = tokenizer(
            [texts[i] for i in picks], padding=True, return_tensors="pt"
        ).to(device)
        with torch.inference_mode():
            hidden = encoder(**tokens, use_cache=False).last_hidden_state
            vectors = normalize_features(hidden, tokens["attention_mask"]).half().cpu()
        for row, index in enumerate(picks):
            out[index] = vectors[row]
    return torch.stack(out)


def load_choice_encoder(path: Path):
    from transformers import AutoModel, AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(path, local_files_only=True)
    tokenizer.padding_side = "right"
    encoder = (
        AutoModel.from_pretrained(
            path,
            local_files_only=True,
            torch_dtype=torch.bfloat16,
            attn_implementation="sdpa",
        )
        .requires_grad_(False)
        .cuda()
        .eval()
    )
    return encoder, tokenizer


def paper_choice_embed(args) -> dict:
    import numpy as np

    protocol = paper_protocol(args.target)
    if protocol["scoring"] != CHOICE_SCORING:
        raise ValueError(f"{args.target} is not choice-scored")
    arms = args.arms or paper_arm_files(args.output)
    rows = [r for arm in arms for r in latest_rows(args.output / f"{arm}.jsonl")]
    if not rows:
        raise ValueError(f"no paper-run rows under {args.output}")
    config, split = PAPER_TARGETS[args.target][0], records_split(args.target, rows)
    texts = choice_embedding_texts(rows)
    index, vectors, meta = load_choice_table(args.output)
    keys = [None] * len(index)
    for key, position in index.items():
        keys[position] = key
    missing = [t for t in texts if text_key(t) not in index]
    encoder_path = Path(args.encoder or (args.assets / DEFAULT_ASSETS["encoder"]))
    cotenant = wait_for_gpu(args.cotenant_limit_mib)
    started = time.perf_counter()
    encoder, tokenizer = load_choice_encoder(encoder_path)
    # Alignment: the question texts of the first rows re-embedded through the
    # same path must reproduce the stored (upstream vLLM) question vectors,
    # and batching must not move a vector.
    store = StoredEmbeddings(embedding_directory(args.data_root, config, split))
    probes = sorted({(r["row"], str(r["question"]).strip()) for r in rows})
    probes = probes[: args.verify_cases]
    batched = embed_pooled_texts(
        encoder, tokenizer, [q for _, q in probes], args.batch_size, args.max_tokens
    )
    verification = []
    for (row, question), vector in zip(probes, batched, strict=True):
        alone = embed_pooled_texts(encoder, tokenizer, [question], 1, args.max_tokens)
        stored = store.question(row).float()
        verification.append(
            {
                "row": row,
                "stored_question_cosine": round(
                    float(
                        torch.nn.functional.cosine_similarity(
                            vector.float(), stored, dim=0
                        )
                    ),
                    5,
                ),
                "batched_vs_single_cosine": round(
                    float(
                        torch.nn.functional.cosine_similarity(
                            vector.float(), alone[0].float(), dim=0
                        )
                    ),
                    5,
                ),
            }
        )
    if any(v["stored_question_cosine"] < args.min_verify_cosine for v in verification):
        raise ValueError(f"scorer does not reproduce stored vectors: {verification}")
    new = embed_pooled_texts(
        encoder, tokenizer, missing, args.batch_size, args.max_tokens
    )
    if missing:
        added = new.numpy()
        vectors = added if vectors is None else np.concatenate([vectors, added])
        keys += [text_key(t) for t in missing]
    meta = {
        **meta,
        "schema": "antfly.memorilla.qwen-hybrid-choice-embeddings.v1",
        "target": args.target,
        "encoder": str(encoder_path),
        "pooling": "last-token, L2-normalized in BF16, FP16 storage",
        "tokenizer_special_tokens": True,
        "batch_size": args.batch_size,
        "verification": verification,
        "arms": sorted(set(meta.get("arms", [])) | set(arms)),
        "updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "gpu_cotenant_mib_at_start": cotenant,
    }
    if vectors is not None:
        save_choice_table(args.output, keys, vectors, meta)
    result = {
        "target": args.target,
        "arms": arms,
        "rows": len(rows),
        "distinct_texts": len(texts),
        "embedded_now": len(missing),
        "table_texts": len(keys),
        "verification": verification,
        "seconds": round(time.perf_counter() - started, 1),
    }
    print(json.dumps(result, indent=1), flush=True)
    return result


# --------------------------------------------------------------- paper-score


def latest_rows(path: Path) -> list[dict]:
    """JSONL rows with one row per name (the last written wins)."""
    rows = {}
    for row in read_rows(path):
        rows[row["name"]] = row
    return list(rows.values())


def choice_row_correct(rows: list[dict], key: str, table, metrics_module) -> list[bool]:
    """Upstream ``choice_accuracy`` per row from the ``paper-choice-embed``
    vectors: each cleaned prediction (stripped) and every non-empty choice
    looked up by text, upstream ``choice_predictions`` picks the most
    cosine-similar choice (FP16 arrays, as upstream's ``embed_texts``
    returns), correct when it is upstream ``choice_index`` of the reference."""
    import numpy as np

    m = metrics_module
    index, vectors = table

    def vector(text: str):
        position = index.get(text_key(text))
        if position is None:
            raise ValueError(
                f"no choice-scorer vector for {text[:60]!r}; run paper-choice-embed"
            )
        return vectors[position]

    if not rows:
        return []
    predictions, candidates, choices = [], [], []
    for row in rows:
        if not row.get(CHOICES_COLUMN):
            raise ValueError(f"{row['name']} carries no choices")
        predictions.append(vector(m.clean_generation(row["predictions"][key]).strip()))
        candidates.append(
            np.stack([vector(c) for c in row[CHOICES_COLUMN] if c.strip()])
        )
        choices.append(row[CHOICES_COLUMN])
    selected = m.choice_predictions(np.stack(predictions), candidates, choices)
    return [
        m.choice_index(row[CHOICES_COLUMN], str(row["answer"]).strip()) == picked
        for row, picked in zip(rows, selected, strict=True)
    ]


def score_arm(
    protocol: dict, rows: list[dict], key: str, metrics_module, choice_table=None
) -> dict:
    """Upstream scoring of one arm at one prediction key ('full' or a prefix
    budget) overall and, for generation targets, by question type. Choice
    targets need the ``paper-choice-embed`` table (``choice_table``)."""
    m = metrics_module
    usable = [r for r in rows if key in r["predictions"]]
    predictions = [m.clean_generation(r["predictions"][key]) for r in usable]
    references = [str(r["answer"]).strip() for r in usable]
    if protocol["scoring"] == CHOICE_SCORING:
        if choice_table is None:
            raise ValueError("choice scoring needs the paper-choice-embed table")
        scores = choice_accuracy_from_correct(
            choice_row_correct(usable, key, choice_table, m),
            [r.get(HARD_COLUMN) for r in usable],
            "accuracy_hard" in protocol["metrics"],
        )
        scores = {name: scores[name] for name in protocol["metrics"]}
    else:
        scores = m.compute_metrics(
            protocol["scoring"], protocol["metrics"], predictions, references
        )
    result = {
        "rows": len(usable),
        "metrics": {k: round(100 * v, 2) for k, v in scores.items()},
        "unknown_rate": round(100 * mean(is_unknown(p) for p in predictions), 2),
        "abstain_rate": round(100 * mean(is_abstain(p) for p in predictions), 2),
        "mean_generated_tokens": round(mean(r["generated_tokens"] for r in usable), 2),
        "length_stops": sum(r["finish_reason"] == "length" for r in usable),
    }
    if protocol["target"] == "narrativeqa":
        by_type = {}
        for kind, bucket in bucket_rows(
            [dict(r, _p=p) for r, p in zip(usable, predictions, strict=True)]
        ).items():
            if not bucket:
                by_type[kind] = {"rows": 0}
                continue
            values = m.generation_metrics(
                [b["_p"] for b in bucket],
                [str(b["answer"]).strip() for b in bucket],
                ("rouge1", "rougeL", "f1"),
            )
            by_type[kind] = {
                "rows": len(bucket),
                **{k: round(100 * v, 2) for k, v in values.items()},
            }
        result["by_question_type"] = by_type
    return result


def per_row_scores(
    protocol: dict, rows: list[dict], key: str, metrics_module, choice_table=None
) -> dict:
    """name -> primary per-row score: EM (TriviaQA), choice correctness
    (pmv2, pv4, factkg), polarity match (PubMedQA; rows whose reference has no
    leading yes/no/maybe are skipped, as upstream ``polarity_accuracy`` does)
    or ROUGE-L (generation targets)."""
    m = metrics_module
    usable = [r for r in rows if key in r["predictions"]]
    if protocol["scoring"] == CHOICE_SCORING:
        correct = choice_row_correct(usable, key, choice_table, m)
        return {r["name"]: float(c) for r, c in zip(usable, correct, strict=True)}
    scores = {}
    for row in usable:
        prediction = m.clean_generation(row["predictions"][key])
        reference = str(row["answer"]).strip()
        if protocol["scoring"] == "exact_match":
            value = float(
                m.normalize_answer(prediction) == m.normalize_answer(reference)
            )
        elif protocol["scoring"] == POLARITY_SCORING:
            gold = m.first_polarity(reference)
            if gold is None:
                continue
            value = float(m.first_polarity(prediction) == gold)
        else:
            value = m.rouge([prediction], [reference])["rougeL"]
        scores[row["name"]] = value
    return scores


def paired_summary(left: dict, right: dict, threshold: float) -> dict:
    """Per-question comparison of two arms on their common rows: wins, losses,
    and the paper's Figure 5 union (answered by either; answered = score >=
    ``threshold``)."""
    common = sorted(set(left) & set(right))
    if not common:
        return {"rows": 0}
    wins = sum(right[n] > left[n] for n in common)
    losses = sum(right[n] < left[n] for n in common)
    answered_left = {n for n in common if left[n] >= threshold}
    answered_right = {n for n in common if right[n] >= threshold}
    return {
        "rows": len(common),
        "right_better": wins,
        "left_better": losses,
        "ties": len(common) - wins - losses,
        "mean_delta": round(100 * mean(right[n] - left[n] for n in common), 2),
        "answered_threshold": threshold,
        "answered_left": len(answered_left),
        "answered_right": len(answered_right),
        "answered_union": len(answered_left | answered_right),
        "answered_left_only": len(answered_left - answered_right),
        "answered_right_only": len(answered_right - answered_left),
    }


def paper_score(args) -> dict:
    from memorilla import metrics as upstream_metrics

    protocol = paper_protocol(args.target)
    arms = args.arms or paper_arm_files(args.output)
    result = {
        "schema": "antfly.memorilla.qwen-hybrid-paper-scores.v1",
        "target": args.target,
        "protocol": protocol,
        "arms": {},
        "paired": {},
    }
    choice_table = None
    if protocol["scoring"] == CHOICE_SCORING:
        index, vectors, meta = load_choice_table(args.output)
        if vectors is None:
            raise ValueError("choice scoring needs paper-choice-embed output")
        choice_table = (index, vectors)
        result["choice_scorer"] = {
            k: meta.get(k)
            for k in ("encoder", "pooling", "texts", "verification", "updated")
        }
    per_row = {}
    for arm in arms:
        rows = latest_rows(args.output / f"{arm}.jsonl")
        if not rows:
            continue
        keys = sorted(
            {k for r in rows for k in r["predictions"]}, key=lambda k: (k != "full", k)
        )
        entry = {
            "rows": len(rows),
            "by_budget": {
                key: score_arm(protocol, rows, key, upstream_metrics, choice_table)
                for key in keys
            },
            "prompt_truncated": sum("prompt_truncated_from" in r for r in rows),
            "canonicalized": sum(bool(r.get("canonicalized")) for r in rows),
            "generation_seconds_per_row": round(
                mean(r["batch_generation_seconds"] / r["batch_size"] for r in rows), 4
            ),
            "prepare_seconds_per_row": round(
                mean(r["batch_prepare_seconds"] / r["batch_size"] for r in rows), 4
            ),
            "mean_prompt_tokens": round(mean(r["prompt_tokens"] for r in rows), 1),
        }
        gated = [r for r in rows if r.get("record_gate") is not None]
        if gated:
            entry["record_gate_open"] = sum(bool(r["record_gate"]) for r in gated)
            entry["record_gate_total"] = len(gated)
            entry["record_errors"] = sum(bool(r.get("record_error")) for r in gated)
        if args.enwiki_lcs and protocol["target"] == "enwiki":
            for key in keys:
                lcs = [rouge_l(r["predictions"][key], str(r["answer"])) for r in rows]
                entry["by_budget"][key]["lcs_rouge_l_f1"] = round(
                    mean(x["f1"] for x in lcs), 4
                )
        result["arms"][arm] = entry
        primary = args.primary_budget if args.primary_budget in keys else "full"
        per_row[arm] = (
            primary,
            per_row_scores(protocol, rows, primary, upstream_metrics, choice_table),
        )
    threshold = args.answered_threshold
    for left, right in args.pairs:
        if left in per_row and right in per_row:
            result["paired"][f"{left}->{right}"] = {
                "budgets": [per_row[left][0], per_row[right][0]],
                **paired_summary(per_row[left][1], per_row[right][1], threshold),
                "bootstrap": paired_bootstrap_ci(
                    per_row[left][1],
                    per_row[right][1],
                    args.bootstrap_resamples,
                    args.bootstrap_seed,
                ),
            }
    save_json(args.output / "scores.json", result)
    # Primary per-row scores of every arm, for seed aggregates
    # (``qwen_span_copy.py seeds``).
    save_json(
        args.output / "per-row.json",
        {arm: {"budget": budget, "scores": scores} for arm, (budget, scores) in per_row.items()},
    )
    print(json.dumps(result, indent=1, ensure_ascii=False), flush=True)
    return result


def parse_pairs(text: str) -> list[tuple[str, str]]:
    pairs = []
    for item in [t.strip() for t in text.split(",") if t.strip()]:
        left, sep, right = item.partition(":")
        if not sep or not left or not right:
            raise ValueError("pairs are left:right, comma separated")
        pairs.append((left, right))
    return pairs


def parse_paper_args(argv):
    command, rest = argv[0], argv[1:]
    p = argparse.ArgumentParser(prog=f"qwen_hybrid_eval.py {command}")
    p.add_argument("--target", choices=sorted(PAPER_TARGETS), required=True)
    p.add_argument(
        "--data-root",
        type=Path,
        required=True,
        help="Memorilla-Data root (data/, embeddings/)",
    )
    if command == "paper-prep":
        p.add_argument("--output", type=Path, required=True, help="cases JSONL")
        p.add_argument("--sample", type=int, default=0, help="rows drawn with --seed")
        p.add_argument("--first", type=int, default=0, help="first N rows instead")
        p.add_argument("--seed", type=int, default=23)
        p.add_argument("--top-k", type=int, default=50, help="chunk texts kept")
        p.add_argument("--datasets-cache", type=Path, required=True)
        p.add_argument(
            "--split",
            choices=("train", "validation", "test"),
            default="",
            help="default: the target's registered split (fresh-row checks use another)",
        )
    elif command == "paper-run":
        p.add_argument("--assets", type=Path, required=True)
        for key in DEFAULT_ASSETS:
            p.add_argument("--" + key.replace("_", "-"), type=Path)
        p.add_argument("--candidate", type=Path, required=True)
        p.add_argument("--cases", type=Path, required=True)
        p.add_argument("--output", type=Path, required=True)
        p.add_argument("--arms", required=True, help="comma-separated paper arms")
        p.add_argument("--released-checkpoint", type=Path)
        p.add_argument("--batch-size", type=int, default=16)
        p.add_argument("--long-batch-size", type=int, default=4, help="rag-k, k>=20")
        p.add_argument("--max-new-tokens", type=int, default=0, help="override")
        p.add_argument("--truncations", default="", help="extra prefix budgets")
        p.add_argument("--record-top-k", type=int, default=RECORD_TOP_K)
        p.add_argument(
            "--record-extractor",
            choices=("candidate", "embed-window", "lead"),
            default="candidate",
            help="record from the candidate, or a cheap window (extractor ablation)",
        )
        p.add_argument("--record-stride", type=int, default=8, help="embed-window scan stride")
        p.add_argument(
            "--record-read-tokens",
            type=int,
            default=0,
            help="budget study B: read top chunks until this many tokens (0: --record-top-k)",
        )
        p.add_argument(
            "--record-window-policy",
            choices=("P1", "P2", "P3"),
            default=None,
            help="budget study B window policy (span-window candidates; default: candidate's)",
        )
        p.add_argument("--record-gate-cosine", type=float, default=RECORD_GATE_COSINE)
        p.add_argument(
            "--record-gate-mode",
            choices=RECORD_GATE_MODES,
            default="cosine",
            help="snap-cosine rule or the copy head's learned gate",
        )
        p.add_argument("--encoder-max-length", type=int, default=2048)
        p.add_argument("--noise-seed", type=int, default=23)
        p.add_argument("--verify-cases", type=int, default=3)
        p.add_argument("--limit", type=int, default=0)
        p.add_argument("--cotenant-limit-mib", type=int, default=40960)
    elif command == "paper-choice-embed":
        p.add_argument(
            "--output", type=Path, required=True, help="paper-run output dir"
        )
        p.add_argument("--assets", type=Path, required=True)
        p.add_argument("--encoder", type=Path, help="default: <assets>/models/...")
        p.add_argument("--arms", default="", help="default: every arm file")
        p.add_argument("--batch-size", type=int, default=32)
        p.add_argument("--max-tokens", type=int, default=8192)
        p.add_argument("--verify-cases", type=int, default=4)
        p.add_argument("--min-verify-cosine", type=float, default=0.999)
        p.add_argument("--cotenant-limit-mib", type=int, default=40960)
    else:
        p.add_argument(
            "--output", type=Path, required=True, help="paper-run output dir"
        )
        p.add_argument("--arms", default="", help="default: every arm file")
        p.add_argument("--pairs", default="", help="left:right,... paired comparisons")
        p.add_argument("--primary-budget", default="full")
        p.add_argument("--answered-threshold", type=float, default=0.2)
        p.add_argument("--enwiki-lcs", action="store_true")
        p.add_argument("--bootstrap-resamples", type=int, default=BOOTSTRAP_RESAMPLES)
        p.add_argument("--bootstrap-seed", type=int, default=23)
    args = p.parse_args(rest)
    args.command = command
    if command == "paper-run":
        args.arms = [a.strip() for a in args.arms.split(",") if a.strip()]
        for arm in args.arms:
            paper_arm_spec(arm)
        args.truncations = [int(t) for t in args.truncations.split(",") if t.strip()]
        if not 0 <= args.record_gate_cosine <= 1:
            p.error("record gate cosine must lie in [0, 1]")
        if args.batch_size < 1:
            p.error("batch size must be positive")
        if args.record_stride < 1:
            p.error("record stride must be positive")
    elif command == "paper-score":
        args.arms = [a.strip() for a in args.arms.split(",") if a.strip()]
        args.pairs = parse_pairs(args.pairs)
        if args.bootstrap_resamples < 1:
            p.error("bootstrap resamples must be positive")
    elif command == "paper-choice-embed":
        args.arms = [a.strip() for a in args.arms.split(",") if a.strip()]
        if args.batch_size < 1:
            p.error("batch size must be positive")
    elif args.sample and args.first:
        p.error("--sample and --first are exclusive")
    return args


def paper_main(argv) -> dict:
    args = parse_paper_args(argv)
    if args.command == "paper-prep":
        return paper_prep(args)
    if args.command == "paper-run":
        return paper_run(args)
    if args.command == "paper-choice-embed":
        return paper_choice_embed(args)
    return paper_score(args)


def entry(argv=None):
    import sys

    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in PAPER_COMMANDS:
        return paper_main(argv)
    return main(argv)


if __name__ == "__main__":
    entry()
