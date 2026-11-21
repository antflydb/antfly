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

"""Span copy half of the Qwen hybrid memory (plan section 8.11).

The copy17 reader + copy head could not bind a copied span to the question on
general QA (plan section 8.10): its 16 latent slots read a pooled question
vector and its copy keys are context-free token embeddings. Here the exact
half is extractive instead:

- features: copy17's encoder (+ its frozen rank-8 q/v adapters) over the
  top-2 chunks, each encoded after the question text (``question-passage-v1``)
  and doubled for self-context; per document-token rows of the final and
  layer-24 states, each L2-normalized, concatenated (5,120 wide) and aligned to
  the copy-source ids;
- a small bidirectional span head scores every start and end position plus a
  no-answer (null) position;
- the record is an exact evidence window of the source tokens around the best
  span (left-biased: a fact's binding context, e.g. a depot site, usually
  precedes the value), fed to the frozen decoder as its input-embedding rows;
  no snapping, and the window is a byte range of the caller's text (citable);
- the gate opens when the best span outscores the null position by the
  candidate's threshold.

Commands: ``oracle`` (decoder reads gold-centred windows; no training),
``cache`` (encode examples once into fp16 feature shards), ``train`` (fit and
export the head from the cache). The exported candidate is a copy17-format
directory plus ``span-head.safetensors`` and protocol ``record_kind:
span-window``; ``qwen_hybrid_eval.py`` dispatches its records here. Research
diagnostic only; nothing here qualifies a serving profile.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import shutil
import time
from collections import Counter
from pathlib import Path

import torch
from torch import nn

from qwen_hybrid_eval import (
    QUESTION_PREFIX_TEMPLATES,
    question_prefixed_documents,
    read_rows,
    save_json,
)

RECORD_KIND = "span-window"
FEATURE_WIDTH = 5120
DEFAULT_PREFIX = "question-passage-v1"
DEFAULT_WINDOW = 32
DEFAULT_LEFT_FRACTION = 0.65
DEFAULT_MAX_SPAN = 30
SCHEMA_CACHE = "antfly.memorilla.qwen-span-copy-cache.v1"
SCHEMA_TRAIN = "antfly.memorilla.qwen-span-copy-train.v1"
SCHEMA_ORACLE = "antfly.memorilla.qwen-span-copy-oracle.v1"
QA_SOURCES = (
    "squad_v2",
    "drop",
    "coqa",
    "quail",
    "msmarco",
    "pubmedqa",
    "triviaqa",
    "narrativeqa",
)
DEPOT = "depot"
# Routed gate (plans 8.16/8.17; Zig ``memorilla_span.decideGate``).
GATE_POLICIES = ("always", "learned", "routed")
GATE_ROUTES_FILE = "span-gate-routes.safetensors"
GATE_ROUTE_DIMENSION = 2560
MAX_ROUTE_PROFILES = 64


# ----------------------------------------------------------------------------
# pure helpers (CPU, unit tested)


def doc_bounds(counts) -> list[tuple[int, int]]:
    """Per-document ``[lo, hi)`` position ranges in the concatenated ids."""
    bounds, offset = [], 0
    for count in counts:
        count = int(count)
        if count < 1:
            raise ValueError("every document needs at least one token")
        bounds.append((offset, offset + count))
        offset += count
    return bounds


def bound_of(bounds, position: int) -> tuple[int, int]:
    for lo, hi in bounds:
        if lo <= position < hi:
            return lo, hi
    raise ValueError(f"position {position} outside the documents")


def token_occurrences(ids, span) -> list[int]:
    """Start positions where ``span`` occurs in ``ids`` (overlaps allowed)."""
    ids, span = [int(i) for i in ids], [int(i) for i in span]
    if not span:
        return []
    width = len(span)
    return [s for s in range(len(ids) - width + 1) if ids[s : s + width] == span]


def gold_spans(ids, span_ids, bounds) -> list[tuple[int, int]]:
    """Every occurrence of ``span_ids`` inside one document -> inclusive
    ``(start, end)`` pairs."""
    width = len(span_ids)
    spans = []
    for start in token_occurrences(ids, span_ids):
        lo, hi = bound_of(bounds, start)
        if start + width <= hi:
            spans.append((start, start + width - 1))
    return spans


def answer_spans(tokenizer, ids, answer: str, bounds) -> list[tuple[int, int]]:
    """Depot answers: the value's tokens (leading-space form first) wherever
    they occur inside one document."""
    for variant in (" " + answer.strip(), answer.strip()):
        span = tokenizer(variant, add_special_tokens=False)["input_ids"]
        spans = gold_spans(ids, span, bounds)
        if spans:
            return spans
    return []


def evidence_window(
    ids, start: int, end: int, bounds, width: int, left_fraction: float, continues
) -> tuple[int, int]:
    """``[lo, hi)`` of an exact source window around the inclusive span
    ``(start, end)``: inside the span's document, ``width`` tokens at most,
    about ``left_fraction`` of the free budget before the span (unused room on
    one side goes to the other), then trimmed so the window neither starts nor
    ends inside a word (``continues(id)``: the token continues the word before
    it). A span longer than ``width`` is cut to its first ``width`` tokens."""
    if not 0 <= left_fraction <= 1 or width < 1 or end < start:
        raise ValueError("bad evidence window request")
    lo_doc, hi_doc = bound_of(bounds, start)
    if end >= hi_doc:
        raise ValueError("span crosses a document boundary")
    if end - start + 1 >= width:
        return start, start + width
    free = width - (end - start + 1)
    left = min(int(round(free * left_fraction)), start - lo_doc)
    right = min(free - left, hi_doc - end - 1)
    left = min(free - right, start - lo_doc)
    lo, hi = start - left, end + 1 + right
    while lo < start and continues(int(ids[lo])):
        lo += 1
    while hi > end + 1 and hi < hi_doc and continues(int(ids[hi])):
        hi -= 1
    return lo, hi


def window_ids(ids, lo: int, hi: int, width: int, pad_id: int) -> list[int]:
    """The window's exact ids padded with ``pad_id`` to ``width`` rows."""
    chosen = [int(i) for i in ids[lo:hi]][:width]
    return chosen + [int(pad_id)] * (width - len(chosen))


# Budget study B (budget-study/DESIGN.md), research options only. ``P1`` is the
# exported candidate's single window (its protocol width); ``P2`` one wider
# window; ``P3`` the best span of every document, one window each, in
# descending span score, concatenated until the row budget.
WINDOW_POLICIES = ("P1", "P2", "P3")
WIDE_WINDOW_ROWS = 64
MULTI_WINDOW_WIDTH = 32
MULTI_WINDOW_ROWS = 64


def read_budget_documents(count_tokens, documents, budget_tokens: int) -> list:
    """Read budget ``R<n>``: the documents in retrieval order while their
    token total (``count_tokens(text)``) stays within ``budget_tokens``; the
    first document is always read, and reading stops at the first document
    that would exceed the budget."""
    if budget_tokens < 1:
        raise ValueError("read budget must be positive")
    chosen, total = [], 0
    for document in documents:
        tokens = int(count_tokens(document))
        if chosen and total + tokens > budget_tokens:
            break
        chosen.append(document)
        total += tokens
    if not chosen:
        raise ValueError("no documents to read")
    return chosen


def document_best_spans(start_logits, end_logits, bounds, max_len: int) -> list[tuple]:
    """The best inclusive span inside each document -> ``(score, start, end)``
    per document, in document order."""
    spans = []
    for bound in bounds:
        s, e, score, _ = best_span(start_logits, end_logits, [bound], max_len)
        spans.append((score, s, e))
    return spans


def multi_windows(
    ids, bounds, document_spans, *, width: int, rows: int, left_fraction: float, continues
) -> tuple[list[int], list[tuple[int, int]]]:
    """Policy ``P3``: one evidence window (at most ``width`` tokens, inside its
    document) around each document's best span, documents in descending span
    score (ties: document order), windows concatenated without separators
    until the next one would pass ``rows``. Returns the concatenated ids (at
    most ``rows``) and the placed ``[lo, hi)`` windows in order."""
    if width < 1 or rows < 1:
        raise ValueError("bad multi-window request")
    order = sorted(range(len(document_spans)), key=lambda i: (-document_spans[i][0], i))
    out, placed = [], []
    for index in order:
        _, start, end = document_spans[index]
        lo, hi = evidence_window(ids, start, end, bounds, width, left_fraction, continues)
        piece = [int(i) for i in ids[lo:hi]]
        if len(out) + len(piece) > rows:
            break
        out.extend(piece)
        placed.append((int(lo), int(hi)))
    return out, placed


def scan_windows(ids, bounds, width: int, stride: int, continues) -> list[tuple[int, int]]:
    """Candidate ``[lo, hi)`` windows for the cheap extractors: inside each
    document, ``width`` tokens at most, one starting every ``stride`` tokens
    plus one ending at the document end, each trimmed like
    ``evidence_window`` so it neither starts nor ends inside a word. Sorted,
    without duplicates (the first window of a document starts at its start)."""
    if width < 1 or stride < 1:
        raise ValueError("bad window scan request")
    windows = set()
    for lo_doc, hi_doc in bounds:
        last = max(hi_doc - width, lo_doc)
        for start in [*range(lo_doc, last + 1, stride), last]:
            lo, hi = start, min(start + width, hi_doc)
            while lo > lo_doc and lo < hi - 1 and continues(int(ids[lo])):
                lo += 1
            while hi > lo + 1 and hi < hi_doc and continues(int(ids[hi])):
                hi -= 1
            windows.add((lo, hi))
    return sorted(windows)


def best_span(start_logits, end_logits, bounds, max_len: int):
    """Best inclusive span over the document positions (index 0 of the logits
    is the null position) -> (start, end, span score, null score) with
    positions in the concatenated ids; spans stay inside one document and are
    at most ``max_len`` tokens."""
    if start_logits.ndim != 1 or start_logits.shape != end_logits.shape:
        raise ValueError("span decoding needs matching [T + 1] logits")
    null = float(start_logits[0] + end_logits[0])
    s, e = start_logits[1:].float(), end_logits[1:].float()
    best = (None, None, -math.inf)
    for lo, hi in bounds:
        if hi > s.shape[0]:
            raise ValueError("bounds exceed the logits")
        length = hi - lo
        scores = s[lo:hi, None] + e[None, lo:hi]
        band = torch.ones(length, length, dtype=torch.bool, device=scores.device)
        band = band.triu().tril(max_len - 1)
        scores = scores.masked_fill(~band, -math.inf)
        flat = int(scores.argmax())
        value = float(scores.view(-1)[flat])
        if value > best[2]:
            i, j = divmod(flat, length)
            best = (lo + i, lo + j, value)
    return best[0], best[1], best[2], null


def marginal_nll(logits, targets, valid):
    """-log of the softmax mass on each row's target set. ``logits`` [B, T+1]
    (index 0 = null), ``targets`` a list of index lists (already shifted by the
    null), ``valid`` [B, T+1] bool."""
    logits = logits.float().masked_fill(~valid, -math.inf)
    total = torch.logsumexp(logits, dim=-1)
    picked = []
    for row, indices in enumerate(targets):
        if not indices:
            raise ValueError("every example needs at least one target")
        index = torch.tensor(indices, device=logits.device)
        picked.append(torch.logsumexp(logits[row, index], dim=0))
    return (total - torch.stack(picked)).mean()


def span_token_f1(pred, gold) -> float:
    """Token-position overlap F1 of two inclusive spans."""
    if pred[0] is None:
        return 0.0
    overlap = max(0, min(pred[1], gold[1]) - max(pred[0], gold[0]) + 1)
    if overlap == 0:
        return 0.0
    precision = overlap / (pred[1] - pred[0] + 1)
    recall = overlap / (gold[1] - gold[0] + 1)
    return 2 * precision * recall / (precision + recall)


def roc_auc(positives, negatives) -> float | None:
    """Probability that a random positive outscores a random negative."""
    if not positives or not negatives:
        return None
    ranked = sorted([(v, 1) for v in positives] + [(v, 0) for v in negatives])
    rank_sum, i = 0.0, 0
    while i < len(ranked):
        j = i
        while j < len(ranked) and ranked[j][0] == ranked[i][0]:
            j += 1
        average = (i + j + 1) / 2
        rank_sum += average * sum(label for _, label in ranked[i:j])
        i = j
    n_pos, n_neg = len(positives), len(negatives)
    return (rank_sum - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)


def best_threshold(positives, negatives) -> tuple[float, float]:
    """Gate threshold on ``span - null`` maximizing accuracy (open on
    positives, closed on negatives) -> (threshold, accuracy)."""
    values = sorted(set(positives) | set(negatives))
    if not values:
        return 0.0, 0.0
    candidates = [values[0] - 1.0] + [(a + b) / 2 for a, b in zip(values, values[1:])]
    candidates.append(values[-1] + 1.0)
    total = len(positives) + len(negatives)
    best = (0.0, -1.0)
    for t in candidates:
        correct = sum(v >= t for v in positives) + sum(v < t for v in negatives)
        if correct / total > best[1]:
            best = (t, correct / total)
    return best


def quota_sample(rows_by_source: dict, quotas: dict, seed: int) -> list[dict]:
    """``quotas[source]`` rows of each source, seeded, without replacement."""
    chosen = []
    for source in sorted(quotas):
        rows = list(rows_by_source.get(source, []))
        random.Random(f"{seed}:{source}").shuffle(rows)
        chosen += rows[: int(quotas[source])]
    random.Random(seed).shuffle(chosen)
    return chosen


def parse_quotas(text: str) -> dict[str, int]:
    quotas = {}
    for item in [t.strip() for t in text.split(",") if t.strip()]:
        name, sep, value = item.partition("=")
        if not sep or name not in (*QA_SOURCES, DEPOT) or int(value) < 0:
            raise ValueError("quotas are source=count with a known source")
        quotas[name] = int(value)
    return quotas


def parse_sources(text: str) -> set[str]:
    """Comma-separated known sources (``--exclude-sources``)."""
    sources = {t.strip() for t in (text or "").split(",") if t.strip()}
    unknown = sources - {*QA_SOURCES, DEPOT}
    if unknown:
        raise ValueError(f"unknown sources: {sorted(unknown)}")
    return sources


def seed_mean_scores(maps: list[dict]) -> dict:
    """Per-row mean of several runs' score maps, over the rows every run has."""
    if not maps:
        return {}
    common = set(maps[0]).intersection(*maps[1:])
    return {name: sum(m[name] for m in maps) / len(maps) for name in sorted(common)}


# ----------------------------------------------------------------------------
# model pieces


class SpanHead(nn.Module):
    """Bidirectional span scorer over per-token encoder rows: projection,
    document-segment and position embeddings, a learned null row in front, a
    small pre-norm transformer, start/end logits (index 0 = null)."""

    def __init__(
        self,
        width: int = FEATURE_WIDTH,
        hidden: int = 1024,
        layers: int = 2,
        heads: int = 8,
        dropout: float = 0.1,
        max_positions: int = 2048,
        max_documents: int = 4,
    ):
        super().__init__()
        self.config = {
            "width": width,
            "hidden": hidden,
            "layers": layers,
            "heads": heads,
            "dropout": dropout,
            "max_positions": max_positions,
            "max_documents": max_documents,
        }
        self.project = nn.Sequential(nn.Linear(width, hidden), nn.LayerNorm(hidden))
        self.segment = nn.Embedding(max_documents, hidden)
        self.position = nn.Embedding(max_positions + 1, hidden)
        self.null = nn.Parameter(torch.zeros(1, 1, hidden))
        layer = nn.TransformerEncoderLayer(
            hidden,
            heads,
            4 * hidden,
            dropout,
            activation="gelu",
            batch_first=True,
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(layer, layers, enable_nested_tensor=False)
        self.norm = nn.LayerNorm(hidden)
        self.out = nn.Linear(hidden, 2)
        with torch.no_grad():
            nn.init.normal_(self.null, std=0.02)
            nn.init.normal_(self.segment.weight, std=0.02)
            nn.init.normal_(self.position.weight, std=0.02)

    def forward(self, features, valid, segments):
        """``features`` [B, T, width], ``valid`` [B, T] bool, ``segments``
        [B, T] long -> (start, end) logits [B, T + 1] and the [B, T + 1]
        validity mask (null first)."""
        batch, length, _ = features.shape
        # Training sees two documents and < 1,400 rows; longer inputs (many
        # documents, long passages) share the last segment and position rows
        # instead of indexing past the tables.
        positions = torch.arange(1, length + 1, device=features.device).clamp(
            max=self.config["max_positions"]
        )
        segments = segments.clamp(max=self.config["max_documents"] - 1)
        x = (
            self.project(features.float())
            + self.segment(segments)
            + self.position(positions)[None]
        )
        x = torch.cat([self.null.expand(batch, 1, -1) + self.position.weight[0], x], 1)
        mask = torch.cat(
            [torch.ones(batch, 1, dtype=torch.bool, device=valid.device), valid], 1
        )
        hidden = self.encoder(x, src_key_padding_mask=~mask)
        logits = self.out(self.norm(hidden)).float()
        logits = logits.masked_fill(~mask[..., None], -math.inf)
        return logits[..., 0], logits[..., 1], mask


def load_encoder(candidate: Path, assets: Path):
    """copy17's BF16 encoder with its frozen adapters -> (encoder, tokenizer,
    protocol, special-row suffix)."""
    from encoder_lora import encoder_special_suffix, inject_lora, restore_adapters
    from encoder_lora_export import DEFAULT_ASSETS, load_candidate
    from safetensors.torch import load_file
    from transformers import AutoModel, AutoTokenizer

    _, protocol = load_candidate(candidate)
    path = Path(assets) / DEFAULT_ASSETS["encoder"]
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
    adapters = inject_lora(
        encoder,
        protocol["rank"],
        protocol["alpha"],
        False,
        tuple(protocol["target_modules"]),
    )
    restore_adapters(adapters, load_file(str(Path(candidate) / "encoder-lora.safetensors")))
    return encoder, tokenizer, protocol, encoder_special_suffix(tokenizer)


@torch.no_grad()
def span_features(encoder, tokenizer, layouts, questions, *, prefix, dual_layer, suffix, max_tokens):
    """Per-example [n_i, 5120] fp16 rows aligned to each layout's copy ids:
    one padded encoder pass over every question-prefixed doubled document,
    the second copy's own-token rows of the final and ``dual_layer`` states,
    each L2-normalized, concatenated."""
    from encoder_lora import encoder_states_pair

    texts, owners = [], []
    for index, (layout, question) in enumerate(zip(layouts, questions, strict=True)):
        prefixed = question_prefixed_documents(
            tokenizer, layout["doubled"], layout["counts"], question, prefix
        )
        texts += prefixed
        owners += [(index, d) for d in range(len(prefixed))]
    with torch.autocast("cuda", dtype=torch.bfloat16):
        tokens = tokenizer(texts, padding=True, return_tensors="pt")
        if tokens.input_ids.shape[1] > max_tokens:
            raise ValueError(f"encoder bound exceeded: {tokens.input_ids.shape[1]}")
        tokens = tokens.to("cuda")
        final, layer = encoder_states_pair(encoder, tokens, dual_layer)
    lengths = tokens.attention_mask.sum(1).tolist()
    pieces = [[] for _ in layouts]
    for row, ((index, document), length) in enumerate(zip(owners, lengths, strict=True)):
        count = layouts[index]["counts"][document]
        lo, hi = length - count - suffix, length - suffix
        f = nn.functional.normalize(final[row, lo:hi].float(), dim=-1)
        m = nn.functional.normalize(layer[row, lo:hi].float(), dim=-1)
        pieces[index].append(torch.cat([f, m], dim=-1).half())
    out = []
    for layout, rows in zip(layouts, pieces, strict=True):
        joined = torch.cat(rows)
        if joined.shape[0] != len(layout["ids"]):
            raise ValueError("feature rows do not align with the copy ids")
        out.append(joined)
    return out


def segments_for(bounds, length: int) -> torch.Tensor:
    segments = torch.zeros(length, dtype=torch.long)
    for index, (lo, hi) in enumerate(bounds):
        segments[lo:hi] = index
    return segments


def pad_batch(rows: list[torch.Tensor], bounds_list):
    """Right-padded features [B, T, W], valid [B, T], segments [B, T]."""
    longest = max(r.shape[0] for r in rows)
    width = rows[0].shape[1]
    features = rows[0].new_zeros((len(rows), longest, width))
    valid = torch.zeros((len(rows), longest), dtype=torch.bool)
    segments = torch.zeros((len(rows), longest), dtype=torch.long)
    for i, (r, bounds) in enumerate(zip(rows, bounds_list, strict=True)):
        features[i, : r.shape[0]] = r
        valid[i, : r.shape[0]] = True
        segments[i, : r.shape[0]] = segments_for(bounds, r.shape[0])
    return features, valid, segments


# ----------------------------------------------------------------------------
# examples


def load_prepared(examples_dir: Path) -> tuple[dict, dict]:
    """``qwen_copy_qa.py prep`` rows by source: (train, held-out)."""
    train, held = {}, {}
    for source in QA_SOURCES:
        path = Path(examples_dir) / f"{source}.jsonl"
        if not path.exists():
            continue
        for row in read_rows(path):
            if row["kind"] not in ("positive", "negative"):
                continue
            (held if row["heldout"] else train).setdefault(source, []).append(row)
    return train, held


def depot_rows(depot_data: Path, split: str) -> list[dict]:
    from encoder_lora import load_split

    rows = []
    for case in load_split(Path(depot_data), split):
        rows.append(
            {
                "name": "depot-" + case["name"] if "name" in case else "depot-" + case["id"],
                "source": DEPOT,
                "question": case["question"],
                "answer": case["answer"],
                "documents": list(case["documents"]),
                "category": case.get("category"),
                "kind": "negative" if case.get("category") == "missing" else "positive",
            }
        )
    return rows


def example_targets(tokenizer, example: dict, layout: dict):
    """Inclusive gold spans of one example (empty = no answer), or None when a
    positive's span can no longer be located."""
    bounds = doc_bounds(layout["counts"])
    if example["kind"] == "negative":
        return []
    if example["source"] == DEPOT:
        spans = answer_spans(tokenizer, layout["ids"], example["answer"], bounds)
        return spans or None
    if len(layout["ids"]) != example["copy_len"]:
        return None
    b, e = example["span_source"]
    spans = gold_spans(layout["ids"], layout["ids"][b:e], bounds)
    return spans or None


# ----------------------------------------------------------------------------
# cache


def cache(args) -> dict:
    from qwen_copy_qa import document_layout, wait_for_room

    args.output.mkdir(parents=True, exist_ok=True)
    train_rows, held_rows = load_prepared(args.examples)
    quotas = parse_quotas(args.quotas)
    depot_quota = quotas.pop(DEPOT, 0)
    if args.split == "train":
        chosen = quota_sample(train_rows, quotas, args.seed)
        if depot_quota:
            depot = depot_rows(args.depot_data, "train")
            random.Random(f"{args.seed}:depot").shuffle(depot)
            chosen += depot[:depot_quota]
            random.Random(args.seed + 1).shuffle(chosen)
    else:
        chosen = quota_sample(held_rows, quotas, args.seed)
        if depot_quota:
            chosen += depot_rows(args.depot_data, "dev")[:depot_quota]
    wait_for_room(24 * 1024, "span cache")
    encoder, tokenizer, protocol, suffix = load_encoder(args.candidate, args.assets)
    index_path = args.output / "index.jsonl"
    done = {r["name"] for r in read_rows(index_path)} if index_path.exists() else set()
    shard = len(list(args.output.glob("shard-*.safetensors")))
    pending = [e for e in chosen if e["name"] not in done]
    counts, started = Counter(), time.time()
    buffer_rows, buffer_meta, offset = [], [], 0

    def flush():
        nonlocal shard, buffer_rows, buffer_meta, offset
        if not buffer_meta:
            return
        from safetensors.torch import save_file

        name = f"shard-{shard:05d}.safetensors"
        save_file({"features": torch.cat(buffer_rows)}, str(args.output / (name + ".partial")))
        (args.output / (name + ".partial")).rename(args.output / name)
        with index_path.open("a") as handle:
            for meta in buffer_meta:
                handle.write(json.dumps({**meta, "shard": name}, ensure_ascii=False) + "\n")
        shard += 1
        buffer_rows, buffer_meta, offset = [], [], 0

    def encode(batch):
        nonlocal offset
        try:
            features = span_features(
                encoder,
                tokenizer,
                [b[1] for b in batch],
                [b[0]["question"] for b in batch],
                prefix=args.question_prefix,
                dual_layer=int(protocol["dual_layer"]),
                suffix=suffix,
                max_tokens=args.max_encoder_tokens,
            )
        except ValueError:
            counts["skip:too-long-batch"] += len(batch)
            return
        for (ex, layout, targets), rows in zip(batch, features, strict=True):
            buffer_rows.append(rows.cpu())
            buffer_meta.append(
                {
                    "name": ex["name"],
                    "source": ex["source"],
                    "kind": "positive" if targets else "negative",
                    "category": ex.get("category"),
                    "offset": offset,
                    "rows": int(rows.shape[0]),
                    "counts": [int(c) for c in layout["counts"]],
                    "gold": [list(t) for t in targets],
                }
            )
            offset += int(rows.shape[0])
            counts[ex["source"] + (":positive" if targets else ":negative")] += 1
        if len(buffer_meta) >= args.shard_examples:
            flush()
            print(
                json.dumps(
                    {
                        "event": "shard",
                        "examples": sum(
                            v for k, v in counts.items() if not k.startswith("skip")
                        ),
                        "of": len(pending),
                        "seconds": round(time.time() - started, 1),
                    }
                ),
                flush=True,
            )

    batch = []
    for example in pending:
        layout = document_layout(tokenizer, list(example["documents"]))
        targets = example_targets(tokenizer, example, layout)
        if targets is None:
            counts["skip:span-lost"] += 1
            continue
        batch.append((example, layout, targets))
        if len(batch) == args.batch:
            encode(batch)
            batch = []
    if batch:
        encode(batch)
    flush()
    receipt = {
        "schema": SCHEMA_CACHE,
        "split": args.split,
        "candidate": str(Path(args.candidate).resolve()),
        "question_prefix": args.question_prefix,
        "quotas": args.quotas,
        "seed": args.seed,
        "counts": dict(counts),
        "seconds": round(time.time() - started, 1),
    }
    save_json(args.output / "cache.json", receipt)
    print(json.dumps(receipt, indent=1), flush=True)
    return receipt


# ----------------------------------------------------------------------------
# train


class CacheReader:
    """Examples of a feature cache, grouped by shard (one shard in memory),
    without the examples of ``exclude`` sources (leave-one-out training)."""

    def __init__(self, directory: Path, exclude=()):
        self.directory = Path(directory)
        self.meta = [
            row
            for row in read_rows(self.directory / "index.jsonl")
            if row["source"] not in set(exclude)
        ]
        self.by_shard = {}
        for row in self.meta:
            self.by_shard.setdefault(row["shard"], []).append(row)

    def shards(self, seed: int | None = None):
        names = sorted(self.by_shard)
        if seed is not None:
            random.Random(seed).shuffle(names)
        return names

    def load(self, shard: str) -> torch.Tensor:
        from safetensors.torch import load_file

        return load_file(str(self.directory / shard))["features"]


def batch_targets(metas):
    """Start/end target index lists (shifted by the null row)."""
    starts, ends = [], []
    for meta in metas:
        if meta["gold"]:
            starts.append(sorted({s + 1 for s, _ in meta["gold"]}))
            ends.append(sorted({e + 1 for _, e in meta["gold"]}))
        else:
            starts.append([0])
            ends.append([0])
    return starts, ends


def run_head(head, features_shard, metas, device="cuda"):
    rows = [features_shard[m["offset"] : m["offset"] + m["rows"]] for m in metas]
    bounds = [doc_bounds(m["counts"]) for m in metas]
    features, valid, segments = pad_batch(rows, bounds)
    with torch.autocast("cuda", dtype=torch.bfloat16):
        start, end, mask = head(features.to(device), valid.to(device), segments.to(device))
    return start, end, mask, bounds


def evaluate(head, reader: CacheReader, args) -> dict:
    head.eval()
    by_source = {}
    diffs = {"positive": [], "negative": []}
    with torch.no_grad():
        for shard in reader.shards():
            features = reader.load(shard)
            metas = reader.by_shard[shard]
            for i in range(0, len(metas), args.batch):
                chunk = metas[i : i + args.batch]
                start, end, _, bounds = run_head(head, features, chunk)
                for row, meta in enumerate(chunk):
                    s, e, score, null = best_span(start[row], end[row], bounds[row], args.max_span)
                    diff = score - null
                    diffs[meta["kind"]].append(diff)
                    stats = by_source.setdefault(
                        meta["source"],
                        Counter(),
                    )
                    stats[meta["kind"]] += 1
                    if meta["kind"] == "positive":
                        golds = [tuple(g) for g in meta["gold"]]
                        stats["exact"] += int((s, e) in golds)
                        stats["f1"] += max(span_token_f1((s, e), g) for g in golds)
                        lo, hi = evidence_window(
                            list(range(meta["rows"])),
                            s,
                            e,
                            bounds[row],
                            args.window,
                            args.left_fraction,
                            lambda _: False,
                        )
                        stats["window_contains"] += int(
                            any(lo <= g[0] and g[1] < hi for g in golds)
                        )
                        stats["open_at_0"] += int(diff >= 0)
                    else:
                        stats["closed_at_0"] += int(diff < 0)
    threshold, accuracy = best_threshold(diffs["positive"], diffs["negative"])
    result = {
        "gate_auc": roc_auc(diffs["positive"], diffs["negative"]),
        "gate_threshold": threshold,
        "gate_accuracy": accuracy,
        "sources": {},
    }
    for source, stats in sorted(by_source.items()):
        pos, neg = stats["positive"], stats["negative"]
        result["sources"][source] = {
            "positives": pos,
            "negatives": neg,
            "exact": round(stats["exact"] / pos, 4) if pos else None,
            "f1": round(stats["f1"] / pos, 4) if pos else None,
            "window_contains": round(stats["window_contains"] / pos, 4) if pos else None,
            "open_at_0": round(stats["open_at_0"] / pos, 4) if pos else None,
            "closed_at_0": round(stats["closed_at_0"] / neg, 4) if neg else None,
        }
    head.train()
    return result


def train(args) -> dict:
    args.output.mkdir(parents=True, exist_ok=True)
    # Leave-one-out: the excluded sources leave both caches, so neither the
    # head nor its exported gate threshold sees them.
    exclude = parse_sources(args.exclude_sources)
    train_reader = CacheReader(args.cache, exclude)
    held_reader = CacheReader(args.heldout_cache, exclude)
    if not train_reader.meta or not held_reader.meta:
        raise ValueError("no examples left after --exclude-sources")
    torch.manual_seed(args.seed)
    head = SpanHead(
        hidden=args.hidden,
        layers=args.layers,
        heads=args.heads,
        dropout=args.dropout,
        # Budget study C: one segment row per read document (default 4 keeps
        # the s31 geometry).
        max_documents=getattr(args, "max_documents", 4),
    ).cuda()
    optimizer = torch.optim.AdamW(head.parameters(), lr=args.lr, weight_decay=args.weight_decay)
    per_epoch = math.ceil(len(train_reader.meta) / args.batch)
    total = per_epoch * args.epochs
    result = {
        "schema": SCHEMA_TRAIN,
        "config": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items() if k != "func"},
        "head": head.config,
        "train_examples": len(train_reader.meta),
        "heldout_examples": len(held_reader.meta),
        "steps": total,
        "history": [],
        "evaluations": [],
    }
    step, started = 0, time.time()
    for epoch in range(args.epochs):
        window_loss, window_n = 0.0, 0
        for shard in train_reader.shards(seed=args.seed + epoch):
            features = train_reader.load(shard)
            metas = list(train_reader.by_shard[shard])
            random.Random(f"{args.seed}:{epoch}:{shard}").shuffle(metas)
            for i in range(0, len(metas), args.batch):
                chunk = metas[i : i + args.batch]
                scale = min(1.0, (step + 1) / args.warmup) * 0.5 * (
                    1 + math.cos(math.pi * min(step, total) / total)
                )
                for group in optimizer.param_groups:
                    group["lr"] = args.lr * scale
                start, end, mask, _ = run_head(head, features, chunk)
                starts, ends = batch_targets(chunk)
                loss = marginal_nll(start, starts, mask) + marginal_nll(end, ends, mask)
                optimizer.zero_grad(set_to_none=True)
                loss.backward()
                norm = float(nn.utils.clip_grad_norm_(head.parameters(), 1.0))
                optimizer.step()
                step += 1
                window_loss += float(loss)
                window_n += 1
                if step % args.log_every == 0:
                    row = {
                        "step": step,
                        "epoch": epoch,
                        "loss": round(window_loss / window_n, 4),
                        "grad_norm": round(norm, 3),
                        "lr_scale": round(scale, 4),
                        "seconds": round(time.time() - started, 1),
                    }
                    result["history"].append(row)
                    print(json.dumps({"event": "step", **row}), flush=True)
                    window_loss, window_n = 0.0, 0
        evaluation = {"epoch": epoch + 1, "step": step, **evaluate(head, held_reader, args)}
        result["evaluations"].append(evaluation)
        print(json.dumps({"event": "evaluation", **evaluation}), flush=True)
        save_json(args.output / "result.json", result)
    from safetensors.torch import save_file

    save_file(
        {k: v.detach().cpu().contiguous() for k, v in head.state_dict().items()},
        str(args.output / "span-head.safetensors"),
    )
    final = result["evaluations"][-1]
    exported = export_candidate(args, head, final["gate_threshold"], result)
    result["candidate"] = str(exported)
    result["completed"] = True
    save_json(args.output / "result.json", result)
    print(json.dumps({"event": "completed", "candidate": str(exported)}), flush=True)
    return result


def export_candidate(args, head: SpanHead, threshold: float, result: dict) -> Path:
    """copy17's candidate files + the span head, with a protocol naming the
    span-window record; candidate.json digests recomputed."""
    from encoder_lora import candidate_artifacts, digest
    from encoder_lora_export import load_candidate
    from safetensors.torch import save_file

    parent = Path(args.parent)
    candidate, protocol = load_candidate(parent)
    directory = args.output / "candidate"
    directory.mkdir(parents=True, exist_ok=True)
    for _, file in candidate_artifacts(protocol):
        if file != "protocol.json":
            shutil.copyfile(parent / file, directory / file)
    save_file(
        {k: v.detach().cpu().contiguous() for k, v in head.state_dict().items()},
        str(directory / "span-head.safetensors"),
    )
    protocol = dict(protocol)
    protocol.update(
        {
            "record_kind": RECORD_KIND,
            "document_question_prefix": args.question_prefix,
            "span_head": head.config,
            "span_window_tokens": args.window,
            "span_window_left_fraction": args.left_fraction,
            "span_max_tokens": args.max_span,
            "span_gate_threshold": float(threshold),
            "span_parent": str(parent.resolve()),
            "span_train": {
                "schema": SCHEMA_TRAIN,
                "cache": str(args.cache),
                "examples": result["train_examples"],
                "epochs": args.epochs,
                "lr": args.lr,
                "seed": args.seed,
                "exclude_sources": sorted(parse_sources(args.exclude_sources)),
            },
        }
    )
    save_json(directory / "protocol.json", protocol)
    out = dict(candidate)
    for field, file in candidate_artifacts(protocol):
        out[field] = digest(directory / file)
    out["span_head_sha256"] = digest(directory / "span-head.safetensors")
    save_json(directory / "candidate.json", out)
    return directory


# ----------------------------------------------------------------------------
# routed gate (plans 8.16/8.17)


def gate_routes_table(gate: dict, seed: str, fallback: dict | None = None) -> dict:
    """The frozen routed gate (``robust/gate/freeze.py``) for one head seed
    and, optionally, the frozen novelty fallback (``freeze_fallback.py``) ->
    profile names and the tensors of ``span-gate-routes.safetensors``:
    ``centroids`` [P, 2560] (unit rows), ``offsets`` [P] (the LCB margin
    offset; +inf never opens, -inf always does) and ``floors`` [P] (the
    centroid-cosine familiarity floor; -inf without a fallback)."""
    names = [str(n) for n in gate["benchmarks"]]
    if not 0 < len(names) <= MAX_ROUTE_PROFILES or len(set(names)) != len(names):
        raise ValueError("route profiles must be 1-64 unique names")
    centroids = torch.as_tensor(gate["centroids"], dtype=torch.float32).contiguous()
    if tuple(centroids.shape) != (len(names), GATE_ROUTE_DIMENSION):
        raise ValueError(f"centroids shape {tuple(centroids.shape)}")
    if not torch.allclose(centroids.norm(dim=1), torch.ones(len(names)), atol=1e-4):
        raise ValueError("centroids must be unit rows")
    offsets = torch.tensor([float(gate["lcb"][seed][n]) for n in names], dtype=torch.float32)
    if fallback is None:
        floors = torch.full((len(names),), -math.inf)
    else:
        if [str(n) for n in fallback["benchmarks"]] != names:
            raise ValueError("fallback profiles differ from the gate's")
        floors = torch.tensor([float(x) for x in fallback["centroid"]], dtype=torch.float32)
    if torch.isnan(offsets).any() or not (torch.isfinite(floors) | (floors == -math.inf)).all():
        raise ValueError("offsets must not be NaN; floors finite or -inf")
    return {"profiles": names, "centroids": centroids, "offsets": offsets, "floors": floors}


def span_gate_decision(
    policy: str,
    *,
    margin: float,
    offset: float = 0.0,
    table: dict | None = None,
    question=None,
    fallback: bool = False,
    declared: str | None = None,
) -> dict:
    """Zig ``decideGate``: a declared profile takes its offset under any
    policy; ``always`` opens; ``learned`` opens at ``margin >= offset``;
    ``routed`` takes the nearest centroid of the L2-normalized question (the
    first maximum) and its offset, or ``offset`` when ``fallback`` and the
    cosine is below the profile's floor."""
    if math.isnan(margin):
        raise ValueError("margin is NaN")
    if declared is not None:
        if table is None:
            raise ValueError("no gate routes for a declared profile")
        if declared not in table["profiles"]:
            raise ValueError(f"unknown gate profile: {declared}")
        i = table["profiles"].index(declared)
        limit = float(table["offsets"][i])
        return {"open": margin >= limit, "offset": limit, "profile": declared,
                "similarity": 0.0, "fallback": False}
    if policy == "always":
        return {"open": True, "offset": -math.inf, "profile": None, "similarity": 0.0, "fallback": False}
    if policy == "learned":
        return {"open": margin >= offset, "offset": offset, "profile": None,
                "similarity": 0.0, "fallback": False}
    if policy != "routed" or table is None:
        raise ValueError(f"gate policy {policy!r} needs routes" if policy == "routed" else policy)
    q = torch.nn.functional.normalize(torch.as_tensor(question, dtype=torch.float32), dim=0)
    sims = table["centroids"] @ q
    i = int(sims.argmax())
    similarity = float(sims[i])
    low = fallback and similarity < float(table["floors"][i])
    limit = offset if low else float(table["offsets"][i])
    return {"open": margin >= limit, "offset": limit, "profile": table["profiles"][i],
            "similarity": similarity, "fallback": bool(low)}


# Read budget per route (plan 8.27 D1): ``span_read_routes`` in protocol.json
# maps route profiles to a read budget and window; other routes read the top
# ``DEFAULT_READ_DOCUMENTS`` with ``span_window_tokens``. Research only: the
# Zig service refuses it since the read cascade replaced the centroid read
# routing (plan 8.32, ``span_read_cascade`` below).
DEFAULT_READ_DOCUMENTS = 2
MAX_READ_WINDOW_TOKENS = 256
MAX_READ_BUDGET_TOKENS = 16384


def span_read_routes_table(profiles, read_routes) -> dict:
    """Validates ``span_read_routes`` as Zig ``parseReadRoutes`` does: a
    nonempty object from route profile names (each a gate route) to exactly
    ``{"read_tokens": 1..16384, "window_tokens": 1..256}`` (integers) -> the
    same mapping with plain ints."""
    if not isinstance(read_routes, dict) or not read_routes:
        raise ValueError("span_read_routes must be a nonempty object")
    out = {}
    for name, setting in read_routes.items():
        if name not in profiles:
            raise ValueError(f"unknown read route: {name}")
        if not isinstance(setting, dict) or set(setting) != {"read_tokens", "window_tokens"}:
            raise ValueError(f"read route {name} needs exactly read_tokens and window_tokens")
        limits = {"read_tokens": MAX_READ_BUDGET_TOKENS, "window_tokens": MAX_READ_WINDOW_TOKENS}
        for field, limit in limits.items():
            value = setting[field]
            if type(value) is not int or not 1 <= value <= limit:
                raise ValueError(f"read route {name}: {field} must be an integer in [1, {limit}]")
        out[name] = {"read_tokens": setting["read_tokens"], "window_tokens": setting["window_tokens"]}
    return out


def span_read_set(read_routes, route, token_counts, window_tokens: int) -> dict:
    """Zig ``readPlan``: the read set of one request routed to ``route``
    over its ranked candidates' encoder token counts (no special tokens).
    ``read_routes`` None or empty: every candidate under ``window_tokens``
    (a bundle without read routes). A route with an entry reads by
    ``read_budget_documents``; any other route the top
    ``DEFAULT_READ_DOCUMENTS`` -> ``documents`` (leading count),
    ``budget_tokens`` (the budget's sum, 0 without one), ``window_tokens``."""
    counts = [int(c) for c in token_counts]
    if not counts:
        raise ValueError("no documents to read")
    if not read_routes:
        return {"documents": len(counts), "budget_tokens": 0, "window_tokens": int(window_tokens)}
    if route is None:
        raise ValueError("read routes need the request's route")
    setting = read_routes.get(route)
    if setting is None:
        return {"documents": min(len(counts), DEFAULT_READ_DOCUMENTS), "budget_tokens": 0,
                "window_tokens": int(window_tokens)}
    chosen = read_budget_documents(lambda c: c, counts, int(setting["read_tokens"]))
    return {"documents": len(chosen), "budget_tokens": sum(chosen),
            "window_tokens": int(setting["window_tokens"])}


# Read cascade (plan 8.32, study F ``replace@t``): ``span_read_cascade`` in
# protocol.json. Every request reads the top ``DEFAULT_READ_DOCUMENTS`` with
# ``span_window_tokens`` first; when that record's ``span_margin`` is below
# ``threshold`` it reads the ranked candidates until ``read_tokens`` with a
# ``window_tokens`` window and uses that record. Zig: ``memorilla_span``
# ``parseReadCascade``, ``widens``.
READ_MODES = ("narrow", "wide", "auto")


CASCADE_REQUIRED = {"threshold", "read_tokens", "window_tokens"}
CASCADE_OPTIONAL = {"floor", "wide_gate_offset"}


def _finite_number(value, what: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"span_read_cascade {what} must be a finite number")
    return float(value)


def span_read_cascade_config(value) -> dict:
    """Validates ``span_read_cascade`` as Zig ``parseReadCascade`` does:
    ``{"threshold": finite number, "read_tokens": 1..16384, "window_tokens":
    1..256}`` (integers for the last two), optionally ``"floor"`` (finite,
    below the threshold) and ``"wide_gate_offset"`` (finite), nothing else ->
    the same with float numbers and plain ints (optional fields only when
    given; absent means C*: floor -inf, the narrow records' gate)."""
    if not isinstance(value, dict) or not CASCADE_REQUIRED <= set(value) <= CASCADE_REQUIRED | CASCADE_OPTIONAL:
        raise ValueError("span_read_cascade needs threshold, read_tokens and window_tokens "
                         "(and optionally floor, wide_gate_offset)")
    out = {"threshold": _finite_number(value["threshold"], "threshold")}
    limits = {"read_tokens": MAX_READ_BUDGET_TOKENS, "window_tokens": MAX_READ_WINDOW_TOKENS}
    for field, limit in limits.items():
        if type(value[field]) is not int or not 1 <= value[field] <= limit:
            raise ValueError(f"span_read_cascade {field} must be an integer in [1, {limit}]")
        out[field] = value[field]
    if "floor" in value:
        out["floor"] = _finite_number(value["floor"], "floor")
        if not out["floor"] < out["threshold"]:
            raise ValueError("span_read_cascade floor must lie below the threshold")
    if "wide_gate_offset" in value:
        out["wide_gate_offset"] = _finite_number(value["wide_gate_offset"], "wide_gate_offset")
    return out


def span_cascade_widens(cascade: dict, mode: str, narrow_margin: float, wide_available: bool = True) -> bool:
    """Zig ``widens``: an ``auto`` request whose narrow margin is below the
    threshold and not below the floor (default -inf; NaN widens) reads wide
    when the wide set is available; ``narrow`` and ``wide`` never run a
    second pass. A narrow record error is handled before this (wide first)."""
    if mode not in READ_MODES:
        raise ValueError(f"unknown read mode {mode!r}")
    floor = cascade.get("floor", -math.inf)
    return (mode == "auto" and bool(wide_available) and not narrow_margin >= cascade["threshold"]
            and not narrow_margin < floor)


def span_cascade_gate_offset(cascade: dict | None, record: str | None, gate_offset: float) -> float:
    """Zig ``gateProtocol``: the learned gate offset for a record of ``record``
    (``narrow``/``wide``/None): a wide record of a cascade with
    ``wide_gate_offset`` takes it, every other record ``gate_offset``."""
    if cascade and record == "wide" and "wide_gate_offset" in cascade:
        return float(cascade["wide_gate_offset"])
    return float(gate_offset)


def parse_read_cascade(text: str, floor: float | None = None, wide_gate_offset: float | None = None) -> dict:
    """``THRESHOLD:READ_TOKENS:WINDOW_TOKENS`` (``--read-cascade``), plus the
    optional ``--read-cascade-floor`` / ``--read-cascade-wide-gate-offset``."""
    parts = text.split(":")
    if len(parts) != 3:
        raise ValueError(f"read cascade {text!r} is not THRESHOLD:READ_TOKENS:WINDOW_TOKENS")
    value = {"threshold": float(parts[0]), "read_tokens": int(parts[1]), "window_tokens": int(parts[2])}
    if floor is not None:
        value["floor"] = float(floor)
    if wide_gate_offset is not None:
        value["wide_gate_offset"] = float(wide_gate_offset)
    return span_read_cascade_config(value)


def parse_read_route(text: str) -> tuple[str, dict]:
    """``NAME=READ_TOKENS:WINDOW_TOKENS`` (``--read-route``)."""
    name, sep, value = text.partition("=")
    tokens, colon, window = value.partition(":")
    if not (sep and colon and name):
        raise ValueError(f"read route {text!r} is not NAME=READ_TOKENS:WINDOW_TOKENS")
    return name, {"read_tokens": int(tokens), "window_tokens": int(window)}


def gate_routes(args) -> dict:
    """A span-window candidate plus the routed gate: copies the parent's
    artifacts, writes ``span-gate-routes.safetensors`` and sets
    ``span_gate_policy``, ``span_gate_offset`` and ``span_gate_routes`` in
    ``protocol.json``; ``candidate.json`` digests recomputed. With
    ``--read-route`` it also sets ``span_read_routes`` (plan 8.27 D1)."""
    import hashlib

    from encoder_lora import candidate_artifacts, digest
    from encoder_lora_export import load_candidate
    from safetensors.torch import save_file

    parent, directory = Path(args.candidate), Path(args.output)
    candidate, protocol = load_candidate(parent)
    if protocol.get("record_kind") != RECORD_KIND:
        raise ValueError("gate routes need a span-window candidate")
    if candidate.get("span_head_sha256") != digest(parent / "span-head.safetensors"):
        raise ValueError("candidate artifact changed: span-head.safetensors")
    gate = torch.load(args.gate, weights_only=False)
    fallback = torch.load(args.fallback, weights_only=False) if args.fallback else None
    table = gate_routes_table(gate, args.seed, fallback)
    read_routes = None
    cascade = None
    if getattr(args, "read_cascade", None):
        if getattr(args, "read_route", None):
            raise ValueError("--read-cascade replaces --read-route; give one")
        cascade = parse_read_cascade(args.read_cascade, getattr(args, "read_cascade_floor", None),
                                     getattr(args, "read_cascade_wide_gate_offset", None))
    elif getattr(args, "read_cascade_floor", None) is not None or getattr(args, "read_cascade_wide_gate_offset", None) is not None:
        raise ValueError("--read-cascade-floor and --read-cascade-wide-gate-offset need --read-cascade")
    if getattr(args, "read_route", None):
        pairs = [parse_read_route(text) for text in args.read_route]
        if len({name for name, _ in pairs}) != len(pairs):
            raise ValueError("a read route is given twice")
        read_routes = span_read_routes_table(table["profiles"], dict(pairs))
    directory.mkdir(parents=True, exist_ok=True)
    for _, file in [*candidate_artifacts(protocol), (None, "span-head.safetensors")]:
        if file != "protocol.json":
            shutil.copyfile(parent / file, directory / file)
    save_file({k: table[k] for k in ("centroids", "offsets", "floors")},
              str(directory / GATE_ROUTES_FILE))
    protocol = dict(protocol)
    protocol.update(
        {
            "span_gate_policy": args.policy,
            "span_gate_offset": float(args.offset),
            "span_gate_routes": {"profiles": table["profiles"], "fallback": fallback is not None},
            "span_gate_fit": {
                "seed": args.seed,
                "gate_sha256": hashlib.sha256(Path(args.gate).read_bytes()).hexdigest(),
                "fallback_sha256": hashlib.sha256(Path(args.fallback).read_bytes()).hexdigest()
                if args.fallback else None,
            },
        }
    )
    if read_routes is not None:
        protocol["span_read_routes"] = read_routes
    if cascade is not None:
        protocol["span_read_cascade"] = cascade
    save_json(directory / "protocol.json", protocol)
    out = dict(candidate)
    for field, file in candidate_artifacts(protocol):
        out[field] = digest(directory / file)
    out["span_head_sha256"] = digest(directory / "span-head.safetensors")
    out["span_gate_routes_sha256"] = digest(directory / GATE_ROUTES_FILE)
    save_json(directory / "candidate.json", out)
    result = {"candidate": str(directory), "profiles": table["profiles"],
              "offsets": [float(x) for x in table["offsets"]],
              "floors": [float(x) for x in table["floors"]],
              "policy": args.policy, "offset": float(args.offset),
              "read_routes": read_routes, "read_cascade": cascade}
    print(json.dumps(result, indent=1))
    return result


# ----------------------------------------------------------------------------
# inference (qwen_hybrid_eval.record_memory dispatches here)


def attach_span_head(models: dict, candidate_dir: Path, protocol: dict) -> None:
    """Load ``span-head.safetensors`` of a span-window candidate into
    ``models["span_head"]`` (no-op for other candidates)."""
    if protocol.get("record_kind") != RECORD_KIND:
        return
    from encoder_lora import digest, token_continues_word
    from safetensors.torch import load_file

    path = Path(candidate_dir) / "span-head.safetensors"
    candidate = json.loads((Path(candidate_dir) / "candidate.json").read_text())
    if candidate.get("span_head_sha256") != digest(path):
        raise ValueError("span head changed since export")
    head = SpanHead(**protocol["span_head"]).cuda().eval()
    head.load_state_dict(load_file(str(path)))
    models["span_head"] = head
    tokenizer = models["dec_tok"]
    cache = {}

    def continues(token_id: int) -> bool:
        if token_id not in cache:
            cache[token_id] = token_continues_word(tokenizer, token_id)
        return cache[token_id]

    models["span_continues"] = continues


def span_record_memory(models, protocol, case, *, suffix, pad_id, max_length, window_policy=None):
    """The span-window record of one case -> (rows [W, width], info) in the
    shape ``record_memory`` returns (``gate`` holds the sigmoid of span minus
    null minus threshold, so ``learned_gate_open`` opens at >= 0.5).

    ``window_policy`` (budget study B, research only; ``None`` = the
    candidate's own single window, unchanged): ``P1`` the same single window,
    ``P2`` one window of ``WIDE_WINDOW_ROWS``, ``P3`` ``multi_windows`` padded
    to ``MULTI_WINDOW_ROWS``. The gate margin is the best span's in every
    policy."""
    from qwen_copy_qa import document_layout

    if "span_head" not in models:
        raise ValueError("span-window candidate loaded without its span head")
    if window_policy is not None and window_policy not in WINDOW_POLICIES:
        raise ValueError(f"unknown window policy {window_policy}")
    enc_tok, dec_tok = models["enc_tok"], models["dec_tok"]
    layout = document_layout(enc_tok, list(case["documents"]))
    features = span_features(
        models["encoder"],
        enc_tok,
        [layout],
        [case["question"]],
        prefix=protocol["document_question_prefix"],
        dual_layer=int(protocol["dual_layer"]),
        suffix=suffix,
        max_tokens=max_length,
    )[0]
    bounds = doc_bounds(layout["counts"])
    features, valid, segments = pad_batch([features], [bounds])
    with torch.inference_mode(), torch.autocast("cuda", dtype=torch.bfloat16):
        start, end, _ = models["span_head"](
            features.cuda(), valid.cuda(), segments.cuda()
        )
    s, e, score, null = best_span(start[0], end[0], bounds, protocol["span_max_tokens"])
    ids = layout["ids"]
    width = int(protocol["span_window_tokens"])
    windows = None
    if window_policy == "P3":
        width = MULTI_WINDOW_ROWS
        joined, windows = multi_windows(
            ids,
            bounds,
            document_best_spans(start[0], end[0], bounds, protocol["span_max_tokens"]),
            width=MULTI_WINDOW_WIDTH,
            rows=MULTI_WINDOW_ROWS,
            left_fraction=protocol["span_window_left_fraction"],
            continues=models["span_continues"],
        )
        lo, hi = windows[0]
        record_ids = joined + [int(pad_id)] * (width - len(joined))
    else:
        if window_policy == "P2":
            width = WIDE_WINDOW_ROWS
        lo, hi = evidence_window(
            ids, s, e, bounds, width, protocol["span_window_left_fraction"], models["span_continues"]
        )
        record_ids = window_ids(ids, lo, hi, width, pad_id)
    margin = score - null - float(protocol["span_gate_threshold"])
    gate = 1.0 / (1.0 + math.exp(-max(min(margin, 60.0), -60.0)))
    embedding = models["decoder"].get_input_embeddings()
    with torch.inference_mode():
        rows = embedding.weight[torch.tensor(record_ids, device="cuda")].detach()
    document, (doc_lo, _) = next(
        (i, b) for i, b in enumerate(bounds) if b[0] <= s < b[1]
    )
    return rows, {
        "copy_ids": [int(i) for i in ids],
        "spans": [[int(s), int(e) + 1, 0]],
        "gate": [round(gate, 6)],
        "span_text": dec_tok.decode([int(i) for i in ids[s : e + 1]]).strip(),
        "span_score": round(score, 4),
        "null_score": round(null, 4),
        "span_margin": round(margin, 4),
        "window": [int(lo), int(hi)],
        "window_document": int(document),
        "window_document_tokens": [int(lo - doc_lo), int(hi - doc_lo)],
        "copy_moved_slots": 0,
        "copy_completed_tokens": 0,
        "self_context_fallbacks": int(layout["fallbacks"]),
        "feature_rows": int(features.shape[1]),
        "snap_min_top1_cosine": 1.0,
        "snap_top1_cosines": [1.0] * width,
        "record_ids": record_ids,
        "record_text": "".join(dec_tok.decode([int(i)]) for i in record_ids),
        **(
            {
                "window_policy": window_policy,
                "windows": [list(w) for w in windows] if windows else [[int(lo), int(hi)]],
                "window_rows_used": sum(h - l for l, h in windows) if windows else int(hi - lo),
                "read_documents": len(bounds),
                "read_tokens": int(len(ids)),
            }
            if window_policy is not None
            else {}
        ),
    }


CHEAP_EXTRACTORS = ("embed-window", "lead")
DEFAULT_STRIDE = 8


def cheap_record_memory(models, case, *, extractor, query, width, stride, pad_id, batch=64):
    """Extractor ablation (plan 8.14): an evidence window of the same top-2
    chunks and width as the span head's, chosen without the head and in the
    same record shape as ``span_record_memory``.

    ``embed-window`` scores every ``scan_windows`` window by the cosine of its
    pooled vector (frozen Qwen3-Embedding-4B, adapters off, the stored chunk
    vectors' last-token pooling) with the stored question vector ``query``;
    window vectors need no question, so they could be computed at ingest.
    ``span_margin`` holds the best cosine so ``sweep`` can gate on it.
    ``lead`` takes the first window of the top-1 chunk (no scoring)."""
    from encoder_lora import token_continues_word
    from encoder_reference import normalize_features
    from qwen_copy_qa import document_layout
    from qwen_hybrid_eval import adapters_disabled

    if extractor not in CHEAP_EXTRACTORS:
        raise ValueError(f"unknown extractor {extractor!r}")
    enc_tok, dec_tok = models["enc_tok"], models["dec_tok"]
    layout = document_layout(enc_tok, list(case["documents"]))
    ids, bounds = layout["ids"], doc_bounds(layout["counts"])
    continues = models.get("span_continues") or (
        lambda token: token_continues_word(dec_tok, token)
    )
    if extractor == "lead":
        windows = scan_windows(ids, bounds[:1], width, width, continues)[:1]
        scores = [0.0]
    else:
        windows = scan_windows(ids, bounds, width, stride, continues)
        texts = [dec_tok.decode([int(i) for i in ids[lo:hi]]).strip() for lo, hi in windows]
        vectors = []
        with torch.inference_mode(), adapters_disabled(models["encoder"]):
            for start in range(0, len(texts), batch):
                tokens = enc_tok(
                    texts[start : start + batch], padding=True, return_tensors="pt"
                ).to("cuda")
                hidden = models["encoder"](**tokens, use_cache=False).last_hidden_state
                vectors.append(normalize_features(hidden, tokens["attention_mask"]).float())
        query = torch.nn.functional.normalize(query.float().cuda(), dim=-1)
        scores = (torch.cat(vectors) @ query).tolist()
    best = max(range(len(windows)), key=lambda i: scores[i])
    lo, hi = windows[best]
    record_ids = window_ids(ids, lo, hi, width, pad_id)
    embedding = models["decoder"].get_input_embeddings()
    with torch.inference_mode():
        rows = embedding.weight[torch.tensor(record_ids, device="cuda")].detach()
    document = next(i for i, (a, b) in enumerate(bounds) if a <= lo < b)
    return rows, {
        "extractor": extractor,
        "spans": [[int(lo), int(hi), 0]],
        "gate": [1.0],
        "span_margin": round(float(scores[best]), 6),
        "windows_scanned": len(windows),
        "window": [int(lo), int(hi)],
        "window_document": int(document),
        "window_document_tokens": [int(lo - bounds[document][0]), int(hi - bounds[document][0])],
        "self_context_fallbacks": int(layout["fallbacks"]),
        "feature_rows": len(ids),
        "snap_min_top1_cosine": 1.0,
        "snap_top1_cosines": [1.0] * width,
        "record_ids": record_ids,
        "record_text": "".join(dec_tok.decode([int(i)]) for i in record_ids),
    }


# ----------------------------------------------------------------------------
# oracle: the decoder reads gold windows (no training)


def oracle(args) -> dict:
    """Depot dev/fresh through the hybrid prompt with gold-span evidence
    windows of each requested width (non-missing cases), canonicalized."""
    from encoder_lora import token_continues_word
    from encoder_lora_export import DEFAULT_ASSETS, load_candidate, load_models
    from qwen_copy_qa import document_layout
    from qwen_hybrid_eval import (
        SoftMemory,
        answer_with,
        canonicalize_answer,
        compose_memory_rows,
        load_released_reader,
        system_prompt_text,
    )

    paths = {k: Path(args.assets) / v for k, v in DEFAULT_ASSETS.items()}
    _, protocol = load_candidate(args.candidate)
    models = load_models(args.candidate, protocol, paths)
    released = load_released_reader(paths, models["reader_config"])
    soft = SoftMemory(models, released)
    dec_tok, enc_tok = models["dec_tok"], models["enc_tok"]
    pad = dec_tok.encode(" ", add_special_tokens=False)[0]
    embedding = models["decoder"].get_input_embeddings()
    system = system_prompt_text("merged")
    widths = [int(w) for w in args.widths.split(",")]
    result = {"schema": SCHEMA_ORACLE, "left_fraction": args.left_fraction, "summary": {}, "rows": []}
    for set_name, path in (("depot-dev", args.depot_dev), ("depot-fresh", args.depot_fresh)):
        for case in json.loads(Path(path).read_text())["cases"]:
            if case["answer"] == "UNKNOWN":
                continue
            documents = list(case["documents"])
            layout = document_layout(enc_tok, documents)
            bounds = doc_bounds(layout["counts"])
            spans = answer_spans(enc_tok, layout["ids"], case["answer"], bounds)
            if not spans:
                continue
            s, e = spans[0]
            soft_rows = soft.rows({"documents": documents, "question": case["question"]})
            for width in widths:
                lo, hi = evidence_window(
                    layout["ids"], s, e, bounds, width, args.left_fraction,
                    lambda i: token_continues_word(dec_tok, i),
                )
                ids = window_ids(layout["ids"], lo, hi, width, pad)
                with torch.inference_mode():
                    rows = embedding.weight[torch.tensor(ids, device="cuda")].detach()
                memories = compose_memory_rows(soft_rows, rows)
                output, _ = answer_with(
                    models, memories, system_prompt=system,
                    question=case["question"], max_new_tokens=48,
                )
                text = "".join(dec_tok.decode([i]) for i in ids)
                answer = canonicalize_answer(output["answer"].strip(), text)
                key = f"{set_name} | w{width}"
                bucket = result["summary"].setdefault(key, Counter())
                bucket["cases"] += 1
                bucket["exact"] += int(answer == case["answer"])
                bucket[f"{case.get('category')}_cases"] += 1
                bucket[f"{case.get('category')}_exact"] += int(answer == case["answer"])
                result["rows"].append(
                    {"set": set_name, "name": case["name"], "width": width, "record": text,
                     "gold": case["answer"], "answer": answer}
                )
    result["summary"] = {k: dict(v) for k, v in result["summary"].items()}
    save_json(args.output, result)
    print(json.dumps(result["summary"], indent=1), flush=True)
    return result


# ----------------------------------------------------------------------------
# sweep: gate thresholds chosen offline (no decoder runs)


def policy_rows(soft_rows: dict, window_rows: dict, margins: dict, threshold: float):
    """Per question: the always-window row when its record margin (span minus
    null minus the exported threshold) is >= ``threshold``, else the soft row
    (a closed gate gives the soft arm's prompt exactly). Questions missing
    from either arm or without a margin are left out."""
    out = []
    for name, soft in soft_rows.items():
        if name not in window_rows or name not in margins:
            continue
        chosen = window_rows[name] if margins[name] >= threshold else soft
        out.append(dict(chosen, policy_threshold=threshold, policy_window=chosen is not soft))
    return out


def sweep(args) -> dict:
    """Write policy arms ``gate<t>.jsonl`` (window when margin >= t) next to
    links to the soft and always-window arms, for ``paper-score``."""
    args.output.mkdir(parents=True, exist_ok=True)
    def latest(path):
        rows = {}
        for row in read_rows(path):
            rows[row["name"]] = row
        return rows

    soft, window = latest(args.soft), latest(args.window)
    margins = {}
    for row in read_rows(args.records):
        if "span_margin" in row:
            margins[row["name"]] = float(row["span_margin"])
    thresholds = [float(t) for t in args.thresholds.split(",")]
    summary = {"rows": len(soft), "with_margin": len(margins), "arms": {}}
    for link, target in (("soft.jsonl", args.soft), ("span-bench.jsonl", args.window)):
        path = args.output / link
        if path.is_symlink() or path.exists():
            path.unlink()
        path.symlink_to(Path(target).resolve())
    for threshold in thresholds:
        rows = policy_rows(soft, window, margins, threshold)
        name = f"gate{threshold:+g}"
        with (args.output / f"{name}.jsonl").open("w") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        summary["arms"][name] = {
            "rows": len(rows),
            "window": sum(r["policy_window"] for r in rows),
        }
    save_json(args.output / "sweep.json", summary)
    print(json.dumps(summary), flush=True)
    return summary


def seeds(args) -> dict:
    """Seed aggregate of one comparison: from a ``paper-score`` per-row file
    (``per-row.json``), the paired delta and CI of each seed's arm against
    ``--left``, and the delta of the seed-mean per-row score (bootstrap over
    rows; the seed spread is reported beside it)."""
    from qwen_hybrid_eval import paired_bootstrap_ci

    per_row = json.loads(Path(args.per_row).read_text())
    arms = [a.strip() for a in args.right.split(",") if a.strip()]
    missing = [a for a in [args.left, *arms] if a not in per_row]
    if missing:
        raise ValueError(f"{args.per_row} has no per-row scores for {missing}")
    left = per_row[args.left]["scores"]
    rights = [per_row[arm]["scores"] for arm in arms]
    per_seed = [paired_bootstrap_ci(left, right, args.resamples, args.bootstrap_seed) for right in rights]
    deltas = [s["mean_delta"] for s in per_seed]
    mean_delta = sum(deltas) / len(deltas)
    spread = (
        math.sqrt(sum((d - mean_delta) ** 2 for d in deltas) / (len(deltas) - 1))
        if len(deltas) > 1
        else 0.0
    )
    result = {
        "left": args.left,
        "right": arms,
        "per_seed": per_seed,
        "seed_sd": round(spread, 2),
        "seed_mean": paired_bootstrap_ci(
            left, seed_mean_scores(rights), args.resamples, args.bootstrap_seed
        ),
    }
    if args.output:
        save_json(args.output, result)
    print(json.dumps(result), flush=True)
    return result


# ----------------------------------------------------------------------------
# CLI


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="command", required=True)
    o = sub.add_parser("oracle")
    o.add_argument("--assets", type=Path, required=True)
    o.add_argument("--candidate", type=Path, required=True, help="copy17 candidate dir")
    o.add_argument("--depot-dev", type=Path, required=True)
    o.add_argument("--depot-fresh", type=Path, required=True)
    o.add_argument("--output", type=Path, required=True)
    o.add_argument("--widths", default="32,48")
    o.add_argument("--left-fraction", type=float, default=DEFAULT_LEFT_FRACTION)
    c = sub.add_parser("cache")
    c.add_argument("--assets", type=Path, required=True)
    c.add_argument("--candidate", type=Path, required=True, help="copy17 candidate dir")
    c.add_argument("--examples", type=Path, required=True, help="qwen_copy_qa prep dir")
    c.add_argument("--depot-data", type=Path, required=True, help="data-paired-v2")
    c.add_argument("--output", type=Path, required=True)
    c.add_argument("--split", choices=("train", "heldout"), required=True)
    c.add_argument("--quotas", required=True, help="source=count,... (depot included)")
    c.add_argument("--seed", type=int, default=29)
    c.add_argument("--batch", type=int, default=8)
    c.add_argument("--shard-examples", type=int, default=512)
    c.add_argument("--max-encoder-tokens", type=int, default=1400)
    c.add_argument(
        "--question-prefix", choices=tuple(QUESTION_PREFIX_TEMPLATES), default=DEFAULT_PREFIX
    )
    t = sub.add_parser("train")
    t.add_argument("--cache", type=Path, required=True)
    t.add_argument("--heldout-cache", type=Path, required=True)
    t.add_argument("--parent", type=Path, required=True, help="copy17 candidate dir")
    t.add_argument("--output", type=Path, required=True)
    t.add_argument("--epochs", type=int, default=3)
    t.add_argument("--batch", type=int, default=32)
    t.add_argument("--lr", type=float, default=3e-4)
    t.add_argument("--weight-decay", type=float, default=0.01)
    t.add_argument("--warmup", type=int, default=200)
    t.add_argument("--hidden", type=int, default=1024)
    t.add_argument("--layers", type=int, default=2)
    t.add_argument("--heads", type=int, default=8)
    t.add_argument("--dropout", type=float, default=0.1)
    t.add_argument(
        "--max-documents",
        type=int,
        default=4,
        help="document segment rows (budget study C: one per read document)",
    )
    t.add_argument("--max-span", type=int, default=DEFAULT_MAX_SPAN)
    t.add_argument("--window", type=int, default=DEFAULT_WINDOW)
    t.add_argument("--left-fraction", type=float, default=DEFAULT_LEFT_FRACTION)
    t.add_argument("--seed", type=int, default=31)
    t.add_argument("--log-every", type=int, default=50)
    t.add_argument(
        "--question-prefix", choices=tuple(QUESTION_PREFIX_TEMPLATES), default=DEFAULT_PREFIX
    )
    t.add_argument(
        "--exclude-sources", default="", help="sources left out of both caches (leave-one-out)"
    )
    w = sub.add_parser("sweep")
    w.add_argument("--soft", type=Path, required=True, help="soft arm JSONL")
    w.add_argument("--window", type=Path, required=True, help="always-window arm JSONL")
    w.add_argument("--records", type=Path, required=True, help="records.jsonl (margins)")
    w.add_argument("--output", type=Path, required=True)
    w.add_argument("--thresholds", default="-8,-6,-4,-2,0,2,4")
    g = sub.add_parser("gate-routes", help="add the routed gate to a span candidate")
    g.add_argument("--candidate", type=Path, required=True, help="span-window candidate")
    g.add_argument("--output", type=Path, required=True, help="new candidate directory")
    g.add_argument("--gate", type=Path, required=True, help="frozen routed gate (.pt)")
    g.add_argument("--fallback", type=Path, help="frozen novelty fallback (.pt)")
    g.add_argument("--seed", required=True, help="head seed of the gate's offsets (s31...)")
    g.add_argument("--policy", choices=GATE_POLICIES, default="routed")
    g.add_argument("--offset", type=float, required=True, help="learned/fallback margin offset")
    g.add_argument(
        "--read-route",
        action="append",
        default=[],
        metavar="NAME=READ_TOKENS:WINDOW_TOKENS",
        help="read budget per route (span_read_routes; repeatable; plan 8.27 D1; refused by Zig)",
    )
    g.add_argument(
        "--read-cascade",
        metavar="THRESHOLD:READ_TOKENS:WINDOW_TOKENS",
        help="read cascade (span_read_cascade; plan 8.32), e.g. --read-cascade=-4:512:64",
    )
    g.add_argument("--read-cascade-floor", type=float, help="span_read_cascade.floor (default -inf)")
    g.add_argument("--read-cascade-wide-gate-offset", type=float,
                   help="span_read_cascade.wide_gate_offset (default: the narrow records' gate)")
    s = sub.add_parser("seeds")
    s.add_argument("--per-row", type=Path, required=True, help="paper-score per-row.json")
    s.add_argument("--left", default="soft")
    s.add_argument("--right", required=True, help="comma-separated arms, one per seed")
    s.add_argument("--output", type=Path)
    s.add_argument("--resamples", type=int, default=10000)
    s.add_argument("--bootstrap-seed", type=int, default=23)
    args = p.parse_args(argv)
    if args.command == "gate-routes" and not math.isfinite(args.offset):
        p.error("the gate offset must be finite")
    if args.command in ("oracle", "train") and not 0 <= args.left_fraction <= 1:
        p.error("left fraction must lie in [0, 1]")
    try:
        if args.command == "cache":
            parse_quotas(args.quotas)
        if args.command == "train":
            parse_sources(args.exclude_sources)
        if args.command == "gate-routes":
            for text in args.read_route:
                parse_read_route(text)
            if args.read_cascade is not None:
                parse_read_cascade(args.read_cascade, args.read_cascade_floor, args.read_cascade_wide_gate_offset)
    except ValueError as error:
        p.error(str(error))
    return args


def main(argv=None):
    args = parse_args(argv)
    commands = {"oracle": oracle, "cache": cache, "train": train, "sweep": sweep, "seeds": seeds,
                "gate-routes": gate_routes}
    return commands[args.command](args)


if __name__ == "__main__":
    main()
