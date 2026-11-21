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

"""Train the copy half of the Qwen hybrid memory on general question answering.

copy17 (``tokens-copy17-gate3-1024``) was trained on depot records only, so on
general text its record is off-topic and its snap-cosine gate fires on most
cases (plan section 8.8). This script continues copy17 decoder-free, exactly as
copy17 itself was trained (lexical token cross-entropy against the frozen
Qwen3-8B input-embedding table at tau 0.05 plus the half squared log-norm term,
pointer loss on the copy head, gate BCE at weight 3, cached-style no-grad
encoder with copy17's rank-8 q/v adapters, self-context doubling, tokens-dual
features at layer 24), on question-answer examples:

- ``prep`` (CPU): samples Memorilla-Data QA rows, takes the top-2 chunks by
  stored chunk-embedding cosine to the stored question embedding (the rule the
  hybrid uses at inference) and builds the 16-slot record target. A normalized
  answer found verbatim in those chunks gives ``" answer" + <exact source
  span>`` (the span widened to word boundaries like ``complete_copied_words``;
  gate 1 on the span slots, the copy head points at the span's positions),
  padded with the space token; an answer not in the chunks gives 16 pads with
  gate 0 (a learned relevance gate). Depot examples keep copy17's records.
  ``--chunking pack256`` (default ``none``) reads 256-token passages instead:
  each collection's chunks are packed with the rag256 rule (``pack_chunks``)
  and the top-2 chunks by the same stored cosine select the packs that hold
  them (``select_top_packs``; a pack is taken once, the next-best chunk's pack
  fills the second place).
- ``probe`` (GPU): checks that the frozen decoder reads ``<label> <span>``
  records under the hybrid's merged prompt before the label is fixed.
- ``train`` (GPU): the continuation from copy17. Only the encoder (+ frozen
  adapters) and the decoder's embedding table are on the GPU; the decoder is
  loaded only for the periodic hybrid evaluation and released afterwards.
- ``report`` (CPU, the scoring venv with upstream ``memorilla.metrics``):
  paired soft / old hybrid / new hybrid comparison with bootstrap confidence
  intervals, gate rates and examples.

The remeasure itself runs ``qwen_hybrid_eval.py`` with ``--record-gate-mode
learned``. Research diagnostic only; nothing here qualifies a serving profile.
"""

from __future__ import annotations

import argparse
import functools
import gc
import hashlib
import json
import math
import random
import re
import signal
import string
import subprocess
import time
from collections import Counter
from pathlib import Path

import torch

from qwen_hybrid_eval import (
    LEARNED_GATE_THRESHOLD,
    QUESTION_PREFIX_TEMPLATES,
    StoredEmbeddings,
    embedding_directory,
    learned_gate_open,
    normalize_answer,
    parquet_cases,
    question_prefixed_documents,
    read_rows,
    retrieval_order,
    sample_rows,
    save_json,
)

SCHEMA_EXAMPLES = "antfly.memorilla.qwen-copy-qa-examples.v1"
SCHEMA_TRAIN = "antfly.memorilla.qwen-copy-qa-train.v1"
SCHEMA_REPORT = "antfly.memorilla.qwen-copy-qa-report.v1"
SLOTS = 16
TOP_K = 2
DEFAULT_LABEL = " answer"
# Stage 2 configs with extractive or short answers, then the Stage 3 TRAIN
# splits the released stage3 reader also saw. Never validation/test.
QA_CONFIGS = (
    "squad_v2",
    "drop",
    "coqa",
    "quail",
    "msmarco",
    "pubmedqa",
    "triviaqa",
    "narrativeqa",
)
QA_SPLIT = "train"
# Polar or abstaining answers carry nothing to copy: the record stays closed
# even when the word happens to occur in a chunk.
POLAR_ANSWERS = {"yes", "no", "maybe", "unknown", "unanswerable", "no answer", "none"}
MIN_ANSWER_CHARS = 2
NUMERIC = re.compile(r"[0-9][0-9 .,]*")
WHITESPACE_WORD = re.compile(r"\S+")
PUNCTUATION = set(string.punctuation)
# DROP numbers are mostly computed (counts, differences); a number found in
# the passage is usually a coincidence, so those rows are left out.
SKIP_NUMERIC_CONFIGS = ("drop",)
DEPOT_RECORD = "asked-site"
DEPOT_NAME_WEIGHT = 4.0
POSITIVE, NEGATIVE, SKIP = "positive", "negative", "skip"
HELDOUT_MODULUS = 200
GPU_BUDGET_MIB = 115 * 1024
CHUNKING_CHOICES = ("none", "pack256")
PACK_TOKENS = 256
PACK_RULE = "rag256-greedy-newline-pack-256-qwen3-tokens-long-chunk-word-start-split-v1"
# Read-set prep (budget study C): the chunks in stored-cosine order, at most
# READ_CAP of them, read while their token total stays within --read-tokens
# (``qwen_span_copy.read_budget_documents``, the eval's R<n> rule).
READ_CAP = 50
READ_RULE = "stored-cosine-order-top50-first-always-stop-at-first-over-budget-qwen3-tokens-no-specials-v1"


# ----------------------------------------------------------------------------
# record construction (CPU, tokenizer only)


def normalized_word_spans(text: str) -> list[tuple[str, int, int]]:
    """Whitespace words of ``text`` under SQuAD normalization, each with the
    character span of the whitespace word it came from. Normalizing word by
    word equals normalizing the text (lower-casing and punctuation removal are
    per character, and whitespace is a word boundary for the article rule)."""
    spans = []
    for match in WHITESPACE_WORD.finditer(text):
        for word in normalize_answer(match.group(0)).split():
            spans.append((word, match.start(), match.end()))
    return spans


def trim_punctuation(text: str, start: int, end: int) -> tuple[int, int]:
    while start < end and text[start] in PUNCTUATION:
        start += 1
    while end > start and text[end - 1] in PUNCTUATION:
        end -= 1
    return start, end


def answer_char_ranges(text: str, answer: str) -> list[tuple[int, int]]:
    """Character ranges of ``text`` whose words normalize to the answer's
    normalized words, in order, trimmed of surrounding punctuation."""
    target = normalize_answer(answer).split()
    if not target:
        return []
    words = normalized_word_spans(text)
    keys = [w[0] for w in words]
    ranges = []
    for index in range(len(keys) - len(target) + 1):
        if keys[index : index + len(target)] == target:
            start, end = trim_punctuation(
                text, words[index][1], words[index + len(target) - 1][2]
            )
            if start < end and (start, end) not in ranges:
                ranges.append((start, end))
    return ranges


def covering_tokens(offsets, start: int, end: int) -> tuple[int, int] | None:
    """[first, last) of the tokens whose character offsets overlap
    ``[start, end)``, or None."""
    inside = [i for i, (a, b) in enumerate(offsets) if a < end and b > start]
    return (inside[0], inside[-1] + 1) if inside else None


def word_complete(ids, first: int, last: int, continues) -> tuple[int, int]:
    """Widen ``[first, last)`` to word boundaries with the exact loop of
    ``complete_copied_words``: back while the first token continues a word,
    forward while the next token does."""
    while first > 0 and continues(ids[first]):
        first -= 1
    while last < len(ids) and continues(ids[last]):
        last += 1
    return first, last


def span_occurrences(ids, span) -> list[int]:
    """Start positions of every occurrence of the id sequence ``span``."""
    ids, span = list(ids), list(span)
    size = len(span)
    if not size:
        return []
    return [s for s in range(len(ids) - size + 1) if ids[s : s + size] == span]


def document_layout(tokenizer, documents) -> dict:
    """The copy half's view of a case's documents (the evaluator's path): the
    space-prefixed texts, their self-context doubles and kept counts, the
    concatenated single ids (copy sources) and per-token ``(document, start,
    end)`` character offsets into each prefixed text."""
    from encoder_lora import prefixed_documents, self_context_documents

    prefixed = prefixed_documents(list(documents), True)
    doubled, counts, singles, fallbacks = self_context_documents(tokenizer, prefixed)
    ids, offsets = [], []
    for index, (text, single) in enumerate(zip(prefixed, singles, strict=True)):
        single = [int(i) for i in single]
        shift = 0
        encoded = tokenizer(text, add_special_tokens=False, return_offsets_mapping=True)
        if list(encoded["input_ids"]) != single:
            shift = 1
            encoded = tokenizer(
                " " + text, add_special_tokens=False, return_offsets_mapping=True
            )
            if list(encoded["input_ids"]) != single:
                raise ValueError("offsets do not reproduce the copy-source ids")
        for a, b in encoded["offset_mapping"]:
            offsets.append((index, max(int(a) - shift, 0), max(int(b) - shift, 0)))
        ids.extend(single)
    return {
        "texts": prefixed,
        "doubled": doubled,
        "counts": counts,
        "ids": ids,
        "offsets": offsets,
        "fallbacks": fallbacks,
    }


def closed_record(kind: str, reason: str, pad_id: int, slots: int = SLOTS) -> dict:
    return {
        "kind": kind,
        "reason": reason,
        "target_ids": [int(pad_id)] * slots,
        "gate": [0] * slots,
        "pointer": [],
    }


def qa_record(
    answer,
    layout: dict,
    *,
    decode,
    continues,
    label_ids,
    pad_id: int,
    slots: int = SLOTS,
    skip_numeric: bool = False,
) -> dict:
    """The 16-slot record target of one QA example.

    ``positive``: the normalized answer occurs verbatim in the documents; the
    record is ``label_ids`` (+ one generated space when the span does not start
    with one) + the exact source span (word-completed, ``continues``) padded
    with ``pad_id``; gate 1 on the span slots, 0 elsewhere; ``pointer`` lists
    ``[slot, [positions]]`` at every occurrence of the span's id sequence.
    ``negative``: answer absent (or polar): all pads, gate 0. ``skip``: the
    answer is present but unusable (span longer than the record, normalization
    mismatch after tokenization, a DROP number) or degenerate.
    """
    norm = normalize_answer(str(answer))
    if not norm:
        return closed_record(SKIP, "empty-answer", pad_id, slots)
    if norm in POLAR_ANSWERS:
        return closed_record(NEGATIVE, "polar", pad_id, slots)
    if len(norm) < MIN_ANSWER_CHARS:
        return closed_record(SKIP, "short-answer", pad_id, slots)
    if skip_numeric and NUMERIC.fullmatch(norm):
        return closed_record(SKIP, "numeric", pad_id, slots)
    ids, label_ids = list(layout["ids"]), [int(i) for i in label_ids]
    failures = Counter()
    for doc_index, text in enumerate(layout["texts"]):
        tokens = [
            (i, a, b) for i, (d, a, b) in enumerate(layout["offsets"]) if d == doc_index
        ]
        for start, end in answer_char_ranges(text, answer):
            cover = covering_tokens([(a, b) for _, a, b in tokens], start, end)
            if cover is None:
                failures["no-token"] += 1
                continue
            first, last = tokens[cover[0]][0], tokens[cover[1] - 1][0] + 1
            first, last = word_complete(ids, first, last, continues)
            span = ids[first:last]
            if normalize_answer(decode(span)) != norm:
                failures["mismatch"] += 1
                continue
            space = [] if decode(span[:1]).startswith(" ") else [int(pad_id)]
            record = label_ids + space + span
            if len(record) > slots:
                failures["long-span"] += 1
                continue
            offset = len(label_ids) + len(space)
            starts = span_occurrences(ids, span)
            gate = [0] * slots
            for k in range(len(span)):
                gate[offset + k] = 1
            return {
                "kind": POSITIVE,
                "reason": "found",
                "target_ids": record + [int(pad_id)] * (slots - len(record)),
                "gate": gate,
                "pointer": [
                    [offset + k, [s + k for s in starts]] for k in range(len(span))
                ],
                "span_text": decode(span),
                "span_source": [first, last],
                "span_occurrences": len(starts),
                "document": doc_index,
            }
    if failures:
        return closed_record(SKIP, failures.most_common(1)[0][0], pad_id, slots)
    return closed_record(NEGATIVE, "absent", pad_id, slots)


def _token_counts(tokenizer, texts: list[str]) -> list[int]:
    """Token counts without specials (one batched call on a real tokenizer)."""
    if not texts:
        return []
    if hasattr(tokenizer, "batch_encode_plus"):
        return [len(ids) for ids in tokenizer(list(texts), add_special_tokens=False)["input_ids"]]
    return [len(tokenizer(t, add_special_tokens=False)["input_ids"]) for t in texts]


def _split_long(text: str, tokenizer, max_tokens: int) -> list[str]:
    """rag256 ``split_long``: pieces of one over-long chunk, each at most
    ``max_tokens`` tokens, cut at word starts."""
    offsets = tokenizer(text, add_special_tokens=False, return_offsets_mapping=True)["offset_mapping"]
    pieces, start_tok = [], 0
    while start_tok < len(offsets):
        end_tok = min(start_tok + max_tokens, len(offsets))
        if end_tok < len(offsets):
            cut = end_tok
            while (
                cut > start_tok + 1
                and not text[offsets[cut][0] - 1 : offsets[cut][0]].isspace()
                and not text[offsets[cut][0] : offsets[cut][0] + 1].isspace()
            ):
                cut -= 1
            end_tok = cut if cut > start_tok + 1 else end_tok
        lo, hi = offsets[start_tok][0], offsets[end_tok - 1][1]
        piece = text[lo:hi].strip()
        if piece:
            pieces.append(piece)
        start_tok = end_tok
    return pieces


def pack_chunks(chunks, tokenizer, max_tokens: int = PACK_TOKENS):
    """The rag256 packing of one collection (``rag256/rechunk.py pack``):
    chunks stripped and in source order, a chunk over ``max_tokens`` split at
    word starts, packed greedily with newline joins to at most ``max_tokens``
    tokens, and a joined pack that tokenizes longer re-split at its middle
    line -> ``(packs, chunk_pack)`` where ``chunk_pack[i]`` is the pack that
    holds chunk ``i``'s first line (``None`` for an empty chunk)."""
    units, owners = [], []
    stripped = [str(c).strip() for c in chunks]
    counts = _token_counts(tokenizer, [c for c in stripped if c])
    count_of = iter(counts)
    unit_counts = []
    for index, chunk in enumerate(stripped):
        if not chunk:
            continue
        n = next(count_of)
        if n <= max_tokens:
            units.append(chunk)
            unit_counts.append(n)
            owners.append(index)
        else:
            pieces = _split_long(chunk, tokenizer, max_tokens)
            units += pieces
            unit_counts += _token_counts(tokenizer, pieces)
            owners += [index] * len(pieces)
    out, current, used = [], [], 0
    for unit, n in zip(units, unit_counts, strict=True):
        if current and used + 1 + n > max_tokens:
            out.append("\n".join(current))
            current, used = [], 0
        current.append(unit)
        used += n + (1 if len(current) > 1 else 0)
    if current:
        out.append("\n".join(current))
    packs = []
    for text, n in zip(out, _token_counts(tokenizer, out), strict=True):
        if n <= max_tokens:
            packs.append(text)
            continue
        lines = text.split("\n")
        half = max(1, len(lines) // 2)
        packs += ["\n".join(lines[:half]), "\n".join(lines[half:])]
    packs = [t for t in packs if t]
    chunk_pack = [None] * len(stripped)
    pack, offset = 0, 0
    for unit, owner in zip(units, owners, strict=True):
        line = unit.split("\n", 1)[0]
        while pack < len(packs):
            found = packs[pack].find(unit, offset)
            if found >= 0:
                offset = found + len(unit)
                break
            # A unit with inner newlines can straddle a middle-line re-split:
            # it starts here (its first line) and continues in the next pack.
            found = packs[pack].find(line, offset)
            if found >= 0:
                offset = len(packs[pack])
                break
            pack, offset = pack + 1, 0
        else:
            raise ValueError("a packed unit is missing from the packs")
        if chunk_pack[owner] is None:
            chunk_pack[owner] = pack
    return packs, chunk_pack


def select_top_packs(chunks, vectors, question, packing, top_k: int = TOP_K):
    """The 256-token-passage variant of ``select_top_chunks``: chunks ranked by
    the same stored cosine, each mapped to the pack that holds it; the first
    ``top_k`` distinct packs in that order (a pack already taken passes its
    place to the next-best chunk's pack) -> (texts, pack indices, scores of
    the selecting chunks, selecting chunk indices)."""
    packs, chunk_pack = packing
    if len(chunks) != vectors.shape[0] or not chunks or len(chunk_pack) != len(chunks):
        raise ValueError("chunk texts, packing and stored vectors disagree")
    order, scores = retrieval_order(vectors, question, len(chunks))
    chosen, chosen_scores, chosen_chunks = [], [], []
    for index, score in zip(order, scores, strict=True):
        pack = chunk_pack[index]
        if pack is None or pack in chosen:
            continue
        chosen.append(pack)
        chosen_scores.append(score)
        chosen_chunks.append(index)
        if len(chosen) == top_k:
            break
    return [packs[p] for p in chosen], chosen, chosen_scores, chosen_chunks


def select_read_set(chunks, vectors, question, count_tokens, budget_tokens: int, cap: int = READ_CAP):
    """Budget study C: the read set the eval's ``--record-read-tokens`` reads
    from a case's top list: chunks ranked by the stored cosine (at most
    ``cap``), read in that order while their token total stays within
    ``budget_tokens`` (the first always) -> (texts, indices, scores)."""
    from qwen_span_copy import read_budget_documents

    if len(chunks) != vectors.shape[0] or not chunks:
        raise ValueError("chunk texts and stored vectors disagree")
    indices, scores = retrieval_order(vectors, question, min(cap, len(chunks)))
    texts = read_budget_documents(count_tokens, [chunks[i] for i in indices], budget_tokens)
    count = len(texts)
    return texts, list(indices[:count]), list(scores[:count])


def select_top_chunks(chunks, vectors, question, top_k: int = TOP_K):
    """The hybrid's record input rule: the ``top_k`` chunks by cosine between
    the stored chunk vectors and the stored question vector, most similar
    first -> (texts, indices, scores)."""
    if len(chunks) != vectors.shape[0] or not chunks:
        raise ValueError("chunk texts and stored vectors disagree")
    indices, scores = retrieval_order(vectors, question, top_k)
    return [chunks[i] for i in indices], indices, scores


def check_record(record: dict, ids, slots: int = SLOTS) -> None:
    """A stored record agrees with its case's copy-source ids."""
    if len(record["target_ids"]) != slots or len(record["gate"]) != slots:
        raise ValueError("record does not fill the slots")
    for slot, positions in record["pointer"]:
        if not record["gate"][slot] or not positions:
            raise ValueError("pointer slot without gate or positions")
        for position in positions:
            if not 0 <= position < len(ids) or ids[position] != record["target_ids"][slot]:
                raise ValueError("pointer position does not hold the target id")
    if sum(record["gate"]) != len(record["pointer"]):
        raise ValueError("gate slots and pointer slots differ")


def heldout(name: str, modulus: int = HELDOUT_MODULUS) -> bool:
    """Stable held-out membership (about 1/modulus of the examples)."""
    value = int(hashlib.sha256(name.encode()).hexdigest()[:8], 16)
    return value % modulus == 0


def answer_in_documents(answer, documents) -> bool:
    """SQuAD-normalized gold answer contained in any document (the copy
    ceiling definition of plan section 8.8)."""
    norm = normalize_answer(str(answer))
    if not norm:
        return False
    return any(norm in normalize_answer(d) for d in documents)


# ----------------------------------------------------------------------------
# statistics


def paired_bootstrap(
    left, right, resamples: int = 10_000, seed: int = 23, confidence: float = 0.95
) -> dict:
    """Mean paired difference ``right - left`` over questions with a
    percentile bootstrap interval (questions resampled with replacement)."""
    left, right = [float(v) for v in left], [float(v) for v in right]
    if len(left) != len(right) or not left:
        raise ValueError("paired bootstrap needs two equal nonempty score lists")
    if resamples < 1 or not 0 < confidence < 1:
        raise ValueError("bootstrap needs resamples >= 1 and 0 < confidence < 1")
    diffs = torch.tensor([r - lft for lft, r in zip(left, right)], dtype=torch.float64)
    count = diffs.numel()
    generator = torch.Generator().manual_seed(int(seed))
    means, chunk = [], max(1, 4_000_000 // count)
    for begin in range(0, resamples, chunk):
        size = min(chunk, resamples - begin)
        index = torch.randint(count, (size, count), generator=generator)
        means.append(diffs[index].mean(dim=1))
    means = torch.cat(means).sort().values
    tail = (1 - confidence) / 2
    low = float(means[int(math.floor(tail * resamples))])
    high = float(means[min(resamples - 1, int(math.ceil((1 - tail) * resamples)) - 1)])
    return {
        "n": count,
        "left_mean": sum(left) / count,
        "right_mean": sum(right) / count,
        "mean_delta": float(diffs.mean()),
        "ci_low": low,
        "ci_high": high,
        "confidence": confidence,
        "resamples": resamples,
        "seed": int(seed),
        "excludes_zero": low > 0 or high < 0,
        "right_better": int((diffs > 0).sum()),
        "left_better": int((diffs < 0).sum()),
    }


def gate_confusion(rows) -> dict:
    """Learned gate against the record label: ``rows`` of (label kind, open)."""
    tp = sum(k == POSITIVE and o for k, o in rows)
    fp = sum(k == NEGATIVE and o for k, o in rows)
    fn = sum(k == POSITIVE and not o for k, o in rows)
    tn = sum(k == NEGATIVE and not o for k, o in rows)
    total = tp + fp + fn + tn
    return {
        "positives": tp + fn,
        "negatives": fp + tn,
        "open_rate": (tp + fp) / total if total else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "false_open_rate": fp / (fp + tn) if fp + tn else None,
        "precision": tp / (tp + fp) if tp + fp else None,
        "accuracy": (tp + tn) / total if total else None,
    }


# ----------------------------------------------------------------------------
# prep (CPU)


def split_files(data_root: Path, config: str, split: str) -> list[Path]:
    return sorted((Path(data_root) / "data" / config).glob(f"{split}-*.parquet"))


def split_total(files) -> int:
    import pyarrow.parquet as pq

    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def iterate_rows(files, rows, columns):
    """Yield ``(global row, record)`` for the wanted rows, one row group at a
    time (long-story splits never sit in memory at once)."""
    import pyarrow.parquet as pq

    wanted = sorted(set(int(r) for r in rows))
    offset, cursor = 0, 0
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
                    yield offset + index, {
                        c: table.column(c)[index].as_py() for c in columns
                    }
                del table
            offset += count
    if cursor != len(wanted):
        raise IndexError("requested rows beyond the split")


def parse_samples(text: str) -> dict[str, int]:
    """``config=count,...`` (0 = every row)."""
    samples = {}
    for item in [t.strip() for t in text.split(",") if t.strip()]:
        name, sep, count = item.partition("=")
        if not sep or name not in QA_CONFIGS:
            raise ValueError("samples are config=count with a known QA config")
        samples[name] = int(count)
    return samples


def resume_partial(path: Path) -> set[str]:
    """Example names already written to a prep ``.partial`` file (``--resume``);
    a torn last line (the writer was killed mid-row) is cut off first."""
    data = Path(path).read_bytes()
    keep = data[: data.rfind(b"\n") + 1] if b"\n" in data else b""
    names = set()
    good = 0
    for line in keep.splitlines(keepends=True):
        try:
            names.add(json.loads(line)["name"])
        except (ValueError, KeyError):
            break
        good += len(line)
    if good != len(data):
        Path(path).write_bytes(data[:good])
    return names


def prep_config(args, config: str, count: int, tokenizer) -> dict:
    """Examples of one config -> ``<output>/<config>.jsonl`` + receipt."""
    files = split_files(args.data_root, config, QA_SPLIT)
    if not files:
        raise FileNotFoundError(f"no {QA_SPLIT} parquet for {config}")
    total = split_total(files)
    store = StoredEmbeddings(embedding_directory(args.data_root, config, QA_SPLIT))
    if store.num_questions != total:
        raise ValueError(f"{config}: {total} rows but {store.num_questions} questions")
    rows = sample_rows(total, count, args.seed)
    label_ids = tokenizer.encode(args.label, add_special_tokens=False)
    pad = tokenizer.encode(" ", add_special_tokens=False)
    if len(pad) != 1 or not label_ids:
        raise ValueError("label must tokenize and the pad must be one token")
    pad_id = pad[0]
    from encoder_lora import token_continues_word

    continues = functools.lru_cache(maxsize=None)(
        lambda i: token_continues_word(tokenizer, i)
    )
    decode = tokenizer.decode
    path = args.output / f"{config}.jsonl"
    temporary = path.with_suffix(".partial")
    stats, reasons, span_lengths = Counter(), Counter(), Counter()
    cached = (None, None, None)
    chunking = getattr(args, "chunking", "none")
    if chunking not in CHUNKING_CHOICES:
        raise ValueError(f"unknown chunking {chunking}")
    read_tokens = int(getattr(args, "read_tokens", 0) or 0)
    if read_tokens < 0 or (read_tokens and chunking != "none"):
        raise ValueError("--read-tokens is positive and excludes --chunking")
    count_tokens = functools.lru_cache(maxsize=65536)(
        lambda text: len(tokenizer(text, add_special_tokens=False)["input_ids"])
    )
    done = (
        resume_partial(temporary)
        if getattr(args, "resume", False) and temporary.exists()
        else None
    )
    started = time.perf_counter()
    with temporary.open("a" if done is not None else "w") as handle:
        for row, item in iterate_rows(
            files, rows, ("collection_id", "question", "answer", "documents")
        ):
            stats["rows"] += 1
            chunks = item["documents"] or []
            collection = int(item["collection_id"])
            if done is not None and f"{config}-{row:06d}-{collection}" in done:
                stats["resumed"] += 1
                continue
            if cached[0] != collection:
                cached = (collection, store.documents(collection), None)
            vectors = cached[1]
            if len(chunks) != vectors.shape[0] or not chunks:
                stats["chunk-mismatch"] += 1
                continue
            extra = {}
            if chunking == "pack256":
                if cached[2] is None:
                    cached = (collection, vectors, pack_chunks(chunks, tokenizer))
                documents, indices, scores, selecting = select_top_packs(
                    chunks, vectors, store.question(row), cached[2], args.top_k
                )
                extra = {"chunking": chunking, "top_chunk_indices": selecting}
            elif read_tokens:
                documents, indices, scores = select_read_set(
                    chunks, vectors, store.question(row), count_tokens, read_tokens
                )
                extra = {"read_tokens": read_tokens, "read_documents": len(documents)}
            else:
                documents, indices, scores = select_top_chunks(
                    chunks, vectors, store.question(row), args.top_k
                )
            try:
                layout = document_layout(tokenizer, documents)
            except ValueError:
                stats["layout-error"] += 1
                continue
            longest = max(len(tokenizer(d).input_ids) for d in layout["doubled"])
            if longest > args.max_encoder_tokens:
                stats["long-input"] += 1
                continue
            record = qa_record(
                item["answer"],
                layout,
                decode=decode,
                continues=continues,
                label_ids=label_ids,
                pad_id=pad_id,
                slots=SLOTS,
                skip_numeric=config in SKIP_NUMERIC_CONFIGS,
            )
            reasons[f"{record['kind']}:{record['reason']}"] += 1
            if record["kind"] == SKIP:
                continue
            if record["kind"] == POSITIVE:
                span_lengths[len(record["pointer"])] += 1
            check_record(record, layout["ids"])
            name = f"{config}-{row:06d}-{collection}"
            example = {
                "name": name,
                "source": config,
                "split": QA_SPLIT,
                "row": row,
                "collection_id": collection,
                "question": item["question"],
                "answer": item["answer"],
                "documents": documents,
                "top_indices": indices,
                "top_scores": scores,
                "copy_len": len(layout["ids"]),
                "encoder_tokens": longest,
                "heldout": heldout(name),
                **extra,
                **record,
            }
            stats[record["kind"]] += 1
            handle.write(json.dumps(example, ensure_ascii=False) + "\n")
    temporary.replace(path)
    if done is not None:
        # Counts of the written rows come from the file; reasons and drops
        # cover only the last attempt.
        stats[POSITIVE] = stats[NEGATIVE] = 0
        span_lengths = Counter()
        for example in read_rows(path):
            stats[example["kind"]] += 1
            if example["kind"] == POSITIVE:
                span_lengths[len(example["pointer"])] += 1
    kept = stats[POSITIVE] + stats[NEGATIVE]
    receipt = {
        "schema": SCHEMA_EXAMPLES,
        "config": config,
        "split": QA_SPLIT,
        "split_rows": total,
        "sampled_rows": len(rows),
        "seed": args.seed,
        "top_k": args.top_k,
        "label": args.label,
        "label_ids": label_ids,
        "pad_id": pad_id,
        "examples": kept,
        "positives": stats[POSITIVE],
        "negatives": stats[NEGATIVE],
        "positive_rate": stats[POSITIVE] / kept if kept else None,
        "dropped": {k: v for k, v in stats.items() if k not in (POSITIVE, NEGATIVE, "rows")},
        "record_reasons": dict(reasons.most_common()),
        "span_tokens": dict(sorted(span_lengths.items())),
        "seconds": round(time.perf_counter() - started, 1),
        "file": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    if chunking != "none":
        receipt.update({"chunking": chunking, "pack_tokens": PACK_TOKENS, "pack_rule": PACK_RULE})
    if read_tokens:
        receipt.update({"read_tokens": read_tokens, "read_cap": READ_CAP, "read_rule": READ_RULE})
    if done is not None:
        receipt["resumed_rows"] = len(done)
    save_json(args.output / f"{config}.json", receipt)
    print(json.dumps({k: v for k, v in receipt.items() if k != "record_reasons"}), flush=True)
    return receipt


def prep(args) -> dict:
    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer, local_files_only=True)
    if not tokenizer.is_fast:
        raise ValueError("record offsets need a fast tokenizer")
    args.output.mkdir(parents=True, exist_ok=True)
    samples = parse_samples(args.samples)
    receipts = {}
    for config, count in samples.items():
        if (args.output / f"{config}.json").exists() and not args.force:
            receipts[config] = json.loads((args.output / f"{config}.json").read_text())
            continue
        receipts[config] = prep_config(args, config, count, tokenizer)
    return receipts


# ----------------------------------------------------------------------------
# GPU helpers shared by probe and train


def gpu_used_mib() -> int:
    output = subprocess.check_output(
        ["nvidia-smi", "--query-compute-apps=pid,used_memory", "--format=csv,noheader"],
        text=True,
    )
    return sum(
        int(line.split(",")[1].strip().split()[0])
        for line in output.splitlines()
        if line.strip()
    )


def wait_for_room(need_mib: int, label: str, poll: int = 30) -> int:
    """Block until the other compute processes plus ``need_mib`` stay under
    the 115 GiB co-tenant budget."""
    while True:
        used = gpu_used_mib()
        if used + need_mib < GPU_BUDGET_MIB:
            return used
        print(json.dumps({"event": "gpu-wait", "for": label, "used_mib": used}), flush=True)
        time.sleep(poll)


def asset_paths(args) -> dict:
    from encoder_lora_export import DEFAULT_ASSETS

    return {k: Path(args.assets) / v for k, v in DEFAULT_ASSETS.items()}


def load_embedding_table(decoder_dir: Path):
    """The frozen Qwen3-8B input-embedding table alone (BF16 on the GPU)."""
    from safetensors import safe_open
    from torch import nn

    decoder_dir = Path(decoder_dir)
    index = json.loads((decoder_dir / "model.safetensors.index.json").read_text())
    key = "model.embed_tokens.weight"
    with safe_open(str(decoder_dir / index["weight_map"][key]), framework="pt") as f:
        weight = f.get_tensor(key)
    if tuple(weight.shape) != (151936, 4096) or weight.dtype != torch.bfloat16:
        raise ValueError("unexpected decoder embedding table")
    return nn.Embedding.from_pretrained(weight, freeze=True).cuda()


class EmbeddingOnly:
    """Stands in for the decoder where only ``get_input_embeddings`` is used
    (copy sources, record rows, snapping)."""

    def __init__(self, embedding):
        self.embedding = embedding

    def get_input_embeddings(self):
        return self.embedding


def load_decoder(paths: dict):
    from transformers import AutoModelForCausalLM

    return (
        AutoModelForCausalLM.from_pretrained(
            paths["decoder"],
            local_files_only=True,
            torch_dtype=torch.bfloat16,
            attn_implementation="sdpa",
        )
        .requires_grad_(False)
        .cuda()
        .eval()
    )


def release(*objects) -> None:
    for item in objects:
        del item
    gc.collect()
    torch.cuda.empty_cache()


# ----------------------------------------------------------------------------
# probe: is "<label> <span>" readable by the frozen decoder under the merged
# prompt? (plain token rows, as the snapped record path delivers them)

PROBE_LABELS = (" answer", " Answer:", " the answer is", "")


def probe(args) -> dict:
    from encoder_lora_export import load_candidate, load_models
    from gpu_quality_reference import generate
    from decoder_reference import prepare_prompt
    from qwen_hybrid_eval import (
        SoftMemory,
        answer_with,
        compose_memory_rows,
        load_released_reader,
        memory_user_turn,
        merged_system_prompt,
        paper_protocol,
        squad_exact,
        system_prompt_text,
    )
    from encoder_lora import token_continues_word

    paths = asset_paths(args)
    _, protocol = load_candidate(args.candidate)
    cases = parquet_cases(Path(args.data_root) / "data", "squad", 64)
    wait_for_room(36 * 1024, "probe")
    models = load_models(args.candidate, protocol, paths)
    released = load_released_reader(paths, models["reader_config"])
    soft = SoftMemory(models, released)
    dec_tok = models["dec_tok"]
    pad_id = dec_tok.encode(" ", add_special_tokens=False)[0]
    continues = functools.lru_cache(maxsize=None)(
        lambda i: token_continues_word(dec_tok, i)
    )
    chosen = []
    for case in cases:
        layout = document_layout(models["enc_tok"], case["documents"])
        record = qa_record(
            case["answer"],
            layout,
            decode=dec_tok.decode,
            continues=continues,
            label_ids=[],
            pad_id=pad_id,
            slots=SLOTS - 4,
        )
        if record["kind"] == POSITIVE:
            span = [record["target_ids"][s] for s, _ in record["pointer"]]
            if record["target_ids"][0] == pad_id and span[0] != pad_id:
                span = [pad_id] + span
            chosen.append((case, span))
        if len(chosen) == args.probe_cases:
            break
    trivia = paper_protocol("triviaqa", _benchmarks_or_none())
    prompts = {
        "merged": (system_prompt_text("merged"), None),
        "triviaqa-merged": (merged_system_prompt(trivia["system"]), trivia["suffix"]),
    }
    embedding = models["decoder"].get_input_embeddings()
    rows = []
    for index, (case, span) in enumerate(chosen):
        soft_rows = soft.rows(case)
        other = chosen[(index + 1) % len(chosen)][1]
        variants = {f"label={label!r}": (label, span) for label in PROBE_LABELS}
        variants["wrong-span label=' answer'"] = (" answer", other)
        for name, (label, ids) in variants.items():
            label_ids = dec_tok.encode(label, add_special_tokens=False) if label else []
            record_ids = (label_ids + ids)[:SLOTS]
            record_ids += [pad_id] * (SLOTS - len(record_ids))
            with torch.inference_mode():
                record_rows = embedding.weight[
                    torch.tensor(record_ids, device="cuda")
                ].detach()
            memories = compose_memory_rows(soft_rows, record_rows)
            for prompt_name, (system, suffix) in prompts.items():
                question = (
                    case["question"]
                    if suffix is None
                    else memory_user_turn(case["question"], suffix)
                )
                if suffix is None:
                    output, _ = answer_with(
                        models,
                        memories,
                        system_prompt=system,
                        question=question,
                        max_new_tokens=48,
                    )
                else:
                    with torch.inference_mode(), torch.autocast(
                        "cuda", dtype=torch.bfloat16
                    ):
                        _, embeds, _ = prepare_prompt(
                            dec_tok,
                            embedding,
                            memories,
                            system_prompt=system,
                            question=question,
                            device="cuda",
                        )
                        output = generate(models["decoder"], dec_tok, embeds, 48)
                answer = output["answer"]
                span_text = dec_tok.decode(ids).strip()
                rows.append(
                    {
                        "case": case["name"],
                        "question": case["question"],
                        "gold": case["answer"],
                        "variant": name,
                        "prompt": prompt_name,
                        "record_text": dec_tok.decode(record_ids),
                        "answer": answer,
                        "em_gold": squad_exact(answer, [case["answer"]]),
                        "em_record_span": squad_exact(answer, [span_text]),
                    }
                )
                print(json.dumps(rows[-1], ensure_ascii=False), flush=True)
    for case, _ in chosen:
        output, _ = answer_with(
            models,
            soft.rows(case),
            system_prompt=system_prompt_text("helpful"),
            question=case["question"],
            max_new_tokens=48,
        )
        rows.append(
            {
                "case": case["name"],
                "variant": "soft-only",
                "prompt": "helpful",
                "answer": output["answer"],
                "em_gold": squad_exact(output["answer"], [case["answer"]]),
            }
        )
    summary = {}
    for row in rows:
        key = f"{row['variant']} | {row['prompt']}"
        bucket = summary.setdefault(key, {"cases": 0, "em_gold": 0, "em_record_span": 0})
        bucket["cases"] += 1
        bucket["em_gold"] += row["em_gold"]
        bucket["em_record_span"] += row.get("em_record_span", 0)
    result = {
        "schema": "antfly.memorilla.qwen-copy-qa-probe.v1",
        "candidate": str(args.candidate),
        "cases": [c["name"] for c, _ in chosen],
        "summary": summary,
        "rows": rows,
    }
    args.output.mkdir(parents=True, exist_ok=True)
    save_json(args.output / "probe.json", result)
    print(json.dumps(summary, indent=1), flush=True)
    return result


def _benchmarks_or_none():
    """Upstream ``memorilla.benchmarks`` when importable (PYTHONPATH has the
    pinned upstream checkout)."""
    from qwen_hybrid_eval import upstream_benchmarks

    return upstream_benchmarks()


# ----------------------------------------------------------------------------
# training


def load_examples(directory: Path, configs) -> tuple[list[dict], list[dict], dict]:
    """(training examples, held-out examples, receipts) of the prepared configs."""
    train, held, receipts = [], [], {}
    for config in configs:
        receipt = json.loads((directory / f"{config}.json").read_text())
        path = Path(receipt["file"])
        if hashlib.sha256(path.read_bytes()).hexdigest() != receipt["sha256"]:
            raise ValueError(f"prepared examples changed: {path}")
        receipts[config] = receipt
        for row in read_rows(path):
            (held if row["heldout"] else train).append(row)
    return train, held, receipts


def parse_weights(text: str) -> dict[str, float]:
    weights = {}
    for item in [t.strip() for t in text.split(",") if t.strip()]:
        name, sep, value = item.partition("=")
        if not sep or name not in QA_CONFIGS or float(value) < 0:
            raise ValueError("weights are config=value with a known QA config")
        weights[name] = float(value)
    return weights


def example_stream(
    qa_by_source: dict, depot_count: int, weights: dict, depot_fraction: float,
    length: int, seed: int, negative_cap: float | None = None,
) -> list[tuple[str, int]]:
    """Deterministic draw order: ``length`` entries of (source, index). Each
    entry is a depot example with probability ``depot_fraction``, else a QA
    config drawn by ``weights``; every source cycles through its own seeded
    shuffles, so resuming at any step replays the same order."""
    rng = random.Random(seed)
    sources = [s for s in qa_by_source if weights.get(s, 0) > 0 and qa_by_source[s]]
    if not sources:
        raise ValueError("no QA source has weight and examples")
    total = sum(weights[s] for s in sources)
    cumulative, running = [], 0.0
    for source in sources:
        running += weights[source] / total
        cumulative.append(running)
    orders, cursors, epochs = {}, Counter(), Counter()

    def next_index(source, size):
        if cursors[source] % size == 0:
            order = list(range(size))
            random.Random(f"{seed}:{source}:{epochs[source]}").shuffle(order)
            orders[source] = order
            epochs[source] += 1
        index = orders[source][cursors[source] % size]
        cursors[source] += 1
        return index

    stream = []
    for _ in range(length):
        if depot_count and rng.random() < depot_fraction:
            stream.append(("depot", next_index("depot", depot_count)))
            continue
        draw = rng.random()
        source = next(s for s, c in zip(sources, cumulative) if draw <= c) if draw <= cumulative[-1] else sources[-1]
        stream.append((source, next_index(source, len(qa_by_source[source]))))
    return stream


class Trainer:
    """copy17 continuation: encoder (+ frozen adapters), reader, copy head and
    the decoder embedding table on the GPU; no decoder forward."""

    def __init__(self, args):
        from transformers import AutoModel, AutoTokenizer

        from encoder_lora import (
            CopyHead,
            LexicalVocabularyTeacher,
            MemoryTokenSnapper,
            copy_excluded_ids,
            encoder_special_suffix,
            inject_lora,
            restore_adapters,
            check_dual_layer,
        )
        from encoder_lora_export import load_candidate
        from reference import load_upstream
        from released_reference import checkpoint_proof
        from safetensors.torch import load_file
        from torch import nn

        self.args = args
        self.paths = asset_paths(args)
        _, self.parent_protocol = load_candidate(args.init)
        torch.set_num_threads(4)
        torch.manual_seed(args.seed)
        torch.backends.cuda.matmul.allow_tf32 = False
        torch.backends.cudnn.allow_tf32 = False
        self.enc_tok = AutoTokenizer.from_pretrained(self.paths["encoder"], local_files_only=True)
        self.enc_tok.padding_side = "right"
        self.dec_tok = AutoTokenizer.from_pretrained(self.paths["decoder"], local_files_only=True)
        self.suffix = encoder_special_suffix(self.enc_tok)
        pad = self.dec_tok.encode(" ", add_special_tokens=False)
        if len(pad) != 1:
            raise ValueError("record pad must be one token")
        self.pad_id = pad[0]
        self.dual_layer = int(self.parent_protocol["dual_layer"])
        # ``--question-prefix``: the question precedes every self-context
        # document in the encoder input; recorded in the protocol so the
        # evaluator (``record_memory``) and exported candidates apply it too.
        self.question_prefix = (
            None if args.question_prefix == "none" else args.question_prefix
        )
        if self.question_prefix:
            self.parent_protocol = dict(
                self.parent_protocol, document_question_prefix=self.question_prefix
            )
        self.encoder = (
            AutoModel.from_pretrained(
                self.paths["encoder"],
                local_files_only=True,
                torch_dtype=torch.bfloat16,
                attn_implementation="sdpa",
            )
            .requires_grad_(False)
            .cuda()
            .eval()
        )
        adapters = inject_lora(
            self.encoder,
            self.parent_protocol["rank"],
            self.parent_protocol["alpha"],
            False,
            tuple(self.parent_protocol["target_modules"]),
        )
        restore_adapters(adapters, load_file(str(args.init / "encoder-lora.safetensors")))
        check_dual_layer(self.dual_layer, self.encoder.config.num_hidden_layers)
        self.config, _ = checkpoint_proof(self.paths["checkpoint"])
        self.reader = (
            load_upstream(
                self.paths["upstream_source"],
                dict(self.config, num_memories=SLOTS),
                load_file(str(args.init / "reader.safetensors")),
            )
            .float()
            .cuda()
            .requires_grad_(True)
        )
        for module in self.reader.modules():
            if isinstance(module, nn.Dropout):
                module.p = 0
            if isinstance(module, nn.MultiheadAttention):
                module.dropout = 0
        self.copy_head = CopyHead(4096, self.parent_protocol["copy_head_temperature"]).cuda()
        self.copy_head.load_state_dict(load_file(str(args.init / "copy-head.safetensors")))
        self.embedding = load_embedding_table(self.paths["decoder"])
        self.teacher = LexicalVocabularyTeacher(self.embedding)
        self.snapper = MemoryTokenSnapper(self.embedding)
        self.excluded = copy_excluded_ids(self.dec_tok)
        projection = list(self.reader.memory_projection.parameters())
        projection_ids = {id(p) for p in projection}
        self.optimizer = torch.optim.AdamW(
            [
                {
                    "params": [p for p in self.reader.parameters() if id(p) not in projection_ids],
                    "lr": args.reader_lr,
                    "base_lr": args.reader_lr,
                },
                {"params": projection, "lr": args.reader_lr, "base_lr": args.reader_lr},
                {
                    "params": list(self.copy_head.parameters()),
                    "lr": args.copy_head_lr,
                    "base_lr": args.copy_head_lr,
                },
            ],
            weight_decay=0,
        )
        self.trainable = [p for g in self.optimizer.param_groups for p in g["params"]]
        self.models = {
            "enc_tok": self.enc_tok,
            "dec_tok": self.dec_tok,
            "encoder": self.encoder,
            "reader": self.reader,
            "copy_head": self.copy_head,
            "snapper": self.snapper,
            "decoder": EmbeddingOnly(self.embedding),
            "reader_config": self.config,
        }

    # ---------------------------------------------------------------- inputs

    def layout(self, example: dict) -> dict:
        layout = document_layout(self.enc_tok, example["documents"])
        if example.get("source") != "depot" and len(layout["ids"]) != example["copy_len"]:
            raise ValueError(f"copy ids changed since prep: {example['name']}")
        return layout

    def depot_target(self, case: dict, layout: dict):
        from encoder_lora import (
            copyable_mask,
            lexical_record_text,
            lexical_slot_weights,
            lexical_target_ids,
        )

        text = lexical_record_text(case, DEPOT_RECORD)
        target = lexical_target_ids(self.dec_tok, text, True, SLOTS)
        weights = lexical_slot_weights(
            self.dec_tok, case, DEPOT_RECORD, SLOTS, DEPOT_NAME_WEIGHT, True
        )
        ids = torch.tensor(layout["ids"], dtype=torch.long)
        copyable, match = copyable_mask(
            torch.tensor([target]),
            ids[None],
            torch.ones((1, len(ids)), dtype=torch.bool),
            self.excluded,
        )
        return target, weights, copyable[0], match[0]

    def qa_target(self, example: dict, layout: dict):
        check_record(example, layout["ids"])
        size = len(layout["ids"])
        match = torch.zeros((SLOTS, size), dtype=torch.bool)
        for slot, positions in example["pointer"]:
            match[slot, positions] = True
        copyable = torch.tensor([bool(g) for g in example["gate"]])
        return list(example["target_ids"]), torch.ones(SLOTS), copyable, match

    @torch.no_grad()
    def features(self, layouts, questions):
        """Batched tokens-dual features (the evaluator's per-case path, with the
        documents and the questions in two padded encoder passes) -> (docs [B,
        M, 2560], key padding mask [B, M], queries [B, 2560])."""
        from encoder_lora import (
            document_features_dual,
            encoder_states_pair,
            pad_document_batch,
            self_context_states,
        )

        texts = [
            t
            for layout, question in zip(layouts, questions, strict=True)
            for t in question_prefixed_documents(
                self.enc_tok,
                layout["doubled"],
                layout["counts"],
                question,
                self.question_prefix,
            )
        ]
        with torch.autocast("cuda", dtype=torch.bfloat16):
            doc_tokens = self.enc_tok(texts, padding=True, return_tensors="pt").to("cuda")
            if doc_tokens.input_ids.shape[1] > self.args.max_encoder_tokens:
                raise ValueError("document batch exceeds the encoder bound")
            doc_final, doc_layer = encoder_states_pair(self.encoder, doc_tokens, self.dual_layer)
            q_tokens = self.enc_tok(list(questions), padding=True, return_tensors="pt").to("cuda")
            q_final, q_layer = encoder_states_pair(self.encoder, q_tokens, self.dual_layer)
            width = max(doc_final.shape[1], q_final.shape[1])

            def padded(tensor):
                return torch.nn.functional.pad(tensor, (0, 0, 0, width - tensor.shape[1]))

            def padded_mask(mask):
                return torch.nn.functional.pad(mask, (0, width - mask.shape[1]))

            docs, queries, offset = [], [], 0
            for index, layout in enumerate(layouts):
                count = len(layout["doubled"])
                final = torch.cat(
                    [padded(doc_final[offset : offset + count]), padded(q_final[index : index + 1])]
                )
                layer = torch.cat(
                    [padded(doc_layer[offset : offset + count]), padded(q_layer[index : index + 1])]
                )
                mask = torch.cat(
                    [
                        padded_mask(doc_tokens.attention_mask[offset : offset + count]),
                        padded_mask(q_tokens.attention_mask[index : index + 1]),
                    ]
                )
                states, kept = self_context_states((final, layer), mask, layout["counts"], self.suffix)
                d, q = document_features_dual(states[0], states[1], kept)
                docs.append(d)
                queries.append(q)
                offset += count
            batch, key_mask = pad_document_batch(docs)
        return batch, key_mask, torch.cat(queries, dim=0)

    def micro_loss(self, examples):
        """Loss of one micro-batch -> (loss tensor, statistics)."""
        from encoder_lora import (
            lexical_token_ce_loss,
            pad_token_embeddings,
            pointer_losses,
            token_ids_embeddings,
        )

        layouts = [self.layout(e) for e in examples]
        docs, key_mask, queries = self.features(layouts, [e["question"] for e in examples])
        with torch.autocast("cuda", dtype=torch.bfloat16):
            memories = self.reader(docs, key_mask, queries)
        sources = pad_token_embeddings(
            [
                (
                    token_ids_embeddings(
                        self.embedding, torch.tensor(layout["ids"], device="cuda")
                    ),
                    torch.tensor(layout["ids"], dtype=torch.long),
                )
                for layout in layouts
            ]
        )
        embeddings, _, source_mask = sources
        output, copy_logits, gate_logits = self.copy_head.forward_detailed(
            memories, embeddings, source_mask
        )
        targets, weights, copyables, matches = [], [], [], []
        width = source_mask.shape[1]
        for example, layout in zip(examples, layouts, strict=True):
            if example["source"] == "depot":
                target, weight, copyable, match = self.depot_target(example, layout)
            else:
                target, weight, copyable, match = self.qa_target(example, layout)
            targets.append(target)
            weights.append(weight)
            copyables.append(copyable)
            matches.append(torch.nn.functional.pad(match, (0, width - match.shape[1])))
        target_ids = torch.tensor(targets, device="cuda")
        weight = torch.stack(weights).cuda()
        copyable = torch.stack(copyables).cuda()
        match = torch.stack(matches).cuda()
        lexical = lexical_token_ce_loss(output, target_ids, self.teacher, SLOTS, weight)
        pointer, gate = pointer_losses(copy_logits, gate_logits, match, copyable, source_mask)
        loss = self.args.lexical_weight * lexical + self.args.pointer_weight * (
            pointer + self.args.gate_weight * gate
        )
        if not torch.isfinite(loss):
            raise RuntimeError("nonfinite training loss")
        probabilities = torch.sigmoid(gate_logits.detach().float())
        opened = probabilities.max(dim=1).values >= LEARNED_GATE_THRESHOLD
        labels = copyable.any(dim=1)
        stats = {
            "lexical": float(lexical.detach()),
            "pointer": float(pointer.detach()),
            "gate": float(gate.detach()),
            "gate_mean": float(probabilities.mean()),
            "open_correct": float((opened == labels).float().mean()),
            "positives": int(sum(e.get("kind") == POSITIVE for e in examples)),
            "negatives": int(sum(e.get("kind") == NEGATIVE for e in examples)),
            "depot": int(sum(e["source"] == "depot" for e in examples)),
            "doc_rows": int(docs.shape[1]),
        }
        return loss, stats

    # ---------------------------------------------------------------- state

    def state(self, step: int, result: dict) -> dict:
        return {
            "step": step,
            "reader": {k: v.detach().cpu() for k, v in self.reader.state_dict().items()},
            "copy_head": {k: v.detach().cpu() for k, v in self.copy_head.state_dict().items()},
            "optimizer": self.optimizer.state_dict(),
            "cpu_rng": torch.get_rng_state(),
            "cuda_rng": torch.cuda.get_rng_state_all(),
            "result": result,
            "contract": self.args.contract,
        }

    def restore(self, state: dict) -> None:
        if state["contract"] != self.args.contract:
            raise ValueError("resume contract differs; choose a new output")
        self.reader.load_state_dict(state["reader"])
        self.copy_head.load_state_dict(state["copy_head"])
        self.optimizer.load_state_dict(state["optimizer"])
        torch.set_rng_state(state["cpu_rng"])
        torch.cuda.set_rng_state_all(state["cuda_rng"])

    def export(self, directory: Path, step: int, result: dict) -> Path:
        """A copy17-format candidate (``load_candidate`` / ``load_models`` /
        ``qwen_hybrid_eval.py --candidate``) of the current weights."""
        import shutil

        from encoder_lora import SCHEMA, candidate_artifacts, digest
        from safetensors.torch import save_file

        directory.mkdir(parents=True, exist_ok=True)
        save_file(
            {k: v.detach().cpu().contiguous() for k, v in self.reader.state_dict().items()},
            str(directory / "reader.safetensors"),
        )
        save_file(
            {k: v.detach().cpu().contiguous() for k, v in self.copy_head.state_dict().items()},
            str(directory / "copy-head.safetensors"),
        )
        for name in ("encoder-lora.safetensors", "base-proofs.json"):
            shutil.copyfile(self.args.init / name, directory / name)
        protocol = dict(self.parent_protocol)
        protocol.update(
            {
                "steps": step,
                "reader_lr": self.args.reader_lr,
                "copy_head_lr": self.args.copy_head_lr,
                "copy_gate_weight": self.args.gate_weight,
                "copy_pointer_weight": self.args.pointer_weight,
                "lexical_warmup_steps": step,
                "lexical_warmup_train_encoder": False,
                "copy_qa": {
                    "schema": SCHEMA_TRAIN,
                    "parent": str(self.args.init.resolve()),
                    "parent_candidate_sha256": digest(self.args.init / "candidate.json"),
                    "label": self.args.contract["label"],
                    "record": "label + exact source span, or 16 pads with gate 0",
                    "top_k": TOP_K,
                    "examples": str(self.args.examples),
                    "configs": self.args.contract["weights"],
                    "depot_fraction": self.args.depot_fraction,
                    "depot_data": str(self.args.depot_data),
                    "micro_batch": self.args.micro_batch,
                    "accum": self.args.accum,
                    "planned_steps": self.args.steps,
                    "schedule": "cosine",
                    "learned_gate_threshold": LEARNED_GATE_THRESHOLD,
                },
            }
        )
        save_json(directory / "protocol.json", protocol)
        save_json(
            directory / "result.json",
            {
                "completed": True,
                "rollout_qualified": False,
                "step": step,
                "history_tail": result["history"][-8:],
                "evaluations": result["evaluations"],
            },
        )
        candidate = {
            "schema": SCHEMA,
            "rollout_qualified": False,
            "experimental_only": True,
            "memory_slots": SLOTS,
        }
        for field, file in candidate_artifacts(protocol):
            candidate[field] = digest(directory / file)
        save_json(directory / "candidate.json", candidate)
        return directory


def lr_scale(step: int, total: int, ramp: int = 100) -> float:
    """``warmup_lr_scale`` cosine: linear ramp then cosine decay to 1%."""
    from encoder_lora import warmup_lr_scale

    return warmup_lr_scale(step, total, "cosine", ramp)


# ------------------------------------------------------------------ evaluation


class Evaluator:
    """Periodic evaluation: depot dev record arm, squad validation 200 record
    span recovery, the held-out gate set (no decoder) and, when the decoder
    fits on the GPU, the hybrid answer on a 64-case squad slice plus the depot
    dev decoder answers."""

    def __init__(self, trainer: Trainer, held: list[dict], args):
        from encoder_lora import load_split
        from qwen_hybrid_eval import load_released_reader

        self.trainer, self.args = trainer, args
        self.depot_dev = load_split(args.depot_data, "dev")
        self.squad = parquet_cases(Path(args.data_root) / "data", "squad", 200)
        rng = random.Random(args.seed)
        positives = [h for h in held if h["kind"] == POSITIVE]
        negatives = [h for h in held if h["kind"] == NEGATIVE]
        rng.shuffle(positives)
        rng.shuffle(negatives)
        half = args.heldout_eval // 2
        self.held = positives[:half] + negatives[: args.heldout_eval - min(half, len(positives))]
        self.released = load_released_reader(trainer.paths, trainer.config)
        soft_path = Path(args.reference_rows) / "soft-squad.jsonl"
        self.soft_squad = {r["name"]: r for r in read_rows(soft_path)} if soft_path.exists() else {}
        self.squad_targets = {}
        from encoder_lora import token_continues_word

        continues = functools.lru_cache(maxsize=None)(
            lambda i: token_continues_word(trainer.dec_tok, i)
        )
        label_ids = trainer.dec_tok.encode(args.contract["label"], add_special_tokens=False)
        for case in self.squad:
            layout = document_layout(trainer.enc_tok, case["documents"])
            self.squad_targets[case["name"]] = qa_record(
                case["answer"],
                layout,
                decode=trainer.dec_tok.decode,
                continues=continues,
                label_ids=label_ids,
                pad_id=trainer.pad_id,
            )

    def record(self, case):
        from qwen_hybrid_eval import record_memory

        return record_memory(
            self.trainer.models,
            self.trainer.parent_protocol,
            {"documents": case["documents"], "question": case["question"]},
            suffix=self.trainer.suffix,
            pad_id=self.trainer.pad_id,
            max_length=2048,
        )

    def span_text(self, info) -> str:
        from qwen_hybrid_eval import span_record_ids

        if not info["spans"]:
            return ""
        ids, _ = span_record_ids(
            [tuple(s) for s in info["spans"]],
            info["copy_ids"],
            self.trainer.dec_tok,
            self.trainer.pad_id,
        )
        return self.trainer.dec_tok.decode([i for i in ids if i != self.trainer.pad_id]).strip()

    def run(self, step: int) -> dict:
        from encoder_lora import lexical_record_text, lexical_target_ids
        from qwen_hybrid_eval import (
            EXACT_PROMPT,
            SoftMemory,
            answer_with,
            canonicalize_answer,
            compose_memory_rows,
            qa_metrics,
            system_prompt_text,
        )

        trainer = self.trainer
        trainer.reader.eval()
        trainer.copy_head.eval()
        started = time.perf_counter()
        result = {"step": step}
        decoder = None
        used = gpu_used_mib()
        if used + self.args.decoder_need_mib < GPU_BUDGET_MIB:
            decoder = load_decoder(trainer.paths)
            if not torch.equal(decoder.get_input_embeddings().weight, trainer.embedding.weight):
                raise ValueError("decoder embedding table differs from the training table")
            trainer.models["decoder"] = decoder
        else:
            result["decoder_skipped_used_mib"] = used
        try:
            # depot dev: record text and (with the decoder) the record-arm answer
            depot = Counter()
            for case in self.depot_dev:
                rows, info = self.record(case)
                expected = lexical_target_ids(
                    trainer.dec_tok, lexical_record_text(case, DEPOT_RECORD), True, SLOTS
                )
                depot["cases"] += 1
                depot["record_exact"] += info["record_ids"] == expected
                depot["gate_open"] += learned_gate_open(info["spans"], info["gate"])
                if decoder is not None:
                    output, _ = answer_with(
                        trainer.models,
                        rows,
                        system_prompt=EXACT_PROMPT,
                        question=case["question"],
                        max_new_tokens=24,
                    )
                    depot["answer_exact"] += output["answer"].strip() == case["answer"]
            result["depot_dev"] = dict(depot)
            # squad validation 200: span recovery and gate rate
            squad, examples = Counter(), []
            for case in self.squad:
                _, info = self.record(case)
                target = self.squad_targets[case["name"]]
                opened = learned_gate_open(info["spans"], info["gate"])
                text = self.span_text(info) if opened else ""
                gold = normalize_answer(case["answer"])
                squad["cases"] += 1
                squad["answer_in_documents"] += target["kind"] == POSITIVE
                squad["gate_open"] += opened
                squad["span_exact"] += opened and normalize_answer(text) == gold
                squad["span_contains"] += opened and bool(gold) and gold in normalize_answer(text)
                squad["record_exact"] += info["record_ids"] == target["target_ids"]
                if len(examples) < 6:
                    examples.append(
                        {"q": case["question"], "gold": case["answer"], "record": info["record_text"], "open": opened}
                    )
            result["squad"] = dict(squad)
            result["squad_examples"] = examples
            # held-out training-distribution examples: gate against the label
            confusion, held = [], Counter()
            for example in self.held:
                _, info = self.record(example)
                opened = learned_gate_open(info["spans"], info["gate"])
                confusion.append((example["kind"], opened))
                if example["kind"] == POSITIVE:
                    held["positives"] += 1
                    span = normalize_answer(self.span_text(info)) if opened else ""
                    held["span_exact"] += span == normalize_answer(example["answer"])
                    held["record_exact"] += info["record_ids"] == example["target_ids"]
            result["heldout"] = {**gate_confusion(confusion), **dict(held)}
            # hybrid answer on the squad slice (decoder only)
            if decoder is not None:
                soft = SoftMemory(trainer.models, self.released)
                hybrid = Counter()
                for case in self.squad[: self.args.hybrid_eval]:
                    soft_rows = soft.rows(case)
                    rows, info = self.record(case)
                    opened = learned_gate_open(info["spans"], info["gate"])
                    if opened:
                        memories = compose_memory_rows(soft_rows, rows)
                        system = system_prompt_text("merged")
                    else:
                        memories, system = soft_rows, system_prompt_text("helpful")
                    output, _ = answer_with(
                        trainer.models,
                        memories,
                        system_prompt=system,
                        question=case["question"],
                        max_new_tokens=48,
                    )
                    answer = output["answer"]
                    if opened:
                        answer = canonicalize_answer(answer, info["record_text"])
                    metrics = qa_metrics(answer, case["answer"], False)
                    hybrid["cases"] += 1
                    hybrid["gate_open"] += opened
                    hybrid["em"] += metrics["em"]
                    hybrid["f1"] += metrics["f1"]
                    reference = self.soft_squad.get(case["name"])
                    if reference is not None:
                        hybrid["soft_em"] += reference["metrics"]["em"]
                        hybrid["soft_f1"] += reference["metrics"]["f1"]
                    soft.cache.clear()
                result["hybrid_squad"] = dict(hybrid)
        finally:
            trainer.models["decoder"] = EmbeddingOnly(trainer.embedding)
            if decoder is not None:
                del decoder
            gc.collect()
            torch.cuda.empty_cache()
            trainer.reader.train()
            trainer.copy_head.train()
        result["seconds"] = round(time.perf_counter() - started, 1)
        return result


def train(args) -> dict:
    from encoder_lora import atomic_checkpoint, load_split

    args.output.mkdir(parents=True, exist_ok=True)
    weights = parse_weights(args.weights)
    train_rows, held_rows, receipts = load_examples(args.examples, weights)
    qa_by_source = {}
    for row in train_rows:
        qa_by_source.setdefault(row["source"], []).append(row)
    depot = [dict(case, source="depot") for case in load_split(args.depot_data, "train")]
    total = args.steps * args.accum * args.micro_batch
    stream = example_stream(
        qa_by_source, len(depot), weights, args.depot_fraction, total, args.seed
    )
    args.contract = {
        "init": str(args.init.resolve()),
        "examples": {c: r["sha256"] for c, r in receipts.items()},
        "label": receipts[next(iter(receipts))]["label"],
        "weights": weights,
        "depot_fraction": args.depot_fraction,
        "depot_data": str(args.depot_data.resolve()),
        "steps": args.steps,
        "accum": args.accum,
        "micro_batch": args.micro_batch,
        "reader_lr": args.reader_lr,
        "copy_head_lr": args.copy_head_lr,
        "gate_weight": args.gate_weight,
        "pointer_weight": args.pointer_weight,
        "lexical_weight": args.lexical_weight,
        **(
            {"question_prefix": args.question_prefix}
            if args.question_prefix != "none"
            else {}
        ),
        "seed": args.seed,
    }
    if len({r["label"] for r in receipts.values()}) != 1:
        raise ValueError("prepared configs disagree on the record label")
    resume_path = args.output / "resume.pt"
    if resume_path.exists() and not args.resume:
        raise FileExistsError(f"{resume_path} exists; pass --resume or a new output")
    used = wait_for_room(args.train_need_mib, "train")
    trainer = Trainer(args)
    result = {
        "schema": SCHEMA_TRAIN,
        "completed": False,
        "contract": args.contract,
        "examples": {c: {k: r[k] for k in ("examples", "positives", "negatives", "positive_rate")} for c, r in receipts.items()},
        "train_examples": {s: len(v) for s, v in qa_by_source.items()} | {"depot": len(depot)},
        "heldout_examples": len(held_rows),
        "stream_sources": dict(Counter(s for s, _ in stream)),
        "gpu_used_mib_at_start": used,
        "history": [],
        "evaluations": [],
    }
    start = 0
    if args.resume and resume_path.exists():
        state = torch.load(resume_path, map_location="cpu", weights_only=False)
        trainer.restore(state)
        start, result = state["step"], state["result"]
        result.setdefault("resumes", []).append({"step": start, "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
        del state
    evaluator = Evaluator(trainer, held_rows, args)
    result_path = args.output / "result.json"
    stop = {"requested": False}

    def request_stop(signum, frame):
        stop["requested"] = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    def lookup(entry):
        source, index = entry
        return depot[index] if source == "depot" else qa_by_source[source][index]

    def evaluate(step):
        evaluation = evaluator.run(step)
        result["evaluations"].append(evaluation)
        save_json(result_path, result)
        print(json.dumps({"event": "evaluation", **evaluation}, ensure_ascii=False), flush=True)
        if step > 0:
            trainer.export(args.output / "snapshots" / f"step-{step:05d}", step, result)

    if start == 0 and not args.skip_initial_eval:
        evaluate(0)
    trainer.reader.train()
    trainer.copy_head.train()
    per_step = args.accum * args.micro_batch
    step = start
    for step in range(start, args.steps):
        began = time.perf_counter()
        scale = lr_scale(step, args.steps)
        for group in trainer.optimizer.param_groups:
            group["lr"] = group["base_lr"] * scale
        trainer.optimizer.zero_grad(set_to_none=True)
        totals, loss_sum = Counter(), 0.0
        for micro in range(args.accum):
            begin = step * per_step + micro * args.micro_batch
            examples = [lookup(e) for e in stream[begin : begin + args.micro_batch]]
            loss, stats = trainer.micro_loss(examples)
            (loss / args.accum).backward()
            loss_sum += float(loss.detach()) / args.accum
            for key, value in stats.items():
                totals[key] += value
            del loss
        norm = float(torch.nn.utils.clip_grad_norm_(trainer.trainable, 1.0, error_if_nonfinite=True))
        trainer.optimizer.step()
        torch.cuda.synchronize()
        row = {
            "step": step + 1,
            "loss": loss_sum,
            "lr_scale": scale,
            "grad_norm": norm,
            "seconds": round(time.perf_counter() - began, 3),
            "cuda_peak_allocated_bytes": torch.cuda.max_memory_allocated(),
        }
        for key in ("lexical", "pointer", "gate", "gate_mean", "open_correct"):
            row[key] = totals[key] / args.accum
        for key in ("positives", "negatives", "depot"):
            row[key] = totals[key]
        row["mean_doc_rows"] = totals["doc_rows"] / args.accum
        result["history"].append(row)
        print(json.dumps({"event": "step", **row}), flush=True)
        done = step + 1
        if done % args.eval_every == 0 or done == args.steps:
            evaluate(done)
        if done % args.checkpoint_every == 0 or done % args.eval_every == 0 or done == args.steps or stop["requested"]:
            atomic_checkpoint(resume_path, trainer.state(done, result))
            save_json(result_path, result)
        if stop["requested"]:
            result["stopped_at"] = done
            save_json(result_path, result)
            print(json.dumps({"event": "stopped", "step": done}), flush=True)
            return result
    result["completed"] = True
    final = trainer.export(args.output / "final", args.steps, result)
    result["final_candidate"] = str(final)
    save_json(result_path, result)
    if args.done_marker:
        args.done_marker.write_text(
            f"completed {args.steps} steps; final candidate {final}\n"
        )
    print(json.dumps({"event": "completed", "final": str(final)}), flush=True)
    return result


# ----------------------------------------------------------------------------
# report (scoring venv: upstream memorilla.metrics importable)


def latest(path: Path) -> dict[str, dict]:
    rows = {}
    for row in read_rows(path):
        rows[row["name"]] = row
    return rows


def paper_row_scores(metrics, row: dict, key: str, target: str) -> dict:
    """Upstream per-row scores of a paper-run row at prediction ``key``."""
    prediction = metrics.clean_generation(row["predictions"][key])
    reference = str(row["answer"]).strip()
    scores = {
        "em": float(metrics.normalize_answer(prediction) == metrics.normalize_answer(reference)),
        "f1": metrics.token_f1([prediction], [reference]),
    }
    if target == "narrativeqa":
        scores["rougeL"] = metrics.rouge([prediction], [reference])["rougeL"]
    return scores


def legacy_answer(row: dict) -> str:
    """The scored answer of a legacy hybrid row under the current
    canonicalization rule (rows written before the three-character tail
    minimum are re-spelled from ``answer_raw``, as plan section 8.4 did)."""
    from qwen_hybrid_eval import canonicalize_answer

    if row.get("record_gate") and "answer_raw" in row:
        record = row.get("hybrid_record_text") or row.get("record_text") or ""
        return canonicalize_answer(row["answer_raw"], record)
    return str(row.get("answer", ""))


def legacy_row_scores(metrics, row: dict, set_name: str) -> dict:
    answer, gold = legacy_answer(row), str(row["gold"])
    if set_name.startswith("depot"):
        return {"exact": float(answer.strip() == gold)}
    prediction = metrics.clean_generation(answer)
    scores = {
        "em": float(metrics.normalize_answer(prediction) == metrics.normalize_answer(gold)),
        "f1": metrics.token_f1([prediction], [gold]),
    }
    if set_name == "drop":
        pieces = [gold] + [p.strip() for p in gold.split(",") if p.strip()]
        scores["em_any"] = max(
            float(metrics.normalize_answer(prediction) == metrics.normalize_answer(p))
            for p in pieces
        )
        scores["f1_any"] = max(metrics.token_f1([prediction], [p]) for p in pieces)
    return scores


def compare_arms(scores: dict[str, dict[str, dict]], metric_names, args, names=None) -> dict:
    """Means per arm and paired bootstrap of each non-soft arm against soft
    (and new against old) on the common questions."""
    arms = list(scores)
    common = set.intersection(*(set(scores[a]) for a in arms))
    if names is not None:
        common &= set(names)
    common = sorted(common)
    table = {"rows": len(common), "means": {}, "paired": {}}
    if not common:
        return table
    for arm in arms:
        table["means"][arm] = {
            m: 100 * sum(scores[arm][n][m] for n in common) / len(common) for m in metric_names
        }
    pairs = [("soft", a) for a in arms if a != "soft"]
    if "old_hybrid" in arms and "new_hybrid" in arms:
        pairs.append(("old_hybrid", "new_hybrid"))
    for left, right in pairs:
        table["paired"][f"{left}->{right}"] = {
            m: _percent(
                paired_bootstrap(
                    [scores[left][n][m] for n in common],
                    [scores[right][n][m] for n in common],
                    args.resamples,
                    args.seed,
                )
            )
            for m in metric_names
        }
    return table


def _percent(result: dict) -> dict:
    out = dict(result)
    for key in ("left_mean", "right_mean", "mean_delta", "ci_low", "ci_high"):
        out[key] = round(100 * out[key], 2)
    return out


def gate_rate(rows: dict[str, dict]) -> dict:
    gated = [r for r in rows.values() if r.get("record_gate") is not None]
    return {
        "open": sum(bool(r["record_gate"]) for r in gated),
        "total": len(gated),
        "rate": round(sum(bool(r["record_gate"]) for r in gated) / len(gated), 4) if gated else None,
        "canonicalized": sum(bool(r.get("canonicalized")) for r in rows.values()),
    }


def report(args) -> dict:
    from memorilla import metrics

    result = {
        "schema": SCHEMA_REPORT,
        "resamples": args.resamples,
        "seed": args.seed,
        "paper": {},
        "legacy": {},
        "examples": {},
    }
    arms_dirs = {
        "soft": args.reference / "paper",
        "old_hybrid": args.reference / "paper",
        "new_hybrid": args.new / "paper",
    }
    files = {"soft": "soft.jsonl", "old_hybrid": "hybrid.jsonl", "new_hybrid": "hybrid.jsonl"}
    for target, key in (("triviaqa", args.triviaqa_key), ("narrativeqa", args.narrativeqa_key)):
        rows = {
            arm: latest(arms_dirs[arm] / target / files[arm]) for arm in files
        }
        if not all(rows.values()):
            result["paper"][target] = {"missing": [a for a, r in rows.items() if not r]}
            continue
        scores = {
            arm: {n: paper_row_scores(metrics, r, key, target) for n, r in arm_rows.items()}
            for arm, arm_rows in rows.items()
        }
        names = ("em", "f1") if target == "triviaqa" else ("rougeL", "f1")
        entry = {
            "prediction_key": key,
            "overall": compare_arms(scores, names, args),
            "gate": {arm: gate_rate(rows[arm]) for arm in ("old_hybrid", "new_hybrid")},
        }
        cases = {c["name"]: c for c in read_rows(args.reference / "paper" / "cases" / f"{target}.jsonl")}
        verbatim = [n for n, c in cases.items() if answer_in_documents(c["answer"], c["top_documents"])]
        verbatim2 = [n for n, c in cases.items() if answer_in_documents(c["answer"], c["top_documents"][:2])]
        entry["verbatim_top50"] = compare_arms(scores, names, args, verbatim)
        entry["verbatim_top2"] = compare_arms(scores, names, args, verbatim2)
        entry["verbatim_counts"] = {"top50": len(verbatim), "top2": len(verbatim2), "cases": len(cases)}
        if target == "narrativeqa":
            by_type = {}
            for kind in ("who", "which", "what", "why", "how", "where", "when", "other"):
                members = [n for n, c in cases.items() if c["question_type"] == kind]
                by_type[kind] = compare_arms(scores, names, args, members)
                by_type[kind]["gate_new"] = sum(
                    bool(rows["new_hybrid"].get(n, {}).get("record_gate")) for n in members
                )
            entry["by_question_type"] = by_type
        result["paper"][target] = entry
        result["examples"][target] = pick_examples(rows, scores, names[0], args.examples)
    legacy_files = {
        "soft": lambda s: args.reference / f"soft-{s}.jsonl",
        "old_hybrid": lambda s: args.reference / "variants" / "gated-canon" / f"hybrid-{s}.jsonl",
        "new_hybrid": lambda s: args.new / "legacy" / f"hybrid-{s}.jsonl",
    }
    for set_name in ("depot-dev", "depot-fresh", "squad", "drop"):
        rows = {arm: latest(path(set_name)) for arm, path in legacy_files.items()}
        if not all(rows.values()):
            result["legacy"][set_name] = {"missing": [a for a, r in rows.items() if not r]}
            continue
        scores = {
            arm: {
                n: legacy_row_scores(metrics, r, set_name)
                for n, r in arm_rows.items()
                if "error" not in r
            }
            for arm, arm_rows in rows.items()
        }
        names = (
            ("exact",)
            if set_name.startswith("depot")
            else ("em", "f1", "em_any", "f1_any")
            if set_name == "drop"
            else ("em", "f1")
        )
        result["legacy"][set_name] = {
            "overall": compare_arms(scores, names, args),
            "gate": {arm: gate_rate(rows[arm]) for arm in ("old_hybrid", "new_hybrid")},
        }
        result["examples"][set_name] = pick_examples(rows, scores, names[0], args.examples, legacy=True)
    save_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k != "examples"}, indent=1), flush=True)
    return result


def pick_examples(rows, scores, metric, count, legacy=False) -> dict:
    """Questions the new hybrid wins and loses against soft."""
    common = sorted(set.intersection(*(set(s) for s in scores.values())))
    wins, losses = [], []
    for name in common:
        delta = scores["new_hybrid"][name][metric] - scores["soft"][name][metric]
        if delta == 0:
            continue
        new = rows["new_hybrid"][name]
        item = {
            "name": name,
            "question": new["question"][:300],
            "gold": str(new.get("gold", new.get("answer")))[:200],
            "soft": _answer(rows["soft"][name], legacy)[:200],
            "old_hybrid": _answer(rows["old_hybrid"][name], legacy)[:200],
            "new_hybrid": _answer(new, legacy)[:200],
            "record_gate": new.get("record_gate"),
            "record_text": new.get("record_text"),
            "delta": round(delta, 3),
        }
        (wins if delta > 0 else losses).append(item)
    return {"wins": wins[:count], "losses": losses[:count], "win_count": len(wins), "loss_count": len(losses)}


def _answer(row: dict, legacy: bool) -> str:
    if legacy:
        return legacy_answer(row)
    return str(row["predictions"].get("full", ""))


# ----------------------------------------------------------------------------
# CLI


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    pr = sub.add_parser("prep")
    pr.add_argument("--data-root", type=Path, required=True)
    pr.add_argument("--tokenizer", type=Path, required=True, help="Qwen3-Embedding-4B dir")
    pr.add_argument("--output", type=Path, required=True)
    pr.add_argument("--samples", required=True, help="config=count,... (0 = all)")
    pr.add_argument("--seed", type=int, default=20261113)
    pr.add_argument("--top-k", type=int, default=TOP_K)
    pr.add_argument("--label", default=DEFAULT_LABEL)
    pr.add_argument("--max-encoder-tokens", type=int, default=1024)
    pr.add_argument(
        "--chunking",
        choices=CHUNKING_CHOICES,
        default="none",
        help="pack256: the top chunks select their 256-token rag256 packs",
    )
    pr.add_argument(
        "--read-tokens",
        type=int,
        default=0,
        help="read set (budget study C): chunks in stored-cosine order (top 50) "
        "while their Qwen3 token total stays within this budget; 0 = top-k",
    )
    pr.add_argument(
        "--resume",
        action="store_true",
        help="keep the rows of an existing <config>.partial and continue",
    )
    pr.add_argument("--force", action="store_true")
    pb = sub.add_parser("probe")
    pb.add_argument("--assets", type=Path, required=True)
    pb.add_argument("--candidate", type=Path, required=True)
    pb.add_argument("--data-root", type=Path, required=True)
    pb.add_argument("--output", type=Path, required=True)
    pb.add_argument("--probe-cases", type=int, default=6)
    tr = sub.add_parser("train")
    tr.add_argument("--assets", type=Path, required=True)
    tr.add_argument("--init", type=Path, required=True, help="copy17 candidate dir")
    tr.add_argument("--examples", type=Path, required=True, help="prep output dir")
    tr.add_argument("--depot-data", type=Path, required=True, help="data-paired-v2")
    tr.add_argument("--data-root", type=Path, required=True)
    tr.add_argument("--reference-rows", type=Path, required=True, help="qwen-hybrid dir")
    tr.add_argument("--output", type=Path, required=True)
    tr.add_argument("--weights", required=True, help="config=weight,...")
    tr.add_argument("--depot-fraction", type=float, default=0.15)
    tr.add_argument("--steps", type=int, default=4096)
    tr.add_argument("--accum", type=int, default=8)
    tr.add_argument("--micro-batch", type=int, default=16)
    tr.add_argument("--reader-lr", type=float, default=1e-4)
    tr.add_argument("--copy-head-lr", type=float, default=2e-4)
    tr.add_argument("--lexical-weight", type=float, default=1.0)
    tr.add_argument("--pointer-weight", type=float, default=1.0)
    tr.add_argument("--gate-weight", type=float, default=3.0)
    tr.add_argument("--seed", type=int, default=23)
    tr.add_argument("--eval-every", type=int, default=512)
    tr.add_argument("--checkpoint-every", type=int, default=128)
    tr.add_argument("--heldout-eval", type=int, default=256)
    tr.add_argument("--hybrid-eval", type=int, default=64)
    tr.add_argument("--max-encoder-tokens", type=int, default=1024)
    tr.add_argument(
        "--question-prefix",
        choices=("none", *QUESTION_PREFIX_TEMPLATES),
        default="none",
        help="encode each document after the question text (protocol "
        "document_question_prefix)",
    )
    tr.add_argument("--train-need-mib", type=int, default=30 * 1024)
    tr.add_argument("--decoder-need-mib", type=int, default=20 * 1024)
    tr.add_argument("--skip-initial-eval", action="store_true")
    tr.add_argument("--resume", action="store_true")
    tr.add_argument("--done-marker", type=Path)
    rp = sub.add_parser("report")
    rp.add_argument("--reference", type=Path, required=True, help="qwen-hybrid dir")
    rp.add_argument("--new", type=Path, required=True, help="qwen-copy-qa remeasure dir")
    rp.add_argument("--output", type=Path, required=True)
    rp.add_argument("--resamples", type=int, default=10_000)
    rp.add_argument("--seed", type=int, default=23)
    rp.add_argument("--examples", type=int, default=6)
    rp.add_argument("--triviaqa-key", default="full")
    rp.add_argument("--narrativeqa-key", default="full")
    args = p.parse_args(argv)
    if args.command == "train":
        if not 0 <= args.depot_fraction < 1 or args.steps < 1 or args.accum < 1 or args.micro_batch < 1:
            p.error("invalid training budget")
    return args


def main(argv=None):
    args = parse_args(argv)
    if args.command == "prep":
        return prep(args)
    if args.command == "probe":
        return probe(args)
    if args.command == "train":
        return train(args)
    return report(args)


if __name__ == "__main__":
    main()
