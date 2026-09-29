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

"""Build an unlabeled text pool for Antenna feature distillation.

Feature distillation needs only (text, schema) pairs: the teacher encoder
supplies the targets, so no labels are written. Texts come from the train
splits in ``antenna_datasets.py`` and, with ``--wikipedia``, from Wikipedia
article passages (the pinned 10k-article JSONL, split at paragraph
boundaries into passages of up to ``--passage-words`` words); each gets a random classification schema
(4-16 labels from the pool's label and entity-type names) or, with
``--entity-share``, a random entity schema (2-10 types).

``--source NAME=ROWS`` (repeatable) replaces that text mix with a sampled
mix of the pinned permissive sources in ``SOURCES`` (and ``wikipedia`` with
``--wikipedia``). They widen both halves of the distillation loss: short
utterances, questions and web sentences for the text rows, and their intent,
emotion, topic and free-form entity-type names for the marker rows. None is an
evaluation dataset. NuNER sentences mostly get entity schemas built from
their own annotated types plus random negatives. Rows are native
boundary training rows (``boundary_dataset.zig`` version 1) with no
annotations, split into train and validation, deduplicated by text, and
limited to texts upstream's word splitter keeps whole.

    ANTFLY_ANTENNA_DATA=<cache> PYTHONDONTWRITEBYTECODE=1 <oracle venv>/bin/python distill_pool.py \\
        --upstream <GLiNER2 checkout> --output <dir outside Git> [--rows 80000] [--entity-share 0.5]
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "gliner25"))
sys.path.insert(0, str(HERE))

import oracle

TEXT_SETS = ("ag_news", "banking77")
LABEL_SETS = ("banking77", "ag_news")
TYPE_SETS = ("crossner_ai", "crossner_literature", "crossner_music", "mit_restaurant")
GENERIC_TYPES = ("person", "organization", "location", "date", "product", "event", "money", "country", "city", "company")
TASK_NAMES = ("intent", "topic", "category", "type")
WIKIPEDIA_SHA256 = "a446ebb8721ce4dd262a8320ba65479a21b361140c639d8a6c4e198892382623"  # wiki-articles-10k-v001.json
MIX_TASK_NAMES = TASK_NAMES + ("sentiment", "emotion", "domain", "subject", "request", "label")

HF = "https://huggingface.co/datasets"
# Pool-only sources: pinned revision and SHA-256, permissive licenses.
SOURCES = {
    # MIT; 1M web sentences with free-form entity types from an LLM.
    "nuner": (f"{HF}/numind/NuNER/resolve/1784de71436044100ab9f153435f8a5c0ea4e1b6/data/entity-00001-of-00001.csv", "91b5533e7a2a89904d221fdb1ed468196411d1abd3327cee07eba7a4b7b63744"),
    # Apache 2.0; MASSIVE English voice-assistant commands, 60 intents.
    "massive": (f"{HF}/mteb/amazon_massive_intent/resolve/940fd47a81eaa7f2cc7b129674d945d618ac38c2/train/en.json.gz", "65e77f0f2596931671074e3d031a481bc798e5800daa2c886ce7aa26cc16cf22"),
    # Apache 2.0; Reddit comments, 28 emotions.
    "go_emotions": (f"{HF}/google-research-datasets/go_emotions/resolve/add492243ff905527e67aeb8b80c082af02207c3/simplified/train-00000-of-00001.parquet", "b7d74279616ae7c9b8374ab62ea9f9d6504d36a577bb17f745d720dc2b0d4e76"),
    # CC BY-SA 4.0; SQuAD questions only.
    "squad": (f"{HF}/rajpurkar/squad/resolve/7b6d24c440a36b6815f21b70d25016731768db1f/plain_text/train-00000-of-00001.parquet", "ea7f52bac024f6b1bdc7aaa2a4ee302cba8c2fdc8d4a235cf18a9a5196b6175b"),
    # CC BY-SA 3.0; DBpedia abstracts, 14 topics.
    "dbpedia": (f"{HF}/fancyzhx/dbpedia_14/resolve/9abd46cf7fc8b4c64290f26993c540b92aa145ac/dbpedia_14/train-00000-of-00001.parquet", "0640e4664a99cc94c47db1d7b2e01c14455d5bbecb8183ad1f93bde59f3f28ee"),
}
NUNER_MIN_TYPE_COUNT = 20


def wikipedia_passages(path: Path, limit: int) -> list[str]:
    """Paragraph-bounded passages of at most ``limit`` words, headings dropped."""
    if oracle.sha256_file(path) != WIKIPEDIA_SHA256:
        raise oracle.ContractError(f"{path} is not the pinned Wikipedia snapshot")
    passages = []
    for line in path.read_text(encoding="utf-8").splitlines():
        article = json.loads(line)
        paragraphs = [p.strip() for p in article["body"].split("\n") if p.strip()]
        # The first line repeats the title; one-word lines ending in "." are section headings.
        paragraphs = [p for p in paragraphs[1:] if not (len(p.split()) <= 3 and p.endswith("."))]
        current: list[str] = []
        for paragraph in paragraphs:
            words = paragraph.split()
            if len(words) > limit:
                continue
            if current and len(current) + len(words) > limit:
                passages.append(" ".join(current))
                current = []
            current.extend(words)
        if len(current) >= 8:
            passages.append(" ".join(current))
    return passages


def _fetch(name: str) -> bytes:
    import hashlib

    import antenna_datasets as datasets

    url, expected = SOURCES[name]
    data = datasets._fetch(url)
    digest = hashlib.sha256(data).hexdigest()
    if digest != expected:
        raise oracle.ContractError(f"{url}: SHA-256 {digest} != {expected}")
    return data


def _natural(identifier: str) -> str:
    """alarm_set -> alarm set; EducationalInstitution -> educational institution."""
    import re

    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", identifier).replace("_", " ").strip().lower()


def _parquet_names(data: bytes, column: str) -> list[str]:
    import io

    import pyarrow.parquet as pq

    features = json.loads(pq.read_schema(io.BytesIO(data)).metadata[b"huggingface"])["info"]["features"][column]
    return list(features.get("names") or features["feature"]["names"])


def load_source(name: str) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """([(text, own entity types)], label names) for a pool source."""
    import ast
    import csv
    import gzip
    import io

    import pyarrow.parquet as pq

    data = _fetch(name)
    if name == "nuner":
        csv.field_size_limit(1 << 24)
        items = []
        for row in csv.DictReader(io.StringIO(data.decode("utf-8"))):
            try:
                spans = ast.literal_eval(row["output"])
            except (SyntaxError, ValueError):
                continue
            types = [part.split(" <> ", 1)[1].strip().lower() for part in spans if " <> " in part]
            items.append((row["input"], list(dict.fromkeys(t for t in types if t))))
        return items, []
    if name == "massive":
        rows = [json.loads(line) for line in gzip.decompress(data).decode("utf-8").splitlines()]
        return [(row["text"], []) for row in rows], sorted({_natural(row["label_text"]) for row in rows})
    table = pq.read_table(io.BytesIO(data)).to_pylist()
    if name == "go_emotions":
        return [(row["text"], []) for row in table], [_natural(n) for n in _parquet_names(data, "labels")]
    if name == "squad":
        return [(question, []) for question in dict.fromkeys(row["question"].strip() for row in table)], []
    if name == "dbpedia":
        return [(row["content"].strip(), []) for row in table], [_natural(n) for n in _parquet_names(data, "label")]
    raise KeyError(name)


def _interleave(rng: random.Random, groups: list[list[Any]]) -> list[Any]:
    """Merge shuffled groups so any prefix mixes them in proportion."""
    total = sum(len(g) for g in groups)
    cursors, mixed = [0] * len(groups), []
    while len(mixed) < total:
        pick = rng.random() * (total - len(mixed))
        for index, group in enumerate(groups):
            left = len(group) - cursors[index]
            if pick < left:
                mixed.append(group[cursors[index]]); cursors[index] += 1
                break
            pick -= left
    return mixed


def build_mix(args: argparse.Namespace) -> dict[str, Any]:
    from collections import Counter

    import antenna_datasets as datasets
    from gliner2.processing.word_splitter import WhitespaceTokenSplitter

    splitter = WhitespaceTokenSplitter()
    rng = random.Random(args.seed)
    requested = dict(item.split("=", 1) for item in args.source)
    labels = {name for dataset in LABEL_SETS for name in datasets.label_names(dataset)} \
        | {kind for dataset in TYPE_SETS for kind in datasets.entity_types(dataset)}
    types = {kind for dataset in TYPE_SETS for kind in datasets.entity_types(dataset)} | set(GENERIC_TYPES)
    groups, counts, own_labels = [], {}, {}
    for name, rows in requested.items():
        if name in TEXT_SETS:
            items = [(record["text"], []) for record in datasets.load_classification(name, "train")]
            names = datasets.label_names(name)
        elif name == "wikipedia":
            if not args.wikipedia:
                raise oracle.ContractError("source wikipedia needs --wikipedia")
            items, names = [(text, []) for text in wikipedia_passages(args.wikipedia, args.passage_words)], []
        else:
            items, names = load_source(name)
        if name == "nuner":
            frequency = Counter(t for _, own in items for t in own)
            # Brackets collide with the schema's reserved markers; parentheses are reserved in labels.
            common = {t for t, n in frequency.items()
                      if n >= NUNER_MIN_TYPE_COUNT and len(t.split()) <= 4 and not any(c in t for c in "()[]")}
            types |= common
            items = [(text, [t for t in own if t in common]) for text, own in items]
        labels |= set(names)
        own_labels[name] = names
        rng.shuffle(items)
        items = [(name, text, own) for text, own in items if len(list(splitter(text, lower=False))) <= args.max_words][:int(rows)]
        counts[name] = len(items)
        groups.append(items)
    labels, types = sorted(labels), sorted(types)
    seen: set[str] = set()
    rows = []
    for name, text, own in _interleave(rng, groups):
        if text in seen:
            continue
        seen.add(text)
        entity = rng.random() < (0.8 if own else args.entity_share)
        if entity:
            count = rng.randint(2, 10)
            chosen = own[:max(1, count - 1)]
            chosen += rng.sample([t for t in types if t not in chosen], max(1, count - len(chosen)))
            rng.shuffle(chosen)
            schema = {"entities": chosen}
        else:
            count = rng.randint(4, 16)
            native = own_labels[name]
            chosen = rng.sample(native, min(len(native), count // 2)) if native else []
            # Half the rest from label names, half from the (far wider) type vocabulary.
            rest = count - len(chosen)
            chosen += rng.sample([l for l in labels if l not in chosen], rest - rest // 2)
            chosen += rng.sample([t for t in types if t not in chosen], rest // 2)
            rng.shuffle(chosen)
            schema = {"classifications": [{"name": rng.choice(MIX_TASK_NAMES), "labels": chosen}]}
        rows.append({"version": 1, "id": f"pool-{len(rows)}", "text": text, "schema": schema})
    return {"rows": rows, "manifest": {
        "sources": {name: {"rows": counts[name], "url": SOURCES[name][0] if name in SOURCES else None} for name in requested},
        "label_names": len(labels), "entity_types": len(types),
        "wikipedia": {"sha256": WIKIPEDIA_SHA256, "passage_words": args.passage_words} if "wikipedia" in requested else None,
    }}


def build(args: argparse.Namespace) -> dict[str, Any]:
    import antenna_datasets as datasets

    provenance, _ = oracle.prepare_runtime(args.upstream)
    if args.source:
        mix = build_mix(args)
        return write(args, mix["rows"], {**mix["manifest"], "text_sets": None}, provenance)
    from gliner2.processing.word_splitter import WhitespaceTokenSplitter

    splitter = WhitespaceTokenSplitter()
    rng = random.Random(args.seed)
    labels = sorted({name for dataset in LABEL_SETS for name in datasets.label_names(dataset)}
                    | {kind for dataset in TYPE_SETS for kind in datasets.entity_types(dataset)})
    types = sorted({kind for dataset in TYPE_SETS for kind in datasets.entity_types(dataset)} | set(GENERIC_TYPES))
    texts = [record["text"] for dataset in TEXT_SETS for record in datasets.load_classification(dataset, "train")]
    wikipedia = wikipedia_passages(args.wikipedia, args.passage_words) if args.wikipedia else []
    rng.shuffle(texts)
    rng.shuffle(wikipedia)
    if wikipedia:
        # Interleave so any prefix of the pool mixes both sources in proportion.
        share = len(wikipedia) / (len(wikipedia) + len(texts))
        mixed, wi, ti = [], 0, 0
        while wi < len(wikipedia) or ti < len(texts):
            if ti >= len(texts) or (wi < len(wikipedia) and rng.random() < share):
                mixed.append(wikipedia[wi]); wi += 1
            else:
                mixed.append(texts[ti]); ti += 1
        texts = mixed
    seen: set[str] = set()
    rows = []
    for text in texts:
        if len(rows) >= args.rows:
            break
        if text in seen or len(list(splitter(text, lower=False))) > args.max_words:
            continue
        seen.add(text)
        if rng.random() < args.entity_share:
            schema = {"entities": rng.sample(types, rng.randint(2, 10))}
        else:
            schema = {"classifications": [{"name": rng.choice(TASK_NAMES), "labels": rng.sample(labels, rng.randint(4, 16))}]}
        rows.append({"version": 1, "id": f"pool-{len(rows)}", "text": text, "schema": schema})
    return write(args, rows, {
        "text_sets": TEXT_SETS, "label_names": len(labels), "entity_types": len(types),
        "wikipedia": {"sha256": WIKIPEDIA_SHA256, "passages": len(wikipedia), "passage_words": args.passage_words} if args.wikipedia else None,
    }, provenance)


def write(args: argparse.Namespace, rows: list[dict[str, Any]], details: dict[str, Any], provenance: dict[str, Any]) -> dict[str, Any]:
    import antenna_datasets as datasets

    validation_count = max(1, int(len(rows) * args.validation_fraction))
    splits = {"validation": rows[:validation_count], "train": rows[validation_count:]}
    with oracle.atomic_output_directory(args.output) as directory:
        files = {}
        for split, items in splits.items():
            path = directory / f"{split}.jsonl"
            path.write_text("".join(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n" for item in items), encoding="utf-8")
            files[split] = {"path": path.name, "records": len(items), "sha256": oracle.sha256_file(path)}
        oracle.write_json(directory / "manifest.json", {
            "dataset_format": "gliner_boundary_dataset.Row/version=1", "purpose": "antenna feature distillation (unlabeled)",
            "files": files, **details,
            "entity_share": args.entity_share, "seed": args.seed, "max_words": args.max_words,
            "generator_sha256": oracle.sha256_file(Path(__file__)),
            "datasets_module_sha256": oracle.sha256_file(Path(datasets.__file__)), "provenance": provenance,
        })
        oracle.verify_upstream_checkout(args.upstream)
    return {"status": "built", "output": str(args.output.resolve()), **{k: v["records"] for k, v in files.items()}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=80000)
    parser.add_argument("--entity-share", type=float, default=0.5)
    parser.add_argument("--validation-fraction", type=float, default=0.01)
    parser.add_argument("--max-words", type=int, default=128)
    parser.add_argument("--seed", type=int, default=20260926)
    parser.add_argument("--wikipedia", type=Path, help="wiki-articles-10k-v001.json (cdn.antfly.io/datasets/)")
    parser.add_argument("--passage-words", type=int, default=100)
    parser.add_argument("--source", action="append", default=[], metavar="NAME=ROWS",
                        help=f"sampled source mix instead of the default texts: {', '.join(TEXT_SETS + ('wikipedia',) + tuple(SOURCES))}")
    print(json.dumps(build(parser.parse_args()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
