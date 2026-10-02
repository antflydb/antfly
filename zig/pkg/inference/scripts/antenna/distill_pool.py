#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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
their own annotated types plus random negatives. ``openjev`` adds typed
decision states, each question a classification task over its options.

``--label-sets label_sets.json`` makes every other classification row draw
its labels from one real label set (the source's own, else a hand-written
set) instead of mixing label and entity-type names. Rows are native
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
GENERIC_TYPES = (
    "person",
    "organization",
    "location",
    "date",
    "product",
    "event",
    "money",
    "country",
    "city",
    "company",
)
TASK_NAMES = ("intent", "topic", "category", "type")
WIKIPEDIA_SHA256 = "a446ebb8721ce4dd262a8320ba65479a21b361140c639d8a6c4e198892382623"  # wiki-articles-10k-v001.json
MIX_TASK_NAMES = TASK_NAMES + (
    "sentiment",
    "emotion",
    "domain",
    "subject",
    "request",
    "label",
)

HF = "https://huggingface.co/datasets"
# Pool-only sources: pinned revision and SHA-256, permissive licenses.
SOURCES = {
    # MIT; 1M web sentences with free-form entity types from an LLM.
    "nuner": (
        f"{HF}/numind/NuNER/resolve/1784de71436044100ab9f153435f8a5c0ea4e1b6/data/entity-00001-of-00001.csv",
        "91b5533e7a2a89904d221fdb1ed468196411d1abd3327cee07eba7a4b7b63744",
    ),
    # Apache 2.0; MASSIVE English voice-assistant commands, 60 intents.
    "massive": (
        f"{HF}/mteb/amazon_massive_intent/resolve/940fd47a81eaa7f2cc7b129674d945d618ac38c2/train/en.json.gz",
        "65e77f0f2596931671074e3d031a481bc798e5800daa2c886ce7aa26cc16cf22",
    ),
    # Apache 2.0; Reddit comments, 28 emotions.
    "go_emotions": (
        f"{HF}/google-research-datasets/go_emotions/resolve/add492243ff905527e67aeb8b80c082af02207c3/simplified/train-00000-of-00001.parquet",
        "b7d74279616ae7c9b8374ab62ea9f9d6504d36a577bb17f745d720dc2b0d4e76",
    ),
    # CC BY-SA 4.0; SQuAD questions only.
    "squad": (
        f"{HF}/rajpurkar/squad/resolve/7b6d24c440a36b6815f21b70d25016731768db1f/plain_text/train-00000-of-00001.parquet",
        "ea7f52bac024f6b1bdc7aaa2a4ee302cba8c2fdc8d4a235cf18a9a5196b6175b",
    ),
    # CC BY-SA 3.0; DBpedia abstracts, 14 topics.
    "dbpedia": (
        f"{HF}/fancyzhx/dbpedia_14/resolve/9abd46cf7fc8b4c64290f26993c540b92aa145ac/dbpedia_14/train-00000-of-00001.parquet",
        "0640e4664a99cc94c47db1d7b2e01c14455d5bbecb8183ad1f93bde59f3f28ee",
    ),
    # CC0; Open-Jev rule-labelled typed decisions (release-v2-redistributable
    # train split). Each record's question becomes a classification task over
    # its options; customer-control-v1 is excluded (Open-Jev notes its
    # question descriptions have no verified source license).
    "openjev": (
        f"{HF}/ZefanCai/Open-Jev/resolve/c67699e13d0ae25e35b77165a4b6b079bedc8aba/raw/release-v2-redistributable/train.jsonl.gz",
        "e67c8aa8b31f341981d78c4da5fc07a195a6d1357ee37ca9d7fc5b2135d3fde5",
    ),
}
NUNER_MIN_TYPE_COUNT = 20
OPENJEV_EXCLUDED = ("customer-control-v1",)
# Synthetic game and pixel generators; the workflow and reasoning sources are
# closer to the decisions Antenna serves, so only a quarter of these are kept.
OPENJEV_GAMES = (
    "painting-geometry-v1",
    "snake-v1",
    "vizdoom-basic-v1",
    "tic_tac_toe-v1",
    "tile_platformer-v1",
    "trex_runner-v1",
)
OPENJEV_QUESTION_WORDS = 24
OPENJEV_OPTION_WORDS = 8
# Task names for sources whose texts carry their own label set.
SOURCE_TASKS = {
    "banking77": "intent",
    "ag_news": "topic",
    "massive": "intent",
    "go_emotions": "emotion",
    "dbpedia": "topic",
    "huffpost": "topic",
}
# Sources loaded through antenna_training_sets.py (pinned there).
TRAINING_SET_SOURCES = ("huffpost",)


def wikipedia_passages(path: Path, limit: int) -> list[str]:
    """Paragraph-bounded passages of at most ``limit`` words, headings dropped."""
    if oracle.sha256_file(path) != WIKIPEDIA_SHA256:
        raise oracle.ContractError(f"{path} is not the pinned Wikipedia snapshot")
    passages = []
    for line in path.read_text(encoding="utf-8").splitlines():
        article = json.loads(line)
        paragraphs = [p.strip() for p in article["body"].split("\n") if p.strip()]
        # The first line repeats the title; one-word lines ending in "." are section headings.
        paragraphs = [
            p for p in paragraphs[1:] if not (len(p.split()) <= 3 and p.endswith("."))
        ]
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

    return (
        re.sub(r"(?<=[a-z])(?=[A-Z])", " ", identifier)
        .replace("_", " ")
        .strip()
        .lower()
    )


def _parquet_names(data: bytes, column: str) -> list[str]:
    import io

    import pyarrow.parquet as pq

    features = json.loads(pq.read_schema(io.BytesIO(data)).metadata[b"huggingface"])[
        "info"
    ]["features"][column]
    return list(features.get("names") or features["feature"]["names"])


def _schema_name(value: str, words: int) -> str:
    """A schema name the native compiler accepts: no brackets or parentheses
    (reserved markers and label syntax), single spaces, at most ``words``."""
    cleaned = "".join(" " if c in "()[]" else c for c in value)
    return " ".join(cleaned.split()[:words])


def _openjev_task(record: dict[str, Any]) -> tuple[str, list[str], str, str] | None:
    """(question, option labels, kind, source), or None when the options do
    not make at least two distinct labels."""
    import ast

    options = record["options"]
    if isinstance(options, str):
        options = ast.literal_eval(options)
    kind = record["kind"]
    if kind == "noul":
        labels = ["no", "yes"]
    elif kind == "choice" and all(": " in o for o in options):
        labels = [o.split(": ", 1)[0] for o in options]
    else:
        labels = list(options)
    labels = [_schema_name(label, OPENJEV_OPTION_WORDS) for label in labels]
    labels = [label for label in dict.fromkeys(labels) if label]
    question = _schema_name(record["question"], OPENJEV_QUESTION_WORDS)
    if len(labels) < 2 or not question:
        return None
    return question, labels, kind, record["source"]


def load_source(name: str) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """([(text, own entity types)], label names) for a pool source.

    Open-Jev items carry a third element, their ``_openjev_task``."""
    import ast
    import csv
    import gzip
    import io

    import pyarrow.parquet as pq

    if name in TRAINING_SET_SOURCES:
        import antenna_training_sets

        records = antenna_training_sets.load_classification(name)
        return [(record["text"], []) for record in records], (
            antenna_training_sets.label_names(name)
        )
    data = _fetch(name)
    if name == "nuner":
        csv.field_size_limit(1 << 24)
        items = []
        for row in csv.DictReader(io.StringIO(data.decode("utf-8"))):
            try:
                spans = ast.literal_eval(row["output"])
            except (SyntaxError, ValueError):
                continue
            types = [
                part.split(" <> ", 1)[1].strip().lower()
                for part in spans
                if " <> " in part
            ]
            items.append((row["input"], list(dict.fromkeys(t for t in types if t))))
        return items, []
    if name == "openjev":
        items = []
        for line in gzip.decompress(data).decode("utf-8").splitlines():
            record = json.loads(line)
            if record["source"] in OPENJEV_EXCLUDED:
                continue
            task = _openjev_task(record)
            state = record["state"]
            # Rendered as scripts/laya/prepare_laya_openjev.py renders it.
            if not isinstance(state, str):
                state = json.dumps(state, ensure_ascii=False)
            if task is not None and state.strip():
                items.append((state, [], task))
        return items, []
    if name == "massive":
        rows = [
            json.loads(line)
            for line in gzip.decompress(data).decode("utf-8").splitlines()
        ]
        return [(row["text"], []) for row in rows], sorted(
            {_natural(row["label_text"]) for row in rows}
        )
    table = pq.read_table(io.BytesIO(data)).to_pylist()
    if name == "go_emotions":
        return [(row["text"], []) for row in table], [
            _natural(n) for n in _parquet_names(data, "labels")
        ]
    if name == "squad":
        return [
            (question, [])
            for question in dict.fromkeys(row["question"].strip() for row in table)
        ], []
    if name == "dbpedia":
        return [(row["content"].strip(), []) for row in table], [
            _natural(n) for n in _parquet_names(data, "label")
        ]
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
                mixed.append(group[cursors[index]])
                cursors[index] += 1
                break
            pick -= left
    return mixed


def _coherent_task(
    rng: random.Random,
    label_sets: list[tuple[str, list[str]]],
    native: list[str],
    source: str,
) -> dict[str, Any]:
    """One classification task drawn from a single real label set: the
    source's own when it has one, otherwise a random set. A quarter of tasks
    get one or two distractor labels from another set."""
    if native:
        task, pool = SOURCE_TASKS.get(source, "category"), native
    else:
        task, pool = rng.choice(label_sets)
    count = min(len(pool), rng.randint(4, 16))
    labels = rng.sample(pool, count)
    if rng.random() < 0.25:
        _, other = rng.choice(label_sets)
        spare = [label for label in other if label not in labels]
        labels += rng.sample(spare, min(len(spare), rng.randint(1, 2)))
    rng.shuffle(labels)
    return {"name": task, "labels": labels}


def build_mix(args: argparse.Namespace) -> dict[str, Any]:
    from collections import Counter

    import antenna_datasets as datasets
    from gliner2.processing.word_splitter import WhitespaceTokenSplitter

    splitter = WhitespaceTokenSplitter()
    rng = random.Random(args.seed)
    requested = dict(item.split("=", 1) for item in args.source)
    labels = {
        name for dataset in LABEL_SETS for name in datasets.label_names(dataset)
    } | {kind for dataset in TYPE_SETS for kind in datasets.entity_types(dataset)}
    types = {
        kind for dataset in TYPE_SETS for kind in datasets.entity_types(dataset)
    } | set(GENERIC_TYPES)
    label_sets = (
        [
            (item["task"], item["labels"])
            for item in json.loads(args.label_sets.read_text(encoding="utf-8"))["sets"]
        ]
        if args.label_sets
        else []
    )
    groups, counts, own_labels = [], {}, {}
    for name, rows in requested.items():
        if name in TEXT_SETS:
            items = [
                (record["text"], [])
                for record in datasets.load_classification(name, "train")
            ]
            names = datasets.label_names(name)
        elif name == "wikipedia":
            if not args.wikipedia:
                raise oracle.ContractError("source wikipedia needs --wikipedia")
            items, names = (
                [
                    (text, [])
                    for text in wikipedia_passages(args.wikipedia, args.passage_words)
                ],
                [],
            )
        else:
            items, names = load_source(name)
        if name == "nuner":
            frequency = Counter(t for _, own in items for t in own)
            # Brackets collide with the schema's reserved markers; parentheses are reserved in labels.
            common = {
                t
                for t, n in frequency.items()
                if n >= NUNER_MIN_TYPE_COUNT
                and len(t.split()) <= 4
                and not any(c in t for c in "()[]")
            }
            types |= common
            items = [(text, [t for t in own if t in common]) for text, own in items]
        labels |= set(names)
        own_labels[name] = names
        if names and args.label_sets:
            label_sets.append((SOURCE_TASKS.get(name, "category"), names))
        if name == "openjev":
            # Yes/no questions are 62% of Open-Jev; keep a third of them so
            # option lists dominate the decision rows, and a quarter of the
            # synthetic game rows.
            def keep(task: tuple[str, list[str], str, str]) -> bool:
                share = (1 / 3 if task[2] == "noul" else 1.0) * (
                    0.25 if task[3] in OPENJEV_GAMES else 1.0
                )
                return rng.random() < share

            items = [item for item in items if keep(item[2])]
        rng.shuffle(items)
        items = [
            (name, item[0], item[1], item[2] if len(item) > 2 else None)
            for item in items
            if len(list(splitter(item[0], lower=False))) <= args.max_words
        ][: int(rows)]
        counts[name] = len(items)
        groups.append(items)
    labels, types = sorted(labels), sorted(types)
    seen: set[str] = set()
    rows = []
    for name, text, own, task in _interleave(rng, groups):
        if task is not None:
            # A decision's state repeats across its questions; keep each question.
            key = f"{text}\u0000{task[0]}"
        else:
            key = text
        if key in seen:
            continue
        seen.add(key)
        if task is not None:
            schema = {"classifications": [{"name": task[0], "labels": task[1]}]}
            rows.append(
                {
                    "version": 1,
                    "id": f"pool-{len(rows)}",
                    "text": text,
                    "schema": schema,
                }
            )
            continue
        entity = rng.random() < (0.8 if own else args.entity_share)
        if entity:
            count = rng.randint(2, 10)
            chosen = own[: max(1, count - 1)]
            chosen += rng.sample(
                [t for t in types if t not in chosen], max(1, count - len(chosen))
            )
            rng.shuffle(chosen)
            schema = {"entities": chosen}
        elif label_sets:
            schema = {
                "classifications": [
                    _coherent_task(rng, label_sets, own_labels[name], name)
                ]
            }
        else:
            count = rng.randint(4, 16)
            native = own_labels[name]
            chosen = rng.sample(native, min(len(native), count // 2)) if native else []
            # Half the rest from label names, half from the (far wider) type vocabulary.
            rest = count - len(chosen)
            chosen += rng.sample(
                [l for l in labels if l not in chosen], rest - rest // 2
            )
            chosen += rng.sample([t for t in types if t not in chosen], rest // 2)
            rng.shuffle(chosen)
            schema = {
                "classifications": [
                    {"name": rng.choice(MIX_TASK_NAMES), "labels": chosen}
                ]
            }
        rows.append(
            {"version": 1, "id": f"pool-{len(rows)}", "text": text, "schema": schema}
        )
    return {
        "rows": rows,
        "manifest": {
            "sources": {
                name: {
                    "rows": counts[name],
                    "url": SOURCES[name][0] if name in SOURCES else None,
                }
                for name in requested
            },
            "label_names": len(labels),
            "label_sets": {
                "count": len(label_sets),
                "sha256": oracle.sha256_file(args.label_sets),
            }
            if args.label_sets
            else None,
            "entity_types": len(types),
            "wikipedia": {
                "sha256": WIKIPEDIA_SHA256,
                "passage_words": args.passage_words,
            }
            if "wikipedia" in requested
            else None,
        },
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    import antenna_datasets as datasets

    provenance, _ = oracle.prepare_runtime(args.upstream)
    if args.source:
        mix = build_mix(args)
        return write(
            args, mix["rows"], {**mix["manifest"], "text_sets": None}, provenance
        )
    from gliner2.processing.word_splitter import WhitespaceTokenSplitter

    splitter = WhitespaceTokenSplitter()
    rng = random.Random(args.seed)
    labels = sorted(
        {name for dataset in LABEL_SETS for name in datasets.label_names(dataset)}
        | {kind for dataset in TYPE_SETS for kind in datasets.entity_types(dataset)}
    )
    types = sorted(
        {kind for dataset in TYPE_SETS for kind in datasets.entity_types(dataset)}
        | set(GENERIC_TYPES)
    )
    texts = [
        record["text"]
        for dataset in TEXT_SETS
        for record in datasets.load_classification(dataset, "train")
    ]
    wikipedia = (
        wikipedia_passages(args.wikipedia, args.passage_words) if args.wikipedia else []
    )
    rng.shuffle(texts)
    rng.shuffle(wikipedia)
    if wikipedia:
        # Interleave so any prefix of the pool mixes both sources in proportion.
        share = len(wikipedia) / (len(wikipedia) + len(texts))
        mixed, wi, ti = [], 0, 0
        while wi < len(wikipedia) or ti < len(texts):
            if ti >= len(texts) or (wi < len(wikipedia) and rng.random() < share):
                mixed.append(wikipedia[wi])
                wi += 1
            else:
                mixed.append(texts[ti])
                ti += 1
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
            schema = {
                "classifications": [
                    {
                        "name": rng.choice(TASK_NAMES),
                        "labels": rng.sample(labels, rng.randint(4, 16)),
                    }
                ]
            }
        rows.append(
            {"version": 1, "id": f"pool-{len(rows)}", "text": text, "schema": schema}
        )
    return write(
        args,
        rows,
        {
            "text_sets": TEXT_SETS,
            "label_names": len(labels),
            "entity_types": len(types),
            "wikipedia": {
                "sha256": WIKIPEDIA_SHA256,
                "passages": len(wikipedia),
                "passage_words": args.passage_words,
            }
            if args.wikipedia
            else None,
        },
        provenance,
    )


def write(
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    details: dict[str, Any],
    provenance: dict[str, Any],
) -> dict[str, Any]:
    import antenna_datasets as datasets

    validation_count = max(1, int(len(rows) * args.validation_fraction))
    # Rows sharing a text (an Open-Jev state's questions) stay in one split:
    # the trainer rejects a text present in both.
    held = {row["text"] for row in rows[:validation_count]}
    splits = {
        "validation": [row for row in rows if row["text"] in held],
        "train": [row for row in rows if row["text"] not in held],
    }
    with oracle.atomic_output_directory(args.output) as directory:
        files = {}
        for split, items in splits.items():
            path = directory / f"{split}.jsonl"
            path.write_text(
                "".join(
                    json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n"
                    for item in items
                ),
                encoding="utf-8",
            )
            files[split] = {
                "path": path.name,
                "records": len(items),
                "sha256": oracle.sha256_file(path),
            }
        oracle.write_json(
            directory / "manifest.json",
            {
                "dataset_format": "gliner_boundary_dataset.Row/version=1",
                "purpose": "antenna feature distillation (unlabeled)",
                "files": files,
                **details,
                "entity_share": args.entity_share,
                "seed": args.seed,
                "max_words": args.max_words,
                "generator_sha256": oracle.sha256_file(Path(__file__)),
                "datasets_module_sha256": oracle.sha256_file(Path(datasets.__file__)),
                "provenance": provenance,
            },
        )
        oracle.verify_upstream_checkout(args.upstream)
    return {
        "status": "built",
        "output": str(args.output.resolve()),
        **{k: v["records"] for k, v in files.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=80000)
    parser.add_argument("--entity-share", type=float, default=0.5)
    parser.add_argument("--validation-fraction", type=float, default=0.01)
    parser.add_argument("--max-words", type=int, default=128)
    parser.add_argument("--seed", type=int, default=20260926)
    parser.add_argument(
        "--wikipedia",
        type=Path,
        help="wiki-articles-10k-v001.json (cdn.antfly.io/datasets/)",
    )
    parser.add_argument("--passage-words", type=int, default=100)
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        metavar="NAME=ROWS",
        help=f"sampled source mix instead of the default texts: {', '.join(TEXT_SETS + ('wikipedia',) + tuple(SOURCES) + TRAINING_SET_SOURCES)}",
    )
    parser.add_argument(
        "--label-sets",
        type=Path,
        help="coherent classification label sets (label_sets.json); --source mixes only",
    )
    print(json.dumps(build(parser.parse_args()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
