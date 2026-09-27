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
``--entity-share``, a random entity schema (2-10 types). Rows are native
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

import oracle  # noqa: E402

TEXT_SETS = ("ag_news", "banking77")
LABEL_SETS = ("banking77", "ag_news")
TYPE_SETS = ("crossner_ai", "crossner_literature", "crossner_music", "mit_restaurant")
GENERIC_TYPES = ("person", "organization", "location", "date", "product", "event", "money", "country", "city", "company")
TASK_NAMES = ("intent", "topic", "category", "type")
WIKIPEDIA_SHA256 = "a446ebb8721ce4dd262a8320ba65479a21b361140c639d8a6c4e198892382623"  # wiki-articles-10k-v001.json


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


def build(args: argparse.Namespace) -> dict[str, Any]:
    import antenna_datasets as datasets

    provenance, _ = oracle.prepare_runtime(args.upstream)
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
            "files": files, "text_sets": TEXT_SETS, "label_names": len(labels), "entity_types": len(types),
            "entity_share": args.entity_share, "seed": args.seed, "max_words": args.max_words,
            "wikipedia": {"sha256": WIKIPEDIA_SHA256, "passages": len(wikipedia), "passage_words": args.passage_words} if args.wikipedia else None,
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
    print(json.dumps(build(parser.parse_args()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
