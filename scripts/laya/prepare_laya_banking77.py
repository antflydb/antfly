#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Build native Laya records for Banking77 (77-way intent choice).

    python3 prepare_laya_banking77.py --train train.csv --test test.csv --output <dir>

Inputs are the PolyAI CSVs (text,category), pinned in
prepare_laya_banking77.sh. Every record is one `choice` question with all 77
intents as labels, so only candidate-packed Laya can serve it (unpacked
choice questions are capped at 20 options). Outputs, all disjoint by
normalized text:

  train.jsonl        --train-per-class examples per intent from train.csv
  calibration.jsonl  --calibration-per-class further examples per intent
  eval.jsonl         --eval-cases examples sampled from test.csv

Sampling is seeded. See zig/pkg/inference/models/laya/LAYA.md (step 0b).
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
from collections import defaultdict
from pathlib import Path

INSTRUCTION = "Which banking intent does this customer message express?"


def normalized(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def read(path: Path) -> list[tuple[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        rows = [(row["text"].strip(), row["category"].strip()) for row in csv.DictReader(stream)]
    if not rows or any(not text or not category for text, category in rows):
        raise ValueError(f"Empty text or category in {path}")
    return rows


def record(split: str, index: int, text: str, category: str, labels: list[str]) -> dict:
    target = [1.0 if label == category else 0.0 for label in labels]
    return {
        "id": f"banking77-{split}-{index}/intent",
        "group_id": f"banking77-{split}-{index}",
        "text": text,
        "kind": "choice",
        "instruction": INSTRUCTION,
        "labels": [label.replace("_", " ").lower() for label in labels],
        "descriptions": [""] * len(labels),
        "target": target,
    }


def write(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as out:
        for r in records:
            out.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--train", type=Path, required=True)
    parser.add_argument("--test", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--train-per-class", type=int, default=20)
    parser.add_argument("--calibration-per-class", type=int, default=5)
    parser.add_argument("--eval-cases", type=int, default=400)
    parser.add_argument("--seed", type=int, default=20260925)
    args = parser.parse_args()

    train, test = read(args.train), read(args.test)
    labels = sorted({category for _, category in train})
    if len(labels) != 77 or {category for _, category in test} - set(labels):
        raise ValueError("Expected the 77 Banking77 intents in both splits")
    rng = random.Random(args.seed)

    # Test messages first, so a text shared by both splits stays in eval.
    test_order = list(range(len(test)))
    rng.shuffle(test_order)
    seen, evaluation = set(), []
    for i in test_order:
        key = normalized(test[i][0])
        if key in seen:
            continue
        seen.add(key)
        evaluation.append(record("test", i, *test[i], labels))
        if len(evaluation) == args.eval_cases:
            break

    by_class = defaultdict(list)
    for i, (text, category) in enumerate(train):
        by_class[category].append(i)
    fit, calibration = [], []
    for category in labels:
        indices = by_class[category][:]
        rng.shuffle(indices)
        taken = 0
        for i in indices:
            key = normalized(train[i][0])
            if key in seen:
                continue
            seen.add(key)
            target = fit if taken < args.train_per_class else calibration
            target.append(record("train", i, *train[i], labels))
            taken += 1
            if taken == args.train_per_class + args.calibration_per_class:
                break
        if taken != args.train_per_class + args.calibration_per_class:
            raise ValueError(f"Too few distinct examples for {category}")
    rng.shuffle(fit)

    args.output.mkdir(parents=True, exist_ok=True)
    write(args.output / "train.jsonl", fit)
    write(args.output / "calibration.jsonl", calibration)
    write(args.output / "eval.jsonl", evaluation)
    print(json.dumps({"train": len(fit), "calibration": len(calibration), "eval": len(evaluation)}))


if __name__ == "__main__":
    main()
