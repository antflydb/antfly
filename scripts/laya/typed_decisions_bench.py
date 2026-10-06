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

# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Compare decision models on the typed-decisions test split.

The community benchmark for System One models (Laya, OpenDecider, Jev) is
LocalLLaMA/typed-decisions `all/test`: 400 cases, 2,000 decisions, scored per
decision as argmax == gold (the Antz AI harness). `prepare_laya_training_data.sh`
writes it as native records (`td/eval.jsonl`).

Each model contributes a predictions file, one JSON line per decision with
`id` ("<case>/<question>"), `kind`, `labels`, `probabilities` and `target`:

  antfly inference finetune eval laya <model> td/eval.jsonl --predictions a.jsonl
  uv run --with opendecider scripts/laya/typed_decisions_bench.py reference \\
      td/eval.jsonl --model manjunathshiva/opendecider-nano --output b.jsonl
  uv run scripts/laya/typed_decisions_bench.py report b.jsonl a.jsonl \\
      --gold td/eval-cases.jsonl

`report` prints accuracy (overall and per kind), Brier score and 10-bin ECE for
each file, a 95% paired bootstrap interval over cases for each file's accuracy
gap to the first, and the largest probability difference to the first (a parity
check when two files run the same model).

A decision is correct when its top label is the gold label. With `--gold`
(the typed-decisions cases, as written by `prepare_laya_training_data.sh`)
that is the dataset's `gold[question].label`, as the Antz AI harness scores
it; without it, the argmax of the record's target distribution, as
`finetune eval laya` scores it. The two disagree on 31 of the 2,000 test
decisions.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

KINDS = ("choice", "score", "noul")


def load(path: Path) -> dict[str, dict]:
    rows = {}
    for line in path.read_text().splitlines():
        if line.strip():
            row = json.loads(line)
            if row["id"] in rows:
                raise ValueError(f"{path}: duplicate id {row['id']}")
            rows[row["id"]] = row
    return rows


def argmax(values) -> int:
    return max(range(len(values)), key=values.__getitem__)


def gold_index(row) -> int:
    return row["gold"] if "gold" in row else argmax(row["target"])


def correct(row) -> bool:
    return argmax(row["probabilities"]) == gold_index(row)


def gold_labels(path: Path) -> dict[str, str]:
    labels = {}
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        case = json.loads(line)
        gold = case["gold"]
        gold = json.loads(gold) if isinstance(gold, str) else gold
        for question, answer in gold.items():
            labels[f"{case['id']}/{question}"] = str(answer["label"]).lower()
    return labels


def ece(rows, bins=10) -> float:
    buckets = [[] for _ in range(bins)]
    for row in rows:
        confidence = max(row["probabilities"])
        buckets[min(bins - 1, int(confidence * bins))].append(
            (confidence, correct(row))
        )
    total = len(rows)
    return sum(
        len(b)
        / total
        * abs(sum(c for c, _ in b) / len(b) - sum(k for _, k in b) / len(b))
        for b in buckets
        if b
    )


def brier(rows) -> float:
    gold = [gold_index(row) for row in rows]
    return sum(
        sum((p - (i == g)) ** 2 for i, p in enumerate(row["probabilities"]))
        for row, g in zip(rows, gold)
    ) / len(rows)


def paired_interval(
    base: dict, other: dict, ids: list[str], draws=2000, seed=13
) -> tuple[float, float]:
    cases: dict[str, list[str]] = {}
    for i in ids:
        cases.setdefault(i.rsplit("/", 1)[0], []).append(i)
    groups = list(cases.values())
    rng = random.Random(seed)
    gaps = []
    for _ in range(draws):
        sample = [i for g in rng.choices(groups, k=len(groups)) for i in g]
        gaps.append(
            sum(correct(other[i]) - correct(base[i]) for i in sample) / len(sample)
        )
    gaps.sort()
    return gaps[int(0.025 * draws)], gaps[int(0.975 * draws) - 1]


def report(paths: list[Path], gold: Path | None) -> None:
    files = [load(p) for p in paths]
    ids = sorted(files[0])
    if gold:
        labels = gold_labels(gold)
        for rows in files:
            for i, row in rows.items():
                row["gold"] = [label.lower() for label in row["labels"]].index(
                    labels[i]
                )
    for path, rows in zip(paths[1:], files[1:]):
        if set(rows) != set(ids):
            raise ValueError(f"{path} covers different decisions than {paths[0]}")
        for i in ids:
            if gold_index(rows[i]) != gold_index(files[0][i]):
                raise ValueError(f"{path}: gold for {i} differs from {paths[0]}")
    print(f"{len(ids)} decisions, {len({i.rsplit('/', 1)[0] for i in ids})} cases")
    print(
        "| predictions | accuracy | choice | score | yes/no | Brier | ECE | gap to first (95% CI) | max prob diff |"
    )
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |")
    for n, (path, rows) in enumerate(zip(paths, files)):
        ordered = [rows[i] for i in ids]
        by_kind = []
        for kind in KINDS:
            subset = [r for r in ordered if r["kind"] == kind]
            by_kind.append(
                f"{sum(map(correct, subset)) / len(subset):.3f}" if subset else ""
            )
        accuracy = sum(map(correct, ordered)) / len(ordered)
        gap, diff = "", ""
        if n > 0:
            lo, hi = paired_interval(files[0], rows, ids)
            base = sum(correct(files[0][i]) for i in ids) / len(ids)
            gap = f"{accuracy - base:+.3f} [{lo:+.3f}, {hi:+.3f}]"
            diff = f"{max(max(abs(a - b) for a, b in zip(rows[i]['probabilities'], files[0][i]['probabilities'])) for i in ids):.2e}"
        print(
            f"| {path.name} | {accuracy:.3f} | {' | '.join(by_kind)} | {brier(ordered):.3f} | {ece(ordered):.3f} | {gap} | {diff} |"
        )


def reference(records: Path, model: str, output: Path, batch: int) -> None:
    """Run OpenDecider-nano's own PyTorch implementation on native records."""
    from opendecider import load as load_model

    impl = load_model(model).impl
    rows = [
        json.loads(line) for line in records.read_text().splitlines() if line.strip()
    ]
    with output.open("x") as out:
        for start in range(0, len(rows), batch):
            items, names = [], []
            for row in rows[start : start + batch]:
                labels = row["labels"]
                descriptions = row.get("descriptions") or [""] * len(labels)
                if row["kind"] == "noul":
                    # OpenDecider's yes/no names, "yes" first (questions.options).
                    opts = {
                        "yes": descriptions[1] or "Yes",
                        "no": descriptions[0] or "No",
                    }
                    names.append(["no", "yes"])
                else:
                    opts = {
                        label: description or None
                        for label, description in zip(labels, descriptions)
                    }
                    names.append(labels)
                items.append((row["text"], row["instruction"], opts))
            for row, probs, order in zip(
                rows[start : start + batch], impl.decide_many(items), names
            ):
                out.write(
                    json.dumps(
                        {
                            "id": row["id"],
                            "kind": row["kind"],
                            "labels": row["labels"],
                            "probabilities": [probs[name] for name in order],
                            "target": row["target"],
                        }
                    )
                    + "\n"
                )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    rep = sub.add_parser("report", help="score and compare predictions files")
    rep.add_argument("predictions", type=Path, nargs="+")
    rep.add_argument("--gold", type=Path, help="typed-decisions cases with gold labels")
    ref = sub.add_parser(
        "reference", help="OpenDecider-nano predictions from its own PyTorch code"
    )
    ref.add_argument("records", type=Path)
    ref.add_argument("--model", default="manjunathshiva/opendecider-nano")
    ref.add_argument("--output", type=Path, required=True)
    ref.add_argument("--batch", type=int, default=16)
    args = parser.parse_args()
    if args.command == "report":
        report(args.predictions, args.gold)
    else:
        reference(args.records, args.model, args.output, args.batch)


if __name__ == "__main__":
    main()
