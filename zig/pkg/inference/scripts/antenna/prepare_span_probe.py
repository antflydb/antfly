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

"""Build a small, content-disjoint ModernBERT span-head repair probe.

Reuse Antenna's pinned CrossNER loader and version-1 training rows. Add real
FewRel endpoints at one fixed source revision; no model-generated labels enter
this probe. This is a head ablation, not the full Antenna distillation recipe.
"""

import argparse
import hashlib
import json
import random
from pathlib import Path

import antenna_datasets as datasets

FEWREL_COMMIT = "278a2315d2138810a379cd8d5718914dc56e2582"
PINS = {
    "fewrel-train.json": "8e9e58a05d559be773cbe18a8d4a16ba17e6d397663f9f05a056f894a94b92c1",
    "fewrel-validation.json": "76bb44c2485b6faed358a3ce040eadafcc8a8835245cd7e6afc1bef672d4a0bf",
    "fewrel-labels.json": "eeedd25512a8e4c2bc06b78009836d27de81807bcba6ed929debd5cd713c07ea",
}


def read_pinned(directory, name):
    value = (directory / name).read_bytes()
    if hashlib.sha256(value).hexdigest() != PINS[name]:
        raise ValueError(f"FewRel source drift: {name}")
    return json.loads(value)


def ner(split, maximum, seed):
    rows = datasets.load_ner("crossner_ai", split)
    random.Random(seed).shuffle(rows)
    selected, excluded = [], []
    for row in rows:
        text = row["text"].encode()
        if any(len(text[e["start"] : e["end"]].split()) > 8 for e in row["entities"]):
            excluded.append(row["id"])
            continue
        selected.append(
            {
                "version": 1,
                "id": row["id"],
                "text": row["text"],
                "schema": {"entities": datasets.entity_types("crossner_ai")},
                "entities": [
                    {
                        "id": str(i),
                        "type": e["type"],
                        "span": {
                            "start": e["start"],
                            "end": e["end"],
                            "unit": "utf8_bytes",
                        },
                    }
                    for i, e in enumerate(row["entities"])
                ],
            }
        )
        if len(selected) == maximum:
            break
    return selected, excluded


def endpoint(tokens, value):
    positions = value[2]
    if len(positions) != 1:
        raise ValueError("ambiguous endpoint occurrence")
    indices = positions[0]
    if not 1 <= len(indices) <= 8 or indices != list(
        range(indices[0], indices[-1] + 1)
    ):
        raise ValueError("noncontiguous or over-width endpoint")
    if not 0 <= indices[0] <= indices[-1] < len(tokens):
        raise ValueError("endpoint outside source")
    start = len(" ".join(tokens[: indices[0]]).encode()) + int(indices[0] > 0)
    end = start + len(" ".join(tokens[indices[0] : indices[-1] + 1]).encode())
    return {"span": {"start": start, "end": end, "unit": "utf8_bytes"}}


def relations(source, names, split, types, per_type, seed, seen):
    selected, excluded = [], []
    for pid in types:
        indexed = list(enumerate(source[pid]))
        random.Random(seed).shuffle(indexed)
        count = 0
        for index, row in indexed:
            record_id = f"fewrel/{split}/{pid}/{index}"
            text = " ".join(row["tokens"])
            try:
                if len(row["tokens"]) > 200 or text in seen:
                    raise ValueError("duplicate or over-size record")
                head, tail = (
                    endpoint(row["tokens"], row["h"]),
                    endpoint(row["tokens"], row["t"]),
                )
            except ValueError:
                excluded.append(record_id)
                continue
            seen.add(text)
            selected.append(
                {
                    "version": 1,
                    "id": record_id,
                    "text": text,
                    "schema": {"relations": [{"type": names[pid][0]}]},
                    "relations": [{"type": names[pid][0], "head": head, "tail": tail}],
                }
            )
            count += 1
            if count == per_type:
                break
        if count != per_type:
            raise ValueError(f"insufficient eligible FewRel records: {pid}")
    return selected, excluded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fewrel-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20260925)
    args = parser.parse_args()
    train_source = read_pinned(args.fewrel_dir, "fewrel-train.json")
    eval_source = read_pinned(args.fewrel_dir, "fewrel-validation.json")
    names = read_pinned(args.fewrel_dir, "fewrel-labels.json")
    train, train_excluded = ner("train", 100, args.seed)
    evaluation, eval_excluded = ner("validation", 32, args.seed)
    train_types, eval_types = sorted(train_source), sorted(eval_source)
    random.Random(args.seed).shuffle(train_types)
    random.Random(args.seed).shuffle(eval_types)
    train_types, eval_types = train_types[:8], eval_types[:4]
    assert not set(train_types) & set(eval_types)
    seen = {r["text"] for r in train + evaluation}
    train_rel, train_rel_excluded = relations(
        train_source, names, "train", train_types, 8, args.seed, seen
    )
    eval_rel, eval_rel_excluded = relations(
        eval_source, names, "validation", eval_types, 8, args.seed, seen
    )
    train += train_rel
    evaluation += eval_rel
    assert not {r["text"] for r in train} & {r["text"] for r in evaluation}
    args.output.mkdir(parents=True, exist_ok=False)
    pins = {}
    for name, rows in (("train.jsonl", train), ("evaluation.jsonl", evaluation)):
        value = "".join(
            json.dumps(row, ensure_ascii=False) + "\n" for row in rows
        ).encode()
        (args.output / name).write_bytes(value)
        pins[name] = {
            "sha256": hashlib.sha256(value).hexdigest(),
            "size_bytes": len(value),
            "records": len(rows),
        }
    receipt = {
        "scope": "frozen-encoder span-head ablation using Antenna training rows",
        "fewrel_commit": FEWREL_COMMIT,
        "fewrel_source_sha256": PINS,
        "seed": args.seed,
        "train_relation_types": train_types,
        "eval_relation_types": eval_types,
        "relation_types_disjoint": True,
        "content_disjoint": True,
        "files": pins,
        "excluded_records": {
            "train_ner": train_excluded,
            "eval_ner": eval_excluded,
            "train_relations": train_rel_excluded,
            "eval_relations": eval_rel_excluded,
        },
        "note": "Whole records with over-eight-word or ambiguous endpoints are excluded; gold spans are never truncated or silently dropped.",
    }
    (args.output / "provenance.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(pins, indent=2))


if __name__ == "__main__":
    main()
