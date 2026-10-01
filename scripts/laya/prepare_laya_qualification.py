#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=21,<26", "torch>=2.6,<3", "transformers>=4.51,<6", "numpy>=2"]
# ///
"""Prepare deterministic, case-disjoint Laya qualification data from local Parquet."""

import argparse
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
from prepare_laya_finetune import convert
from transformers import AutoTokenizer


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("train", "test", "model", "common", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--revision", required=True)
    p.add_argument("--train-cases-per-domain", type=int, default=8)
    p.add_argument("--eval-cases-per-domain", type=int, default=8)
    p.add_argument("--calibration-cases-per-domain", type=int, default=4)
    p.add_argument("--max-tokens", type=int, default=512)
    p.add_argument("--seed", type=int, default=20260921)
    args = p.parse_args()
    if (
        min(
            args.train_cases_per_domain,
            args.eval_cases_per_domain,
            args.calibration_cases_per_domain,
        )
        < 1
    ):
        p.error("Case counts must be positive")
    if args.output.exists():
        p.error("Output must be new")
    spec = importlib.util.spec_from_file_location("laya_common", args.common)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    cfg = json.loads((args.model / "config.json").read_text())["laya"]
    tok = AutoTokenizer.from_pretrained(args.model, local_files_only=True)
    selected = {"train": [], "eval": [], "calibration": []}
    sequences = {key: [] for key in selected}
    counts, excluded = Counter(), Counter()
    used_texts, used_ids = set(), set()
    for source, path in (("train", args.train), ("test", args.test)):
        rows = pq.read_table(path).to_pylist()
        domains = sorted({r["workflow"] for r in rows})
        for domain in domains:
            candidates = [r for r in rows if r["workflow"] == domain]
            candidates.sort(
                key=lambda r: hashlib.sha256(f"{args.seed}/{r['id']}".encode()).digest()
            )
            needed = (
                [("train", args.train_cases_per_domain)]
                if source == "train"
                else [
                    ("eval", args.eval_cases_per_domain),
                    ("calibration", args.calibration_cases_per_domain),
                ]
            )
            for split, count in needed:
                while counts[split, domain] < count:
                    if not candidates:
                        raise ValueError(
                            f"Insufficient eligible cases: {split}/{domain}"
                        )
                    case = candidates.pop(0)
                    records = convert(case)
                    text_hash = hashlib.sha256(records[0]["text"].encode()).hexdigest()
                    if text_hash in used_texts or case["id"] in used_ids:
                        excluded["duplicate_case"] += 1
                        continue
                    prepared = []
                    for r in records:
                        criteria = (
                            r["descriptions"]
                            if r["kind"] == "score"
                            else dict(zip(r["labels"], r["descriptions"]))
                        )
                        ids, markers = common.build_sequence(
                            tok,
                            r["text"],
                            {"t": r["kind"], "ins": r["instruction"], "crit": criteria},
                            100000,
                            cfg["head_max_len"],
                        )
                        prepared.append(
                            {
                                "id": r["id"],
                                "domain": domain,
                                "ids": ids,
                                "markers": markers,
                                "qtype": common.QTYPES[r["kind"]],
                            }
                        )
                    if any(
                        len(r["ids"]) > min(cfg["max_len"], args.max_tokens)
                        for r in prepared
                    ):
                        excluded[f"{source}/{domain}/overlength_case"] += 1
                        continue
                    used_texts.add(text_hash)
                    used_ids.add(case["id"])
                    selected[split].extend(records)
                    sequences[split].extend(prepared)
                    counts[split, domain] += 1
    args.output.mkdir(parents=True)
    for split, records in selected.items():
        (args.output / f"{split}.jsonl").write_text(
            "".join(
                json.dumps(r, ensure_ascii=False, allow_nan=False) + "\n"
                for r in records
            )
        )
        (args.output / f"{split}_sequences.json").write_text(
            json.dumps(sequences[split]) + "\n"
        )
    manifest = {
        "dataset": "LocalLLaMA/typed-decisions",
        "revision": args.revision,
        "seed": args.seed,
        "source_sha256": {
            str(path): sha(path) for path in (args.train, args.test, args.common)
        },
        "max_tokens": args.max_tokens,
        "excluded": dict(excluded),
        "splits": {
            split: {
                "decisions": len(records),
                "cases": len({r["group_id"] for r in records}),
                "by_kind": dict(Counter(r["kind"] for r in records)),
                "by_domain": {
                    domain: count for (s, domain), count in counts.items() if s == split
                },
                "max_sequence": max(len(r["ids"]) for r in sequences[split]),
                "sha256": sha(args.output / f"{split}.jsonl"),
            }
            for split, records in selected.items()
        },
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
