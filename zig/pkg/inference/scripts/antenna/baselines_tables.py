#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Markdown tables from ``baselines.py`` and ``laya_baselines.py`` reports.

    python baselines_tables.py name=report.json [name=report.json ...] \\
        [--typed-subset name] [--override typed_decisions=name:report.json ...]

``--override`` replaces one dataset's result in a report (for example a
typed-decisions rerun). ``--typed-subset`` also scores every report that has
typed-decisions predictions on the questions the named report evaluated (for
a model with a smaller input budget).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import baselines  # noqa: E402


def cell(result: dict | None) -> str:
    if result is None:
        return ""
    if "not_applicable" in result:
        return "n/a"
    if "error" in result:
        return "error"
    if result["task"] == "ner":
        return f"{result['f1']:.3f}"
    return f"{result['accuracy']:.3f}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("reports", nargs="+", help="name=path")
    parser.add_argument("--override", action="append", default=[], help="dataset=name:path")
    parser.add_argument("--typed-subset", help="report name whose typed-decisions questions define the subset")
    args = parser.parse_args()
    reports = {}
    for item in args.reports:
        name, path = item.split("=", 1)
        reports[name] = json.loads(Path(path).read_text())
    for item in args.override:
        dataset, rest = item.split("=", 1)
        name, path = rest.split(":", 1)
        reports[name]["datasets"][dataset] = json.loads(Path(path).read_text())["datasets"][dataset]
    for report in reports.values():
        report["groups"] = baselines.summarize(report["datasets"])

    names = list(reports)
    print("Accuracy (classification) or exact-span micro F1 (NER).\n")
    print("| Group | Dataset | " + " | ".join(names) + " |")
    print("|---|---|" + "---:|" * len(names))
    for group, datasets in (("in-domain", baselines.IN_DOMAIN), ("held-out", baselines.HELD_OUT)):
        for dataset in datasets:
            print(f"| {group} | {dataset} | " + " | ".join(cell(r["datasets"].get(dataset)) for r in reports.values()) + " |")
    for group in ("in_domain", "held_out"):
        for metric, label in (("mean_classification_accuracy", "mean classification accuracy"), ("mean_ner_f1", "mean NER F1")):
            values = [r["groups"][group][metric] for r in reports.values()]
            print(f"| {group.replace('_', '-')} | {label} | " + " | ".join("" if v is None else f"{v:.3f}" for v in values) + " |")

    print("\nClassification calibration (ECE, 15 bins) and macro-F1 where reported.\n")
    print("| Dataset | " + " | ".join(names) + " |")
    print("|---|" + "---:|" * len(names))
    for dataset in ("banking77", "clinc150", "ag_news", "sst5", "typed_decisions"):
        cells = []
        for r in reports.values():
            result = r["datasets"].get(dataset)
            if not result or "task" not in result:
                cells.append(cell(result))
                continue
            text = f"ECE {result['ece']:.3f}"
            if "macro_f1" in result:
                text += f", mF1 {result['macro_f1']:.3f}"
            if "soft_cross_entropy" in result:
                text += f", sCE {result['soft_cross_entropy']:.3f}"
            cells.append(text)
        print(f"| {dataset} | " + " | ".join(cells) + " |")

    if args.typed_subset:
        subset = set(reports[args.typed_subset]["datasets"]["typed_decisions"]["sample_ids"])
        print(f"\nTyped-decisions on the {len(subset)} questions {args.typed_subset} admits.\n")
        print("| Model | Accuracy | Soft CE | Questions |")
        print("|---|---:|---:|---:|")
        for name, r in reports.items():
            result = r["datasets"].get("typed_decisions") or {}
            rows = [p for p in result.get("predictions", []) if p["id"] in subset]
            if rows:
                accuracy = sum(p["correct"] for p in rows) / len(rows)
                ce = sum(p["soft_cross_entropy"] for p in rows) / len(rows)
                print(f"| {name} | {accuracy:.3f} | {ce:.3f} | {len(rows)} |")
            elif result.get("task") and set(result["sample_ids"]) == subset:
                print(f"| {name} | {result['accuracy']:.3f} | {result['soft_cross_entropy']:.3f} | {result['records']} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
