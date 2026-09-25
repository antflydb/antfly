#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Evaluate a Laya checkpoint on the Antenna classification baselines.

Converts the same seeded test subsamples as ``baselines.py`` into native Laya
choice records (one question per text, labels named as in
``antenna_datasets``; typed-decisions keeps its per-question labels,
descriptions and gold distribution, as scripts/laya/prepare_laya_finetune.py
does) and scores them with ``antfly-inference finetune eval laya`` through the
serving pipeline (models/laya/LAYA.md). A dataset with more labels than the
model admits (20, or 255 when candidate-packed) is recorded as not applicable
rather than truncated. Typed-decisions questions whose state exceeds 316 Laya
tokens are dropped, as in LAYA.md's step-0 evaluation (the unpacked
512-token budget reserves 192 for the question); the report counts them.

    python laya_baselines.py --binary <antfly-inference> --model-dir <laya dir> \\
        --output report.json [--backend metal|native]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import antenna_datasets as datasets  # noqa: E402
import baselines  # noqa: E402

INSTRUCTIONS = {
    "banking77": "Which banking intent does this customer message express?",
    "clinc150": "Which intent does this request express?",
    "ag_news": "Which topic is this news article about?",
    "sst5": "What is the sentiment of this movie review?",
}


def laya_records(name: str, records: list[dict]) -> list[dict]:
    if name == "typed_decisions":
        return [{"id": r["id"], "group_id": r["group_id"], "text": r["text"], "kind": r["kind"],
                 "instruction": r["task"], "labels": r["keys"], "descriptions": r["descriptions"],
                 "target": r["target"]} for r in records]
    labels = datasets.label_names(name)
    return [{"id": r["id"], "group_id": r["id"], "text": r["text"], "kind": "choice",
             "instruction": INSTRUCTIONS[name], "labels": labels, "descriptions": [""] * len(labels),
             "target": [1.0 if label == r["label"] else 0.0 for label in labels]} for r in records]


MAX_STATE_TOKENS = 316


def admissible(model_dir: Path, records: list[dict]) -> list[dict]:
    """Typed-decisions questions whose state fits Laya's unpacked budget."""
    from tokenizers import Tokenizer

    tokenizer = Tokenizer.from_file(str(model_dir / "tokenizer.json"))
    return [r for r in records if len(tokenizer.encode(r["text"], add_special_tokens=False).ids) <= MAX_STATE_TOKENS]


def max_options(model_dir: Path) -> int:
    config = json.loads((model_dir / "config.json").read_text()).get("laya", {})
    packing = config.get("packing") or {}
    mode = packing.get("mode") if isinstance(packing, dict) else packing
    return 255 if mode == "candidate" else 20


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", type=Path, required=True, help="antfly-inference built from this tree")
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--datasets", nargs="*", default=list(datasets.CLASSIFICATION))
    parser.add_argument("--classification-limit", type=int, default=500)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--seed", type=int, default=baselines.DEFAULT_SEED)
    parser.add_argument("--backend", default="metal", choices=("metal", "native"))
    args = parser.parse_args()
    limit_options = max_options(args.model_dir)
    report = {"format_version": 1, "model": {"directory": str(args.model_dir.resolve()),
              "weights_sha256": baselines.sha256(args.model_dir / "model.safetensors"),
              "max_options": limit_options}, "backend": args.backend, "seed": args.seed, "datasets": {}}
    started = time.perf_counter()
    with tempfile.TemporaryDirectory() as scratch:
        for name in args.datasets:
            records = datasets.load_classification(name, "test")
            limit = None if args.full or name == "typed_decisions" else args.classification_limit
            chosen = baselines.sample(records, limit, args.seed)
            dropped = 0
            if name == "typed_decisions":
                kept = admissible(args.model_dir, chosen)
                dropped = len(chosen) - len(kept)
                chosen = kept
            converted = laya_records(name, chosen)
            widest = max(len(r["labels"]) for r in converted)
            result: dict = {"test_records": len(records), "sample_ids": [r["id"] for r in chosen]}
            if dropped:
                result["dropped_over_state_budget"] = dropped
            if widest > limit_options:
                result["not_applicable"] = f"{widest} labels exceed the model's {limit_options}"
            else:
                path = Path(scratch) / f"{name}.jsonl"
                path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in converted))
                run = subprocess.run([str(args.binary), "finetune", "eval", "laya", str(args.model_dir), str(path),
                                      "--backend", args.backend], capture_output=True, text=True)
                if run.returncode:
                    result["error"] = run.stderr[-2000:]
                else:
                    native = json.loads(run.stdout)
                    result.update({"task": "classification", "records": native["overall"]["decisions"],
                                   "accuracy": native["overall"]["accuracy"], "soft_cross_entropy": native["overall"]["soft_ce"],
                                   "ece": native["overall"]["ece"], "seconds": native["seconds"],
                                   "packing": native["packing"],
                                   "records_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
                    kinds = {k: native[k]["accuracy"] for k in ("choice", "score", "noul") if native.get(k)}
                    if name == "typed_decisions":
                        result["accuracy_by_kind"] = kinds
            report["datasets"][name] = result
            print(json.dumps({"dataset": name, **{k: v for k, v in result.items() if k != "sample_ids"}}), flush=True)
    report["groups"] = baselines.summarize(report["datasets"])
    report["seconds"] = time.perf_counter() - started
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
