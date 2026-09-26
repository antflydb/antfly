#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Evaluate a GLiNER2 checkpoint (span or boundary) on the Antenna baselines.

Classification (accuracy, macro-F1, ECE): banking77, clinc150, ag_news, sst5,
and typed_decisions (per question; also soft cross-entropy against the gold
distribution). Zero-shot NER (exact span and type micro P/R/F1): CrossNER
ai/literature/music/politics/science, MIT restaurant and movie. Datasets and
label names come from ``antenna_datasets`` so evaluation matches training.

Each dataset is a fixed seeded subsample of its test split (500 classification
records, 300 NER sentences, all typed-decisions questions) unless ``--full``.
The report records the sampled ids, source pins, model revision and weight
SHA-256, device, versions and wall time.

Classification scores every label in one pass with the upstream
``Classifier`` (``probability`` is a softmax for single-label tasks); NER uses
``batch_extract_entities`` at threshold 0.5. The upstream checkout is imported
from ``--upstream`` (scripts/gliner25/oracle.py pins the commit).

    python baselines.py --model-dir <checkpoint dir> [--model-id fastino/GLiNER2.5-Decide --revision <sha>] \\
        --upstream <GLiNER2 checkout> --output report.json

Any boundary or span checkpoint directory loads, including students exported
by the native trainer; its weights are identified by SHA-256. The report
groups datasets into in-domain (the pilot's training datasets) and held-out.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import random
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import antenna_datasets as datasets  # noqa: E402

CLASSIFICATION_TASKS = {"banking77": "intent", "clinc150": "intent", "ag_news": "topic", "sst5": "sentiment"}
# The Antenna pilot trains on these datasets' train splits; the rest stay
# held out for zero-shot evaluation.
IN_DOMAIN = ("banking77", "ag_news", "crossner_ai", "crossner_literature", "crossner_music", "mit_restaurant")
HELD_OUT = ("clinc150", "sst5", "typed_decisions", "crossner_politics", "crossner_science", "mit_movie")
DEFAULT_SEED = 20260925
UPSTREAM_COMMIT = "3c913c7369301133d3b7699252074c4303ada50e"


def sample(records: list[dict], limit: int | None, seed: int) -> list[dict]:
    if limit is None or limit >= len(records):
        return records
    indices = sorted(random.Random(seed).sample(range(len(records)), limit))
    return [records[i] for i in indices]


def ece(confidences: list[float], correct: list[bool], bins: int = 15) -> float:
    total = len(confidences)
    error = 0.0
    for b in range(bins):
        lo, hi = b / bins, (b + 1) / bins
        members = [i for i, c in enumerate(confidences) if (lo < c <= hi) or (b == 0 and c == 0)]
        if members:
            accuracy = sum(correct[i] for i in members) / len(members)
            confidence = sum(confidences[i] for i in members) / len(members)
            error += len(members) / total * abs(accuracy - confidence)
    return error


def macro_f1(gold: list[str], predicted: list[str], labels: list[str]) -> float:
    scores = []
    for label in labels:
        tp = sum(g == label and p == label for g, p in zip(gold, predicted))
        fp = sum(g != label and p == label for g, p in zip(gold, predicted))
        fn = sum(g == label and p != label for g, p in zip(gold, predicted))
        if tp + fp + fn:
            scores.append(2 * tp / (2 * tp + fp + fn))
    return sum(scores) / len(scores) if scores else 0.0


def classification_schema(record: dict, name: str, labels: list[str]) -> Any:
    from gliner2.classification import ClassificationSchema

    schema = ClassificationSchema()
    if name != "typed_decisions":
        return schema.single(CLASSIFICATION_TASKS[name], labels)
    task = datasets._natural(record["id"].rsplit("/", 1)[1])
    described = {label: (description or None) for label, description in zip(record["labels"], record["descriptions"])}
    if record["kind"] == "score":
        # Score levels are already named by their descriptions.
        return schema.ordinal(task, record["labels"], instruction=record["task"])
    return schema.single(task, described, instruction=record["task"])


def evaluate_classification(model: Any, name: str, records: list[dict], batch_size: int, device: str) -> dict:
    from gliner2.classification import Classifier, ClassificationConfig

    classifier = Classifier(model, device=device)
    config = ClassificationConfig(batch_size=batch_size)
    fixed = datasets.label_names(name)
    gold, predicted, confidences, correct, cross_entropy, by_kind, rows = [], [], [], [], [], {}, []
    started = time.perf_counter()
    for offset in range(0, len(records), batch_size):
        chunk = records[offset:offset + batch_size]
        schemas = [classification_schema(r, name, fixed or r["labels"]) for r in chunk]
        # Per-record schemas (typed_decisions) go straight to the scorer.
        compiled = [classifier.compile_schema(schema) for schema in schemas]
        scores = classifier.scorer.batch_score([r["text"] for r in chunk], compiled, batch_size=batch_size,
                                               max_len=config.max_len)
        for record, score, schema in zip(chunk, scores, schemas):
            task = schema.task_order[0]
            labels = fixed or record["labels"]
            probabilities = [score.probability(task, label) for label in labels]
            best = max(range(len(labels)), key=lambda i: probabilities[i])
            gold.append(record["label"])
            predicted.append(labels[best])
            confidences.append(probabilities[best])
            correct.append(labels[best] == record["label"])
            row = {"id": record["id"], "predicted": labels[best], "confidence": probabilities[best], "correct": correct[-1]}
            if "target" in record:
                cross_entropy.append(-sum(t * math.log(max(p, 1e-12)) for t, p in zip(record["target"], probabilities)))
                row["soft_cross_entropy"] = cross_entropy[-1]
                kind = by_kind.setdefault(record["kind"], [0, 0])
                kind[0] += correct[-1]
                kind[1] += 1
            rows.append(row)
    result = {
        "task": "classification", "records": len(records),
        "accuracy": sum(correct) / len(correct),
        "macro_f1": macro_f1(gold, predicted, fixed or sorted(set(gold) | set(predicted))),
        "ece": ece(confidences, correct), "seconds": time.perf_counter() - started,
        # Per-record predictions allow comparisons on another model's admissible subset.
        "predictions": rows,
    }
    if cross_entropy:
        result["soft_cross_entropy"] = sum(cross_entropy) / len(cross_entropy)
        result["accuracy_by_kind"] = {kind: hits / total for kind, (hits, total) in sorted(by_kind.items())}
    return result


def evaluate_ner(model: Any, name: str, records: list[dict], batch_size: int) -> dict:
    types = datasets.entity_types(name)
    started = time.perf_counter()
    outputs = model.batch_extract_entities([r["text"] for r in records], types, batch_size=batch_size,
                                           threshold=0.5, include_spans=True, include_confidence=True)
    tp = fp = fn = 0
    for record, output in zip(records, outputs):
        text = record["text"]
        gold = {(e["start"], e["end"], e["type"]) for e in record["entities"]}
        found = set()
        for entity_type, spans in output["entities"].items():
            for span in spans:
                # Upstream reports character offsets; the datasets use UTF-8 bytes.
                start = len(text[:span["start"]].encode("utf-8"))
                end = len(text[:span["end"]].encode("utf-8"))
                found.add((start, end, entity_type))
        tp += len(gold & found)
        fp += len(found - gold)
        fn += len(gold - found)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {"task": "ner", "records": len(records), "precision": precision, "recall": recall,
            "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
            "gold_entities": tp + fn, "seconds": time.perf_counter() - started}


def summarize(results: dict[str, dict]) -> dict:
    """Mean classification accuracy and NER F1 over the in-domain and held-out sets."""
    groups = {}
    for group, names in (("in_domain", IN_DOMAIN), ("held_out", HELD_OUT)):
        # Errors and not-applicable datasets carry no metrics.
        present = [n for n in names if n in results and "task" in results[n]]
        classification = [results[n]["accuracy"] for n in present if results[n]["task"] == "classification"]
        ner = [results[n]["f1"] for n in present if results[n]["task"] == "ner"]
        groups[group] = {
            "datasets": list(names), "evaluated": present,
            "mean_classification_accuracy": sum(classification) / len(classification) if classification else None,
            "mean_ner_f1": sum(ner) / len(ner) if ner else None,
        }
    return groups


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--model-dir", "--model", dest="model", type=Path, required=True, help="checkpoint directory")
    parser.add_argument("--model-id", help="Hub id, when the directory is a pinned download")
    parser.add_argument("--revision", help="Hub revision, when the directory is a pinned download")
    parser.add_argument("--upstream", type=Path, required=True, help="GLiNER2 checkout at the pinned commit")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--datasets", nargs="*", default=list(datasets.CLASSIFICATION + datasets.NER))
    parser.add_argument("--classification-limit", type=int, default=500)
    parser.add_argument("--ner-limit", type=int, default=300)
    parser.add_argument("--full", action="store_true", help="evaluate complete test splits")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--device", default="mps", choices=("cpu", "mps"))
    args = parser.parse_args()

    commit = subprocess.run(["git", "-C", str(args.upstream), "rev-parse", "HEAD"], check=True,
                            capture_output=True, text=True).stdout.strip()
    if commit != UPSTREAM_COMMIT:
        parser.error(f"upstream checkout is at {commit}, expected {UPSTREAM_COMMIT}")
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.upstream.resolve()))
    import torch
    import transformers
    from gliner2 import AutoExtractor

    model = AutoExtractor.from_pretrained(str(args.model), local_files_only=True, map_location="cpu",
                                          use_flashdeberta=False).float().eval()
    model.to(args.device)
    report: dict[str, Any] = {
        "format_version": 1,
        "model": {"id": args.model_id, "revision": args.revision, "directory": str(args.model.resolve()),
                  "architecture": getattr(model, "architecture", None),
                  "encoder": getattr(getattr(model, "encoder", None), "config", None).model_type
                  if getattr(model, "encoder", None) is not None else None,
                  "weights_sha256": sha256(args.model / "model.safetensors")},
        "upstream_commit": commit, "device": args.device, "seed": args.seed, "full": args.full,
        "versions": {"python": platform.python_version(), "torch": torch.__version__,
                     "transformers": transformers.__version__, "machine": platform.machine()},
        "datasets": {},
    }
    started = time.perf_counter()
    for name in args.datasets:
        ner = name in datasets.NER
        records = datasets.load_ner(name, "test") if ner else datasets.load_classification(name, "test")
        limit = None if args.full or name == "typed_decisions" else (args.ner_limit if ner else args.classification_limit)
        chosen = sample(records, limit, args.seed)
        try:
            with torch.inference_mode():
                result = evaluate_ner(model, name, chosen, args.batch_size) if ner else \
                    evaluate_classification(model, name, chosen, args.batch_size, args.device)
        except Exception as error:  # noqa: BLE001 - record the failure, keep evaluating
            result = {"error": f"{type(error).__name__}: {error}"}
        result["test_records"] = len(records)
        result["sample_ids"] = [r["id"] for r in chosen]
        result["sources"] = datasets.source_pins(name)
        report["datasets"][name] = result
        print(json.dumps({"dataset": name, **{k: v for k, v in result.items() if k not in ("sample_ids", "sources", "predictions")}}), flush=True)
    report["groups"] = summarize(report["datasets"])
    report["seconds"] = time.perf_counter() - started
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
