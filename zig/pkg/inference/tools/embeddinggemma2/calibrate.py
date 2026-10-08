#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Fit raw cosine abstention thresholds using disjoint fit/validation/holdout splits.

This tool never manufactures probabilities. The artifact binds to an exact
asset identity, renderer, task, dimensions, labels and category prototype set.
Holdout data is used once for qualification, never threshold selection.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def lower_bound(correct, count):
    if not count:
        return 0.0
    z = 1.959963984540054
    p = correct / count
    return (p + z*z/(2*count) - z*math.sqrt(p*(1-p)/count + z*z/(4*count*count))) / (1 + z*z/count)


def validate(data):
    b = data["binding"]
    if set(b) != {"model_identity", "renderer_version", "task_type", "dimensions", "prototype_set_hash", "labels", "mode"}:
        raise ValueError("binding must contain all exact recipe and category fields")
    if b["renderer_version"] != "instruction-category-v1" or b["task_type"] not in ("CLUSTERING", "CLASSIFICATION") or b["dimensions"] not in (128, 256, 512, 768) or b["mode"] not in ("single", "multi"):
        raise ValueError("unsupported calibration binding")
    for key in ("model_identity", "prototype_set_hash"):
        if len(b[key]) != 64 or any(c not in "0123456789abcdef" for c in b[key]):
            raise ValueError(f"invalid {key}")
    labels = b["labels"]
    if len(labels) < (2 if b["mode"] == "single" else 1) or len(labels) > 64 or len(set(labels)) != len(labels) or any(not x for x in labels):
        raise ValueError("invalid labels")
    splits = {k: [] for k in ("fit", "validation", "holdout")}
    seen = set()
    for sample in data["samples"]:
        identity = sample["state_sha256"]
        if identity in seen or len(identity) != 64 or any(c not in "0123456789abcdef" for c in identity):
            raise ValueError("duplicate state or split leakage")
        seen.add(identity)
        if sample["split"] not in splits:
            raise ValueError("unknown split")
        scores = sample["scores"]
        if len(scores) != len(labels) or any(not math.isfinite(x) or x < -1 or x > 1 for x in scores):
            raise ValueError("invalid raw cosine scores")
        truth = sample["labels"]
        if any(x not in labels for x in truth) or len(set(truth)) != len(truth) or (b["mode"] == "single" and len(truth) != 1):
            raise ValueError("invalid truth labels")
        splits[sample["split"]].append(sample)
    if any(not samples for samples in splits.values()):
        raise ValueError("all three independent splits are required")
    return b, splits


def rank(scores):
    order = sorted(range(len(scores)), key=lambda i: (-scores[i], i))
    return order[0], scores[order[0]] - scores[order[1]]


def single_metrics(samples, labels, similarity, margin):
    selected = correct = 0
    for row in samples:
        index, gap = rank(row["scores"])
        if gap > 1e-6 and row["scores"][index] >= similarity and gap >= margin:
            selected += 1
            correct += labels[index] == row["labels"][0]
    return {"count": len(samples), "selected": selected, "correct": correct,
            "coverage": selected / len(samples), "accuracy": correct / selected if selected else 0,
            "accuracy_lower_95": lower_bound(correct, selected), "dataset_sha256": digest(samples)}


def grid(values):
    values = sorted(set(values))
    return [values[round(i * (len(values)-1) / min(32, len(values)-1))] for i in range(min(32, len(values)-1)+1)] if len(values) > 1 else values


def fit(data, precision=0.9, min_coverage=0.2):
    b, splits = validate(data)
    if not 0 < precision <= 1 or not 0 < min_coverage <= 1:
        raise ValueError("invalid qualification target")
    if b["mode"] == "single":
        similarities = [-1.0] + grid([max(row["scores"]) for row in splits["fit"]])
        margins = [0.0] + grid([rank(row["scores"])[1] for row in splits["fit"]])
        candidates = []
        for similarity in similarities:
            for margin in margins:
                train = single_metrics(splits["fit"], b["labels"], similarity, margin)
                validation = single_metrics(splits["validation"], b["labels"], similarity, margin)
                if train["accuracy"] >= precision and validation["accuracy"] >= precision:
                    candidates.append((validation["coverage"], train["coverage"], -similarity, -margin, similarity, margin))
        if not candidates:
            raise ValueError("no threshold meets fit and validation accuracy")
        _, _, _, _, similarity, margin = max(candidates)
        thresholds = {"min_similarity": similarity, "min_margin": margin}
        metrics = {name: single_metrics(rows, b["labels"], similarity, margin) for name, rows in splits.items()}
        holdout = metrics["holdout"]
        qualified = holdout["accuracy_lower_95"] >= precision and holdout["coverage"] >= min_coverage
    else:
        thresholds = {"similarity_thresholds": {}}
        metrics = {name: {"count": len(rows), "dataset_sha256": digest(rows), "labels": {}} for name, rows in splits.items()}
        qualified = True
        for index, label in enumerate(b["labels"]):
            def measure(rows, threshold):
                tp = fp = fn = 0
                for row in rows:
                    pred = row["scores"][index] >= threshold
                    truth = label in row["labels"]
                    tp += pred and truth
                    fp += pred and not truth
                    fn += not pred and truth
                return {"tp": tp, "fp": fp, "fn": fn, "f1": 2*tp/(2*tp+fp+fn) if tp else 0,
                        "precision": tp/(tp+fp) if tp+fp else 0,
                        "precision_lower_95": lower_bound(tp, tp+fp)}
            candidates = [-1.0, 1.0] + grid([r["scores"][index] for r in splits["fit"]])
            threshold = max(candidates, key=lambda t: (measure(splits["validation"], t)["f1"], measure(splits["fit"], t)["f1"], t))
            thresholds["similarity_thresholds"][label] = threshold
            for name, rows in splits.items():
                metrics[name]["labels"][label] = measure(rows, threshold)
            positives = sum(label in r["labels"] for r in splits["holdout"])
            heldout = metrics["holdout"]["labels"][label]
            qualified &= positives >= 30 and len(splits["holdout"])-positives >= 30 and heldout["precision_lower_95"] >= precision and heldout["f1"] >= min_coverage
    qualified &= len(splits["fit"]) >= 30 and len(splits["validation"]) >= 30 and len(splits["holdout"]) >= 100
    return {"version": 1, "method": "heldout-abstention-v1", "binding": b,
            "qualified": bool(qualified), "thresholds": thresholds, "metrics": metrics,
            "targets": {"precision_lower_95": precision, "minimum_coverage_or_f1": min_coverage},
            "source_sha256": digest(data)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--precision", type=float, default=0.9)
    parser.add_argument("--minimum-coverage", type=float, default=0.2)
    args = parser.parse_args()
    result = fit(json.loads(args.input.read_text()), args.precision, args.minimum_coverage)
    # Exclusive ownership and atomic publish preserve an earlier good artifact.
    temp = args.output.with_name(args.output.name + ".partial")
    with temp.open("x") as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write("\n")
    temp.replace(args.output)
    print(json.dumps({"qualified": result["qualified"], "artifact_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))


if __name__ == "__main__":
    main()
