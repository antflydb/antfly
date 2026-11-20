#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Check prepared GLiNER outputs against upstream Python CUDA BF16.

The classification fixtures use the recorded hotel schema (6-way intent,
6 independent topics at threshold 0.4, and 2-way urgency). Entity fixtures use
the default entity schema and upstream overlap resolution. This tool qualifies
these fixtures; it does not infer model-wide qualification from their shapes.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

import numpy as np



def sha(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def classifications(candidate, reference, tolerance, allow_boundary_changes, tasks=None):
    tasks = tasks or [
        {"name": "intent", "size": 6},
        {"name": "topics", "size": 6, "multi_label": True, "threshold": 0.4},
        {"name": "urgent", "size": 2},
    ]
    sizes = [task["size"] for task in tasks]
    if not sizes or any(type(n) is not int or n <= 0 for n in sizes):
        raise ValueError("Invalid classification task sizes")
    width = sum(sizes)
    actual, expected = (np.asarray(x).reshape(-1, width) for x in (candidate, reference))
    delta, changes, start = 0.0, [], 0
    for task, size in zip(tasks, sizes, strict=True):
        stop = start + size
        a, r = actual[:, start:stop], expected[:, start:stop]
        if task.get("multi_label", False):
            threshold = task.get("threshold", 0.5)
            if not isinstance(threshold, (int, float)) or not 0 <= threshold <= 1:
                raise ValueError("Invalid classification threshold")
            a, r = (1 / (1 + np.exp(-np.clip(x, -700, 700))) for x in (a, r))
            for row, label in zip(*np.nonzero((a >= threshold) != (r >= threshold)), strict=True):
                margin = abs(float(r[row, label]) - threshold)
                changes.append({"row": int(row), "task": task["name"], "label": int(label),
                                "reference_margin": margin, "within_boundary_band": margin <= tolerance})
        else:
            def softmax(x):
                e = np.exp(x - x.max(axis=1, keepdims=True))
                return e / e.sum(axis=1, keepdims=True)
            a, r = softmax(a), softmax(r)
            for row, (ai, ri) in enumerate(zip(a.argmax(axis=1), r.argmax(axis=1), strict=True)):
                if ai != ri:
                    margin = float(r[row, ri] - r[row, ai])
                    changes.append({"row": row, "task": task["name"], "reference_margin": margin,
                                    "within_boundary_band": margin <= 2 * tolerance})
        delta = max(delta, float(np.max(np.abs(a - r))))
        start = stop
    return {"max_confidence_delta": delta, "exact_decisions": not changes,
            "boundary_changes": changes,
            "passed": delta <= tolerance and (not changes or (
                allow_boundary_changes and all(c["within_boundary_band"] for c in changes)))}


def entities(vector, fixture, settings):
    from gliner2.inference.overlap import resolve_overlaps

    if settings.get("adaptive_threshold", False):
        raise ValueError("Adaptive-count entity decoding requires separate qualification")
    b, words, q = (fixture["inputs"][i]["shape"][j] for i, j in ((0, 0), (2, 1), (4, 1)))
    index_start = fixture["candidate_indices_offset"]
    capacity, remainder = divmod(len(vector) - index_start, b * q * 3)
    if remainder or capacity < 1:
        raise ValueError("Invalid sparse candidate output layout")
    pairs = vector[index_start - b * q * capacity:index_start].reshape(b, q, capacity)
    coords = vector[index_start:index_start + 2 * b * q * capacity].reshape(b, q, capacity, 2)
    valid = vector[index_start + 2 * b * q * capacity:].reshape(b, q, capacity)
    null = vector[b * q * (3 * words + 2):b * q * (3 * words + 3)].reshape(b, q)
    if not np.isin(valid, (0, 1)).all():
        raise ValueError("Candidate validity mask is not boolean")
    null_probability = 1 / (1 + np.exp(-np.clip(null, -700, 700)))
    temperature = settings.get("pair_temperature", 1.0)
    if not np.isfinite(temperature) or temperature <= 0:
        raise ValueError("Invalid pair temperature")
    scores = 1 / (1 + np.exp(-np.clip(pairs / temperature, -700, 700)))
    result = {}
    for bi in range(b):
        for qi in range(q):
            candidates = []
            if null_probability[bi, qi] <= settings.get("abstention_threshold", 0.5):
                for ci in range(capacity):
                    if not valid[bi, qi, ci]:
                        continue
                    raw = coords[bi, qi, ci]
                    start, end = map(int, raw)
                    if not np.array_equal(raw, (start, end)) or not 0 <= start < end <= words:
                        raise ValueError("Invalid candidate coordinates")
                    if scores[bi, qi, ci] >= 0.5:
                        candidates.append((float(scores[bi, qi, ci]), start, end))
            selected = resolve_overlaps(candidates, settings.get("overlap_policy", "flat"),
                                        score=lambda x: x[0], start=lambda x: x[1], end=lambda x: x[2])
            for score, start, end in selected:
                result[(bi, qi, start, end)] = score
    return result


def compare(candidate, reference, fixture, tolerance, allow_boundary_changes=False, settings=None):
    count = int(np.prod(fixture["outputs"][0]["shape"]))
    actual, expected = (np.asarray(x, dtype=np.float64) for x in (candidate, reference))
    if any(x.ndim != 1 or len(x) != count or not np.isfinite(x).all() for x in (actual, expected)):
        raise ValueError("Invalid output geometry or nonfinite output")
    if fixture.get("kind", "classification") == "classification":
        tasks = fixture.get("classification_tasks")
        width = sum(t["size"] for t in tasks) if tasks else 14
        if fixture["inputs"][2]["shape"][1] != width:
            raise ValueError("Classification task metadata differs from output geometry")
        return classifications(actual, expected, tolerance, allow_boundary_changes, tasks)
    if settings is None:
        raise ValueError("Entity qualification requires the checkpoint's boundary settings")
    a, r = (entities(x, fixture, settings) for x in (actual, expected))
    matched = a.keys() == r.keys()
    delta = max((abs(a[key] - r[key]) for key in a.keys() & r.keys()), default=0.0)
    return {"exact_entities": matched, "entity_count_reference": len(r),
            "entity_count_candidate": len(a), "max_confidence_delta": delta,
            "passed": matched and delta <= tolerance}


def run_decide_serving(bench, model, manifest, output_dir):
    """Render requests and run the loaded public Decide pipeline with auto precision."""
    if manifest.get("architecture") != "span" or manifest.get("precision") != "bf16":
        raise ValueError("Automatic Decide serving requires a span BF16 manifest")
    if sha(model / "model.safetensors") != manifest["model_sha256"]:
        raise ValueError("Serving model differs from the BF16 reference")
    requests = []
    for case in manifest["cases"]:
        if case.get("kind", "classification") != "classification":
            raise ValueError("Expected classification cases for Decide serving")
        tasks = {}
        for task in json.loads(case["schema_json"])["classifications"]:
            tasks[task["name"]] = {key: value for key, value in task.items()
                                   if key not in ("name", "threshold")}
            if "threshold" in task:
                tasks[task["name"]]["cls_threshold"] = task["threshold"]
        requests.append({"id": case["id"], "texts": case["texts"], "tasks": tasks})
    with tempfile.TemporaryDirectory(dir=output_dir) as directory:
        fixture = Path(directory) / "cases.json"
        fixture.write_text(json.dumps({"format_version": 1, "cases": requests}))
        result = subprocess.run(
            [str(bench.resolve()), "--model-dir", str(model), "--backend", "cuda",
             "--precision", "auto", "--controlled", "1", "--cases", str(fixture),
             "--all-cases", "1", "--warmup", "0", "--reps", "3"],
            check=True, stdout=subprocess.PIPE, text=True, timeout=1800,
        )
    rows = [json.loads(line) for line in result.stdout.splitlines() if line]
    cases = []
    for index, (case, row) in enumerate(zip(manifest["cases"], rows, strict=True)):
        ids, mask = case["inputs"][:2]
        batch, length = ids["shape"]
        tokens = [[token for token, keep in zip(ids["data"][start:start + length],
                   mask["data"][start:start + length], strict=True) if keep]
                  for start in range(0, batch * length, length)]
        if (row["case_index"] != index or row["backend"] != "cuda"
                or row["requested_precision"] != "auto" or row["precision"] != "bf16_full"
                or row["model_variant"] != "decide_deberta"
                or row["batch_size"] != batch or row["physical_batch_limit"] != 8
                or row["prepared_input_ids"] != tokens
                or row["cuda_transfers"]["matmul_tuning_searches"] != 0
                or row["cuda_transfers"]["bf16_attention_calls"] == 0):
            raise ValueError(f"{case['id']}: automatic BF16 serving evidence differs")
        cases.append({"id": case["id"],
                      "physical_forwards": row["bf16_physical_forwards_per_call"],
                      "logits": [[v for item in row["decisions"] for v in item["raw_logits"]]]})
    return {"cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "reference", "out"):
        parser.add_argument("--" + name, type=Path, required=True)
    candidate = parser.add_mutually_exclusive_group(required=True)
    candidate.add_argument("--native", type=Path)
    candidate.add_argument("--serving-bench", type=Path)
    parser.add_argument("--model", type=Path, help="Original Decide checkpoint for --serving-bench")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--max-confidence-delta", type=float, default=0.02)
    parser.add_argument("--allow-boundary-changes", action="store_true")
    args = parser.parse_args()
    if args.out.exists() or not 0 < args.max_confidence_delta <= 0.02:
        parser.error("Use a fresh output and a confidence limit in (0, 0.02]")
    if (args.serving_bench is None) != (args.model is None):
        parser.error("--serving-bench and --model must be supplied together")
    manifest, reference = (json.loads(p.read_text()) for p in (args.manifest, args.reference))
    if (reference.get("precision") != "bf16" or reference.get("dtype") != "torch.bfloat16"
            or reference.get("model_sha256") != manifest["model_sha256"]
            or reference.get("manifest_sha256") != sha(args.manifest)
            or reference.get("inference_mode") is not True
            or reference.get("autocast") is not False):
        raise ValueError("Reference is not authenticated Python CUDA BF16 for this manifest")
    if reference.get("matmul_settings", {}).get("allow_bf16_reduced_precision_reduction") is not True:
        raise ValueError("Reference must record the stock CUDA BF16 reduction setting")
    native = (run_decide_serving(args.serving_bench, args.model, manifest, args.out.parent)
              if args.serving_bench else json.loads(args.native.read_text()))
    if native.get("incomplete", False):
        raise ValueError("Incomplete native run cannot qualify")
    ids = [c["id"] for c in manifest["cases"]]
    if not ids or len(set(ids)) != len(ids) or any(
            [c["id"] for c in report["cases"]] != ids for report in (native, reference)):
        raise ValueError("Case identity/coverage differs")
    settings = json.loads(args.config.read_text()).get("boundary_head") if args.config else None
    rows = []
    for fixture, n, r in zip(manifest["cases"], native["cases"], reference["cases"], strict=True):
        expected_forwards = (fixture["inputs"][0]["shape"][0] + 7) // 8
        if n.get("physical_forwards") != expected_forwards or not n["logits"] or not r["logits"]:
            raise ValueError("Missing samples or an unqualified physical batch")
        checks = [compare(a, b, fixture, args.max_confidence_delta, args.allow_boundary_changes, settings)
                  for a in n["logits"] for b in r["logits"]]
        rows.append({"id": fixture["id"], "kind": fixture.get("kind", "classification"),
                     "passed": all(c["passed"] for c in checks), "checks": checks})
    report = {"passed": all(r["passed"] for r in rows), "reference": "Python CUDA BF16",
              "max_confidence_delta": args.max_confidence_delta,
              "allow_boundary_changes": args.allow_boundary_changes,
              "entity_selection_requires_exact_match": True,
              "candidate": "automatic BF16 loaded Decide" if args.serving_bench else "prepared native outputs",
              "provenance": {str(p): sha(p) for p in
                             (args.manifest, args.serving_bench or args.native, args.reference, Path(__file__))},
              "cases": rows}
    args.out.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(f"{sum(r['passed'] for r in rows)}/{len(rows)} cases passed")
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
