#!/usr/bin/env python3
"""Score a release adapter over the full held-out set in native Zig."""

from __future__ import annotations

import argparse
import json
import math
import subprocess
from pathlib import Path
from typing import Any, Iterable

from gliner2_release_contract import CANONICAL_NORMALIZATION, path_fingerprint
from validate_gliner2_release_data import adapter_bundle_fingerprint, base_model_fingerprint


NATIVE_EVALUATION_CONTRACT = "gliner2_native_full_task_evaluation/v1"

FULL_TASK_MINIMUM_KEYS = (
    "classifications.micro_f1",
    "classifications.exact_match",
    "json_structures.micro_f1",
    "json_structures.exact_match",
    "relations.micro_f1",
    "relations.exact_match",
    "count.accuracy",
)


def jsonl_files(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    if path.is_dir():
        files = sorted(path.glob("*.jsonl"))
        if files:
            yield from files
            return
    raise ValueError(f"{path}: expected a JSONL file or directory")


def eval_entity_labels(eval_data: Path) -> list[str]:
    labels: list[str] = []
    for source in jsonl_files(eval_data):
        with source.open(encoding="utf-8") as lines:
            for line_no, raw in enumerate(lines, 1):
                if not raw.strip():
                    continue
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{source}:{line_no}: invalid JSON: {exc}") from exc
                if not isinstance(record, dict):
                    raise ValueError(f"{source}:{line_no}: record must be an object")
                output = record.get("output")
                entities = output.get("entities") if isinstance(output, dict) else None
                if entities is not None:
                    if not isinstance(entities, dict):
                        raise ValueError(f"{source}:{line_no}: output.entities must be an object")
                    candidates = entities
                else:
                    legacy = record.get("entities", [])
                    if not isinstance(legacy, list):
                        raise ValueError(f"{source}:{line_no}: entities must be an array")
                    candidates = {
                        item.get("label"): None
                        for item in legacy
                        if isinstance(item, dict)
                    }
                for raw_label in candidates:
                    if not isinstance(raw_label, str) or not raw_label or raw_label != raw_label.strip():
                        raise ValueError(f"{source}:{line_no}: invalid entity label")
                    if raw_label not in labels:
                        labels.append(raw_label)
    if not labels:
        raise ValueError("eval data has no entity schema")
    return labels


def release_entity_schema(adapter_dir: Path, eval_data: Path) -> tuple[list[str], dict[str, Any]]:
    manifest_path = adapter_dir / "training_manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid Zig training manifest: {manifest_path}: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ValueError("Zig training manifest must be a JSON object")
    manifest_labels = manifest.get("entity_labels")
    if not isinstance(manifest_labels, list) or any(not isinstance(label, str) for label in manifest_labels):
        raise ValueError("Zig training manifest requires entity_labels")
    labels = eval_entity_labels(eval_data)
    missing = [label for label in labels if label not in manifest_labels]
    if missing:
        raise ValueError(f"eval entity labels are absent from the trained schema: {', '.join(missing)}")
    return labels, manifest


def unit_interval(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a number") from exc
    if not math.isfinite(result) or not 0 <= result <= 1:
        raise argparse.ArgumentTypeError("value must be finite and between 0 and 1")
    return result


def release_threshold(value: str) -> float:
    threshold = unit_interval(value)
    if threshold == 0:
        raise argparse.ArgumentTypeError("threshold must be greater than 0")
    return threshold


def parse_full_task_minima(items: list[str]) -> dict[str, float]:
    minima: dict[str, float] = {}
    for item in items:
        key, separator, raw = item.partition("=")
        if not separator or key not in FULL_TASK_MINIMUM_KEYS or key in minima:
            raise ValueError(f"invalid or duplicate --min-task-metric {item!r}")
        try:
            minima[key] = release_threshold(raw)
        except argparse.ArgumentTypeError as exc:
            raise ValueError(f"invalid --min-task-metric {item!r}: {exc}") from exc
    missing = set(FULL_TASK_MINIMUM_KEYS) - minima.keys()
    if missing:
        raise ValueError(
            "missing required --min-task-metric values: " + ",".join(sorted(missing))
        )
    return minima


def build_command(
    model_dir: Path,
    adapter_dir: Path,
    eval_data: Path,
    manifest: dict[str, Any],
    threshold: float,
    min_f1: float,
    min_exact_match: float,
    full_task_minima: dict[str, float],
    native_output: Path,
) -> list[str]:
    command = [
        "zig",
        "build",
        "-Donnx=false",
        "-Dmetal=false",
        "eval-gliner2-autodiff-adapter-dataset",
        "--",
        str(model_dir),
        str(adapter_dir),
        str(eval_data),
        # The evaluator derives each record's schema from the structured JSONL.
        # This sentinel avoids lossy CSV transport and a false global 256-label cap.
        "-",
        "--objective",
        "gliner2-total-loss",
        "--backend",
        "native",
        "--seq-len",
        str(manifest.get("seq_len", 256)),
        "--max-span-width",
        str(manifest.get("max_span_width", 8)),
        "--min-prediction-score",
        str(threshold),
        "--match-text-label-only",
        "--min-f1",
        str(min_f1),
        "--min-exact-match",
        str(min_exact_match),
        "--out",
        str(native_output),
    ]
    for key in FULL_TASK_MINIMUM_KEYS:
        command.extend(("--min-task-metric", f"{key}={full_task_minima[key]}"))
    return command


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2), encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--adapter-dir", type=Path, required=True)
    parser.add_argument("--eval-data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threshold", type=release_threshold, default=0.5)
    parser.add_argument("--min-f1", type=release_threshold, required=True)
    parser.add_argument("--min-exact-match", type=release_threshold, required=True)
    parser.add_argument("--min-task-metric", action="append", default=[])
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[5])
    args = parser.parse_args()
    model_dir = args.model_dir.expanduser().resolve()
    adapter_dir = args.adapter_dir.expanduser().resolve()
    eval_data = args.eval_data.expanduser().resolve()
    output = args.output.expanduser().resolve()
    repo_root = args.repo_root.expanduser().resolve()
    try:
        labels, manifest = release_entity_schema(adapter_dir, eval_data)
        full_task_minima = parse_full_task_minima(args.min_task_metric)
    except ValueError as exc:
        parser.error(str(exc))

    native_output = output.with_suffix(output.suffix + ".native.tmp")
    native_output.unlink(missing_ok=True)
    command = build_command(
        model_dir,
        adapter_dir,
        eval_data,
        manifest,
        args.threshold,
        args.min_f1,
        args.min_exact_match,
        full_task_minima,
        native_output,
    )
    completed = subprocess.run(
        command,
        cwd=repo_root / "zig/pkg/inference",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        native_summary = json.loads(native_output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(
            f"native held-out full-task evaluator returned no valid report: {exc}\n"
            f"{completed.stdout}\n{completed.stderr}"
        ) from exc
    finally:
        native_output.unlink(missing_ok=True)

    passed = completed.returncode == 0 and native_summary.get("status") == "passed"
    report = {
        "contract": NATIVE_EVALUATION_CONTRACT,
        "pass": passed,
        "status": "pass" if passed else "fail",
        "deployment_scope": "native_full_heldout_gliner2_quality",
        "threshold": args.threshold,
        "minimums": {
            "entities.micro_f1": args.min_f1,
            "entities.exact_match": args.min_exact_match,
            **full_task_minima,
        },
        "entity_types": labels,
        "evaluated_record_count": native_summary.get("example_count", 0),
        "evaluated_entity_record_count": native_summary.get("evaluated_entity_record_count", 0),
        "native_summary": native_summary,
        "normalization": native_summary.get("full_task", {}).get("normalization"),
        "expected_normalization": CANONICAL_NORMALIZATION,
        "artifacts": {
            "base_model_fingerprint_sha256": base_model_fingerprint(model_dir),
            "adapter_bundle_fingerprint_sha256": adapter_bundle_fingerprint(adapter_dir),
            "eval_data_fingerprint_sha256": path_fingerprint(eval_data),
        },
    }
    write_report(output, report)
    if not passed:
        raise SystemExit(
            f"native held-out full-task quality failed\n{completed.stdout}\n{completed.stderr}"
        )
    print(f"native held-out full-task quality: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
