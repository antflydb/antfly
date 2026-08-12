#!/usr/bin/env python3
"""Merge a required warm-server verdict into CUDA release evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib


def merge(
    summary_path: pathlib.Path,
    evidence_path: pathlib.Path,
    benchmark_exit_code: int,
    *,
    lane_field: str = "long_e2e",
    lane_label: str = "Gemma 4 E2B warm-server long-context E2E gate",
) -> dict[str, object]:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    evidence_bytes = evidence_path.read_bytes() if evidence_path.is_file() else None
    evidence: dict[str, object] = {}
    evidence_parse_error = "evidence file is missing" if evidence_bytes is None else None
    if evidence_bytes is not None:
        try:
            parsed_evidence = json.loads(evidence_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            evidence_parse_error = str(exc)
        else:
            if isinstance(parsed_evidence, dict):
                evidence = parsed_evidence
            else:
                evidence_parse_error = "evidence root must be a JSON object"
    passed = benchmark_exit_code == 0 and evidence_parse_error is None and bool(evidence.get("passed"))
    summary[lane_field] = {
        "required": True,
        "enabled": True,
        "benchmark_exit_code": benchmark_exit_code,
        "path": str(evidence_path),
        "sha256": hashlib.sha256(evidence_bytes).hexdigest() if evidence_bytes is not None else None,
        "parse_error": evidence_parse_error,
        "schema": evidence.get("schema"),
        "contract": evidence.get("contract"),
        "comparison": evidence.get("comparison"),
        "passed": passed,
    }
    if not passed:
        error = f"{lane_label} failed (exit={benchmark_exit_code})"
        errors = summary.setdefault("errors", [])
        if error not in errors:
            errors.append(error)
    summary["passed"] = bool(summary.get("passed")) and passed
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=pathlib.Path)
    parser.add_argument("evidence", type=pathlib.Path)
    parser.add_argument("benchmark_exit_code", type=int)
    parser.add_argument(
        "--lane-field",
        choices=("long_e2e", "e4b_regression"),
        default="long_e2e",
    )
    parser.add_argument(
        "--lane-label",
        default="Gemma 4 E2B warm-server long-context E2E gate",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = merge(
        args.summary,
        args.evidence,
        args.benchmark_exit_code,
        lane_field=args.lane_field,
        lane_label=args.lane_label,
    )
    lane = result.get(args.lane_field)
    if bool(result.get("passed")) and isinstance(lane, dict) and bool(lane.get("passed")):
        return 0
    return args.benchmark_exit_code if args.benchmark_exit_code != 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
