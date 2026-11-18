# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare saved first-step logits to the independent FP32 CPU forward."""

import argparse
import json
from pathlib import Path

import numpy as np


def compare(actual, reference):
    if actual.shape != (262144,) or reference.shape != actual.shape:
        raise ValueError("Expected one complete E4B vocabulary")
    if not np.isfinite(actual).all() or not np.isfinite(reference).all():
        raise ValueError("Non-finite logits")
    actual, reference = actual.astype(np.float64), reference.astype(np.float64)
    norm = np.linalg.norm(reference)
    if norm == 0 or np.linalg.norm(actual) == 0:
        raise ValueError("Zero logit norm")
    relative_l2 = float(np.linalg.norm(actual - reference) / norm)
    cosine = float(np.dot(actual, reference) / (np.linalg.norm(actual) * norm))
    expected, observed = int(reference.argmax()), int(actual.argmax())
    gap = float(reference[expected] - reference[observed])
    # Report exact decisions separately: close logits do not imply identical tokens.
    return {
        "cosine": cosine,
        "relative_l2": relative_l2,
        "max_abs": float(np.max(np.abs(actual - reference))),
        "expected_argmax": expected,
        "actual_argmax": observed,
        "argmax_matches": expected == observed,
        "reference_argmax_gap": gap,
        "logit_tolerance_pass": cosine >= 0.9999 and relative_l2 <= 0.02,
    }


def validate_campaign(campaign, cpu):
    provenance = json.loads((campaign / "provenance.json").read_text())
    if not provenance.get("validation_only") or provenance["iterations"] != 1:
        raise ValueError(
            "Expected a validation campaign with one measured call per case"
        )
    summary = json.loads((campaign / "summary.json").read_text())["results"]
    expected = {row["id"] for row in summary}
    arms = {arm for row in summary for arm in row["ms"]}
    if (
        not expected
        or len(expected) != len(summary)
        or not arms
        or not arms <= {"native", "pjrt"}
        or any(set(row["ms"]) != arms for row in summary)
        or not 1 <= provenance["rounds"] <= 10
    ):
        raise ValueError("Invalid campaign coverage")
    directories = {
        f"r{index}-{arm}" for index in range(provenance["rounds"]) for arm in arms
    }
    if {path.name for path in campaign.glob("r*-*") if path.is_dir()} != directories:
        raise ValueError("Incomplete validation rounds/arms")
    references = {}
    for directory in cpu:
        for path in directory.glob("*.f32"):
            if path.stem in references:
                raise ValueError("Duplicate CPU case: " + path.stem)
            references[path.stem] = np.fromfile(path, dtype="<f4")
    if not references:
        raise ValueError("No CPU references")
    rows = []
    for name in sorted(directories):
        directory = campaign / name
        files = {
            case_id + ("-1" if name.endswith("-pjrt") else "") + ".f32": case_id
            for case_id in expected
        }
        if {path.name for path in directory.glob("*.f32")} != set(files):
            raise ValueError("Incomplete logit coverage: " + name)
        for filename, case_id in sorted(files.items()):
            path = directory / filename
            if case_id not in references:
                raise ValueError("Missing CPU reference: " + case_id)
            rows.append(
                {
                    "case": case_id,
                    "arm": directory.name,
                    "file": path.name,
                    **compare(np.fromfile(path, dtype="<f4"), references[case_id]),
                }
            )
    logits_pass = all(row["logit_tolerance_pass"] for row in rows)
    argmax_pass = all(row["argmax_matches"] for row in rows)
    return {
        "scope": "first-step logits only; full generation parity is separate",
        "logit_tolerance_pass": logits_pass,
        "argmax_all_match": argmax_pass,
        "passed": logits_pass and argmax_pass,
        "rows": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--cpu", type=Path, action="append", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    report = validate_campaign(args.campaign, args.cpu)
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    for row in report["rows"]:
        print(row)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
