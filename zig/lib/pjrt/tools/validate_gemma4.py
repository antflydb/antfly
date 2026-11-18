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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--cpu", type=Path, action="append", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    references = {}
    for directory in args.cpu:
        for path in directory.glob("*.f32"):
            if path.stem in references:
                raise ValueError("Duplicate CPU case: " + path.stem)
            references[path.stem] = np.fromfile(path, dtype="<f4")
    if not references:
        raise ValueError("No CPU references")
    rows = []
    expected = {
        row["id"]
        for row in json.loads((args.campaign / "summary.json").read_text())["results"]
    }
    for directory in sorted(args.campaign.glob("r*-*")):
        covered = set()
        for path in sorted(directory.glob("*.f32")):
            case_id = (
                path.stem.rsplit("-", 1)[0]
                if directory.name.endswith("-pjrt")
                else path.stem
            )
            if case_id not in references:
                raise ValueError("Missing CPU reference: " + case_id)
            covered.add(case_id)
            rows.append(
                {
                    "case": case_id,
                    "arm": directory.name,
                    "file": path.name,
                    **compare(np.fromfile(path, dtype="<f4"), references[case_id]),
                }
            )
        if covered != expected:
            raise ValueError("Incomplete logit coverage: " + directory.name)
    if not rows:
        raise ValueError("No measured logits")
    args.out.write_text(
        json.dumps(
            {
                "scope": "first-step logits only; full generation parity is separate",
                "rows": rows,
            },
            indent=2,
        )
        + "\n"
    )
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
