# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compact a completed media campaign without changing its qualification gates."""

import argparse
import hashlib
import json
import statistics
from pathlib import Path

from media_campaign import metrics


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run", type=Path, required=True)
    p.add_argument("--fp32-oracle", type=Path, required=True)
    p.add_argument("--fp32-acceptance", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    report = json.loads((a.run / "summary.json").read_text())
    diagnostic = json.loads(a.fp32_acceptance.read_text())
    oracle = json.loads(a.fp32_oracle.read_text())
    if any(
        c["max_abs"] > diagnostic["wrapper_upstream_max_abs"] for c in oracle["cases"]
    ):
        raise ValueError("FP32 static wrapper failed its upstream oracle")
    expected = {c["name"]: c["upstream_vector"] for c in oracle["cases"]}
    for case in report["cases"]:
        case["fp32_diagnostic"] = {}
        case["round_medians_ms"] = {}
        for role in ("control", "candidate", "pjrt"):
            runs = [
                json.loads((a.run / f"r{r}-{role}" / "results.json").read_text())
                for r in range(report["rounds"])
            ]
            runs = [
                next(c for c in run["cases"] if c["name"] == case["name"])
                for run in runs
            ]
            comparisons = [
                metrics(v, expected[case["name"]])
                for run in runs
                for v in run["vectors"]
            ]
            case["round_medians_ms"][role] = [
                statistics.median(t * 1000 for t in run["samples_seconds"])
                for run in runs
            ]
            case["fp32_diagnostic"][role] = {
                "min_cosine": min(c["cosine"] for c in comparisons),
                "max_abs": max(c["max_abs"] for c in comparisons),
                "passed": all(
                    c["cosine"] >= diagnostic["min_cosine"]
                    and c["max_abs"] <= diagnostic["max_abs"]
                    for c in comparisons
                ),
            }
    report["provenance"] = json.loads((a.run / "provenance.json").read_text())
    report["fp32_diagnostic_acceptance"] = diagnostic
    report["fp32_oracle_sha256"] = hashlib.sha256(
        a.fp32_oracle.read_bytes()
    ).hexdigest()
    report["fp32_diagnostic_changes_bf16_qualification"] = False
    a.out.write_text(json.dumps(report, indent=2) + "\n")
    for case in report["cases"]:
        print(case["name"], case["ms"], "qualified=", case["passed"])


if __name__ == "__main__":
    main()
