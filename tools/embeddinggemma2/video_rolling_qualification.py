#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Verify real rolling/sparse live outputs and projection reuse across eviction.

The pinned PyTorch oracle covers three original consecutive intervals. Its gates
apply only to the matching rolling subset; other windows use complete-clip native
baselines. This verifier does not claim fresh PyTorch coverage for those windows.
"""

import argparse
import copy
import json
from pathlib import Path

import video_live_qualification


def validate(report, oracle):
    if report.get("status") != "pass" or len(report.get("cases", [])) != 8:
        raise ValueError("eight completed rolling/sparse outputs required")
    rolling = report["cases"][:6]
    sparse = report["cases"][6:]
    for rows, name, advance in (
        (rolling, "sintel-6s", 1_000_000_000),
        (sparse, "sintel-6s-sparse", 3_000_000_000),
    ):
        for ordinal, row in enumerate(rows):
            start = 200_000_000 + ordinal * advance
            end = min(start + 2_000_000_000, 6_200_000_000)
            tokens = 124 if end - start == 1_000_000_000 else 246
            if (
                row["case"] != name
                or row["ordinal"] != ordinal
                or row["epoch"] != 0
                or row["start_ns"] != start
                or row["end_ns"] != end
                or row["input_tokens"] != tokens
            ):
                raise ValueError("source interval, identity or token count changed")
            if row["vector"] != row["baseline_vector"] or row["max_abs"] != 0:
                raise ValueError("cached live/complete-clip vector drift")
            expected_hits = ordinal if name == "sintel-6s" else 0
            expected_stored = (
                min(ordinal + 2, 6) if name == "sintel-6s" else 2 * (ordinal + 1)
            )
            if (
                row["live_cache_hits"] != expected_hits
                or row["live_cache_stored"] != expected_stored
            ):
                raise ValueError("rolling reuse/sparse cache isolation changed")
    # Explicit verification view: preserve source times and vectors, renumber
    # only the three matched intervals to the original oracle's ordinals.
    subset = copy.deepcopy(report)
    subset["cases"] = [copy.deepcopy(rolling[i]) for i in (0, 2, 4)]
    for ordinal, row in enumerate(subset["cases"]):
        row["ordinal"] = ordinal
    result = video_live_qualification.validate(subset, oracle)
    result["rolling_cache_hits"] = rolling[-1]["live_cache_hits"]
    result["rolling_cache_stored"] = rolling[-1]["live_cache_stored"]
    result["scope"] = (
        "Pinned PyTorch gates cover rolling windows 0, 2, 4 matching the original "
        "non-overlapping intervals. All eight rolling/sparse outputs match "
        "complete-clip native baselines exactly. Five rolling cache hits survive "
        "preroll eviction; sparse intervals have no shared selected pictures."
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = validate(
        json.loads(args.report.read_text()), json.loads(args.reference.read_text())
    )
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
