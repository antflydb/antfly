#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validate complete, matched fixed-output HTTP campaigns before reporting medians."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics


def validate_campaign(root: Path, fixtures: Path | None = None) -> dict:
    provenance = json.loads((root / "provenance.json").read_text())
    arguments = provenance["arguments"]
    if Counter(arguments["arms"]) != Counter(("antfly", "llama")):
        raise ValueError("a paired campaign requires each engine exactly once")
    fixtures = fixtures or Path(arguments["fixtures"])
    if (
        hashlib.sha256(fixtures.read_bytes()).hexdigest()
        != provenance["hashes"]["fixtures"]
    ):
        raise ValueError("fixture checksum differs from the measured campaign")
    cells = json.loads(fixtures.read_text())
    if (
        not cells
        or len({c["id"] for c in cells}) != len(cells)
        or any(not c["calls"] for c in cells)
    ):
        raise ValueError("empty or duplicate workload definitions")
    lifecycles, repeats = arguments["lifecycles"], arguments["repeats"]
    if lifecycles < 1 or repeats < 1:
        raise ValueError("invalid repetition counts")
    rows = [
        json.loads(line)
        for line in (root / "calls.jsonl").read_text().splitlines()
        if line.strip()
    ]

    def key(row):
        return row["arm"], row["lc"], row["cell"], row["repeat"], row["index"]

    expected = {
        (arm, lc, c["id"], repeat, index)
        for arm in ("antfly", "llama")
        for lc in range(lifecycles)
        for c in cells
        for repeat in range(repeats)
        for index in range(len(c["calls"]))
    }
    counts = Counter(key(row) for row in rows)
    if set(counts) != expected or any(n != 1 for n in counts.values()):
        raise ValueError("incomplete or duplicate campaign measurements")
    by_key = {key(row): row for row in rows}
    by_cell = {c["id"]: c for c in cells}
    outputs = defaultdict(set)
    for row in rows:
        arm, *rest = key(row)
        other = by_key[("llama" if arm == "antfly" else "antfly", *rest)]
        tokens = row["usage"]["prompt_tokens"]
        if (
            type(tokens) is not int
            or tokens <= 0
            or tokens != other["usage"]["prompt_tokens"]
        ):
            raise ValueError(f"prompt token count differs between engines: {rest}")
        completed = row["usage"]["completion_tokens"]
        if (
            type(completed) is not int
            or completed != by_cell[row["cell"]]["max_tokens"]
        ):
            raise ValueError("fixed completion cap not reached")
        if any(
            not math.isfinite(row[k]) or row[k] <= 0
            for k in ("ms", "ttft_ms", "decode_tok_s")
        ):
            raise ValueError("invalid timing")
        if row["ttft_ms"] >= row["ms"]:
            raise ValueError("first-token time does not precede stream completion")
        outputs[arm, row["cell"], row["index"]].add(row["text"])
    unstable = [[*k, len(v)] for k, v in outputs.items() if len(v) != 1]
    if any(k[0] == "antfly" for k in unstable):
        raise ValueError("native greedy output changed across repeated inputs")

    report = {
        "name": root.name,
        "calls_per_arm": len(expected) // 2,
        "prompt_counts_equal": True,
        "all_completion_caps_reached": True,
        "native_text_stable": True,
        "output_determinism_changes": unstable,
        "cells": [],
        "memory": {},
    }
    for cell in cells:
        data = {
            "cell": cell["id"],
            "samples_per_arm": len(cell["calls"]) * lifecycles * repeats,
        }
        for arm in ("antfly", "llama"):
            group = [r for r in rows if r["cell"] == cell["id"] and r["arm"] == arm]
            data[arm] = {
                k: statistics.median(r[k] for r in group)
                for k in ("ms", "ttft_ms", "decode_tok_s")
            }
            data[arm]["lifecycle_median_ms"] = [
                statistics.median(r["ms"] for r in group if r["lc"] == lc)
                for lc in range(lifecycles)
            ]
        data["speedup"] = data["llama"]["ms"] / data["antfly"]["ms"]
        report["cells"].append(data)
    for arm in ("antfly", "llama"):
        metadata = [
            json.loads((root / f"lc{lc}-{arm}.json").read_text())["memory"]
            for lc in range(lifecycles)
        ]
        if any(
            d["valid"] is not True
            or d["errors"]
            or type(d["samples"]) is not int
            or d["samples"] <= 0
            or any(
                type(d[k]) is not int or d[k] <= 0
                for k in ("peak_footprint_bytes", "peak_gpu_bytes", "peak_pss_bytes")
            )
            for d in metadata
        ):
            raise ValueError("invalid memory sampler")
        # Use the simultaneous sampled PSS+NVML peak, never add separate component peaks.
        report["memory"][arm] = {
            k: [d[k] for d in metadata]
            for k in ("peak_footprint_bytes", "peak_gpu_bytes", "peak_pss_bytes")
        }
    report["all_cells_faster"] = all(c["speedup"] > 1 for c in report["cells"])
    report["cell_geomean_speedup"] = math.prod(
        c["speedup"] for c in report["cells"]
    ) ** (1 / len(cells))
    for name, metric in (
        ("combined_memory_lower", "peak_footprint_bytes"),
        ("gpu_memory_lower", "peak_gpu_bytes"),
    ):
        report[name] = max(report["memory"]["antfly"][metric]) < max(
            report["memory"]["llama"][metric]
        )
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument(
        "--fixtures", type=Path, help="Relocated copy of the identical measured fixture"
    )
    args = parser.parse_args()
    report = validate_campaign(args.results, args.fixtures)
    (args.results / "validated-summary.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    print("| Workload | Antfly ms | llama.cpp ms | Speedup |")
    print("|---|---:|---:|---:|")
    for c in report["cells"]:
        print(
            f"| {c['cell']} | {c['antfly']['ms']:.1f} | {c['llama']['ms']:.1f} | {c['speedup']:.3f}x |"
        )
    print(
        "All cells faster:",
        report["all_cells_faster"],
        "Combined footprint lower:",
        report["combined_memory_lower"],
        "VRAM lower:",
        report["gpu_memory_lower"],
    )


if __name__ == "__main__":
    main()
