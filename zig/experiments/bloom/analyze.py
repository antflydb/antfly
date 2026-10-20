#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Aggregate completed runs, retaining the measured false-positive tradeoff."""

import csv
import json
from pathlib import Path
import statistics

HERE = Path(__file__).resolve().parent
KEY = ("mode", "kind", "n", "requested_bpk", "budget_pct", "bytes")


def main():
    grouped = {}
    for path in sorted(HERE.glob("results-[0-9].csv")):
        for row in csv.DictReader(path.open()):
            grouped.setdefault(tuple(row[k] for k in KEY), []).append(row)
    rows = []
    for key, samples in grouped.items():
        row = dict(zip(KEY, key))
        row["runs"] = len(samples)
        for col in ("build_with_keygen_ns_key", "miss_ns", "hit_ns", "fpr"):
            row[col] = statistics.median(float(s[col]) for s in samples)
        row["miss_min_ns"] = min(float(s["miss_min_ns"]) for s in samples)
        row["miss_max_ns"] = max(float(s["miss_max_ns"]) for s in samples)
        rows.append(row)
    baselines = {(r["mode"], r["n"]): r for r in rows if r["kind"] == "baseline"}
    for row in rows:
        base = baselines[(row["mode"], row["n"])]
        row["miss_speedup"] = base["miss_ns"] / row["miss_ns"]
        row["hit_speedup"] = base["hit_ns"] / row["hit_ns"]
        row["memory_ratio"] = int(row["bytes"]) / int(base["bytes"])
        row["fpr_ratio"] = row["fpr"] / base["fpr"] if base["fpr"] else float("nan")
    if not rows:
        raise SystemExit("No results found")
    with (HERE / "aggregate.csv").open("w") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(
        "| Mode | Keys | Filter | Memory | FP rate | Miss ns | Miss speedup | Hit speedup |"
    )
    print("|---|---:|---|---:|---:|---:|---:|---:|")
    for (mode, n), base in baselines.items():
        options = [r for r in rows if r["mode"] == mode and r["n"] == n]
        optimized = next(r for r in options if r["kind"] == "optimized")
        matched = [
            r
            for r in options
            if r["kind"] not in ("baseline", "optimized") and r["fpr"] <= base["fpr"]
        ]
        best = min(matched, key=lambda r: r["miss_ns"])
        for row in (base, optimized, best):
            print(
                f"| {mode} | {int(n):,} | {row['kind']} | {row['memory_ratio']:.2f}x | {row['fpr']:.6%} | {row['miss_ns']:.3f} | {row['miss_speedup']:.2f}x | {row['hit_speedup']:.2f}x |"
            )

    prefix = (
        "storage-threeway" if list(HERE.glob("storage-threeway-*.txt")) else "storage"
    )
    summarize_storage(prefix, "storage-aggregate.csv")
    if list(HERE.glob("large-[0-9]-*.txt")):
        print("\nMillion-key storage comparison:")
        summarize_storage("large", "large-aggregate.csv")
    if list(HERE.glob("large-onehash-[0-9]-*.txt")):
        print("\nMillion-key storage with one input hash:")
        summarize_storage("large-onehash", "large-onehash-aggregate.csv")
        print("\nCombined million-key baseline/split comparison:")
        summarize_storage(
            ("large", "large-onehash"),
            "large-combined-aggregate.csv",
            kinds=("baseline", "split"),
        )


def summarize_storage(prefix, output_name, kinds=None):
    storage = {}
    prefixes = (prefix,) if isinstance(prefix, str) else prefix
    paths = sorted({p for item in prefixes for p in HERE.glob(f"{item}-[0-9]-*.txt")})
    for path in paths:
        name = path.stem.rsplit("-", 1)[1]
        if kinds is not None and name not in kinds:
            continue
        for line in path.read_text().splitlines():
            if line.startswith("{"):
                row = json.loads(line)
                if row["workload"] in ("warm_read_hits", "warm_read_misses"):
                    storage.setdefault(
                        (name, row["cache"], row["workload"]), []
                    ).append(row)
    print(
        "\n| Filter | Cache | Storage workload | Samples | Median ns/op | Min | Max |"
    )
    print("|---|---|---|---:|---:|---:|---:|")
    storage_rows = []
    for (name, cache, workload), samples in sorted(storage.items()):
        values = [s["ns_per_op"] for s in samples]
        row = dict(
            kind=name,
            cache=cache,
            workload=workload,
            samples=len(values),
            median_ns=statistics.median(values),
            min_ns=min(values),
            max_ns=max(values),
            bloom_negatives=samples[0]["read_bloom_negatives"],
        )
        storage_rows.append(row)
        print(
            f"| {name} | {cache} | {workload} | {len(values)} | {row['median_ns']:.2f} | {min(values):.2f} | {max(values):.2f} |"
        )
    if storage_rows:
        with (HERE / output_name).open("w") as output:
            writer = csv.DictWriter(
                output, fieldnames=list(storage_rows[0]), lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(storage_rows)


if __name__ == "__main__":
    main()
