"""Record combined selection/scan diagnostics on a saved immutable base.

The benchmark does not replay delta/WAL overlays or claim live-index recall.
It retains authenticated leaf bytes throughout the run, not live query leases.
"""

import argparse
import hashlib
import json
import platform
import statistics
import subprocess
from pathlib import Path


def digest(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def summarize(rows):
    modes = ("control", "sort_predicate", "partition_predicate", "partition_ranges")
    if len(rows) != 24:
        raise ValueError("expected one warmup and five measured rounds per mode")
    result = {}
    selected_checksums = set()
    for mode in modes:
        all_rounds = [row for row in rows if row["mode"] == mode]
        if len(all_rounds) != 6 or {r["round"] for r in all_rounds} != set(range(6)):
            raise ValueError("missing or duplicate rounds")
        if any(row["warmup"] != (row["round"] == 0) for row in all_rounds):
            raise ValueError("incorrect warmup marker")
        measured = [row for row in all_rounds if not row["warmup"]]
        if mode != "control":
            selected_checksums.update(row["checksum"] for row in all_rounds)
        result[mode] = {
            key: statistics.median(row[key] for row in measured)
            for key in (
                "routing_ns",
                "selection_ns",
                "scan_ns",
                "vectors_per_query",
                "frontier_vectors_per_query",
            )
        }
        totals = [
            row["routing_ns"] + row["selection_ns"] + row["scan_ns"] for row in measured
        ]
        result[mode].update(
            total_ns=statistics.median(totals),
            total_min_ns=min(totals),
            total_max_ns=max(totals),
        )
    if len(selected_checksums) != 1:
        raise ValueError("selected candidate outputs differ")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("segment", type=Path)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rotation", choices=("none", "givens"), required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    sources = [
        root / name
        for name in (
            "zig/tools/bench_subgroup_scan.zig",
            "zig/tools/bench_subgroup_routing.zig",
            "zig/lib/vector/src/quantizer.zig",
            "zig/lib/vector/src/rabitq.zig",
            "zig/lib/vectorindex/src/weighted_subgroup_selection.zig",
            "zig/lib/vectorindex/src/search_results.zig",
            "zig/lib/vectorindex/src/quantized_directory.zig",
            "zig/lib/vectorindex/src/posting_segment.zig",
        )
    ]
    inputs = [
        args.binary.resolve(),
        args.segment.resolve(),
        args.fixture.resolve(),
        *sources,
    ]
    before = {str(path): digest(path) for path in inputs}
    process = subprocess.run(
        [
            str(args.binary.resolve()),
            str(args.segment.resolve()),
            str(args.fixture.resolve()),
            args.rotation,
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if process.returncode:
        raise RuntimeError(process.stderr)
    rows = [
        json.loads(line) for line in process.stderr.splitlines() if line.startswith("{")
    ]
    medians = summarize(rows)
    if before != {str(path): digest(path) for path in inputs}:
        raise RuntimeError("benchmark inputs changed during execution")
    report = {
        "qualification": __doc__,
        "input_sha256": before,
        "rotation": args.rotation,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "rows": rows,
        "medians": medians,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(medians, indent=2), flush=True)


if __name__ == "__main__":
    main()
