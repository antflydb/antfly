"""Record standalone ReleaseFast representative kernel evidence, not QPS."""

import argparse
import hashlib
import json
import platform
import statistics
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    binary, fixture = args.binary.resolve(), args.fixture.resolve()
    process = subprocess.run(
        [str(binary), str(fixture)], capture_output=True, text=True, timeout=120
    )
    if process.returncode:
        raise RuntimeError(process.stderr)
    rows = [
        json.loads(line) for line in process.stderr.splitlines() if line.startswith("{")
    ]
    if len(rows) != 24:
        raise RuntimeError("missing benchmark rounds")
    medians = {}
    for mode in ("scalar_f64", "simd_f32", "simd_f16", "simd_i8"):
        measured = [row for row in rows if row["mode"] == mode and not row["warmup"]]
        if {row["round"] for row in measured} != set(range(1, 6)):
            raise RuntimeError("missing measured rounds")
        medians[mode] = {
            key: statistics.median(row[key] for row in measured)
            for key in ("score_ns_per_query", "sort_ns_per_query")
        }
        medians[mode]["score_plus_sort_ns_per_query"] = statistics.median(
            row["score_ns_per_query"] + row["sort_ns_per_query"] for row in measured
        )
    report = {
        "qualification": "standalone warm CPU kernel; NOT end-to-end latency, QPS, or admission cost",
        "binary": str(binary),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "fixture": str(fixture),
        "fixture_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
        "source_sha256": hashlib.sha256(
            Path(__file__)
            .resolve()
            .parent.parent.joinpath("zig/tools/bench_subgroup_routing.zig")
            .read_bytes()
        ).hexdigest(),
        "host": platform.platform(),
        "machine": platform.machine(),
        "rounds": rows,
        "medians": medians,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(medians, indent=2), flush=True)


if __name__ == "__main__":
    main()
