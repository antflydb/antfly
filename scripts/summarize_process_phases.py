"""Attribute sampled process CPU/I/O to harness phases, without crossing restarts."""

import argparse
import bisect
import json
import math
from collections import Counter
from itertools import pairwise
from pathlib import Path

COUNTERS = (
    "user_cpu_ns",
    "system_cpu_ns",
    "pageins",
    "disk_read_bytes",
    "disk_written_bytes",
    "logical_written_bytes",
    "instructions",
    "cycles",
)


def summarize(samples, phases):
    boundaries = [float(phase["wall_time"]) for phase in phases]
    if any(not math.isfinite(t) for t in boundaries) or any(
        b <= a for a, b in pairwise(boundaries)
    ):
        raise ValueError("phase clock is not strictly increasing and finite")
    result = [
        {
            "phase": phase["phase"],
            "start_wall_time": boundary,
            "samples": 0,
            "peak_rss_bytes": 0,
            "peak_phys_footprint_bytes": 0,
            "counters": {},
            "combined_cpu": {"delta": 0, "observed_seconds": 0, "intervals": 0},
        }
        for phase, boundary in zip(phases, boundaries, strict=True)
    ]
    excluded = Counter()
    previous = None
    previous_phase = None
    for sample in samples:
        wall = float(sample["wall_time_s"])
        if not math.isfinite(wall):
            raise ValueError("invalid sample wall time")
        index = bisect.bisect_right(boundaries, wall) - 1
        if index < 0:
            previous = None
            excluded["before_first_phase"] += 1
            continue
        phase = result[index]
        phase["samples"] += 1
        phase["peak_rss_bytes"] = max(phase["peak_rss_bytes"], sample["rss_bytes"])
        phase["peak_phys_footprint_bytes"] = max(
            phase["peak_phys_footprint_bytes"], sample["phys_footprint_bytes"]
        )
        if previous is not None:
            identity = (sample["pid"], sample["start_abstime"])
            before_identity = (previous["pid"], previous["start_abstime"])
            elapsed = (sample["monotonic_ns"] - previous["monotonic_ns"]) / 1e9
            wall_elapsed = wall - previous["wall_time_s"]
            if identity != before_identity:
                excluded["process_lifetime_changed"] += 1
            elif previous_phase != index:
                excluded["phase_boundary"] += 1
            elif elapsed <= 0 or abs(wall_elapsed - elapsed) > max(0.25, elapsed * 0.1):
                excluded["clock_discontinuity"] += 1
            else:
                cpu_deltas = []
                for field in COUNTERS:
                    if field not in sample or field not in previous:
                        excluded[f"missing_{field}"] += 1
                        continue
                    if field.endswith("cpu_ns") and not all(
                        row.get("cpu_timebase_numer", 0) > 0
                        and row.get("cpu_timebase_denom", 0) > 0
                        for row in (sample, previous)
                    ):
                        # Old experimental receipts mislabeled Mach ticks ns.
                        excluded["uncertified_cpu_units"] += 1
                        continue
                    delta = sample[field] - previous[field]
                    if delta < 0:
                        excluded[f"counter_reset_{field}"] += 1
                        continue
                    counter = phase["counters"].setdefault(
                        field, {"delta": 0, "observed_seconds": 0, "intervals": 0}
                    )
                    counter["delta"] += delta
                    counter["observed_seconds"] += elapsed
                    counter["intervals"] += 1
                    if field.endswith("cpu_ns"):
                        cpu_deltas.append(delta)
                # Combine only CPU components observed in the *same* interval.
                # Equal accumulated durations do not prove overlapping samples.
                if len(cpu_deltas) == 2:
                    cpu = phase["combined_cpu"]
                    cpu["delta"] += sum(cpu_deltas)
                    cpu["observed_seconds"] += elapsed
                    cpu["intervals"] += 1
        previous, previous_phase = sample, index
    for phase in result:
        for counter in phase["counters"].values():
            counter["per_second"] = counter["delta"] / counter["observed_seconds"]
        cpu = phase["combined_cpu"]
        phase["average_cpu_cores"] = (
            cpu["delta"] / cpu["observed_seconds"] / 1e9 if cpu["intervals"] else None
        )
    return {
        "phases": result,
        "excluded_intervals": dict(excluded),
        "notes": [
            "CPU is process-wide, not request CPU; average cores may exceed one.",
            "Intervals crossing phase boundaries, clock discontinuities, or process lifetimes are excluded.",
            "Only certified Mach-to-nanosecond CPU samples are accepted.",
            "Harness phases may combine load and query; those phases cannot isolate query CPU.",
            "Sampled RSS and footprint peaks are not lifetime high-water marks.",
        ],
    }


def read_rows(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream if line.strip()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", type=Path)
    parser.add_argument("phases", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = summarize(read_rows(args.samples), read_rows(args.phases))
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        with args.output.open("x") as output:
            output.write(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
