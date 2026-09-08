"""Keep worker, handoff, and nested checkpoint stages separate when diagnosing stalls."""

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

EVENTS = {
    "dense checkpoint worker ": "worker",
    "dense checkpoint handoff ": "handoff",
    "dense checkpoint install ": "install",
    "dense checkpoint rebase worker ": "rebase_worker",
    "dense checkpoint completion blockers ": "completion_blockers",
    "dense posting checkpoint staging ": "staging",
}


def summarize_lines(lines):
    events = defaultdict(list)
    reasons = Counter()
    invalid = []
    for number, line in enumerate(lines, 1):
        values = dict(re.findall(r"(\w+)=([^\s,]+)", line))
        if "shared vector-block maintenance needed " in line:
            reasons[values.get("reason", "unknown")] += 1
            for field in (
                "stable_tip_finalizing",
                "posting_base_has_vectors",
                "sequence_ready",
                "count_ready",
            ):
                if field in values:
                    reasons[f"{field}={values[field]}"] += 1
        for marker, kind in EVENTS.items():
            if marker not in line:
                continue
            row = {"line": number, **values}
            try:
                for field, value in values.items():
                    if field.endswith(("_ns", "_bytes")) or field in (
                        "sequence",
                        "generation",
                        "bytes",
                    ):
                        row[field] = None if value == "null" else int(value)
                        if row[field] is not None and row[field] < 0:
                            raise ValueError(field)
            except ValueError:
                invalid.append(number)
                continue
            events[kind].append(row)
    groups = {}
    for kind, rows in events.items():
        fields = sorted(
            {field for row in rows for field in row if field.endswith("_ns")}
        )
        timings = {}
        for field in fields:
            measured = [row for row in rows if row.get(field) is not None]
            timings[field] = {
                "measured_count": len(measured),
                "missing_count": len(rows) - len(measured),
                "sum_ms": sum(row[field] for row in measured) / 1e6,
                "max_ms": (
                    max((row[field] for row in measured), default=0) / 1e6
                    if measured
                    else None
                ),
                "largest_events": sorted(
                    measured, key=lambda row: row[field], reverse=True
                )[:5],
            }
        groups[kind] = {"count": len(rows), "timings": timings}
    return {
        "groups": groups,
        "readiness_observations": dict(reasons),
        "invalid_lines": invalid,
        "notes": [
            "Install is nested in handoff; staging is nested in worker. Never add these groups together.",
            "Thread CPU excludes other threads; wall minus CPU is not proof of I/O or CPU contention.",
            "Readiness counts are observations, not elapsed durations or independent rebuilds.",
            "Historical logs without worker/handoff events cannot attribute queue or completion waiting.",
            "Capture overlap and rebase worker time can overlap each other inside completed_wait; do not add them or label the remainder scheduling time.",
            "Lock deferrals count failed admission observations, not lock-held duration; they can include time before the worker completed.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with args.log.open(errors="replace") as stream:
        result = {"log": str(args.log), **summarize_lines(stream)}
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        with args.output.open("x") as output:
            output.write(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
