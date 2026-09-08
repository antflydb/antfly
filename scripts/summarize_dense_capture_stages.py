"""Attribute capture and WAL stages without double-counting nested timers."""

import argparse
import json
import re
from pathlib import Path

from profile_vdbbench_public_query import timing_summary


def summarize(path, after_sequence=None, through_sequence=None):
    captures = []
    wal = []
    failed = 0
    for line in path.read_text(errors="replace").splitlines():
        if "dense capture stages " not in line and "dense WAL stages " not in line:
            continue
        values = dict(re.findall(r"(\w+)=(true|false|[0-9]+)", line))
        if after_sequence is not None or through_sequence is not None:
            sequence = int(values["sequence"])
            if after_sequence is not None and sequence <= after_sequence:
                continue
            if through_sequence is not None and sequence > through_sequence:
                continue
        if "dense capture stages " in line:
            if values.get("completed") != "true":
                failed += 1
                continue
            captures.append(values)
        else:
            wal.append(values)

    def stages(rows):
        return {
            field: {
                "count": len(values),
                "sum_ms": sum(values),
                **timing_summary(values),
            }
            for field in sorted(
                {key for row in rows for key in row if key.endswith("_ns")}
            )
            for values in [[int(row[field]) / 1e6 for row in rows if field in row]]
        }

    return {
        "log": str(path),
        "after_sequence": after_sequence,
        "through_sequence": through_sequence,
        "completed_captures": len(captures),
        "incomplete_captures": failed,
        "capture_stages": stages(captures),
        "wal_appends": len(wal),
        "synced_wal_appends": sum(row.get("sync") == "true" for row in wal),
        "unsynced_wal_appends": sum(row.get("sync") == "false" for row in wal),
        "wal_bytes": sum(int(row.get("bytes", 0)) for row in wal),
        "wal_stages": stages(wal),
        "note": "WAL stages are nested in capture wal_ns; do not add their durations or stage percentiles.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--after-sequence", type=int)
    parser.add_argument("--through-sequence", type=int)
    args = parser.parse_args()
    if any(
        value is not None and value < 0
        for value in (args.after_sequence, args.through_sequence)
    ):
        parser.error("source sequences must be nonnegative")
    if (
        args.after_sequence is not None
        and args.through_sequence is not None
        and args.after_sequence >= args.through_sequence
    ):
        parser.error("sequence interval must be nonempty")
    result = (
        json.dumps(
            [
                summarize(path, args.after_sequence, args.through_sequence)
                for path in args.logs
            ],
            indent=2,
        )
        + "\n"
    )
    if args.output:
        args.output.write_text(result)
    print(result, end="")


if __name__ == "__main__":
    main()
