#!/usr/bin/env python3
"""Summarize ANTFLY_TEST_TIMINGS=1 logs, including buffered CI/partition output.

Columns in the runner's TIMING records are setup, body, I/O cleanup, and
allocator cleanup in integer nanoseconds, followed by the full test name.
Summed per-test elapsed durations are not parallel suite wall time or CPU time.
"""

import argparse
import json
from pathlib import Path
import re

FIELDS = ("setup", "body", "io_cleanup", "allocator_cleanup")
RECORD = re.compile(r"TIMING\t(\d+)\t(\d+)\t(\d+)\t(\d+)\t([^\r\n]+)")


def parse_timings(text):
    records = []
    for match in RECORD.finditer(text):
        row = dict(zip(FIELDS, (int(value) / 1e9 for value in match.groups()[:4])))
        row["total"] = sum(row.values())
        row["name"] = match[5]
        records.append(row)
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", type=Path, nargs="+")
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--json", type=Path, help="Write all records, in seconds")
    args = parser.parse_args()
    rows = []
    for path in args.logs:
        rows.extend(parse_timings(path.read_text(errors="replace")))
    if not rows:
        parser.error("no TIMING records found")
    rows.sort(key=lambda row: row["total"], reverse=True)
    if args.json:
        args.json.write_text(json.dumps(rows, indent=2) + "\n")
    print(f"{len(rows)} test executions; summed durations (not suite wall time):")
    for field in (*FIELDS, "total"):
        print(f"  {field}: {sum(row[field] for row in rows):.3f}s")
    print("\n total_s   body_s  io_cleanup_s  alloc_cleanup_s  setup_s  test")
    for row in rows[: args.top]:
        print(
            f"{row['total']:8.3f} {row['body']:8.3f} "
            f"{row['io_cleanup']:13.3f} {row['allocator_cleanup']:16.3f} "
            f"{row['setup']:8.3f}  {row['name']}"
        )


if __name__ == "__main__":
    main()
