#!/usr/bin/env python3
"""Extract TLA+ trace ndjson from zap-style test logs.

Usage:
  scripts/extract-tla-traces.py txn <logfile> <outfile>
  scripts/extract-tla-traces.py raft <logfile> <outdir>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_wrapped_json(line: str) -> dict[str, Any] | None:
    start = line.find("{")
    if start < 0:
        return None
    try:
        value = json.loads(line[start:])
    except json.JSONDecodeError:
        return None
    if not isinstance(value, dict):
        return None
    return value


def dump_json_line(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def extract_txn(logfile: Path, outfile: Path) -> int:
    seen: set[str] = set()
    kept: list[str] = []

    for raw in logfile.read_text().splitlines():
        obj = parse_wrapped_json(raw)
        if not obj or obj.get("tag") != "antfly-trace":
            continue

        event = obj.get("event")
        if isinstance(event, dict) and event.get("name") == "ResolveIntentsOnShard":
            state = event.get("state")
            if isinstance(state, dict) and state.get("intentsCount") == 0:
                continue

        encoded = dump_json_line(obj)
        if encoded in seen:
            continue
        seen.add(encoded)
        kept.append(encoded)

    outfile.parent.mkdir(parents=True, exist_ok=True)
    if kept:
        outfile.write_text("".join(line + "\n" for line in kept))
    else:
        outfile.write_text("")
    return len(kept)


def should_start_new_raft_segment(current: list[dict[str, Any]], obj: dict[str, Any]) -> bool:
    if not current:
        return False
    event = obj.get("event")
    if not isinstance(event, dict):
        return False
    if event.get("name") != "InitState":
        return False
    return str(event.get("nid")) == "1"


def flush_raft_segment(outdir: Path, shard_id: str, segment_idx: int, segment: list[dict[str, Any]]) -> None:
    if not segment:
        return
    path = outdir / f"{shard_id}-{segment_idx:03d}.ndjson"
    path.write_text("".join(dump_json_line(obj) + "\n" for obj in segment))


def extract_raft(logfile: Path, outdir: Path) -> int:
    outdir.mkdir(parents=True, exist_ok=True)
    segments: dict[str, list[dict[str, Any]]] = {}
    segment_counts: dict[str, int] = {}
    written = 0

    for raw in logfile.read_text().splitlines():
        obj = parse_wrapped_json(raw)
        if not obj or obj.get("tag") != "trace":
            continue

        if not obj.get("shardID"):
            continue
        shard_id = str(obj["shardID"])
        current = segments.setdefault(shard_id, [])
        if should_start_new_raft_segment(current, obj):
            segment_counts[shard_id] = segment_counts.get(shard_id, 0) + 1
            flush_raft_segment(outdir, shard_id, segment_counts[shard_id], current)
            written += 1
            segments[shard_id] = []
            current = segments[shard_id]
        current.append(obj)

    for shard_id, segment in segments.items():
        if not segment:
            continue
        segment_counts[shard_id] = segment_counts.get(shard_id, 0) + 1
        flush_raft_segment(outdir, shard_id, segment_counts[shard_id], segment)
        written += 1

    return written


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)

    txn = sub.add_parser("txn")
    txn.add_argument("logfile", type=Path)
    txn.add_argument("outfile", type=Path)

    raft = sub.add_parser("raft")
    raft.add_argument("logfile", type=Path)
    raft.add_argument("outdir", type=Path)

    args = parser.parse_args()
    if args.mode == "txn":
        count = extract_txn(args.logfile, args.outfile)
        print(f"wrote {count} transaction trace event(s) to {args.outfile}")
        return 0
    count = extract_raft(args.logfile, args.outdir)
    print(f"wrote {count} raft trace file(s) to {args.outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
