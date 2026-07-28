#!/usr/bin/env python3
"""Extract and segment Antfly protocol traces from one or more process logs."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_line(raw: str) -> dict[str, Any] | None:
    start = raw.find("{")
    if start < 0:
        return None
    try:
        value = json.loads(raw[start:])
    except json.JSONDecodeError:
        return None
    if not isinstance(value, dict):
        return None
    family = value.get("family")
    if value.get("tag") not in ("antfly-protocol-trace", f"{family}-trace"):
        return None
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "family",
        choices=(
            "placement-readiness",
            "index-lifecycle",
            "derived-replay",
            "enrichment-lease",
        ),
    )
    parser.add_argument("outdir", type=Path)
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    written = 0
    for source in args.logs:
        segments: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for raw in source.read_text(errors="replace").splitlines():
            obj = parse_line(raw)
            if obj is None or obj.get("family") != args.family:
                continue
            event = obj.get("event")
            facts = event.get("facts") if isinstance(event, dict) else None
            if args.family == "index-lifecycle":
                # Ordinary index creation is useful raw telemetry but is not a
                # durable-generation lifecycle behavior.
                if (
                    isinstance(event, dict)
                    and event.get("name") == "RequestGeneration"
                    and isinstance(facts, dict)
                    and facts.get("durableWork") is False
                ):
                    continue
            trace_id = str(obj.get("traceId", "unknown"))
            segments[trace_id].append(obj)

        for trace_id, events in segments.items():
            events.sort(key=lambda item: int(item.get("seq", 0)))
            safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in trace_id)
            path = args.outdir / f"{args.family}-{source.stem}-{safe_id}.ndjson"
            path.write_text(
                "".join(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n" for event in events)
            )
            written += 1

    if written == 0:
        raise SystemExit(f"no {args.family} protocol traces found")
    print(f"wrote {written} {args.family} trace segment(s) to {args.outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
