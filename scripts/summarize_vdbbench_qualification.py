#!/usr/bin/env python3
"""Summarize public VectorDBBench load, query, restart, and memory evidence."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def result_rows(run_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted((run_root / "results").rglob("result*.json")):
        payload = load_json(path)
        for result in payload.get("results", []):
            task = result.get("task_config", {})
            metrics = result.get("metrics", {})
            db_config = task.get("db_config", {})
            rows.append(
                {
                    "label": db_config.get("db_label"),
                    "case": task.get("case_config", {}).get("case_id"),
                    "insert_seconds": metrics.get("insert_duration"),
                    "ready_seconds": metrics.get("load_duration"),
                    "serial_latency_ms": {
                        percentile: round(float(metrics.get(f"serial_latency_{percentile}", 0)) * 1000, 3)
                        for percentile in ("p50", "p95", "p99")
                    },
                    "recall": metrics.get("recall"),
                    "max_qps": metrics.get("qps"),
                    "concurrency": metrics.get("conc_num_list", []),
                    "concurrent_qps": metrics.get("conc_qps_list", []),
                    "concurrent_latency_ms": {
                        percentile: [round(float(value) * 1000, 3) for value in metrics.get(field, [])]
                        for percentile, field in (
                            ("avg", "conc_latency_avg_list"),
                            ("p95", "conc_latency_p95_list"),
                            ("p99", "conc_latency_p99_list"),
                        )
                    },
                    "source": str(path.relative_to(run_root)),
                }
            )
    return rows


def summarize(run_root: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {"runs": result_rows(run_root)}
    footprint_path = run_root / "footprint.json"
    if footprint_path.exists():
        footprint = load_json(footprint_path)
        summary["memory"] = {
            key: footprint.get(key)
            for key in (
                "demand_peak_bytes",
                "phys_footprint_ledger_peak_bytes",
                "peak_rss_bytes",
                "wired_growth_peak_bytes",
                "samples",
            )
            if key in footprint
        }
    return summary


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} RUN_ROOT", file=sys.stderr)
        return 2
    run_root = Path(sys.argv[1]).resolve()
    summary = summarize(run_root)
    output = run_root / "qualification-summary.json"
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
