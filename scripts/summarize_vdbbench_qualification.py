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
                        percentile: round(
                            float(metrics.get(f"serial_latency_{percentile}", 0))
                            * 1000,
                            3,
                        )
                        for percentile in ("p50", "p95", "p99")
                    },
                    "recall": metrics.get("recall"),
                    "max_qps": metrics.get("qps"),
                    "concurrency": metrics.get("conc_num_list", []),
                    "concurrent_qps": metrics.get("conc_qps_list", []),
                    "concurrent_latency_ms": {
                        percentile: [
                            round(float(value) * 1000, 3)
                            for value in metrics.get(field, [])
                        ]
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


def phase_rss_profiles(run_root: Path) -> dict[str, dict[str, Any]]:
    """Split a shared RSS trace at explicit qualification phase boundaries."""
    phases_path = run_root / "phases.jsonl"
    rss_path = run_root / "rss-live.tsv"
    if not phases_path.is_file() or not rss_path.is_file():
        return {}

    phases: dict[str, float] = {}
    with phases_path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            event = json.loads(line)
            phases[str(event["phase"])] = float(event["wall_time"])

    samples: list[tuple[float, int]] = []
    with rss_path.open(encoding="utf-8") as handle:
        next(handle, None)
        for line in handle:
            fields = line.split()
            if len(fields) != 2:
                continue
            samples.append((float(fields[0]), int(fields[1])))

    windows = {
        "read_only": ("live_load_and_query_start", "live_load_and_query_end"),
        "mixed": ("mixed_profile_start", "mixed_profile_end"),
    }
    profiles: dict[str, dict[str, Any]] = {}
    for label, (start_name, end_name) in windows.items():
        start = phases.get(start_name)
        end = phases.get(end_name)
        if start is None or end is None:
            continue
        # The sampler stores whole-second wall time. Use a half-open interval
        # so adjacent phases cannot both claim the same boundary sample.
        values = [rss_kib for timestamp, rss_kib in samples if start <= timestamp < end]
        if not values:
            continue
        peak_kib = max(values)
        profiles[label] = {
            "peak_rss_bytes": peak_kib * 1024,
            "peak_rss_kib": peak_kib,
            "samples": len(values),
            "start_wall_time_s": start,
            "end_wall_time_s": end,
            "source": rss_path.name,
        }
    return profiles


def summarize(run_root: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {"runs": result_rows(run_root)}
    memory_profiles: dict[str, Any] = {}
    for footprint_path in sorted(run_root.glob("footprint*.json")):
        footprint = load_json(footprint_path)
        memory = {
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
        memory_profiles[footprint_path.stem] = memory
        if footprint_path.name == "footprint.json":
            summary["memory"] = memory
    if memory_profiles:
        summary["memory_profiles"] = memory_profiles

    rss_profiles: dict[str, Any] = {}
    for rss_path in sorted(run_root.glob("rss-*.json")):
        rss = load_json(rss_path)
        rss_profiles[rss_path.stem] = {
            key: rss.get(key)
            for key in (
                "peak_rss_bytes",
                "peak_rss_kib",
                "samples",
                "first_wall_time_s",
                "last_wall_time_s",
                "interval_seconds",
                "source",
            )
            if key in rss
        }
    if rss_profiles:
        summary["rss_profiles"] = rss_profiles

    phase_profiles = phase_rss_profiles(run_root)
    if phase_profiles:
        summary["phase_rss_profiles"] = phase_profiles

    public_profiles: dict[str, Any] = {}
    for profile_path in sorted(run_root.glob("public-query-profile*.json")):
        profile = load_json(profile_path)
        public_profiles[profile_path.stem] = {
            key: profile.get(key)
            for key in (
                "count",
                "recall",
                "mean_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
                "leaves_mean",
                "approximate_vectors_mean",
                "exact_vectors_mean",
                "server_timings",
            )
            if key in profile
        }
    if public_profiles:
        summary["public_query_profiles"] = public_profiles

    mixed_profiles: dict[str, Any] = {}
    for profile_path in sorted(run_root.glob("public-mixed-profile*.json")):
        profile = load_json(profile_path)
        mixed_profiles[profile_path.stem] = {
            key: profile.get(key)
            for key in (
                "duration_seconds",
                "query_workers",
                "write_workers",
                "write_batch",
                "query_count",
                "query_qps",
                "query_latency",
                "server_latency",
                "hbc_latency",
                "recall",
                "write_batches",
                "written_rows",
                "write_rows_per_second",
                "write_latency",
                "catchup_seconds",
                "errors",
            )
            if key in profile
        }
    if mixed_profiles:
        summary["public_mixed_profiles"] = mixed_profiles
    return summary


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} RUN_ROOT", file=sys.stderr)
        return 2
    run_root = Path(sys.argv[1]).resolve()
    summary = summarize(run_root)
    output = run_root / "qualification-summary.json"
    output.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
