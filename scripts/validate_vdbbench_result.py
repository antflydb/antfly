#!/usr/bin/env python3
"""Reject partial VectorDBBench stages, even when the client labels them NORMAL."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any


def validate_lifecycle_log(path: Path) -> None:
    failures = (
        "quarantined after repeated zero progress",
        "PostingWalMutationOutsideCapture",
        "PostingWalMutationStoreUnavailable",
        "PostingStoreRequiresReopen",
    )
    with path.open(encoding="utf-8", errors="replace") as handle:
        for number, line in enumerate(handle, 1):
            if any(failure in line for failure in failures):
                raise ValueError(f"native lifecycle failure at {path}:{number}")


def positive_finite(value: Any) -> bool:
    return (
        isinstance(value, (float, int))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def validate_metrics(
    result: dict[str, Any],
    expected_load_count: int,
    require_serial: bool,
    expected_concurrency: list[int],
) -> None:
    if result.get("label") == "x":
        raise ValueError("VectorDBBench reported a failed stage")
    metrics = result.get("metrics", {})
    loaded = metrics.get("inserted_count", 0) or metrics.get("max_load_count", 0) or 0
    if (
        not isinstance(loaded, (float, int))
        or not math.isfinite(loaded)
        or loaded < expected_load_count
    ):
        raise ValueError(f"loaded {loaded}, expected at least {expected_load_count}")
    if require_serial:
        recall = metrics.get("recall", 0)
        if (
            not positive_finite(recall)
            or recall > 1
            or not positive_finite(metrics.get("serial_latency_p95", 0))
        ):
            raise ValueError("missing or invalid serial recall/latency")
    if expected_concurrency:
        if metrics.get("conc_num_list") != expected_concurrency:
            raise ValueError(
                f"incomplete concurrency curve: got {metrics.get('conc_num_list')}, expected {expected_concurrency}"
            )
        for field in (
            "conc_qps_list",
            "conc_latency_avg_list",
            "conc_latency_p95_list",
            "conc_latency_p99_list",
        ):
            values = metrics.get(field, [])
            if len(values) != len(expected_concurrency) or not all(
                positive_finite(value) for value in values
            ):
                raise ValueError(
                    f"missing or invalid concurrency measurements: {field}"
                )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_root", type=Path)
    parser.add_argument("label")
    parser.add_argument("expected_load_count", type=int)
    parser.add_argument("require_serial", type=int, choices=(0, 1))
    parser.add_argument("--server-log", type=Path)
    parser.add_argument(
        "expected_concurrency", help="comma-separated curve, or empty for serial-only"
    )
    args = parser.parse_args()
    matches = []
    for path in args.results_root.rglob("*.json"):
        with path.open(encoding="utf-8") as handle:
            for result in json.load(handle).get("results", []):
                if (
                    result.get("task_config", {}).get("db_config", {}).get("db_label")
                    == args.label
                ):
                    matches.append((path, result))
    if len(matches) != 1:
        raise SystemExit(
            f"expected one result for {args.label!r}, found {len(matches)}"
        )
    path, result = matches[0]
    try:
        validate_metrics(
            result,
            args.expected_load_count,
            bool(args.require_serial),
            [int(value) for value in args.expected_concurrency.split(",") if value],
        )
        if args.server_log is not None:
            validate_lifecycle_log(args.server_log)
    except ValueError as error:
        raise SystemExit(
            f"VectorDBBench stage {args.label!r} is not qualified: {error}; see {path}"
        ) from error


if __name__ == "__main__":
    main()
