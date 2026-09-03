#!/usr/bin/env python3
"""Summarize the critical path of an Nsight Systems CUDA prefill trace.

The Nsight SQLite export reports CUDA API time and device time independently.
Adding those totals is misleading because asynchronous launch work normally
overlaps the kernels already queued on the device.  This tool anchors the
request at the first token-embedding kernel and the final argmax kernel, then
reports device busy/idle time alongside CUDA API totals in the same window.

It intentionally depends only on Python's standard library so a captured
trace can be audited on a CPU-only host.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Iterable


START_KERNEL = "termite_embedding_lookup_q4_0_f32"
END_KERNEL = "termite_argmax_last_row_f32"


def _category(name: str) -> str:
    if "gqa_attention_prefill" in name:
        return "attention"
    if "bf16" in name and (name.startswith("ampere_") or "cutlass::Kernel2" in name):
        return "bf16_gemm"
    if name == "termite_linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4":
        return "ple_gate_q4"
    if name == "termite_quantize_f32_q8_1_rows":
        return "q8_1_quantize"
    if name == "termite_activation_multiply_f32":
        return "ffn_activation"
    if name == "termite_f32_to_bf16":
        return "bf16_staging"
    if "rms_norm" in name or "rope" in name:
        return "norm_and_rope"
    if "embedding_lookup" in name:
        return "embedding"
    if "argmax" in name or "lm_head" in name:
        return "lm_head"
    return "other"


def _merged_duration_ns(intervals: Iterable[tuple[int, int]]) -> int:
    ordered = sorted(intervals)
    if not ordered:
        return 0
    total = 0
    current_start, current_end = ordered[0]
    for start, end in ordered[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
            continue
        total += current_end - current_start
        current_start, current_end = start, end
    return total + current_end - current_start


def _require_tables(connection: sqlite3.Connection) -> None:
    required = {
        "CUPTI_ACTIVITY_KIND_KERNEL",
        "CUPTI_ACTIVITY_KIND_RUNTIME",
        "StringIds",
    }
    present = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    missing = sorted(required - present)
    if missing:
        raise ValueError(
            f"not an Nsight CUDA SQLite export; missing tables: {', '.join(missing)}"
        )


def summarize(path: Path) -> dict[str, object]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        _require_tables(connection)
        anchors = connection.execute(
            """
            SELECT
                MIN(CASE WHEN strings.value = ? THEN kernels.start END),
                MAX(CASE WHEN strings.value = ? THEN kernels.end END)
            FROM CUPTI_ACTIVITY_KIND_KERNEL AS kernels
            JOIN StringIds AS strings ON strings.id = kernels.demangledName
            """,
            (START_KERNEL, END_KERNEL),
        ).fetchone()
        request_start, request_end = anchors
        if request_start is None or request_end is None or request_end <= request_start:
            raise ValueError(
                f"could not find a valid {START_KERNEL} -> {END_KERNEL} request window"
            )

        category_ns: dict[str, int] = defaultdict(int)
        category_launches: dict[str, int] = defaultdict(int)
        intervals: list[tuple[int, int]] = []
        for start, end, name in connection.execute(
            """
            SELECT kernels.start, kernels.end, strings.value
            FROM CUPTI_ACTIVITY_KIND_KERNEL AS kernels
            JOIN StringIds AS strings ON strings.id = kernels.demangledName
            WHERE kernels.start >= ? AND kernels.end <= ?
            """,
            (request_start, request_end),
        ):
            category = _category(name)
            category_ns[category] += end - start
            category_launches[category] += 1
            intervals.append((start, end))

        elapsed_ns = request_end - request_start
        busy_ns = _merged_duration_ns(intervals)
        api: dict[str, dict[str, float | int]] = {}
        for name, calls, duration_ns in connection.execute(
            """
            SELECT strings.value, COUNT(*), SUM(runtime.end - runtime.start)
            FROM CUPTI_ACTIVITY_KIND_RUNTIME AS runtime
            JOIN StringIds AS strings ON strings.id = runtime.nameId
            WHERE runtime.start >= ? AND runtime.start < ?
            GROUP BY strings.value
            ORDER BY SUM(runtime.end - runtime.start) DESC
            """,
            (request_start, request_end),
        ):
            api[name] = {"calls": calls, "host_api_ms": duration_ns / 1_000_000}

        categories = {
            category: {
                "launches": category_launches[category],
                "device_ms": duration_ns / 1_000_000,
                "request_fraction": duration_ns / elapsed_ns,
            }
            for category, duration_ns in sorted(
                category_ns.items(), key=lambda item: item[1], reverse=True
            )
        }
        return {
            "schema": "antfly.cuda_prefill_nsys_summary.v1",
            "source": str(path.resolve()),
            "anchors": {"start_kernel": START_KERNEL, "end_kernel": END_KERNEL},
            "request": {
                "elapsed_ms": elapsed_ns / 1_000_000,
                "device_busy_ms": busy_ns / 1_000_000,
                "device_idle_ms_upper_bound": (elapsed_ns - busy_ns) / 1_000_000,
                "device_utilization": busy_ns / elapsed_ns,
                "kernel_launches": len(intervals),
            },
            "device_categories": categories,
            "cuda_api_in_request": api,
            "interpretation": {
                "cuda_api_time_is_not_additive": True,
                "reason": "asynchronous CUDA API calls overlap queued device work",
            },
        }
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sqlite", type=Path, help="Nsight Systems SQLite export")
    parser.add_argument("--output", type=Path, help="write JSON here instead of stdout")
    args = parser.parse_args()
    summary = summarize(args.sqlite)
    encoded = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(encoded, end="")
    else:
        args.output.write_text(encoded, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
