#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Summarize warmed Nsight CUDA traces without conflating GPU busy time with SM utilization.

Each forward ends with a normalized 768-float D2H readback. GPU activity and CUDA
API totals overlap; they must not be added. Hardware-counter metrics are separate.
"""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3
import statistics


def merged_ns(intervals):
    total = 0
    lo = hi = None
    for begin, end in sorted(intervals):
        if hi is None:
            lo, hi = begin, end
        elif begin <= hi:
            hi = max(hi, end)
        else:
            total += hi - lo
            lo, hi = begin, end
    return total + (0 if hi is None else hi - lo)


def category(name):
    n = name.lower()
    if any(x in n for x in ("attention", "fmha", "sdpa")):
        return "attention"
    if ("gemm" in n and "embedding_gemma" not in n) or any(
        x in n for x in ("cutlass", "ampere_", "turing_")
    ):
        return "gemm"
    if any(x in n for x in ("rms", "norm", "rope")):
        return "norm_rope"
    return "other_fusion_or_pointwise"


def summarize(path):
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    tables = {
        r[0] for r in con.execute("select name from sqlite_master where type='table'")
    }
    kernels = [
        dict(r)
        for r in con.execute("""
        SELECT k.*, s.value AS name FROM CUPTI_ACTIVITY_KIND_KERNEL k
        JOIN StringIds s ON k.demangledName=s.id ORDER BY k.start""")
    ]
    copies = (
        [
            dict(r)
            for r in con.execute(
                "SELECT * FROM CUPTI_ACTIVITY_KIND_MEMCPY ORDER BY start"
            )
        ]
        if "CUPTI_ACTIVITY_KIND_MEMCPY" in tables
        else []
    )
    readbacks = [c for c in copies if c["copyKind"] == 2 and c["bytes"] == 768 * 4]
    if not kernels or not readbacks:
        raise ValueError("Expected captured forward kernels and 768-float readbacks")
    calls = []
    previous = 0
    for readback in readbacks:
        end = readback["end"]
        selected = [k for k in kernels if previous <= k["start"] and k["end"] <= end]
        transfers = [c for c in copies if previous <= c["start"] and c["end"] <= end]
        previous = end
        if not selected:
            continue
        begin = min([k["start"] for k in selected] + [c["start"] for c in transfers])
        active = merged_ns(
            [(k["start"], k["end"]) for k in selected]
            + [(c["start"], c["end"]) for c in transfers]
        )
        span = end - begin
        calls.append(
            {
                "gpu_span_ms": span / 1e6,
                "gpu_activity_ms": active / 1e6,
                "gpu_gap_ms": (span - active) / 1e6,
                "gpu_activity_fraction": active / span,
                "kernel_count": len(selected),
                "readback_end_ns": end,
            }
        )
    groups = defaultdict(list)
    categories = defaultdict(int)
    for k in kernels:
        groups[k["name"]].append(k)
        categories[category(k["name"])] += k["end"] - k["start"]
    api = []
    for table in ("CUPTI_ACTIVITY_KIND_RUNTIME", "CUPTI_ACTIVITY_KIND_DRIVER"):
        if table not in tables:
            continue
        for r in con.execute(f"""SELECT s.value name, COUNT(*) calls, SUM(a.end-a.start)/1e6 ms
              FROM {table} a JOIN StringIds s ON a.nameId=s.id GROUP BY s.value ORDER BY ms DESC"""):
            api.append(dict(r))
    total_kernel = sum(k["end"] - k["start"] for k in kernels)
    top = []
    for name, rows in sorted(
        groups.items(),
        key=lambda kv: sum(x["end"] - x["start"] for x in kv[1]),
        reverse=True,
    ):
        r = rows[0]
        duration = sum(x["end"] - x["start"] for x in rows)
        top.append(
            {
                "name": name,
                "calls": len(rows),
                "total_ms": duration / 1e6,
                "per_forward_ms": duration / 1e6 / len(calls),
                "kernel_time_fraction": duration / total_kernel,
                "registers_per_thread": r["registersPerThread"],
                "block": [r["blockX"], r["blockY"], r["blockZ"]],
                "grid": [r["gridX"], r["gridY"], r["gridZ"]],
                "shared_bytes": r["staticSharedMemory"] + r["dynamicSharedMemory"],
                "local_bytes_per_thread": r["localMemoryPerThread"],
            }
        )
    result = {
        "source": str(path),
        "forwards": len(calls),
        "calls": calls,
        "median": {
            k: statistics.median(c[k] for c in calls)
            for k in calls[0]
            if k != "readback_end_ns"
        },
        "kernel_categories_per_forward_ms": {
            k: v / 1e6 / len(calls) for k, v in categories.items()
        },
        "top_kernels": top,
        "cuda_api": sorted(api, key=lambda x: x["ms"], reverse=True),
        "copy_bytes": {
            str(kind): sum(c["bytes"] for c in copies if c["copyKind"] == kind)
            for kind in {c["copyKind"] for c in copies}
        },
        "interpretation": "GPU activity fraction measures time with a captured kernel or copy active, not SM occupancy, Tensor Core utilization or achieved bandwidth. Kernel and API durations overlap.",
    }
    con.close()
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("sqlite", type=Path)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    a.out.write_text(json.dumps(summarize(a.sqlite), indent=2) + "\n")
