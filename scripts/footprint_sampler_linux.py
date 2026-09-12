#!/usr/bin/env python3
"""Linux stand-in for antfly-circus footprint_sampler.py (which needs vm_stat).

Implements the argument subset run_vdbbench_qualification.sh uses:
  --capture-wired-baseline PATH
  --pid-file PATH --wired-baseline-file PATH --out PATH --timeline PATH --interval S

Process-tree footprint is taken from /proc/<pid>/status: VmRSS as the current
footprint and VmHWM as the ledger peak. Wired growth is reported as 0.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def read_status(pid: int) -> dict[str, int] | None:
    try:
        text = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
    except OSError:
        return None
    out: dict[str, int] = {}
    for line in text.splitlines():
        if line.startswith(("VmRSS:", "VmHWM:", "PPid:")):
            key, value = line.split(":", 1)
            out[key] = int(value.strip().split()[0])
    return out


def process_tree(root: int) -> list[int]:
    children: dict[int, list[int]] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        status = read_status(int(entry))
        if status is None:
            continue
        children.setdefault(status.get("PPid", 0), []).append(int(entry))
    tree = [root]
    stack = [root]
    while stack:
        for child in children.get(stack.pop(), []):
            tree.append(child)
            stack.append(child)
    return tree


def atomic_write(path: Path, value: object) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--wired-baseline-file", type=Path)
    parser.add_argument("--capture-wired-baseline", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--timeline", type=Path)
    parser.add_argument("--interval", type=float, default=0.3)
    args = parser.parse_args()

    if args.capture_wired_baseline:
        atomic_write(args.capture_wired_baseline, {"wired_bytes": 0, "captured_at": time.time()})
        return 0
    if not (args.pid_file and args.out):
        parser.error("--pid-file and --out are required for sampling")

    root_pid = int(args.pid_file.read_text().strip())
    state: dict[str, object] = {
        "kind": "native_process_tree",
        "schema_version": 3,
        "metric": "native_process_tree_memory_views",
        "platform": "linux_proc_status",
        "started": time.time(),
        "samples": 0,
        "failed_samples": 0,
        "baseline_source": "linux_no_wired",
        "peak_rss_bytes": 0,
        "process_tree_footprint_peak_bytes": 0,
        "wired_growth_peak_bytes": 0,
        "conservative_host_impact_peak_bytes": 0,
        "processes_seen": [],
        "footprint_fallback_samples": 0,
        "native_ledger_interval_seconds": 0.0,
        "valid_footprint_samples": 0,
        "footprint_fallback_peak_rss_bytes": 0,
    }
    seen: dict[int, dict[str, object]] = {}
    timeline = args.timeline.open("a", encoding="utf-8") if args.timeline else None
    try:
        while True:
            tree = process_tree(root_pid)
            rss = 0
            hwm = 0
            complete = True
            for pid in tree:
                status = read_status(pid)
                if status is None:
                    complete = False
                    continue
                rss += status.get("VmRSS", 0) * 1024
                hwm += status.get("VmHWM", 0) * 1024
                seen[pid] = {"pid": pid, "ppid": status.get("PPid", 0), "command": Path(f"/proc/{pid}/comm").read_text().strip() if Path(f"/proc/{pid}/comm").exists() else ""}
            if not tree or rss == 0:
                state["failed_samples"] = int(state["failed_samples"]) + 1
            else:
                point = {
                    "monotonic": time.monotonic(),
                    "wall_time": time.time(),
                    "rss_bytes": rss,
                    "process_tree_footprint_bytes": rss,
                    "process_tree_footprint_ledger_peak_bytes": hwm,
                    "conservative_host_impact_bytes": hwm,
                    "wired_growth_bytes": 0,
                    "footprint_complete": complete,
                    "pids": tree,
                }
                state["samples"] = int(state["samples"]) + 1
                state["peak_rss_bytes"] = max(int(state["peak_rss_bytes"]), rss)
                state["process_tree_footprint_peak_bytes"] = max(int(state["process_tree_footprint_peak_bytes"]), hwm)
                state["conservative_host_impact_peak_bytes"] = max(int(state["conservative_host_impact_peak_bytes"]), hwm)
                state["valid_footprint_samples"] = int(state["valid_footprint_samples"]) + 1
                state["processes_seen"] = [seen[p] for p in sorted(seen)]
                if timeline:
                    timeline.write(json.dumps(point, sort_keys=True) + "\n")
                    timeline.flush()
            atomic_write(args.out, state)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        if timeline:
            timeline.close()
    return 0 if int(state["samples"]) > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
