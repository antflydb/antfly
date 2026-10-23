#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Paired source-I/O counters and latency for native directory events.

Usage: watch_events.py CURRENT BASELINE OUTPUT
"""

import argparse
import os
import tempfile
import json
import hashlib
import platform
import statistics
import subprocess
from pathlib import Path
from registered import Client, rss_kib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--files", type=int, nargs="+", default=[64, 512])
    parser.add_argument("--directories", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=11)
    parser.add_argument("--workspace-memory-limit", default="1G")
    args = parser.parse_args()
    if min(*args.files, args.directories, args.iterations) < 1:
        parser.error("sizes and iterations must be positive")
    bins = {"current": args.current.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "host": platform.platform(),
        "cpu_count": os.cpu_count(),
        "affinity_cpus": len(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else None,
        "cgroup_cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
        if Path("/sys/fs/cgroup/cpu.max").exists()
        else None,
        "load_average_start": os.getloadavg(),
        "measurement": "Alternating paired warm filesystem workspace refresh; no provider; forced rescan revision equality outside timing. Other host builds may contend for CPU.",
        "binaries": {
            k: {
                "sha256": hashlib.sha256(v.read_bytes()).hexdigest(),
                "version": subprocess.check_output(
                    [str(v), "--version"], text=True
                ).strip(),
            }
            for k, v in bins.items()
        },
        "cases": [],
    }
    for count in args.files:
        with tempfile.TemporaryDirectory(prefix="grep-watch-pair-") as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            paths = []
            for i in range(count):
                parent = (
                    root / "src"
                    if args.directories == 1
                    else root / "src" / f"d{i % args.directories:04}"
                )
                parent.mkdir(exist_ok=True)
                path = parent / f"f{i:04}.txt"
                path.write_text("STOP original\n")
                paths.append(path)
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
            }
            env.update(HOME=tmp, XDG_CONFIG_HOME=tmp)
            clients = {
                k: Client(
                    v,
                    root,
                    "src",
                    ["STOP"],
                    env,
                    start_args=[
                        "--workspace-watch",
                        "--workspace-max-files=200000",
                        f"--workspace-memory-limit={args.workspace_memory_limit}",
                    ],
                )
                for k, v in bins.items()
            }
            try:
                for mode in ["unchanged", "inplace", "atomic", "add_delete"]:
                    trials = []
                    for n in range(args.iterations):
                        path = paths[-1]
                        if mode == "inplace":
                            path.write_text(f"STOP inplace {n}\n")
                        elif mode == "atomic":
                            replacement = path.parent / ".save-temp"
                            replacement.write_text(f"STOP atomic {n}\n")
                            os.replace(replacement, path)
                        elif mode == "add_delete":
                            path = path.parent / "new.txt"
                            if n % 2 == 0:
                                path.write_text(f"STOP addition {n}\n")
                            elif path.exists():
                                path.unlink()
                        pair = {"load_average": os.getloadavg()}
                        for key in list(bins) if n % 2 else list(reversed(bins)):
                            value, rows = clients[key].exchange("refresh")
                            if platform.system() == "Linux":
                                status = Path(
                                    f"/proc/{clients[key].process.pid}/status"
                                ).read_text()
                                value["rss_kib"] = next(
                                    int(line.split()[1])
                                    for line in status.splitlines()
                                    if line.startswith("VmRSS:")
                                )
                            else:
                                value["rss_kib"] = rss_kib(clients[key])
                            pair[key] = value
                        trials.append(pair)
                    for key, client in clients.items():
                        previous = client.ready["revision"]
                        value, _ = client.exchange("refresh", force_rescan=True)
                        assert value["ready"]["revision"] == previous, (
                            count,
                            mode,
                            key,
                            previous,
                            value,
                        )
                    report["cases"].append(
                        {
                            "files": count,
                            "directories": args.directories,
                            "capture_seconds": {
                                k: c.capture_seconds for k, c in clients.items()
                            },
                            "mode": mode,
                            "trials": trials,
                            "median": {
                                k: {
                                    "retained_bytes": statistics.median(
                                        t[k]["ready"]["retained_bytes"] for t in trials
                                    ),
                                    "rss_kib": statistics.median(
                                        t[k]["rss_kib"] for t in trials
                                    ),
                                    "seconds": statistics.median(
                                        t[k]["seconds"] for t in trials
                                    ),
                                    "fallback_runs": sum(
                                        t[k]["ready"]["watch_fallback"] for t in trials
                                    ),
                                    "sources_opened": statistics.median(
                                        t[k]["ready"]["sources_opened"] for t in trials
                                    ),
                                    "entries_visited": statistics.median(
                                        t[k]["ready"]["refresh_entries_visited"]
                                        for t in trials
                                    ),
                                    "namespace_paths_checked": statistics.median(
                                        t[k]["ready"].get("namespace_paths_checked", 0)
                                        for t in trials
                                    )
                                    if k == "current"
                                    else None,
                                    "source_bytes_read": statistics.median(
                                        t[k]["ready"]["source_bytes_read"]
                                        for t in trials
                                    ),
                                }
                                for k in bins
                            },
                        }
                    )
            finally:
                for client in clients.values():
                    client.close()
    report["load_average_end"] = os.getloadavg()
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
