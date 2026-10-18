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
from pathlib import Path
from registered import Client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    bins = {"current": args.current.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "host": platform.platform(),
        "cpu_count": os.cpu_count(),
        "load_average_start": os.getloadavg(),
        "measurement": "Alternating paired warm filesystem workspace refresh; no provider; forced rescan revision equality outside timing. Other host builds may contend for CPU.",
        "binaries": {
            k: {"sha256": hashlib.sha256(v.read_bytes()).hexdigest()}
            for k, v in bins.items()
        },
        "cases": [],
    }
    for count in [64, 512]:
        with tempfile.TemporaryDirectory(prefix="grep-watch-pair-") as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            for i in range(count):
                (root / "src" / f"f{i:04}.txt").write_text("STOP original\n")
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
            }
            env.update(HOME=tmp, XDG_CONFIG_HOME=tmp)
            clients = {
                k: Client(
                    v, root, "src", ["STOP"], env, start_args=["--workspace-watch"]
                )
                for k, v in bins.items()
            }
            try:
                for mode in ["unchanged", "inplace", "atomic", "add_delete"]:
                    trials = []
                    for n in range(11):
                        path = root / "src" / f"f{count - 1:04}.txt"
                        if mode == "inplace":
                            path.write_text(f"STOP inplace {n}\n")
                        elif mode == "atomic":
                            replacement = root / "src" / ".save-temp"
                            replacement.write_text(f"STOP atomic {n}\n")
                            os.replace(replacement, path)
                        elif mode == "add_delete":
                            path = root / "src" / "new.txt"
                            if n % 2 == 0:
                                path.write_text(f"STOP addition {n}\n")
                            elif path.exists():
                                path.unlink()
                        pair = {"load_average": os.getloadavg()}
                        for key in list(bins) if n % 2 else list(reversed(bins)):
                            value, rows = clients[key].exchange("refresh")
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
                            "mode": mode,
                            "trials": trials,
                            "median": {
                                k: {
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
