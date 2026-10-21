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

"""Paired wide-repository namespace refresh qualification, with forced rescans.

Warm shared filesystem; independent native Linux hardware is not available.
Watch and metadata admission are explicit. Every mutation compares its revision
with a forced full observation outside the timing interval.
"""

import argparse
import hashlib
import json
import os
import platform
import statistics
import tempfile
from pathlib import Path
from registered import Client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--directories", type=int, default=5000)
    parser.add_argument("--rounds", type=int, default=7)
    args = parser.parse_args()
    binaries = {"current": args.current.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "host": platform.platform(),
        "measurement": __doc__,
        "directories": args.directories,
        "rounds": args.rounds,
        "binaries": {
            k: hashlib.sha256(v.read_bytes()).hexdigest() for k, v in binaries.items()
        },
        "trials": [],
    }
    cache = Path(__file__).resolve().parents[4] / ".zig-cache"
    with tempfile.TemporaryDirectory(dir=cache, prefix="grep-namespace-") as directory:
        root = Path(directory)
        (root / ".git").mkdir()
        for i in range(args.directories):
            path = root / f"src/d{i:05}/file.txt"
            path.parent.mkdir(parents=True)
            path.write_text("STOP original\n")
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
        }
        env.update(HOME=directory, XDG_CONFIG_HOME=directory)
        clients = {}
        try:
            for name, binary in binaries.items():
                clients[name] = Client(
                    binary, root, "src", [], env, start_args=("--workspace-watch",)
                )
            for iteration in range(args.rounds):
                trial = {"load_average": os.getloadavg()}
                for operation in ("stable", "edit"):
                    if operation == "edit":
                        (root / "src/d00000/file.txt").write_text(
                            f"STOP changed {iteration}\n"
                        )
                    trial[operation] = {}
                    revisions = []
                    for name in (
                        list(clients) if iteration % 2 else list(reversed(clients))
                    ):
                        measured, rows = clients[name].exchange("refresh")
                        ready = rows[-1]
                        trial[operation][name] = {
                            "seconds": measured["seconds"],
                            **{
                                k: ready[k]
                                for k in (
                                    "watch_fallback",
                                    "source_bytes_read",
                                    "refresh_entries_visited",
                                    "discovery_reused",
                                    "sources_opened",
                                    "namespace_paths_checked",
                                )
                            },
                        }
                        revisions.append(ready["revision"])
                    assert revisions[0] == revisions[1]
                for client in clients.values():
                    revision = client.ready["revision"]
                    _, rows = client.exchange("refresh", force_rescan=True)
                    assert rows[-1]["revision"] == revision
                report["trials"].append(trial)
        finally:
            for client in clients.values():
                client.close()
    report["median_seconds"] = {
        op: {
            name: statistics.median(t[op][name]["seconds"] for t in report["trials"])
            for name in binaries
        }
        for op in ("stable", "edit")
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["median_seconds"], indent=2))


if __name__ == "__main__":
    main()
