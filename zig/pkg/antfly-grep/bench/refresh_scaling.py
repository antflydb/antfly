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

"""Paired startup and dirty-refresh controls; lexical queries need no provider."""

import argparse
import hashlib
import json
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path

from registered import Client


def exchange(service, op, **fields):
    # Client.search requires a positive result; all fixtures below contain STOP.
    return service.exchange(op, **fields)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=11)
    parser.add_argument("--functions-per-file", type=int, default=64)
    parser.add_argument("--files", type=int, nargs="+", default=[64, 256])
    args = parser.parse_args()
    paths = {"current": args.binary.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "measurement": "Warm paired dirty refresh, alternating order; forced rescan correctness outside timing. Initial lexical capture includes protocol startup. No decision provider.",
        "binaries": {
            n: {
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                "size_bytes": p.stat().st_size,
            }
            for n, p in paths.items()
        },
        "corpora": [],
    }
    report["functions_per_file"] = args.functions_per_file
    report["startup_measurement"] = (
        "Seven alternating new-process captures on a warm filesystem, after one initial warmup per binary; includes protocol startup."
    )
    for count in args.files:
        with tempfile.TemporaryDirectory(prefix="grep-refresh-control-") as directory:
            root = Path(directory)
            source = root / "src"
            source.mkdir()
            for i in range(count):
                (source / f"f{i:04}.py").write_text(
                    "def retry():\n    STOP()\n" * args.functions_per_file
                )
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
            }
            env.update(HOME=str(root), XDG_CONFIG_HOME=str(root / "config"))
            clients = {}
            initial = {}
            try:
                for name, binary in paths.items():
                    start = time.monotonic()
                    client = Client(
                        binary,
                        root,
                        "src",
                        ["STOP"],
                        env,
                        start_args=["--workspace-watch"],
                    )
                    initial[name] = {
                        "seconds": time.monotonic() - start,
                        "ready": client.ready,
                    }
                    clients[name] = client
                    exchange(client, "refresh")
                startup_trials = []
                for iteration in range(7):
                    pair = {}
                    for name in (
                        ["current", "baseline"]
                        if iteration % 2
                        else ["baseline", "current"]
                    ):
                        start = time.monotonic()
                        candidate = Client(
                            paths[name],
                            root,
                            "src",
                            ["STOP"],
                            env,
                            start_args=["--workspace-watch"],
                        )
                        pair[name] = time.monotonic() - start
                        candidate.close()
                    startup_trials.append(pair)
                trials = []
                for i in range(args.rounds):
                    (source / "f0000.py").write_text(f"STOP updated {i}\n")
                    order = (
                        ["current", "baseline"] if i % 2 else ["baseline", "current"]
                    )
                    trial = {}
                    for name in order:
                        result, rows = exchange(clients[name], "refresh")
                        trial[name] = result
                        revision = rows[-1]["revision"]
                        _, search = exchange(
                            clients[name],
                            "search",
                            revision=revision,
                            args=["STOP", "--json-compact"],
                        )
                        stats = next(
                            row["stats"] for row in search if row["type"] == "summary"
                        )
                        assert (
                            stats["complete"] and stats["files_with_matches"] == count
                        )
                    trials.append(trial)
                for name, client in clients.items():
                    revision = client.ready["revision"]
                    _, full = exchange(client, "refresh", force_rescan=True)
                    assert full[-1]["revision"] == revision
                report["corpora"].append(
                    {
                        "files": count,
                        "initial_warmup": initial,
                        "startup_trials": startup_trials,
                        "startup_median_seconds": {
                            name: statistics.median(t[name] for t in startup_trials)
                            for name in clients
                        },
                        "trials": trials,
                        "median_seconds": {
                            name: statistics.median(t[name]["seconds"] for t in trials)
                            for name in clients
                        },
                        "revision_matches_forced_rescan": True,
                    }
                )
            finally:
                for client in clients.values():
                    client.close()
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
