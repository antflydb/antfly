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

"""Measure work admitted by a zero-region budget, including cold identities.

Both binaries run resident against identical 4,096-line captured bytes. No
provider requests are allowed. Per-trial evidence records the actual budget,
completion, evaluated-region and transport counters; host load is retained.
"""

import argparse
import hashlib
import json
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path

from session_compare import Service


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--rounds", type=int, default=9)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binaries = {
        name: getattr(args, name).resolve() for name in ("baseline", "candidate")
    }
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
    }
    cache_root = Path(__file__).resolve().parents[3] / ".zig-cache"
    cache_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="grep-budget-", dir=cache_root
    ) as directory:
        root = Path(directory)
        env["HOME"] = str(root / "home")
        env["XDG_CONFIG_HOME"] = str(root / "config")
        (root / "source.txt").write_text("STOP bounded admission\n" * 4096)
        services = {
            name: Service(binary, root, "source.txt", [], env)
            for name, binary in binaries.items()
        }
        trials = []
        try:
            for trial in range(args.rounds):
                for cache in (True, False):
                    for name in (
                        ("baseline", "candidate")
                        if trial % 2 == 0
                        else ("candidate", "baseline")
                    ):
                        service = services[name]
                        flags = [
                            "--ask=" + str(trial),
                            "--model=fixture",
                            "--no-refine",
                            "--unit=line",
                            "--context-lines=500",
                            "--max-regions=0",
                            "--max-requests=1",
                            "--threads=1",
                            "--cache-dir=" + str(root / "scores"),
                        ]
                        if not cache:
                            flags.append("--no-cache")
                        request = {
                            "version": 1,
                            "id": trial,
                            "op": "search",
                            "revision": service.ready["revision"],
                            "args": flags,
                        }
                        started = time.monotonic()
                        service.process.stdin.write(
                            json.dumps(request).encode() + b"\n"
                        )
                        service.process.stdin.flush()
                        rows = []
                        while True:
                            row = service.read()
                            rows.append(row)
                            if row["type"] == "workspace_end":
                                break
                        elapsed = time.monotonic() - started
                        summary = next(
                            (row for row in rows if row["type"] == "summary"), None
                        )
                        if summary is None:
                            raise RuntimeError(rows)
                        if (
                            rows[-1]["exit_code"] != 2
                            or summary["requests"]
                            or summary["stats"]["regions_evaluated"]
                            or summary["stats"]["complete"]
                        ):
                            raise RuntimeError(rows)
                        trials.append(
                            {
                                "binary": name,
                                "cache": cache,
                                "seconds": elapsed,
                                "load_average": os.getloadavg(),
                                "summary": summary,
                            }
                        )
        finally:
            for service in services.values():
                service.close()
        report = {
            "platform": platform.platform(),
            "binaries": {
                name: hashlib.sha256(binary.read_bytes()).hexdigest()
                for name, binary in binaries.items()
            },
            "median_seconds": {
                name: {
                    str(cache): statistics.median(
                        row["seconds"]
                        for row in trials
                        if row["binary"] == name and row["cache"] == cache
                    )
                    for cache in (True, False)
                }
                for name in binaries
            },
            "trials": trials,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report["median_seconds"]))


if __name__ == "__main__":
    main()
