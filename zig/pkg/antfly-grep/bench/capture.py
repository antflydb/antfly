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

"""Alternating fresh-process workspace captures on warm filesystem data.

No cache eviction is attempted. Timings include process spawn through readiness;
RSS is sampled after readiness, and revision equality is checked outside timing.
"""

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

from registered import Client, rss_kib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--files", type=int, nargs="+", default=[1000, 10000])
    parser.add_argument("--directories", type=int, default=64)
    parser.add_argument("--iterations", type=int, default=7)
    args = parser.parse_args()
    if min(*args.files, args.directories, args.iterations) < 1:
        parser.error("sizes and iterations must be positive")
    binaries = {"current": args.current.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "host": platform.platform(),
        "cpu_count": os.cpu_count(),
        "cgroup_cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
        if Path("/sys/fs/cgroup/cpu.max").exists()
        else None,
        "measurement": __doc__.strip(),
        "binaries": {
            key: {
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "version": subprocess.check_output(
                    [str(binary), "--version"], text=True
                ).strip(),
            }
            for key, binary in binaries.items()
        },
        "cases": [],
    }
    for count in args.files:
        with tempfile.TemporaryDirectory(prefix="grep-capture-") as tmp:
            root = Path(tmp)
            for i in range(count):
                parent = root / "src" / f"d{i % args.directories:04}"
                parent.mkdir(parents=True, exist_ok=True)
                (parent / f"f{i:06}.txt").write_text("STOP original\n")
            env = {
                key: value
                for key, value in os.environ.items()
                if not key.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
            }
            env.update(HOME=tmp, XDG_CONFIG_HOME=tmp)
            trials = []
            revision = None
            # One unrecorded warmup pair, followed by alternating serial trials.
            for iteration in range(-1, args.iterations):
                trial = {"load_average": os.getloadavg()}
                order = list(binaries) if iteration % 2 else list(reversed(binaries))
                for key in order:
                    started = time.monotonic()
                    client = Client(
                        binaries[key],
                        root,
                        "src",
                        ["STOP"],
                        env,
                        start_args=[
                            "--workspace-watch",
                            "--workspace-max-files=200000",
                            "--workspace-memory-limit=1G",
                        ],
                    )
                    elapsed = time.monotonic() - started
                    try:
                        if revision is None:
                            revision = client.ready["revision"]
                        if revision != client.ready["revision"]:
                            raise RuntimeError("capture revision mismatch")
                        if platform.system() == "Linux":
                            status = Path(
                                f"/proc/{client.process.pid}/status"
                            ).read_text()
                            rss = next(
                                int(line.split()[1])
                                for line in status.splitlines()
                                if line.startswith("VmRSS:")
                            )
                        else:
                            rss = rss_kib(client)
                        trial[key] = {
                            "seconds": elapsed,
                            "rss_kib": rss,
                            "ready": client.ready,
                        }
                    finally:
                        client.close()
                if iteration >= 0:
                    trials.append(trial)
            report["cases"].append(
                {
                    "files": count,
                    "directories": args.directories,
                    "trials": trials,
                    "median": {
                        key: {
                            "seconds": statistics.median(
                                t[key]["seconds"] for t in trials
                            ),
                            "rss_kib": statistics.median(
                                t[key]["rss_kib"] for t in trials
                            ),
                            "retained_bytes": statistics.median(
                                t[key]["ready"]["retained_bytes"] for t in trials
                            ),
                        }
                        for key in binaries
                    },
                }
            )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
