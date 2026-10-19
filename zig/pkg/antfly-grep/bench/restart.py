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

"""Paired decision searches after fresh workspace processes, using warm disk artifacts.

Synthetic native decision fixture only. Capture is measured separately; every
recorded query must return identical evidence and make zero provider calls.
"""

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import tempfile
from pathlib import Path

from session_compare import Service


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--files", type=int, default=32)
    parser.add_argument("--lines", type=int, default=128)
    parser.add_argument("--iterations", type=int, default=7)
    args = parser.parse_args()
    if min(args.files, args.lines, args.iterations) < 1:
        parser.error("sizes and iterations must be positive")
    binaries = {"current": args.current.resolve(), "baseline": args.baseline.resolve()}
    report = {
        "measurement": __doc__.strip(),
        "host": platform.platform(),
        "cpu_count": os.cpu_count(),
        "files": args.files,
        "lines_per_file": args.lines,
        "output_max_count": 1,
        "cgroup_cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
        if Path("/sys/fs/cgroup/cpu.max").exists()
        else None,
        "fixture_sha256": hashlib.sha256(args.library.read_bytes()).hexdigest(),
        "binaries": {
            key: {
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "version": subprocess.check_output(
                    [str(binary), "--version"], text=True
                ).strip(),
            }
            for key, binary in binaries.items()
        },
        "trials": [],
    }
    with tempfile.TemporaryDirectory(prefix="grep-restart-") as temporary:
        root = Path(temporary)
        (root / "src").mkdir()
        for i in range(args.files):
            (root / "src" / f"f{i:04}.py").write_text(
                f"def retry_{i}():\n" + "    STOP()\n" * args.lines
            )
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
        }
        env.update(HOME=temporary, XDG_CONFIG_HOME=temporary)
        query = [
            "--ask=retry",
            "--model=fixture",
            "--backend=local",
            f"--library={args.library.resolve()}",
            f"--cache-dir={root / 'scores'}",
            "--json",
            "--no-refine",
            "--max-count=1",
            "--no-rank",
            "--threads=1",
        ]
        service = Service(binaries["current"], root, "src", query, env)
        try:
            _, expected = service.search(require_warm=False)
        finally:
            service.close()
        report["evidence_sha256"] = hashlib.sha256(
            json.dumps(expected, sort_keys=True).encode()
        ).hexdigest()
        for iteration in range(-1, args.iterations):
            trial = {"load_average": os.getloadavg()}
            for key in list(binaries) if iteration % 2 else list(reversed(binaries)):
                service = Service(
                    binaries[key],
                    root,
                    "src",
                    [*query, "--cached-only", "--max-requests=0"],
                    env,
                )
                try:
                    value, evidence = service.search(require_warm=False)
                    if evidence != expected or value["summary"]["requests"] != 0:
                        raise RuntimeError("restart evidence or provider-call mismatch")
                    if (
                        key == "current"
                        and value["summary"]["stats"]["source_parses"] != 0
                    ):
                        raise RuntimeError("warm restart reparsed source")
                    trial[key] = {**value, "capture_seconds": service.capture_seconds}
                finally:
                    service.close()
            if iteration >= 0:
                report["trials"].append(trial)
        report["median"] = {
            key: {
                field: statistics.median(
                    trial[key][field] for trial in report["trials"]
                )
                for field in (
                    "seconds",
                    "producer_seconds",
                    "capture_seconds",
                    "response_bytes",
                )
            }
            for key in binaries
        }
        report["disk_bytes"] = sum(
            path.stat().st_size
            for path in (root / "scores").rglob("*")
            if path.is_file()
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
