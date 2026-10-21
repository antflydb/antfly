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

"""Paired output-memory and prepared-discovery benchmarks; fixtures only."""

import argparse
import hashlib
import json
import os
import platform
import random
import shutil
import statistics
import subprocess
import tempfile
from pathlib import Path

from compare import first_result, linux_time, measure, target_rss


def signature(argv, cwd, env, json_output=False, unordered=False):
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, check=False)
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr.decode(errors="replace"))
    value = result.stdout
    if json_output:
        # Native provenance and per-process elapsed/printed-byte accounting
        # intentionally differ. Compare source bytes, every match span and path.
        rows = [json.loads(line) for line in value.splitlines()]
        files = {
            row["file_id"]: row["data"]["path"]
            for row in rows
            if row["type"] == "begin" and "file_id" in row
        }
        matches = [
            dict(row["data"], path=files[row["file_id"]])
            if "file_id" in row
            else row["data"]
            for row in rows
            if row["type"] == "match"
        ]
        value = json.dumps(matches, sort_keys=True, separators=(",", ":")).encode()
    elif unordered:
        value = b"\n".join(sorted(value.splitlines()))
    return {"sha256": hashlib.sha256(value).hexdigest(), "exit": result.returncode}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--repository", type=Path)
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Measure compact candidate JSON against standard baseline/oracle JSON after reconstruction",
    )
    parser.add_argument("--rounds", type=int, default=11)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("rounds must be positive")
    binaries = {
        "antfly-grep": str(args.binary.resolve()),
        "baseline": str(args.baseline.resolve()),
    }
    if shutil.which("rg"):
        binaries["ripgrep"] = shutil.which("rg")
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
    }
    report = {
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "cpu_count": os.cpu_count(),
        "rounds": args.rounds,
        "limitations": "Shared host, warm filesystem; CPU/RSS are command measurements. JSON checks compare all match data, excluding native provenance and timing. No provider contacted.",
        "binaries": {
            n: {
                "path": b,
                "sha256": hashlib.sha256(Path(b).read_bytes()).hexdigest(),
                "bytes": Path(b).stat().st_size,
            }
            for n, b in binaries.items()
        },
        "cases": {},
    }
    supervisor = linux_time()
    with tempfile.TemporaryDirectory(prefix="grep-output-bench-") as directory:
        root = Path(directory)
        env.update(HOME=str(root), TERM="xterm-256color")
        source = b"k " * 262144 + b"\n"
        (root / "dense").write_bytes(source)
        report["source"] = {
            "generator": "b'k ' * 262144 + b'\\n'",
            "bytes": len(source),
            "sha256": hashlib.sha256(source).hexdigest(),
        }
        cases = {
            "dense-json": (["--json", "k", "dense"], root),
            "dense-color": (["--color=always", "k", "dense"], root),
            "dense-only-matching": (["--color=never", "-o", "k", "dense"], root),
            "dense-regex-color": (["--color=always", "[k]+", "dense"], root),
        }
        if args.repository:
            repo = args.repository.resolve()
            cases["types-all"] = (["--files", "--hidden", "--type=all", "."], repo)
            cases["exclusion"] = (["--files", "--exclude=*.never", "."], repo)
        for case, (flags, cwd) in cases.items():
            commands = {
                n: [
                    b,
                    *(
                        ["--files", "-g", "!*.never", "."]
                        if case == "exclusion" and n == "ripgrep"
                        else ["--json-compact", *flags[1:]]
                        if args.compact and case == "dense-json" and n == "antfly-grep"
                        else flags
                    ),
                ]
                for n, b in binaries.items()
            }
            expected = None
            entries = {}
            for name, argv in commands.items():
                output = signature(
                    argv,
                    cwd,
                    env,
                    json_output=case == "dense-json",
                    unordered=case in ("types-all", "exclusion"),
                )
                if expected is not None and expected != output:
                    raise RuntimeError(f"{case}: mismatched output for {name}")
                expected = output
                output_bytes = len(
                    subprocess.run(
                        argv, cwd=cwd, env=env, capture_output=True, check=False
                    ).stdout
                )
                entries[name] = {
                    "output": output,
                    "output_bytes": output_bytes,
                    "runs": [],
                }
                measure(argv, cwd, env)
            order = list(commands)
            random.Random(case).shuffle(order)
            for trial in range(args.rounds):
                for name in order if trial % 2 == 0 else reversed(order):
                    entries[name]["runs"].append(measure(commands[name], cwd, env))
            for name, entry in entries.items():
                entry["median_seconds"] = statistics.median(
                    run["seconds"] for run in entry["runs"]
                )
                entry["median_cpu_seconds"] = statistics.median(
                    run.get("user_seconds", 0) + run.get("system_seconds", 0)
                    for run in entry["runs"]
                )
                rss = [
                    run["peak_rss_kib"]
                    for run in entry["runs"]
                    if run["peak_rss_kib"] is not None
                ]
                if supervisor:
                    rss = [
                        target_rss(commands[name], cwd, env, supervisor)
                        for _ in range(3)
                    ]
                entry["rss_runs_kib"] = rss
                entry["median_rss_kib"] = statistics.median(rss) if rss else None
                entry["first_result_seconds"] = first_result(commands[name], cwd, env)
            report["cases"][case] = entries
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
