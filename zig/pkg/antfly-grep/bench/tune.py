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

"""Measure file/provider concurrency and gathering independently on a fixed workload.

Example: tune.py --workers 1,4,8 --providers 1,2,4 --output tune.json --
  ./antfly-grep --ask retry --model fixture --json --no-cache ./repository
Calls only the endpoint explicitly selected by the supplied command/config.
"""

import argparse
import hashlib
import itertools
import json
import os
import platform
import statistics
import subprocess
import time
from pathlib import Path

from compare import first_result, linux_time, measure, target_rss, timing_quality


def counts(value):
    values = list(dict.fromkeys(int(v) for v in value.split(",")))
    if not values or any(v < 1 or v > 256 for v in values):
        raise argparse.ArgumentTypeError("counts must be 1..256")
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=counts, default=[1, 4, 8])
    parser.add_argument("--providers", type=counts, default=[1, 2, 4])
    parser.add_argument("--batch-sizes", type=counts)
    parser.add_argument("--wait-ms", default="1")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--cwd", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.rounds < 3:
        parser.error("a command and at least three rounds are required")
    try:
        waits = list(dict.fromkeys(int(v) for v in args.wait_ms.split(",")))
    except ValueError:
        parser.error("wait-ms must be 0..1000")
    if not waits or any(v < 0 or v > 1000 for v in waits):
        parser.error("wait-ms must be 0..1000")
    if args.batch_sizes and any(v > 128 for v in args.batch_sizes):
        parser.error("batch-sizes must be 1..128")
    binary = Path(command[0]).resolve()
    command[0] = str(binary)
    env = dict(os.environ)
    recorded_command = []
    secret_next = False
    for arg in command:
        recorded_command.append(
            "<redacted>"
            if secret_next
            else "--token=<redacted>"
            if arg.startswith("--token=")
            else arg
        )
        secret_next = arg == "--token"
    report = {
        "host": {"platform": platform.platform(), "cpu_count": os.cpu_count()},
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "binary_version": subprocess.check_output(
            [str(binary), "--version"], text=True
        ).strip(),
        "recorded_at_unix": time.time(),
        "command": recorded_command,
        "cwd": str(args.cwd.resolve()),
        "rounds": args.rounds,
        "limitations": "Uses the supplied workload/cache/storage/provider; no automatic cache eviction or relevance qualification. First-output latency is a separate run; ranked output can require scanning all inputs.",
        "cases": [],
    }
    expected = None
    supervisor = linux_time()
    commands = []
    for workers, providers, wait, batch in itertools.product(
        args.workers, args.providers, waits, args.batch_sizes or [None]
    ):
        argv = [
            *command,
            f"--threads={workers}",
            f"--decision-concurrency={providers}",
            f"--decision-batch-wait-ms={wait}",
            *([f"--batch-size={batch}"] if batch is not None else []),
        ]
        result = subprocess.run(
            argv, cwd=args.cwd, env=env, capture_output=True, check=False
        )
        if result.returncode not in (0, 1):
            raise RuntimeError(result.stderr.decode(errors="replace"))
        rows = [json.loads(row) for row in result.stdout.splitlines()]
        if not rows or rows[-1].get("type") != "summary":
            raise RuntimeError("command must emit JSON Lines with a final summary")
        signature = sorted(
            json.dumps(
                {
                    k: row.get(k)
                    for k in (
                        "type",
                        "data",
                        "probability",
                        "scope",
                        "line_number",
                        "file_id",
                        "path",
                        "rows",
                        "regions",
                        "declarations",
                        "calls",
                        "relevance",
                    )
                },
                sort_keys=True,
            )
            for row in rows
            if row.get("type") in ("match", "file")
        )
        digest = hashlib.sha256("\n".join(signature).encode()).hexdigest()
        if expected is not None and digest != expected:
            raise RuntimeError("result evidence changed across tuning cases")
        expected = digest
        commands.append(argv)
        report["cases"].append(
            {
                "workers": workers,
                "provider_lanes": providers,
                "wait_ms": wait,
                "batch_size": batch,
                "evidence_sha256": digest,
                "qualification_summary": rows[-1],
                "runs": [],
            }
        )
    # Alternate case order to avoid attributing gradual host drift to a knob.
    for trial in range(args.rounds):
        order = (
            range(len(commands)) if trial % 2 == 0 else reversed(range(len(commands)))
        )
        for i in order:
            report["cases"][i]["runs"].append(measure(commands[i], args.cwd, env))
    for case, argv in zip(report["cases"], commands):
        samples = case["runs"]
        case["median_seconds"] = statistics.median(s["seconds"] for s in samples)
        case["timing_quality"] = timing_quality([s["seconds"] for s in samples])
        case["first_output_runs"] = [
            first_result(argv, args.cwd, env) for _ in range(args.rounds)
        ]
        observed = [value for value in case["first_output_runs"] if value is not None]
        case["first_output_seconds"] = statistics.median(observed) if observed else None
        if supervisor:
            case["target_peak_rss_kib"] = target_rss(argv, args.cwd, env, supervisor)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
