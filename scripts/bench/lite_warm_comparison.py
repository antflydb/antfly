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

"""Compare warm writes on independent clones of a closed large-graph fixture.

Create the fixture with lite_large_graph.py first. Libraries must use the same
optimization profile. This measures the complete batch/SQL calls, requires zero
warm proof walks, and retains integrity/reopen checks from that benchmark.
"""

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run(args):
    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    fixture = Path(args.fixture).resolve()
    libraries = {
        name: Path(getattr(args, name)).resolve() for name in ("before", "after")
    }
    report = {
        "status": "running",
        "system": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "before_revision": args.before_revision,
        "library_sha256": {name: sha256(path) for name, path in libraries.items()},
        "fixture_sha256": sha256(fixture),
        "fixture_bytes": fixture.stat().st_size,
        "records": args.records,
        "iterations_per_operation": args.iterations,
        "candidate_source_sha256": {
            name: sha256(ROOT / name)
            for name in (
                "zig/pkg/antfly-embedded/src/storage/lite/native.zig",
                "zig/pkg/antfly-embedded/src/storage/lite/allocator_v4.zig",
                "zig/pkg/antfly-embedded/src/capi/lite_connection.zig",
                "scripts/bench/lite_large_graph.py",
                "scripts/bench/lite_warm_comparison.py",
            )
        },
        "runs": [],
    }
    output = directory / "summary.json"
    for repeat in range(args.repeats):
        for name in ("before", "after") if repeat % 2 == 0 else ("after", "before"):
            trial = directory / f"{name}-{repeat}"
            trial.mkdir()
            destination = trial / "large.aflite"
            copy_flags = (
                ["-c"]
                if sys.platform == "darwin"
                else ["--reflink=auto", "--sparse=always"]
            )
            copy_program = "/bin/cp" if sys.platform == "darwin" else "cp"
            subprocess.run(
                [copy_program, *copy_flags, str(fixture), str(destination)], check=True
            )
            load_before = os.getloadavg()
            with (trial / "run.log").open("w") as log:
                result = subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "scripts/bench/lite_large_graph.py"),
                        "--library",
                        str(libraries[name]),
                        "--directory",
                        str(trial),
                        "--reuse-fixture",
                        "--certified-handoff",
                        "--records",
                        str(args.records),
                        "--operations",
                        "read",
                        "--warm-iterations",
                        str(args.iterations),
                    ],
                    check=False,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=args.watchdog,
                    env={**os.environ, "ANTFLY_LITE_REQUIRE_LIBRARY": "1"},
                )
            rows = []
            for line in (trial / "run.log").read_text().splitlines():
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
            samples = [row for row in rows if row.get("kind") == "warm_operation"]
            passed = (
                result.returncode == 0
                and {row["operation"] for row in samples} == {"batch", "sql"}
                and len(samples) == 2
                and all(row["proof_walks"] == 0 for row in samples)
                and any(
                    row.get("kind") == "integrity" and row.get("status") == "ok"
                    for row in rows
                )
                and any(
                    row.get("kind") == "complete" and row.get("status") == "ok"
                    for row in rows
                )
            )
            report["runs"].append(
                {
                    "library": name,
                    "repeat": repeat,
                    "exit_code": result.returncode,
                    "passed": passed,
                    "load_before": load_before,
                    "load_after": os.getloadavg(),
                    "samples": samples,
                }
            )
            report["status"] = "running" if passed else "failed"
            output.write_text(json.dumps(report, indent=2) + "\n")
            if not passed:
                raise RuntimeError(f"comparison failed: {trial / 'run.log'}")
    report["medians"] = {}
    for operation in ("batch", "sql"):
        medians = {}
        for name in libraries:
            samples = [
                sample
                for trial in report["runs"]
                if trial["library"] == name
                for sample in trial["samples"]
                if sample["operation"] == operation
            ]
            medians[name] = {
                metric: statistics.median(sample[metric] for sample in samples)
                for metric in ("p50_ns", "p95_ns", "calls_per_second", "cpu_seconds")
            }
        medians["p50_after_over_before"] = (
            medians["after"]["p50_ns"] / medians["before"]["p50_ns"]
        )
        report["medians"][operation] = medians
    report["status"] = "passed"
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["medians"], indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--records", type=int, default=12288)
    parser.add_argument("--iterations", type=int, default=512)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--watchdog", type=float, default=300)
    arguments = parser.parse_args()
    if (
        min(
            arguments.records,
            arguments.iterations,
            arguments.repeats,
            arguments.watchdog,
        )
        <= 0
    ):
        parser.error("records, iterations, repeats and watchdog must be positive")
    run(arguments)
