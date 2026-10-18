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

"""Cross-file request batching with a bounded-concurrency local HTTP fixture."""

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from compare import measure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--files", type=int, default=128)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--provider-lanes", type=int, default=2)
    parser.add_argument("--candidate-concurrency", type=int, default=0)
    parser.add_argument("--delay-ms", type=int, default=20)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if (
        min(args.rounds, args.files, args.threads, args.provider_lanes) < 1
        or args.delay_ms < 0
        or not 0 <= args.candidate_concurrency <= 256
    ):
        parser.error("positive counts and nonnegative delay required")
    binaries = {
        "antfly-grep": str(args.binary.resolve()),
        "baseline": str(args.baseline.resolve()),
    }
    tests = Path(__file__).resolve().parents[1] / "tests/test_cli.py"
    spec = importlib.util.spec_from_file_location("grep_fixtures", tests)
    module = importlib.util.module_from_spec(spec)
    old_argv = sys.argv
    sys.argv = [str(tests), binaries["antfly-grep"], "unused"]
    spec.loader.exec_module(module)
    sys.argv = old_argv
    cache = Path(__file__).resolve().parents[3] / ".zig-cache"
    cache.mkdir(exist_ok=True)
    tempfile.tempdir = str(cache)
    module.CliTests.setUpClass()
    case = module.CliTests("test_cross_file_microbatches_route_results_and_reuse_cache")
    case.setUp()
    report = {
        "fixture_only": True,
        "files": args.files,
        "threads": args.threads,
        "provider_lanes": args.provider_lanes,
        "candidate_concurrency": args.candidate_concurrency,
        "fixture_delay_ms": args.delay_ms,
        "host": {"platform": platform.platform(), "cpu_count": os.cpu_count()},
        "fixture_source_sha256": hashlib.sha256(tests.read_bytes()).hexdigest(),
        "limitations": "Synthetic constant-score fixture with bounded provider lanes; warm filesystem, no cache, no model relevance or billed-token savings established",
        "binaries": {},
        "cases": {},
    }
    try:
        case.server.provider_lanes = threading.Semaphore(args.provider_lanes)
        case.server.delay = args.delay_ms / 1000
        for i in range(args.files):
            case.write(f"src/f{i:04}.txt", "STOP HIGH\n" if i % 2 else "STOP LOW\n")
        flags = [
            *case.decision_args(),
            "--json",
            "--no-cache",
            "--no-refine",
            "--max-files=0",
            "--max-requests=unlimited",
            "--batch-size=8",
            f"--threads={args.threads}",
            "src",
        ]
        commands = {
            name: [
                binary,
                *flags,
                *(
                    [f"--decision-concurrency={args.candidate_concurrency}"]
                    if name == "antfly-grep" and args.candidate_concurrency
                    else []
                ),
            ]
            for name, binary in binaries.items()
        }
        expected = None
        for name, binary in binaries.items():
            report["binaries"][name] = {
                "sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
                "version": subprocess.check_output([binary, "--version"])
                .decode()
                .strip(),
            }
            result = subprocess.run(
                commands[name],
                cwd=case.root,
                env=case.env,
                capture_output=True,
                check=True,
            )
            rows = [json.loads(row) for row in result.stdout.splitlines()]
            signature = [
                (
                    row["data"]["path"],
                    row["data"]["lines"],
                    row["probability"],
                    row["scope"],
                )
                for row in rows
                if row["type"] == "match"
            ]
            if expected is not None and expected != signature:
                raise RuntimeError("non-equivalent evidence")
            expected = signature
            report["cases"][name] = {"runs": [], "qualification_summary": rows[-1]}
        for trial in range(args.rounds):
            for name in binaries if trial % 2 == 0 else reversed(binaries):
                case.server.requests.clear()
                sample = measure(commands[name], case.root, case.env)
                sample["requests"] = len(case.server.requests)
                sample["request_bytes"] = sum(
                    len(json.dumps(body, separators=(",", ":")).encode())
                    for _, _, body in case.server.requests
                )
                sample["inputs"] = sum(
                    len(body["inputs"]) for _, _, body in case.server.requests
                )
                if sample["inputs"] != args.files:
                    raise RuntimeError("lost or duplicated targets")
                report["cases"][name]["runs"].append(sample)
        for entry in report["cases"].values():
            entry["median_seconds"] = statistics.median(
                r["seconds"] for r in entry["runs"]
            )
            entry["median_requests"] = statistics.median(
                r["requests"] for r in entry["runs"]
            )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    finally:
        case.tearDown()
        module.CliTests.tearDownClass()


if __name__ == "__main__":
    main()
