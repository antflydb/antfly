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

"""Compare logical-source readers with pinned artifacts and raw HTTP trials.

Run on native hosts or inside a constrained container; never label a process
restart as a cold-storage measurement. Baseline/candidate order alternates by
round. Existing evidence is never overwritten: use a fresh output directory.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--grep", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument(
        "--baseline-legacy-value-wire",
        action="store_true",
        help="adapt benchmark requests/responses only for a frozen baseline predating execution.value_verification",
    )
    parser.add_argument("--clients", default="1,4")
    parser.add_argument(
        "--suite",
        choices=("scalable", "concurrency", "ingestion", "both", "all"),
        default="both",
    )
    parser.add_argument(
        "--controls",
        action="store_true",
        help="also measure candidate full reads, scalar and fixed batch sizes",
    )
    parser.add_argument(
        "--advise-file-cache",
        action="store_true",
        help="Linux only: advise eviction of fixture files before process restarts",
    )
    args = parser.parse_args()
    if args.advise_file_cache and platform.system() != "Linux":
        parser.error("--advise-file-cache requires Linux")
    if args.advise_file_cache and args.suite in ("scalable", "ingestion"):
        parser.error("--advise-file-cache requires the concurrency or both suites")
    assert args.rounds >= 1
    root = Path(__file__).resolve().parents[4]
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {
        "baseline": args.baseline.resolve(),
        "candidate": args.candidate.resolve(),
    }
    report = {
        "protocol": 1,
        "host": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "file_cache_advice_requested": args.advise_file_cache,
        "baseline_legacy_value_wire": args.baseline_legacy_value_wire,
        "driver_sha256": digest(__file__),
        "fixture_sha256": digest(root / "zig/e2e/antfly/test_grep_indexed.py"),
        "artifacts": {
            k: {"path": str(v), "sha256": digest(v)} for k, v in binaries.items()
        },
        "scope": "Native ISA as executed; optional fixture-file cache advice is recorded per restart and cannot guarantee cold storage. CPU/RSS includes background work.",
        "experiments": [],
    }
    # Clear inherited controls, so one experiment cannot contaminate the next.
    base_env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("ANTFLY_BENCH_VALUE_SOURCE_")
        and not k.startswith("ANTFLY_LOGICAL_")
    }
    cases = [("baseline", {}), ("candidate", {})]
    if args.controls:
        cases += [
            ("full_read", {"ANTFLY_BENCH_VALUE_SOURCE_FULL_READ": "1"}),
            ("scalar", {"ANTFLY_BENCH_VALUE_SOURCE_SCALAR": "1"}),
            *[
                (f"rows{n}", {"ANTFLY_BENCH_VALUE_SOURCE_ROWS": str(n)})
                for n in (16, 32, 64)
            ],
        ]
    suites = (
        ("scalable", "concurrency", "ingestion")
        if args.suite == "all"
        else ("scalable", "concurrency")
        if args.suite == "both"
        else (args.suite,)
    )
    for round_number in range(args.rounds):
        for label, controls in (
            cases if round_number % 2 == 0 else list(reversed(cases))
        ):
            for suite in suites:
                name = f"r{round_number}-{label}-{suite}"
                destination = args.output / (name + ".json")
                control_key, test = {
                    "scalable": (
                        "ANTFLY_LOGICAL_SCALABLE_REPORT",
                        "logical_scalable_work_profile",
                    ),
                    "concurrency": (
                        "ANTFLY_LOGICAL_CONCURRENCY_REPORT",
                        "logical_source_concurrency_profile",
                    ),
                    "ingestion": (
                        "ANTFLY_LOGICAL_INGESTION_REPORT",
                        "logical_substring_ingestion_profile",
                    ),
                }[suite]
                binary = binaries["baseline" if label == "baseline" else "candidate"]
                env = {
                    **base_env,
                    **controls,
                    "ANTFLY_BIN": str(binary),
                    "ANTFLY_GREP_BIN": str(args.grep.resolve()),
                    control_key: str(destination),
                    "ANTFLY_LOGICAL_CLIENTS": args.clients,
                    "ANTFLY_BENCH_LEGACY_VALUE_WIRE": "1"
                    if args.baseline_legacy_value_wire and label == "baseline"
                    else "0",
                    "ANTFLY_LOGICAL_EVICT_FILE_CACHE": "1"
                    if args.advise_file_cache
                    else "0",
                }
                command = [
                    args.python,
                    "-m",
                    "pytest",
                    "-q",
                    "zig/e2e/antfly/test_grep_indexed.py",
                    "-k",
                    test,
                ]
                started = time.time()
                with (args.output / (name + ".log")).open("w") as log:
                    result = subprocess.run(
                        command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT
                    )
                entry = {
                    "name": name,
                    "controls": controls,
                    "command": command,
                    "started_unix": started,
                    "exit_code": result.returncode,
                }
                if destination.exists():
                    entry["raw"] = json.loads(destination.read_text())
                elif result.returncode == 0:
                    entry["exit_code"] = 3
                    entry["error"] = "fixture produced no benchmark report"
                report["experiments"].append(entry)
                (args.output / "report.json").write_text(
                    json.dumps(report, separators=(",", ":")) + "\n"
                )
                print(name, "exit", result.returncode, flush=True)
                if entry["exit_code"]:
                    raise SystemExit(entry["exit_code"])


if __name__ == "__main__":
    main()
