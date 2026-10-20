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

"""Run serial native base/head comparisons over identical Parquet layouts.

Uses the existing HN correctness/latency harness. Every binary, mode, layout and
cache state gets a fresh private state directory. Results distinguish empty
serving caches from immediate-after-readiness searches; OS caches are retained.
"""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def matrix(layouts, modes, cache_states, cycles):
    for cycle, layout, mode, cache in itertools.product(
        range(cycles), layouts, modes, cache_states
    ):
        # Alternate binary order instead of always giving the candidate the
        # warmer filesystem. The source layout remains identical within a pair.
        for label in (("base", "head") if cycle % 2 == 0 else ("head", "base")):
            yield dict(cycle=cycle, files=layout, mode=mode, cache=cache, binary=label)


def command(poc, binary, fixture, state, case, args, rows):
    result = [sys.executable, str(poc), "--binary", str(binary),
              "--filesystem-source", str(fixture), "--state", str(state),
              "--expected-rows", str(rows), "--port", str(args.port),
              "--repeats", str(args.repeats), "--concurrency", "1",
              "--build-timeout", str(args.build_timeout),
              "--process-memory-budget-mb", str(args.process_memory_budget_mb),
              "--query-term", args.term, "--workload-terms", args.term,
              "--store-source", "--immediate-first-query"]
    if case["cache"] == "empty":
        result += ["--cold-cache", "--disable-navigation-warming"]
    if case["mode"] == "plain":
        result += ["--no-highlights"]
    if case["mode"] == "composed":
        result += ["--qualify-composed-sources", "--benchmark-composed",
                   "--composed-current-rows", str(min(1000, rows))]
    return result


def inventory(root):
    objects = [p for p in root.rglob("objects/**/*") if p.is_file()] if root.exists() else []
    return {"objects": len(objects), "bytes": sum(p.stat().st_size for p in objects),
            "live_bytes": None, "orphan_bytes": None,
            "scope": "Physical post-run artifact footprint, including retained and attempt-private objects. This is not a live-byte or post-GC claim."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-binary", type=Path, required=True)
    parser.add_argument("--head-binary", type=Path, required=True)
    parser.add_argument("--base-revision", required=True)
    parser.add_argument("--head-revision", required=True)
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--layouts", nargs="+", type=int, default=[1, 1024])
    parser.add_argument("--modes", nargs="+", choices=["plain", "highlights", "composed"], default=["plain", "highlights", "composed"])
    parser.add_argument("--cache-states", nargs="+", choices=["empty", "readiness"], default=["empty", "readiness"])
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--process-memory-budget-mb", type=int, default=4096)
    parser.add_argument("--cpu-cores", type=int, nargs="+", help="Pin this process and its children on Linux; omit to report unpinned execution")
    parser.add_argument("--term", default="database")
    parser.add_argument("--port", type=int, default=8877)
    parser.add_argument("--build-timeout", type=int, default=1800)
    args = parser.parse_args()
    if min(args.cycles, args.repeats, args.process_memory_budget_mb, *args.layouts) < 1 or max(args.layouts) > 4096 or args.cycles > 10 or args.repeats > 100:
        parser.error("Counts must be positive; at most 4096 files, 10 cycles and 100 samples")
    if args.cpu_cores:
        if not hasattr(os, "sched_setaffinity"):
            parser.error("CPU affinity is supported on Linux; native macOS runs are explicitly unpinned")
        os.sched_setaffinity(0, args.cpu_cores)
    binaries = {"base": args.base_binary.resolve(strict=True), "head": args.head_binary.resolve(strict=True)}
    import pyarrow.parquet as pq
    table = pq.read_table(args.parquet)
    if table.num_rows < max(2, *args.layouts):
        parser.error("Source must have at least two rows and one row per requested file")
    required = {"hn_id", "title", "item_type", "author", "points", "created_at", "body"}
    if not required.issubset(table.column_names):
        parser.error("Source must have the HN benchmark columns")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    fixtures = {}
    for files in sorted(set(args.layouts)):
        root = args.output / "fixtures" / str(files)
        archive = root / "archive"
        archive.mkdir(parents=True)
        for number in range(files):
            path = archive / f"part-{number:05d}.parquet"
            if files == 1:
                shutil.copyfile(args.parquet, path)
            else:
                begin = table.num_rows * number // files
                end = table.num_rows * (number + 1) // files
                pq.write_table(table.slice(begin, end - begin), path, compression="snappy")
        fixtures[files] = root
    report = {"complete": False, "source_sha256": digest(args.parquet), "rows": table.num_rows,
              "binaries": {label: {"sha256": digest(binary), "revision": getattr(args, label + "_revision")} for label, binary in binaries.items()},
              "fixtures": {str(files): {p.name: digest(p) for p in (root / "archive").glob("*.parquet")} for files, root in fixtures.items()},
              "resources": {"process_memory_budget_mb": args.process_memory_budget_mb,
                            "cpu_affinity": args.cpu_cores, "platform": platform.platform()},
              "cache_scope": "Fresh state per case; OS file caches retained. Empty clears only that state's serving cache after publication and disables navigation warming. Readiness keeps construction caches and starts the first archive query without diagnostic refresh sleeps.",
              "cases": []}
    output = args.output / "comparison.json"
    output.write_text(json.dumps(report, indent=2) + "\n")
    poc = Path(__file__).with_name("poc.py")
    for case in matrix(args.layouts, args.modes, args.cache_states, args.cycles):
        state = args.output / "runs" / ("{cycle}-{files}-{mode}-{cache}-{binary}".format(**case))
        state.mkdir(parents=True)
        with (state / "harness.log").open("w") as log:
            completed = subprocess.run(command(poc, binaries[case["binary"]], fixtures[case["files"]], state, case, args, table.num_rows), stdout=log, stderr=subprocess.STDOUT, check=False)
        result = {**case, "exit_code": completed.returncode, "state": str(state.relative_to(args.output)), "artifact_footprint": inventory(state / "data" / "artifacts")}
        if (state / "report.json").exists():
            measured = json.loads((state / "report.json").read_text())
            result["measurements"] = {key: measured.get(key) for key in ("ready_seconds", "registration_seconds", "first_search_ms", "warm_samples_ms", "engine_child_peak_rss_bytes", "build_peak_rss_bytes", "io_profiles", "composed_benchmarks", "verified")}
        report["cases"].append(result)
        output.write_text(json.dumps(report, indent=2) + "\n")
        if completed.returncode:
            raise SystemExit("Qualification failed; partial observations retained in " + str(output))
    report["complete"] = True
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
