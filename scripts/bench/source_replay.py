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

"""Compare qualified JSON source replay with the legacy publication path.

Run an optimized source-replay-bench-build executable. These fixtures exercise
real collectors, mapper preparation, publication and query delivery. They do not
qualify source-only field reuse, distributed transfer or automatic rollout.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import random
import re
import resource
import statistics
import subprocess
import time

from source_family import fingerprint, optional_text

PHASES = ("collect_ns", "prepare_ns", "publish_ns", "read_ns")


def parse_metrics(text: str, documents: int, family: bool, limit: int | None = None, forced_snapshot: bool = False) -> dict[str, int]:
    records = []
    for line in text.splitlines():
        if "SOURCE_REPLAY " not in line:
            continue
        record = {}
        for token in line.split("SOURCE_REPLAY ", 1)[1].split():
            match = re.fullmatch(r"(\w+)=(\d+)", token)
            if not match or match[1] in record:
                raise RuntimeError("malformed or duplicate native replay metric")
            record[match[1]] = int(match[2])
        records.append(record)
    if len(records) != 1:
        raise RuntimeError("expected exactly one native replay metrics record")
    result = records[0]
    required = (*PHASES, "documents", "family", "verified", "production_allocator", "optimize_mode")
    if any(key not in result for key in required):
        raise RuntimeError("incomplete replay metrics; rebuild the benchmark")
    expected_hits = documents if limit is None else limit
    if (result["documents"], result["family"], result["verified"], result["production_allocator"]) != (documents, int(family), expected_hits, 1):
        raise RuntimeError("replay fixture failed workload or correctness contract")
    if result.get("limit", documents) != expected_hits or result.get("forced_snapshot", 0) != int(forced_snapshot):
        raise RuntimeError("fixture did not honor the requested limit or reader policy")
    if "point_eligible" in result:
        if result["point_eligible"] not in (0, 1) or (result["point_eligible"] and (not family or expected_hits > 64)):
            raise RuntimeError("invalid point-reader admission metric")
    if "probe_reads" in result:
        expected_probe = int(family and expected_hits <= 64 and not forced_snapshot and result.get("point_eligible", 1))
        if result["probe_reads"] != expected_probe:
            raise RuntimeError("native fixture did not use its bounded reader policy")
    if result["optimize_mode"] not in range(4):
        raise RuntimeError("unknown optimization mode")
    result["write_ns"] = sum(result[phase] for phase in PHASES[:3])
    return result


def paired_comparisons(runs: list[dict], labels: list[str]) -> list[dict]:
    """Compare matched trials; report observed variation without a speed claim."""
    by_key = {}
    for row in runs:
        key = (row["binary"], row["documents"], row["family"], row["trial"])
        if key in by_key:
            raise RuntimeError("duplicate paired replay trial")
        by_key[key] = row
    workloads = sorted({(row["documents"], row["family"]) for row in runs})
    comparisons = []
    for label in labels:
        if label == "current":
            continue
        for documents, family in workloads:
            trials = sorted(key[3] for key in by_key if key[:3] == ("current", documents, family))
            other_trials = sorted(key[3] for key in by_key if key[:3] == (label, documents, family))
            if not trials or trials != other_trials:
                raise RuntimeError("incomplete paired replay trials")
            for phase in (*PHASES, "write_ns", "cpu_seconds", "peak_rss_bytes"):
                pairs = [(by_key[("current", documents, family, trial)][phase],
                          by_key[(label, documents, family, trial)][phase]) for trial in trials]
                ratios = [current / other for current, other in pairs if other > 0]
                comparisons.append({"reference": label, "documents": documents, "family": family,
                    "metric": phase, "pairs": len(pairs), "ratio_pairs": len(ratios),
                    "current_wins": sum(current < other for current, other in pairs),
                    "median_delta": statistics.median(current - other for current, other in pairs),
                    "median_ratio": statistics.median(ratios) if ratios else None,
                    "min_ratio": min(ratios) if ratios else None,
                    "max_ratio": max(ratios) if ratios else None})
    return comparisons


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--baseline-binary", type=Path, help="Interleave an earlier executable under the same workloads")
    parser.add_argument("--compare-snapshot", action="store_true", help="Also run the current fixture with snapshot reads forced")
    parser.add_argument("--limit", type=int, help="Selected result count; defaults to every document")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--documents", nargs="+", type=int, default=[65, 131, 1024])
    parser.add_argument("--trials", type=int, default=7)
    parser.add_argument("--seed", type=int, default=137)
    parser.add_argument("--allow-nonrelease", action="store_true")
    args = parser.parse_args()
    if args.trials < 1 or len(set(args.documents)) != len(args.documents) or any(n < 1 or n > 4099 for n in args.documents):
        parser.error("positive trials and distinct document counts 1..4099 required")
    if args.limit is not None and (args.limit < 1 or args.limit > min(args.documents)):
        parser.error("limit must be 1..the smallest document count")
    binary = args.binary.resolve(strict=True)
    root = Path(__file__).resolve().parents[2]
    args.output = args.output.resolve()
    if not args.output.is_relative_to(root / "output"):
        parser.error("reports must live under this worktree's ignored output/ directory")
    args.output.mkdir(parents=True, exist_ok=True)
    if (args.output / "report.json").exists():
        parser.error("output already contains a report; choose a fresh directory")
    binary_hash = fingerprint(binary)
    binaries = {"current": {"path": str(binary), "sha256": binary_hash}}
    if args.compare_snapshot:
        binaries["snapshot"] = {"path": str(binary), "sha256": binary_hash, "force_snapshot": True}
    if args.baseline_binary:
        baseline = args.baseline_binary.resolve(strict=True)
        binaries["baseline"] = {"path": str(baseline), "sha256": fingerprint(baseline)}
    report = {
        "status": "running", "binary": str(binary), "sha256": binary_hash,
        "binaries": binaries,
        "host": platform.platform(), "python": platform.python_version(),
        "cpu_count": os.cpu_count(), "seed": args.seed,
        "cpu_max": optional_text("/sys/fs/cgroup/cpu.max"),
        "memory_max": optional_text("/sys/fs/cgroup/memory.max"),
        "scope": "qualified JSON replay; queries include all stored source bytes",
        "rss_scope": "whole fixture including inputs, authority changes, cleanup and inherited parent RSS floor",
        "configuration": {"documents": args.documents, "trials": args.trials, "limit": args.limit},
        "cache": "fresh stores; OS cache uncontrolled", "runs": [],
    }
    report_path = args.output / "report.json"

    def save() -> None:
        temporary = args.output / "report.pending"
        temporary.write_text(json.dumps(report, indent=2) + "\n")
        temporary.replace(report_path)

    save()
    schedule = [(trial, documents, family, label) for trial in range(args.trials)
                for documents in args.documents for family in (False, True) for label in binaries]
    random.Random(args.seed).shuffle(schedule)
    try:
        for trial, documents, family, label in schedule:
            selected = binaries[label]
            selected_path = Path(selected["path"])
            if fingerprint(selected_path) != selected["sha256"]:
                raise RuntimeError("benchmark binary changed during measurement")
            env = os.environ.copy()
            env.pop("ANTFLY_BENCH_SOURCE_REPLAY_LEGACY", None)
            env.pop("ANTFLY_BENCH_SOURCE_LIMIT", None)
            env.pop("ANTFLY_BENCH_SOURCE_FORCE_SNAPSHOT", None)
            if args.limit is not None:
                env["ANTFLY_BENCH_SOURCE_LIMIT"] = str(args.limit)
            if selected.get("force_snapshot", False):
                env["ANTFLY_BENCH_SOURCE_FORCE_SNAPSHOT"] = "1"
            env["ANTFLY_BENCH_SOURCE_REPLAY"] = "1"
            env["ANTFLY_BENCH_SOURCE_DOCUMENTS"] = str(documents)
            if not family:
                env["ANTFLY_BENCH_SOURCE_REPLAY_LEGACY"] = "1"
            log = args.output / f"{label}-trial-{trial}-docs-{documents}-family-{int(family)}.log"
            parent_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            load_before = os.getloadavg()
            started = time.perf_counter_ns()
            with log.open("w") as stream:
                child = subprocess.Popen([str(selected_path), "--suite-filter", "ordinary replay"],
                                         env=env, stdout=stream, stderr=subprocess.STDOUT)
                try:
                    _, status, usage = os.wait4(child.pid, 0)
                    child.returncode = os.waitstatus_to_exitcode(status)
                except BaseException:
                    child.kill()
                    child.wait()
                    raise
            elapsed = time.perf_counter_ns() - started
            if child.returncode:
                raise RuntimeError(f"native fixture failed; see {log}")
            metrics = parse_metrics(log.read_text(), documents, family, args.limit, selected.get("force_snapshot", False))
            if not args.allow_nonrelease and metrics["optimize_mode"] != 2:
                raise RuntimeError("ReleaseFast required for performance results")
            rss_scale = 1 if platform.system() == "Darwin" else 1024
            report["runs"].append({"trial": trial, "binary": label, **metrics, "elapsed_ns": elapsed,
                "cpu_seconds": usage.ru_utime + usage.ru_stime,
                "peak_rss_bytes": usage.ru_maxrss * rss_scale,
                "parent_peak_rss_bytes": parent_rss * rss_scale,
                "load_before": load_before, "log": log.name})
            save()
        for selected in binaries.values():
            if fingerprint(Path(selected["path"])) != selected["sha256"]:
                raise RuntimeError("benchmark binary changed during measurement")
        report["medians"] = [
            {"binary": label, "documents": documents, "family": int(family), **{
                phase: statistics.median(row[phase] for row in report["runs"]
                    if row["documents"] == documents and row["family"] == int(family) and row["binary"] == label)
                for phase in (*PHASES, "write_ns", "cpu_seconds", "peak_rss_bytes", "parent_peak_rss_bytes")}}
            for documents in args.documents for family in (False, True) for label in binaries]
        report["comparisons"] = paired_comparisons(report["runs"], list(binaries))
        report["status"] = "complete"
    except BaseException as error:
        report["status"] = "interrupted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "failed"
        report["error"] = str(error)
        raise
    finally:
        save()
    print(report_path)


if __name__ == "__main__":
    main()
