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

"""Qualify native payload layouts against a measured production baseline.

Build `zig build document-payload-test-build -Doptimize=ReleaseFast` (or
`document-patch-public-test-build` for --suite public), then run
this script inside the host/container that owns the binary. Public production
now includes automatic admission; use --baseline-binary for a pre-upgrade
comparison. Reports must live under ignored output/. Each layout runs in its own process, with wait4 CPU/RSS
accounting, randomized order, full WAL sync assertions, and paired confidence
intervals. `--gate` fails on confirmed regressions; it never declares universality.
"""
from __future__ import annotations

import argparse
import hashlib
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


def fingerprint(path: Path) -> str:
    # Whole-binary reads inflate the parent's high-water mark before exec and
    # can impose an inherited RSS floor on low-footprint child measurements.
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(256 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def interval(ratios: list[float]) -> tuple[float, float]:
    rng = random.Random(137)
    draws = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(2000))
    return draws[49], draws[1949]


def comparison_status(lower: float, upper: float, threshold: float) -> str:
    if lower > 1 + threshold:
        return "regression"
    if upper < 1:
        return "improved"
    if upper <= 1 + threshold:
        return "within_budget"
    return "inconclusive"


def parse_log(path: Path) -> list[dict]:
    rows = []
    seen = set()
    for line in path.read_text().splitlines():
        kind = next((k for k in ("DOCUMENT_MIXED", "DOCUMENT_COLD", "DOCUMENT_PUBLIC") if k + " " in line), None)
        if not kind:
            continue
        row = dict(re.findall(r"(\w+)=([^ ]+)", line.split(kind + " ", 1)[1]))
        row["kind"] = kind
        for key in ("payload", "changing", "reads_per_write", "elapsed_ns", "rounds", "wal_bytes", "table_bytes", "sync_ns", "patches", "wal_syncs", "compactions", "compaction_bytes", "row_count", "commit_syncs", "preflight", "transforms", "field_compilations_avoided"):
            if key in row:
                row[key] = int(row[key])
        row.setdefault("row_count", 1)
        if row.get("allocator") != os.environ.get("ANTFLY_BENCH_DOCUMENT_ALLOCATOR", "production"):
            raise RuntimeError("fixture did not honor allocator selection; rebuild the benchmark binary")
        if kind == "DOCUMENT_PUBLIC" and "transforms" not in row:
            raise RuntimeError("public fixture does not report transforms; rebuild the current benchmark binary")
        if kind == "DOCUMENT_MIXED":
            if row.get("commit_syncs", row.get("wal_syncs")) != row["rounds"] or row.get("wal_syncs", 0) < row["rounds"]:
                raise RuntimeError("fixture did not verify full WAL sync on each timed commit")
        if kind == "DOCUMENT_PUBLIC" and row.get("preflight", 0) != int("ANTFLY_BENCH_DOCUMENT_PREFLIGHT" in os.environ):
            raise RuntimeError("public fixture did not honor preflight selection")
        if kind == "DOCUMENT_PUBLIC" and row.get("wal_syncs", 0) < row["rounds"]:
            raise RuntimeError("public fixture did not verify primary WAL synchronization")
        if kind != "DOCUMENT_COLD":
            row["write_and_flush_ns"] = row["elapsed_ns"] + row["sync_ns"]
        identity = (kind, row["payload"], row["changing"], row["reads_per_write"], row["row_count"])
        if identity in seen:
            raise RuntimeError(f"duplicate workload record in {path}: {identity}")
        seen.add(identity)
        rows.append(row)
    if not rows:
        raise RuntimeError(f"no workload records in {path}")
    return rows


def run(binary: Path, output: Path, dataset: str, layout: str, trial: int, suite: str) -> dict:
    actual_layout = "production" if layout == "current-production" else layout
    env = dict(os.environ, ANTFLY_BENCH_DOCUMENT_MIXED="1", ANTFLY_BENCH_DOCUMENT_LAYOUT=actual_layout)
    env.pop("ANTFLY_BENCH_DOCUMENT_PUBLIC", None)
    if suite == "public":
        env["ANTFLY_BENCH_DOCUMENT_PUBLIC"] = "1"
    env.pop("ANTFLY_BENCH_DOCUMENT_ENTROPY", None)
    if dataset == "entropy":
        env["ANTFLY_BENCH_DOCUMENT_ENTROPY"] = "1"
    log = output / f"{dataset}-{layout}-{trial}.log"
    load_before = os.getloadavg()
    parent_peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.monotonic_ns()
    suite_filter = "document payload public transforms" if suite == "public" else "document payload mixed workload"
    with log.open("w") as stream:
        child = subprocess.Popen([str(binary), "--suite-filter", suite_filter], env=env, stdout=stream, stderr=subprocess.STDOUT)
        _, status, usage = os.wait4(child.pid, 0)
        child.returncode = os.waitstatus_to_exitcode(status)
    elapsed = time.monotonic_ns() - started
    if child.returncode:
        raise RuntimeError(f"fixture exited {child.returncode}; see {log}")
    rows = parse_log(log)
    for row in rows:
        if row.get("layout") != actual_layout or row["row_count"] != int(env.get("ANTFLY_BENCH_DOCUMENT_ROWS", "1")):
            raise RuntimeError("fixture did not honor layout/working-set selection")
        for name, field in (("BYTES", "payload"), ("READS", "reads_per_write"), ("ROUNDS", "rounds"), ("TRANSFORMS", "transforms")):
            selected = env.get("ANTFLY_BENCH_DOCUMENT_" + name)
            if selected is not None and field in row and row[field] != int(selected):
                raise RuntimeError(f"fixture did not honor {name}={selected}")
    checkpoints = []
    for line in log.read_text().splitlines():
        if "DOCUMENT_MEMORY " in line:
            fields = dict(re.findall(r"(\w+)=([^ ]+)", line.split("DOCUMENT_MEMORY ", 1)[1]))
            checkpoints.append({key: value if key == "phase" else int(value) for key, value in fields.items()})
    return dict(binary=str(binary), actual_layout=actual_layout, memory_checkpoints=checkpoints, dataset=dataset, layout=layout, trial=trial, elapsed_ns=elapsed, host_load_average_before=load_before, parent_peak_rss_before_exec=parent_peak_rss,
                user_seconds=usage.ru_utime, system_seconds=usage.ru_stime, peak_rss=usage.ru_maxrss,
                log=log.name, rows=rows)


def summarize(runs: list[dict], threshold: float) -> list[dict]:
    cases = {}
    for sample in runs:
        for row in sample["rows"]:
            key = (sample["dataset"], row["kind"], row["payload"], row["changing"], row["reads_per_write"], row["row_count"])
            cases.setdefault(key, {}).setdefault(sample["layout"], {})[sample["trial"]] = row
    shapes = {}
    for sample in runs:
        shape = {(r["kind"], r["payload"], r["changing"], r["reads_per_write"], r["row_count"]) for r in sample["rows"]}
        prior = shapes.setdefault(sample["dataset"], shape)
        if shape != prior:
            raise RuntimeError("workload cases differ across layouts/trials")
    results = []
    for key, layouts in sorted(cases.items()):
        if "production" not in layouts:
            raise RuntimeError("production baseline is required")
        for layout in ("current-production", "inline", "adaptive", "fields"):
            if layout not in layouts:
                continue
            common = sorted(set(layouts["production"]) & set(layouts[layout]))
            for metric in (("elapsed_ns", "write_and_flush_ns") if key[1] != "DOCUMENT_COLD" else ("elapsed_ns",)):
                ratios = [layouts[layout][t][metric] / max(1, layouts["production"][t][metric]) for t in common]
                if len(ratios) < 5:
                    raise RuntimeError("at least five paired samples per condition are required")
                lo, hi = interval(ratios)
                results.append(dict(dataset=key[0], kind=key[1], payload=key[2], changing=key[3], reads_per_write=key[4], row_count=key[5],
                                    layout=layout, metric=metric, ratio=statistics.median(ratios), lower_95=lo, upper_95=hi,
                                    regression=lo > 1 + threshold, noninferior=hi <= 1 + threshold, status=comparison_status(lo, hi, threshold), samples=len(ratios)))
    return results


def summarize_processes(runs: list[dict], threshold: float) -> list[dict]:
    grouped = {}
    for sample in runs:
        metrics = dict(cpu_seconds=sample["user_seconds"] + sample["system_seconds"],
                       peak_rss=sample["peak_rss"], elapsed_ns=sample["elapsed_ns"])
        grouped.setdefault(sample["dataset"], {}).setdefault(sample["layout"], {})[sample["trial"]] = metrics
    results = []
    for dataset, layouts in sorted(grouped.items()):
        for layout in ("current-production", "inline", "adaptive", "fields"):
            if layout not in layouts:
                continue
            common = sorted(set(layouts["production"]) & set(layouts[layout]))
            for metric in ("cpu_seconds", "peak_rss", "elapsed_ns"):
                ratios = [layouts[layout][t][metric] / max(1e-9, layouts["production"][t][metric]) for t in common]
                lo, hi = interval(ratios)
                results.append(dict(dataset=dataset, layout=layout, metric=metric, ratio=statistics.median(ratios),
                                    lower_95=lo, upper_95=hi, regression=lo > 1 + threshold, noninferior=hi <= 1 + threshold, status=comparison_status(lo, hi, threshold), samples=len(ratios)))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("storage", "public"), default="storage")
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--baseline-binary", type=Path, help="optional earlier binary for the production baseline; also measure current-production to expose changes affecting both layouts")
    parser.add_argument("--output", default=Path("output/document-payload"), type=Path)
    parser.add_argument("--trials", default=7, type=int)
    parser.add_argument("--datasets", nargs="+", choices=("repeated", "entropy"), default=["repeated", "entropy"])
    parser.add_argument("--layouts", nargs="+", choices=("production", "current-production", "inline", "adaptive", "fields"), default=["production", "inline", "adaptive"])
    parser.add_argument("--max-regression", default=.05, type=float)
    parser.add_argument("--rows", type=int, default=1, help="working-set rows for the storage fixture (1..256)")
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--bytes", type=int, choices=(4096, 65536, 262144, 1048576))
    parser.add_argument("--reads", type=int, choices=(0, 8, 64))
    parser.add_argument("--cold", action="store_true", help="advise only fixture files out of the Linux page cache before reopen")
    parser.add_argument("--allocator", choices=("production", "checked"), default="production", help="benchmark with the process allocator or diagnostic checked test allocator")
    parser.add_argument("--transforms", type=int, default=1, help="same-document transforms per public batch (1..128)")
    parser.add_argument("--preflight", action="store_true", help="public fixture only: include the extra prepared-plan correctness probe before timing")
    parser.add_argument("--memory", action="store_true", help="public fixture only: diagnostic process/LSM memory checkpoints; incompatible with --gate")
    parser.add_argument("--gate", action="store_true", help="fail on confirmed regressions")
    parser.add_argument("--strict-gate", action="store_true", help="also fail on inconclusive comparisons; require the upper confidence bound within budget")
    args = parser.parse_args()
    if "fields" in args.layouts and args.suite != "public":
        parser.error("fields measures indexed publication and requires --suite public")
    if args.strict_gate:
        args.gate = True
    if args.baseline_binary and "current-production" not in args.layouts:
        args.layouts.insert(1, "current-production")
    if "current-production" in args.layouts and not args.baseline_binary:
        parser.error("current-production requires --baseline-binary")
    if args.trials < 5 or "production" not in args.layouts or args.max_regression < 0:
        parser.error("use >=5 trials, include production, and a nonnegative regression limit")
    if len(set(args.datasets)) != len(args.datasets) or len(set(args.layouts)) != len(args.layouts):
        parser.error("datasets and layouts must be unique")
    if len(args.layouts) < 2:
        parser.error("include at least one layout to compare against production")
    if not 1 <= args.rows <= 256 or (args.suite == "public" and args.rows != 1):
        parser.error("storage rows must be 1..256; the public fixture uses one row")
    if not 2 <= args.rounds <= 1024:
        parser.error("rounds must be between 2 and 1024")
    if not 1 <= args.transforms <= 128 or (args.transforms != 1 and args.suite != "public"):
        parser.error("--transforms must be 1..128 and applies to the public fixture")
    os.environ.pop("ANTFLY_BENCH_DOCUMENT_TRANSFORMS", None)
    if args.suite == "public":
        os.environ["ANTFLY_BENCH_DOCUMENT_TRANSFORMS"] = str(args.transforms)
    if (args.preflight or args.memory) and args.suite != "public":
        parser.error("--preflight and --memory apply only to the public fixture")
    if (args.memory or args.preflight) and args.gate:
        parser.error("memory checkpoints and extra preflight are diagnostic instrumentation; run qualification without them")
    for name, enabled in (("PREFLIGHT", args.preflight), ("MEMORY", args.memory)):
        key = "ANTFLY_BENCH_DOCUMENT_" + name
        os.environ.pop(key, None)
        if enabled:
            os.environ[key] = "1"
    os.environ["ANTFLY_BENCH_DOCUMENT_ALLOCATOR"] = args.allocator
    # Explicit options make a report reproducible regardless of parent env.
    for name, value in (("ROWS", args.rows), ("ROUNDS", args.rounds), ("BYTES", args.bytes), ("READS", args.reads)):
        key = "ANTFLY_BENCH_DOCUMENT_" + name
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(value)
    os.environ.pop("ANTFLY_BENCH_DOCUMENT_COLD", None)
    if args.cold:
        os.environ["ANTFLY_BENCH_DOCUMENT_COLD"] = "1"
    if args.suite == "public" and args.cold:
        parser.error("--cold applies to the storage fixture; public fixture measures indexed updates")
    binary = args.binary.resolve()
    binary_hash = fingerprint(binary)
    baseline_binary = args.baseline_binary.resolve() if args.baseline_binary else binary
    baseline_hash = fingerprint(baseline_binary)
    harness_hash = fingerprint(Path(__file__))
    output = args.output.resolve()
    if "output" not in output.parts:
        parser.error("place raw qualification reports under ignored output/")
    if output.exists() and any(output.iterdir()):
        parser.error("use a fresh output directory; existing evidence is never overwritten")
    output.mkdir(parents=True, exist_ok=True)
    rng = random.Random(317)
    sample_files = []
    for trial in range(args.trials):
        jobs = [(dataset, layout) for dataset in args.datasets for layout in args.layouts]
        rng.shuffle(jobs)
        for dataset, layout in jobs:
            sample = run(baseline_binary if layout == "production" else binary, output, dataset, layout, trial, args.suite)
            sample_file = output / f"{dataset}-{layout}-{trial}.sample.json"
            sample_file.write_text(json.dumps(sample) + "\n")
            sample_files.append(sample_file)
            del sample
    # Load aggregate data only after all children exit. Retaining every trial's
    # decoded rows in the parent can otherwise inflate later children’s RSS floor.
    runs = [json.loads(path.read_text()) for path in sample_files]
    results = summarize(runs, args.max_regression)
    process_results = summarize_processes(runs, args.max_regression)
    limits = {}
    for name in ("cpu.max", "memory.max"):
        file = Path("/sys/fs/cgroup") / name
        if file.exists():
            limits[name] = file.read_text().strip()
    if fingerprint(binary) != binary_hash:
        raise RuntimeError("benchmark binary changed during qualification")
    if fingerprint(baseline_binary) != baseline_hash:
        raise RuntimeError("baseline binary changed during qualification")
    report = dict(version=7, binary=str(binary), sha256=binary_hash, baseline_binary=str(baseline_binary), baseline_sha256=baseline_hash, harness_sha256=harness_hash,
                  recorded_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                  host=dict(system=platform.system(), release=platform.release(), machine=platform.machine(), cpu_count=os.cpu_count(), cgroup=limits, allocator_environment={key: os.environ[key] for key in ("MALLOC_MMAP_THRESHOLD_", "MALLOC_TRIM_THRESHOLD_", "MALLOC_ARENA_MAX", "GLIBC_TUNABLES") if key in os.environ}),
                  workload=dict(suite=args.suite, rows=args.rows, rounds=args.rounds, payload=args.bytes, reads=args.reads, cold=args.cold, preflight=args.preflight, memory_checkpoints=args.memory, transforms=args.transforms, allocator=args.allocator),
                  rss_units="bytes" if platform.system() == "Darwin" else "KiB", threshold=args.max_regression,
                  gate="strict" if args.strict_gate else "confirmed_regressions" if args.gate else "report",
                  limitations=["engine cache is cold on reopen; OS cache uncontrolled unless explicitly advised by fixture",
                               "same host; repeated words and seeded word-separated base64-like payloads are synthetic",
                               "allocator selection covers workload/DB allocations; the executable still uses test-runner I/O/setup and is not a standalone server profile",
                               "checked allocator runs include additional test bookkeeping; compare allocator modes separately",
                               "multi-row storage fixtures drive ordinary maintenance inline because they have no background executor",
                               "paired bootstrap intervals are per condition, not a universal guarantee",
                               "wait4 RSS can include an inherited pre-exec floor; compare parent_peak_rss_before_exec before interpreting small-process RSS",
                               "public fixture includes full_index updates, but WAL counters cover the primary backend only" if args.suite == "public" else "source-layout fixture is not a substitute for public indexed-update qualification"],
                  runs=runs, results=results, process_results=process_results)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    regressions = [row for row in results if row["regression"]]
    process_regressions = [row for row in process_results if row["regression"]]
    print(f"{len(results)} workload comparisons; {len(regressions)} confirmed regressions beyond {args.max_regression:.0%}")
    for row in regressions:
        print(f"{row['layout']} {row['dataset']} {row['kind']} {row['payload']} bytes changing={row['changing']} reads={row['reads_per_write']}: {row['ratio']:.2f}x {row['metric']} ({row['lower_95']:.2f}–{row['upper_95']:.2f})")
    for row in process_regressions:
        print(f"process {row['layout']} {row['dataset']} {row['metric']}: {row['ratio']:.2f}x ({row['lower_95']:.2f}–{row['upper_95']:.2f})")
    inconclusive = [row for row in results + process_results if row["status"] == "inconclusive"]
    if args.strict_gate:
        print(f"{len(inconclusive)} inconclusive comparisons; strict qualification requires every upper confidence bound within budget")
    return 1 if args.gate and (regressions or process_regressions or (args.strict_gate and inconclusive)) else 0


if __name__ == "__main__":
    raise SystemExit(main())
