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

"""Paired public-API qualification of bounded analyzed-contribution reuse.

Build document-patch-public-test-build -Doptimize=ReleaseFast first. Both arms
use the same executable and durability checks unless --baseline-binary pins an
earlier qualified fixture. Each run validates the selected input/reader state. The reference disables only the
optional contribution cache; --reference inline compares the complete native
layout with the inline control instead. These are different comparisons and
must not be combined. Reports and logs belong under ignored output/.
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

from document_payload import fingerprint, interval, comparison_status

# Actual source code includes escaped newlines/quotes/Unicode, not only plain
# synthetic strings. Wide/sparse/changing inputs exercise safe cache misses.
PROFILES = {
    "code-dense": (8, 65536, 16, False, False, "code"),
    "code-sparse": (32, 65536, 32, True, False, "code"),
    "code-wide": (64, 16384, 8, False, False, "code"),
    "large-compaction": (128, 65536, 8, False, False, "code"),
    "prose": (8, 65536, 16, False, False, "prose"),
    "entropy": (8, 65536, 16, False, False, "entropy"),
    "changing": (8, 65536, 16, False, True, "code"),
    "small": (8, 4096, 16, False, False, "code"),
}


def run(binary: Path, corpus: Path, output: Path, profile: str, trial: int,
        reference: bool, reference_kind: str, allow_debug: bool) -> dict:
    rows, size, rounds, sparse, changing, dataset = PROFILES[profile]
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("ANTFLY_BENCH_CONTRIBUTIONS", "ANTFLY_BENCH_DOCUMENT"))}
    env.update(ANTFLY_BENCH_CONTRIBUTIONS="1", ANTFLY_BENCH_DOCUMENT_ROWS=str(rows),
               ANTFLY_BENCH_DOCUMENT_BYTES=str(size), ANTFLY_BENCH_DOCUMENT_ROUNDS=str(rounds))
    if reference:
        env["ANTFLY_BENCH_CONTRIBUTIONS_REFERENCE"] = "1"
        if reference_kind == "inline":
            env["ANTFLY_BENCH_CONTRIBUTIONS_INLINE"] = "1"
    if sparse:
        env["ANTFLY_BENCH_CONTRIBUTIONS_SPARSE"] = "1"
    if changing:
        env["ANTFLY_BENCH_CONTRIBUTIONS_CHANGING"] = "1"
    if dataset == "code":
        env["ANTFLY_BENCH_CONTRIBUTIONS_CORPUS"] = str(corpus)
    if dataset == "entropy":
        env["ANTFLY_BENCH_DOCUMENT_ENTROPY"] = "1"
    mode = "reference" if reference else "reuse"
    log = output / f"{profile}-{trial}-{mode}.log"
    load = os.getloadavg()
    started = time.monotonic_ns()
    with log.open("w") as stream:
        child = subprocess.Popen([str(binary), "--suite-filter", "document payload admitted contribution workload"],
                                 env=env, stdout=stream, stderr=subprocess.STDOUT)
        _, status, usage = os.wait4(child.pid, 0)
        child.returncode = os.waitstatus_to_exitcode(status)
    elapsed = time.monotonic_ns() - started
    if child.returncode:
        raise RuntimeError(f"fixture exited {child.returncode}: {log}")
    records = [line.split("DOCUMENT_CONTRIBUTIONS ", 1)[1] for line in log.read_text().splitlines()
               if "DOCUMENT_CONTRIBUTIONS " in line]
    if len(records) != 1:
        raise RuntimeError(f"expected one workload record: {log}")
    row = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", records[0])}
    expected = dict(rows=rows, payload=size, rounds=rounds, sparse=int(sparse), changing=int(changing),
                    reference=int(reference), inline=int(reference and reference_kind == "inline"),
                    code=int(dataset == "code"), entropy=int(dataset == "entropy"),
                    native_rows=0 if reference and reference_kind == "inline" or size < 65536 else rows)
    if any(row.get(k) != v for k, v in expected.items()) or row.get("wal_syncs", 0) < rounds:
        raise RuntimeError(f"fixture did not honor the selected workload/durability: {log}")
    if row.get("fast") != 1 and not allow_debug:
        raise RuntimeError("performance qualification requires a ReleaseFast binary; --allow-debug is for smoke checks only")
    if not reference and profile in ("code-dense", "prose") and row.get("cache_hits", 0) == 0:
        raise RuntimeError(f"reuse workload never exercised the contribution cache: {log}")
    row.update(cpu_seconds=usage.ru_utime + usage.ru_stime,
               peak_rss=usage.ru_maxrss, process_elapsed_ns=elapsed)
    return dict(profile=profile, trial=trial, mode=mode, load_before=load, log=log.name, metrics=row)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--baseline-binary", type=Path, help="optional qualified earlier fixture binary for the reference arm; records both hashes")
    parser.add_argument("--corpus", type=Path, default=Path("zig/pkg/antfly-embedded/src/search/analysis.zig"))
    parser.add_argument("--profiles", nargs="+", choices=PROFILES,
                        default=["code-dense", "code-sparse", "entropy", "changing", "small"])
    parser.add_argument("--trials", type=int, default=7)
    parser.add_argument("--reference", choices=("cache-disabled", "inline"), default="cache-disabled")
    parser.add_argument("--max-regression", type=float, default=.05)
    parser.add_argument("--min-speedup", type=float, default=2.0)
    parser.add_argument("--benefit-profile", choices=PROFILES, default="code-dense")
    parser.add_argument("--gate", action="store_true", help="require measured benefit and reject confirmed CPU/RSS regressions")
    parser.add_argument("--allow-debug", action="store_true")
    parser.add_argument("--strict-gate", action="store_true", help="also reject inconclusive CPU/RSS regression limits")
    args = parser.parse_args()
    root = Path.cwd()
    binary, corpus, output = args.binary.resolve(), args.corpus.resolve(), args.output.resolve()
    if not output.is_relative_to(root / "output"):
        parser.error("reports must be stored beneath this worktree's ignored output/")
    if args.trials < 3 or len(set(args.profiles)) != len(args.profiles):
        parser.error("use at least three trials and unique profiles")
    if args.allow_debug and (args.gate or args.strict_gate):
        parser.error("debug smoke checks cannot satisfy an upgrade gate")
    if (args.gate or args.strict_gate) and args.benefit_profile not in args.profiles:
        parser.error("gated runs must include the benefit profile")
    output.mkdir(parents=True, exist_ok=True)
    baseline = args.baseline_binary.resolve() if args.baseline_binary else binary
    binary_hash, baseline_hash, corpus_hash = fingerprint(binary), fingerprint(baseline), fingerprint(corpus)
    rng = random.Random(16421)
    samples = []
    for trial in range(args.trials):
        profiles = args.profiles[:]
        rng.shuffle(profiles)
        for profile in profiles:
            modes = [False, True]
            rng.shuffle(modes)
            for reference in modes:
                sample = run(baseline if reference else binary, corpus, output, profile, trial, reference, args.reference, args.allow_debug)
                samples.append(sample)
                print(profile, trial, sample["mode"], sample["metrics"]["elapsed_ns"], flush=True)
    if fingerprint(binary) != binary_hash or fingerprint(baseline) != baseline_hash or fingerprint(corpus) != corpus_hash:
        raise RuntimeError("binary/corpus changed during qualification")
    comparisons = []
    for profile in args.profiles:
        arms = {mode: {r["trial"]: r["metrics"] for r in samples if r["profile"] == profile and r["mode"] == mode}
                for mode in ("reference", "reuse")}
        for metric in ("elapsed_ns", "cpu_seconds", "peak_rss", "process_elapsed_ns", "initial_compaction_ns", "compaction_ns", "wal_bytes"):
            ratios = [arms["reuse"][t][metric] / max(1e-9, arms["reference"][t][metric]) for t in range(args.trials)]
            lo, hi = interval(ratios)
            comparisons.append(dict(profile=profile, metric=metric, ratio=statistics.median(ratios),
                                    lower_95=lo, upper_95=hi, samples=len(ratios),
                                    status=comparison_status(lo, hi, args.max_regression)))
    target = next((c for c in comparisons if c["profile"] == args.benefit_profile and c["metric"] == "elapsed_ns"), None)
    benefit = target is not None and target["upper_95"] <= 1 / args.min_speedup
    controlled = [c for c in comparisons if c["metric"] in ("cpu_seconds", "peak_rss")]
    regressions = [c for c in controlled if c["status"] == "regression"]
    uncertain = [c for c in controlled if c["status"] == "inconclusive"]
    report = dict(host=dict(platform=platform.platform(), machine=platform.machine(), cpu_count=os.cpu_count()),
                  binary=str(binary), binary_sha256=binary_hash, baseline_binary=str(baseline), baseline_sha256=baseline_hash, corpus=str(corpus), corpus_sha256=corpus_hash,
                  reference=args.reference, arguments=vars(args) | {k: str(v) for k, v in vars(args).items() if isinstance(v, Path)},
                  comparisons=comparisons, benefit_demonstrated=benefit, runs=samples)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    for comparison in comparisons:
        if comparison["metric"] in ("elapsed_ns", "cpu_seconds", "peak_rss"):
            print(comparison)
    print("Benefit demonstrated:", benefit, "confirmed regressions:", len(regressions), "inconclusive:", len(uncertain))
    return int(((args.gate or args.strict_gate) and (not benefit or bool(regressions))) or (args.strict_gate and bool(uncertain)))


if __name__ == "__main__":
    raise SystemExit(main())
