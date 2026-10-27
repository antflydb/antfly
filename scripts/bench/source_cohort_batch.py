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

"""Compare atomic and sequential source-cohort refreshes on fresh disk stores.

This measures the native source-only primitive with two dirty cohorts and an
unchanged body-only index. It does not qualify automatic mutation eligibility,
field reanalysis or migration. Reports and logs must remain in ignored output/.
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

PHASES = ("prepare_ns", "publish_ns", "verify_ns")


def parse_metrics(text: str, documents: int, payload: int, sequential: bool) -> dict[str, int]:
    lines = [line.split("SOURCE_COHORT_BATCH ", 1)[1] for line in text.splitlines() if "SOURCE_COHORT_BATCH " in line]
    if len(lines) != 1:
        raise RuntimeError("expected exactly one native cohort-batch metrics record")
    result = {}
    for token in lines[0].split():
        match = re.fullmatch(r"(\w+)=(\d+)", token)
        if not match or match[1] in result:
            raise RuntimeError("malformed or duplicate cohort-batch metric")
        result[match[1]] = int(match[2])
    required = (*PHASES, "documents", "payload", "sequential", "cohorts", "barriers", "verified", "physical_shared", "production_allocator", "optimize_mode")
    if any(key not in result for key in required):
        raise RuntimeError("incomplete cohort-batch metrics")
    expected = dict(documents=documents, payload=payload, sequential=int(sequential), cohorts=2,
                    barriers=2 if sequential else 1, verified=5, physical_shared=1, production_allocator=1)
    if any(result[key] != value for key, value in expected.items()) or result["optimize_mode"] not in range(4):
        raise RuntimeError("fixture failed workload, sharing, atomic publication or retained-source verification")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--documents", nargs="+", type=int, default=[131, 4099])
    parser.add_argument("--payload", nargs="+", type=int, default=[0, 4096])
    parser.add_argument("--trials", type=int, default=7)
    parser.add_argument("--seed", type=int, default=137)
    parser.add_argument("--allow-nonrelease", action="store_true")
    args = parser.parse_args()
    if args.trials < 1 or len(set(args.documents)) != len(args.documents) or any(n < 131 or n > 4099 or n % 64 != 3 for n in args.documents):
        parser.error("positive trials and distinct document counts 64*k+3 in 131..4099 required")
    if len(set(args.payload)) != len(args.payload) or any(n < 0 or n > 16384 for n in args.payload):
        parser.error("distinct payload sizes 0..16384 required")
    root = Path(__file__).resolve().parents[2]
    binary = args.binary.resolve(strict=True)
    output = args.output.resolve()
    if not output.is_relative_to(root / "output"):
        parser.error("reports must live in this worktree's ignored output/")
    output.mkdir(parents=True, exist_ok=True)
    path = output / "report.json"
    if path.exists():
        parser.error("output already contains a report")
    digest = fingerprint(binary)
    report = dict(status="running", binary=str(binary), sha256=digest, host=platform.platform(),
                  cpu_count=os.cpu_count(), cpu_max=optional_text("/sys/fs/cgroup/cpu.max"),
                  memory_max=optional_text("/sys/fs/cgroup/memory.max"), seed=args.seed,
                  scope="native source-only refresh; two dirty cohorts; same source histories; fresh disk stores",
                  rss_scope="whole fixture including inputs, setup, cleanup and inherited parent RSS floor",
                  configuration=dict(documents=args.documents, payload=args.payload, trials=args.trials), runs=[])

    def save():
        temporary = output / "report.pending"
        temporary.write_text(json.dumps(report, indent=2) + "\n")
        temporary.replace(path)

    save()
    schedule = [(trial, n, size, sequential) for trial in range(args.trials) for n in args.documents
                for size in args.payload for sequential in (False, True)]
    random.Random(args.seed).shuffle(schedule)
    try:
        for trial, documents, payload, sequential in schedule:
            if fingerprint(binary) != digest:
                raise RuntimeError("benchmark binary changed during measurement")
            env = os.environ.copy()
            env.pop("ANTFLY_BENCH_SOURCE_COHORT_SEQUENTIAL", None)
            env.update(ANTFLY_BENCH_SOURCE_COHORT_BATCH="1", ANTFLY_BENCH_SOURCE_DOCUMENTS=str(documents), ANTFLY_BENCH_SOURCE_BYTES=str(payload))
            if sequential:
                env["ANTFLY_BENCH_SOURCE_COHORT_SEQUENTIAL"] = "1"
            log = output / f"trial-{trial}-docs-{documents}-bytes-{payload}-sequential-{int(sequential)}.log"
            parent_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            load_before = os.getloadavg()
            started = time.perf_counter_ns()
            with log.open("w") as stream:
                child = subprocess.Popen([str(binary), "--suite-filter", "batches dirty source cohorts"], cwd=root,
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
            metrics = parse_metrics(log.read_text(), documents, payload, sequential)
            if not args.allow_nonrelease and metrics["optimize_mode"] != 2:
                raise RuntimeError("ReleaseFast required for performance measurement")
            scale = 1 if platform.system() == "Darwin" else 1024
            report["runs"].append(dict(trial=trial, **metrics, elapsed_ns=elapsed,
                cpu_seconds=usage.ru_utime + usage.ru_stime, peak_rss_bytes=usage.ru_maxrss * scale,
                parent_peak_rss_bytes=parent_rss * scale, load_before=load_before, log=log.name))
            save()
        if fingerprint(binary) != digest:
            raise RuntimeError("benchmark binary changed during measurement")
        report["medians"] = [dict(documents=n, payload=size, sequential=int(sequential), **{
            phase: statistics.median(row[phase] for row in report["runs"] if
                (row["documents"], row["payload"], row["sequential"]) == (n, size, int(sequential)))
            for phase in (*PHASES, "cpu_seconds", "peak_rss_bytes")})
            for n in args.documents for size in args.payload for sequential in (False, True)]
        report["status"] = "complete"
    except BaseException as error:
        report["status"] = "interrupted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "failed"
        report["error"] = str(error)
        raise
    finally:
        save()
    print(path)


if __name__ == "__main__":
    main()
