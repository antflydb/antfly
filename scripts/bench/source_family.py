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

"""Measure real native source-family publication, updates, reads and reclamation.

Build source-family-bench-build with -Doptimize=ReleaseFast.
Every trial uses its own process and fresh disk-backed primary/index stores.
These are native storage measurements, not public mutation/outbox qualification.
Reports belong in ignored output/. No speedup or zero-cost conclusion is inferred
without a comparable baseline. Run inside the host/container being measured.
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

PHASES = (
    "prepare_ns", "publish_ns", "update_prepare_ns", "update_publish_ns",
    "reclaim_ns", "read_ns", "reopen_ns", "drop_ns",
)


def fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(256 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def optional_text(path: str) -> str | None:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def parse_metrics(text: str, documents: int, payload: int) -> dict[str, int]:
    records = []
    for line in text.splitlines():
        if "SOURCE_FAMILY " in line:
            record = {}
            for token in line.split("SOURCE_FAMILY ", 1)[1].split():
                match = re.fullmatch(r"(\w+)=(\d+)", token)
                if not match or match[1] in record:
                    raise RuntimeError("malformed or duplicate native metrics field")
                record[match[1]] = int(match[2])
            records.append(record)
    if len(records) != 1:
        raise RuntimeError(f"expected one native metrics record, found {len(records)}")
    result = records[0]
    required = (*PHASES, "documents", "payload", "update_claims", "physical_shared", "verified", "claims_after_gc", "production_allocator", "claims_after_drop", "optimize_mode")
    if any(key not in result for key in required):
        raise RuntimeError("incomplete metrics; rebuild the current benchmark binary")
    if (result["documents"], result["payload"]) != (documents, payload):
        raise RuntimeError("fixture did not honor workload parameters")
    if (result["physical_shared"] != 1 or result["verified"] != 12 or result["claims_after_gc"] != documents
            or result["production_allocator"] != 1 or result["claims_after_drop"] != 0
            or result["update_claims"] != (documents - 1) % 64 + 1 or result["optimize_mode"] not in range(4)):
        raise RuntimeError("fixture did not verify sharing, source reads, restart and reclamation")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--documents", type=int, nargs="+", default=[128, 131, 4096, 4099])
    parser.add_argument("--payload", type=int, nargs="+", default=[0, 4096])
    parser.add_argument("--trials", type=int, default=7)
    parser.add_argument("--seed", type=int, default=137)
    parser.add_argument("--allow-nonrelease", action="store_true", help="Allow correctness smoke runs with non-ReleaseFast binaries")
    args = parser.parse_args()
    if len(set(args.documents)) != len(args.documents) or len(set(args.payload)) != len(args.payload):
        parser.error("workload dimensions must be unique")
    if args.trials < 1 or any(n < 128 or n > 65539 or n % 64 not in (0, 3) for n in args.documents):
        parser.error("trials must be positive; documents must be 64*k or 64*k+3 between 128 and 65539")
    if any(n < 0 or n > 1024 * 1024 for n in args.payload):
        parser.error("payload must be between 0 and 1048576 bytes")
    if any(n * size > 64 * 1024 * 1024 for n in args.documents for size in args.payload):
        parser.error("fixture source payload must fit within 64 MiB")
    binary = args.binary.resolve(strict=True)
    root = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    if not output.is_relative_to(root / "output"):
        parser.error("reports must live under this worktree's ignored output/ directory")
    output.mkdir(parents=True, exist_ok=True)
    if (output / "report.json").exists():
        parser.error("output already contains a report; choose a fresh directory")
    binary_identity = binary.stat()
    report = {
        "status": "running",
        "scope": "native_source_family_storage_only",
        "fixture": "body-only index; shared repeated ASCII padding; one metadata change on the last document",
        "configuration": {"documents": args.documents, "payload": args.payload, "trials": args.trials},
        "rss_scope": "whole_fixture_including_inputs_and_cleanup",
        "binary": str(binary), "sha256": fingerprint(binary),
        "host": {"platform": platform.platform(), "machine": platform.machine(),
                 "cpus": os.cpu_count(), "cpu_quota": optional_text("/sys/fs/cgroup/cpu.max"),
                 "memory_limit": optional_text("/sys/fs/cgroup/memory.max")},
        "seed": args.seed, "runs": [],
    }
    workloads = [(n, size, trial) for n in args.documents for size in args.payload for trial in range(args.trials)]
    random.Random(args.seed).shuffle(workloads)
    for n, size, trial in workloads:
        log = output / f"source-family-{n}-{size}-{trial}.log"
        env = dict(os.environ, ANTFLY_BENCH_SOURCE_FAMILY="1", ANTFLY_BENCH_SOURCE_DOCUMENTS=str(n), ANTFLY_BENCH_SOURCE_BYTES=str(size))
        started = time.monotonic_ns()
        load_before = os.getloadavg()
        parent_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        with log.open("w") as stream:
            child = subprocess.Popen([str(binary), "--suite-filter", "family leaves publish exact sources"],
                                     cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
            _, status, usage = os.wait4(child.pid, 0)
            child.returncode = os.waitstatus_to_exitcode(status)
        if child.returncode:
            raise RuntimeError(f"native fixture failed ({child.returncode}); see {log}")
        current_binary = binary.stat()
        if any(getattr(current_binary, key) != getattr(binary_identity, key)
               for key in ("st_dev", "st_ino", "st_size", "st_mtime_ns")):
            raise RuntimeError("benchmark binary changed during measurement; freeze builds and rerun")
        metrics = parse_metrics(log.read_text(), n, size)
        if metrics["optimize_mode"] != 2 and not args.allow_nonrelease:
            raise RuntimeError("performance qualification requires ReleaseFast; use --allow-nonrelease for smoke checks")
        report["optimize_mode"] = ("Debug", "ReleaseSafe", "ReleaseFast", "ReleaseSmall")[metrics["optimize_mode"]]
        report["runs"].append({"trial": trial, "metrics": metrics, "wall_ns": time.monotonic_ns() - started,
                               "cpu_seconds": usage.ru_utime + usage.ru_stime,
                               "peak_rss_bytes": usage.ru_maxrss if platform.system() == "Darwin" else usage.ru_maxrss * 1024,
                               "parent_peak_rss_bytes": parent_rss if platform.system() == "Darwin" else parent_rss * 1024,
                               "load_before": load_before, "log": str(log.relative_to(root))})
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    report["medians"] = [dict(documents=n, payload=size, **{
        phase: statistics.median(run["metrics"][phase] for run in report["runs"]
                                 if run["metrics"]["documents"] == n and run["metrics"]["payload"] == size)
        for phase in PHASES}) for n in args.documents for size in args.payload]
    report["status"] = "complete"
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["medians"], indent=2))


if __name__ == "__main__":
    main()
