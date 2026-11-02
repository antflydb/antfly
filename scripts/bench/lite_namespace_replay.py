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

"""Local replay feedback loop; uses prebuilt libraries and synthetic inputs.

Example (each case runs in a separate process, without rebuilding):
  python3 scripts/bench/lite_namespace_replay.py --library /path/libantfly.dylib \
      --idle-namespaces 0 8 45 --output .tools/namespace-bench

The measured interval includes a write burst and a quiet background replay
window. No database status calls occur during that window. Explicit draining,
acknowledgment, search, integrity and reopen checks follow outside measurement.
This diagnostic is not a substitute for original-importer qualification.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

from lite_contention import percentile, sdk, usage_delta


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def progress(db):
    return [
        {
            key: index.get(key)
            for key in (
                "name",
                "kind",
                "replay_applied_sequence",
                "replay_target_sequence",
                "replay_catch_up_required",
                "doc_count",
                "term_count",
            )
        }
        for index in db.stats()["indexes"]
    ]


def counters(db):
    return db.pending_work_stats().get("background_publications", {})


def delta(before, after):
    # Missing instrumentation is unknown, not zero. Preserve resets explicitly.
    return {
        key: (value - before[key] if value >= before[key] else None)
        for key, value in after.items()
        if key in before
        and type(value) is int
        and type(before[key]) is int
        and key not in ("retirement_work_pages", "replay_budget_table_id")
        and not key.endswith("_table_id")
        and not key.endswith("_max_ns")
    }


def worker(args):
    af = sdk(str(Path(args.library).resolve()))
    case = args.output
    case.mkdir(parents=True, exist_ok=False)
    path = case / "benchmark.aflite"
    row = {
        "schema_version": 1,
        "library_sha256": digest(args.library),
        "benchmark_sha256": digest(__file__),
        "idle_namespaces": args.idle_namespaces[0],
        "batches": args.batches,
        "documents_per_batch": args.documents,
        "text_bytes": args.text_bytes,
        "quiet_seconds": args.seconds,
        "active_table": args.active_table,
        "no_sync": args.no_sync,
        "busy_timeout": args.busy_timeout,
        "settle_timeout_seconds": args.settle_timeout,
        "peak_sampling": "post-burst and every 50 ms during quiet window",
        "status": "error",
        "pid": os.getpid(),
        "load_average": os.getloadavg(),
    }
    started = time.monotonic()
    acknowledged = {}
    latencies = []
    failures = []
    with (
        af.create(path, no_sync=args.no_sync, busy_timeout=args.busy_timeout) as root,
        ExitStack() as handles,
    ):
        # Include real index artifacts in otherwise idle namespaces.
        for number in range(args.idle_namespaces[0]):
            name = f"idle_{number:03d}"
            root.create_table(name, {})
            with root.open_table(name) as table:
                table.add_index({"name": "text", "type": "full_text"})
                table.batch_json(
                    {"inserts": {"seed": {"text": "idle seeded artifact"}}}
                )
                table.run_until_idle()
        if args.active_table != "default":
            root.create_table(args.active_table, {})
        db = handles.enter_context(root.open_table(args.active_table))
        if args.active_table != "default":
            db.add_index({"name": "text", "type": "full_text"})
        root.run_until_idle()
        # Manual draining does not service the background owner's startup
        # schedule. Let it visit every namespace before measuring steady work.
        # Polling is confined to setup; it stops before the measured interval.
        settled = counters(root)
        initial_round = settled.get("maintenance_rounds")
        if initial_round is None:
            raise RuntimeError("library lacks maintenance-round instrumentation")
        settle_began = time.monotonic()
        while settled["maintenance_rounds"] <= initial_round:
            if time.monotonic() - settle_began >= args.settle_timeout:
                raise TimeoutError("background startup round did not complete")
            time.sleep(0.2)
            settled = counters(root)
        row["startup_settle_seconds"] = time.monotonic() - settle_began
        before = counters(root)
        initial_progress = progress(db)
        initial_size = path.stat().st_size
        row["setup_seconds"] = time.monotonic() - started
        usage = resource.getrusage(resource.RUSAGE_SELF)
        began = time.monotonic()
        for batch in range(args.batches):
            documents = {
                f"doc-{batch}-{i}": {
                    "text": f"namespacecanary payload {batch} {i} "
                    + ("amber bronze cedar delta " * (args.text_bytes // 25 + 1))[
                        : args.text_bytes
                    ],
                    "batch": batch,
                    "ordinal": i,
                }
                for i in range(args.documents)
            }
            call = time.monotonic_ns()
            try:
                db.batch_json({"inserts": documents, "sync_level": "write"})
                acknowledged.update(documents)
            except af.BusyError as error:
                failures.append({"batch": batch, "error": type(error).__name__})
            latencies.append(time.monotonic_ns() - call)
        burst_seconds = time.monotonic() - began
        peak_size = path.stat().st_size
        quiet = time.monotonic()
        while time.monotonic() - quiet < args.seconds:
            peak_size = max(peak_size, path.stat().st_size)
            time.sleep(min(0.05, max(0, args.seconds - (time.monotonic() - quiet))))
        elapsed = time.monotonic() - began
        measured_usage = usage_delta(usage, resource.getrusage(resource.RUSAGE_SELF))
        size = path.stat().st_size
        # These calls may acquire admission; keep them outside the timed window.
        observed_progress = progress(db)
        after = counters(root)
        row.update(
            measured_seconds=elapsed,
            burst_seconds=burst_seconds,
            successful_batches=args.batches - len(failures),
            busy=failures,
            acknowledged_documents=len(acknowledged),
            p50_ms=percentile(latencies, 50) / 1e6,
            p95_ms=percentile(latencies, 95) / 1e6,
            initial_bytes=initial_size,
            final_bytes=size,
            peak_bytes=peak_size,
            growth_bytes=size - initial_size,
            progress_before=initial_progress,
            progress_after=observed_progress,
            counters_before=before,
            counters_after=after,
            counter_deltas=delta(before, after),
            **measured_usage,
        )
        # Save diagnostics even if drain hangs and the parent watchdog fires.
        (case / "measurement.json").write_text(json.dumps(row, indent=2) + "\n")
        verify = time.monotonic()
        db.run_until_idle()
        for key, expected in acknowledged.items():
            if db.lookup(key) != expected:
                raise RuntimeError(f"acknowledged document mismatch: {key}")
        row["acknowledged_verification_passed"] = True
        row["progress_after_drain"] = progress(db)
        (case / "measurement.json").write_text(json.dumps(row, indent=2) + "\n")
        if any(
            index["replay_catch_up_required"] for index in row["progress_after_drain"]
        ):
            raise RuntimeError("explicit drain left active index behind")
        indexes = [
            index
            for index in row["progress_after_drain"]
            if index["kind"] == "full_text"
        ]
        if not indexes or any(
            index["doc_count"] != len(acknowledged) for index in indexes
        ):
            raise RuntimeError(
                "drained index count differs from acknowledged documents"
            )
        if acknowledged:
            result = db.search(
                {
                    "full_text_search": {
                        "match": {"field": "text", "text": "namespacecanary"}
                    },
                    "limit": 10,
                }
            )
            if not result["responses"][0]["hits"]["hits"]:
                raise RuntimeError("search canary missing")
        if not root.check()["valid"]:
            raise RuntimeError("integrity failed")
        row["verification_seconds"] = time.monotonic() - verify
    with af.open_readonly(path) as root, root.open_table(args.active_table) as db:
        for key, expected in acknowledged.items():
            if db.lookup(key) != expected:
                raise RuntimeError(f"reopen mismatch: {key}")
        if not root.check()["valid"]:
            raise RuntimeError("reopen integrity failed")
    row.update(status="ok" if not failures else "busy", verified=True)
    (case / "result.json").write_text(json.dumps(row, indent=2) + "\n")
    print(json.dumps(row), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument(
        "--active-table", choices=("default", "active"), default="active"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--idle-namespaces", type=int, nargs="+", default=[0, 8, 45])
    parser.add_argument("--batches", type=int, default=8)
    parser.add_argument("--documents", type=int, default=128)
    parser.add_argument("--text-bytes", type=int, default=1024)
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--busy-timeout", type=float, default=5)
    parser.add_argument("--watchdog", type=float, default=120)
    parser.add_argument("--settle-timeout", type=float, default=15)
    parser.add_argument("--no-sync", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if (
        min(args.idle_namespaces) < 0
        or min(args.batches, args.documents, args.text_bytes) < 1
        or min(args.seconds, args.watchdog, args.busy_timeout, args.settle_timeout) <= 0
    ):
        parser.error(
            "counts and durations must be positive; idle namespaces may be zero"
        )
    if args.worker:
        try:
            worker(args)
        except Exception as error:
            measured = args.output / "measurement.json"
            row = json.loads(measured.read_text()) if measured.exists() else {}
            row.update(status="error", error_type=type(error).__name__, verified=False)
            if args.output.exists():
                (args.output / "result.json").write_text(
                    json.dumps(row, indent=2) + "\n"
                )
            raise
        return
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    for order, count in enumerate(args.idle_namespaces):
        case = args.output / f"{order:02d}-idle-{count}"
        command = [
            sys.executable,
            __file__,
            "--worker",
            "--library",
            args.library,
            "--output",
            str(case),
            "--idle-namespaces",
            str(count),
            "--active-table",
            args.active_table,
        ]
        for option in (
            "batches",
            "documents",
            "text_bytes",
            "seconds",
            "busy_timeout",
            "settle_timeout",
        ):
            command.extend(
                ["--" + option.replace("_", "-"), str(getattr(args, option))]
            )
        if args.no_sync:
            command.append("--no-sync")
        with (args.output / f"{order:02d}.log").open("w") as log:
            try:
                process = subprocess.run(
                    command, stdout=log, stderr=log, timeout=args.watchdog
                )
                status = "ok" if process.returncode == 0 else "error"
            except subprocess.TimeoutExpired:
                status = "watchdog_timeout"
        result_path = case / "result.json"
        measurement_path = case / "measurement.json"
        if result_path.exists():
            result = json.loads(result_path.read_text())
        else:
            result = (
                json.loads(measurement_path.read_text())
                if measurement_path.exists()
                else {}
            )
            result.update(
                status=status, idle_namespaces=count, case=str(case), verified=False
            )
        result["order"] = order
        results.append(result)
        (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(result), flush=True)
    if any(result["status"] != "ok" for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
