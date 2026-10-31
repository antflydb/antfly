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

"""Bounded queue and multi-process Lite contention measurements.

Writes synthetic fixtures only to fresh subdirectories of --output's parent.
Queue watchdog expirations are recorded as stalls, never as throughput samples.
Native latency measures entire batch calls (admission plus mutation), not an
invented decomposition. Proof counters separately measure detached graph walks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import multiprocessing as mp
import os
import platform
import queue
import resource
import subprocess
import sys
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SDK = ROOT / "py/packages/embedded/src"


def command(*args: str) -> str | None:
    try:
        return subprocess.check_output(
            args, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def usage_delta(before, after) -> dict:
    return {
        "cpu_seconds": (after.ru_utime + after.ru_stime)
        - (before.ru_utime + before.ru_stime),
        "voluntary_context_switches": after.ru_nvcsw - before.ru_nvcsw,
        "involuntary_context_switches": after.ru_nivcsw - before.ru_nivcsw,
    }


def percentile(values: list[int], percent: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percent / 100) - 1)]


def metadata(args) -> dict:
    return {
        "kind": "metadata",
        "schema_version": 1,
        "revision": command("git", "-C", str(ROOT), "rev-parse", "HEAD"),
        "dirty": bool(command("git", "-C", str(ROOT), "status", "--porcelain")),
        "system": platform.platform(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_sha256": {
            str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in [
                Path(__file__).resolve(),
                ROOT / "scripts/bench/lite_queue.zig",
                ROOT / "zig/pkg/antfly-embedded/src/capi/db.zig",
                ROOT / "zig/pkg/antfly-embedded/src/capi/lite_connection.zig",
                ROOT / "zig/lib/platform/src/sync.zig",
            ]
        },
        "arguments": vars(args),
        "vm_stat": command("vm_stat"),
        "swap_usage": command("sysctl", "-n", "vm.swapusage"),
        "load_average": os.getloadavg(),
    }


def emit(output: Path, row: dict) -> None:
    encoded = json.dumps(row, sort_keys=True)
    with output.open("a") as stream:
        stream.write(encoded + "\n")
    print(encoded, flush=True)


def queue_cases(args):
    binary = Path(args.binary).resolve()
    fingerprint = hashlib.sha256(binary.read_bytes()).hexdigest()
    for threads in args.concurrency:
        for repeat in range(args.repeats):
            for condition in ["std", "epoch"] if repeat % 2 == 0 else ["epoch", "std"]:
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                started = time.monotonic()
                row = {
                    "kind": "queue",
                    "condition": condition,
                    "threads": threads,
                    "repeat": repeat,
                    "binary_sha256": fingerprint,
                    "cpu_includes_process_startup": True,
                    "futex_counts_include_warmup_acquisitions": threads,
                }
                try:
                    completed = subprocess.run(
                        [str(binary), condition, str(threads), str(args.operations)],
                        text=True,
                        capture_output=True,
                        check=False,
                        timeout=args.watchdog,
                    )
                    if completed.returncode:
                        row.update(
                            status="error",
                            returncode=completed.returncode,
                            diagnostic=(completed.stdout + completed.stderr)[-4000:],
                        )
                    else:
                        result = json.loads(completed.stdout + completed.stderr)
                        row.update(result, status="ok")
                        row["operations_per_second"] = (
                            result["operations"] * 1e9 / result["elapsed_ns"]
                        )
                        row["condition_waits_per_operation"] = (
                            result["condition_wait_calls"] / result["operations"]
                        )
                except subprocess.TimeoutExpired:
                    row.update(status="watchdog_timeout", censored=True)
                row["process_seconds"] = time.monotonic() - started
                row.update(
                    usage_delta(before, resource.getrusage(resource.RUSAGE_CHILDREN))
                )
                yield row


def sdk(library: str):
    os.environ["ANTFLY_LIBRARY"] = library
    sys.path.insert(0, str(SDK))
    import antfly_embedded as af

    return af


PROOF_FIELDS = (
    "allocator_proof_walks",
    "allocator_proof_failures",
    "allocator_proof_ns",
)


ADMISSION_FIELDS = (
    "mutation_admission_count",
    "mutation_admission_ns",
    "mutation_admission_max_ns",
    "mutation_admission_log2_ns",
)


def measurement_stats(db) -> dict:
    from antfly_embedded import BusyError

    deadline = time.monotonic() + 30
    while True:
        try:
            stats = db.pending_work_stats().get("background_publications", {})
            break
        except BusyError:
            # Diagnostic reads occur outside the timed workload and are safe
            # to retry even when the measured mutation policy is fail-fast.
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.001)
    if any(field not in stats for field in PROOF_FIELDS + ADMISSION_FIELDS):
        raise RuntimeError(
            "library lacks allocator-proof instrumentation; rebuild this follow-up"
        )
    return stats


def seed(
    library: str, database: Path, count: int, no_sync: bool, no_index: bool = False
) -> None:
    af = sdk(library)
    with af.create(database, no_sync=no_sync, busy_timeout=5.0) as db:
        if no_index and not db.delete_index("full_text_index_v0"):
            raise RuntimeError("default index was not removed")
        for start in range(0, count, 64):
            db.batch_json(
                {
                    "inserts": {
                        f"seed-{i}": {"text": "s" * 4096}
                        for i in range(start, min(count, start + 64))
                    }
                }
            )
        for start in range(0, count, 64):
            db.batch_json(
                {
                    "deletes": [
                        f"seed-{i}"
                        for i in range(start, min(count, start + 64))
                        if i % 2
                    ]
                }
            )
        compact = db.compact()
        measurement_stats(db)
        print(
            json.dumps(
                {
                    "retained_seed_records": (count + 1) // 2,
                    "compact": compact,
                    "database": db.stats(),
                }
            )
        )


def native_worker(config, index, ready, start, results, finish=None):
    announced = False
    completed_workload = None
    try:
        af = sdk(config["library"])
        with af.open(
            config["database"],
            no_sync=config["no_sync"],
            busy_timeout=config["timeout_ms"] / 1000,
        ) as db:
            initial_stats = measurement_stats(db)
            proof_observed_from = time.monotonic()
            ready.put({"index": index, "status": "ok"})
            announced = True
            if not start.wait(config["watchdog"]):
                raise RuntimeError("start barrier timed out")
            before = resource.getrusage(resource.RUSAGE_SELF)
            began = time.monotonic()
            deadline = began + config["seconds"]
            success, busy = [], []
            sequence = 0
            last_write = None
            while time.monotonic() < deadline:
                key = f"worker-{index}-{sequence if config['workload'] == 'append' else sequence % 128}"
                payload = {"inserts": {key: {"text": "v" * 1024, "sequence": sequence}}}
                began_call = time.perf_counter_ns()
                try:
                    db.batch_json(payload)
                    success.append(time.perf_counter_ns() - began_call)
                    last_write = {"key": key, "sequence": sequence}
                except af.BusyError:
                    busy.append(time.perf_counter_ns() - began_call)
                sequence += 1
            ended = time.monotonic()
            # Preserve measured progress if a later diagnostic or close fails.
            # The terminal result remains an error and cannot pass qualification.
            completed_workload = {
                "started": began,
                "ended": ended,
                "successful_calls": len(success),
                "busy_calls": len(busy),
                "last_write": last_write,
            }
            # Diagnostics can acquire admission and run proofs. Wait until every
            # timed call has finished before allowing these reads.
            if finish is not None:
                finish.wait()
            usage = usage_delta(before, resource.getrusage(resource.RUSAGE_SELF))
            final_stats = measurement_stats(db)
            proof_observed_until = time.monotonic()
            # Keep peer close/publication work out of all final snapshots.
            if finish is not None:
                finish.wait()
            proofs = {
                field: final_stats[field] - initial_stats[field]
                for field in PROOF_FIELDS
            }
            admission = {
                "count": final_stats["mutation_admission_count"]
                - initial_stats["mutation_admission_count"],
                "ns": final_stats["mutation_admission_ns"]
                - initial_stats["mutation_admission_ns"],
                "max_ns": (
                    final_stats["mutation_admission_max_ns"]
                    if initial_stats["mutation_admission_count"] == 0
                    and final_stats["mutation_admission_count"] > 0
                    else None
                ),
                "log2_ns": [
                    after - before
                    for before, after in zip(
                        initial_stats["mutation_admission_log2_ns"],
                        final_stats["mutation_admission_log2_ns"],
                        strict=True,
                    )
                ],
            }
            result = {
                "index": index,
                "status": "ok",
                "started": began,
                "ended": ended,
                "success_latency_ns": success,
                "busy_latency_ns": busy,
                "proofs": proofs,
                "background_publication_deltas": {
                    field: final_stats.get(field, 0) - initial_stats.get(field, 0)
                    for field in (
                        "maintenance_rounds",
                        "attempts",
                        "refused",
                        "rebased",
                        "generated_attempts",
                        "generated_refused",
                        "replay_attempts",
                        "replay_refused",
                        "maintenance_errors",
                    )
                },
                "last_maintenance_error": final_stats.get("last_maintenance_error"),
                "proof_observed_from": proof_observed_from,
                "proof_observed_until": proof_observed_until,
                "admission": admission,
                "last_write": last_write,
                **usage,
            }
        # close()/__exit__ is part of worker success. Emit exactly one terminal
        # result so a close failure cannot follow an already accepted success.
        results.put(result)
    except BaseException:  # noqa: BLE001 - report a terminal result even on worker interruption
        if finish is not None:
            finish.abort()
        failure = {
            "index": index,
            "status": "error",
            "diagnostic": traceback.format_exc(),
            "completed_workload": completed_workload,
        }
        if not announced:
            ready.put(failure)
        results.put(failure)


def verify(library: str, database: Path, expected: list[dict]) -> None:
    af = sdk(library)
    with af.open(database, busy_timeout=5.0) as db:
        for write in expected:
            if db.lookup(write["key"])["sequence"] != write["sequence"]:
                raise RuntimeError(
                    f"last successful write was not retained: {write['key']}"
                )
        integrity = db.check()
        if isinstance(integrity, dict) and integrity.get("valid") is False:
            raise RuntimeError(f"database integrity failed: {integrity}")
        print(
            json.dumps(
                {
                    "checked_last_writes": len(expected),
                    "integrity": integrity,
                    "database": db.stats(),
                }
            )
        )


def histogram_percentile_bounds(
    histogram: list[int], percent: int
) -> tuple[int | None, int | None]:
    total = sum(histogram)
    if not total:
        return None, None
    rank = max(1, math.ceil(total * percent / 100))
    seen = 0
    for bucket, count in enumerate(histogram):
        seen += count
        if seen >= rank:
            return (0 if bucket == 0 else 1 << bucket), (1 << (bucket + 1)) - 1
    raise AssertionError("inconsistent admission histogram")


def aggregate(workers: list[dict]) -> dict:
    successful = [value for row in workers for value in row["success_latency_ns"]]
    busy = [value for row in workers for value in row["busy_latency_ns"]]
    elapsed = max(row["ended"] for row in workers) - min(
        row["started"] for row in workers
    )
    total = len(successful) + len(busy)
    walks = sum(row["proofs"]["allocator_proof_walks"] for row in workers)
    proof_elapsed = max(row["proof_observed_until"] for row in workers) - min(
        row["proof_observed_from"] for row in workers
    )
    if proof_elapsed <= 0:
        raise RuntimeError("invalid proof-observation interval")
    starved = sum(not row["success_latency_ns"] for row in workers)
    result = {
        "status": (
            "no_progress" if not successful else "starved_workers" if starved else "ok"
        ),
        "workers_without_success": starved,
        "per_worker_background_publication_deltas": [
            row.get("background_publication_deltas", {}) for row in workers
        ],
        "last_maintenance_errors": [
            row["last_maintenance_error"]
            for row in workers
            if row.get("last_maintenance_error")
        ],
        "elapsed_seconds": elapsed,
        "successful_calls": len(successful),
        "busy_calls": len(busy),
        "busy_fraction": len(busy) / total if total else None,
        "successful_calls_per_second": len(successful) / elapsed,
        "proof_observation_seconds": proof_elapsed,
        "proof_walks_per_second": walks / proof_elapsed,
        "observed_proof_walks_per_success": (
            walks / len(successful) if successful else None
        ),
        "per_worker_successful_calls": [
            len(row["success_latency_ns"]) for row in workers
        ],
        "per_worker_proof_walks": [
            row["proofs"]["allocator_proof_walks"] for row in workers
        ],
    }
    for name, values in (("success", successful), ("busy", busy)):
        for percent in (50, 95, 99):
            result[f"{name}_p{percent}_ns"] = percentile(values, percent)
        result[f"{name}_max_ns"] = max(values, default=None)
    for field in PROOF_FIELDS:
        result[field] = sum(row["proofs"][field] for row in workers)
    admission_count = sum(row["admission"]["count"] for row in workers)
    admission_ns = sum(row["admission"]["ns"] for row in workers)
    histogram = [
        sum(row["admission"]["log2_ns"][i] for row in workers) for i in range(64)
    ]
    if sum(histogram) != admission_count:
        raise RuntimeError("admission histogram does not match its sample count")
    result.update(
        admission_count=admission_count,
        admission_ns=admission_ns,
        admission_mean_ns=admission_ns / admission_count if admission_count else None,
        admission_log2_ns=histogram,
        admission_max_ns=(
            max(
                (
                    row["admission"]["max_ns"]
                    for row in workers
                    if row["admission"]["max_ns"] is not None
                ),
                default=None,
            )
            if admission_count
            else None
        ),
    )
    for percent in (50, 95, 99):
        lower, upper = histogram_percentile_bounds(histogram, percent)
        result[f"admission_p{percent}_lower_ns"] = lower
        result[f"admission_p{percent}_upper_ns"] = upper
    for field in (
        "cpu_seconds",
        "voluntary_context_switches",
        "involuntary_context_switches",
    ):
        result[field] = sum(row[field] for row in workers)
    return result


def native_cases(args):
    library = str(Path(args.library).resolve())
    fingerprint = hashlib.sha256(Path(library).read_bytes()).hexdigest()
    context = mp.get_context("spawn")
    for concurrency in args.concurrency:
        for repeat in range(args.repeats):
            directory = (
                Path(args.output).resolve().parent
                / f"{Path(args.output).stem}-p{concurrency}-r{repeat}"
            )
            directory.mkdir(exist_ok=False)
            database = directory / "database.aflite"
            seed_started = time.monotonic()
            try:
                seeded = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "_seed",
                        library,
                        str(database),
                        str(args.seed_records),
                        str(int(args.no_sync)),
                        str(int(args.no_index)),
                    ],
                    capture_output=True,
                    check=False,
                    text=True,
                    timeout=args.watchdog,
                )
            except subprocess.TimeoutExpired:
                yield {
                    "kind": "native",
                    "status": "seed_watchdog_timeout",
                    "censored": True,
                    "concurrency": concurrency,
                    "repeat": repeat,
                }
                return
            if seeded.returncode:
                yield {
                    "kind": "native",
                    "status": "seed_error",
                    "concurrency": concurrency,
                    "repeat": repeat,
                    "diagnostic": (seeded.stdout + seeded.stderr)[-4000:],
                }
                return
            seed_seconds = time.monotonic() - seed_started
            seed_stats = json.loads(seeded.stdout)
            ready, results = context.Queue(), context.Queue()
            start = context.Event()
            finish = context.Barrier(concurrency, timeout=args.watchdog)
            config = {
                "library": library,
                "database": str(database),
                "no_sync": args.no_sync,
                "timeout_ms": args.timeout_ms,
                "seconds": args.seconds,
                "watchdog": args.watchdog,
                "workload": args.workload,
            }
            processes = [
                context.Process(
                    target=native_worker,
                    args=(config, i, ready, start, results, finish),
                )
                for i in range(concurrency)
            ]
            workers = []
            worker_started = time.monotonic()
            row = {
                "kind": "native",
                "concurrency": concurrency,
                "repeat": repeat,
                "library_sha256": fingerprint,
                "timeout_ms": args.timeout_ms,
                "no_sync": args.no_sync,
                "workload": args.workload,
                "no_index": args.no_index,
                "seed_seconds": seed_seconds,
                "seed_stats": seed_stats,
                "latency_scope": "whole batch call including admission and mutation",
                "admission_metric_scope": "successful positive-timeout mutation admission before its body; percentile bounds from log2 nanosecond buckets",
                "proof_counter_scope": "process-owner totals between diagnostic snapshots; includes readiness wait, background work, and final diagnostic admission",
            }
            try:
                ready_by = time.monotonic() + args.watchdog
                for process in processes:
                    process.start()
                for _ in processes:
                    notice = ready.get(timeout=max(0.01, ready_by - time.monotonic()))
                    if notice["status"] != "ok":
                        raise RuntimeError(notice["diagnostic"])
                start.set()
                finish_by = (
                    time.monotonic()
                    + args.seconds
                    + args.timeout_ms / 1000
                    + args.watchdog
                )
                for _ in processes:
                    workers.append(
                        results.get(timeout=max(0.01, finish_by - time.monotonic()))
                    )
                for process in processes:
                    process.join(timeout=max(0.01, finish_by - time.monotonic()))
                if any(process.is_alive() for process in processes):
                    raise TimeoutError("worker close exceeded watchdog")
                if any(worker["status"] != "ok" for worker in workers):
                    row.update(status="error", workers=workers)
                elif any(process.exitcode != 0 for process in processes):
                    row.update(
                        status="error",
                        exitcodes=[process.exitcode for process in processes],
                    )
                else:
                    row.update(aggregate(workers))
            except RuntimeError as error:
                row.update(status="error", diagnostic=str(error))
            except (queue.Empty, TimeoutError):
                row.update(
                    status="watchdog_timeout",
                    censored=True,
                    completed_workers=len(workers),
                )
            finally:
                start.set()
                finish.abort()
                for process in processes:
                    if process.is_alive():
                        process.terminate()
                    process.join(timeout=5)
                    if process.is_alive():
                        process.kill()
                        process.join()
                ready.close()
                results.close()
            row["worker_lifetime_seconds"] = time.monotonic() - worker_started
            row["file_bytes"] = database.stat().st_size
            if row["status"] in ("ok", "no_progress", "starved_workers"):
                expected = [
                    worker["last_write"]
                    for worker in workers
                    if worker["last_write"] is not None
                ]
                try:
                    checked = subprocess.run(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "_verify",
                            library,
                            str(database),
                            json.dumps(expected),
                        ],
                        capture_output=True,
                        check=False,
                        text=True,
                        timeout=args.watchdog,
                    )
                    if checked.returncode:
                        row.update(
                            status="verification_error",
                            diagnostic=(checked.stdout + checked.stderr)[-4000:],
                        )
                    else:
                        row["verification"] = json.loads(checked.stdout)
                except subprocess.TimeoutExpired:
                    row.update(status="verification_watchdog_timeout", censored=True)
            yield row
            if row["status"] not in ("ok", "no_progress", "starved_workers"):
                return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("queue", "native"))
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--binary", default=str(ROOT / "zig/zig-out/bin/lite-queue-bench")
    )
    parser.add_argument("--library")
    parser.add_argument(
        "--concurrency", nargs="+", type=int, default=[1, 2, 4, 8, 16, 32]
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--operations", type=int, default=1000, help="queue operations per thread"
    )
    parser.add_argument(
        "--seconds",
        type=float,
        default=10,
        help="native measurement seconds per worker",
    )
    parser.add_argument("--timeout-ms", type=int, default=1000)
    parser.add_argument("--seed-records", type=int, default=1024)
    parser.add_argument("--workload", choices=("append", "churn"), default="churn")
    parser.add_argument("--no-sync", action="store_true")
    parser.add_argument(
        "--no-index",
        action="store_true",
        help="native control: remove the default full-text index before seeding",
    )
    parser.add_argument("--watchdog", type=float, default=30)
    args = parser.parse_args()
    if (
        min(args.concurrency) < 1
        or max(args.concurrency) > 128
        or args.repeats < 1
        or args.operations < 1
        or args.seconds <= 0
        or args.watchdog <= 0
        or args.timeout_ms < 0
        or args.seed_records < 1
    ):
        parser.error(
            "counts/durations must be positive; concurrency <= 128; timeout >= 0"
        )
    if args.mode == "native" and not args.library:
        parser.error("native mode requires --library")
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x"):
        pass
    emit(output, metadata(args))
    failed = False
    for row in queue_cases(args) if args.mode == "queue" else native_cases(args):
        emit(output, row)
        expected_baseline_stall = (
            args.mode == "queue"
            and row.get("condition") == "std"
            and row["status"] == "watchdog_timeout"
        )
        failed |= row["status"] != "ok" and not expected_baseline_stall
    emit(
        output,
        {
            "kind": "host_after",
            "vm_stat": command("vm_stat"),
            "swap_usage": command("sysctl", "-n", "vm.swapusage"),
            "load_average": os.getloadavg(),
        },
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_seed":
        seed(
            sys.argv[2],
            Path(sys.argv[3]),
            int(sys.argv[4]),
            bool(int(sys.argv[5])),
            bool(int(sys.argv[6])) if len(sys.argv) > 6 else False,
        )
    elif len(sys.argv) > 1 and sys.argv[1] == "_verify":
        verify(sys.argv[2], Path(sys.argv[3]), json.loads(sys.argv[4]))
    else:
        main()
