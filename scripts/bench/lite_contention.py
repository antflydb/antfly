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
import contextlib
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
import threading
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
    # A baseline library is driven through its own revision's SDK, which
    # binds exactly the functions that library exports.
    sys.path.insert(0, os.environ.get("ANTFLY_BENCH_SDK_PATH", str(SDK)))
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


LEASE_GROUPS = ("foreground_lease", "maintenance_lease")
LEASE_COUNTERS = (
    "acquisitions",
    "uncontended",
    "queued",
    "handoffs",
    "busy",
    "busy_wait_ns",
    "wait_ns",
    "holds",
    "hold_ns",
)


def lease_delta(before: dict, after: dict) -> dict | None:
    """Per-worker lease timing between two snapshots; None if not exposed."""
    if any(group not in after for group in LEASE_GROUPS):
        return None
    delta = {}
    for group in LEASE_GROUPS:
        old, new = before.get(group, {}), after[group]
        row = {field: new[field] - old.get(field, 0) for field in LEASE_COUNTERS}
        for field in ("wait_log2_ns", "hold_log2_ns"):
            row[field] = [
                b - a for a, b in zip(old.get(field, [0] * 64), new[field], strict=True)
            ]
        # Lifetime maxima: exact only when the worker started from zero.
        for field in ("wait_max_ns", "hold_max_ns"):
            row[field] = new[field] if not old.get("acquisitions") else None
        delta[group] = row
    delta["gate_wait_ns"] = after.get("gate_wait_ns", 0) - before.get("gate_wait_ns", 0)
    return delta


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
        db.run_until_idle()
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
        with (
            af.open(
                config["database"],
                no_sync=config["no_sync"],
                busy_timeout=config["timeout_ms"] / 1000,
            ) as db,
            contextlib.ExitStack() as thread_handles,
        ):
            threads = max(1, config.get("threads_per_worker", 1))
            # Each writer thread gets its own handle on the same database, so
            # threads contend at the process-wide path gate (where the lease
            # is handed off), not on one connection's mutex.
            handles = (
                [db]
                if threads == 1
                else [
                    thread_handles.enter_context(
                        af.open(
                            config["database"],
                            no_sync=config["no_sync"],
                            busy_timeout=config["timeout_ms"] / 1000,
                        )
                    )
                    for _ in range(threads)
                ]
            )
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
            window_seconds = config.get("progress_window_seconds", 60)
            success_windows = [0] * max(
                1, math.ceil(config["seconds"] / window_seconds)
            )
            lock = threading.Lock()
            last_write = None
            latest_churn_writes = {}
            # Longest interval without an acknowledged write, per thread: the
            # direct measure of starvation, independent of total throughput.
            thread_gaps = [0.0] * threads
            thread_last_progress = [0.0] * threads
            thread_successes = [0] * threads
            thread_windows = [[0] * len(success_windows) for _ in range(threads)]
            thread_errors = []
            visibility_samples = []
            visibility_interval = config.get("visibility_interval", 0)
            next_visibility = began + visibility_interval
            text_payload = (
                ("searchable churn alpha beta gamma delta " * 28)[:1024]
                if config.get("text_payload") in ("words", "changing-words")
                else "v" * 1024
            )

            def write_loop(thread_index):
                nonlocal last_write, next_visibility
                handle = handles[thread_index]
                last_progress = 0.0
                max_gap = 0.0
                sequence = 0
                prefix = (
                    f"worker-{index}"
                    if threads == 1
                    else f"worker-{index}-t{thread_index}"
                )
                try:
                    while time.monotonic() < deadline:
                        batch_docs = config.get("batch_docs", 1)
                        keyspace = max(128, batch_docs)
                        keys = [
                            f"{prefix}-{(sequence * batch_docs + j) if config['workload'] == 'append' else (sequence * batch_docs + j) % keyspace}"
                            for j in range(batch_docs)
                        ]
                        key = keys[-1]
                        token = (
                            f"churnworker{index}thread{thread_index}sequence{sequence}"
                            if config.get("text_payload") == "changing-words"
                            else None
                        )
                        payload = {
                            "inserts": {
                                k: {
                                    "text": f"{token} {text_payload}"
                                    if token
                                    else text_payload,
                                    "sequence": sequence,
                                }
                                for k in keys
                            }
                        }
                        began_call = time.perf_counter_ns()
                        try:
                            handle.batch_json(payload)
                            latency = time.perf_counter_ns() - began_call
                            elapsed = time.monotonic() - began
                            write = {"key": key, "sequence": sequence}
                            with lock:
                                success.append(latency)
                                window = min(
                                    len(success_windows) - 1,
                                    int(elapsed / window_seconds),
                                )
                                success_windows[window] += 1
                                thread_windows[thread_index][window] += 1
                                thread_successes[thread_index] += 1
                                for k in keys:
                                    item = {"key": k, "sequence": sequence}
                                    if token:
                                        item["text_token"] = token
                                        previous = latest_churn_writes.get(k)
                                        if previous is not None:
                                            item["replaced_text_token"] = previous[
                                                "text_token"
                                            ]
                                    if config["workload"] == "churn":
                                        latest_churn_writes[k] = item
                                    if k == key:
                                        last_write = item
                            max_gap = max(max_gap, elapsed - last_progress)
                            last_progress = elapsed
                        except af.BusyError:
                            with lock:
                                busy.append(time.perf_counter_ns() - began_call)
                        sequence += 1
                        if (
                            index == 0
                            and thread_index == 0
                            and visibility_interval > 0
                            and time.monotonic() >= next_visibility
                        ):
                            observed = time.monotonic()
                            try:
                                state = handle.pending_work_stats()
                                visibility_samples.append(
                                    {
                                        "started_seconds": observed - began,
                                        "ended_seconds": time.monotonic() - began,
                                        "debt": state.get("index_replay_debt", []),
                                        "backlog": state.get(
                                            "background_publications", {}
                                        ).get("replay_backlog"),
                                    }
                                )
                            except af.BusyError:
                                visibility_samples.append(
                                    {
                                        "started_seconds": observed - began,
                                        "ended_seconds": time.monotonic() - began,
                                        "error": "Busy",
                                    }
                                )
                            next_visibility = time.monotonic() + visibility_interval
                except BaseException:  # noqa: BLE001 - surfaced as a worker failure below
                    thread_errors.append(traceback.format_exc())
                thread_gaps[thread_index] = max_gap
                thread_last_progress[thread_index] = last_progress

            if threads == 1:
                write_loop(0)
            else:
                pool = [
                    threading.Thread(target=write_loop, args=(t,))
                    for t in range(threads)
                ]
                for thread in pool:
                    thread.start()
                for thread in pool:
                    thread.join()
            if thread_errors:
                raise RuntimeError(thread_errors[0])
            ended = time.monotonic()
            # The trailing interval after each thread's last acknowledged write.
            thread_gaps = [
                max(gap, (ended - began) - last)
                for gap, last in zip(thread_gaps, thread_last_progress, strict=True)
            ]
            max_progress_gap = max(thread_gaps)
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
                "success_windows": success_windows,
                "max_progress_gap_seconds": max_progress_gap,
                "thread_max_progress_gap_seconds": thread_gaps,
                "thread_successful_calls": thread_successes,
                "thread_success_windows": thread_windows,
                "progress_window_seconds": window_seconds,
                "lease": lease_delta(initial_stats, final_stats),
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
                        "allocator_service_admissions",
                    )
                },
                "last_maintenance_error": final_stats.get("last_maintenance_error"),
                "proof_observed_from": proof_observed_from,
                "proof_observed_until": proof_observed_until,
                "admission": admission,
                "last_write": last_write,
                "visibility_samples": visibility_samples,
                # At most 128 keys per worker, including holes left by Busy.
                "latest_churn_writes": list(latest_churn_writes.values()),
                **usage,
            }
            if config.get("allocator_diagnostics", False):
                result["background_publication_snapshot"] = final_stats
                # Admission/status reads can perturb storage. Collect only
                # after every timed worker and proof snapshot has finished.
                result["allocator_diagnostics"] = (
                    db.status().get("storage", {}).get("reclamation", {})
                )
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


def status_reader(config, stop, results):
    """The #1431 shape: an independent status-only reader once per interval."""
    reads, errors, latencies, startup_error = 0, [], [], None
    try:
        try:
            af = sdk(config["library"])
        except Exception as error:  # noqa: BLE001 - reported, and fails the run
            startup_error = f"{type(error).__name__}: {error}"[:300]
            raise
        while not stop.wait(config["status_interval"]):
            began = time.perf_counter_ns()
            try:
                options = af.OpenOptions(
                    mode=af.OpenMode.STATUS_ONLY,
                    profile=af.Profile.NATIVE,
                    busy_timeout=config["timeout_ms"] / 1000,
                )
                with af.open_with_options(config["database"], options) as db:
                    db.status()
                reads += 1
                latencies.append(time.perf_counter_ns() - began)
            except Exception as error:  # noqa: BLE001 - every failure is a reported reader error
                errors.append(f"{type(error).__name__}: {error}"[:300])
    finally:
        results.put(
            {
                "reads": reads,
                "errors": len(errors),
                "startup_error": startup_error,
                "first_errors": errors[:5],
                "p50_ns": percentile(latencies, 50),
                "max_ns": max(latencies, default=None),
            }
        )


def status_reader_problem(result: dict | None, exitcode: int | None) -> str | None:
    """Why the status reader cannot vouch for this run, or None.

    A reader that never started, crashed, or completed no read reports nothing
    about reader latency, so it must fail the run rather than read as clean.
    """
    if result is None:
        return "status_reader_failed"
    if result.get("startup_error") or exitcode not in (0, None):
        return "status_reader_failed"
    if result.get("reads", 0) == 0:
        return "status_reader_failed"
    if result.get("errors"):
        return "status_reader_errors"
    return None


def handoff_problem(row: dict, threads_per_worker: int) -> str | None:
    """A multi-threaded run exists to exercise in-process lease handoff.

    It qualifies only if handoffs were observed. A library without lease
    timing (an older control) cannot report them and is not judged here.
    """
    if threads_per_worker <= 1:
        return None
    lease = row.get("lease")
    if lease is None:
        return None
    if lease["foreground_lease"]["handoffs"] == 0:
        return "no_handoffs"
    return None


def acknowledged_writes(workers: list[dict]) -> list[dict]:
    """Use actual acknowledgments; Busy can leave holes in a churn sequence."""
    return [
        write
        for worker in workers
        for write in (
            worker.get("latest_churn_writes")
            or ([worker["last_write"]] if worker.get("last_write") else [])
        )
    ]


def verify(library: str, database: Path, expected: list[dict]) -> None:
    af = sdk(library)
    with af.open(database, busy_timeout=5.0) as db:
        for write in expected:
            document = db.lookup(write["key"])
            if document["sequence"] != write["sequence"] or (
                write.get("text_token")
                and not document["text"].startswith(write["text_token"] + " ")
            ):
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


def lease_summary(workers: list[dict]) -> dict | None:
    rows = [row.get("lease") for row in workers]
    if not rows or any(row is None for row in rows):
        return None
    summary = {}
    for group in LEASE_GROUPS:
        merged = {
            field: sum(row[group][field] for row in rows) for field in LEASE_COUNTERS
        }
        for field in ("wait_log2_ns", "hold_log2_ns"):
            merged[field] = [
                sum(row[group][field][i] for row in rows) for i in range(64)
            ]
        for kind, histogram in (
            ("wait", merged["wait_log2_ns"]),
            ("hold", merged["hold_log2_ns"]),
        ):
            for percent in (50, 99):
                lower, upper = histogram_percentile_bounds(histogram, percent)
                merged[f"{kind}_p{percent}_lower_ns"] = lower
                merged[f"{kind}_p{percent}_upper_ns"] = upper
        merged["per_worker_acquisitions"] = [row[group]["acquisitions"] for row in rows]
        merged["per_worker_busy"] = [row[group]["busy"] for row in rows]
        merged["per_worker_hold_ns"] = [row[group]["hold_ns"] for row in rows]
        merged["per_worker_wait_p99_upper_ns"] = [
            histogram_percentile_bounds(row[group]["wait_log2_ns"], 99)[1]
            for row in rows
        ]
        summary[group] = merged
    summary["gate_wait_ns"] = sum(row["gate_wait_ns"] for row in rows)
    return summary


def fairness(per_worker: list[int]) -> dict:
    """Jain's index (1.0 = equal shares) and min/max share ratio."""
    total = sum(per_worker)
    squares = sum(value * value for value in per_worker)
    return {
        "jain_index": (total * total) / (len(per_worker) * squares)
        if squares
        else None,
        "min_max_ratio": (min(per_worker) / max(per_worker))
        if max(per_worker, default=0)
        else None,
    }


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
    # Judge progress per writer thread when workers report it: a process can
    # make progress while one of its threads starves.
    starved = sum(
        sum(count == 0 for count in row["thread_successful_calls"])
        if row.get("thread_successful_calls") is not None
        else int(not row["success_latency_ns"])
        for row in workers
    )
    stalled = sum(
        sum(0 in windows for windows in row["thread_success_windows"])
        if row.get("thread_success_windows") is not None
        else int(0 in row.get("success_windows", []))
        for row in workers
    )
    status = "ok"
    if not successful:
        status = "no_progress"
    elif starved:
        status = "starved_workers"
    elif stalled:
        status = "stalled_workers"
    result = {
        "status": status,
        "workers_without_success": starved,
        "workers_with_stalled_windows": stalled,
        "progress_window_seconds": workers[0].get("progress_window_seconds", 60),
        "per_worker_max_progress_gap_seconds": [
            row.get("max_progress_gap_seconds") for row in workers
        ],
        "max_progress_gap_seconds": max(
            (row.get("max_progress_gap_seconds") or 0 for row in workers), default=None
        ),
        "throughput_fairness": fairness(
            [len(row["success_latency_ns"]) for row in workers]
        ),
        "lease": lease_summary(workers),
        "per_worker_success_windows": [
            row.get("success_windows", []) for row in workers
        ],
        "per_thread_successful_calls": [
            row.get("thread_successful_calls") for row in workers
        ],
        "per_thread_max_progress_gap_seconds": [
            row.get("thread_max_progress_gap_seconds") for row in workers
        ],
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
    # Workers arrive in completion order. Preserve identity alongside every
    # parallel per-worker array; array position is never a worker ID.
    if all("index" in row for row in workers):
        result["per_worker_indices"] = [row["index"] for row in workers]
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
    if any("allocator_diagnostics" in row for row in workers):
        result["per_worker_background_publication_snapshots"] = [
            {"index": row["index"], **row.get("background_publication_snapshot", {})}
            for row in workers
        ]
        result["allocator_diagnostics_after_workload"] = [
            {"index": row["index"], **row.get("allocator_diagnostics", {})}
            for row in workers
        ]
    result["visibility_samples"] = [
        sample for row in workers for sample in row.get("visibility_samples", [])
    ]
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
                "allocator_diagnostics": args.allocator_diagnostics,
                "visibility_interval": getattr(args, "visibility_interval", 0),
                "text_payload": getattr(args, "text_payload", "opaque"),
                "progress_window_seconds": args.progress_window,
                "status_interval": args.status_interval,
                "threads_per_worker": args.threads_per_worker,
                "batch_docs": args.batch_docs,
            }
            reader_stop, reader_results = context.Event(), context.Queue()
            reader = (
                context.Process(
                    target=status_reader, args=(config, reader_stop, reader_results)
                )
                if args.status_interval > 0
                else None
            )
            processes = [
                context.Process(
                    target=native_worker,
                    args=(config, i, ready, start, results, finish),
                )
                for i in range(concurrency)
            ]
            workers = []
            file_size_samples = []
            file_size_errors = []
            sample_stop = threading.Event()
            sample_thread = None

            def sample_file_size():
                began = time.monotonic()
                while True:
                    try:
                        file_size_samples.append(
                            {
                                "elapsed_seconds": time.monotonic() - began,
                                "file_bytes": database.stat().st_size,
                            }
                        )
                    except OSError as error:
                        file_size_errors.append(str(error))
                    if sample_stop.wait(1):
                        return

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
                sample_thread = threading.Thread(target=sample_file_size, daemon=True)
                sample_thread.start()
                if reader is not None:
                    reader.start()
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
                if reader is not None:
                    reader_stop.set()
                    try:
                        row["status_reader"] = reader_results.get(timeout=args.watchdog)
                    except queue.Empty:
                        row["status_reader"] = None
                    reader.join(timeout=args.watchdog)
                    row["status_reader_exitcode"] = reader.exitcode
                if any(worker["status"] != "ok" for worker in workers):
                    row.update(status="error", workers=workers)
                elif any(process.exitcode != 0 for process in processes):
                    row.update(
                        status="error",
                        exitcodes=[process.exitcode for process in processes],
                    )
                else:
                    row.update(aggregate(workers))
                    if row["status"] == "ok":
                        problem = handoff_problem(row, args.threads_per_worker)
                        if problem:
                            row.update(status=problem)
                    if reader is not None:
                        problem = status_reader_problem(
                            row["status_reader"], row["status_reader_exitcode"]
                        )
                        if problem:
                            row.update(status=problem)
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
                reader_stop.set()
                if reader is not None and reader.is_alive():
                    reader.join(timeout=5)
                    if reader.is_alive():
                        reader.kill()
                        reader.join()
                for process in processes:
                    if process.is_alive():
                        process.terminate()
                    process.join(timeout=5)
                    if process.is_alive():
                        process.kill()
                        process.join()
                sample_stop.set()
                if sample_thread is not None:
                    sample_thread.join()
                ready.close()
                results.close()
            row["file_size_samples"] = file_size_samples
            row["peak_file_bytes"] = max(
                (sample["file_bytes"] for sample in file_size_samples),
                default=database.stat().st_size,
            )
            row["file_size_sampling_errors"] = file_size_errors
            if file_size_errors:
                row.update(status="file_size_sampling_error")
            row["worker_lifetime_seconds"] = time.monotonic() - worker_started
            row["file_bytes"] = database.stat().st_size
            row["peak_file_bytes"] = max(row["peak_file_bytes"], row["file_bytes"])
            if row["status"] in (
                "ok",
                "no_progress",
                "starved_workers",
                "stalled_workers",
            ):
                expected = [
                    worker["last_write"]
                    for worker in workers
                    if worker["last_write"] is not None
                ]
                row["expected_last_writes"] = expected
                expected = acknowledged_writes(workers)
                row["expected_latest_writes"] = expected
                try:
                    checked = subprocess.run(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "_verify",
                            library,
                            str(database),
                            "-",
                        ],
                        input=json.dumps(expected),
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
            if getattr(args, "require_sustained", False):
                row["sustained_qualification"] = sustained_qualification(
                    row, args.seconds
                )
                if (
                    row["status"] == "ok"
                    and not row["sustained_qualification"]["passed"]
                ):
                    row["status"] = "sustained_qualification_failed"
            yield row
            if row["status"] not in ("ok", "no_progress", "starved_workers"):
                return


def active_visibility_qualification(
    samples: list[dict], seconds: float, maximum_seconds: float = 60
) -> dict:
    observed = [s for s in samples if "error" not in s and s.get("debt")]
    if len(observed) < 2 or observed[-1]["ended_seconds"] < seconds - 15:
        return {"passed": False, "reason": "insufficient_active_visibility_evidence"}
    maximum_observed = 0.0
    for sample in observed:
        if sample["started_seconds"] > seconds - maximum_seconds:
            continue  # Right-censored targets are checked by final catch-up.
        for debt in sample["debt"]:
            completed = next(
                (
                    later
                    for later in observed
                    if later["ended_seconds"] >= sample["ended_seconds"]
                    and any(
                        row["index_name"] == debt["index_name"]
                        and row["applied_sequence"] >= debt["target_sequence"]
                        for row in later["debt"]
                    )
                ),
                None,
            )
            if completed is None:
                return {
                    "passed": False,
                    "reason": "active_visibility_did_not_reach_prior_target",
                }
            upper_seconds = completed["ended_seconds"] - sample["started_seconds"]
            maximum_observed = max(maximum_observed, upper_seconds)
    return {
        "passed": maximum_observed <= maximum_seconds,
        "maximum_watermark_visibility_upper_seconds": maximum_observed,
        "maximum_allowed_seconds": maximum_seconds,
        "samples": len(observed),
    }


def sustained_qualification(row: dict, seconds: float) -> dict:
    """A surviving short run cannot qualify active-write storage or throughput.

    Compare complete minute windows and sample active file size after a
    three-minute warmup. These declared tolerances are a regression gate for
    this bounded churn workload, not a general database space guarantee.
    """
    policy = {
        "warmup_seconds": 180,
        "maximum_tail_growth_fraction": 0.25,
        "minimum_growth_allowance_bytes": 16 * 1024 * 1024,
        "minimum_late_throughput_fraction": 0.75,
    }
    windows = row.get("per_worker_success_windows", [])
    samples = row.get("file_size_samples", [])
    tail = [s for s in samples if 180 <= s["elapsed_seconds"] <= seconds]
    complete_minutes = int(seconds // 60)
    if (
        seconds < 600
        or not windows
        or any(len(w) < complete_minutes for w in windows)
        or not tail
        or tail[-1]["elapsed_seconds"] < seconds - 5
    ):
        return {
            "passed": False,
            "reason": "insufficient_sustained_evidence",
            "policy": policy,
        }
    totals = [sum(w[i] for w in windows) for i in range(complete_minutes)]
    early, late = sum(totals[1:4]), sum(totals[-3:])
    fraction = late / early if early else None
    initial = tail[0]["file_bytes"]
    growth = max(s["file_bytes"] for s in tail) - initial
    allowance = max(
        policy["minimum_growth_allowance_bytes"],
        initial * policy["maximum_tail_growth_fraction"],
    )
    visibility = active_visibility_qualification(
        row.get("visibility_samples", []), seconds
    )
    return {
        "passed": row.get("status") == "ok"
        and visibility["passed"]
        and growth <= allowance
        and fraction is not None
        and fraction >= policy["minimum_late_throughput_fraction"],
        "policy": policy,
        "minute_successful_calls": totals,
        "late_to_early_throughput_fraction": fraction,
        "active_visibility": visibility,
        "tail_initial_file_bytes": initial,
        "tail_growth_bytes": growth,
        "tail_growth_allowance_bytes": allowance,
    }


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
    parser.add_argument(
        "--visibility-interval",
        type=float,
        default=0,
        help="sample index progress during writes; observations consume measured wall time",
    )
    parser.add_argument(
        "--text-payload",
        choices=("opaque", "words", "changing-words"),
        default="opaque",
    )
    parser.add_argument("--no-sync", action="store_true")
    parser.add_argument(
        "--allocator-diagnostics",
        action="store_true",
        help="collect allocator counters after timed work; status reads can perturb storage",
    )
    parser.add_argument(
        "--no-index",
        action="store_true",
        help="native control: remove the default full-text index before seeding",
    )
    parser.add_argument(
        "--require-sustained",
        action="store_true",
        help="require >=10 minutes of bounded active-write growth and stable throughput",
    )
    parser.add_argument("--watchdog", type=float, default=30)
    parser.add_argument(
        "--progress-window",
        type=float,
        default=60,
        help="native: seconds per per-worker progress window (a zero window marks a stall)",
    )
    parser.add_argument(
        "--batch-docs",
        type=int,
        default=1,
        help="native: documents per batch call (one journal record each)",
    )
    parser.add_argument(
        "--threads-per-worker",
        type=int,
        default=1,
        help="native: writer threads per process sharing one handle (exercises in-process lease handoff)",
    )
    parser.add_argument(
        "--status-interval",
        type=float,
        default=0,
        help="native: run a status-only reader process every N seconds (0 disables)",
    )
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
        or args.progress_window <= 0
        or args.status_interval < 0
        or not 1 <= args.threads_per_worker <= 64
        or not 1 <= args.batch_docs <= 4096
    ):
        parser.error(
            "counts/durations must be positive; concurrency <= 128; timeout >= 0"
        )
    if args.mode == "native" and not args.library:
        parser.error("native mode requires --library")
    if args.require_sustained and args.visibility_interval <= 0:
        parser.error(
            "--require-sustained requires --visibility-interval for active index progress evidence"
        )
    if args.require_sustained and (
        args.mode != "native" or args.seconds < 600 or args.workload != "churn"
    ):
        parser.error(
            "--require-sustained requires native churn lasting at least 600 seconds"
        )
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
        verify(
            sys.argv[2],
            Path(sys.argv[3]),
            json.loads(sys.stdin.read() if sys.argv[4] == "-" else sys.argv[4]),
        )
    else:
        main()
