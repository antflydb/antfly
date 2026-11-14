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

"""Public-API vector ingestion, concurrent queries, RSS/FD sampling and recovery.

Runs each soft-limit profile in a fresh server. The client retains its own FD
limit. Synthetic data is deterministic, not a substitute for Cohere results.
No retries hide failed commits or queries. Latencies have no machine-specific
pass threshold. Use --data for paper-prepared vectors.npy and permissions.npy.
"""

import argparse
from builtins import BaseExceptionGroup
import ctypes
import datetime
import resource
import shlex
import hashlib
import json
import math
import platform
import random
import shutil
import statistics
import sys
import threading
import time
from pathlib import Path

import requests

from conftest import ANTFLY_PUBLIC_API_ROOT, StandaloneAntflyServer, find_free_port
from test_grep_indexed import logical_process_reader


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        raise ValueError("no latency observations")
    return {
        "count": len(ordered),
        "p50_ms": statistics.median(ordered) * 1000,
        "p95_ms": ordered[math.ceil(0.95 * len(ordered)) - 1] * 1000,
        "p99_ms": ordered[math.ceil(0.99 * len(ordered)) - 1] * 1000,
        "max_ms": ordered[-1] * 1000,
    }


def descriptor_reader(pid):
    if sys.platform == "linux":
        return lambda: len(list(Path(f"/proc/{pid}/fd").iterdir()))
    if sys.platform == "darwin":
        lib = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        lib.proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        lib.proc_pidinfo.restype = ctypes.c_int

        def read():
            # PROC_PIDLISTFDS returns proc_fdinfo (int fd, uint32 type).
            size = lib.proc_pidinfo(pid, 1, 0, None, 0)
            if size <= 0:
                raise OSError(ctypes.get_errno(), "proc_pidinfo size")
            buffer = ctypes.create_string_buffer(size + 4096)
            used = lib.proc_pidinfo(pid, 1, 0, buffer, len(buffer))
            if used <= 0 or used == len(buffer):
                raise OSError(ctypes.get_errno(), "proc_pidinfo truncated")
            return used // 8

        return read
    raise ValueError("qualification requires Linux or macOS")


def vector_mapping_rss(pid):
    """Resident bytes of .afvb mappings, including retained deleted generations."""
    if sys.platform != "linux":
        return None
    total, vector = 0, False
    for line in Path(f"/proc/{pid}/smaps").read_text().splitlines():
        fields = line.split()
        if fields and "-" in fields[0] and not fields[0].endswith(":"):
            vector = ".afvb" in line
        elif vector and line.startswith("Rss:"):
            total += int(fields[1]) * 1024
    return total


class Monitor:
    def __init__(self, process):
        self.process = process
        pid = process.pid
        self.samples_pid = pid
        self.memory = logical_process_reader(pid)
        self.fds = descriptor_reader(pid)
        self.samples = []
        self.failure = None
        self.failure_event = None
        self.vector_rss_samples = []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.run)
        self.closed = False

    lookup_grace_seconds = 0.5

    def run(self):
        missing_since = None
        try:
            while not self.done.is_set() and self.process.poll() is None:
                try:
                    now = time.monotonic_ns()
                    self.samples.append(
                        {"time_ns": now, "fds": self.fds(), **self.memory()}
                    )
                    if (
                        not self.vector_rss_samples
                        or now - self.vector_rss_samples[-1]["time_ns"] >= 1_000_000_000
                    ):
                        self.vector_rss_samples.append(
                            {
                                "time_ns": now,
                                "rss_bytes": vector_mapping_rss(self.samples_pid),
                            }
                        )
                    missing_since = None
                except OSError as exc:
                    # proc_pidinfo and /proc can briefly lose the image during
                    # exec/exit before waitpid observes termination. Retry only
                    # lookup races, never permission or actual sampling errors.
                    if isinstance(exc, (ProcessLookupError, FileNotFoundError)):
                        if self.process.poll() is not None:
                            return
                        missing_since = (
                            time.monotonic() if missing_since is None else missing_since
                        )
                        if time.monotonic() - missing_since < self.lookup_grace_seconds:
                            self.done.wait(0.01)
                            continue
                    raise
                self.done.wait(0.05)
        except BaseException as exc:
            self.failure = exc
            if self.failure_event is not None:
                self.failure_event.set()

    def start(self):
        self.thread.start()

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.done.set()
        if self.thread.ident is not None:
            self.thread.join()
        if self.failure:
            raise self.failure
        if not self.samples:
            raise RuntimeError("process monitor captured no samples")


class QueryMeasurements:
    """Assign by the request interval; never contaminate either steady phase."""

    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.lock = threading.Lock()
        self.phase = "ingestion"
        self.boundaries = []
        self.samples = {"ingestion": [], "steady": [], "transition": []}

    def begin(self):
        with self.lock:
            return self.phase, self.clock()

    def finish(self, started):
        with self.lock:
            phase, timestamp = started
            ended = self.clock()
            bucket = (
                phase
                if phase in ("ingestion", "steady") and phase == self.phase
                else "transition"
            )
            self.samples[bucket].append(ended - timestamp)

    def switch(self, phase):
        with self.lock:
            self.boundaries.append(
                {"phase": phase, "time_ns": int(self.clock() * 1_000_000_000)}
            )
            self.phase = phase

    def summary(self):
        with self.lock:
            return {
                phase: distribution(values) if values else None
                for phase, values in self.samples.items()
            }


class ProcessMeasurements:
    """One sampler per launched PID, started before the readiness wait."""

    def __init__(self):
        self.phase = "initial"
        self.monitors = {}
        self.failed = threading.Event()

    def start(self, process):
        if self.phase in self.monitors:
            raise RuntimeError(f"duplicate process phase {self.phase}")
        monitor = Monitor(process)
        monitor.failure_event = self.failed
        self.monitors[self.phase] = monitor
        monitor.start()

    def check(self):
        # Constant-time healthy path; callbacks publish the exception before
        # signalling. Query failures also wake this event and are checked by
        # the qualification's shared health check.
        if not self.failed.is_set():
            return
        errors = [
            monitor.failure
            for monitor in self.monitors.values()
            if monitor.failure is not None
        ]
        if errors:
            raise BaseExceptionGroup("process monitoring failed", errors)

    def wait(self, seconds):
        self.failed.wait(seconds)
        self.check()

    def finish_phase(self):
        self.monitors[self.phase].close()

    def close(self):
        errors = []
        for monitor in self.monitors.values():
            try:
                monitor.close()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise BaseExceptionGroup("process monitoring failed", errors)

    def records(self):
        return {
            phase: {
                "pid": monitor.process.pid,
                "sampling_error": repr(monitor.failure)
                if monitor.failure is not None
                else None,
                "peak_fds": max(
                    (sample["fds"] for sample in monitor.samples), default=None
                ),
                "peak_rss_bytes": max(
                    (sample["rss_bytes"] for sample in monitor.samples), default=None
                ),
                "peak_vector_mapping_rss_bytes": max(
                    (
                        sample["rss_bytes"]
                        for sample in monitor.vector_rss_samples
                        if sample["rss_bytes"] is not None
                    ),
                    default=None,
                ),
                "samples": monitor.samples,
                "vector_mapping_rss_samples": monitor.vector_rss_samples,
            }
            for phase, monitor in self.monitors.items()
        }

    def peaks(self):
        samples = [
            sample for monitor in self.monitors.values() for sample in monitor.samples
        ]
        vector_rss = [
            sample["rss_bytes"]
            for monitor in self.monitors.values()
            for sample in monitor.vector_rss_samples
            if sample["rss_bytes"] is not None
        ]
        return {
            "peak_fds": max(sample["fds"] for sample in samples),
            "peak_rss_bytes": max(sample["rss_bytes"] for sample in samples),
            "peak_vector_mapping_rss_bytes": max(vector_rss, default=None),
        }


def join_query_worker(worker, timeout):
    worker.join(timeout=timeout)
    if worker.is_alive():
        raise RuntimeError("query worker failed to stop")


def finish_qualification(
    report,
    output,
    limit,
    *,
    server,
    session,
    wrapper,
    measurements,
    primary_error,
    query_worker=None,
):
    """Attempt every cleanup even when diagnostics or earlier cleanup fail."""
    errors = []

    def attempt(name, action):
        try:
            action()
        except BaseException as exc:
            report.setdefault("cleanup_errors", []).append(
                {"operation": name, "error": str(exc)}
            )
            errors.append(exc)
            report["completed"] = False

    if server is not None:
        attempt(
            "copy_log",
            lambda: shutil.copyfile(server.log_path, output / f"server-{limit}.log"),
        )
        attempt("stop_server", lambda: server.stop(test_failed=not report["completed"]))
    # On a primary failure, terminate the server before waiting for an
    # outstanding query: closing its connection unblocks the reader. Every
    # teardown failure remains secondary to the operation that failed.
    if query_worker is not None:
        attempt("close_query_worker", lambda: join_query_worker(query_worker, 10))
    attempt("close_monitors", measurements.close)
    attempt("close_session", session.close)
    attempt("unlink_wrapper", lambda: wrapper.unlink(missing_ok=True))
    attempt(
        "capture_samples",
        lambda: report.update(phase_measurements=measurements.records()),
    )
    attempt(
        "write_report",
        lambda: (output / f"limit-{limit}.json").write_text(
            json.dumps(report, indent=2) + "\n"
        ),
    )
    if errors:
        if primary_error is not None:
            primary_error.add_note(
                f"Qualification cleanup also failed: {report['cleanup_errors']}"
            )
        else:
            raise BaseExceptionGroup("qualification cleanup failed", errors)


def corpus(data, count):
    if data:
        import numpy as np

        vectors = np.load(data / "vectors.npy", mmap_mode="r", allow_pickle=False)
        masks = np.load(data / "permissions.npy", mmap_mode="r", allow_pickle=False)
        if (
            vectors.ndim != 2
            or vectors.shape[1] != 768
            or len(vectors) < count
            or len(masks) < count
        ):
            raise ValueError(
                "expected at least --documents rows of 768-dimensional vectors and permission masks"
            )

        def row(index):
            vector = vectors[index].tolist()
            if not all(math.isfinite(v) for v in vector) or not any(vector):
                raise ValueError(f"invalid vector at row {index}")
            mask = int(masks[index])
            if not 0 <= mask <= 255:
                raise ValueError("permission mask must contain eight bits")
            return vector, mask

        return row

    def row(index):
        rng = random.Random(index + 126)
        return [rng.uniform(-1, 1) for _ in range(768)], rng.randrange(256)

    return row


def call(session, url, method, path, body=None):
    response = session.request(method, url + path, json=body, timeout=300)
    if not response.ok:
        raise RuntimeError(
            f"{method} {path}: HTTP {response.status_code}: {response.text[:2048]}"
        )
    return response.json()


def query(session, url, vector):
    result = call(
        session,
        url,
        "POST",
        "/tables/documents/query",
        {
            "embeddings": {"vec": vector},
            "indexes": ["vec"],
            "limit": 1,
        },
    )
    hits = result["responses"][0]["hits"]["hits"]
    if not hits or hits[0]["_id"] != "d000000000":
        raise RuntimeError(f"self-query did not recover first document: {hits}")


def wait_visible(session, url, count):
    deadline = time.monotonic() + 300
    while True:
        state = call(session, url, "GET", "/tables/documents/indexes/vec").get(
            "status", {}
        )
        if (
            state.get("total_indexed") == count
            and state.get("query_visible_doc_count") == count
        ):
            return state
        if time.monotonic() >= deadline:
            raise RuntimeError(f"index visibility did not reach {count}: {state}")
        time.sleep(0.25)


def write_server_wrapper(path, binary, limit):
    # Shell quoting supports interpreter/binary paths containing spaces. The
    # wrapper changes only its own limit, then replaces itself with the server.
    code = (
        "import os, resource, sys; "
        f"resource.setrlimit(resource.RLIMIT_NOFILE, ({limit}, resource.getrlimit(resource.RLIMIT_NOFILE)[1])); "
        f"os.execv({str(binary)!r}, [{str(binary)!r}, *sys.argv[1:]])"
    )
    path.write_text(
        f'#!/bin/sh\nexec {shlex.quote(sys.executable)} -c {shlex.quote(code)} "$@"\n'
    )
    path.chmod(0o700)


def qualify(binary, limit, count, data, output, steady_seconds):
    row = corpus(data, count)
    wrapper = output / f"server-{limit}.sh"
    report = {
        "schema_version": 2,
        "soft_limit": limit,
        "documents": count,
        "dimensions": 768,
        "batch_size": 256,
        "sync_level": "full_index",
        "source": str(data) if data else "synthetic:random.Random(row+126)",
        "permission_fields": 8,
        "query_think_seconds": 0.02,
        "sample_interval_seconds": 0.05,
        "completed": False,
    }
    server = None
    measurements = ProcessMeasurements()
    session = requests.Session()
    worker = None
    worker_started = False
    try:
        write_server_wrapper(wrapper, binary, limit)
        server = StandaloneAntflyServer(
            str(wrapper),
            "127.0.0.1",
            find_free_port(),
            api_root=ANTFLY_PUBLIC_API_ROOT,
            on_process_started=measurements.start,
        )
        report["server_root"] = str(server.root)
        measurements.check()
        url = server.api_url
        call(session, url, "POST", "/tables/documents", {"num_shards": 1})
        call(
            session,
            url,
            "POST",
            "/tables/documents/indexes/vec",
            {
                "type": "embeddings",
                "dimension": 768,
                "distance_metric": "cosine",
                "external": True,
            },
        )
        first_vector = row(0)[0]
        query_done = threading.Event()
        query_errors, ingest_times = [], []
        query_times = QueryMeasurements()
        digest = hashlib.sha256()

        def check_health():
            measurements.check()
            if query_errors:
                raise RuntimeError(query_errors[0])

        def queries():
            with requests.Session() as reader:
                try:
                    while not query_done.is_set():
                        check_health()
                        started_query = query_times.begin()
                        query(reader, url, first_vector)
                        query_times.finish(started_query)
                        query_done.wait(0.02)
                except BaseException as exc:
                    query_errors.append(str(exc))
                    measurements.failed.set()

        started = time.perf_counter()
        try:
            for offset in range(0, count, 256):
                check_health()
                documents = {}
                for index in range(offset, min(offset + 256, count)):
                    vector, mask = row(index)
                    digest.update(
                        json.dumps(
                            [vector, mask], separators=(",", ":"), allow_nan=False
                        ).encode()
                    )
                    documents[f"d{index:09d}"] = {
                        "_embeddings": {"vec": vector},
                        **{
                            f"allow{i}": "yes" if mask & (1 << i) else "no"
                            for i in range(8)
                        },
                    }
                start = time.perf_counter()
                result = call(
                    session,
                    url,
                    "POST",
                    "/tables/documents/batch",
                    {"inserts": documents, "sync_level": "full_index"},
                )
                ingest_times.append(time.perf_counter() - start)
                if result.get("status") != "committed":
                    raise RuntimeError(f"incomplete acknowledgement: {result}")
                if worker is None:
                    worker = threading.Thread(target=queries)
                    worker.start()
                    worker_started = True
                report["committed"] = min(offset + 256, count)
                if (
                    offset == 0
                    or report["committed"] % 4096 == 0
                    or report["committed"] == count
                ):
                    print(
                        json.dumps(
                            {
                                "soft_limit": limit,
                                "committed": report["committed"],
                                "elapsed_seconds": time.perf_counter() - started,
                            }
                        ),
                        flush=True,
                    )
            check_health()
            report["visibility"] = wait_visible(session, url, count)
            report["ingestion_seconds"] = time.perf_counter() - started
            query_times.switch("steady")
            measurements.wait(steady_seconds)
            check_health()
        finally:
            query_times.switch("finished")
            query_done.set()
            # Never join a thread whose start failed, nor replace a primary
            # operation failure with a join error. Failure cleanup stops the
            # server first and then performs the bounded join.
            if worker_started and sys.exception() is None:
                join_query_worker(worker, 310)
        check_health()
        report.update(
            {
                "corpus_sha256": digest.hexdigest(),
                "batch_latency": distribution(ingest_times),
                "query_latency": query_times.summary(),
                "query_phase_boundaries": query_times.boundaries,
                "documents_per_second": count / report["ingestion_seconds"],
            }
        )
        process = server.proc
        server.pause()
        report["graceful_exit_code"] = process.returncode
        if process.returncode != 0:
            raise RuntimeError(f"server failed graceful shutdown: {process.returncode}")
        measurements.finish_phase()
        measurements.phase = "recovery"
        server.resume()
        measurements.check()
        report["recovered_visibility"] = wait_visible(session, url, count)
        for _ in range(20):
            measurements.check()
            query(session, url, first_vector)
        process = server.proc
        server.pause()
        report["recovered_exit_code"] = process.returncode
        if process.returncode != 0:
            raise RuntimeError(
                f"reopened server failed graceful shutdown: {process.returncode}"
            )
        measurements.close()
        report.update(measurements.peaks())
        if report["peak_fds"] >= limit:
            raise RuntimeError("sampled descriptor use reached the soft limit")
        report["completed"] = True
    except BaseException as exc:
        report["error"] = str(exc)
        raise
    finally:
        finish_qualification(
            report,
            output,
            limit,
            server=server,
            session=session,
            wrapper=wrapper,
            measurements=measurements,
            primary_error=sys.exception(),
            query_worker=worker if worker_started else None,
        )
    return report


def sha256_file(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--documents", type=int, default=100000)
    parser.add_argument("--limits", type=int, nargs="+", default=[256, 1024])
    parser.add_argument("--data", type=Path)
    parser.add_argument("--steady-seconds", type=float, default=10)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (
        args.documents < 256
        or not math.isfinite(args.steady_seconds)
        or args.steady_seconds < 1
        or any(v < 256 for v in args.limits)
        or len(set(args.limits)) != len(args.limits)
    ):
        parser.error("require >=256 documents, >=1 steady second and soft limits >=256")
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "schema_version": 2,
        "captured_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "client_nofile": resource.getrlimit(resource.RLIMIT_NOFILE),
        "platform": platform.platform(),
        "python_version": sys.version,
        "qualifier_sha256": sha256_file(Path(__file__)),
        "binary": str(args.binary.resolve()),
        "binary_sha256": sha256_file(args.binary),
        "scope": "Whole initial and recovered server lifetimes, launch through shutdown; warm OS cache; one concurrent self-query worker with 20ms think time; boundary-crossing queries reported separately; empty query phases are null; end-to-end client ingestion; sampled RSS/FD peaks are lower bounds; no hosted inference; sequential full_index batches; separate fresh databases per limit.",
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    for limit in args.limits:
        qualify(
            args.binary.resolve(),
            limit,
            args.documents,
            args.data,
            args.output,
            args.steady_seconds,
        )


if __name__ == "__main__":
    main()
