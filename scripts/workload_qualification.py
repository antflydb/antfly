"""Retained process/Docker scheduling experiments; never claims the full release matrix.

Create a plan with `template`, edit both pinned arms/configs, then `run PLAN --output DIR`.
Only fresh local data directories/containers are used. No external cluster is touched.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import itertools
import json
import math
import os
import platform
import re
import shutil
import socket
import subprocess
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import workload_vector_qualification as vectors

TIERS = {
    "starter": (1, 4 << 30, 50 << 30),
    "standard": (2, 4 << 30, 100 << 30),
    "pro": (4, 8 << 30, 200 << 30),
}
GATES = {
    "low_load_p99_ratio": 1.10,
    "low_load_p99_add_ms": 1.0,
    "throughput_ratio": 0.95,
    "cgroup_peak_ratio": 0.90,
}
RELEASE_BASELINE = "64f1afbb373d5da0a932f08e456116da139e9e9a"
TABLE = "workload_scheduling_fixture"


def save(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def command(
    argv: list[str], *, timeout: float = 30, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=check,
    )


def template(runtime: str) -> dict[str, Any]:
    arm = {
        "revision": "REPLACE_WITH_FULL_GIT_SHA",
        "optimization": "Debug",
        "config": {
            "admission": {
                "query": {"max_concurrent_requests": 80},
                "write": {"max_concurrent_requests": 80},
            }
        },
    }
    arm["binary" if runtime == "process" else "image"] = (
        "/absolute/path/to/antfly"
        if runtime == "process"
        else "antfly@sha256:REPLACE_WITH_IMAGE_DIGEST"
    )
    return {
        "schema": 1,
        "purpose": "smoke",
        "runtime": runtime,
        "tier": "starter",
        "storage_description": "REPLACE_WITH_ACTUAL_HOST_DISK_AND_FILESYSTEM",
        "warmup_seconds": 2,
        "seconds": 5,
        "runs": 1,
        "concurrency": [1, 5],
        "open_factors": [0.5, 1.25, 2.0],
        "overload_seconds": 2,
        "recovery_seconds": 2,
        "request_timeout": 5,
        "generator_workers": 80,
        "generator_queue": 160,
        "documents": 128,
        "workloads": [
            {"name": "lookup", "read_percent": 100, "query_percent_of_reads": 0},
            {"name": "mixed90", "read_percent": 90, "query_percent_of_reads": 50},
            {"name": "mixed50", "read_percent": 50, "query_percent_of_reads": 50},
        ],
        "arms": {
            "baseline": json.loads(json.dumps(arm)),
            "candidate": json.loads(json.dumps(arm)),
        },
        "note": "Smoke template. Release-sized rows use 60s warmup, 300s measurement, three runs, C1/5/10/20/30/40/60/80 and open factors .5/.8/1/1.25/2. Other release workloads remain separate and required.",
    }


def release_plan(
    tier: str,
    baseline_image: str | None = None,
    candidate_image: str | None = None,
    candidate_revision: str | None = None,
) -> dict[str, Any]:
    plan = template("docker")
    _, memory, _ = TIERS[tier]
    waiting = {
        "max_queued_requests": 160,
        "max_queued_bytes": memory // 64,
        "max_retained_bytes": memory // 8,
        "max_wait_ms": 1000,
    }
    fixed_config = {
        "admission": {
            kind: {"max_concurrent_requests": 80, "waiting": waiting.copy()}
            for kind in ("query", "write")
        }
    }
    plan.update(
        purpose="qualification",
        tier=tier,
        warmup_seconds=60,
        seconds=300,
        runs=3,
        concurrency=[1, 5, 10, 20, 30, 40, 60, 80],
        open_factors=[0.5, 0.8, 1.0, 1.25, 2.0],
        overload_seconds=60,
        recovery_seconds=60,
        request_timeout=30,
        generator_workers=160,
        generator_queue=320,
        documents=4096,
    )
    plan["arms"] = {
        "baseline": {
            "revision": RELEASE_BASELINE,
            "optimization": "ReleaseFast",
            "image": baseline_image,
            "config": json.loads(json.dumps(fixed_config)),
        },
        "candidate": {
            "revision": candidate_revision,
            "optimization": "ReleaseFast",
            "image": candidate_image,
            "config": json.loads(json.dumps(fixed_config)),
        },
    }
    plan["note"] = (
        "Prepared fixed-policy single-node lookup/mixed workload subset. Null image/candidate revision fields are deliberately unresolved until builds from the final committed sources exist. Both arms use identical fixed admission; this isolates implementation overhead. This plan does not qualify lane isolation, dense/graph/aggregate work, remote ownership or write recovery. Record actual storage class and retain build receipts before running. Untouched legacy-default UX comparisons require a separate plan."
    )
    return plan


def validate(plan: dict[str, Any]) -> None:
    if (
        plan.get("schema") != 1
        or plan.get("runtime") not in {"process", "docker"}
        or plan.get("tier") not in TIERS
    ):
        raise ValueError("invalid plan schema/runtime/tier")
    for key in (
        "warmup_seconds",
        "seconds",
        "runs",
        "overload_seconds",
        "recovery_seconds",
        "request_timeout",
        "generator_workers",
        "documents",
    ):
        value = plan.get(key)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{key} must be finite and positive")
    for key in ("runs", "generator_workers", "documents", "generator_queue"):
        if type(plan.get(key)) is not int or plan[key] < (
            0 if key == "generator_queue" else 1
        ):
            raise ValueError(f"{key} must be a bounded integer")
    if (
        plan["generator_workers"] > 256
        or plan["generator_queue"] > 4096
        or plan["documents"] > 100000
    ):
        raise ValueError(
            "generator or fixture exceeds this harness's bounded small-workload scope"
        )
    if not plan.get("concurrency") or any(
        type(c) is not int or not 1 <= c <= plan["generator_workers"]
        for c in plan["concurrency"]
    ):
        raise ValueError("concurrency must fit generator_workers")
    if not plan.get("open_factors") or any(
        not isinstance(f, (int, float)) or not math.isfinite(f) or f <= 0
        for f in plan["open_factors"]
    ):
        raise ValueError("open_factors must be finite and positive")
    if set(plan.get("arms", {})) != {"baseline", "candidate"}:
        raise ValueError("exactly baseline and candidate arms are required")
    modes = set()
    for arm in plan["arms"].values():
        if not re.fullmatch(r"[0-9a-f]{40}", arm.get("revision") or "") and not (
            plan.get("purpose") == "smoke" and arm.get("revision") == "unknown"
        ):
            raise ValueError("pin each declared source revision with a full Git SHA")
        modes.add(arm.get("optimization"))
        if not isinstance(arm.get("config"), dict):
            raise TypeError("each arm needs an explicit config object")
        if plan["runtime"] == "process" and not Path(arm.get("binary", "")).is_file():
            raise ValueError("process binary does not exist")
        if plan["runtime"] == "docker" and not arm.get("image"):
            raise ValueError(
                "Docker image is required; the resolved image ID will be retained"
            )
    if len(modes) != 1 or next(iter(modes)) not in (
        {"Debug", "ReleaseSafe", "ReleaseFast", "unknown"}
        if plan.get("purpose") == "smoke"
        else {"ReleaseFast"}
    ):
        raise ValueError("baseline/candidate need the same declared optimization mode")
    if plan.get("purpose") not in {"smoke", "qualification"}:
        raise ValueError("purpose must be smoke or qualification")
    if plan["purpose"] == "qualification" and (
        plan["runtime"] != "docker"
        or plan["warmup_seconds"] < 60
        or plan["seconds"] < 300
        or plan["runs"] < 3
        or plan["overload_seconds"] < 60
        or plan["recovery_seconds"] < 60
        or set(plan["concurrency"]) != {1, 5, 10, 20, 30, 40, 60, 80}
        or set(plan["open_factors"]) != {0.5, 0.8, 1, 1.25, 2}
    ):
        raise ValueError(
            "qualification needs Docker resource limits and the full release-sized timing/concurrency/rate schedule"
        )
    if not plan.get("workloads"):
        raise ValueError("at least one workload is required")
    names = set()
    for workload in plan["workloads"]:
        if (
            not re.fullmatch(r"[a-z][a-z0-9_-]*", workload.get("name", ""))
            or workload["name"] in names
        ):
            raise ValueError("workload names must be unique safe filenames")
        names.add(workload["name"])
        if workload.get("kind") == "vector":
            if "vector" not in plan:
                raise ValueError(
                    "vector workloads require an explicit fixture specification"
                )
            continue
        for field in ("read_percent", "query_percent_of_reads"):
            if type(workload.get(field)) is not int or not 0 <= workload[field] <= 100:
                raise ValueError(f"{field} must be an integer percentage")
    if "vector" in plan:
        vectors.validate(
            plan["vector"], qualification=plan["purpose"] == "qualification"
        )


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class HTTP:
    def __init__(self, port: int, timeout: float):
        self.connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)

    def close(self) -> None:
        self.connection.close()

    def request(
        self, method: str, path: str, value: Any = None
    ) -> tuple[int, bytes, dict[str, str]]:
        body = (
            None if value is None else json.dumps(value, separators=(",", ":")).encode()
        )
        try:
            self.connection.request(
                method, path, body, {"Content-Type": "application/json"}
            )
            response = self.connection.getresponse()
            data = response.read((1 << 20) + 1)
            if len(data) > 1 << 20:
                raise ValueError("response exceeded harness 1 MiB bound")
            return response.status, data, dict(response.getheaders())
        except BaseException:
            self.close()
            raise


def fixture_document(index: int) -> dict[str, Any]:
    return {
        "marker": f"fixture-{index}",
        "body": f"workload bucket{index % 16}",
        "ordinal": index,
    }


def seed(port: int, documents: int, directory: Path) -> None:
    client = HTTP(port, 30)
    receipts = []
    try:
        status, body, _ = client.request(
            "POST", f"/db/v1/tables/{TABLE}", {"num_shards": 1}
        )
        receipts.append(
            {
                "operation": "create",
                "status": status,
                "body": body.decode(errors="replace"),
            }
        )
        if status not in {200, 201, 202}:
            raise RuntimeError(f"fixture create failed: {status} {body!r}")
        for first in range(0, documents, 128):
            values = {
                f"doc{index}": fixture_document(index)
                for index in range(first, min(first + 128, documents))
            }
            status, body, _ = client.request(
                "POST",
                f"/db/v1/tables/{TABLE}/batch",
                {"inserts": values, "sync_level": "full_index"},
            )
            receipts.append(
                {
                    "operation": "seed",
                    "first": first,
                    "count": len(values),
                    "status": status,
                    "body": body.decode(errors="replace"),
                }
            )
            if status not in {200, 201}:
                raise RuntimeError(
                    f"fixture seed failed (not replayed): {status} {body!r}"
                )
        # A small corpus is verified completely before performance measurement.
        for index in range(documents):
            status, body, _ = client.request(
                "GET", f"/db/v1/tables/{TABLE}/documents/doc{index}"
            )
            if status != 200 or json.loads(body).get("marker") != f"fixture-{index}":
                raise RuntimeError(
                    f"fixture lookup mismatch at {index}: {status} {body!r}"
                )
        receipts.append(
            {"operation": "verify_all_documents", "count": documents, "passed": True}
        )
    finally:
        client.close()
        save(directory / "fixture.json", receipts)


def operation(
    sequence: int, workload: dict[str, Any], documents: int
) -> tuple[str, str, str, Any, int]:
    # Independent permutations give exact long-run offered mixes, independent of
    # completion rates. Updates rewrite identical values so expected reads stay fixed.
    index = sequence % documents
    if workload.get("kind") == "vector":
        fixture = workload["_fixture"]
        query_id = (
            fixture.spec["calibration_queries"]
            + sequence % fixture.spec["held_out_queries"]
        )
        return (
            "vector",
            "POST",
            f"/db/v1/tables/{vectors.TABLE}/query",
            fixture.query(query_id, workload["_effort"]),
            query_id,
        )
    if sequence % 100 >= workload["read_percent"]:
        return (
            "write",
            "POST",
            f"/db/v1/tables/{TABLE}/batch",
            {"inserts": {f"doc{index}": fixture_document(index)}},
            index,
        )
    read_sequence = sequence // 100 * workload["read_percent"] + sequence % 100
    if (read_sequence * 37) % 100 < workload["query_percent_of_reads"]:
        return (
            "query",
            "POST",
            f"/db/v1/tables/{TABLE}/query",
            {"full_text_search": {"match_all": {}}, "fields": [], "limit": 10},
            index,
        )
    return "lookup", "GET", f"/db/v1/tables/{TABLE}/documents/doc{index}", None, index


def classify(kind: str, index: int, status: int, data: bytes, documents: int) -> str:
    if status == 429:
        return "rejected"
    if status >= 400:
        return "unexpected_http_error"
    try:
        value = json.loads(data)
        if kind == "lookup":
            valid = status == 200 and value.get("marker") == f"fixture-{index}"
        elif kind == "query":
            rows = value["responses"]
            hits = rows[0]["hits"]["hits"]
            ids = [hit["_id"] for hit in hits]
            valid = (
                status == 200
                and len(rows) == 1
                and len(ids) == min(10, documents)
                and len(set(ids)) == len(ids)
                and all(
                    re.fullmatch(r"doc\d+", key) and int(key[3:]) < documents
                    for key in ids
                )
                and not rows[0].get("error")
            )
        else:
            valid = status in {200, 201} and not value.get("error")
        return "completed" if valid else "invalid_result"
    except (ValueError, TypeError, KeyError, IndexError, AttributeError):
        return "invalid_result"


def percentiles(values: list[float]) -> dict[str, float | None]:
    ordered = sorted(values)
    return {
        f"p{p}_ms": ordered[
            min(len(ordered) - 1, math.ceil(len(ordered) * p / 100) - 1)
        ]
        if ordered
        else None
        for p in (50, 95, 99)
    }


def run_load(
    port: int,
    workload: dict[str, Any],
    plan: dict[str, Any],
    path: Path,
    *,
    seconds: float,
    concurrency: int | None = None,
    rate: float | None = None,
    rate_schedule: list[tuple[str, float, float]] | None = None,
) -> dict[str, Any]:
    if rate is not None and (
        not math.isfinite(rate) or rate <= 0 or rate * seconds > 5_000_000
    ):
        raise ValueError(
            "offered arrivals exceed this bounded harness's 5-million samples per point limit"
        )
    if rate_schedule is not None and (
        not math.isclose(sum(duration for _, duration, _ in rate_schedule), seconds)
        or any(
            not math.isfinite(arrivals) or arrivals <= 0 or duration <= 0
            for _, duration, arrivals in rate_schedule
        )
        or sum(duration * arrivals for _, duration, arrivals in rate_schedule)
        > 5_000_000
    ):
        raise ValueError("invalid or oversized continuous arrival schedule")
    open_loop = rate is not None or rate_schedule is not None
    workers = concurrency or plan["generator_workers"]
    slots = threading.BoundedSemaphore(
        workers + (plan["generator_queue"] if open_loop else 0)
    )
    lock = threading.Lock()
    counts: Counter[str] = Counter()
    class_counts: dict[str, Counter[str]] = defaultdict(Counter)
    latencies: dict[str, list[float]] = defaultdict(list)
    windows: dict[int, Counter[str]] = defaultdict(Counter)
    clients: list[HTTP] = []
    local = threading.local()
    counter = itertools.count()
    started = time.monotonic()
    end = started + seconds
    completed_in_window = 0
    vector_matches = 0
    vector_completed = 0
    peak_outstanding = 0
    outstanding = 0
    phases = rate_schedule or [("measurement", seconds, rate or 0)]
    phase_counts: dict[str, Counter[str]] = defaultdict(Counter)

    with path.open("w") as raw:

        def record(sample: dict[str, Any]) -> None:
            nonlocal completed_in_window, vector_matches, vector_completed
            with lock:
                phase_counts[sample.get("arrival_phase", "measurement")][
                    sample["outcome"]
                ] += 1
                raw.write(json.dumps(sample, separators=(",", ":")) + "\n")
                outcome, kind = sample["outcome"], sample["class"]
                counts[outcome] += 1
                class_counts[kind][outcome] += 1
                if outcome == "completed":
                    if kind == "vector":
                        vector_matches += sample["matched_neighbors"]
                        vector_completed += 1
                    latencies[kind].append(sample["latency_ms"])
                    windows[int(sample["finished_s"] // 5)][kind] += 1
                    if sample["finished_s"] <= seconds:
                        completed_in_window += 1

        def execute(
            sequence: int, submitted: float, arrival_phase: str = "measurement"
        ) -> None:
            nonlocal outstanding
            kind, method, route, body, index = operation(
                sequence, workload, plan["documents"]
            )
            dispatched = time.monotonic()
            sample: dict[str, Any] = {
                "sequence": sequence,
                "class": kind,
                "submitted_s": submitted - started,
                "dispatch_s": dispatched - started,
                "client_wait_ms": (dispatched - submitted) * 1000,
                "retries": 0,
                "arrival_phase": arrival_phase,
            }
            try:
                remaining = submitted + plan["request_timeout"] - dispatched
                if remaining <= 0:
                    sample["outcome"] = "client_deadline_before_dispatch"
                else:
                    if not hasattr(local, "client"):
                        local.client = HTTP(port, remaining)
                        with lock:
                            clients.append(local.client)
                    connection = local.client.connection
                    connection.timeout = remaining
                    if connection.sock:
                        connection.sock.settimeout(remaining)
                    status, data, headers = local.client.request(method, route, body)
                    if kind == "vector" and status == 200:
                        sample["status"] = status
                        try:
                            result = workload["_fixture"].result(index, status, data)
                            sample.update(
                                outcome="completed",
                                query_id=index,
                                ids=result["ids"],
                                recall=result["recall"],
                                matched_neighbors=result["matched_neighbors"],
                                search_effort=workload["_effort"],
                            )
                        except (ValueError, TypeError, KeyError, IndexError):
                            sample["outcome"] = "invalid_result"
                    elif kind == "vector":
                        sample.update(
                            status=status,
                            outcome="rejected"
                            if status == 429
                            else "unexpected_http_error",
                        )
                    else:
                        sample.update(
                            status=status,
                            outcome=classify(
                                kind, index, status, data, plan["documents"]
                            ),
                        )
                    if sample["outcome"] != "completed":
                        sample.update(
                            error_body=data[:16384].decode(errors="replace"),
                            retry_after=headers.get("Retry-After"),
                        )
                    if time.monotonic() > submitted + plan["request_timeout"]:
                        sample["deadline_exceeded"] = True
                        sample["outcome"] = "late_response"
            except (OSError, http.client.HTTPException, ValueError) as error:
                sample.update(
                    outcome="unknown_write_outcome"
                    if kind == "write"
                    else "transport_error",
                    error=f"{type(error).__name__}: {error}",
                )
            finally:
                finished = time.monotonic()
                sample.update(
                    finished_s=finished - started,
                    latency_ms=(finished - submitted) * 1000,
                )
                record(sample)
                with lock:
                    outstanding -= 1
                slots.release()

        def dispatch(
            executor: ThreadPoolExecutor,
            sequence: int,
            submitted: float,
            arrival_phase: str,
        ) -> None:
            nonlocal outstanding, peak_outstanding
            kind = operation(sequence, workload, plan["documents"])[0]
            if not slots.acquire(blocking=False):
                record(
                    {
                        "sequence": sequence,
                        "class": kind,
                        "submitted_s": submitted - started,
                        "finished_s": time.monotonic() - started,
                        "latency_ms": None,
                        "outcome": "generator_dropped",
                        "retries": 0,
                        "arrival_phase": arrival_phase,
                    }
                )
                return
            with lock:
                outstanding += 1
                peak_outstanding = max(peak_outstanding, outstanding)
            executor.submit(execute, sequence, submitted, arrival_phase)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            if open_loop:
                sequence = 0
                phase_start = started
                for arrival_phase, duration, arrivals in phases:
                    phase_sequence = 0
                    while (
                        due := phase_start + phase_sequence / arrivals
                    ) < phase_start + duration:
                        time.sleep(max(0, due - time.monotonic()))
                        dispatch(executor, sequence, due, arrival_phase)
                        sequence += 1
                        phase_sequence += 1
                    # Keep the executor, connections and all outstanding work
                    # alive across pressure removal. Recovery does not wait for drain.
                    phase_start += duration
            else:

                def closed_worker() -> None:
                    nonlocal outstanding, peak_outstanding
                    while time.monotonic() < end:
                        slots.acquire()
                        with lock:
                            outstanding += 1
                            peak_outstanding = max(peak_outstanding, outstanding)
                        execute(next(counter), time.monotonic())

                futures = [executor.submit(closed_worker) for _ in range(workers)]
                for future in futures:
                    future.result()
        for client in clients:
            client.close()
    all_latencies = [value for values in latencies.values() for value in values]
    return {
        "vector": {
            "search_effort": workload["_effort"],
            "completed_queries": vector_completed,
            "recall": vector_matches
            / (vector_completed * workload["_fixture"].spec["k"])
            if vector_completed
            else None,
            "recall_floor_pass": bool(
                vector_completed
                and vector_matches / (vector_completed * workload["_fixture"].spec["k"])
                >= 0.95
            ),
        }
        if workload.get("kind") == "vector"
        else None,
        "seconds": seconds,
        "elapsed_including_drain": time.monotonic() - started,
        "offered": sum(counts.values()),
        "offered_qps": sum(counts.values()) / seconds,
        "completed_qps": completed_in_window / seconds,
        "completed_within_window": completed_in_window,
        "counts": dict(counts),
        "arrival_phases": [
            {
                "name": label,
                "seconds": duration,
                "offered_rate": arrivals,
                "counts": dict(phase_counts[label]),
            }
            for label, duration, arrivals in phases
        ],
        "success_latency": percentiles(all_latencies),
        "classes": {
            kind: {
                "counts": dict(class_counts[kind]),
                "success_latency": percentiles(latencies[kind]),
            }
            for kind in class_counts
        },
        "completion_windows_5s": {
            str(window): dict(values) for window, values in windows.items()
        },
        "generator_peak_outstanding": peak_outstanding,
        "concurrency": concurrency,
        "offered_rate": rate,
    }


def docker_command(
    plan: dict[str, Any],
    arm: dict[str, Any],
    directory: Path,
    port: int,
    name: str,
    metrics_port: int,
) -> list[str]:
    cpus, memory, _ = TIERS[plan["tier"]]
    return [
        "docker",
        "run",
        "--detach",
        "--no-healthcheck",
        "--name",
        name,
        "--cpus",
        str(cpus),
        "--memory",
        str(memory),
        "--memory-swap",
        str(memory),
        "--pids-limit",
        "512",
        "--publish",
        f"127.0.0.1:{port}:8080",
        "--publish",
        f"127.0.0.1:{metrics_port}:4200",
        "--mount",
        f"type=bind,src={directory / 'config.json'},dst=/qualification/config.json,readonly",
        "--mount",
        f"type=bind,src={directory / 'data'},dst=/qualification/data",
        "--entrypoint",
        arm.get("container_binary", "/antfly"),
        arm["image"],
        "standalone",
        "--host",
        "0.0.0.0",
        "--port",
        "8080",
        "--health",
        "true",
        "--health-port",
        "4200",
        "--data-dir",
        "/qualification/data",
        "--config",
        "/qualification/config.json",
    ]


def process_command(
    binary: Path, directory: Path, port: int, metrics_port: int
) -> list[str]:
    return [
        str(binary),
        "standalone",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--health",
        "true",
        "--health-port",
        str(metrics_port),
        "--data-dir",
        str(directory / "data"),
        "--config",
        str(directory / "config.json"),
    ]


def prometheus_error(status: int, body: bytes, headers: dict[str, str]) -> str | None:
    if status != 200:
        return f"metrics HTTP status {status}"
    content_type = (
        {key.lower(): value for key, value in headers.items()}.get("content-type", "")
        .split(";", 1)[0]
        .strip()
        .lower()
    )
    if content_type not in {"text/plain", "application/openmetrics-text"}:
        return f"unexpected metrics content type {content_type!r}"
    try:
        lines = body.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        return "metrics response is not UTF-8"
    # Validate actual samples, not just HTTP success or HELP/TYPE comments.
    sample = re.compile(
        r'[a-zA-Z_:][a-zA-Z0-9_:]*(?:\{(?:[^"\\}]|"(?:\\.|[^"\\])*")*\})?\s+(?:[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|[+-]?Inf|NaN)(?:\s+[0-9]+)?'
    )
    samples = [
        line.strip()
        for line in lines
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not samples or not all(sample.fullmatch(line) for line in samples):
        return "metrics response is not valid Prometheus sample text"
    return None


def telemetry_summary(lifecycles: list[dict[str, Any]]) -> dict[str, Any]:
    missing = []
    for index, lifecycle in enumerate(lifecycles):
        snapshots = lifecycle.get("metrics_snapshots", [])
        if not snapshots:
            missing.append(
                {"lifecycle": index, "error": "no metrics snapshots retained"}
            )
        for observation in snapshots:
            if not observation.get("metrics_valid"):
                missing.append({"lifecycle": index, **observation})
    return {
        "telemetry_complete": bool(lifecycles) and not missing,
        "telemetry_failures": missing,
        "telemetry_scope": "validated Prometheus snapshots; not a per-stage coverage or release-gate attestation",
    }


CGROUP_FILES = (
    "cpu.max",
    "cpu.stat",
    "memory.max",
    "memory.current",
    "memory.peak",
    "memory.events",
    "memory.swap.max",
    "pids.max",
    "pids.current",
)


def snapshot(
    runtime: dict[str, Any], port: int, directory: Path, label: str
) -> dict[str, Any]:
    result: dict[str, Any] = {"time": time.time()}
    if runtime.get("container"):
        for field in CGROUP_FILES:
            value = command(
                [
                    "docker",
                    "exec",
                    runtime["container"],
                    "cat",
                    f"/sys/fs/cgroup/{field}",
                ],
                check=False,
            )
            result[field] = value.stdout.strip() if value.returncode == 0 else None
    else:
        result["process_rss_kib"] = command(
            ["ps", "-o", "rss=", "-p", str(runtime["pid"])], check=False
        ).stdout.strip()
    # Public API /metrics may be a dashboard fallback with HTTP 200.
    # Only the explicitly launched health listener is a metrics source.
    del port
    result.update(metrics_port=runtime.get("metrics_port"), metrics_valid=False)
    client = None
    try:
        if result["metrics_port"] is None:
            raise ValueError("dedicated metrics port was not recorded")
        client = HTTP(result["metrics_port"], 3)
        status, body, headers = client.request("GET", "/metrics")
        result["metrics_status"] = status
        result["metrics_content_type"] = {
            key.lower(): value for key, value in headers.items()
        }.get("content-type")
        failure = prometheus_error(status, body, headers)
        result["metrics_valid"] = failure is None
        filename = (
            f"{label}.prom" if failure is None else f"{label}.metrics-invalid.body"
        )
        (directory / filename).write_bytes(body)
        result["metrics_body_file"] = filename
        if failure is not None:
            result["metrics_error"] = failure
    except (OSError, ValueError, http.client.HTTPException) as error:
        result["metrics_error"] = str(error)
    finally:
        if client is not None:
            client.close()
    runtime.setdefault("metrics_snapshots", []).append(
        {
            "label": label,
            **{
                key: value
                for key, value in result.items()
                if key.startswith("metrics_")
            },
        }
    )
    save(directory / f"{label}.resources.json", result)
    return result


def verify_cgroup(values: dict[str, Any], tier: str) -> bool:
    cpus, memory, _ = TIERS[tier]
    try:
        quota, period = values["cpu.max"].split()
        return (
            int(quota) / int(period) == cpus
            and int(values["memory.max"]) == memory
            and int(values["memory.swap.max"]) == 0
        )
    except (TypeError, ValueError, KeyError, ZeroDivisionError):
        return False


@contextmanager
def launch(plan: dict[str, Any], arm: dict[str, Any], directory: Path):
    directory.mkdir(parents=True)
    (directory / "data").mkdir(mode=0o777)
    if plan["runtime"] == "docker":
        # Image UID 10001 needs write access to this fresh, harness-owned data directory.
        (directory / "data").chmod(0o777)
    save(directory / "config.json", arm["config"])
    port = free_port()
    metrics_port = free_port()
    while metrics_port == port:
        metrics_port = free_port()
    runtime: dict[str, Any] = {
        "port": port,
        "metrics_port": metrics_port,
        "declared_revision": arm["revision"],
        "declared_optimization": arm["optimization"],
        "config_sha256": checksum(directory / "config.json"),
    }
    process = None
    log = None
    try:
        if plan["runtime"] == "docker":
            name = f"antfly-workload-{os.getpid()}-{time.time_ns()}"
            image = json.loads(
                command(["docker", "image", "inspect", arm["image"]]).stdout
            )[0]
            save(directory / "image.json", image)
            runtime["image_id"] = image["Id"]
            # Run the resolved ID so a mutable tag cannot change between inspection and dispatch.
            argv = docker_command(
                plan, {**arm, "image": image["Id"]}, directory, port, name, metrics_port
            )
            runtime["command"] = argv
            runtime["container"] = command(argv).stdout.strip()
        else:
            binary = Path(arm["binary"]).resolve()
            runtime["binary_sha256"] = checksum(binary)
            argv = process_command(binary, directory, port, metrics_port)
            runtime["command"] = argv
            log = (directory / "server.log").open("w")
            process = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT)
            runtime["pid"] = process.pid
        save(directory / "runtime.json", runtime)
        ready_by = time.monotonic() + 120
        while time.monotonic() < ready_by:
            if process is not None and process.poll() is not None:
                raise RuntimeError(f"server exited at startup: {process.returncode}")
            client = HTTP(port, 1)
            try:
                status, body, _ = client.request("GET", "/readyz")
                if status == 200 and json.loads(body).get("status") == "ready":
                    break
            except (OSError, ValueError, http.client.HTTPException):
                pass
            finally:
                client.close()
            time.sleep(0.1)
        else:
            raise RuntimeError("server did not become ready in 120 seconds")
        first = snapshot(runtime, port, directory, "ready")
        runtime["resource_limits_verified"] = bool(
            runtime.get("container")
        ) and verify_cgroup(first, plan["tier"])
        if runtime.get("container") and not runtime["resource_limits_verified"]:
            raise RuntimeError(
                "effective cgroup v2 CPU/memory/swap limits differ from plan"
            )
        yield runtime
    finally:
        stopped = time.monotonic()
        if process is not None:
            runtime["exited_before_shutdown"] = process.poll() is not None
            process.terminate()
            try:
                process.wait(timeout=30)
                runtime["forced_kill"] = False
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
                runtime["forced_kill"] = True
            runtime["exit_code"] = process.returncode
        if runtime.get("container"):
            before_stop = json.loads(
                command(["docker", "inspect", runtime["container"]]).stdout
            )[0]["State"]
            runtime["exited_before_shutdown"] = not before_stop["Running"]
            command(
                ["docker", "stop", "--time", "30", runtime["container"]],
                timeout=45,
                check=False,
            )
            state = json.loads(
                command(["docker", "inspect", runtime["container"]]).stdout
            )[0]
            save(directory / "container-final.json", state)
            runtime["exit_code"] = state["State"]["ExitCode"]
            runtime["oom_killed"] = state["State"]["OOMKilled"]
            runtime["forced_kill"] = runtime["exit_code"] == 137
            logs = command(["docker", "logs", runtime["container"]], check=False)
            (directory / "server.log").write_text(logs.stdout + logs.stderr)
            command(["docker", "rm", runtime["container"]], check=False)
        if log is not None:
            log.close()
        runtime["shutdown_seconds"] = time.monotonic() - stopped
        save(directory / "runtime.json", runtime)


def successful_baseline(point: dict[str, Any]) -> bool:
    return (
        (point.get("vector") is None or point["vector"]["recall_floor_pass"])
        and point["counts"].get("completed", 0) > 0
        and all(
            count == 0 or kind == "completed" for kind, count in point["counts"].items()
        )
    )


def compare(
    points: list[dict[str, Any]], expected_runs: int = 1
) -> list[dict[str, Any]]:
    output = []
    for workload in sorted({point["workload"] for point in points}):
        selected = [
            point
            for point in points
            if point["workload"] == workload and point["phase"] == "closed"
        ]
        arms = {
            arm: [point for point in selected if point["arm"] == arm]
            for arm in ("baseline", "candidate")
        }
        # Compare complete repeated points, not the luckiest single lifecycle.
        groups = {}
        for arm, rows in arms.items():
            by_concurrency = defaultdict(list)
            for point in rows:
                by_concurrency[point["concurrency"]].append(point)
            groups[arm] = {
                concurrency: trials
                for concurrency, trials in by_concurrency.items()
                if len(trials) == expected_runs
                and all(successful_baseline(point) for point in trials)
            }
        sustainable = {
            arm: max(
                (
                    sum(point["completed_qps"] for point in trials) / len(trials)
                    for trials in values.values()
                ),
                default=None,
            )
            for arm, values in groups.items()
        }
        low = {
            arm: max(
                (point["success_latency"]["p99_ms"] for point in values.get(1, [])),
                default=None,
            )
            for arm, values in groups.items()
        }
        valid = all(
            value is not None and value > 0 for value in sustainable.values()
        ) and all(value is not None for value in low.values())
        output.append(
            {
                "workload": workload,
                "sustainable_closed_qps": sustainable,
                "worst_c1_p99_ms": low,
                "measured_gate_pass": bool(
                    valid
                    and sustainable["candidate"]
                    >= sustainable["baseline"] * GATES["throughput_ratio"]
                    and low["candidate"]
                    <= low["baseline"] * GATES["low_load_p99_ratio"]
                    + GATES["low_load_p99_add_ms"]
                ),
                "scope": "observed closed-loop points only; no interpolation or full-release claim",
            }
        )
    return output


def freeze_artifacts(plan: dict[str, Any], output: Path) -> dict[str, Any]:
    # Freeze binaries once per arm before either lifecycle. Concurrent builds
    # cannot replace a later arm's executable underneath its recorded checksum.
    execution_plan = json.loads(json.dumps(plan))
    if plan["runtime"] == "process":
        for arm_name, arm in execution_plan["arms"].items():
            binary = output / "artifacts" / arm_name / "antfly"
            binary.parent.mkdir(parents=True)
            shutil.copy2(Path(arm["binary"]).resolve(), binary)
            arm["binary"] = str(binary)
    else:
        for arm_name, arm in execution_plan["arms"].items():
            image = json.loads(
                command(["docker", "image", "inspect", arm["image"]]).stdout
            )[0]
            save(output / f"{arm_name}-image.json", image)
            arm["image"] = image["Id"]
    save(output / "execution-plan.json", execution_plan)
    return execution_plan


def outcome_summary(
    purpose: str,
    observations: list[dict[str, Any]],
    lifecycles: list[dict[str, Any]],
    error: str | None,
) -> dict[str, Any]:
    """Fail closed on incorrect results without mislabeling generator pressure."""
    failures = []
    generator_valid = True
    generator_outcomes = {"generator_dropped", "client_deadline_before_dispatch"}
    for point in observations:
        counts = point["counts"]
        generator_valid &= not any(counts.get(kind, 0) for kind in generator_outcomes)
        unexpected = {
            kind: count
            for kind, count in counts.items()
            if count and kind not in {"completed", "rejected", *generator_outcomes}
        }
        vector_failed = (
            point.get("vector") is not None and not point["vector"]["recall_floor_pass"]
        )
        if unexpected or vector_failed:
            failures.append(
                {
                    "source": "requests",
                    **{
                        key: point.get(key)
                        for key in ("arm", "trial", "workload", "phase")
                    },
                    "outcomes": unexpected,
                    "recall_floor_failed": vector_failed,
                }
            )
    shutdown_clean = bool(lifecycles)
    for index, lifecycle in enumerate(lifecycles):
        clean = (
            lifecycle.get("exit_code") == 0
            and not lifecycle.get("forced_kill")
            and not lifecycle.get("oom_killed")
            and not lifecycle.get("exited_before_shutdown")
        )
        shutdown_clean &= clean
        if not clean:
            failures.append(
                {
                    "source": "runtime",
                    "lifecycle": index,
                    **{
                        key: lifecycle.get(key)
                        for key in (
                            "exit_code",
                            "forced_kill",
                            "oom_killed",
                            "exited_before_shutdown",
                        )
                    },
                }
            )
    if not observations or not lifecycles:
        failures.append({"source": "incomplete_experiment"})
    correctness_passed = not error and not failures
    status = (
        "experiment_failed"
        if not correctness_passed
        else "generator_invalid"
        if not generator_valid
        else "smoke_evidence"
        if purpose == "smoke"
        else "partial_matrix_evidence"
    )
    return {
        "status": status,
        "exit_code": 1 if not correctness_passed else 2 if not generator_valid else 0,
        "correctness_passed": correctness_passed,
        "correctness_failures": failures,
        "generator_valid": generator_valid,
        "shutdown_clean": shutdown_clean,
    }


def run(plan: dict[str, Any], output: Path) -> dict[str, Any]:
    validate(plan)
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    save(output / "plan.json", plan)
    shutil.copy2(__file__, output / "workload_qualification.py")
    shutil.copy2(vectors.__file__, output / "workload_vector_qualification.py")
    save(
        output / "host.json",
        {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logical_cpus": os.cpu_count(),
            "tier_requested": plan["tier"],
            "tier_envelope": TIERS[plan["tier"]],
            "disk_enforced": False,
            "storage_description": plan.get("storage_description"),
            "started_at": time.time(),
            "gate_policy": GATES,
            "build_provenance": "declared revision/mode; artifact digests recorded, not independent build attestation",
        },
    )
    points: list[dict[str, Any]] = []
    lifecycles: list[dict[str, Any]] = []
    warmups: list[dict[str, Any]] = []
    error = None
    baseline_rates: dict[str, float] = {}
    vector_fixture = None
    try:
        if "vector" in plan:
            vector_fixture = vectors.Fixture(plan["vector"])
            vector_fixture.retain(output)
        execution_plan = freeze_artifacts(plan, output)
        # The first baseline establishes offered rates. Subsequent pairs alternate
        # order to expose host drift while retaining identical fixed offered rates.
        for trial in range(plan["runs"]):
            for arm_name in (
                ("baseline", "candidate")
                if trial % 2 == 0
                else ("candidate", "baseline")
            ):
                directory = output / f"trial-{trial}" / arm_name
                print(f"starting trial={trial} arm={arm_name}", flush=True)
                with launch(
                    execution_plan, execution_plan["arms"][arm_name], directory
                ) as runtime:
                    lifecycles.append(runtime)
                    port = runtime["port"]
                    if any(
                        workload.get("kind") != "vector"
                        for workload in plan["workloads"]
                    ):
                        seed(port, plan["documents"], directory)
                    vector_effort = None
                    if vector_fixture is not None:
                        vectors.seed(HTTP, port, vector_fixture, directory)
                        vector_effort = vectors.calibrate(
                            HTTP,
                            port,
                            vector_fixture,
                            directory,
                            plan["request_timeout"],
                        )
                    for configured_workload in plan["workloads"]:
                        workload = configured_workload.copy()
                        if workload.get("kind") == "vector":
                            workload.update(
                                _fixture=vector_fixture, _effort=vector_effort
                            )
                        name = workload["name"]
                        for concurrency in plan["concurrency"]:
                            prefix = f"{name}-closed-{concurrency}"
                            warmup = run_load(
                                port,
                                workload,
                                plan,
                                directory / f"{prefix}-warmup.jsonl",
                                seconds=plan["warmup_seconds"],
                                concurrency=concurrency,
                            )
                            warmup.update(
                                arm=arm_name,
                                trial=trial,
                                workload=name,
                                phase=prefix + "-warmup",
                            )
                            warmups.append(warmup)
                            save(output / "warmups.json", warmups)
                            snapshot(runtime, port, directory, prefix + "-before")
                            point = run_load(
                                port,
                                workload,
                                plan,
                                directory / f"{prefix}.jsonl",
                                seconds=plan["seconds"],
                                concurrency=concurrency,
                            )
                            point.update(
                                arm=arm_name,
                                trial=trial,
                                workload=name,
                                phase="closed",
                                resources=snapshot(
                                    runtime, port, directory, prefix + "-after"
                                ),
                            )
                            points.append(point)
                            save(output / "points.json", points)
                            print(
                                f"  {prefix}: completed={point['completed_qps']:.1f}/s counts={point['counts']}",
                                flush=True,
                            )
                        if arm_name == "baseline" and name not in baseline_rates:
                            baseline_rates[name] = max(
                                (
                                    point["completed_qps"]
                                    for point in points
                                    if point["arm"] == "baseline"
                                    and point["workload"] == name
                                    and point["phase"] == "closed"
                                    and successful_baseline(point)
                                ),
                                default=0,
                            )
                            if baseline_rates[name] <= 0:
                                raise RuntimeError(
                                    f"no error-free baseline rate for {name}; open-loop comparison withheld"
                                )
                        rate = baseline_rates[name]
                        for factor in plan["open_factors"]:
                            prefix = f"{name}-open-{factor}"
                            warmup = run_load(
                                port,
                                workload,
                                plan,
                                directory / f"{prefix}-warmup.jsonl",
                                seconds=plan["warmup_seconds"],
                                rate=rate * factor,
                            )
                            warmup.update(
                                arm=arm_name,
                                trial=trial,
                                workload=name,
                                phase=prefix + "-warmup",
                            )
                            warmups.append(warmup)
                            save(output / "warmups.json", warmups)
                            point = run_load(
                                port,
                                workload,
                                plan,
                                directory / f"{prefix}.jsonl",
                                seconds=plan["seconds"],
                                rate=rate * factor,
                            )
                            point.update(
                                arm=arm_name,
                                trial=trial,
                                workload=name,
                                phase="open",
                                factor=factor,
                                resources=snapshot(
                                    runtime, port, directory, prefix + "-after"
                                ),
                            )
                            points.append(point)
                            save(output / "points.json", points)
                        point = run_load(
                            port,
                            workload,
                            plan,
                            directory / f"{name}-overload-recovery.jsonl",
                            seconds=plan["overload_seconds"] + plan["recovery_seconds"],
                            rate_schedule=[
                                ("overload", plan["overload_seconds"], rate * 2),
                                ("recovery", plan["recovery_seconds"], rate * 0.5),
                            ],
                        )
                        point.update(
                            arm=arm_name,
                            trial=trial,
                            workload=name,
                            phase="overload_recovery",
                            resources=snapshot(
                                runtime,
                                port,
                                directory,
                                f"{name}-overload-recovery-after",
                            ),
                        )
                        points.append(point)
                    save(output / "points.json", points)
        if vector_fixture is not None:
            vector_fixture.verify_files()
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
        raise
    finally:
        summary = {
            **outcome_summary(plan["purpose"], points + warmups, lifecycles, error),
            "release_qualified": False,
            "error": error,
            "comparisons": compare(points, plan["runs"])
            if plan["purpose"] == "qualification"
            else [],
            "lifecycles": lifecycles,
            "unmeasured_gates": [
                "vector recall/calibration"
                if vector_fixture is None
                else "retained vector datasets and cold storage"
                if vector_fixture.spec["source"] == "deterministic_cosine"
                else "cold vector storage",
                "graph/aggregation/scan isolation",
                "slow output and inference",
                "per-stage ownership ceilings",
                "distributed attempts/fencing",
                "durable recovery",
                "exact 10-second queue/recovery gate",
                "cold storage",
                "automatic/adaptive policy",
            ],
            "runtime_resource_envelope_verified": bool(lifecycles)
            and all(
                lifecycle.get("resource_limits_verified") for lifecycle in lifecycles
            ),
        }
        summary.update(telemetry_summary(lifecycles))
        if not summary["telemetry_complete"]:
            summary["unmeasured_gates"].append("Prometheus telemetry snapshots")
            if summary["exit_code"] == 0:
                summary.update(status="telemetry_unavailable", exit_code=3)
        save(output / "summary.json", summary)
        files = sorted(
            path
            for path in output.rglob("*")
            if path.is_file()
            and "data" not in path.relative_to(output).parts
            and path.name != "checksums.json"
        )
        save(
            output / "checksums.json",
            {str(path.relative_to(output)): checksum(path) for path in files},
        )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("template")
    create.add_argument("--runtime", choices=("process", "docker"), default="process")
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--vector", action="store_true")
    execute = sub.add_parser("run")
    execute.add_argument("plan", type=Path)
    execute.add_argument("--output", type=Path, required=True)
    releases = sub.add_parser("release-plans")
    releases.add_argument("--output", type=Path, required=True)
    releases.add_argument("--baseline-image")
    releases.add_argument("--candidate-image")
    releases.add_argument("--candidate-revision")
    releases.add_argument("--vectors", action="store_true")
    args = parser.parse_args()
    if args.command == "template":
        if args.output.exists():
            parser.error("template output already exists")
        plan = template(args.runtime)
        if args.vector:
            plan.update(
                vector=vectors.specification(),
                workloads=[{"name": "vector", "kind": "vector"}],
            )
        save(args.output, plan)
    elif args.command == "release-plans":
        args.output.mkdir(parents=True, exist_ok=False)
        for tier in TIERS:
            if args.vectors:
                for rows, dimensions in ((50_000, 1536), (1_000_000, 768)):
                    plan = release_plan(
                        tier,
                        args.baseline_image,
                        args.candidate_image,
                        args.candidate_revision,
                    )
                    plan.update(
                        vector=vectors.specification(rows, dimensions),
                        workloads=[{"name": "vector", "kind": "vector"}],
                    )
                    plan["note"] = (
                        "Prepared retained vector workload specification, not executed. Pin the original dataset file hashes/provenance and final ReleaseFast images before running. Each fresh lifecycle independently calibrates repeated measured throughput at 95% recall, verifies held-out queries, and freezes effort. Warm runs only; cold-storage and full release matrix remain separate."
                    )
                    save(args.output / f"{tier}-{rows}x{dimensions}.json", plan)
                continue
            save(
                args.output / f"{tier}.json",
                release_plan(
                    tier,
                    args.baseline_image,
                    args.candidate_image,
                    args.candidate_revision,
                ),
            )
    else:
        result = run(json.loads(args.plan.read_text()), args.output)
        print(
            json.dumps(
                {
                    key: result[key]
                    for key in (
                        "status",
                        "exit_code",
                        "correctness_passed",
                        "release_qualified",
                        "generator_valid",
                        "shutdown_clean",
                    )
                },
                indent=2,
            )
        )
        return result["exit_code"]
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
