"""Periodic raw evidence and fail-closed fixed-policy gates (never full qualification)."""

from __future__ import annotations

import json
import hashlib
import math
import re
import threading
import time
from contextlib import contextmanager

SAMPLE = re.compile(
    r'^([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{(?:[^"\\}]|"(?:\\.|[^"\\])*")*\})?)\s+([^\s]+)(?:\s+\d+)?$'
)


def metrics(body):
    result = {}
    for line in body.decode().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        match = SAMPLE.fullmatch(line)
        if match is None:
            raise ValueError("invalid metric sample")
        name, value = match.groups()
        number = float(value)
        if not math.isfinite(number) or name in result:
            raise ValueError("nonfinite or duplicate metric sample")
        result[name] = number
    if not result:
        raise ValueError("empty metrics")
    return result


def validate(spec):
    interval = spec.get("interval_seconds", 1)
    if (
        isinstance(interval, bool)
        or not isinstance(interval, (int, float))
        or not math.isfinite(interval)
        or not 0.1 <= interval <= 1
    ):
        raise ValueError("periodic sampling interval must be 0.1..1 seconds")
    for metric, limit in spec.get("ceilings", {}).items():
        if (
            not isinstance(metric, str)
            or not isinstance(limit, (int, float))
            or not math.isfinite(limit)
            or limit < 0
        ):
            raise ValueError(
                "explicit metric ceilings must be finite nonnegative values"
            )
    bound = spec.get("recovery_queue_bound")
    if bound is not None and (
        isinstance(bound, bool)
        or not isinstance(bound, (int, float))
        or not math.isfinite(bound)
        or bound < 0
    ):
        raise ValueError("recovery_queue_bound must be finite and nonnegative")
    if not all(
        isinstance(name, str) and name for name in spec.get("queue_metrics", [])
    ):
        raise ValueError("queue_metrics must name exact exported series")


@contextmanager
def observe(http, command, runtime, path, spec):
    stop = threading.Event()
    started = time.monotonic()
    observations = []

    def capture():
        sample_start = time.monotonic()
        record = {
            "started_s": sample_start - started,
            "started_monotonic": sample_start,
        }
        client = None
        try:
            client = http(runtime["metrics_port"], 1)
            status, body, headers = client.request("GET", "/metrics")
            content_type = {key.lower(): value for key, value in headers.items()}.get(
                "content-type", ""
            )
            record["metrics_raw"] = body.decode(errors="replace")
            if status != 200 or not content_type.startswith(
                ("text/plain", "application/openmetrics-text")
            ):
                raise ValueError("dedicated listener did not return Prometheus metrics")
            record["metrics"] = metrics(body)
            received = time.monotonic()
            record["metrics_received_monotonic"] = received
            record["metrics_body_sha256"] = hashlib.sha256(body).hexdigest()
            normalized_headers = {key.lower(): value for key, value in headers.items()}
            age_header = normalized_headers.get("x-antfly-metrics-age-ms")
            collected = record["metrics"].get(
                "antfly_metrics_collected_timestamp_seconds"
            )
            if age_header is not None:
                age = float(age_header) / 1000 + received - sample_start
                record["metrics_freshness_source"] = (
                    "server cache age header plus full scrape duration"
                )
            elif collected is not None:
                age = time.time() - collected
                record["metrics_freshness_source"] = (
                    "source collection wall timestamp (same host clock required)"
                )
            else:
                age = None
                record["metrics_freshness_source"] = (
                    "unavailable; HTTP polling is not underlying sample collection"
                )
            record["metrics_age_seconds"] = age
            record["metrics_fresh"] = (
                age is not None and math.isfinite(age) and 0 <= age <= 1
            )
            if record["metrics_fresh"]:
                record["metrics_sample_monotonic"] = received - age
            if runtime.get("container"):
                result = command(
                    [
                        "docker",
                        "exec",
                        runtime["container"],
                        "cat",
                        *[
                            f"/sys/fs/cgroup/{name}"
                            for name in (
                                "memory.current",
                                "memory.peak",
                                "memory.max",
                                "memory.events",
                            )
                        ],
                    ],
                    timeout=1,
                    check=False,
                )
                record["cgroup_raw"] = result.stdout
                if result.returncode:
                    raise ValueError(
                        "cgroup sample unavailable: " + result.stderr[:1024]
                    )
                lines = result.stdout.splitlines()
                (
                    record["memory_current"],
                    record["memory_peak"],
                    record["memory_limit"],
                ) = map(int, lines[:3])
                record["memory_events"] = {
                    key: int(value)
                    for key, value in (line.split() for line in lines[3:])
                }
            else:
                record["cgroup_unavailable"] = (
                    "direct process has no enforced qualification cgroup"
                )
        except Exception as error:
            record["error"] = f"{type(error).__name__}: {error}"
        finally:
            if client is not None:
                client.close()
            record["finished_monotonic"] = time.monotonic()
            record["finished_s"] = record["finished_monotonic"] - started
        observations.append(
            {
                key: value
                for key, value in record.items()
                if key not in {"metrics_raw", "cgroup_raw"}
            }
        )
        return record

    def worker():
        with path.open("w") as output:
            while True:
                began = time.monotonic()
                output.write(json.dumps(capture(), separators=(",", ":")) + "\n")
                output.flush()
                if stop.wait(
                    max(0, spec.get("interval_seconds", 1) - (time.monotonic() - began))
                ):
                    output.write(json.dumps(capture(), separators=(",", ":")) + "\n")
                    output.flush()
                    break

    thread = threading.Thread(target=worker, name="qualification-telemetry")
    thread.start()
    try:
        yield observations
    finally:
        stop.set()
        thread.join()


def periodic_gates(observations, spec, seconds):
    failures, unavailable = [], []
    if not observations:
        return {
            "status": "unavailable",
            "unavailable": ["no periodic evidence"],
            "failures": [],
        }
    if any(row.get("error") for row in observations):
        unavailable.append("failed periodic samples")
    times = [row["finished_s"] for row in observations]
    # Includes actual polling latency and end coverage; never infer unseen peaks
    # from RSS. cgroup memory.peak itself is a kernel-retained high-water mark.
    gaps = [
        times[0],
        *[b - a for a, b in zip(times, times[1:])],
        max(0, seconds - times[-1]),
    ]
    if max(gaps) > 2:
        unavailable.append("periodic coverage gap exceeds two seconds")
    for row in observations:
        if row.get("metrics_fresh") is not True:
            unavailable.append(
                "ownership source sample freshness unavailable or older than one second"
            )
        if not row.get("memory_limit"):
            unavailable.append("cgroup memory limit/peak unavailable")
        elif row["memory_peak"] > row["memory_limit"] * 0.90:
            failures.append("cgroup peak exceeds 90% of enforced memory limit")
        if row.get("memory_events", {}).get("oom", 0) or row.get(
            "memory_events", {}
        ).get("oom_kill", 0):
            failures.append("cgroup reports OOM")
        for name, limit in spec.get("ceilings", {}).items():
            value = row.get("metrics", {}).get(name)
            if value is None:
                unavailable.append(f"missing required series {name}")
            elif value > limit:
                failures.append(f"observed ceiling exceeded: {name}")
    if not spec.get("ceilings"):
        unavailable.append("per-stage count/byte ceilings not declared")
    return {
        "status": "failed" if failures else "unavailable" if unavailable else "passed",
        "failures": sorted(set(failures)),
        "unavailable": sorted(set(unavailable)),
        "max_sample_gap_seconds": max(gaps),
        "memory_gate_status": (
            "failed"
            if any("cgroup" in failure for failure in failures)
            else (
                "unavailable"
                if any(
                    "cgroup" in missing or "failed periodic" in missing
                    for missing in unavailable
                )
                else "passed"
            )
        ),
        "ownership_source_freshness_verified": all(
            row.get("metrics_fresh") is True for row in observations
        ),
        "scope": "kernel memory peak and sampled declared series; unseen gauge excursions and exact ownership require runtime evidence",
    }


def progress_gates(samples, operations, seconds):
    """Only claim windows where backlog existed at EVERY point in the window."""
    count = int(seconds // 5)
    kinds = [
        operation["class"]
        for operation in operations
        if operation.get("stream", {}).get("mode") != "disconnect"
    ]
    coverage = {kind: [window * 5.0 for window in range(count)] for kind in kinds}
    completions = {kind: [0] * count for kind in kinds}
    for row in samples:
        kind = row["class"]
        if kind not in coverage:
            continue
        first = max(0, int(row["submitted_s"] // 5))
        last = min(count - 1, int(row["finished_s"] // 5))
        for window in range(first, last + 1):
            start, end = window * 5, (window + 1) * 5
            left, right = max(start, row["submitted_s"]), min(end, row["finished_s"])
            if left <= coverage[kind][window]:
                coverage[kind][window] = max(coverage[kind][window], right)
        finished_window = int(row["finished_s"] // 5)
        if row["outcome"] == "completed" and 0 <= finished_window < count:
            completions[kind][finished_window] += 1
    result = []
    for kind in kinds:
        eligible = [
            window
            for window in range(count)
            if coverage[kind][window] >= (window + 1) * 5
        ]
        failed = [window for window in eligible if not completions[kind][window]]
        result.append(
            {
                "class": kind,
                "client_backlogged_windows": eligible,
                "without_completion": failed,
                "status": (
                    "failed"
                    if failed
                    else "observed_progress" if eligible else "unavailable"
                ),
                "scope": "client backlog only; server continuously-eligible proof remains required",
            }
        )
    return result


def recovery_gate(
    samples, observations, spec, recovery_start, total_seconds, baseline_p99
):
    queues = spec.get("queue_metrics", [])
    queue_bound = spec.get("recovery_queue_bound")
    if not queues or queue_bound is None or baseline_p99 is None:
        return {
            "status": "unavailable",
            "reason": "need declared queue series/pre-burst bound and baseline 50%-load p99",
        }
    # Require every one-second window after t=10 to recover, not one lucky
    # completion; requests are attributed by original submission timestamps.
    limit = baseline_p99 * 1.10 + 1
    failures, unavailable = [], []
    per_second = {}
    for row in samples:
        second = int(row["submitted_s"])
        if recovery_start + 10 <= second < total_seconds:
            bucket = per_second.setdefault(second, {"latency": [], "failed": False})
            if row["outcome"] == "completed":
                bucket["latency"].append(row["latency_ms"])
            else:
                bucket["failed"] = True
    for second in range(math.ceil(recovery_start + 10), math.floor(total_seconds)):
        bucket = per_second.get(second, {"latency": [], "failed": False})
        completed = bucket["latency"]
        if not completed:
            unavailable.append(second)
            continue
        if bucket["failed"]:
            failures.append(second)
        ordered = sorted(completed)
        if ordered[math.ceil(0.99 * len(ordered)) - 1] > limit:
            failures.append(second)
        telemetry = [
            row
            for row in observations
            if row.get("metrics_fresh") is True
            and second <= row.get("metrics_sample_s", row["finished_s"]) < second + 1
        ]
        if not telemetry or any(
            any(name not in row.get("metrics", {}) for name in queues)
            for row in telemetry
        ):
            unavailable.append(second)
        elif any(
            sum(row["metrics"][name] for name in queues) > queue_bound
            for row in telemetry
        ):
            failures.append(second)
    if total_seconds <= recovery_start + 10:
        unavailable.append("window too short")
    return {
        "status": "failed" if failures else "unavailable" if unavailable else "passed",
        "failed_seconds": sorted(set(failures)),
        "unavailable_seconds": unavailable,
        "p99_limit_ms": limit,
        "queue_bound": queue_bound,
        "recovery_deadline_seconds": 10,
    }
