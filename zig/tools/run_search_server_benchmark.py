#!/usr/bin/env python3
"""Persistent-HTTP product benchmark with open-loop load and freshness probes.

Request templates are JSON files with `method`, `path`, optional `headers`, and
optional `body`.  Strings in freshness templates may contain `{marker}`.
This runner deliberately benchmarks one declared durability configuration at a
time so differently durable products cannot be combined under one label.
"""

from __future__ import annotations

import argparse
import http.client
import json
import math
import os
import queue
import shlex
import ssl
import statistics
import struct
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import urlopen


def parse_csv_int(raw: str) -> list[int]:
    values = [int(item) for item in raw.split(",")]
    if not values or any(value <= 0 for value in values):
        raise argparse.ArgumentTypeError("expected positive comma-separated integers")
    return values


def parse_csv_float(raw: str) -> list[float]:
    values = [float(item) for item in raw.split(",")]
    if not values or any(value <= 0 for value in values):
        raise argparse.ArgumentTypeError("expected positive comma-separated numbers")
    return values


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--search-template", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--concurrency", type=parse_csv_int, default=parse_csv_int("1,2,4,8,16,32,64"))
    parser.add_argument("--offered-rps", type=parse_csv_float, required=True, help="one value or one per concurrency point")
    parser.add_argument("--duration-seconds", type=float, default=30)
    parser.add_argument("--warmup-seconds", type=float, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=10)
    parser.add_argument("--write-template", type=Path)
    parser.add_argument("--write-fraction", type=float, default=0)
    parser.add_argument("--freshness-write-template", type=Path)
    parser.add_argument("--freshness-search-template", type=Path)
    parser.add_argument("--freshness-probes", type=int, default=0)
    parser.add_argument("--freshness-poll-ms", type=float, default=10)
    parser.add_argument("--durability-profile", required=True, choices=("unsafe-throughput", "process-durable", "machine-durable"))
    parser.add_argument("--durability-config", required=True, type=Path)
    parser.add_argument("--readiness-template", type=Path)
    parser.add_argument("--graceful-restart-command")
    parser.add_argument("--crash-restart-command")
    parser.add_argument("--server-pid", type=int)
    parser.add_argument("--docker-container", help="sample server RSS/CPU with docker stats")
    parser.add_argument("--server-metrics-url", help="sample Antfly Prometheus metrics for physical-footprint and allocator peaks")
    parser.add_argument("--server-data-dir", type=Path)
    parser.add_argument("--index-command", help="optional corpus load command, timed before warmup")
    parser.add_argument("--index-seconds", type=float, help="record a previously completed reusable index load")
    parser.add_argument("--indexed-documents", type=int)
    args = parser.parse_args()
    if len(args.offered_rps) not in (1, len(args.concurrency)):
        parser.error("--offered-rps must contain one value or one per concurrency point")
    if not (0 <= args.write_fraction < 1):
        parser.error("--write-fraction must be in [0,1)")
    if args.write_fraction and not args.write_template:
        parser.error("mixed workload requires --write-template")
    if args.freshness_probes and not (args.freshness_write_template and args.freshness_search_template):
        parser.error("freshness probes require write and search templates")
    if args.indexed_documents is not None and args.indexed_documents <= 0:
        parser.error("--indexed-documents must be positive")
    if args.index_seconds is not None and args.index_seconds <= 0:
        parser.error("--index-seconds must be positive")
    if args.index_command and args.index_seconds is not None:
        parser.error("--index-command and --index-seconds are mutually exclusive")
    if args.indexed_documents is not None and not (args.index_command or args.index_seconds is not None):
        parser.error("--indexed-documents requires --index-command or --index-seconds")
    if args.server_pid is not None and args.docker_container:
        parser.error("--server-pid and --docker-container are mutually exclusive")
    return args


def load_template(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or "path" not in value:
        raise ValueError(f"invalid request template: {path}")
    return value


def substitute(value: Any, marker: str) -> Any:
    if isinstance(value, str):
        return value.replace("{marker}", marker)
    if isinstance(value, list):
        return [substitute(item, marker) for item in value]
    if isinstance(value, dict):
        return {substitute(key, marker): substitute(item, marker) for key, item in value.items()}
    return value


def template_has_marker(value: Any) -> bool:
    if isinstance(value, str):
        return "{marker}" in value
    if isinstance(value, list):
        return any(template_has_marker(item) for item in value)
    if isinstance(value, dict):
        return any(template_has_marker(item) for item in value.values())
    return False


def response_hit_count(payload: bytes) -> int | None:
    """Return the hit-array length for supported Antfly/Quickwit responses."""
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None

    hits = value.get("hits")
    if isinstance(hits, list):  # Quickwit.
        return len(hits)
    if isinstance(hits, dict) and isinstance(hits.get("hits"), list):
        return len(hits["hits"])

    responses = value.get("responses")  # Antfly multi-response envelope.
    if not isinstance(responses, list):
        return None
    count = 0
    for response in responses:
        if not isinstance(response, dict):
            return None
        response_hits = response.get("hits")
        if not isinstance(response_hits, dict) or not isinstance(response_hits.get("hits"), list):
            return None
        count += len(response_hits["hits"])
    return count


@dataclass
class Observation:
    scheduled_ns: int
    started_ns: int
    completed_ns: int
    status: int | None
    response_bytes: int
    operation: str
    error: str | None = None

    @property
    def service_ns(self) -> int:
        return self.completed_ns - self.started_ns

    @property
    def end_to_end_ns(self) -> int:
        return self.completed_ns - self.scheduled_ns


class Client:
    def __init__(self, base_url: str, timeout: float):
        parsed = urlsplit(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"invalid base URL: {base_url}")
        self.parsed = parsed
        self.timeout = timeout
        self.connection: http.client.HTTPConnection | None = None

    def connect(self) -> None:
        port = self.parsed.port or (443 if self.parsed.scheme == "https" else 80)
        if self.parsed.scheme == "https":
            self.connection = http.client.HTTPSConnection(self.parsed.hostname, port, timeout=self.timeout, context=ssl.create_default_context())
        else:
            self.connection = http.client.HTTPConnection(self.parsed.hostname, port, timeout=self.timeout)

    def request(self, template: dict[str, Any], scheduled_ns: int, operation: str) -> Observation:
        if self.connection is None:
            self.connect()
        if "raw_body" in template:
            body = str(template["raw_body"]).encode()
        else:
            body_value = template.get("body")
            body = None if body_value is None else json.dumps(body_value, separators=(",", ":")).encode()
        headers = {str(k): str(v) for k, v in template.get("headers", {}).items()}
        if body is not None:
            headers.setdefault("Content-Type", "application/json" if "raw_body" not in template else "application/octet-stream")
        path = self.parsed.path.rstrip("/") + str(template["path"])
        started = time.perf_counter_ns()
        try:
            assert self.connection
            self.connection.request(str(template.get("method", "POST")), path, body=body, headers=headers)
            response = self.connection.getresponse()
            payload = response.read()
            completed = time.perf_counter_ns()
            error = None if 200 <= response.status < 300 else f"HTTP {response.status}"
            expected = template.get("expect_contains")
            if error is None and expected is not None and str(expected).encode() not in payload:
                error = "expected marker absent"
            if error is None and template.get("expect_nonempty_hits"):
                hit_count = response_hit_count(payload)
                if hit_count is None:
                    error = "unrecognized search response"
                elif hit_count == 0:
                    error = "search returned no hits"
            return Observation(scheduled_ns, started, completed, response.status, len(payload), operation, error)
        except Exception as exc:  # Preserve failures as benchmark observations.
            completed = time.perf_counter_ns()
            if self.connection:
                self.connection.close()
            self.connection = None
            return Observation(scheduled_ns, started, completed, None, 0, operation, type(exc).__name__)

    def close(self) -> None:
        if self.connection:
            self.connection.close()


def parse_byte_size(raw: str) -> int:
    value = raw.strip()
    units = {
        "B": 1,
        "kB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
    }
    for suffix in sorted(units, key=len, reverse=True):
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)].strip()) * units[suffix])
    raise ValueError(f"unsupported byte size: {raw!r}")


def parse_prometheus_metrics(payload: str) -> dict[str, int]:
    """Parse numeric Prometheus samples while preserving their label sets."""
    observed: dict[str, int] = {}
    for line in payload.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) < 2:
            continue
        key, raw_value = fields[0], fields[1]
        try:
            value = float(raw_value)
        except ValueError:
            continue
        if not math.isfinite(value):
            continue
        observed[key] = int(value)
    return observed


class ProcessSampler:
    metric_prefixes = (
        "antfly_process_",
        "antfly_resource_",
        "antfly_lsm_",
        "antfly_full_text_",
        "antfly_text_merge_",
        "antfly_async_index_",
        "antfly_data_replay_debt_",
    )

    def __init__(self, pid: int | None, docker_container: str | None = None, metrics_url: str | None = None):
        self.pid = pid
        self.docker_container = docker_container
        self.metrics_url = metrics_url
        self.peak_rss_bytes = 0
        self.cpu_percent_samples: list[float] = []
        self.metrics_latest: dict[str, int] = {}
        self.metrics_peak: dict[str, int] = {}
        self.metrics_at_peak_footprint: dict[str, int] = {}
        self.metrics_at_peak_rss: dict[str, int] = {}
        self.metrics_at_peak_malloc: dict[str, int] = {}
        self.peak_footprint_sample_seconds: float | None = None
        self.peak_rss_sample_seconds: float | None = None
        self.peak_malloc_sample_seconds: float | None = None
        self.started = time.monotonic()
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None

    @classmethod
    def diagnostic_metric(cls, key: str) -> bool:
        name = key.split("{", 1)[0]
        return name.startswith(cls.metric_prefixes)

    def observe_metrics(self, payload: str) -> None:
        observed = {
            key: value
            for key, value in parse_prometheus_metrics(payload).items()
            if self.diagnostic_metric(key)
        }
        if not observed:
            return
        self.metrics_latest = observed
        for key, value in observed.items():
            self.metrics_peak[key] = max(self.metrics_peak.get(key, value), value)

        elapsed = time.monotonic() - self.started
        footprint = observed.get("antfly_process_footprint_bytes", 0)
        previous_footprint = self.metrics_at_peak_footprint.get("antfly_process_footprint_bytes", -1)
        if footprint > previous_footprint:
            self.metrics_at_peak_footprint = observed.copy()
            self.peak_footprint_sample_seconds = elapsed

        rss = observed.get("antfly_process_resident_bytes", 0)
        previous_rss = self.metrics_at_peak_rss.get("antfly_process_resident_bytes", -1)
        if rss > previous_rss:
            self.metrics_at_peak_rss = observed.copy()
            self.peak_rss_sample_seconds = elapsed

        malloc = observed.get("antfly_process_malloc_allocated_bytes", 0)
        previous_malloc = self.metrics_at_peak_malloc.get("antfly_process_malloc_allocated_bytes", -1)
        if malloc > previous_malloc:
            self.metrics_at_peak_malloc = observed.copy()
            self.peak_malloc_sample_seconds = elapsed

    def _run(self) -> None:
        assert self.pid is not None or self.docker_container is not None or self.metrics_url is not None
        while not self.stop.wait(0.5):
            try:
                if self.docker_container:
                    raw = subprocess.check_output(
                        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}|{{.CPUPerc}}", self.docker_container],
                        text=True,
                        stderr=subprocess.DEVNULL,
                    ).strip()
                    if raw:
                        memory, cpu = raw.split("|", 1)
                        rss = parse_byte_size(memory.split("/", 1)[0])
                        self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
                        self.cpu_percent_samples.append(float(cpu.strip().removesuffix("%")))
                else:
                    raw = subprocess.check_output(
                        ["ps", "-o", "rss=,%cpu=", "-p", str(self.pid)],
                        text=True,
                        stderr=subprocess.DEVNULL,
                    ).strip()
                    if raw:
                        rss, cpu = raw.split()
                        self.peak_rss_bytes = max(self.peak_rss_bytes, int(rss) * 1024)
                        self.cpu_percent_samples.append(float(cpu))
            except (OSError, ValueError, subprocess.SubprocessError):
                pass
            if self.metrics_url:
                try:
                    with urlopen(self.metrics_url, timeout=1.0) as response:
                        payload = response.read().decode("utf-8")
                    self.observe_metrics(payload)
                except (OSError, UnicodeDecodeError, ValueError):
                    pass

    def __enter__(self) -> "ProcessSampler":
        if self.pid is not None or self.docker_container is not None or self.metrics_url is not None:
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop.set()
        if self.thread:
            self.thread.join()

    def summary(self) -> dict[str, Any]:
        return {
            "peak_rss_bytes": self.peak_rss_bytes or None,
            "cpu_percent_mean": statistics.mean(self.cpu_percent_samples) if self.cpu_percent_samples else None,
            "cpu_percent_max": max(self.cpu_percent_samples) if self.cpu_percent_samples else None,
            "metrics_latest": dict(sorted(self.metrics_latest.items())),
            "metrics_peak": dict(sorted(self.metrics_peak.items())),
            "metrics_at_peak_footprint": dict(sorted(self.metrics_at_peak_footprint.items())),
            "metrics_at_peak_rss": dict(sorted(self.metrics_at_peak_rss.items())),
            "metrics_at_peak_malloc": dict(sorted(self.metrics_at_peak_malloc.items())),
            "peak_footprint_sample_seconds": self.peak_footprint_sample_seconds,
            "peak_rss_sample_seconds": self.peak_rss_sample_seconds,
            "peak_malloc_sample_seconds": self.peak_malloc_sample_seconds,
        }


class DiskSampler:
    def __init__(self, path: Path | None, interval_seconds: float = 5.0):
        self.path = path
        self.interval_seconds = interval_seconds
        self.peak_total_bytes = 0
        self.peak_categories: dict[str, dict[str, int]] = {}
        self.peak_lsm_manifests: dict[str, dict[str, int]] = {}
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None

    def _sample(self) -> None:
        inventory = directory_inventory(self.path, limit=0)
        if inventory is None:
            return
        self.peak_total_bytes = max(self.peak_total_bytes, inventory["total_bytes"])
        for category, current in inventory["storage_categories"].items():
            peak = self.peak_categories.setdefault(category, {"files": 0, "bytes": 0})
            peak["files"] = max(peak["files"], current["files"])
            peak["bytes"] = max(peak["bytes"], current["bytes"])
        for current in inventory["lsm_manifests"]:
            peak = self.peak_lsm_manifests.setdefault(current["path"], {})
            for section in ("active", "obsolete", "physical", "untracked"):
                for field in ("files", "bytes", "missing"):
                    name = f"{section}_{field}"
                    peak[name] = max(peak.get(name, 0), current[section][field])

    def _run(self) -> None:
        while True:
            try:
                self._sample()
            except OSError:
                pass
            if self.stop.wait(self.interval_seconds):
                return

    def __enter__(self) -> "DiskSampler":
        if self.path is not None:
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop.set()
        if self.thread:
            self.thread.join()
        try:
            self._sample()
        except OSError:
            pass

    def summary(self) -> dict[str, Any] | None:
        if self.path is None:
            return None
        return {
            "peak_total_bytes": self.peak_total_bytes,
            "peak_storage_categories": dict(sorted(self.peak_categories.items())),
            "peak_lsm_manifests": dict(sorted(self.peak_lsm_manifests.items())),
            "sample_interval_seconds": self.interval_seconds,
        }


def directory_bytes(path: Path | None) -> int | None:
    if path is None or not path.exists():
        return None
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def directory_inventory(path: Path | None, limit: int = 32) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    subtree_bytes: dict[str, int] = {}
    storage_categories: dict[str, dict[str, int]] = {}
    largest_files: list[tuple[int, str]] = []
    lsm_manifest_paths: list[Path] = []
    total = 0
    for item in path.rglob("*"):
        if not item.is_file():
            continue
        size = item.stat().st_size
        total += size
        relative = item.relative_to(path)
        if item.name == "manifest.bin" and (item.parent / "runs").is_dir():
            lsm_manifest_paths.append(item)
        largest_files.append((size, str(relative)))
        category = storage_category(relative)
        category_stats = storage_categories.setdefault(category, {"files": 0, "bytes": 0})
        category_stats["files"] += 1
        category_stats["bytes"] += size
        parts = relative.parts[:-1]
        for depth in range(1, len(parts) + 1):
            subtree = str(Path(*parts[:depth]))
            subtree_bytes[subtree] = subtree_bytes.get(subtree, 0) + size
    return {
        "total_bytes": total,
        "storage_categories": dict(sorted(storage_categories.items())),
        "lsm_manifests": [
            manifest
            for manifest_path in sorted(lsm_manifest_paths)
            if (manifest := lsm_manifest_inventory(path, manifest_path)) is not None
        ],
        "largest_subtrees": [
            {"path": name, "bytes": size}
            for name, size in sorted(subtree_bytes.items(), key=lambda item: (-item[1], item[0]))[:limit]
        ],
        "largest_files": [
            {"path": name, "bytes": size}
            for size, name in sorted(largest_files, key=lambda item: (-item[0], item[1]))[:limit]
        ],
    }


def lsm_manifest_inventory(root: Path, manifest_path: Path) -> dict[str, Any] | None:
    """Decode Antfly LSM v8 run ownership without opening or mutating the store."""
    try:
        raw = manifest_path.read_bytes()
        if len(raw) < 28 or raw[:8] != b"ALSMMAN1":
            return None
        offset = 8
        version = struct.unpack_from("<I", raw, offset)[0]
        offset += 4
        if version != 8:
            return None
        next_run_id = struct.unpack_from("<Q", raw, offset)[0]
        offset += 8
        run_count, obsolete_count = struct.unpack_from("<II", raw, offset)
        offset += 8
        active_paths: list[Path] = []
        active_runs: list[dict[str, Any]] = []
        active_logical_bytes = 0
        for _ in range(run_count):
            run_id, level, size_bytes = struct.unpack_from("<QIQ", raw, offset)
            offset += 20 + 40  # id/level/size plus five compression counters
            lengths = struct.unpack_from("<IIIII", raw, offset)
            offset += 20
            entry_count = struct.unpack_from("<I", raw, offset)[0]
            offset += 4
            path_length, smallest_namespace_length, smallest_key_length, largest_namespace_length, largest_key_length = lengths
            run_path = raw[offset : offset + path_length].decode("utf-8")
            offset += path_length
            smallest_namespace = raw[offset : offset + smallest_namespace_length]
            offset += smallest_namespace_length
            smallest_key = raw[offset : offset + smallest_key_length]
            offset += smallest_key_length
            largest_namespace = raw[offset : offset + largest_namespace_length]
            offset += largest_namespace_length
            largest_key = raw[offset : offset + largest_key_length]
            offset += largest_key_length
            active_paths.append(resolve_manifest_path(manifest_path.parent, run_path))
            active_logical_bytes += size_bytes
            active_runs.append(
                {
                    "id": run_id,
                    "level": level,
                    "size_bytes": size_bytes,
                    "entry_count": entry_count,
                    "smallest_namespace_hex": smallest_namespace.hex(),
                    "smallest_key_hex": smallest_key.hex(),
                    "largest_namespace_hex": largest_namespace.hex(),
                    "largest_key_hex": largest_key.hex(),
                    "partition_prefix_equal": bool(
                        smallest_namespace == largest_namespace
                        and smallest_key
                        and largest_key
                        and smallest_key[0] == largest_key[0]
                    ),
                }
            )
        obsolete_paths: list[Path] = []
        for _ in range(obsolete_count):
            _, path_length = struct.unpack_from("<QI", raw, offset)
            offset += 12
            obsolete_path = raw[offset : offset + path_length].decode("utf-8")
            offset += path_length
            obsolete_paths.append(resolve_manifest_path(manifest_path.parent, obsolete_path))
        if offset != len(raw):
            return None
    except (OSError, UnicodeDecodeError, struct.error):
        return None

    physical_paths = list((manifest_path.parent / "runs").glob("*.tbl"))
    tracked = {str(item) for item in active_paths + obsolete_paths}
    untracked_paths = [item for item in physical_paths if str(item) not in tracked]
    active = path_stats(active_paths)
    active["logical_bytes"] = active_logical_bytes
    return {
        "path": str(manifest_path.relative_to(root)),
        "version": version,
        "next_run_id": next_run_id,
        "active": active,
        "active_runs": active_runs,
        "obsolete": path_stats(obsolete_paths),
        "physical": path_stats(physical_paths),
        "untracked": path_stats(untracked_paths),
    }


def resolve_manifest_path(lsm_root: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else lsm_root / path


def path_stats(paths: list[Path]) -> dict[str, int]:
    files = 0
    bytes_used = 0
    missing = 0
    for path in paths:
        try:
            stat = path.stat()
        except FileNotFoundError:
            missing += 1
            continue
        files += 1
        bytes_used += stat.st_size
    return {"files": files, "bytes": bytes_used, "missing": missing}


def storage_category(relative: Path) -> str:
    parts = relative.parts
    try:
        table_db = parts.index("table-db")
    except ValueError:
        return "other"
    tail = parts[table_db + 1 :]
    if not tail:
        return "primary_metadata"
    if tail[0] == "runs":
        return "primary_runs"
    if tail[0] == "wal":
        return "primary_wal"
    if tail[0] == "change_journal":
        return "change_journal"
    if tail[0] != "indexes":
        return "primary_metadata"
    if "segments" in tail:
        return "text_segments"
    if "wal" in tail:
        return "index_wal"
    if "runs" in tail:
        return "index_runs"
    return "index_metadata"


def run_timed_command(raw_command: str) -> float:
    started = time.perf_counter()
    subprocess.run(shlex.split(raw_command), check=True)
    return time.perf_counter() - started


def run_index_command(
    raw_command: str,
    server_pid: int | None,
    docker_container: str | None,
    server_data_dir: Path | None = None,
    server_metrics_url: str | None = None,
) -> tuple[float, dict[str, Any], dict[str, float]]:
    started = time.perf_counter()
    with ProcessSampler(server_pid, docker_container, server_metrics_url) as process_sampler, DiskSampler(server_data_dir) as disk_sampler:
        process = subprocess.Popen(shlex.split(raw_command))
        _, status, usage = os.wait4(process.pid, 0)
        process.returncode = os.waitstatus_to_exitcode(status)
        if process.returncode:
            raise subprocess.CalledProcessError(process.returncode, process.args)
    elapsed = time.perf_counter() - started
    loader = {
        "user_cpu_seconds": usage.ru_utime,
        "system_cpu_seconds": usage.ru_stime,
    }
    loader["cpu_seconds"] = loader["user_cpu_seconds"] + loader["system_cpu_seconds"]
    loader["cpu_utilization"] = loader["cpu_seconds"] / elapsed
    server = process_sampler.summary()
    server["disk"] = disk_sampler.summary()
    return elapsed, server, loader


def percentile(values: list[int], quantile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1)]


def summarize_operation(observations: list[Observation], duration: float, offered: float) -> dict[str, Any]:
    success = [obs for obs in observations if obs.error is None]
    end_to_end = [obs.end_to_end_ns for obs in success]
    service = [obs.service_ns for obs in success]
    queue_delay = [obs.started_ns - obs.scheduled_ns for obs in success]
    return {
        "offered_rps": offered,
        "achieved_rps": len(success) / duration,
        "measurement_elapsed_seconds": duration,
        "requests": len(observations),
        "successes": len(success),
        "errors": len(observations) - len(success),
        "response_bytes": sum(obs.response_bytes for obs in success),
        "latency_ns": {"p50": percentile(end_to_end, 0.50), "p95": percentile(end_to_end, 0.95), "p99": percentile(end_to_end, 0.99), "max": max(end_to_end) if end_to_end else None},
        "service_ns": {"p50": percentile(service, 0.50), "p95": percentile(service, 0.95), "p99": percentile(service, 0.99)},
        "queue_delay_ns": {"p50": percentile(queue_delay, 0.50), "p95": percentile(queue_delay, 0.95), "p99": percentile(queue_delay, 0.99)},
    }


def summarize(observations: list[Observation], duration: float, offered: float) -> dict[str, Any]:
    elapsed = duration
    if observations:
        elapsed = max(duration, (max(observation.completed_ns for observation in observations) - min(observation.scheduled_ns for observation in observations)) / 1_000_000_000)
    result = summarize_operation(observations, elapsed, offered)
    result["operations"] = {
        operation: summarize_operation(
            [observation for observation in observations if observation.operation == operation],
            elapsed,
            sum(observation.operation == operation for observation in observations) / elapsed,
        )
        for operation in sorted({observation.operation for observation in observations})
    }
    return result


def run_open_loop(base_url: str, timeout: float, search: dict[str, Any], write: dict[str, Any] | None, write_fraction: float, concurrency: int, offered_rps: float, duration: float) -> list[Observation]:
    jobs: queue.Queue[tuple[int, int] | None] = queue.Queue(maxsize=max(1024, concurrency * 16))
    output: list[Observation] = []
    output_lock = threading.Lock()
    write_ratio = Fraction(str(write_fraction))

    def worker() -> None:
        client = Client(base_url, timeout)
        local: list[Observation] = []
        while True:
            job = jobs.get()
            if job is None:
                break
            sequence, scheduled = job
            is_write = write is not None and (sequence * write_ratio.numerator) % write_ratio.denominator < write_ratio.numerator
            local.append(client.request(write if is_write else search, scheduled, "write" if is_write else "search"))
        client.close()
        with output_lock:
            output.extend(local)

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    for thread in threads:
        thread.start()
    start = time.perf_counter_ns()
    interval_ns = 1_000_000_000 / offered_rps
    sequence = 0
    end = start + int(duration * 1_000_000_000)
    while True:
        scheduled = start + int(sequence * interval_ns)
        if scheduled >= end:
            break
        delay = scheduled - time.perf_counter_ns()
        if delay > 0:
            time.sleep(delay / 1_000_000_000)
        jobs.put((sequence, scheduled))
        sequence += 1
    for _ in threads:
        jobs.put(None)
    for thread in threads:
        thread.join()
    output.sort(key=lambda obs: obs.scheduled_ns)
    return output


def readiness(base_url: str, timeout: float, template: dict[str, Any]) -> float:
    client = Client(base_url, timeout)
    started = time.perf_counter()
    while time.perf_counter() - started < timeout:
        observation = client.request(template, time.perf_counter_ns(), "readiness")
        if observation.error is None:
            client.close()
            return time.perf_counter() - started
        time.sleep(0.05)
    client.close()
    raise TimeoutError("server did not become ready")


def request_readiness(base_url: str, timeout: float, template: dict[str, Any], operation: str) -> tuple[float, Observation, int]:
    client = Client(base_url, timeout)
    started = time.perf_counter()
    attempts = 0
    last: Observation | None = None
    while time.perf_counter() - started < timeout:
        attempts += 1
        last = client.request(template, time.perf_counter_ns(), operation)
        if last.error is None:
            client.close()
            return time.perf_counter() - started, last, attempts
        time.sleep(0.05)
    client.close()
    error = last.error if last is not None else "no attempts"
    raise TimeoutError(f"server request did not become ready: {error}")


def freshness(args: argparse.Namespace, write_template: dict[str, Any], search_template: dict[str, Any]) -> list[int]:
    if not template_has_marker(write_template):
        raise ValueError("freshness write template must contain {marker}")
    if not template_has_marker(search_template.get("expect_contains")):
        raise ValueError("freshness search template expect_contains must contain {marker}")
    client = Client(args.base_url, args.timeout_seconds)
    results: list[int] = []
    for _ in range(args.freshness_probes):
        marker = f"antfly-benchmark-{uuid.uuid4().hex}"
        write = substitute(write_template, marker)
        search = substitute(search_template, marker)
        acknowledged = client.request(write, time.perf_counter_ns(), "freshness_write")
        if acknowledged.error:
            raise RuntimeError(f"freshness write failed: {acknowledged.error}")
        ack_ns = acknowledged.completed_ns
        deadline = time.perf_counter() + args.timeout_seconds
        while True:
            observed = client.request(search, time.perf_counter_ns(), "freshness_search")
            if observed.error is None and observed.response_bytes > 0:
                results.append(observed.completed_ns - ack_ns)
                break
            if time.perf_counter() >= deadline:
                raise TimeoutError(f"freshness marker not observed: {marker}")
            time.sleep(args.freshness_poll_ms / 1000)
    client.close()
    return results


def main() -> int:
    args = arguments()
    args.output.mkdir(parents=True, exist_ok=False)
    search = load_template(args.search_template)
    write = load_template(args.write_template) if args.write_template else None
    disk_before_inventory = directory_inventory(args.server_data_dir)
    disk_before_load = disk_before_inventory["total_bytes"] if disk_before_inventory else None
    index_server: dict[str, Any] | None = None
    index_loader: dict[str, float] | None = None
    if args.index_command:
        index_seconds, index_server, index_loader = run_index_command(
            args.index_command,
            args.server_pid,
            args.docker_container,
            args.server_data_dir,
            args.server_metrics_url,
        )
    else:
        index_seconds = args.index_seconds
    disk_after_inventory = directory_inventory(args.server_data_dir)
    disk_after_load = disk_after_inventory["total_bytes"] if disk_after_inventory else None
    # Warmup uses the first load point and is intentionally excluded.
    run_open_loop(args.base_url, args.timeout_seconds, search, write, args.write_fraction, args.concurrency[0], args.offered_rps[0], args.warmup_seconds)

    raw_file = args.output / "requests.jsonl"
    summaries: list[dict[str, Any]] = []
    with raw_file.open("w", encoding="utf-8") as raw:
        for index, concurrency in enumerate(args.concurrency):
            offered = args.offered_rps[0] if len(args.offered_rps) == 1 else args.offered_rps[index]
            client_cpu_started = time.process_time()
            with ProcessSampler(args.server_pid, args.docker_container, args.server_metrics_url) as process_sampler:
                observations = run_open_loop(args.base_url, args.timeout_seconds, search, write, args.write_fraction, concurrency, offered, args.duration_seconds)
            client_cpu_seconds = time.process_time() - client_cpu_started
            for obs in observations:
                raw.write(json.dumps({**obs.__dict__, "service_ns": obs.service_ns, "end_to_end_ns": obs.end_to_end_ns, "concurrency": concurrency, "offered_rps": offered}, sort_keys=True) + "\n")
            load_summary = summarize(observations, args.duration_seconds, offered)
            summaries.append({
                "concurrency": concurrency,
                **load_summary,
                "server": process_sampler.summary(),
                "client_cpu_seconds": client_cpu_seconds,
                "client_cpu_utilization": client_cpu_seconds / load_summary["measurement_elapsed_seconds"],
            })
    (args.output / "loads.json").write_text(json.dumps(summaries, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    freshness_values: list[int] = []
    if args.freshness_probes:
        freshness_values = freshness(args, load_template(args.freshness_write_template), load_template(args.freshness_search_template))
    (args.output / "freshness.json").write_text(json.dumps(freshness_values, indent=2) + "\n", encoding="utf-8")

    recovery: dict[str, Any] = {}
    readiness_template = load_template(args.readiness_template) if args.readiness_template else None
    for label, raw_command in (("graceful", args.graceful_restart_command), ("crash", args.crash_restart_command)):
        if raw_command:
            if readiness_template is None:
                raise ValueError("restart commands require --readiness-template")
            command_seconds = run_timed_command(raw_command)
            ready_seconds = readiness(args.base_url, args.timeout_seconds, readiness_template)
            search_ready_seconds, observation, search_attempts = request_readiness(
                args.base_url,
                args.timeout_seconds,
                search,
                f"{label}_recovery_search",
            )
            recovery[label] = {
                "command_seconds": command_seconds,
                "readiness_seconds": ready_seconds,
                "search_ready_seconds": search_ready_seconds,
                "search_attempts": search_attempts,
                "recovery_seconds": command_seconds + ready_seconds + search_ready_seconds,
                "first_search_service_ns": observation.service_ns,
                "first_search_end_to_end_ns": observation.end_to_end_ns,
                "first_search_status": observation.status,
                "first_search_error": observation.error,
            }
            (args.output / "recovery.json").write_text(json.dumps(recovery, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    durability_config = json.loads(args.durability_config.read_text(encoding="utf-8"))
    manifest = {
        "schema_version": 1,
        "benchmark": "database-server",
        "engine": args.engine,
        "base_url": args.base_url,
        "persistent_http": True,
        "load_model": "open_loop_scheduled_end_to_end_latency",
        "concurrency": args.concurrency,
        "offered_rps": args.offered_rps,
        "duration_seconds": args.duration_seconds,
        "write_fraction": args.write_fraction,
        "durability_profile": args.durability_profile,
        "durability_config": durability_config,
        "templates": {"search": search, "write": write},
        "server_pid": args.server_pid,
        "docker_container": args.docker_container,
        "server_metrics_url": args.server_metrics_url,
        "server_data_dir": str(args.server_data_dir) if args.server_data_dir else None,
        "index_command": args.index_command,
        "index_seconds": index_seconds,
        "indexed_documents": args.indexed_documents,
        "index_server": index_server,
        "index_loader": index_loader,
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    disk_end_inventory = directory_inventory(args.server_data_dir)
    result = {
        "schema_version": 1,
        "loads": summaries,
        "freshness_ns": {"samples": len(freshness_values), "p50": percentile(freshness_values, 0.50), "p95": percentile(freshness_values, 0.95), "p99": percentile(freshness_values, 0.99)},
        "recovery": recovery,
        "indexing": {
            "seconds": index_seconds,
            "documents": args.indexed_documents,
            "documents_per_second": (args.indexed_documents / index_seconds) if index_seconds and args.indexed_documents else None,
            "server": index_server,
            "loader": index_loader,
        },
        "disk_bytes": {
            "before_load": disk_before_load,
            "after_load": disk_after_load,
            "end": disk_end_inventory["total_bytes"] if disk_end_inventory else None,
        },
        "disk_inventory": {
            "before_load": disk_before_inventory,
            "after_load": disk_after_inventory,
            "end": disk_end_inventory,
        },
    }
    (args.output / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
