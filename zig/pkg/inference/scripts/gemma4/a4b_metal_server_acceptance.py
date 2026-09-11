#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Cautious live-server acceptance gate for Gemma4 26B-A4B Metal.

This is intentionally a serial, short-output gate. It preloads one immutable
streamed A4B policy, monitors macOS memory pressure/RSS/swapouts while the
server is alive, sends deterministic HTTP requests, and fails on any expert
route fallback or a regression to the old per-layer submission count.

It is not a long soak or a concurrency benchmark. Those are promotion steps
after this gate passes with comfortable resource headroom.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Any
import urllib.error
import urllib.request


SCHEMA = "antfly.gemma4_a4b_metal_server_acceptance.v1"
POLICY_ENV_PREFIXES = ("TERMITE_", "ANTFLY_GEMMA4_", "ANTFLY_INFERENCE_")
SELECTED_PAGE_LOGICAL_BYTES_PER_ATTEMPT = 26_763_264
SELECTED_PAGE_ALLOCATIONS_PER_ATTEMPT = 16
SELECTED_PAGE_MAX_MAPPED_BYTES_PER_ATTEMPT = 64 * 1024 * 1024
RUNTIME_MARKER = re.compile(
    r"gemma4_a4b_runtime:\s+mode=(auto|streamed|resident)\s+"
    r"budget_mb=(\d+)\s+kv_mb=(\d+)\s+safety_mb=(\d+)\s+"
    r"expert_slots=(\d+)\b"
)
SELECTED_PAGE_MOE_MARKER = re.compile(
    r"metal_a4b_selected_page_moe:\s+enabled=1\s+"
    r"logical_bytes=(?P<logical_bytes>\d+)\s+"
    r"mapped_page_bytes=(?P<mapped_page_bytes>\d+)\s+"
    r"allocations=(?P<allocations>\d+)\s+"
    r"persistent_cache=(?P<persistent_cache>\d+)\s+"
    r"weight_copies=(?P<weight_copies>\d+)\b"
)
TELEMETRY_MARKER = re.compile(
    r"metal_a4b_server_request:\s+scope=process_delta\s+model=.*?\s+"
    r"route_select_attempts=(?P<route_select_attempts>\d+)\s+"
    r"route_select_successes=(?P<route_select_successes>\d+)\s+"
    r"route_select_fallbacks=(?P<route_select_fallbacks>\d+)\s+"
    r"slot_lookup_attempts=(?P<slot_lookup_attempts>\d+)\s+"
    r"slot_route_hits=(?P<slot_route_hits>\d+)\s+"
    r"slot_route_misses=(?P<slot_route_misses>\d+)\s+"
    r"slot_all_hit_layers=(?P<slot_all_hit_layers>\d+)\s+"
    r"slot_map_publications=(?P<slot_map_publications>\d+)\s+"
    r"slot_map_publish_failures=(?P<slot_map_publish_failures>\d+)\s+"
    r"slot_arena_attempts=(?P<slot_arena_attempts>\d+)\s+"
    r"slot_arena_successes=(?P<slot_arena_successes>\d+)\s+"
    r"slot_arena_failures=(?P<slot_arena_failures>\d+)\s+"
    r"slot_upload_batches=(?P<slot_upload_batches>\d+)\s+"
    r"slot_uploads=(?P<slot_uploads>\d+)\s+"
    r"mapped_attempts=(?P<mapped_attempts>\d+)\s+"
    r"mapped_fallbacks=(?P<mapped_fallbacks>\d+)\s+"
    r"mapped_failures=(?P<mapped_failures>\d+)\s+"
    r"linear_attempts=(?P<linear_attempts>\d+)\s+"
    r"linear_successes=(?P<linear_successes>\d+)\s+"
    r"linear_fallbacks=(?P<linear_fallbacks>\d+)\s+"
    r"pair_attempts=(?P<pair_attempts>\d+)\s+"
    r"pair_successes=(?P<pair_successes>\d+)\s+"
    r"pair_fallbacks=(?P<pair_fallbacks>\d+)\s+"
    r"scatter_attempts=(?P<scatter_attempts>\d+)\s+"
    r"scatter_successes=(?P<scatter_successes>\d+)\s+"
    r"scatter_fallbacks=(?P<scatter_fallbacks>\d+)\s+"
    r"frame_begins=(?P<frame_begins>\d+)\s+"
    r"frame_submits=(?P<frame_submits>\d+)\s+"
    r"compute_encoders=(?P<compute_encoders>\d+)\s+"
    r"blit_encoders=(?P<blit_encoders>\d+)\s+"
    r"bootstrap_misses=(?P<bootstrap_misses>\d+)"
)
SELECTED_PAGE_REQUEST_TELEMETRY_MARKER = re.compile(
    r"metal_a4b_server_selected_page:\s+scope=process_delta\s+model=.*?\s+"
    r"attempts=(?P<selected_page_attempts>\d+)\s+"
    r"successes=(?P<selected_page_successes>\d+)\s+"
    r"failures=(?P<selected_page_failures>\d+)\s+"
    r"logical_bytes=(?P<selected_page_logical_bytes>\d+)\s+"
    r"mapped_bytes=(?P<selected_page_mapped_bytes>\d+)\s+"
    r"allocations=(?P<selected_page_allocations>\d+)"
)
GENERATION_TIMING_MARKER = re.compile(
    r"generate_timing_ms:\s+"
    r"prompt_format=(?P<prompt_format>\d+)\s+"
    r"tokenize=(?P<tokenize>\d+)\s+"
    r"runtime_prepare=(?P<runtime_prepare>\d+)\s+"
    r"prefill=(?P<prefill>\d+)\s+"
    r"decode=(?P<decode>\d+)\s+"
    r"text_decode=(?P<text_decode>\d+)\s+"
    r"total=(?P<total>\d+)"
)
HEX_SHA256 = re.compile(r"[0-9a-f]{64}")


class AcceptanceError(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class ResourceSample:
    elapsed_seconds: float
    free_percent: int
    rss_mib: float
    swapout_mib: float


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def parse_memory_pressure_free_percent(output: str) -> int:
    match = re.search(r"System-wide memory free percentage:\s*(\d+)%", output)
    if match is None:
        raise AcceptanceError("memory_pressure did not report free percentage")
    value = int(match.group(1))
    if not 0 <= value <= 100:
        raise AcceptanceError(f"invalid memory free percentage: {value}")
    return value


def parse_vm_stat_swapout_bytes(output: str) -> int:
    page_match = re.search(r"page size of\s+(\d+)\s+bytes", output)
    swap_match = re.search(r"^Swapouts:\s*(\d+)\.", output, re.MULTILINE)
    if page_match is None or swap_match is None:
        raise AcceptanceError("vm_stat did not report page size and Swapouts")
    return int(page_match.group(1)) * int(swap_match.group(1))


def parse_process_rss_kib(output: str) -> int:
    value = output.strip()
    if not value or not value.isdigit():
        raise AcceptanceError(f"ps did not report numeric RSS: {value!r}")
    return int(value)


def _command_text(command: tuple[str, ...]) -> str:
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=10,
    )
    if completed.returncode != 0:
        raise AcceptanceError(
            f"resource probe exited {completed.returncode}: {' '.join(command)}: "
            f"{completed.stdout.strip()}"
        )
    return completed.stdout


def _memory_free_percent() -> int:
    return parse_memory_pressure_free_percent(
        _command_text(("/usr/bin/memory_pressure", "-Q"))
    )


def _swapout_bytes() -> int:
    return parse_vm_stat_swapout_bytes(_command_text(("/usr/bin/vm_stat",)))


def _process_rss_mib(pid: int) -> float:
    output = _command_text(("/bin/ps", "-o", "rss=", "-p", str(pid)))
    return parse_process_rss_kib(output) / 1024.0


class ResourceMonitor:
    def __init__(
        self,
        process: subprocess.Popen[bytes],
        *,
        baseline_swapout_bytes: int,
        min_free_percent: int,
        max_rss_mib: float,
        max_swapout_growth_mib: float,
        interval_seconds: float,
    ) -> None:
        self.process = process
        self.baseline_swapout_bytes = baseline_swapout_bytes
        self.min_free_percent = min_free_percent
        self.max_rss_mib = max_rss_mib
        self.max_swapout_growth_mib = max_swapout_growth_mib
        self.interval_seconds = interval_seconds
        self.started_at = time.monotonic()
        self.samples: list[ResourceSample] = []
        self.violation: str | None = None
        self.probe_failure: str | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(self.interval_seconds * 2, 2.0))

    def check(self) -> None:
        if self.probe_failure is not None:
            raise AcceptanceError(
                f"resource monitoring failed closed: {self.probe_failure}"
            )
        if self.violation is not None:
            raise AcceptanceError(self.violation)

    def _terminate_server(self) -> None:
        if self.process.poll() is not None:
            return
        try:
            os.killpg(self.process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    def _run(self) -> None:
        while not self._stop.is_set() and self.process.poll() is None:
            try:
                free_percent = _memory_free_percent()
                swapout_bytes = _swapout_bytes()
                rss_mib = _process_rss_mib(self.process.pid)
            except (AcceptanceError, OSError, subprocess.SubprocessError) as exc:
                if self.process.poll() is None:
                    self.probe_failure = str(exc)
                    self._terminate_server()
                return
            swapout_growth_mib = max(swapout_bytes - self.baseline_swapout_bytes, 0) / (
                1024 * 1024
            )
            sample = ResourceSample(
                elapsed_seconds=time.monotonic() - self.started_at,
                free_percent=free_percent,
                rss_mib=rss_mib,
                swapout_mib=swapout_growth_mib,
            )
            self.samples.append(sample)
            if free_percent < self.min_free_percent:
                self.violation = (
                    f"memory free percentage {free_percent}% fell below "
                    f"{self.min_free_percent}%"
                )
            elif rss_mib > self.max_rss_mib:
                self.violation = (
                    f"server RSS {rss_mib:.1f} MiB exceeded {self.max_rss_mib:.1f} MiB"
                )
            elif swapout_growth_mib > self.max_swapout_growth_mib:
                self.violation = (
                    f"system swapout growth {swapout_growth_mib:.1f} MiB exceeded "
                    f"{self.max_swapout_growth_mib:.1f} MiB"
                )
            if self.violation is not None:
                self._terminate_server()
                return
            self._stop.wait(self.interval_seconds)

    def summary(self) -> dict[str, Any]:
        return {
            "sample_count": len(self.samples),
            "min_free_percent": min(
                (sample.free_percent for sample in self.samples), default=None
            ),
            "max_rss_mib": max(
                (sample.rss_mib for sample in self.samples), default=None
            ),
            "max_swapout_growth_mib": max(
                (sample.swapout_mib for sample in self.samples), default=None
            ),
            "violation": self.violation,
            "probe_failure": self.probe_failure,
            "samples": [dataclasses.asdict(sample) for sample in self.samples],
        }


def choose_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def resolve_decoder_gguf(model_dir: Path) -> Path:
    candidates = sorted(
        path.resolve()
        for path in model_dir.iterdir()
        if path.is_file()
        and path.suffix.lower() == ".gguf"
        and "mmproj" not in path.name.lower()
        and "projector" not in path.name.lower()
    )
    if len(candidates) != 1:
        rendered = ", ".join(str(path) for path in candidates) or "none"
        raise AcceptanceError(
            f"model directory must contain exactly one decoder GGUF; found {rendered}"
        )
    return candidates[0]


def model_identifier(model_dir: Path, models_dir: Path) -> str:
    try:
        relative = model_dir.resolve().relative_to(models_dir.resolve())
    except ValueError as exc:
        raise AcceptanceError("model-dir must be contained by models-dir") from exc
    if relative == Path(".") or any(part in {"", ".", ".."} for part in relative.parts):
        raise AcceptanceError("model-dir must be a named directory below models-dir")
    return relative.as_posix()


def build_server_config(
    *,
    models_dir: Path,
    model_id: str,
    residency_mode: str,
    memory_budget_mb: int,
) -> dict[str, Any]:
    return {
        "models_dir": str(models_dir.resolve()),
        "max_loaded_models": 1,
        "admission": {"inference": {"max_concurrent_requests": 1}},
        "generation_batching": {
            "mode": "off",
            "max_step_items": 1,
            "max_step_query_tokens": 128,
            "max_idle_prefill_chunk_size": 128,
        },
        "prompt_cache": {"enabled": False},
        "preload": [
            {
                "kind": "generator",
                "name": model_id,
                "backend": "metal",
                "residency_mode": residency_mode,
                "memory_budget_mb": memory_budget_mb,
            }
        ],
    }


def effective_server_environment(
    *,
    debug_metal_timing: bool = False,
    active_paged_slot_attention_deferred: bool = False,
    disable_active_paged_slot_attention_deferred: bool = False,
    disable_device_moe_route_select: bool = False,
    selected_expert_page_moe: bool = False,
) -> dict[str, str]:
    if (
        active_paged_slot_attention_deferred
        and disable_active_paged_slot_attention_deferred
    ):
        raise ValueError("active paged-slot attention overrides are mutually exclusive")
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(POLICY_ENV_PREFIXES)
    }
    environment["TERMITE_SERVER_A4B_TELEMETRY"] = "1"
    environment["TERMITE_SERVER_GENERATE_TIMING"] = "1"
    if debug_metal_timing:
        environment["TERMITE_DEBUG_METAL_TIMING"] = "1"
    if active_paged_slot_attention_deferred:
        environment["TERMITE_METAL_ENABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED"] = "1"
    if disable_active_paged_slot_attention_deferred:
        environment["TERMITE_METAL_DISABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED"] = "1"
    if disable_device_moe_route_select:
        environment["TERMITE_METAL_DISABLE_DEVICE_MOE_ROUTE_SELECT"] = "1"
    if selected_expert_page_moe:
        environment["TERMITE_METAL_ENABLE_A4B_SELECTED_EXPERT_PAGE_MOE"] = "1"
    return environment


def _wait_for_health(
    *,
    process: subprocess.Popen[bytes],
    monitor: ResourceMonitor,
    url: str,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        monitor.check()
        if process.poll() is not None:
            raise AcceptanceError(
                f"server exited during startup with {process.returncode}"
            )
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if 200 <= response.status < 300:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    raise AcceptanceError(f"server did not become healthy within {timeout_seconds}s")


def _health_check(port: int, timeout_seconds: float) -> None:
    url = f"http://127.0.0.1:{port}/healthz"
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            if response.status != 200:
                raise AcceptanceError(
                    f"post-request health returned HTTP {response.status}"
                )
    except (OSError, urllib.error.URLError) as exc:
        raise AcceptanceError(f"post-request health failed: {exc}") from exc


def generation_request(
    *,
    model_id: str,
    prompt: str,
    output_tokens: int,
    ignore_eos: bool = True,
    enable_thinking: bool = False,
) -> dict[str, Any]:
    return {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "chat_template_kwargs": {"enable_thinking": enable_thinking},
        "backend": "metal",
        "mode": "compiled",
        "compiled_target": "whole-model",
        "cache_dtype": "f16",
        "max_tokens": output_tokens,
        "temperature": 0,
        "top_p": 1,
        "top_k": 1,
        "ignore_eos": ignore_eos,
        "prompt_cache": False,
        "stream": False,
    }


def _post_generation(
    port: int,
    body: dict[str, Any],
    timeout_seconds: float,
) -> tuple[dict[str, Any], float]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout_seconds)
    payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode()
    started = time.monotonic()
    try:
        connection.request(
            "POST",
            "/ai/v1/generate",
            body=payload,
            headers={"content-type": "application/json", "accept": "application/json"},
        )
        response = connection.getresponse()
        raw = response.read()
    finally:
        connection.close()
    elapsed_ms = (time.monotonic() - started) * 1000.0
    if response.status != 200:
        raise AcceptanceError(
            f"generation returned HTTP {response.status}: {raw.decode(errors='replace')[:1000]}"
        )
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AcceptanceError("generation returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise AcceptanceError("generation response must be a JSON object")
    return value, elapsed_ms


def summarize_response(
    response: dict[str, Any],
    *,
    expected_tokens: int,
    expected_substring: str,
    fixed_length: bool = True,
) -> dict[str, Any]:
    choices = response.get("choices")
    usage = response.get("usage")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise AcceptanceError("generation response is missing choices[0]")
    if not isinstance(usage, dict):
        raise AcceptanceError("generation response is missing usage")
    choice = choices[0]
    message = choice.get("message")
    reasoning_content = (
        message.get("reasoning_content") if isinstance(message, dict) else None
    )
    if reasoning_content is not None and not isinstance(reasoning_content, str):
        raise AcceptanceError("reasoning_content must be a string or null")
    content = str(
        message.get("content", "")
        if isinstance(message, dict)
        else choice.get("text", "")
    )
    completion_tokens = usage.get("completion_tokens")
    finish_reason = choice.get("finish_reason")
    if fixed_length:
        if completion_tokens != expected_tokens:
            raise AcceptanceError(
                f"completion_tokens={completion_tokens!r}, expected {expected_tokens}"
            )
        if finish_reason != "length":
            raise AcceptanceError(f"finish_reason={finish_reason!r}, expected 'length'")
    else:
        if (
            not isinstance(completion_tokens, int)
            or isinstance(completion_tokens, bool)
            or not 1 <= completion_tokens <= expected_tokens
        ):
            raise AcceptanceError(
                f"completion_tokens={completion_tokens!r}, expected 1..{expected_tokens}"
            )
        if finish_reason != "stop":
            raise AcceptanceError(
                f"finish_reason={finish_reason!r}, expected natural EOS 'stop'"
            )
    if expected_substring.casefold() not in content.casefold():
        raise AcceptanceError(
            f"response did not contain required semantic substring {expected_substring!r}"
        )
    content_bytes = content.encode("utf-8")
    return {
        "completion_tokens": completion_tokens,
        "prompt_tokens": usage.get("prompt_tokens"),
        "finish_reason": finish_reason,
        "content": content,
        "reasoning_content": reasoning_content,
        "content_utf8_bytes": len(content_bytes),
        "content_sha256": _sha256_bytes(content_bytes),
    }


def parse_runtime_markers(log: str) -> list[dict[str, Any]]:
    return [
        {
            "mode": match.group(1),
            "memory_budget_mb": int(match.group(2)),
            "kv_budget_mb": int(match.group(3)),
            "safety_budget_mb": int(match.group(4)),
            "expert_slots": int(match.group(5)),
        }
        for match in RUNTIME_MARKER.finditer(log)
    ]


def parse_server_telemetry(log: str) -> list[dict[str, int]]:
    return [
        {key: int(value) for key, value in match.groupdict().items()}
        for match in TELEMETRY_MARKER.finditer(log)
    ]


def parse_selected_page_request_telemetry(log: str) -> list[dict[str, int]]:
    return [
        {key: int(value) for key, value in match.groupdict().items()}
        for match in SELECTED_PAGE_REQUEST_TELEMETRY_MARKER.finditer(log)
    ]


def parse_selected_page_moe_markers(log: str) -> list[dict[str, int]]:
    return [
        {key: int(value) for key, value in match.groupdict().items()}
        for match in SELECTED_PAGE_MOE_MARKER.finditer(log)
    ]


def parse_generation_timings(log: str) -> list[dict[str, int]]:
    return [
        {key: int(value) for key, value in match.groupdict().items()}
        for match in GENERATION_TIMING_MARKER.finditer(log)
    ]


def release_qualification(*, semantic_oracle_attested: bool) -> dict[str, Any]:
    """Describe this cautious gate without promoting it to a release verdict."""
    return {
        "scope": "short_serial_acceptance",
        "semantic_oracle_attested": semantic_oracle_attested,
        "release_ready": False,
        "reason": (
            "long-output, concurrency, soak, and paired performance qualification "
            "are separate promotion gates"
        ),
    }


def validate_server_log(
    log: str,
    *,
    residency_mode: str,
    memory_budget_mb: int,
    output_tokens: int,
    request_count: int,
    max_frame_submits: int,
    min_decode_tok_s: float = 0.0,
    require_packed_moe: bool = False,
    require_device_route_select: bool = True,
    require_selected_page_moe: bool = False,
    completion_token_counts: list[int] | None = None,
) -> dict[str, Any]:
    runtime_markers = parse_runtime_markers(log)
    if not runtime_markers:
        raise AcceptanceError("qualified A4B runtime marker is missing from server log")
    runtime = runtime_markers[-1]
    if (
        runtime["mode"] != residency_mode
        or runtime["memory_budget_mb"] != memory_budget_mb
    ):
        raise AcceptanceError(
            "server loaded a different A4B residency policy: "
            f"{runtime['mode']}/{runtime['memory_budget_mb']}"
        )
    if (
        runtime["kv_budget_mb"] <= 0
        or runtime["safety_budget_mb"] <= 0
        or runtime["expert_slots"] < 8
    ):
        raise AcceptanceError(f"invalid bounded A4B runtime geometry: {runtime}")

    selected_page_markers = parse_selected_page_moe_markers(log)
    selected_page_moe = selected_page_markers[-1] if selected_page_markers else None
    if require_selected_page_moe:
        if selected_page_moe is None:
            raise AcceptanceError("selected-expert page MoE marker is missing")
        if (
            selected_page_moe["logical_bytes"]
            != SELECTED_PAGE_LOGICAL_BYTES_PER_ATTEMPT
            or selected_page_moe["mapped_page_bytes"]
            < selected_page_moe["logical_bytes"]
            or selected_page_moe["mapped_page_bytes"]
            > SELECTED_PAGE_MAX_MAPPED_BYTES_PER_ATTEMPT
            or selected_page_moe["allocations"] != SELECTED_PAGE_ALLOCATIONS_PER_ATTEMPT
            or selected_page_moe["persistent_cache"] != 0
            or selected_page_moe["weight_copies"] != 0
        ):
            raise AcceptanceError(
                f"invalid selected-expert page MoE contract: {selected_page_moe}"
            )
    elif selected_page_moe is not None:
        raise AcceptanceError(
            "selected-expert page MoE was enabled without an explicit harness request"
        )

    events = parse_server_telemetry(log)
    if len(events) < request_count:
        raise AcceptanceError(
            f"found {len(events)} measured A4B telemetry events, expected {request_count}"
        )
    selected_page_events = parse_selected_page_request_telemetry(log)
    if len(selected_page_events) < request_count:
        raise AcceptanceError(
            "found "
            f"{len(selected_page_events)} measured selected-expert page MoE telemetry "
            f"events, expected {request_count}"
        )
    measured = events[-request_count:]
    measured_selected_page = selected_page_events[-request_count:]
    for event, selected_page_event in zip(
        measured, measured_selected_page, strict=True
    ):
        event.update(selected_page_event)
    for index, event in enumerate(measured, 1):
        forbidden = {
            key: event[key]
            for key in (
                "mapped_fallbacks",
                "mapped_failures",
                "linear_fallbacks",
                "pair_fallbacks",
                "scatter_fallbacks",
                "slot_map_publish_failures",
                "slot_arena_failures",
                "bootstrap_misses",
            )
            if event[key] != 0
        }
        if forbidden:
            raise AcceptanceError(
                f"request {index} reported forbidden A4B fallback counters: {forbidden}"
            )
        selected_page_counts = {
            key: event[key]
            for key in (
                "selected_page_attempts",
                "selected_page_successes",
                "selected_page_failures",
                "selected_page_logical_bytes",
                "selected_page_mapped_bytes",
                "selected_page_allocations",
            )
        }
        if require_selected_page_moe:
            selected_page_attempts = event["selected_page_attempts"]
            expected_logical_bytes = (
                selected_page_attempts * SELECTED_PAGE_LOGICAL_BYTES_PER_ATTEMPT
            )
            expected_allocations = (
                selected_page_attempts * SELECTED_PAGE_ALLOCATIONS_PER_ATTEMPT
            )
            max_mapped_bytes = (
                selected_page_attempts * SELECTED_PAGE_MAX_MAPPED_BYTES_PER_ATTEMPT
            )
            if (
                selected_page_attempts == 0
                or selected_page_attempts != event["selected_page_successes"]
                or event["selected_page_failures"] != 0
                or selected_page_attempts != event["route_select_attempts"]
                or event["selected_page_logical_bytes"] != expected_logical_bytes
                or event["selected_page_allocations"] != expected_allocations
                or event["selected_page_mapped_bytes"]
                < event["selected_page_logical_bytes"]
                or event["selected_page_mapped_bytes"] > max_mapped_bytes
            ):
                raise AcceptanceError(
                    f"request {index} has invalid selected-expert page MoE telemetry: "
                    f"{selected_page_counts}"
                )
        elif any(selected_page_counts.values()):
            raise AcceptanceError(
                f"request {index} reported selected-expert page MoE telemetry "
                f"without an explicit harness request: {selected_page_counts}"
            )
        if event["blit_encoders"] != event["slot_upload_batches"]:
            raise AcceptanceError(
                f"request {index} reported unattributed A4B blit encoders: "
                f"blits={event['blit_encoders']} "
                f"slot_upload_batches={event['slot_upload_batches']}"
            )
        if require_device_route_select:
            if (
                event["route_select_attempts"] == 0
                or event["route_select_attempts"] != event["route_select_successes"]
                or event["route_select_fallbacks"] != 0
            ):
                raise AcceptanceError(
                    f"request {index} did not keep A4B expert selection on device: "
                    f"attempts={event['route_select_attempts']} "
                    f"successes={event['route_select_successes']} "
                    f"fallbacks={event['route_select_fallbacks']}"
                )
        elif event["route_select_successes"] != 0:
            raise AcceptanceError(
                f"request {index} did not exercise the host route-selection rollback: "
                f"attempts={event['route_select_attempts']} "
                f"successes={event['route_select_successes']} "
                f"fallbacks={event['route_select_fallbacks']}"
            )
        packed_route_counts = {
            key: event[key]
            for key in (
                "linear_attempts",
                "linear_successes",
                "pair_attempts",
                "pair_successes",
                "scatter_attempts",
                "scatter_successes",
            )
        }
        if require_packed_moe:
            if (
                event["linear_attempts"] == 0
                or event["linear_attempts"] != event["linear_successes"]
                or event["pair_attempts"] == 0
                or event["pair_attempts"] != event["pair_successes"]
                or event["scatter_attempts"] == 0
                or event["scatter_attempts"] != event["scatter_successes"]
            ):
                raise AcceptanceError(
                    f"request {index} did not stay on the packed A4B MoE path: "
                    f"{packed_route_counts}"
                )
        elif any(packed_route_counts.values()):
            raise AcceptanceError(
                f"request {index} escaped compiled baseline A4B execution: "
                f"{packed_route_counts}"
            )
        if event["mapped_attempts"] == 0:
            raise AcceptanceError(
                f"request {index} did not exercise mapped quantized weights"
            )
        if (
            event["frame_begins"] == 0
            or event["frame_submits"] == 0
            or event["compute_encoders"] < event["frame_submits"]
        ):
            raise AcceptanceError(
                f"request {index} has invalid retained-frame telemetry: {event}"
            )
        if event["frame_submits"] > max_frame_submits:
            raise AcceptanceError(
                f"request {index} frame_submits={event['frame_submits']} exceeds "
                f"{max_frame_submits}"
            )

    timing_events = parse_generation_timings(log)
    if len(timing_events) < request_count:
        raise AcceptanceError(
            f"found {len(timing_events)} generation timing events, expected at least "
            f"{request_count}"
        )
    measured_timings = timing_events[-request_count:]
    if completion_token_counts is None:
        completion_token_counts = [output_tokens] * request_count
    if len(completion_token_counts) != request_count:
        raise AcceptanceError(
            "completion-token timing cardinality does not match request count"
        )
    for index, (timing, completion_tokens) in enumerate(
        zip(measured_timings, completion_token_counts, strict=True), 1
    ):
        if timing["prefill"] <= 0 or timing["total"] <= 0:
            raise AcceptanceError(
                f"request {index} has invalid generation phase timing: {timing}"
            )
        if completion_tokens > 1 and timing["decode"] <= 0:
            raise AcceptanceError(
                f"request {index} has no measured decode time: {timing}"
            )
        if timing["prefill"] + timing["decode"] > timing["total"] + 2:
            raise AcceptanceError(
                f"request {index} generation phases do not reconcile: {timing}"
            )
        decode_frames = max(completion_tokens - 1, 0)
        decode_tok_s = (
            decode_frames * 1000.0 / timing["decode"]
            if decode_frames > 0 and timing["decode"] > 0
            else 0.0
        )
        timing["decode_tok_s"] = decode_tok_s
        if min_decode_tok_s > 0 and decode_tok_s < min_decode_tok_s:
            raise AcceptanceError(
                f"request {index} decode throughput {decode_tok_s:.3f} tok/s is below "
                f"{min_decode_tok_s:.3f} tok/s"
            )
    return {
        "runtime": runtime,
        "selected_page_moe": selected_page_moe,
        "all_event_count": len(events),
        "measured_events": measured,
        "generation_timings_ms": measured_timings,
    }


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def _git_provenance(repo: Path) -> dict[str, Any]:
    environment = {**os.environ, "LC_ALL": "C"}
    revision = (
        subprocess.check_output(
            ("git", "-C", str(repo), "rev-parse", "HEAD"), env=environment
        )
        .decode()
        .strip()
    )
    status = subprocess.check_output(
        ("git", "-C", str(repo), "status", "--porcelain=v1", "--untracked-files=all"),
        env=environment,
    )
    diff = subprocess.check_output(
        ("git", "-C", str(repo), "diff", "--binary", "--no-ext-diff", "HEAD", "--"),
        env=environment,
    )
    return {
        "revision": revision,
        "dirty": bool(status.rstrip(b"\n")),
        "status_sha256": hashlib.sha256(status).hexdigest(),
        "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
    }


def run_acceptance(args: argparse.Namespace) -> dict[str, Any]:
    repo = Path(__file__).resolve().parents[5]
    model_dir = args.model_dir.resolve()
    models_dir = (args.models_dir or model_dir.parent).resolve()
    decoder_gguf = resolve_decoder_gguf(model_dir)
    model_id = model_identifier(model_dir, models_dir)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=False)
    log_path = output_dir / "server.log"
    config_path = output_dir / "server.json"
    summary_path = output_dir / "summary.json"
    config = build_server_config(
        models_dir=models_dir,
        model_id=model_id,
        residency_mode=args.residency_mode,
        memory_budget_mb=args.memory_budget_mb,
    )
    _write_json(config_path, config)

    preflight_free_percent = _memory_free_percent()
    if preflight_free_percent < args.min_free_percent:
        raise AcceptanceError(
            f"preflight memory free percentage {preflight_free_percent}% is below "
            f"{args.min_free_percent}%"
        )
    baseline_swapout_bytes = _swapout_bytes()
    port = choose_port()
    command = [
        str(args.antfly_bin.resolve()),
        "run",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--models-dir",
        str(models_dir),
        "--config",
        str(config_path),
        "--allow-unknown-models",
        "--host-budget-mb",
        str(args.host_budget_mb),
        "--backend-budget-mb",
        str(args.backend_budget_mb),
        "--combined-budget-mb",
        str(args.combined_budget_mb),
        "--kv-budget-mb",
        str(args.kv_budget_mb),
        "--scratch-budget-mb",
        str(args.scratch_budget_mb),
    ]
    environment = effective_server_environment(
        debug_metal_timing=args.debug_metal_timing,
        active_paged_slot_attention_deferred=args.active_paged_slot_attention_deferred,
        disable_active_paged_slot_attention_deferred=(
            args.disable_active_paged_slot_attention_deferred
        ),
        disable_device_moe_route_select=args.disable_device_moe_route_select,
        selected_expert_page_moe=args.selected_expert_page_moe,
    )
    responses: list[dict[str, Any]] = []
    process: subprocess.Popen[bytes] | None = None
    monitor: ResourceMonitor | None = None
    with log_path.open("wb") as log_file:
        try:
            process = subprocess.Popen(
                command,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=environment,
                start_new_session=True,
            )
            monitor = ResourceMonitor(
                process,
                baseline_swapout_bytes=baseline_swapout_bytes,
                min_free_percent=args.min_free_percent,
                max_rss_mib=args.max_rss_mb,
                max_swapout_growth_mib=args.max_swapout_growth_mb,
                interval_seconds=args.monitor_interval,
            )
            monitor.start()
            _wait_for_health(
                process=process,
                monitor=monitor,
                url=f"http://127.0.0.1:{port}/healthz",
                timeout_seconds=args.startup_timeout,
            )
            body = generation_request(
                model_id=model_id,
                prompt=args.prompt,
                output_tokens=args.output_tokens,
                ignore_eos=not args.natural_eos,
                enable_thinking=args.enable_thinking,
            )
            for request_index in range(1, args.requests + 1):
                monitor.check()
                try:
                    response, elapsed_ms = _post_generation(
                        port, body, args.request_timeout
                    )
                except (OSError, http.client.HTTPException) as exc:
                    monitor.check()
                    raise AcceptanceError(
                        f"generation connection failed without a resource-guard reason: {exc}"
                    ) from exc
                summary = summarize_response(
                    response,
                    expected_tokens=args.output_tokens,
                    expected_substring=args.expected_substring,
                    fixed_length=not args.natural_eos,
                )
                if not args.enable_thinking and summary["reasoning_content"] not in (
                    None,
                    "",
                ):
                    raise AcceptanceError(
                        "enable_thinking=false returned private reasoning content"
                    )
                summary["request"] = request_index
                summary["latency_ms"] = elapsed_ms
                summary["end_to_end_tok_s"] = (
                    max(summary["completion_tokens"] - 1, 0) * 1000.0 / elapsed_ms
                )
                responses.append(summary)
                _health_check(port, min(args.request_timeout, 10.0))
                monitor.check()
        finally:
            if process is not None:
                _stop_process(process)
            if monitor is not None:
                monitor.stop()
    if monitor is None or process is None:
        raise AcceptanceError("server process was not started")
    monitor.check()
    log = log_path.read_text(encoding="utf-8", errors="replace")
    telemetry = validate_server_log(
        log,
        residency_mode=args.residency_mode,
        memory_budget_mb=args.memory_budget_mb,
        output_tokens=args.output_tokens,
        request_count=args.requests,
        max_frame_submits=args.max_frame_submits,
        min_decode_tok_s=args.min_decode_tok_s,
        require_packed_moe=args.require_packed_moe,
        require_device_route_select=not args.disable_device_moe_route_select,
        require_selected_page_moe=args.selected_expert_page_moe,
        completion_token_counts=[
            response["completion_tokens"] for response in responses
        ],
    )
    for response, timing in zip(
        responses, telemetry["generation_timings_ms"], strict=True
    ):
        response["generation_timing_ms"] = timing
    content_hashes = {response["content_sha256"] for response in responses}
    if len(content_hashes) != 1:
        raise AcceptanceError(
            f"deterministic requests produced different content hashes: {sorted(content_hashes)}"
        )
    content_sha256 = next(iter(content_hashes))
    if args.expected_content_sha256 and content_sha256 != args.expected_content_sha256:
        raise AcceptanceError(
            f"content SHA-256 {content_sha256} differs from expected "
            f"{args.expected_content_sha256}"
        )
    if args.require_oracle and not args.expected_content_sha256:
        raise AcceptanceError("--require-oracle requires --expected-content-sha256")

    qualification = release_qualification(
        semantic_oracle_attested=bool(args.expected_content_sha256)
    )
    result = {
        "schema": SCHEMA,
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "passed": True,
        "release_ready": qualification["release_ready"],
        "qualification": qualification,
        "contract": {
            "serial_requests": args.requests,
            "output_tokens": args.output_tokens,
            "residency_mode": args.residency_mode,
            "memory_budget_mb": args.memory_budget_mb,
            "process_budgets_mb": {
                "host": args.host_budget_mb,
                "backend": args.backend_budget_mb,
                "combined": args.combined_budget_mb,
                "kv": args.kv_budget_mb,
                "scratch": args.scratch_budget_mb,
            },
            "execution_mode": "compiled_whole_model",
            "max_frame_submits": args.max_frame_submits,
            "min_decode_tok_s": args.min_decode_tok_s,
            "debug_metal_timing": args.debug_metal_timing,
            "active_paged_slot_attention_deferred": args.active_paged_slot_attention_deferred,
            "active_paged_slot_attention_deferred_override": (
                "enable"
                if args.active_paged_slot_attention_deferred
                else "disable"
                if args.disable_active_paged_slot_attention_deferred
                else "runtime_default"
            ),
            "require_packed_moe": args.require_packed_moe,
            "device_moe_route_select": not args.disable_device_moe_route_select,
            "selected_expert_page_moe": args.selected_expert_page_moe,
            "enable_thinking": args.enable_thinking,
            "expected_substring": args.expected_substring,
            "expected_content_sha256": args.expected_content_sha256,
            "prompt_sha256": _sha256_bytes(args.prompt.encode()),
            "prompt_cache": False,
            "ignore_eos": not args.natural_eos,
            "generation_batching": "off",
        },
        "provenance": {
            "binary": {
                "path": str(args.antfly_bin.resolve()),
                "sha256": _sha256_file(args.antfly_bin.resolve()),
            },
            "model": {
                "directory": str(model_dir),
                "identifier": model_id,
                "decoder_gguf": str(decoder_gguf),
                "decoder_size_bytes": decoder_gguf.stat().st_size,
            },
            "harness_sha256": _sha256_file(Path(__file__).resolve()),
            "git": _git_provenance(repo),
            "platform": platform.platform(),
        },
        "semantic_oracle": {
            "kind": "pinned_content_sha256"
            if args.expected_content_sha256
            else "repeat_determinism_plus_required_substring",
            "content_sha256": content_sha256,
            "attested": bool(args.expected_content_sha256),
        },
        "responses": responses,
        "telemetry": telemetry,
        "resources": {
            "preflight_free_percent": preflight_free_percent,
            **monitor.summary(),
        },
        "artifacts": {
            "server_log": str(log_path),
            "server_config": str(config_path),
        },
    }
    _write_json(summary_path, result)
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    repo = Path(__file__).resolve().parents[5]
    default_output = Path("/private/tmp") / (
        "antfly-a4b-server-acceptance-"
        + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--antfly-bin",
        type=Path,
        default=repo / "zig/pkg/inference/zig-out/bin/antfly-inference",
    )
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--models-dir", type=Path)
    parser.add_argument("--output-dir", type=Path, default=default_output)
    parser.add_argument(
        "--residency-mode", choices=("auto", "streamed"), default="streamed"
    )
    parser.add_argument("--memory-budget-mb", type=int, default=2048)
    parser.add_argument("--host-budget-mb", type=int, default=2048)
    # The resident A4B lease accounts the full 2 GiB model envelope. Leave a
    # small, bounded backend margin for request-local KV/scratch admission.
    # The combined limit also includes the resident tokenizer and other host
    # model state, so keep aggregate accounting separate from expert residency.
    parser.add_argument("--backend-budget-mb", type=int, default=2304)
    parser.add_argument("--combined-budget-mb", type=int, default=4096)
    parser.add_argument("--kv-budget-mb", type=int, default=640)
    parser.add_argument("--scratch-budget-mb", type=int, default=384)
    parser.add_argument("--requests", type=int, default=2)
    parser.add_argument("--output-tokens", type=int, default=8)
    parser.add_argument(
        "--prompt", default="Complete with one word only: The capital of France is"
    )
    parser.add_argument("--expected-substring", default="Paris")
    parser.add_argument("--expected-content-sha256")
    parser.add_argument("--require-oracle", action="store_true")
    parser.add_argument("--natural-eos", action="store_true")
    parser.add_argument("--max-frame-submits", type=int)
    parser.add_argument("--min-decode-tok-s", type=float, default=0.0)
    parser.add_argument("--debug-metal-timing", action="store_true")
    active_paged_slot_group = parser.add_mutually_exclusive_group()
    active_paged_slot_group.add_argument(
        "--active-paged-slot-attention-deferred", action="store_true"
    )
    active_paged_slot_group.add_argument(
        "--disable-active-paged-slot-attention-deferred", action="store_true"
    )
    parser.add_argument("--require-packed-moe", action="store_true")
    parser.add_argument("--disable-device-moe-route-select", action="store_true")
    parser.add_argument("--selected-expert-page-moe", action="store_true")
    parser.add_argument("--enable-thinking", action="store_true")
    parser.add_argument("--min-free-percent", type=int, default=20)
    parser.add_argument("--max-rss-mb", type=float, default=4096.0)
    parser.add_argument("--max-swapout-growth-mb", type=float, default=64.0)
    parser.add_argument("--monitor-interval", type=float, default=0.5)
    parser.add_argument("--startup-timeout", type=float, default=300.0)
    parser.add_argument("--request-timeout", type=float, default=120.0)
    args = parser.parse_args(argv)
    if platform.system() != "Darwin":
        parser.error("this Metal resource gate requires macOS")
    if not args.antfly_bin.is_file():
        parser.error(f"Antfly binary does not exist: {args.antfly_bin}")
    if not args.model_dir.is_dir():
        parser.error(f"model directory does not exist: {args.model_dir}")
    if args.models_dir is not None and not args.models_dir.is_dir():
        parser.error(f"models directory does not exist: {args.models_dir}")
    if not 1 <= args.requests <= 10:
        parser.error("--requests must be between 1 and 10")
    if not 1 <= args.output_tokens <= 128:
        parser.error("--output-tokens must be between 1 and 128 for this cautious gate")
    if not 2048 <= args.memory_budget_mb <= 4096:
        parser.error("--memory-budget-mb must be between 2048 and 4096")
    if not 1 <= args.min_free_percent <= 90:
        parser.error("--min-free-percent must be between 1 and 90")
    if args.max_rss_mb <= 0 or args.max_swapout_growth_mb < 0:
        parser.error("resource ceilings must be positive/non-negative")
    if (
        args.monitor_interval <= 0
        or args.startup_timeout <= 0
        or args.request_timeout <= 0
    ):
        parser.error("timeouts and monitor interval must be positive")
    if args.expected_content_sha256 is not None:
        args.expected_content_sha256 = args.expected_content_sha256.lower()
        if HEX_SHA256.fullmatch(args.expected_content_sha256) is None:
            parser.error("--expected-content-sha256 must be a SHA-256")
    if args.require_oracle and args.expected_content_sha256 is None:
        parser.error("--require-oracle requires --expected-content-sha256")
    if args.max_frame_submits is None:
        # The retained cross-layer frame uses 457 submits for the canonical
        # eight-token case; the rolled-back per-layer path uses 660.
        args.max_frame_submits = args.output_tokens * 65
    if args.max_frame_submits < 1:
        parser.error("--max-frame-submits must be positive")
    if args.min_decode_tok_s < 0:
        parser.error("--min-decode-tok-s must be non-negative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_acceptance(args)
    except (AcceptanceError, OSError, subprocess.SubprocessError) as exc:
        print(f"A4B server acceptance failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
