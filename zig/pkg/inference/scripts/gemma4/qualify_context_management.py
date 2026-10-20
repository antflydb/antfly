#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Hardware-backed Gemma 4 context-management qualification evidence.

This runner uses only the Python standard library. A passing artifact is evidence
for the exact server, model revision, and backend under test; it does not by
itself mark a model/backend combination production-qualified.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import math
import platform
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("percentile requires observations")
    ordered = sorted(values)
    return ordered[math.ceil(fraction * len(ordered)) - 1]


def git_provenance(repo: Path, excluded_paths: set[Path] | None = None) -> dict[str, Any]:
    def git(*arguments: str) -> bytes:
        return subprocess.check_output(("git", "-C", str(repo), *arguments))

    status = git("status", "--porcelain=v1", "--untracked-files=all")
    diff = git("diff", "--binary", "--no-ext-diff", "HEAD", "--")
    untracked = git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
    untracked_digest = hashlib.sha256()
    excluded_paths = {path.resolve() for path in (excluded_paths or set())}
    source_suffixes = {".c", ".cc", ".cu", ".cuh", ".go", ".h", ".json", ".m", ".md", ".metal", ".py", ".rs", ".toml", ".ts", ".yaml", ".yml", ".zig", ".zon"}
    source_paths = sorted(
        path for path in untracked
        if path
        and Path(path.decode()).suffix.lower() in source_suffixes
        and (repo / path.decode()).resolve() not in excluded_paths
    )
    for encoded_path in source_paths:
        untracked_digest.update(len(encoded_path).to_bytes(8, "big"))
        untracked_digest.update(encoded_path)
        content = (repo / encoded_path.decode()).read_bytes()
        untracked_digest.update(len(content).to_bytes(8, "big"))
        untracked_digest.update(content)
    head = git("rev-parse", "HEAD").decode().strip()
    snapshot_digest = hashlib.sha256()
    for component in (head.encode(), diff, untracked_digest.digest()):
        snapshot_digest.update(len(component).to_bytes(8, "big"))
        snapshot_digest.update(component)
    return {
        "head": head,
        "dirty": bool(status.rstrip(b"\n")),
        "status_sha256": hashlib.sha256(status).hexdigest(),
        "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
        "untracked_content_sha256": untracked_digest.hexdigest(),
        "untracked_source_files": len(source_paths),
        "source_snapshot_sha256": snapshot_digest.hexdigest(),
    }


def load_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def memory_evidence(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    rows: list[dict[str, Any]] = []
    lines = path.read_text().splitlines()
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            if index == len(lines) - 1:
                break
            raise
        if not isinstance(row, dict):
            raise ValueError(f"{path}:{index + 1} must contain a JSON object")
        rows.append(row)
    if not rows:
        return {"path": str(path), "sha256": sha256_file(path), "samples": 0}
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "samples": len(rows),
        "first_wall_time_s": min(row["wall_time_s"] for row in rows),
        "last_wall_time_s": max(row["wall_time_s"] for row in rows),
        "first_monotonic_ns": min(row["monotonic_ns"] for row in rows),
        "last_monotonic_ns": max(row["monotonic_ns"] for row in rows),
        "pids": sorted({row["pid"] for row in rows}),
        "process_lifetimes": sorted({(row["pid"], row["start_abstime"]) for row in rows}),
        "peak_rss_bytes": max(row["rss_bytes"] for row in rows),
        "peak_phys_footprint_bytes": max(row["phys_footprint_bytes"] for row in rows),
        "lifetime_max_phys_footprint_bytes": max(row["lifetime_max_phys_footprint_bytes"] for row in rows),
        "conservative_peak_phys_footprint_bytes": max(
            max(row["phys_footprint_bytes"] for row in rows),
            max(row["lifetime_max_phys_footprint_bytes"] for row in rows),
        ),
    }


def wait_for_memory_evidence(path: Path | None, campaign_finished_monotonic_ns: int, timeout_s: float = 5.0) -> dict[str, Any] | None:
    if path is None:
        return None
    deadline = time.monotonic() + timeout_s
    while True:
        evidence = memory_evidence(path)
        if evidence and evidence.get("last_monotonic_ns", -1) >= campaign_finished_monotonic_ns:
            return evidence
        if time.monotonic() >= deadline:
            return evidence
        time.sleep(0.1)


def cuda_device_evidence() -> list[dict[str, Any]]:
    output = subprocess.check_output((
        "nvidia-smi", "--query-gpu=index,uuid,name,compute_cap,driver_version,memory.total",
        "--format=csv,noheader,nounits",
    ), text=True)
    devices = []
    for line in output.splitlines():
        if not line.strip():
            continue
        index, uuid, name, compute_capability, driver, total_mib = [value.strip() for value in line.split(",", 5)]
        devices.append({"index": int(index), "uuid": uuid, "name": name, "compute_capability": compute_capability, "driver_version": driver, "total_memory_bytes": int(total_mib) * 1024 * 1024})
    return devices


def sample_cuda_process(pid: int) -> dict[str, Any]:
    output = subprocess.check_output((
        "nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_gpu_memory",
        "--format=csv,noheader,nounits",
    ), text=True)
    allocations = []
    for line in output.splitlines():
        fields = [value.strip() for value in line.split(",", 2)]
        if len(fields) == 3 and fields[1].isdigit() and int(fields[1]) == pid:
            allocations.append({"gpu_uuid": fields[0], "used_memory_bytes": int(fields[2]) * 1024 * 1024})
    rss_kib = None
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            rss_kib = int(line.split()[1])
            break
    if rss_kib is None:
        raise RuntimeError(f"VmRSS missing for PID {pid}")
    return {"wall_time_s": time.time(), "monotonic_ns": time.monotonic_ns(), "pid": pid, "host_rss_bytes": rss_kib * 1024, "gpu_allocations": allocations}


class CudaProcessSampler:
    def __init__(self, pid: int, interval_s: float = 0.25):
        self.pid = pid
        self.interval_s = interval_s
        self.rows: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._sample()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _sample(self) -> None:
        try:
            self.rows.append(sample_cuda_process(self.pid))
        except Exception as error:
            if len(self.errors) < 10:
                self.errors.append(f"{type(error).__name__}: {error}")

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            self._sample()

    def finish(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        self._sample()
        gpu_values = [allocation["used_memory_bytes"] for row in self.rows for allocation in row["gpu_allocations"]]
        return {
            "source": "nvidia-smi+/proc",
            "pid": self.pid,
            "samples": len(self.rows),
            "errors": self.errors,
            "first_wall_time_s": min((row["wall_time_s"] for row in self.rows), default=None),
            "last_wall_time_s": max((row["wall_time_s"] for row in self.rows), default=None),
            "first_monotonic_ns": min((row["monotonic_ns"] for row in self.rows), default=None),
            "last_monotonic_ns": max((row["monotonic_ns"] for row in self.rows), default=None),
            "peak_host_rss_bytes": max((row["host_rss_bytes"] for row in self.rows), default=None),
            "peak_gpu_memory_bytes": max(gpu_values, default=None),
            "gpu_uuids": sorted({allocation["gpu_uuid"] for row in self.rows for allocation in row["gpu_allocations"]}),
            "observations": self.rows,
        }


def fixture_messages(filler_turns: int = 16) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": "Always preserve SYSTEM_MARKER_CTX_V1."},
        {"role": "developer", "content": "Always preserve DEVELOPER_MARKER_CTX_V1."},
        {"role": "user", "content": "Remember FACT_MARKER_CTX_V1: the project codename is amber-ant."},
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [{"id": "call_ctx_v1", "type": "function", "function": {"name": "lookup_fact", "arguments": "{\"key\":\"codename\"}"}}],
        },
        {"role": "tool", "tool_call_id": "call_ctx_v1", "content": "TOOL_FACT_MARKER_CTX_V1=amber-ant"},
        {"role": "assistant", "content": "The verified codename is amber-ant."},
    ]
    for index in range(filler_turns):
        messages.extend(
            [
                {"role": "user", "content": f"filler question {index}: " + ("context " * 24)},
                {"role": "assistant", "content": f"filler answer {index}: " + ("response " * 24)},
            ]
        )
    messages.append({"role": "user", "content": "State the codename and its evidence source."})
    return messages


def instruction_pairs(messages: list[dict[str, Any]]) -> list[tuple[str, str]]:
    return [(m["role"], m.get("content", "")) for m in messages if m.get("role") in {"system", "developer"}]


def assert_tool_atomic(messages: list[dict[str, Any]]) -> None:
    calls: set[str] = set()
    results: set[str] = set()
    for message in messages:
        for call in message.get("tool_calls") or []:
            calls.add(call["id"])
        if message.get("role") == "tool" and message.get("tool_call_id"):
            results.add(message["tool_call_id"])
    if calls != results:
        raise AssertionError(f"tool call/result units split: calls={sorted(calls)} results={sorted(results)}")


def validate_window(source: list[dict[str, Any]], response: dict[str, Any], target: int) -> None:
    context = response["context"]
    if instruction_pairs(response["messages"]) != instruction_pairs(source):
        raise AssertionError("system/developer instructions were not preserved byte-for-byte")
    assert_tool_atomic(response["messages"])
    if context["effective_input_tokens"] > target:
        raise AssertionError("window result exceeds requested exact input budget")
    if not context["windowed"] or context["dropped_messages"] <= 0:
        raise AssertionError("fixture did not exercise context windowing")
    if context["original_input_tokens"] <= context["effective_input_tokens"]:
        raise AssertionError("windowing did not reduce the rendered token count")


def validate_checkpoint(checkpoint: dict[str, Any]) -> None:
    for field in (
        "covered_prefix_sha256", "memory_sha256", "model_revision", "tokenizer_revision", "template_revision"
    ):
        value = checkpoint.get(field, "")
        if len(value) != 64 or any(c not in "0123456789abcdef" for c in value.lower()):
            raise AssertionError(f"invalid checkpoint digest/revision field {field}")
    if checkpoint.get("version") != "v1":
        raise AssertionError("checkpoint version is not v1")


def validate_fixture_memory(checkpoint: dict[str, Any]) -> None:
    memory = checkpoint.get("memory")
    if not isinstance(memory, dict):
        raise AssertionError("checkpoint omitted structured memory")

    def retains(text: Any, label: str) -> bool:
        return (isinstance(text, str) and re.search(r"(?<!\w)" + re.escape(label) + r"(?!\w)", text) is not None
                and "amber-ant" in text)

    # FACT_MARKER is a suffix of TOOL_FACT_MARKER. A whole-memory substring
    # search can therefore pass after the original user fact has disappeared.
    if not retains(memory.get("conversation"), "FACT_MARKER_CTX_V1"):
        raise AssertionError("conversation lost deterministic fixture marker FACT_MARKER_CTX_V1 or its value")
    evidence = memory.get("tool_evidence", [])
    if not isinstance(evidence, list) or not any(
        isinstance(item, dict) and item.get("tool_call_id") == "call_ctx_v1"
        and item.get("tool_name") == "lookup_fact" and retains(item.get("facts"), "TOOL_FACT_MARKER_CTX_V1")
        for item in evidence
    ):
        raise AssertionError("structured memory lost tool result TOOL_FACT_MARKER_CTX_V1, its value, or its attribution")


def validate_managed(response: dict[str, Any], *, target: int, max_tokens: int, checkpoint: bool) -> None:
    context = response.get("context")
    if not isinstance(context, dict):
        raise AssertionError("managed generation omitted context diagnostics")
    if context.get("policy") != "reject":
        raise AssertionError("managed generation attested the wrong policy")
    if context.get("checkpoint_applied") is not checkpoint:
        raise AssertionError("managed generation attested the wrong checkpoint state")
    if context.get("kv_cache_mode") != "exact_hybrid" or context.get("kv_cache_version") != "v1":
        raise AssertionError("managed generation did not attest exact_hybrid/v1 KV")
    effective = context.get("effective_input_tokens")
    if type(effective) is not int or not 0 < effective <= target:
        raise AssertionError("managed generation reported an invalid effective input budget")
    if (type(context.get("target_input_tokens")) is not int or context["target_input_tokens"] != target
            or type(context.get("reserved_output_tokens")) is not int or context["reserved_output_tokens"] != max_tokens):
        raise AssertionError("managed generation attested the wrong input/output reservation")


def validate_stream(events: list[dict[str, Any]], *, target: int, max_tokens: int, checkpoint: bool) -> str:
    if any("error" in event for event in events):
        raise AssertionError(f"SSE stream returned an error: {events}")
    if not events or not events[-1].get("done") or sum(event.get("done") is True for event in events) != 1:
        raise AssertionError("SSE stream must end with exactly one [DONE]")
    metadata = [index for index, event in enumerate(events) if "context" in event]
    first_choice = next((index for index, event in enumerate(events) if event.get("choices")), None)
    if len(metadata) != 1 or first_choice is None or metadata[0] >= first_choice:
        raise AssertionError("SSE context metadata must precede token choices exactly once")
    validate_managed(events[metadata[0]], target=target, max_tokens=max_tokens, checkpoint=checkpoint)
    text = []
    finished = False
    for event in events:
        for choice in event.get("choices", []):
            if finished or choice.get("index", 0) != 0:
                raise AssertionError("SSE stream returned choices after completion or an unexpected choice index")
            delta = choice.get("delta", {})
            content = delta.get("content")
            if content is not None:
                if not isinstance(content, str):
                    raise AssertionError("SSE stream returned non-text content")
                text.append(content)
            reason = choice.get("finish_reason")
            if reason is not None:
                if reason != "stop":
                    raise AssertionError(f"SSE stream did not complete normally: {reason}")
                finished = True
    result = "".join(text)
    if not finished or not result.strip():
        raise AssertionError("SSE stream omitted completed text output")
    return result


def parse_sse(data: bytes) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    fields: list[str] = []
    event_name = "message"
    for raw_line in data.decode("utf-8").replace("\r\n", "\n").split("\n"):
        if raw_line == "":
            if fields:
                joined = "\n".join(fields)
                if joined == "[DONE]":
                    events.append({"done": True})
                elif event_name == "error":
                    events.append({"error": joined})
                else:
                    events.append(json.loads(joined))
                fields = []
                event_name = "message"
            continue
        if raw_line.startswith("event:"):
            event_name = raw_line[6:].lstrip(" ")
        if raw_line.startswith("data:"):
            fields.append(raw_line[5:].lstrip(" "))
    return events


def read_bounded(response: Any, limit: int) -> bytes:
    data = response.read(limit + 1)
    if len(data) > limit:
        raise AssertionError(f"HTTP response exceeded {limit} bytes")
    return data


class Client:
    def __init__(self, base_url: str, timeout: float, token: str | None, max_response_bytes: int):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.token = token
        self.max_response_bytes = max_response_bytes
        self.timings_ms: list[dict[str, Any]] = []

    def _record_timing(self, operation: str, path: str, started: float, headers: Any = None, qualification_group: str | None = None) -> None:
        item = {
            "operation": operation,
            "path": path,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        }
        if headers is not None:
            item["inference_backend"] = headers.get("X-Antfly-Inference-Backend")
            item["inference_route"] = headers.get("X-Antfly-Inference-Route")
            item["inference_pid"] = headers.get("X-Antfly-Inference-Pid")
        if qualification_group:
            item["qualification_group"] = qualification_group
        self.timings_ms.append(item)

    def post(self, path: str, body: dict[str, Any], expected: int = 200, qualification_group: str | None = None) -> tuple[int, dict[str, Any]]:
        started = time.monotonic()
        status = None
        response_headers = None
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(body, separators=(",", ":")).encode(),
            headers=self._headers("application/json"),
            method="POST",
        )
        try:
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    status = response.status
                    response_headers = response.headers
                    payload = json.loads(read_bounded(response, self.max_response_bytes))
            except urllib.error.HTTPError as error:
                status = error.code
                payload = json.loads(read_bounded(error, self.max_response_bytes) or b"{}")
        finally:
            self._record_timing("post", path, started, response_headers if status == 200 else None, qualification_group if status == 200 else None)
        if status != expected:
            raise AssertionError(f"POST {path} returned {status}, expected {expected}: {payload}")
        return status, payload

    def stream(self, path: str, body: dict[str, Any], qualification_group: str | None = None) -> list[dict[str, Any]]:
        started = time.monotonic()
        response_headers = None
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(body, separators=(",", ":")).encode(),
            headers=self._headers("text/event-stream"),
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                response_headers = response.headers
                if response.status != 200:
                    raise AssertionError(f"stream returned {response.status}")
                return parse_sse(read_bounded(response, self.max_response_bytes))
        finally:
            self._record_timing("stream", path, started, response_headers, qualification_group)

    def _headers(self, accept: str) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": accept}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers


def run(args: argparse.Namespace, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    run_started = time.monotonic()
    client = Client(args.base_url, args.timeout, args.token, args.max_response_bytes)
    source = fixture_messages(max(args.filler_turns, args.target_input_tokens // 48))
    frozen_source = json.dumps(source, sort_keys=True, separators=(",", ":"))
    tools = [{"type": "function", "function": {"name": "lookup_fact", "description": "Look up a fixture fact", "parameters": {"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"]}}}]
    common = {"model": args.model, "tools": tools, "tool_choice": "auto", "chat_template_kwargs": {"enable_thinking": False}, "max_tokens": args.max_tokens, "max_input_tokens": args.target_input_tokens, "keep_recent_turns": 2}
    if evidence is None:
        evidence = {}
    # The campaign owns this record before requests start, so failed cases keep
    # their response and timing evidence instead of disappearing from `runs`.
    evidence.update({"schema_version": 2, "qualification": "candidate_evidence", "started_unix_ms": int(time.time() * 1000), "base_url": args.base_url, "model": args.model, "operator_backend_expectation": args.backend, "target_input_tokens": args.target_input_tokens, "max_tokens": args.max_tokens, "cases": {}, "evidence_passed": False, "request_timings_ms": client.timings_ms})

    window_request = {**common, "mode": "window", "messages": copy.deepcopy(source)}
    _, window = client.post("/ai/v1/chat/context/prepare", window_request)
    validate_window(source, window, args.target_input_tokens)
    if json.dumps(source, sort_keys=True, separators=(",", ":")) != frozen_source:
        raise AssertionError("qualification source fixture was mutated")
    evidence["cases"]["window"] = {"request_sha256": hashlib.sha256(json.dumps(window_request, sort_keys=True).encode()).hexdigest(), "context": window["context"], "retained_roles": [m["role"] for m in window["messages"]]}

    generation_request = {"model": args.model, "messages": window["messages"], "tools": tools, "tool_choice": "auto", "chat_template_kwargs": {"enable_thinking": False}, "max_tokens": args.max_tokens, "temperature": 0, "context_management": {"mode": "reject", "max_input_tokens": args.target_input_tokens}, "kv_cache": {"mode": "exact_hybrid", "version": "v1"}}
    _, generated = client.post("/ai/v1/generate", generation_request, qualification_group="prepared_generation")
    validate_managed(generated, target=args.target_input_tokens, max_tokens=args.max_tokens, checkpoint=False)
    if generated["context"]["original_input_tokens"] != window["context"]["effective_input_tokens"]:
        raise AssertionError("prepared/generation rendered-token parity failed")
    evidence["cases"]["generation_parity"] = {"context": generated["context"], "usage": generated["usage"]}

    compact_request = {**common, "mode": "compact", "messages": copy.deepcopy(source)}
    _, compacted = client.post("/ai/v1/chat/context/prepare", compact_request, qualification_group="compaction")
    checkpoint = compacted.get("checkpoint")
    evidence["cases"]["compact"] = {"context": compacted.get("context"), "checkpoint": checkpoint if isinstance(checkpoint, dict) else {}}
    if not checkpoint:
        raise AssertionError("compact did not return a checkpoint")
    validate_checkpoint(checkpoint)
    validate_fixture_memory(checkpoint)
    evidence["model_revision"] = checkpoint["model_revision"]
    evidence["tokenizer_revision"] = checkpoint["tokenizer_revision"]
    evidence["template_revision"] = checkpoint["template_revision"]

    active = [m for m in compacted["messages"] if m["role"] in {"system", "developer"}]
    active.append({"role": "user", "content": "New suffix only: repeat the verified codename."})
    if "FACT_MARKER_CTX_V1" in json.dumps(active):
        raise AssertionError("archived conversation was replayed in active suffix")
    consume = {"model": args.model, "messages": active, "tools": tools, "tool_choice": "auto", "chat_template_kwargs": {"enable_thinking": False}, "max_tokens": args.max_tokens, "temperature": 0, "context_management": {"mode": "reject", "checkpoint": checkpoint, "max_input_tokens": args.target_input_tokens}, "kv_cache": {"mode": "exact_hybrid", "version": "v1"}}
    _, consumed = client.post("/ai/v1/generate", consume, qualification_group="checkpoint_generation")
    validate_managed(consumed, target=args.target_input_tokens, max_tokens=args.max_tokens, checkpoint=True)
    answer = " ".join(str(choice.get("message", {}).get("content", "")) for choice in consumed.get("choices", []))
    if "amber-ant" not in answer.lower():
        raise AssertionError("checkpoint consumption answer lost the known codename")
    evidence["cases"]["checkpoint_consume"] = {"active_messages": active, "context": consumed["context"], "usage": consumed["usage"]}

    tampered = copy.deepcopy(checkpoint)
    tampered["memory"]["conversation"] += " tampered"
    tamper_request = {**consume, "context_management": {**consume["context_management"], "checkpoint": tampered}}
    status, rejected = client.post("/ai/v1/generate", tamper_request, expected=409)
    evidence["cases"]["tampered_memory"] = {"status": status, "response": rejected}

    recompact_filler_turns = max(args.filler_turns, args.target_input_tokens // 48)
    recompact_messages = active + fixture_messages(recompact_filler_turns)[2:]
    recompact_messages.append({"role": "user", "content": "Preserve lineage and the verified codename."})
    _, recompact = client.post("/ai/v1/chat/context/prepare", {**common, "mode": "compact", "messages": recompact_messages, "checkpoint": checkpoint}, qualification_group="recompaction")
    child = recompact.get("checkpoint")
    if not child or not child.get("parent_checkpoint_sha256"):
        raise AssertionError("recompaction did not record parent checkpoint lineage")
    evidence["cases"]["recompaction"] = {"context": recompact["context"], "checkpoint": child}
    if child["covered_messages"] <= checkpoint["covered_messages"]:
        raise AssertionError("recompaction coverage did not advance")
    validate_checkpoint(child)
    validate_fixture_memory(child)

    # Exercise the child checkpoint after recompaction, with no archived
    # messages replayed into the independent active suffix.
    stream_request = {**consume, "stream": True, "context_management": {**consume["context_management"], "checkpoint": child}}
    stream_events = client.stream("/ai/v1/generate", stream_request, qualification_group="checkpoint_stream_generation")
    streamed_answer = validate_stream(stream_events, target=args.target_input_tokens, max_tokens=args.max_tokens, checkpoint=True)
    if "amber-ant" not in streamed_answer.lower():
        raise AssertionError("streamed checkpoint answer lost the known codename")
    metadata_index = next(i for i, event in enumerate(stream_events) if "context" in event)
    token_index = next((i for i, event in enumerate(stream_events) if event.get("choices")), None)
    evidence["cases"]["streaming"] = {"checkpoint_memory_sha256": child["memory_sha256"], "metadata_index": metadata_index, "first_token_index": token_index, "event_count": len(stream_events), "context": stream_events[metadata_index]["context"]}
    evidence["finished_unix_ms"] = int(time.time() * 1000)
    evidence["duration_ms"] = round((time.monotonic() - run_started) * 1000, 3)
    evidence["request_timings_ms"] = client.timings_ms
    evidence["evidence_passed"] = True
    return evidence


def latency_summary(values: list[float]) -> dict[str, float | int]:
    mean = statistics.fmean(values) if values else 0.0
    return {
        "count": len(values),
        "mean": round(mean, 3),
        "p50": round(percentile(values, 0.50), 3) if values else 0.0,
        "p95": round(percentile(values, 0.95), 3) if values else 0.0,
        "max": round(max(values), 3) if values else 0.0,
        "cv": round(statistics.pstdev(values) / mean, 6) if len(values) > 1 and mean else 0.0,
    }


def evaluate_qualification(
    runs: list[dict[str, Any]],
    args: argparse.Namespace,
    provenance: dict[str, Any],
    memory: dict[str, Any] | None,
    campaign_started_s: float,
    campaign_finished_s: float,
    campaign_started_monotonic_ns: int,
    campaign_finished_monotonic_ns: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    gates: list[dict[str, Any]] = []

    def gate(name: str, passed: bool, observed: Any, required: Any) -> None:
        gates.append({"name": name, "passed": bool(passed), "observed": observed, "required": required})

    request_latencies = [
        timing["elapsed_ms"] for item in runs for timing in item.get("request_timings_ms", [])
    ]
    durations = [item["duration_ms"] for item in runs if "duration_ms" in item]
    request_stats = latency_summary(request_latencies)
    duration_stats = latency_summary(durations)
    grouped_latencies: dict[str, list[float]] = {}
    for item in runs:
        for timing in item.get("request_timings_ms", []):
            group = timing.get("qualification_group")
            if group:
                grouped_latencies.setdefault(group, []).append(timing["elapsed_ms"])
    group_stats = {name: latency_summary(values) for name, values in grouped_latencies.items()}
    max_group_cv = max((stats["cv"] for stats in group_stats.values()), default=0.0)
    revisions = {
        (item.get("model_revision"), item.get("tokenizer_revision"), item.get("template_revision"))
        for item in runs
    }
    memory_digests = {
        item.get("cases", {}).get("compact", {}).get("checkpoint", {}).get("memory_sha256")
        for item in runs
    }
    hardware = provenance.get("hardware_evidence") or {}
    device = hardware.get("device")
    source = provenance.get("source") or {}
    binary = provenance.get("server_binary") or {}

    gate("qualification_mode", args.qualification_mode, args.qualification_mode, True)
    gate("repeated_runs", len(runs) >= 3, len(runs), ">=3")
    gate("functional_cases", len(runs) == args.runs and all(item.get("evidence_passed") for item in runs), sum(bool(item.get("evidence_passed")) for item in runs), args.runs)
    gate("stable_revisions", len(revisions) == 1 and None not in next(iter(revisions), (None,)), sorted(str(value) for value in revisions), "one complete revision tuple")
    gate("deterministic_memory", len(memory_digests) == 1 and None not in memory_digests, sorted(str(value) for value in memory_digests), "one memory digest")
    gate("latency_threshold_supplied", args.max_p95_latency_ms is not None, args.max_p95_latency_ms, "explicit positive milliseconds")
    gate("request_p95_latency", args.max_p95_latency_ms is not None and request_stats["p95"] <= args.max_p95_latency_ms, request_stats["p95"], args.max_p95_latency_ms)
    gate("stability_threshold_supplied", args.max_latency_cv is not None, args.max_latency_cv, "explicit nonnegative CV")
    gate("model_request_latency_stability", args.max_latency_cv is not None and len(runs) >= 2 and bool(group_stats) and all(stats["count"] == len(runs) for stats in group_stats.values()) and max_group_cv <= args.max_latency_cv, {"max_cv": max_group_cv, "groups": sorted(group_stats)}, args.max_latency_cv)
    gate("source_snapshot", bool(source.get("head") and source.get("status_sha256") and source.get("tracked_diff_sha256") and source.get("untracked_content_sha256") and source.get("source_snapshot_sha256")), source, "HEAD plus status, tracked diff, untracked content, and composite snapshot SHA256")
    gate("server_binary", bool(binary.get("sha256") and binary.get("size_bytes")), binary, "binary path, size, and SHA256")
    successful_generates = [
        timing for item in runs for timing in item.get("request_timings_ms", [])
        if timing.get("path") == "/ai/v1/generate" and "inference_backend" in timing
    ]
    allowed_generation_routes = {"compiled-whole-model", "compiled-partition", "scheduler-eager", "direct-eager"}
    expected_generates = 3 * len(runs)
    gate("generation_route_headers", len(successful_generates) == expected_generates and expected_generates > 0, len(successful_generates), expected_generates)
    gate("backend_executed", bool(successful_generates) and all(item.get("inference_backend") == args.backend for item in successful_generates), [item.get("inference_backend") for item in successful_generates], f"{args.backend} on every successful generation")
    gate("generation_route_instrumented", bool(successful_generates) and all(item.get("inference_route") in allowed_generation_routes for item in successful_generates), [item.get("inference_route") for item in successful_generates], sorted(allowed_generation_routes))
    gate("memory_samples", bool(memory and memory.get("samples", 0) >= 2), None if memory is None else memory.get("samples"), ">=2")
    server_pid = args.server_pid or hardware.get("server_pid")
    gate("generation_server_pid", bool(successful_generates and server_pid) and all(item.get("inference_pid") == str(server_pid) for item in successful_generates), [item.get("inference_pid") for item in successful_generates], str(server_pid) if server_pid else "probed server PID on every successful generation")
    gate("memory_campaign_coverage", bool(memory and memory.get("first_monotonic_ns", math.inf) <= campaign_started_monotonic_ns and memory.get("last_monotonic_ns", -math.inf) >= campaign_finished_monotonic_ns), None if memory is None else [memory.get("first_monotonic_ns"), memory.get("last_monotonic_ns")], [campaign_started_monotonic_ns, campaign_finished_monotonic_ns])
    gate("memory_wallclock_coverage", bool(memory and memory.get("first_wall_time_s", math.inf) <= campaign_started_s and memory.get("last_wall_time_s", -math.inf) >= campaign_finished_s), None if memory is None else [memory.get("first_wall_time_s"), memory.get("last_wall_time_s")], [campaign_started_s, campaign_finished_s])
    if args.backend == "cuda":
        devices = provenance.get("cuda_devices") or []
        gate("linux_cuda_host", provenance.get("machine", {}).get("system") == "Linux", provenance.get("machine"), "Linux")
        gate("cuda_device_identity", bool(devices and all(item.get("uuid") and item.get("compute_capability") for item in devices)), devices, "nvidia-smi UUID and compute capability")
        gate("memory_process_identity", bool(memory and server_pid and memory.get("pid") == server_pid), None if memory is None else memory.get("pid"), server_pid)
        gate("cuda_sampler_clean", bool(memory is not None and not memory.get("errors")), None if memory is None else memory.get("errors"), [])
        gate("cuda_process_allocation", bool(memory and memory.get("gpu_uuids") and set(memory["gpu_uuids"]).issubset({item["uuid"] for item in devices})), None if memory is None else memory.get("gpu_uuids"), "tested PID allocation on enumerated CUDA device")
        gate("gpu_memory_threshold_supplied", args.max_gpu_memory_bytes is not None, args.max_gpu_memory_bytes, "explicit positive bytes")
        gate("peak_gpu_memory", bool(memory and args.max_gpu_memory_bytes is not None and memory.get("peak_gpu_memory_bytes", math.inf) <= args.max_gpu_memory_bytes), None if memory is None else memory.get("peak_gpu_memory_bytes"), args.max_gpu_memory_bytes)
        gate("host_memory_threshold_supplied", args.max_host_rss_bytes is not None, args.max_host_rss_bytes, "explicit positive bytes")
        gate("peak_host_rss", bool(memory and args.max_host_rss_bytes is not None and memory.get("peak_host_rss_bytes", math.inf) <= args.max_host_rss_bytes), None if memory is None else memory.get("peak_host_rss_bytes"), args.max_host_rss_bytes)
    else:
        gate("darwin_arm64", provenance.get("machine", {}).get("system") == "Darwin" and provenance.get("machine", {}).get("machine") == "arm64", provenance.get("machine"), "Darwin arm64")
        gate("hardware_probe_schema", hardware.get("schema") == "antfly.macos_hardware_evidence.v1", hardware.get("schema"), "antfly.macos_hardware_evidence.v1")
        gate("metal_device_identity", bool(device and (not isinstance(device, dict) or device.get("name"))), device, "nonempty Metal device identity")
        gate("memory_threshold_supplied", args.max_peak_phys_footprint_bytes is not None, args.max_peak_phys_footprint_bytes, "explicit positive bytes")
        gate("memory_process_identity", bool(memory and server_pid and memory.get("pids") == [server_pid] and len(memory.get("process_lifetimes", [])) == 1), None if memory is None else {"pids": memory.get("pids"), "lifetimes": memory.get("process_lifetimes")}, {"server_pid": server_pid, "lifetimes": 1})
        gate("peak_physical_footprint", bool(memory and args.max_peak_phys_footprint_bytes is not None and memory.get("conservative_peak_phys_footprint_bytes", math.inf) <= args.max_peak_phys_footprint_bytes), None if memory is None else memory.get("conservative_peak_phys_footprint_bytes"), args.max_peak_phys_footprint_bytes)
    return gates, {"request_latency_ms": request_stats, "run_duration_ms": duration_stats, "comparable_request_groups": group_stats, "max_comparable_request_cv": max_group_cv}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--model", required=True)
    parser.add_argument("--backend", choices=("metal", "cuda"), default="metal")
    parser.add_argument("--target-input-tokens", type=int, default=1024)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--filler-turns", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--max-response-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--max-artifact-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--qualification-mode", action="store_true")
    parser.add_argument("--max-p95-latency-ms", type=float)
    parser.add_argument("--max-latency-cv", type=float)
    parser.add_argument("--max-peak-phys-footprint-bytes", type=int)
    parser.add_argument("--max-gpu-memory-bytes", type=int)
    parser.add_argument("--max-host-rss-bytes", type=int)
    parser.add_argument("--server-pid", type=int)
    parser.add_argument("--server-binary", type=Path)
    parser.add_argument("--hardware-evidence", "--runtime-evidence", dest="hardware_evidence", type=Path)
    parser.add_argument("--memory-samples", type=Path)
    parser.add_argument("--token")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    if args.max_p95_latency_ms is not None and args.max_p95_latency_ms <= 0:
        parser.error("--max-p95-latency-ms must be positive")
    if args.max_latency_cv is not None and args.max_latency_cv < 0:
        parser.error("--max-latency-cv must be nonnegative")
    if args.max_peak_phys_footprint_bytes is not None and args.max_peak_phys_footprint_bytes <= 0:
        parser.error("--max-peak-phys-footprint-bytes must be positive")
    if args.max_gpu_memory_bytes is not None and args.max_gpu_memory_bytes <= 0:
        parser.error("--max-gpu-memory-bytes must be positive")
    if args.max_host_rss_bytes is not None and args.max_host_rss_bytes <= 0:
        parser.error("--max-host-rss-bytes must be positive")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[5]
    hardware = load_json(args.hardware_evidence)
    provenance_exclusions = {
        path for path in (args.output, args.hardware_evidence, args.memory_samples, args.server_binary)
        if path is not None
    }
    provenance: dict[str, Any] = {
        "source": git_provenance(repo, provenance_exclusions),
        "harness": {"path": str(Path(__file__).resolve()), "sha256": sha256_file(Path(__file__).resolve())},
        "machine": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": sys.version,
        },
        "hardware_evidence": hardware,
    }
    if args.backend == "cuda":
        provenance["cuda_devices"] = cuda_device_evidence()
    if args.hardware_evidence:
        provenance["hardware_evidence_file"] = {"path": str(args.hardware_evidence.resolve()), "sha256": sha256_file(args.hardware_evidence)}
    if args.server_binary:
        provenance["server_binary"] = {
            "path": str(args.server_binary.resolve()),
            "size_bytes": args.server_binary.stat().st_size,
            "sha256": sha256_file(args.server_binary),
        }
    cuda_sampler = CudaProcessSampler(args.server_pid) if args.backend == "cuda" and args.server_pid else None
    if cuda_sampler:
        cuda_sampler.start()
    campaign_started_s = time.time()
    campaign_started_monotonic_ns = time.monotonic_ns()
    runs: list[dict[str, Any]] = []
    run_error: Exception | None = None
    try:
        for run_index in range(args.runs):
            item = {"run_index": run_index, "evidence_passed": False}
            runs.append(item)
            run_started = time.monotonic()
            run(args, item)
    except Exception as error:
        run_error = error
        if runs:
            runs[-1].update({
                "evidence_passed": False,
                "error_type": type(error).__name__,
                "error": str(error),
                "finished_unix_ms": round(time.time() * 1000),
                "duration_ms": round((time.monotonic() - run_started) * 1000, 3),
            })
    campaign_finished_monotonic_ns = time.monotonic_ns()
    campaign_finished_s = time.time()
    memory = cuda_sampler.finish() if cuda_sampler else wait_for_memory_evidence(args.memory_samples, campaign_finished_monotonic_ns)
    gates, aggregate = evaluate_qualification(
        runs, args, provenance, memory, campaign_started_s, campaign_finished_s,
        campaign_started_monotonic_ns, campaign_finished_monotonic_ns,
    )
    qualified = all(item["passed"] for item in gates)
    evidence = {
        "schema_version": 2,
        "qualification": "production_gate" if args.qualification_mode else "candidate_evidence",
        "qualification_scope": {
            "api": ["POST /ai/v1/chat/context/prepare", "POST /ai/v1/generate"],
            "context_policy": ["window", "compact", "checkpoint_consume"],
            "kv_cache": {"mode": "exact_hybrid", "version": "v1"},
            "backend": args.backend,
        },
        "explicitly_unqualified": [
            "select_v1 KV selection",
            "sampled or approximate KV policies",
            "accelerator kernels outside the exercised HTTP request paths",
            "models, revisions, devices, binaries, and source snapshots other than those recorded",
        ],
        "qualified": qualified,
        "evidence_passed": len(runs) == args.runs and all(item.get("evidence_passed") for item in runs),
        "started_unix_ms": round(campaign_started_s * 1000),
        "finished_unix_ms": round(campaign_finished_s * 1000),
        "base_url": args.base_url,
        "model": args.model,
        "operator_backend_expectation": args.backend,
        "thresholds": {
            "max_p95_latency_ms": args.max_p95_latency_ms,
            "max_latency_cv": args.max_latency_cv,
            "max_peak_phys_footprint_bytes": args.max_peak_phys_footprint_bytes,
            "max_gpu_memory_bytes": args.max_gpu_memory_bytes,
            "max_host_rss_bytes": args.max_host_rss_bytes,
        },
        "provenance": provenance,
        "memory": memory,
        "aggregate": aggregate,
        "gates": gates,
        "runs": runs,
    }
    if run_error:
        evidence["error_type"] = type(run_error).__name__
        evidence["error"] = str(run_error)
    encoded = (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode()
    if len(encoded) > args.max_artifact_bytes:
        raise AssertionError(f"evidence artifact exceeded {args.max_artifact_bytes} bytes")
    args.output.write_bytes(encoded)
    print(args.output)
    if run_error and not args.qualification_mode:
        raise run_error
    return 0 if (not args.qualification_mode or qualified) else 1


if __name__ == "__main__":
    raise SystemExit(main())
