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

"""Measure llama.cpp Metal OCR precision and memory tradeoffs on macOS."""

from __future__ import annotations

import argparse
import base64
import ctypes
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
from html.parser import HTMLParser
from http.client import HTTPException
from itertools import groupby
import json
import os
from pathlib import Path
import platform
import re
import signal
import socket
import uuid
import statistics
import subprocess
import tempfile
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from benchmark_corpus import cer, normalize, plain_text


GROUNDING_BOX = re.compile(
    r"!\[[a-z_]+\+?\]\(\s*(\d{1,4})\s*,\s*(\d{1,4})\s*,"
    r"\s*(\d{1,4})\s*,\s*(\d{1,4})\s*\)"
)


@dataclass(frozen=True)
class Profile:
    name: str
    precision: str = "Q4_K_M"
    vision: str = "F16"
    context_per_slot: int = 8192
    ubatch: int = 512
    image_max_tokens: int | None = None
    cache_k: str = "f16"
    cache_v: str = "f16"


def profiles():
    baseline = Profile("q4")
    return [
        replace(baseline, name="bf16", precision="BF16"),
        baseline,
        replace(baseline, name="q8", precision="Q8_0"),
        replace(baseline, name="q4-ctx4096", context_per_slot=4096),
        replace(baseline, name="q4-ubatch256", ubatch=256),
        replace(baseline, name="q4-image1024", image_max_tokens=1024),
        replace(baseline, name="q4-q8-k", cache_k="q8_0"),
        replace(baseline, name="q4-q8-v", cache_v="q8_0"),
        replace(baseline, name="q4-q8-kv", cache_k="q8_0", cache_v="q8_0"),
        replace(baseline, name="q4-q8-vision", vision="Q8_0"),
        replace(
            baseline,
            name="q4-lean",
            context_per_slot=4096,
            ubatch=256,
            image_max_tokens=2048,
        ),
    ]


def read_json(url, body=None, timeout=120, *, on_submit=None):
    data = None if body is None else json.dumps(body).encode()
    request = Request(url, data=data, headers={"Content-Type": "application/json"})
    if on_submit is not None:
        on_submit()
    try:
        with urlopen(request, timeout=timeout) as result:
            return json.load(result)
    except HTTPException as error:
        raise OSError(f"HTTP protocol error for {request.selector}: {error}") from error


def memory_peak(log, label):
    value = re.search(rf"^\s*(\d+)\s+{re.escape(label)}$", log, re.MULTILINE)
    return int(value[1]) if value else None


class RusageInfoV2(ctypes.Structure):
    # Darwin sys/resource.h, RUSAGE_INFO_V2.
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (field, ctypes.c_uint64)
        for field in (
            "user_time",
            "system_time",
            "pkg_idle_wkups",
            "interrupt_wkups",
            "pageins",
            "wired_size",
            "resident_size",
            "phys_footprint",
            "proc_start_abstime",
            "proc_exit_abstime",
            "child_user_time",
            "child_system_time",
            "child_pkg_idle_wkups",
            "child_interrupt_wkups",
            "child_pageins",
            "child_elapsed_abstime",
            "diskio_bytesread",
            "diskio_byteswritten",
        )
    ]


def process_memory(pid):
    usage = RusageInfoV2()
    if libproc.proc_pid_rusage(pid, 2, ctypes.byref(usage)) != 0:
        return None
    return {
        "rss_bytes": usage.resident_size,
        "physical_footprint_bytes": usage.phys_footprint,
    }


def child_pid(parent):
    result = subprocess.run(
        ["pgrep", "-P", str(parent)], capture_output=True, text=True
    )
    children = result.stdout.split()
    return int(children[0]) if len(children) == 1 else None


def signal_pass_group(proc, sig):
    try:
        os.killpg(proc.pid, sig)
    except ProcessLookupError:
        pass


def shutdown_pass(proc, record_memory):
    try:
        pid = child_pid(proc.pid) if record_memory else proc.pid
    except OSError:
        pid = None
    # Let /usr/bin/time reap the server and write its counters when possible.
    if pid is not None:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pid = None
    if pid is None:
        signal_pass_group(proc, signal.SIGTERM)
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        signal_pass_group(proc, signal.SIGKILL)
        proc.wait(timeout=10)
    else:
        # The launcher can exit while server descendants still own the listener.
        signal_pass_group(proc, signal.SIGKILL)


def wait_ready(proc, url, alias):
    deadline = time.monotonic() + 120
    detail = "no readiness response"
    while proc.poll() is None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            read_json(url + "/health", timeout=min(1, remaining))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            models = read_json(url + "/v1/models", timeout=min(1, remaining))
            entries = models.get("data") if isinstance(models, dict) else None
            if not isinstance(entries, list) or not all(
                isinstance(item, dict) for item in entries
            ):
                raise ValueError("invalid /v1/models response")
            if (
                any(model.get("id") == alias for model in entries)
                and proc.poll() is None
            ):
                return
            detail = f"/v1/models did not advertise {alias}"
        except (OSError, ValueError) as error:
            detail = str(error)
        time.sleep(max(0, min(0.2, deadline - time.monotonic())))
    raise RuntimeError(f"llama-server failed to become ready: {detail}")


def request_case(url, case, alias, prompt="", *, on_submit=None):
    content = [{"type": "image_url", "image_url": {"url": case["uri"]}}]
    if prompt:
        content.append({"type": "text", "text": prompt})
    body = {
        "model": alias,
        "messages": [{"role": "user", "content": content}],
        "max_tokens": case["max_new_tokens"],
        "temperature": 0,
        "cache_prompt": False,
    }
    start = time.perf_counter()
    try:
        reply = read_json(url + "/v1/chat/completions", body, on_submit=on_submit)
        choice = reply["choices"][0]
        text = choice["message"]["content"] or ""
        finish = choice["finish_reason"]
        if not isinstance(text, str):
            raise ValueError("OCR response content is not text")
    except HTTPError as error:
        with error:
            try:
                message = error.read(1500).decode(errors="replace")
            except (OSError, HTTPException) as body_error:
                message = f"Could not read error response: {body_error}"
        return {"case": case["name"], "http_status": error.code, "message": message}
    except (OSError, ValueError, KeyError, IndexError, TypeError) as error:
        return {
            "case": case["name"],
            "seconds": time.perf_counter() - start,
            "error": str(error),
        }
    seconds = time.perf_counter() - start
    return {
        "case": case["name"],
        "prompt": prompt,
        "seconds": seconds,
        "finish": finish,
        "usage": reply.get("usage"),
        "text": text,
    }


class HtmlTables(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self.rows = None
        self.row = None
        self.cell = None
        self.spanned = False

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.rows, self.row, self.cell = [], None, None
            self.spanned = False
        elif self.rows is not None and tag == "tr":
            self.row, self.cell = [], None
        elif self.row is not None and tag in ("th", "td"):
            self.cell = []
            self.spanned |= any(
                key in ("colspan", "rowspan") and value != "1" for key, value in attrs
            )

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("th", "td") and self.cell is not None:
            self.row.append(normalize("".join(self.cell)))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            self.rows.append(self.row)
            self.row, self.cell = None, None
        elif tag == "table" and self.rows is not None:
            if not self.spanned:
                self.tables.append(self.rows)
            self.rows, self.row, self.cell = None, None, None


def parsed_tables(text):
    parser = HtmlTables()
    parser.feed(text)
    parser.close()
    for is_table, block in groupby(text.splitlines(), key=lambda line: "|" in line):
        if not is_table:
            continue
        rows = [
            [
                normalize(cell)
                for cell in line.strip().removeprefix("|").removesuffix("|").split("|")
            ]
            for line in block
        ]
        if (
            len(rows) >= 2
            and len(rows[0]) == len(rows[1])
            and all(re.fullmatch(r":?-{3,}:?", cell) for cell in rows[1])
        ):
            parser.tables.append([rows[0], *rows[2:]])
    return parser.tables


def score_case(row, case):
    if "text" not in row:
        return
    text = row["text"]
    expected = case["expected"]
    boxes = [tuple(map(int, box)) for box in GROUNDING_BOX.findall(text)]
    valid_boxes = {
        (x0, y0, x1, y1)
        for x0, y0, x1, y1 in boxes
        if 0 <= x0 < x1 <= 1000 and 0 <= y0 < y1 <= 1000
    }
    transcript = GROUNDING_BOX.sub("", text)
    table_rows = case.get("expected_table_rows", [])
    content = transcript
    if table_rows:
        content = re.sub(r"(?m)^\s*\|?[ :|\-]+\|?\s*$", "", content).replace("|", " ")
    plain = plain_text(content)
    tables = parsed_tables(transcript) if table_rows else []
    expected_cells = [[normalize(cell) for cell in row] for row in table_rows]
    table_matches = not table_rows or expected_cells in tables
    row.update(
        exact=normalize(expected) == normalize(transcript),
        cer=cer(expected, plain),
        table_matches=table_matches,
        missing=[term for term in case["required"] if term not in transcript],
        has_table_markup="<table" in text.lower()
        or bool(re.search(r"(?m)^\s*\|.+\|\s*$", text)),
        has_grounding=bool(valid_boxes),
        grounding_boxes=len(boxes),
        valid_grounding_boxes=len(valid_boxes),
    )


def quality_passes(row):
    return (
        row.get("finish") == "stop"
        and row.get("cer", float("inf")) <= 0.05
        and not row.get("missing")
        and row.get("table_matches", True)
    )


def grounding_quality_passes(row, case):
    return (
        quality_passes(row)
        and row["valid_grounding_boxes"] >= case.get("min_grounding_boxes", 1)
        and row["grounding_boxes"] == row["valid_grounding_boxes"]
    )


ACTIVE_MEMORY_PHASES = ("serve", "grounding")


def active_slot_tasks(url, expected_slots):
    slots = read_json(url + "/slots", timeout=1)
    if (
        not isinstance(slots, list)
        or len(slots) != expected_slots
        or not all(
            isinstance(slot, dict)
            and type(slot.get("id")) is int
            and isinstance(slot.get("is_processing"), bool)
            and (not slot["is_processing"] or type(slot.get("id_task")) is int)
            for slot in slots
        )
    ):
        raise ValueError(f"expected {expected_slots} valid llama.cpp slot records")
    if len({slot["id"] for slot in slots}) != expected_slots:
        raise ValueError("llama.cpp /slots returned duplicate slot IDs")
    return {(slot["id"], slot["id_task"]) for slot in slots if slot["is_processing"]}


def read_active_memory(read_memory, read_active_tasks):
    before = read_active_tasks()
    if not before:
        return None, 0
    values = read_memory()
    # Shared task IDs give a lower bound on occupancy throughout the read.
    return values, len(before & read_active_tasks()) if values else 0


def update_peaks(peaks, values):
    for key, value in values.items():
        peaks[key] = max(peaks.get(key, 0), value)


def record_phase_sample(
    peaks, task_peaks, current_phase, read_memory, read_active_tasks
):
    phase = current_phase()
    active = phase[0] in ACTIVE_MEMORY_PHASES
    if active:
        values, tasks = read_active_memory(read_memory, read_active_tasks)
        if not tasks:
            return
    else:
        values = read_memory()
    if not values or phase != current_phase():
        return
    if (
        active
        and values.get("rss_bytes", 0) > 0
        and values.get("physical_footprint_bytes", 0) > 0
    ):
        update_peaks(task_peaks.setdefault(phase[0], {}).setdefault(tasks, {}), values)
    update_peaks(peaks.setdefault(phase[0], {}), values)


def missing_memory_phases(peaks, phases):
    return [
        phase
        for phase in phases
        if any(
            peaks.get(phase, {}).get(metric, 0) <= 0
            for metric in ("rss_bytes", "physical_footprint_bytes")
        )
    ]


def memory_measurement_valid(peaks, phases, peak_rss, peak_footprint):
    return bool(
        peak_rss and peak_rss > 0 and peak_footprint and peak_footprint > 0
    ) and not missing_memory_phases(peaks, phases)


def required_memory_phases(args, grounding_case):
    phases = [] if args.grounding_only else ["serve"]
    if grounding_case is not None:
        phases.append("grounding")
    return phases


def required_memory_occupancy(args, cases, grounding_case, parallel):
    requested = {}
    if not args.grounding_only:
        requested["serve"] = min(parallel, len(cases) * args.rounds)
    if grounding_case is not None:
        requested["grounding"] = 1
    return requested


def memory_occupancy_report(task_peaks, required):
    highest = {phase: max(task_peaks.get(phase, {}), default=0) for phase in required}
    missing = [phase for phase, count in required.items() if highest[phase] < count]
    return {
        "required_verified_tasks_by_phase": required,
        "max_verified_tasks_by_phase": highest,
        "sampled_peak_bytes_by_phase_and_minimum_verified_occupancy": task_peaks,
        "missing_memory_occupancy_phases": missing,
    }


def run_profile(args, cases, grounding_case, size, profile, parallel):
    try:
        return measure_profile(args, cases, grounding_case, size, profile, parallel)
    except (OSError, subprocess.SubprocessError) as error:
        return {
            "qualified": False,
            "quality_qualified": False,
            "memory_measurement_valid": False,
            "model": size,
            "profile": asdict(profile),
            "parallel": parallel,
            "completed_pages": 0,
            "quality_pages": 0,
            "requests": 0,
            "artifact_bytes": None,
            "peak_rss_bytes": None,
            "peak_physical_footprint_bytes": None,
            "sampled_peak_bytes_by_phase": {},
            "missing_memory_phases": required_memory_phases(args, grounding_case),
            **memory_occupancy_report(
                {}, required_memory_occupancy(args, cases, grounding_case, parallel)
            ),
            "warm_idle": None,
            "slot_probe_errors": {},
            "memory_workload_qualified": False,
            "memory_error": None,
            "seconds": None,
            "pages_per_second": None,
            "p50_seconds": None,
            "mean_cer": None,
            "backend_buffer_report": [],
            "grounding": None,
            "startup_error_log": None,
            "memory_error_log": None,
            "error": str(error),
            "results": [],
        }


def measure_profile(args, cases, grounding_case, size, profile, parallel):
    model = args.models_dir / f"LightOnOCR-3-{size}-{profile.precision}.gguf"
    projector = args.models_dir / f"mmproj-LightOnOCR-3-{size}-{profile.vision}.gguf"
    if not model.is_file() or not projector.is_file():
        raise FileNotFoundError(
            f"Missing weights for {size}/{profile.name}: {model}, {projector}"
        )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", args.port))
        port = listener.getsockname()[1]
    command = [
        args.binary,
        "--model",
        str(model),
        "--mmproj",
        str(projector),
        "--reasoning",
        "off",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "-ngl",
        "99",
        "--parallel",
        str(parallel),
        "--ctx-size",
        str(profile.context_per_slot * parallel),
        "--ubatch-size",
        str(profile.ubatch),
        "--no-cache-prompt",
        "--cache-ram",
        "0",
        "--cache-type-k",
        profile.cache_k,
        "--cache-type-v",
        profile.cache_v,
        "--flash-attn",
        "on",
    ]
    if profile.image_max_tokens is not None:
        command += ["--image-max-tokens", str(profile.image_max_tokens)]
    url = f"http://127.0.0.1:{port}"
    timing = run_pass(
        args, cases, grounding_case, parallel, command, url, record_memory=False
    )
    memory = run_pass(
        args, cases, grounding_case, parallel, command, url, record_memory=True
    )
    scored, ok, quality_pages, quality_qualified = score_pass(
        args, cases, grounding_case, timing.rows, timing.grounding, timing.error
    )
    _, _, _, memory_workload_qualified = score_pass(
        args, cases, grounding_case, memory.rows, memory.grounding, memory.error
    )

    peak_rss = memory_peak(memory.log, "maximum resident set size")
    peak_footprint = memory_peak(memory.log, "peak memory footprint")
    phases = required_memory_phases(args, grounding_case)
    memory_valid = memory_measurement_valid(
        memory.sampled_peaks, phases, peak_rss, peak_footprint
    )
    occupancy = memory_occupancy_report(
        memory.task_peaks,
        required_memory_occupancy(args, cases, grounding_case, parallel),
    )
    memory_valid = memory_valid and not occupancy["missing_memory_occupancy_phases"]
    return {
        "qualified": quality_qualified and memory_workload_qualified and memory_valid,
        "quality_qualified": quality_qualified,
        "memory_measurement_valid": memory_valid,
        "model": size,
        "profile": asdict(profile),
        "parallel": parallel,
        "artifact_bytes": model.stat().st_size + projector.stat().st_size,
        "peak_rss_bytes": peak_rss,
        "peak_physical_footprint_bytes": peak_footprint,
        "sampled_peak_bytes_by_phase": memory.sampled_peaks,
        "missing_memory_phases": missing_memory_phases(memory.sampled_peaks, phases),
        **occupancy,
        "warm_idle": memory.idle,
        "slot_probe_errors": memory.slot_probe_errors,
        "memory_workload_qualified": memory_workload_qualified,
        "memory_error": memory.error,
        "seconds": timing.elapsed,
        "completed_pages": len(ok),
        "quality_pages": len(quality_pages),
        "requests": len(timing.rows),
        "pages_per_second": len(ok) / timing.elapsed if timing.elapsed else None,
        "p50_seconds": statistics.median(row["seconds"] for row in ok) if ok else None,
        "mean_cer": statistics.mean(row["cer"] for row in scored) if scored else None,
        "backend_buffer_report": [
            line for line in memory.log.splitlines() if "buffer size" in line.lower()
        ],
        "grounding": timing.grounding,
        "error": timing.error,
        "results": timing.rows,
        "startup_error_log": timing.log[-3000:] if timing.error else None,
        "memory_error_log": memory.log[-3000:] if memory.error else None,
    }


@dataclass
class BenchmarkPass:
    rows: list[dict]
    grounding: dict | None
    elapsed: float | None
    idle: dict | None
    sampled_peaks: dict
    task_peaks: dict
    slot_probe_errors: dict
    error: str | None
    log: str


def run_pass(args, cases, grounding_case, parallel, command, url, *, record_memory):
    alias = "ocr-" + uuid.uuid4().hex
    command = [*command, "--alias", alias]
    phase = ("load", 0)
    phase_changed = threading.Condition()
    sampled_peaks = {}
    task_peaks = {}
    slot_probe_errors = {}

    def read_slots():
        return active_slot_tasks(url, parallel)

    def set_phase(name):
        nonlocal phase
        with phase_changed:
            if phase[0] != name:
                phase = (name, phase[1] + 1)
                phase_changed.notify()

    rows = []
    idle = None
    elapsed = None
    grounding = None
    failure = None

    output = ""
    try:
        with tempfile.TemporaryFile() as log:
            if record_memory:
                command = ["/usr/bin/time", "-l", *command, "--slots"]
            else:
                command = [*command, "--no-slots"]
            proc = subprocess.Popen(
                command, stdout=log, stderr=log, start_new_session=True
            )

            def sample():
                pid = None

                def read_memory():
                    nonlocal pid
                    pid = pid or child_pid(proc.pid)
                    return process_memory(pid) if pid else None

                while True:
                    with phase_changed:
                        observed = phase
                        if observed[0] == "finished":
                            return
                    try:
                        record_phase_sample(
                            sampled_peaks,
                            task_peaks,
                            lambda: phase,
                            read_memory,
                            read_slots,
                        )
                    except (OSError, ValueError) as error:
                        entry = slot_probe_errors.setdefault(
                            observed[0], {"count": 0, "first_error": str(error)}
                        )
                        entry["count"] += 1
                    # Wake on HTTP submission even if the periodic poll just ran.
                    interval = 0.01 if observed[0] in ACTIVE_MEMORY_PHASES else 0.10
                    with phase_changed:
                        phase_changed.wait_for(lambda: phase != observed, interval)

            sampler = threading.Thread(target=sample) if record_memory else None
            if sampler is not None:
                sampler.start()
            try:
                wait_ready(proc, url, alias)
                set_phase("warmup")
                warmup = request_case(url, cases[0], alias)
                if warmup.get("finish") != "stop":
                    raise RuntimeError(f"Warm-up request failed: {warmup}")
                set_phase("idle")
                if record_memory:
                    try:
                        read_slots()
                    except (OSError, ValueError) as error:
                        slot_probe_errors["preflight"] = {
                            "count": 1,
                            "first_error": str(error),
                        }
                        raise RuntimeError(
                            f"llama.cpp /slots preflight failed: {error}"
                        ) from error
                    pid = child_pid(proc.pid)
                    idle = process_memory(pid) if pid else None
                serve_submit = (lambda: set_phase("serve")) if record_memory else None
                grounding_submit = (
                    (lambda: set_phase("grounding")) if record_memory else None
                )
                if not args.grounding_only:
                    with ThreadPoolExecutor(max_workers=parallel) as pool:
                        start = time.perf_counter()
                        rows = list(
                            pool.map(
                                lambda case: request_case(
                                    url, case, alias, on_submit=serve_submit
                                ),
                                cases * args.rounds,
                            )
                        )
                        elapsed = time.perf_counter() - start
                        set_phase("idle")
                if grounding_case:
                    grounding = request_case(
                        url,
                        grounding_case,
                        alias,
                        "grounding",
                        on_submit=grounding_submit,
                    )
                    set_phase("idle")
            except (OSError, RuntimeError, ValueError, KeyError) as error:
                failure = str(error)
            finally:
                set_phase("finished")
                if sampler is not None:
                    sampler.join()
                shutdown_pass(proc, record_memory)
                log.seek(0)
                output = log.read().decode(errors="replace")
    except (OSError, subprocess.SubprocessError) as error:
        failure = (
            str(error) if failure is None else f"{failure}; cleanup failed: {error}"
        )
    return BenchmarkPass(
        rows=rows,
        grounding=grounding,
        elapsed=elapsed,
        idle=idle,
        sampled_peaks=sampled_peaks,
        task_peaks=task_peaks,
        slot_probe_errors=slot_probe_errors,
        error=failure,
        log=output,
    )


def score_pass(args, cases, grounding_case, rows, grounding, failure):
    for row, case in zip(rows, cases * args.rounds):
        score_case(row, case)
    if grounding is not None:
        score_case(grounding, grounding_case)
    scored = [row for row in rows if "cer" in row]
    ok = [row for row in scored if row["finish"] == "stop" and not row["missing"]]
    quality_pages = [row for row in ok if quality_passes(row)]
    layout_ok = grounding_case is None or (
        grounding is not None and grounding_quality_passes(grounding, grounding_case)
    )
    quality_qualified = (
        failure is None
        and bool(layout_ok)
        and (args.grounding_only or len(quality_pages) == len(cases) * args.rounds)
    )
    return scored, ok, quality_pages, quality_qualified


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="llama-server")
    parser.add_argument("--models-dir", required=True, type=Path)
    parser.add_argument(
        "--cases", type=Path, default=Path(__file__).with_name("corpus") / "cases.json"
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--size", nargs="+", choices=["0.8B", "4B"], default=["0.8B", "4B"]
    )
    parser.add_argument("--profile", nargs="+", choices=[p.name for p in profiles()])
    parser.add_argument("--parallel", nargs="+", type=int, default=[1, 4])
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--grounding", action="store_true")
    parser.add_argument("--grounding-only", action="store_true")
    parser.add_argument("--grounding-case", default="receipt")
    args = parser.parse_args()
    if platform.system() != "Darwin":
        parser.error("This harness uses macOS process footprint counters")
    global libproc
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    libproc.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    libproc.proc_pid_rusage.restype = ctypes.c_int
    if min(args.parallel) < 1 or args.rounds < 1:
        parser.error("parallel and rounds must be positive")
    cases = json.loads(args.cases.read_text())["cases"]
    if not cases:
        parser.error("The corpus must contain at least one case")
    for case in cases:
        table_rows = case.get("expected_table_rows", [])
        if not isinstance(table_rows, list) or any(
            not isinstance(row, list)
            or not row
            or not all(isinstance(cell, str) for cell in row)
            for row in table_rows
        ):
            parser.error(
                "expected_table_rows must contain cell lists; regenerate older dense fixtures"
            )
        image = (args.cases.parent / case["image"]).read_bytes()
        case["uri"] = "data:image/png;base64," + base64.b64encode(image).decode()
    grounding_case = None
    if args.grounding or args.grounding_only:
        grounding_case = next(
            (case for case in cases if case["name"] == args.grounding_case), None
        )
        if grounding_case is None:
            parser.error(f"Unknown grounding case: {args.grounding_case}")
    report = {
        "platform": platform.platform(),
        "llama_version": subprocess.run(
            [args.binary, "--version"], capture_output=True, text=True, check=True
        ).stderr.strip(),
        "cases": str(args.cases),
        "rounds": args.rounds,
        "grounding_case": args.grounding_case if grounding_case else None,
        "grounding_only": args.grounding_only,
        "active_memory_sampling": "llama.cpp slot probes bracket process samples",
        "timing_sampling": "separate server process without slot probes",
        "runs": [],
    }
    for size in args.size:
        for parallel in args.parallel:
            for profile in profiles():
                if args.profile and profile.name not in args.profile:
                    continue
                run = run_profile(args, cases, grounding_case, size, profile, parallel)
                report["runs"].append(run)
                pending_output = args.output.with_name(args.output.name + ".tmp")
                pending_output.write_text(json.dumps(report, indent=2) + "\n")
                pending_output.replace(args.output)
                print(
                    size,
                    parallel,
                    profile.name,
                    "qualified" if run["qualified"] else "incomplete",
                    run["completed_pages"],
                    run["requests"],
                    run["peak_physical_footprint_bytes"],
                    run["error"] or run["memory_error"] or "",
                    (
                        "missing phase samples="
                        + ",".join(run["missing_memory_phases"])
                        if run["missing_memory_phases"]
                        else ""
                    ),
                    (
                        "missing verified occupancy="
                        + ",".join(run["missing_memory_occupancy_phases"])
                        if run["missing_memory_occupancy_phases"]
                        else ""
                    ),
                    flush=True,
                )
    if any(not run["qualified"] for run in report["runs"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
