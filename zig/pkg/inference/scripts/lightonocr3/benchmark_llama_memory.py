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
import json
import os
from pathlib import Path
import platform
import re
import signal
import statistics
import subprocess
import tempfile
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from benchmark_corpus import cer, normalize, plain_text


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


def read_json(url, body=None, timeout=120):
    data = None if body is None else json.dumps(body).encode()
    request = Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=timeout) as result:
        return json.load(result)


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


def wait_ready(proc, url):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and proc.poll() is None:
        try:
            read_json(url + "/health", timeout=1)
            return
        except (URLError, TimeoutError):
            time.sleep(0.2)
    raise RuntimeError("llama-server failed to become ready")


def request_case(url, case, prompt=""):
    content = [{"type": "image_url", "image_url": {"url": case["uri"]}}]
    if prompt:
        content.append({"type": "text", "text": prompt})
    body = {
        "model": "ocr",
        "messages": [{"role": "user", "content": content}],
        "max_tokens": case["max_new_tokens"],
        "temperature": 0,
        "cache_prompt": False,
    }
    start = time.perf_counter()
    try:
        reply = read_json(url + "/v1/chat/completions", body)
    except HTTPError as error:
        return {
            "case": case["name"],
            "http_status": error.code,
            "message": error.read().decode()[:1500],
        }
    seconds = time.perf_counter() - start
    choice = reply["choices"][0]
    text = choice["message"]["content"] or ""
    return {
        "case": case["name"],
        "prompt": prompt,
        "seconds": seconds,
        "finish": choice["finish_reason"],
        "usage": reply.get("usage"),
        "text": text,
    }


def score_case(row, case):
    if "text" not in row:
        return
    text = row["text"]
    expected = case["expected"]
    row.update(
        exact=normalize(expected) == normalize(text),
        cer=cer(expected, plain_text(text)),
        missing=[term for term in case["required"] if term not in text],
        has_table_markup="<table" in text.lower()
        or bool(re.search(r"(?m)^\s*\|.+\|\s*$", text)),
        has_grounding=bool(re.search(r"<\|?(?:bbox|box|region)", text, re.I)),
    )


def run_profile(args, cases, grounding_case, size, profile, parallel):
    model = args.models_dir / f"LightOnOCR-3-{size}-{profile.precision}.gguf"
    projector = args.models_dir / f"mmproj-LightOnOCR-3-{size}-{profile.vision}.gguf"
    if not model.is_file() or not projector.is_file():
        raise FileNotFoundError(
            f"Missing weights for {size}/{profile.name}: {model}, {projector}"
        )
    command = [
        args.binary,
        "--model",
        str(model),
        "--mmproj",
        str(projector),
        "--alias",
        "ocr",
        "--reasoning",
        "off",
        "--host",
        "127.0.0.1",
        "--port",
        str(args.port),
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
    url = f"http://127.0.0.1:{args.port}"
    phase = "load"
    sampled_peaks = {}
    finished = threading.Event()
    rows = []
    idle = None
    elapsed = None
    grounding = None
    failure = None

    with tempfile.TemporaryFile() as log:
        proc = subprocess.Popen(
            ["/usr/bin/time", "-l", *command], stdout=log, stderr=log
        )

        def sample():
            pid = None
            while not finished.is_set():
                pid = pid or child_pid(proc.pid)
                values = process_memory(pid) if pid else None
                if values:
                    peaks = sampled_peaks.setdefault(phase, {})
                    for key, value in values.items():
                        peaks[key] = max(peaks.get(key, 0), value)
                finished.wait(0.10)

        sampler = threading.Thread(target=sample)
        sampler.start()
        try:
            wait_ready(proc, url)
            phase = "warmup"
            request_case(url, cases[0])
            pid = child_pid(proc.pid)
            idle = process_memory(pid) if pid else None
            if not args.grounding_only:
                phase = "serve"
                with ThreadPoolExecutor(max_workers=parallel) as pool:
                    start = time.perf_counter()
                    rows = list(
                        pool.map(
                            lambda case: request_case(url, case), cases * args.rounds
                        )
                    )
                    elapsed = time.perf_counter() - start
            if grounding_case:
                phase = "grounding"
                grounding = request_case(url, grounding_case, "grounding")
        except (OSError, RuntimeError, ValueError, KeyError) as error:
            failure = str(error)
        finally:
            finished.set()
            sampler.join()
            pid = child_pid(proc.pid)
            if pid:
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                if pid:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                proc.wait(timeout=10)
            log.seek(0)
            output = log.read().decode(errors="replace")
    for row, case in zip(rows, cases * args.rounds):
        score_case(row, case)
    if grounding is not None:
        score_case(grounding, grounding_case)
    scored = [row for row in rows if "cer" in row]
    ok = [row for row in scored if row["finish"] == "stop" and not row["missing"]]
    return {
        "model": size,
        "profile": asdict(profile),
        "parallel": parallel,
        "artifact_bytes": model.stat().st_size + projector.stat().st_size,
        "peak_rss_bytes": memory_peak(output, "maximum resident set size"),
        "peak_physical_footprint_bytes": memory_peak(output, "peak memory footprint"),
        "sampled_peak_bytes_by_phase": sampled_peaks,
        "warm_idle": idle,
        "seconds": elapsed,
        "completed_pages": len(ok),
        "requests": len(rows),
        "pages_per_second": len(ok) / elapsed if elapsed else None,
        "p50_seconds": statistics.median(row["seconds"] for row in ok) if ok else None,
        "mean_cer": statistics.mean(row["cer"] for row in scored) if scored else None,
        "backend_buffer_report": [
            line for line in output.splitlines() if "buffer size" in line.lower()
        ],
        "grounding": grounding,
        "error": failure,
        "results": rows,
        "startup_error_log": output[-3000:] if failure else None,
    }


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
    parser.add_argument("--port", type=int, default=18494)
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
    for case in cases:
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
        "runs": [],
    }
    for size in args.size:
        for parallel in args.parallel:
            for profile in profiles():
                if args.profile and profile.name not in args.profile:
                    continue
                run = run_profile(args, cases, grounding_case, size, profile, parallel)
                report["runs"].append(run)
                args.output.write_text(json.dumps(report, indent=2) + "\n")
                print(
                    size,
                    parallel,
                    profile.name,
                    run["completed_pages"],
                    run["requests"],
                    run["peak_physical_footprint_bytes"],
                    run["error"] or "",
                    flush=True,
                )
    if any(run["error"] for run in report["runs"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
