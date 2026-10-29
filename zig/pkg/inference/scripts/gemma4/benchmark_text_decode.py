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

"""Small warm HTTP decode benchmark for profiling and paired engine checks.

Runs one resident server at a time. Never use profiler-instrumented results as
headline throughput. Token counts come from authoritative terminal SSE usage.
"""
import argparse
from datetime import datetime, timezone
import math
import hashlib
import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import time
import urllib.request

import qualify_transcription as q


def process_tree(pid):
    """Include the native supervisor's GPU worker, not unrelated GPU services."""
    found = {pid}
    pending = [pid]
    while pending:
        try:
            children = Path(f"/proc/{pending.pop()}/task").glob("*/children")
            for path in children:
                for child in map(int, path.read_text().split()):
                    if child not in found:
                        found.add(child)
                        pending.append(child)
        except (FileNotFoundError, ProcessLookupError):
            pass
    return found


def audit_gpu_activity(log, owned_pids, samples):
    """pmon timestamps label one-second samples; retain a conservative margin."""
    start = math.floor(min(s["started_at_unix"] for s in samples)) - 1
    end = math.ceil(max(s["finished_at_unix"] for s in samples)) + 1
    timestamps, competing, unknown = set(), [], []
    for line in log.splitlines():
        cols = line.split()
        if len(cols) < 7 or line.lstrip().startswith("#"):
            continue
        try:
            stamp = datetime.strptime(cols[0] + cols[1], "%Y%m%d%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            pid = int(cols[3])
        except ValueError:
            continue
        if not start <= stamp <= end:
            continue
        timestamps.add(stamp)
        if pid in owned_pids or "C" not in cols[4]:
            continue
        record = {"timestamp": stamp, "pid": pid, "sm_percent": cols[5]}
        if cols[5] == "-":
            unknown.append(record)
        elif int(cols[5]) > 0:
            competing.append(record)
    return {"observed_seconds": len(timestamps), "owned_pids": sorted(owned_pids),
            "competing_activity": competing, "unknown_external_activity": unknown,
            "quiet": len(timestamps) >= 2 and not competing and not unknown,
            "scope": "measured samples plus a conservative one-second margin; graphics processes excluded"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=["native", "llama"], default="native")
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--model", default=q.DEFAULT_MODEL)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tokens", type=int, default=256)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--prompt-file", type=Path)
    parser.add_argument("--request-json", default="{}")
    parser.add_argument("--monitor-gpu", action="store_true", help="Record timestamped per-process NVIDIA GPU activity")
    parser.add_argument("--require-quiet-gpu", action="store_true", help="Reject runs with competing or unknown compute activity (implies monitoring)")
    args = parser.parse_args()
    if args.tokens < 2 or args.warmups < 0 or args.repeats < 1:
        parser.error("invalid output budget or sample count")
    options = json.loads(args.request_json)
    prompt = args.prompt_file.read_text() if args.prompt_file else None
    args.output.mkdir(parents=True, exist_ok=False)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    binary = str(args.binary.resolve())
    if args.engine == "native":
        command = [binary, "run", "--host", "127.0.0.1", "--port", str(port),
                   "--models-dir", str(args.models_dir.resolve()), "--preload-model", f"generator:cuda:{args.model}",
                   "--max-work-units", "256", "--max-concurrent-requests", "2"]
        route, health = "/ai/v1/generate", "/healthz"
        options.setdefault("prompt_cache", False)
    else:
        directory = args.models_dir.resolve() / "generators" / args.model
        cache = options.pop("cache_dtype", "f32")
        command = [binary, "-m", str(directory / "gemma-4-E4B_q4_0-it.gguf"),
                   "--host", "127.0.0.1", "--port", str(port), "-ngl", "99", "-np", "1",
                   "-c", "8192", "-b", "2048", "-ub", "2048", "-ctk", cache, "-ctv", cache,
                   "-t", "8", "--reasoning-format", "none"]
        route, health = "/v1/chat/completions", "/health"
        options.setdefault("cache_prompt", False)
    report = {"engine": args.engine, "model": args.model, "binary_sha256": q.file_sha(args.binary),
              "harness_sha256": q.file_sha(Path(__file__)), "command": command,
              "environment": {k: v for k, v in os.environ.items() if k.startswith(("ANTFLY_", "TERMITE_", "CUDA_", "GGML_"))},
              "request_options": options, "prompt": prompt, "gpu_before": q.gpu_snapshot(), "generation": []}
    process = monitor = None
    owned_pids = set()
    with (args.output / "gpu-activity.log").open("wb") as gpu_log, (args.output / "server.log").open("wb") as log:
        try:
            if args.monitor_gpu or args.require_quiet_gpu:
                monitor = subprocess.Popen(["nvidia-smi", "pmon", "-s", "um", "-d", "1", "-o", "DT"],
                    stdout=gpu_log, stderr=subprocess.STDOUT, env={**os.environ, "TZ": "UTC"})
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 180
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"Server exited {process.returncode}")
                try:
                    with urllib.request.urlopen(base + health, timeout=1):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise RuntimeError("Server startup timed out")
                    time.sleep(0.25)
            owned_pids = process_tree(process.pid)
            for i in range(args.warmups + args.repeats):
                started = time.time()
                result = q.sse_generate(base, args.model, args.tokens, route=route, extra_body=options, prompt=prompt)
                result["started_at_unix"] = started
                result["finished_at_unix"] = time.time()
                result["warmup"] = i < args.warmups
                report["generation"].append(result)
                q.save(args.output / "report.json", report)
                print(f"sample {i}: {result['decode_tokens_per_second']:.3f} decode tok/s; {result['usage']}", flush=True)
            samples = [r for r in report["generation"] if not r["warmup"]]
            report["summary"] = {k: statistics.median(r[k] for r in samples) for k in [
                "decode_tokens_per_second", "end_to_end_tokens_per_second", "ttft_seconds"]}
            report["summary"]["text_hashes"] = sorted({hashlib.sha256(r["text"].encode()).hexdigest() for r in samples})
            report["summary"]["deterministic_text"] = len(report["summary"]["text_hashes"]) == 1
            report["summary"]["completion_tokens"] = [r["usage"]["completion_tokens"] for r in samples]
            if not all(r["usage"]["completion_tokens"] == args.tokens for r in samples):
                raise RuntimeError("Measured completion did not reach the requested token budget")
            if not report["summary"]["deterministic_text"]:
                raise RuntimeError("Greedy output changed across repeated requests")
        except Exception as error:
            report["error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            if monitor is not None:
                time.sleep(1.1)
                if monitor.poll() is None:
                    monitor.terminate()
                monitor.wait(timeout=10)
                gpu_log.flush()
                samples = [r for r in report["generation"] if not r["warmup"]]
                if samples:
                    report["gpu_activity"] = audit_gpu_activity((args.output / "gpu-activity.log").read_text(), owned_pids, samples)
                else:
                    report["gpu_activity"] = {"quiet": False, "reason": "no measured samples"}
            report["gpu_after"] = q.gpu_snapshot()
            q.save(args.output / "report.json", report)
    if args.require_quiet_gpu and not report.get("gpu_activity", {}).get("quiet"):
        raise RuntimeError("GPU isolation check failed; run retained as diagnostic evidence")


if __name__ == "__main__":
    main()
