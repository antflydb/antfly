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

"""Alternate resident Gemma E4B HTTP servers on identical GGUF/media fixtures.

Fixture JSON is a list of {id, max_tokens, warmup?, calls: [{text, image?, audio?}]}.
Media paths resolve relative to the fixture. Responses, timings and sampled
process-tree PSS/RSS/GPU peaks are retained; no prefix-cache hits are measured.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import json
import mimetypes
import os
from pathlib import Path
import signal
import socket
import statistics
import subprocess
import threading
import time


def sha256(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def host_snapshot():
    value = {"time": time.time(), "load_average": os.getloadavg()}
    try:
        value["gpu"] = subprocess.check_output([
            "nvidia-smi", "--query-gpu=name,utilization.gpu,temperature.gpu,clocks.sm,power.draw",
            "--format=csv,noheader",
        ], text=True, timeout=5).strip().splitlines()
    except (OSError, subprocess.SubprocessError) as error:
        value["telemetry_error"] = str(error)
    return value


def request(port, path, body=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=900)
    started = time.perf_counter()
    try:
        conn.request("POST" if body is not None else "GET", path,
                     body=json.dumps(body) if body is not None else None,
                     headers={"Content-Type": "application/json"})
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {response.read(4096)!r}")
        if body is None:
            return json.loads(response.read())
        text, reasoning, first, usage, finish, done = [], [], None, None, None, False
        for line in response:
            if not line.startswith(b"data:"):
                continue
            value = line[5:].strip()
            if value == b"[DONE]":
                done = True
                break
            event = json.loads(value)
            if event.get("error"):
                raise RuntimeError(event["error"])
            usage = event.get("usage") or usage
            for choice in event.get("choices") or []:
                delta = choice.get("delta") or {}
                content, thought = delta.get("content") or "", delta.get("reasoning_content") or ""
                if first is None and (content or thought):
                    first = time.perf_counter()
                text.append(content)
                reasoning.append(thought)
                finish = choice.get("finish_reason") or finish
        ended = time.perf_counter()
        count = (usage or {}).get("completion_tokens")
        count_valid = isinstance(count, int) and (count == body["max_tokens"] or (
            body.get("ignore_eos", True) is False and finish == "stop" and 0 < count < body["max_tokens"]))
        if not done or first is None or not count_valid:
            raise RuntimeError(f"Incomplete stream or output count: done={done}, usage={usage}, finish={finish}")
        return {"ms": (ended-started)*1000, "ttft_ms": (first-started)*1000,
                "decode_tok_s": (usage["completion_tokens"]-1)/(ended-first),
                "usage": usage, "finish": finish, "text": "".join(text), "reasoning": "".join(reasoning)}
    finally:
        conn.close()


class MemorySampler:
    def __init__(self, process, path):
        import psutil
        import pynvml
        pynvml.nvmlInit()
        self.psutil, self.nvml = psutil, pynvml
        self.handles = [pynvml.nvmlDeviceGetHandleByIndex(i) for i in range(pynvml.nvmlDeviceGetCount())]
        self.process, self.path = process, path
        self.stop_event = threading.Event()
        self.peak_rss_bytes = self.peak_gpu_bytes = self.peak_pss_bytes = self.peak_footprint_bytes = self.samples = 0
        self.errors = []
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        with self.path.open("w") as out:
            while not self.stop_event.is_set():
                pids, rss, pss = set(), 0, 0
                try:
                    root = self.psutil.Process(self.process.pid)
                    processes = [root, *root.children(recursive=True)]
                except self.psutil.NoSuchProcess:
                    processes = []
                for process in processes:
                    try:
                        pids.add(process.pid)
                        for line in Path(f"/proc/{process.pid}/smaps_rollup").read_text().splitlines():
                            if line.startswith("Pss:"):
                                pss += int(line.split()[1]) * 1024
                            elif line.startswith("Rss:"):
                                rss += int(line.split()[1]) * 1024
                    except (FileNotFoundError, ProcessLookupError):
                        continue
                    except OSError as error:
                        self.errors.append(repr(error))
                try:
                    gpu = 0
                    for handle in self.handles:
                        for process in self.nvml.nvmlDeviceGetComputeRunningProcesses(handle):
                            if process.pid in pids:
                                used = process.usedGpuMemory
                                if not isinstance(used, int) or not 0 <= used < (1 << 60):
                                    raise RuntimeError("NVML memory unavailable")
                                gpu += used
                    self.peak_gpu_bytes = max(self.peak_gpu_bytes, gpu)
                    self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
                    self.peak_pss_bytes = max(self.peak_pss_bytes, pss)
                    self.peak_footprint_bytes = max(self.peak_footprint_bytes, pss + gpu)
                    self.samples += 1
                    out.write(json.dumps({"time": time.time(), "rss_bytes": rss, "pss_bytes": pss,
                                          "gpu_bytes": gpu, "footprint_bytes": pss + gpu})+"\n")
                except Exception as error:
                    self.errors.append(repr(error))
                self.stop_event.wait(0.2)

    def stop(self):
        self.stop_event.set()
        self.thread.join()
        self.nvml.nvmlShutdown()
        return {"peak_rss_bytes": self.peak_rss_bytes, "peak_gpu_bytes": self.peak_gpu_bytes,
                "peak_pss_bytes": self.peak_pss_bytes, "peak_footprint_bytes": self.peak_footprint_bytes,
                "samples": self.samples, "errors": self.errors,
                "valid": self.samples > 0 and self.peak_pss_bytes > 0 and self.peak_gpu_bytes > 0 and not self.errors,
                "method": "process-tree smaps_rollup PSS plus NVML; 0.2s polling"}


def body_for(args, engine, cell, call):
    content = []
    for kind in ("image", "audio"):
        if kind not in call:
            continue
        path = args.fixtures.parent/call[kind]
        data = base64.b64encode(path.read_bytes()).decode()
        mime = mimetypes.guess_type(path.name)[0]
        if kind == "image":
            content.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{data}"}})
        elif engine == "antfly":
            content.append({"type": "media", "mime_type": "audio/wav", "data": data})
        else:
            content.append({"type": "input_audio", "input_audio": {"format": "wav", "data": data}})
    content.append({"type": "text", "text": call["text"]})
    body = {"messages": [{"role": "user", "content": content}], "temperature": 0,
            "max_tokens": cell["max_tokens"], "ignore_eos": cell.get("ignore_eos", True), "stream": True,
            "stream_options": {"include_usage": True}, "chat_template_kwargs": {"enable_thinking": False}}
    if engine == "antfly":
        body.update(model=args.model.resolve().parent.relative_to(args.model.resolve().parent.parent.parent).as_posix(), backend="cuda", cache_dtype="f16", prompt_cache=False)
    else:
        body["cache_prompt"] = False
    return body


def run_arm(args, engine, lifecycle, cells):
    active = subprocess.check_output(["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True).strip()
    if active:
        raise RuntimeError(f"GPU already has compute processes: {active}")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tag = f"lc{lifecycle}-{engine}"
    if engine == "antfly":
        config_path = args.output/f"{tag}.config.json"
        config_path.write_text(json.dumps({"inference": {"keep_alive": "0"}}))
        command = [str(args.antfly.resolve())]
        if args.antfly.name == "antfly":
            command.append("inference")
        command += ["run", "--models-dir", str(args.model.resolve().parent.parent.parent),
                    "--config", str(config_path.resolve()), "--host-budget-mb", "8192",
                    "--host", "127.0.0.1", "--port", str(port)]
        if args.antfly_scratch_budget_mb is not None:
            command += ["--scratch-budget-mb", str(args.antfly_scratch_budget_mb)]
        endpoint, ready_path = "/ai/v1/generate", "/ai/v1/models"
        cwd = Path.cwd()
    else:
        command = ([str(args.llama_loader.resolve()), "--library-path", args.llama_library_path]
                   if args.llama_loader else [])
        command += [str(args.llama.resolve()), "-m", str(args.model.resolve()), "--mmproj", str(args.mmproj.resolve()),
                    "-ngl", "999", "-c", "8192", "-np", "1", "-ctk", "f16", "-ctv", "f16",
                    "--image-max-tokens", "280", "--jinja", "--no-cache-prompt", "-cram", "0", "--no-webui",
                    "--host", "127.0.0.1", "--port", str(port)]
        if args.llama_image_min_tokens is not None:
            command += ["--image-min-tokens", str(args.llama_image_min_tokens)]
        endpoint, ready_path, cwd = "/v1/chat/completions", "/health", args.llama.resolve().parent
    meta = {"arm": engine, "lc": lifecycle, "command": command, "host_before": host_snapshot()}
    env = os.environ.copy()
    if engine == "antfly":
        env.update(ANTFLY_INFERENCE_REQUIRED_BACKEND="cuda", ANTFLY_INFERENCE_WORKER=str(args.antfly.resolve()))
        env.update(dict(entry.split("=", 1) for entry in args.antfly_env))
    with (args.output/f"{tag}.log").open("w") as log:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        sampler = None
        import psutil
        try:
            sampler = MemorySampler(process, args.output/f"{tag}.memory.jsonl")
            sampler.thread.start()
            deadline = time.monotonic()+300
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"Server exited {process.returncode}; see {log.name}")
                try:
                    request(port, ready_path)
                    break
                except (OSError, RuntimeError):
                    if time.monotonic() > deadline:
                        raise TimeoutError("Server startup")
                    time.sleep(0.25)
            if args.cold_call:
                meta["first_call"] = request(port, endpoint, body_for(args, engine, cells[0], cells[0]["calls"][0]))
            for cell in cells:
                for index in range(cell.get("warmup", args.warmups)):
                    request(port, endpoint, body_for(args, engine, cell, cell["calls"][index % len(cell["calls"])]))
                for repeat in range(args.repeats):
                    for index, call in enumerate(cell["calls"]):
                        row = dict(arm=engine, lc=lifecycle, cell=cell["id"], repeat=repeat, index=index)
                        row.update(request(port, endpoint, body_for(args, engine, cell, call)))
                        with (args.output/"calls.jsonl").open("a") as out:
                            out.write(json.dumps(row)+"\n")
                        print(f"{tag} {cell['id']} {row['ms']:.1f} ms TTFT {row['ttft_ms']:.1f} ms decode {row['decode_tok_s']:.1f} tok/s", flush=True)
        finally:
            try:
                children = psutil.Process(process.pid).children(recursive=True)
            except psutil.NoSuchProcess:
                children = []
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            _, alive = psutil.wait_procs(children, timeout=15)
            for child in alive:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            psutil.wait_procs(alive, timeout=5)
            meta["memory"] = sampler.stop() if sampler is not None else {"valid": False}
            meta["host_after"] = host_snapshot()
            (args.output/f"{tag}.json").write_text(json.dumps(meta, indent=2)+"\n")
    if not meta["memory"]["valid"]:
        raise RuntimeError(f"Invalid memory measurement for {tag}: {meta['memory']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("antfly", "llama", "model", "mmproj", "fixtures", "output"):
        parser.add_argument("--"+key, type=Path, required=True)
    parser.add_argument("--llama-loader", type=Path)
    parser.add_argument("--llama-library-path", default="")
    parser.add_argument("--llama-image-min-tokens", type=int,
                        help="Optional reference image minimum; use 256 to match the fixture's native patch grids with the 280-token maximum")
    parser.add_argument("--antfly-env", action="append", default=[])
    parser.add_argument("--antfly-scratch-budget-mb", type=int,
                        help="Explicit server scratch admission ceiling; actual memory is sampled")
    parser.add_argument("--lifecycles", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--cold-call", action="store_true", help="Record an unmeasured first call before cell warmups")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--arms", nargs="+", choices=("antfly", "llama"), default=["antfly", "llama"])
    args = parser.parse_args()
    if args.lifecycles < 1 or args.warmups < 0 or args.repeats < 1:
        parser.error("invalid repetition counts")
    if args.antfly_scratch_budget_mb is not None and args.antfly_scratch_budget_mb < 1:
        parser.error("scratch budget must be positive")
    if args.llama_image_min_tokens is not None and not 1 <= args.llama_image_min_tokens <= 280:
        parser.error("reference image minimum must be between 1 and 280")
    args.output.mkdir(parents=True, exist_ok=False)
    cells = json.loads(args.fixtures.read_text())
    provenance = {"arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                  "hashes": {key: sha256(getattr(args, key)) for key in ("model", "mmproj", "antfly", "llama", "fixtures")},
                  "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()}
    provenance["harness_sha256"] = sha256(Path(__file__))
    media_paths = {str(call[kind]) for cell in cells for call in cell["calls"]
                   for kind in ("image", "audio") if kind in call}
    provenance["media"] = {name: {"sha256": sha256(args.fixtures.parent / name),
                                "size_bytes": (args.fixtures.parent / name).stat().st_size}
                           for name in sorted(media_paths)}
    provenance["gpu"] = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=name,driver_version,memory.total,compute_cap", "--format=csv,noheader"], text=True).strip()
    provenance["inherited_tuning"] = {key: value for key, value in os.environ.items()
        if key.startswith(("ANTFLY_INFERENCE_CUDA_", "ANTFLY_CUDA_", "TERMITE_CUDA_"))}
    expected = {
        "model": "676c35070db6dbe52f93e9c864ee0fba4eddea94b9c875d9cb10daff453fbaee",
        "mmproj": "7498a37cb619e55f2fcf87eb931f56e99389ed6d432e4c5c66110694c0d65578",
    }
    for key, digest in expected.items():
        if provenance["hashes"][key] != digest:
            raise RuntimeError(f"{key} does not match the pinned E4B benchmark artifact")
    (args.output/"provenance.json").write_text(json.dumps(provenance, indent=2)+"\n")
    for lifecycle in range(args.lifecycles):
        for engine in args.arms if lifecycle % 2 == 0 else reversed(args.arms):
            run_arm(args, engine, lifecycle, cells)
    rows = [json.loads(line) for line in (args.output/"calls.jsonl").read_text().splitlines()]
    summary = []
    for cell in cells:
        for engine in args.arms:
            group = [r for r in rows if r["cell"] == cell["id"] and r["arm"] == engine]
            summary.append(dict(cell=cell["id"], arm=engine, samples=len(group),
                                **{key: statistics.median(r[key] for r in group) for key in ("ms", "ttft_ms", "decode_tok_s")}))
    (args.output/"summary.json").write_text(json.dumps(summary, indent=2)+"\n")


if __name__ == "__main__":
    main()
