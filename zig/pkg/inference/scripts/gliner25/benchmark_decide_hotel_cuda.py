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

"""Compare the Decide loaded-model CUDA API on the release hotel workload.

Native timings exclude HTTP unless --http selects the production HTTP server.
Both arms include preprocessing, inference, decoding and device synchronization.
Each arm runs alone in a fresh process. Outputs and samples are retained.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import random
import selectors
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time

TASKS = {
    "intent": {"labels": ["maintenance", "room_change", "checkout", "billing", "complaint", "information"]},
    "topics": {"labels": ["hvac", "billing", "noise", "cleanliness", "wifi", "staff"], "multi_label": True, "cls_threshold": 0.4},
    "urgent": {"labels": ["yes", "no"]},
}


def cases():
    rng = random.Random(11)
    parts = [
        "Guest in room {r} says the AC has been out since yesterday and they want to move tonight.",
        "The wifi in {r} keeps dropping during video calls; can someone take a look before 9am?",
        "I was charged twice for the minibar on my final bill for room {r}. Please refund one charge.",
        "Loud music from the room next to {r} until 2am again. This is the third night.",
        "What time is checkout? We'd like a late checkout for room {r} if possible.",
        "The bathroom in {r} wasn't cleaned today and there are no fresh towels.",
        "Front desk staff were incredibly kind when our flight was cancelled. Thank you!",
        "Water is leaking from the ceiling in {r} onto the bed. Please send someone immediately.",
    ]
    texts = [" ".join(rng.choice(parts).format(r=rng.randint(100, 1899)) for _ in range(rng.randint(1, 3))) for _ in range(256)]
    return [dict(id=f"decide_b{b}_{i // b}", texts=texts[i:i + b], tasks=TASKS)
            for b, n in [(1, 64), (8, 128), (32, 128)] for i in range(0, n, b)]


def python_worker(args):
    import torch
    from gliner2 import AutoExtractor, Schema
    import importlib.metadata
    torch.set_num_threads(args.threads)
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA unavailable")
    model = AutoExtractor.from_pretrained(str(args.model)).to("cuda").eval()
    schema = Schema.from_dict({"classifications": [{"task": k, **v} for k, v in TASKS.items()]})
    fixture = cases()
    print(json.dumps(dict(event="ready", torch=torch.__version__, gliner2=importlib.metadata.version("gliner2"),
                         transformers=importlib.metadata.version("transformers"), dtype=str(next(model.parameters()).dtype),
                         device=torch.cuda.get_device_name(), threads=torch.get_num_threads(),
                         allow_tf32=torch.backends.cuda.matmul.allow_tf32, matmul_precision=torch.get_float32_matmul_precision())), flush=True)
    for line in sys.stdin:
        cmd = json.loads(line)
        if cmd["op"] == "stop":
            return
        texts = fixture[cmd["case_index"]]["texts"]
        torch.cuda.synchronize()
        start = time.perf_counter_ns()
        with torch.inference_mode():
            if len(texts) == 1:
                result = [model.extract(texts[0], schema, include_spans=True, include_confidence=True)]
            else:
                result = model.batch_extract(texts, schema, batch_size=len(texts), include_spans=True, include_confidence=True)
        torch.cuda.synchronize()
        elapsed = time.perf_counter_ns() - start
        print(json.dumps(dict(duration_ns=elapsed, outputs=result)), flush=True)


def normalized(arm, result):
    if arm == "native" and "decisions" in result:
        return [{name: sorted(config["labels"][s["label_index"]] for s in row["tasks"][i]["selections"])
                 for i, (name, config) in enumerate(TASKS.items())} for row in result["decisions"]]
    def labels(value):
        if isinstance(value, dict):
            return [value["label"]]
        if isinstance(value, list):
            return sorted(v["label"] if isinstance(v, dict) else v for v in value)
        return [value]
    return [{k: labels(row[k]) for k in TASKS} for row in result["outputs"]]


def confidences(arm, result):
    if arm == "native" and "decisions" in result:
        return [{(name, config["labels"][s["label_index"]]): s["probability"]
                 for i, (name, config) in enumerate(TASKS.items()) for s in row["tasks"][i]["selections"]}
                for row in result["decisions"]]
    return [{(name, item["label"]): item["confidence"] for name in TASKS
             for item in (row[name] if isinstance(row[name], list) else [row[name]])}
            for row in result["outputs"]]


def gpu_inventory(nvml):
    """NVML indices are physical indices, independent of CUDA_VISIBLE_DEVICES."""
    devices = []
    for i in range(nvml.nvmlDeviceGetCount()):
        handle = nvml.nvmlDeviceGetHandleByIndex(i)
        devices.append((handle, dict(uuid=nvml.nvmlDeviceGetUUID(handle),
                                    name=nvml.nvmlDeviceGetName(handle),
                                    total_bytes=nvml.nvmlDeviceGetMemoryInfo(handle).total)))
    return devices


def process_gpu_bytes(nvml, devices, pids):
    """Attribute memory by PID across physical GPUs, never by CUDA ordinal."""
    usage = {}
    for handle, info in devices:
        for process in nvml.nvmlDeviceGetComputeRunningProcesses(handle):
            if process.pid not in pids:
                continue
            value = process.usedGpuMemory
            # pynvml converts the C NVML_VALUE_NOT_AVAILABLE sentinel to None
            # in some versions and exposes the unsigned integer in others.
            if value is None or value == 2**64 - 1 or value < 0:
                raise RuntimeError(f"GPU memory unavailable for PID {process.pid} on {info['uuid']}")
            usage[info["uuid"]] = usage.get(info["uuid"], 0) + value
    return usage


def run_arm(args, arm, lc, fixture, selected):
    import pynvml as nvml
    import psutil
    nvml.nvmlInit()
    devices = gpu_inventory(nvml)
    hardware = dict(driver=nvml.nvmlSystemGetDriverVersion(), cpu_count=os.cpu_count(),
                    gpu_inventory=[info for _, info in devices])
    command = ([str(args.native), "--model-dir", str(args.native_model), "--backend", "cuda", "--cases", str(fixture), "--worker", "1"]
               if arm == "native" else [sys.executable, __file__, "--worker", "--model", str(args.model), "--threads", str(args.threads)])
    http_mode = arm == "native" and args.http
    if http_mode:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        command = [str(args.native), "run", "--host", "127.0.0.1", "--port", str(port),
                   "--models-dir", str(args.native_model.parent), "--ml-dir", str(args.out / f"{lc}-state"), "--max-loaded-models", "1"]
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=300)
    log = (args.out / f"{lc}-{arm}.stderr").open("w")
    proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=log if http_mode else subprocess.PIPE, stderr=log, text=True,
                            start_new_session=True,
                            env={**os.environ, "TOKENIZERS_PARALLELISM": "false", "ANTFLY_INFERENCE_REQUIRED_BACKEND": "cuda"})
    peak = {"host_pss_bytes": 0, "gpu_bytes": 0, "combined_bytes": 0}
    stop = threading.Event()
    monitor_errors = []
    observed_gpus = set()
    def monitor():
        while not stop.is_set():
            try:
                pids = {proc.pid, *(p.pid for p in psutil.Process(proc.pid).children(recursive=True))}
                host = 0
                for pid in pids:
                    try:
                        lines = Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines()
                        host += int(next(l for l in lines if l.startswith("Pss:")).split()[1]) * 1024
                    except (FileNotFoundError, ProcessLookupError):
                        pass
                usage = process_gpu_bytes(nvml, devices, pids)
                observed_gpus.update(usage)
                device = sum(usage.values())
                for key, value in zip(peak, [host, device, host + device]):
                    peak[key] = max(peak[key], value)
            except (FileNotFoundError, ProcessLookupError, psutil.NoSuchProcess):
                pass
            except Exception as exc:
                monitor_errors.append(repr(exc))
                return
            stop.wait(0.02)
    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    selector = selectors.DefaultSelector()
    if not http_mode:
        selector.register(proc.stdout, selectors.EVENT_READ)
    def receive():
        if not selector.select(timeout=300):
            raise TimeoutError(f"{arm} response timeout: inspect {log.name}")
        line = proc.stdout.readline()
        if not line:
            raise RuntimeError(f"{arm} exited: inspect {log.name}")
        return json.loads(line)
    all_cases = cases()
    def call(index):
        if http_mode:
            start = time.perf_counter_ns()
            case = all_cases[index]
            schema = {"classifications": [dict(name=k, labels=v["labels"],
                      **({"multi_label": True, "threshold": v["cls_threshold"]} if v.get("multi_label") else {})) for k, v in TASKS.items()]}
            body = dict(schema_version=2, model=args.native_model.name, schema=schema,
                        inputs=[dict(content=t) for t in case["texts"]], options=dict(include_spans=True, include_confidence=True))
            client.request("POST", "/ai/v1/extract", json.dumps(body), {"Content-Type": "application/json"})
            response = client.getresponse()
            data = json.loads(response.read())
            elapsed = time.perf_counter_ns() - start
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}: {data}")
            outputs = []
            for row in data["data"]:
                output = {k: [] for k in TASKS}
                for c in row["classifications"]:
                    output[c["name"]].append(dict(label=c["label"], confidence=c["score"]))
                outputs.append(output)
            return dict(duration_ns=elapsed, outputs=outputs, http_response=data)
        proc.stdin.write(json.dumps(dict(request_id=index, op="run", case_index=index)) + "\n")
        proc.stdin.flush()
        return receive()
    rows = []
    try:
        if http_mode:
            deadline = time.monotonic() + 60
            while True:
                try:
                    client.request("GET", "/healthz")
                    response = client.getresponse()
                    response.read()
                    if response.status != 200:
                        raise OSError(f"health HTTP {response.status}")
                    break
                except OSError:
                    client.close()
                    if proc.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError(f"HTTP server did not start: {log.name}")
                    time.sleep(0.1)
            ready = dict(event="ready", backend_required="cuda", transport="http", port=port)
        else:
            ready = receive()
        warmed = set()
        with (args.out / f"{lc}-{arm}.jsonl").open("w") as raw:
            for index, case in selected:
                batch = len(case["texts"])
                if batch not in warmed:
                    for _ in range(args.warmup):
                        call(index)
                    warmed.add(batch)
                response = call(index)
                if arm == "native" and not http_mode and response["cuda_transfers"]["kernel_launches"] == 0:
                    raise RuntimeError("native worker reported no CUDA work")
                row = dict(arm=arm, lifecycle=lc, index=index, batch=batch, ms=response["duration_ns"] / 1e6,
                           labels=normalized(arm, response), response=response)
                if len(row["labels"]) != batch:
                    raise RuntimeError("incomplete batch output")
                raw.write(json.dumps(row) + "\n")
                raw.flush()
                rows.append(row)
        if not http_mode:
            proc.stdin.write('{"request_id":0,"op":"stop"}\n')
            proc.stdin.flush()
            proc.wait(timeout=30)
            if proc.returncode:
                raise RuntimeError(f"{arm} exit {proc.returncode}")
    finally:
        if http_mode:
            client.close()
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
        stop.set()
        thread.join()
        selector.close()
        log.close()
        nvml.nvmlShutdown()
    if monitor_errors:
        raise RuntimeError(monitor_errors)
    if not observed_gpus or peak["gpu_bytes"] == 0:
        raise RuntimeError("No process GPU memory observed; refusing an unverified CUDA memory result")
    used_devices = [info for _, info in devices if info["uuid"] in observed_gpus]
    hardware.update(gpu=", ".join(info["name"] for info in used_devices),
                    gpu_uuids=[info["uuid"] for info in used_devices],
                    gpu_total_bytes=sum(info["total_bytes"] for info in used_devices))
    (args.out / f"{lc}-{arm}-metadata.json").write_text(json.dumps(dict(command=command, ready=ready, hardware=hardware, peak=peak), indent=2))
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--model", type=Path, required=True)
    p.add_argument("--native", type=Path)
    p.add_argument("--native-model", type=Path, help="Native artifact directory; defaults to --model (Python reference)")
    p.add_argument("--allow-label-differences", action="store_true", help="Report quantization disagreements without treating them as exact-parity failures")
    p.add_argument("--out", type=Path)
    p.add_argument("--worker", action="store_true")
    p.add_argument("--http", action="store_true", help="Native binary is the production antfly-inference-http-server")
    p.add_argument("--arms", nargs="+", choices=["native", "python"], default=["native", "python"])
    p.add_argument("--batches", nargs="+", type=int, default=[1, 8, 32])
    p.add_argument("--lifecycles", type=int, default=3)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument("--limit", type=int, default=0, help="Calls per batch cell; 0 runs the entire release workload")
    p.add_argument("--threads", type=int, default=2)
    args = p.parse_args()
    if args.lifecycles < 1 or args.warmup < 0 or args.limit < 0 or not args.batches or any(b not in (1, 8, 32) for b in args.batches):
        p.error("positive lifecycles, nonnegative warmup/limit and batches 1, 8, 32 required")
    args.model = args.model.resolve()
    args.native_model = (args.native_model or args.model).resolve()
    if args.worker:
        python_worker(args)
        return
    bench_lock = open("/tmp/antfly-decide-cuda.lock", "a")
    fcntl.flock(bench_lock, fcntl.LOCK_EX)
    if args.out is None or ("native" in args.arms and args.native is None):
        p.error("--out and (for native) --native are required")
    if args.native:
        args.native = args.native.resolve()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    def sha(path):
        with path.open("rb") as f:
            return hashlib.file_digest(f, "sha256").hexdigest()
    provenance = dict(
        source_head=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        source_diff_sha256=hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"])).hexdigest(),
        harness_sha256=sha(Path(__file__)),
        native_sha256=sha(args.native) if args.native else None,
        model_sha256=sha(args.model / "model.safetensors"),
        native_model_sha256={path.name: sha(path) for path in sorted(args.native_model.iterdir())
                             if path.suffix in (".safetensors", ".gguf")},
        timing_boundary="native_http_python_loaded_model_api" if args.http else "loaded_model_public_api_without_http",
        memory="20ms sampled process-tree PSS plus per-process NVML; includes startup",
        controls={k: v for k, v in os.environ.items() if k.startswith(("ANTFLY_CUDA_", "ANTFLY_INFERENCE_CUDA_", "TERMITE_CUDA_", "CUBLAS_", "NVIDIA_TF32_", "OMP_", "MKL_"))},
        cuda_visible_devices=os.environ.get("CUDA_VISIBLE_DEVICES"),
        arguments={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
    )
    (args.out / "provenance.json").write_text(json.dumps(provenance, indent=2))
    fixture = args.out / "cases.json"
    fixture.write_text(json.dumps(dict(format_version=1, cases=cases())))
    selected, counts = [], {}
    for i, case in enumerate(cases()):
        b = len(case["texts"])
        if b not in args.batches or (args.limit and counts.get(b, 0) >= args.limit):
            continue
        counts[b] = counts.get(b, 0) + 1
        selected.append((i, case))
    rows = []
    for lc in range(args.lifecycles):
        for arm in args.arms if lc % 2 == 0 else reversed(args.arms):
            rows += run_arm(args, arm, lc, fixture, selected)
            print(f"finished {arm} lifecycle {lc}", flush=True)
    summary = {}
    for b in args.batches:
        cell = {}
        for arm in args.arms:
            values = [r["ms"] for r in rows if r["arm"] == arm and r["batch"] == b]
            cell[arm] = dict(median_ms=statistics.median(values), p95_ms=sorted(values)[min(len(values) - 1, (len(values) * 95 + 99) // 100 - 1)],
                             calls=len(values), documents_per_second=1000 * b * len(values) / sum(values))
        if set(args.arms) == {"native", "python"}:
            native = {(r["lifecycle"], r["index"]): r["labels"] for r in rows if r["arm"] == "native" and r["batch"] == b}
            python = {(r["lifecycle"], r["index"]): r["labels"] for r in rows if r["arm"] == "python" and r["batch"] == b}
            cell["identical_documents"] = sum(a == c for i in native for a, c in zip(native[i], python[i], strict=True))
            cell["documents"] = sum(len(v) for v in native.values())
            cell["task_comparisons"] = cell["documents"] * len(TASKS)
            cell["task_disagreements"] = [dict(lifecycle=i[0], case_index=i[1], row=j, task=task,
                                                native=a[task], python=c[task])
                                            for i in native for j, (a, c) in enumerate(zip(native[i], python[i], strict=True))
                                            for task in TASKS if a[task] != c[task]]
            cell["speedup"] = cell["python"]["median_ms"] / cell["native"]["median_ms"]
            scored = {arm: {(r["lifecycle"], r["index"]): confidences(arm, r["response"])
                            for r in rows if r["arm"] == arm and r["batch"] == b} for arm in args.arms}
            differences = [abs(left[k] - right[k]) for i in scored["native"]
                           for left, right in zip(scored["native"][i], scored["python"][i], strict=True)
                           for k in left.keys() & right.keys()]
            cell["max_selected_confidence_error"] = max(differences, default=None)
        summary[b] = cell
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    if not args.allow_label_differences and any(cell.get("identical_documents") != cell.get("documents") for cell in summary.values()):
        raise RuntimeError("native and Python classification labels differ; inspect retained outputs")


if __name__ == "__main__":
    main()
