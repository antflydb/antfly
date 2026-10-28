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

"""Run video/recovery checks against an owned CUDA server; retain failed evidence.

This is a native CUDA qualification, not an independent reference parity claim.
Use --clip repeatedly for the production reproductions and progressively longer
clips. Budget overrides make admission failures reproducible without changing
the model or sampling policy.
"""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.request


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate_vector(vector):
    if len(vector) != 768 or any(
        isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x)
        for x in vector
    ):
        raise ValueError("expected 768 finite components")
    norm = math.sqrt(sum(x * x for x in vector))
    if abs(norm - 1) > 1e-4:
        raise ValueError(f"embedding is not normalized: {norm}")
    return norm


def media_input(mime, data):
    return [
        {
            "content": [
                {
                    "type": "media",
                    "mime_type": mime,
                    "data": base64.b64encode(data).decode(),
                }
            ]
        }
    ]


def cleanup_snapshot(url, pid):
    with urllib.request.urlopen(url + "/ml/v1/metrics", timeout=5) as response:
        raw = response.read(4 * 1024 * 1024 + 1)
    if len(raw) > 4 * 1024 * 1024:
        raise ValueError("metrics response exceeds qualification bound")
    metrics = raw.decode()
    parsed = {}
    for line in metrics.splitlines():
        fields = line.split()
        if len(fields) == 2 and not line.startswith("#"):
            value = float(fields[1])
            if math.isfinite(value):
                parsed[fields[0]] = value
    for name in (
        "antfly_inference_embedding_groups_active",
        "antfly_admission_inference_in_flight_requests",
    ):
        if parsed.get(name) != 0:
            raise ValueError(
                f"request did not release admission: {name}={parsed.get(name)}"
            )
    status = Path(f"/proc/{pid}/status").read_text()
    memory = {
        line.split(":")[0]: line.split(":", 1)[1].strip()
        for line in status.splitlines()
        if line.startswith(("VmRSS:", "VmHWM:"))
    }
    # The CLI may supervise a worker. Measuring only the launched PID would
    # report the supervisor's few MiB instead of the inference process.
    tree = []
    pending = [pid]
    seen = set()
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        try:
            current_status = Path(f"/proc/{current}/status").read_text()
            children = Path(f"/proc/{current}/task/{current}/children").read_text()
        except FileNotFoundError:
            continue
        values = {
            line.split(":")[0]: line.split(":", 1)[1].strip()
            for line in current_status.splitlines()
            if ":" in line
        }
        tree.append(
            {
                "pid": current,
                "name": values["Name"],
                "rss_kib": int(values.get("VmRSS", "0 kB").split()[0]),
                "hwm_kib": int(values.get("VmHWM", "0 kB").split()[0]),
            }
        )
        pending.extend(int(child) for child in children.split())
    return {
        "process": memory,
        "process_tree": tree,
        "tree_rss_kib": sum(row["rss_kib"] for row in tree),
        "metrics": parsed,
    }


def request(url, body, timeout, deadline_ms=None):
    req = urllib.request.Request(
        url + "/ai/v1/embed",
        json.dumps(body).encode(),
        {
            "Content-Type": "application/json",
            "X-Antfly-Inference-Timeout-Ms": str(
                deadline_ms if deadline_ms is not None else int(timeout * 1000)
            ),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read(65536).decode(errors="replace")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--clip", type=Path, action="append", required=True)
    parser.add_argument("--audio", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--mixed-jobs", type=int, default=6)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--backend-budget-mb", type=int, default=8192)
    parser.add_argument("--scratch-budget-mb", type=int, default=4096)
    args = parser.parse_args()
    if args.iterations < 2 or args.mixed_jobs < 0 or not 0 < args.timeout <= 600:
        parser.error("at least two iterations and a timeout in (0,600] are required")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "status": "running",
        "scope": "native CUDA repeatability and recovery",
        "binary_sha256": sha256(args.binary),
        "tool_sha256": sha256(Path(__file__)),
        "checkpoint_receipt": json.loads(
            (args.model_dir / "embeddinggemma2_receipt.json").read_text()
        ),
        "clips": [{"path": str(p.resolve()), "sha256": sha256(p)} for p in args.clip],
        "audio_sha256": sha256(args.audio),
        "cases": [],
        "backend_budget_mb": args.backend_budget_mb,
        "scratch_budget_mb": args.scratch_budget_mb,
    }
    result["source_sha"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    result["source_diff_sha256"] = hashlib.sha256(
        subprocess.check_output(["git", "diff", "HEAD"])
    ).hexdigest()
    result["gpu"] = subprocess.check_output(["nvidia-smi"], text=True)
    for item in result["checkpoint_receipt"]["files"]:
        if sha256(args.model_dir / item["path"]) != item["sha256"]:
            raise ValueError(f"checkpoint receipt mismatch: {item['path']}")
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", 0))
        port = endpoint.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    command = [
        str(args.binary.resolve()),
        "inference",
        "run",
        "--port",
        str(port),
        "--models-dir",
        str(args.model_dir.resolve().parent),
        "--host-budget-mb",
        "8192",
        "--backend-budget-mb",
        str(args.backend_budget_mb),
        "--combined-budget-mb",
        str(8192 + args.backend_budget_mb),
        "--scratch-budget-mb",
        str(args.scratch_budget_mb),
        "--process-memory-budget-mb",
        "16384",
    ]
    result["command"] = command
    baseline = {}
    identity = None
    failures = []
    log_path = args.output.with_suffix(".server.log")
    with log_path.open("w") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=log,
            env=dict(
                os.environ,
                ANTFLY_INFERENCE_REQUIRED_BACKEND="cuda",
                ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_STATS="1",
            ),
        )
        try:
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("owned server failed to start")
                try:
                    with urllib.request.urlopen(url + "/healthz", timeout=1):
                        break
                except OSError:
                    time.sleep(0.1)
            text = ["Video qualification recovery control."]
            audio = media_input("audio/wav", args.audio.read_bytes())
            cases = [("text", text, 200), ("audio", audio, 200)]
            for iteration in range(args.iterations):
                for index, clip in enumerate(args.clip):
                    cases.append(
                        (
                            f"video_{index}",
                            media_input("video/mp4", clip.read_bytes()),
                            200,
                        )
                    )
                cases.extend(
                    [
                        ("corrupt", media_input("video/mp4", b"not an MP4"), 400),
                        (
                            "truncated",
                            media_input("video/mp4", args.clip[0].read_bytes()[:32]),
                            400,
                        ),
                        ("text", text, 200),
                        ("audio", audio, 200),
                    ]
                )
            cases.extend(
                [
                    (
                        "deadline",
                        media_input("video/mp4", args.clip[-1].read_bytes()),
                        504,
                    ),
                    ("text", text, 200),
                    ("audio", audio, 200),
                ]
            )
            for name, inputs, expected_status in cases:
                row = {"case": name, "expected_status": expected_status}
                result["cases"].append(row)
                started = time.monotonic()
                try:
                    status, response = request(
                        url,
                        {"model": args.model_dir.name, "input": inputs},
                        args.timeout,
                        deadline_ms=1 if name == "deadline" else None,
                    )
                    row.update(http_status=status, seconds=time.monotonic() - started)
                    if status != expected_status:
                        row["response"] = response
                        raise ValueError(
                            f"expected HTTP {expected_status}, got {status}"
                        )
                    if status == 200:
                        vector = response["data"][0]["embedding"]
                        row["norm"] = validate_vector(vector)
                        row["vector"] = vector
                        current_identity = response["model_identity"]
                        if identity is not None and identity != current_identity:
                            raise ValueError("model identity changed")
                        identity = current_identity
                        if name in baseline:
                            row["repeat_max_abs"] = max(
                                abs(x - y) for x, y in zip(vector, baseline[name])
                            )
                            if row["repeat_max_abs"] > 1e-5:
                                raise ValueError(
                                    "embedding changed across repeats/recovery"
                                )
                        baseline[name] = vector
                    else:
                        row["response"] = response
                    row["cleanup"] = cleanup_snapshot(url, process.pid)
                    row["status"] = "pass"
                except Exception as error:
                    row.update(status="failed", error=str(error))
                    failures.append(name)
                    try:
                        row["cleanup"] = cleanup_snapshot(url, process.pid)
                    except Exception as cleanup_error:
                        row["cleanup_error"] = str(cleanup_error)
                print(name, row["status"], flush=True)
                args.output.write_text(json.dumps(result, indent=2) + "\n")
            result["model_identity"] = identity
            if not failures and args.mixed_jobs:
                mixed = [
                    ("text", text),
                    ("audio", audio),
                    ("video_0", media_input("video/mp4", args.clip[0].read_bytes())),
                ]
                with ThreadPoolExecutor(max_workers=3) as executor:
                    submitted = {}
                    for index in range(args.mixed_jobs):
                        name, inputs = mixed[index % len(mixed)]
                        future = executor.submit(
                            request,
                            url,
                            {"model": args.model_dir.name, "input": inputs},
                            args.timeout,
                        )
                        submitted[future] = name
                    for future in as_completed(submitted):
                        name = submitted[future]
                        row = {"case": "mixed_" + name}
                        result["cases"].append(row)
                        try:
                            status, response = future.result()
                            row["http_status"] = status
                            if status != 200:
                                row["response"] = response
                                raise ValueError(
                                    f"mixed request returned HTTP {status}"
                                )
                            vector = response["data"][0]["embedding"]
                            row["norm"] = validate_vector(vector)
                            row["repeat_max_abs"] = max(
                                abs(x - y) for x, y in zip(vector, baseline[name])
                            )
                            if (
                                response["model_identity"] != identity
                                or row["repeat_max_abs"] > 1e-5
                            ):
                                raise ValueError(
                                    "mixed request changed identity or embedding"
                                )
                            row["status"] = "pass"
                        except Exception as error:
                            row.update(status="failed", error=str(error))
                            failures.append("mixed_" + name)
                        print(row["case"], row["status"], flush=True)
                result["mixed_cleanup"] = cleanup_snapshot(url, process.pid)
        except Exception as error:
            result["error"] = str(error)
            failures.append("server")
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    if "selected backend cuda for" not in log_path.read_text():
        failures.append("CUDA backend confirmation")
    result["failures"] = failures
    result["status"] = "failed" if failures else "pass"
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
