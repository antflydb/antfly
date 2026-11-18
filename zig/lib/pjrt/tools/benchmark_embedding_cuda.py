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

"""Measure the resident native CUDA HTTP server on the prepared embedding cases."""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
from memory import MemorySampler


def post(port, body):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=600)
    started = time.perf_counter()
    try:
        conn.request(
            "POST",
            "/ai/v1/embed",
            body=json.dumps(body),
            headers={"content-type": "application/json"},
        )
        response = conn.getresponse()
        data = json.loads(response.read())
        elapsed = (time.perf_counter() - started) * 1000
        if response.status != 200:
            raise RuntimeError((response.status, data))
        return data, elapsed
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--full-cli",
        action="store_true",
        help="Binary needs inference run instead of run",
    )
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    active = subprocess.check_output(
        ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True
    ).strip()
    if active:
        raise RuntimeError(f"GPU already has compute processes: {active}")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with args.binary.open("rb") as f:
        binary_sha = hashlib.file_digest(f, "sha256").hexdigest()
    argv = [str(args.binary.resolve())]
    if args.full_cli:
        argv.append("inference")
    argv += [
        "run",
        "--models-dir",
        str(args.models_dir.resolve()),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--host-budget-mb",
        "6144",
        "--backend-budget-mb",
        "12288",
        "--combined-budget-mb",
        "18432",
        "--scratch-budget-mb",
        "8192",
    ]
    env = dict(
        os.environ,
        ANTFLY_INFERENCE_REQUIRED_BACKEND="cuda",
        ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_STATS="1",
    )
    rows = []
    manifest = json.loads(args.manifest.read_text())
    with (args.out / "server.log").open("w") as log:
        process = subprocess.Popen(
            argv, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
        sampler = None
        try:
            sampler = MemorySampler(process, args.out / "memory.jsonl")
            sampler.thread.start()
            for _ in range(1200):
                if process.poll() is not None:
                    raise RuntimeError("Server exited; see server.log")
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                try:
                    conn.request("GET", "/healthz")
                    response = conn.getresponse()
                    response.read()
                    if response.status == 200:
                        break
                except OSError:
                    pass
                finally:
                    conn.close()
                time.sleep(0.1)
            else:
                raise TimeoutError("Server startup")
            for case in manifest["cases"]:
                body = {
                    "model": args.model,
                    "input": [case["text"]],
                    "task_type": "RETRIEVAL_DOCUMENT",
                }
                for rep in range(manifest["warmups"] + manifest["repeats"]):
                    data, elapsed = post(port, body)
                    vector = data["data"][0]["embedding"]
                    if len(vector) != 768:
                        raise ValueError("Unexpected vector shape")
                    rows.append(
                        {
                            "cell": case["id"],
                            "repeat": rep,
                            "warmup": rep < manifest["warmups"],
                            "ms": elapsed,
                            "vector": vector,
                            "identity": data.get("model_identity"),
                            "usage": data.get("usage"),
                        }
                    )
                    print(case["id"], rep, elapsed, flush=True)
                    (args.out / "results.json").write_text(
                        json.dumps(
                            {
                                "rows": rows,
                                "command": argv,
                                "binary_sha256": binary_sha,
                                "policy_env": {
                                    k: v
                                    for k, v in env.items()
                                    if k.startswith(
                                        "ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2"
                                    )
                                },
                            },
                            indent=2,
                        )
                    )
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            if sampler is not None:
                (args.out / "memory.json").write_text(
                    json.dumps(sampler.stop(), indent=2)
                )


if __name__ == "__main__":
    main()
