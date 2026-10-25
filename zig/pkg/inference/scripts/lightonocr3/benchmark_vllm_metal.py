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

"""Benchmark an isolated vLLM Metal server with uncached OCR image requests.

Uses the original BF16 LightOn checkpoint, greedy decoding and image-only
prompts. Includes loopback HTTP overhead. Never attaches to or stops an existing
server. Keep GPU benchmarks sequential when comparing with the native runner.
"""

import argparse
import base64
import json
import os
import re
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from benchmark_antfly_readers import checkpoint, digest, hardware


def cache_override_admission(startup_log, requested):
    """Reject explicit capacities above the server's own probed safe limit."""
    if requested is None:
        return {"mode": "automatic", "admitted": True}
    matches = re.findall(
        r"Overriding num_gpu_blocks=(\d+) with num_gpu_blocks_override=(\d+)",
        startup_log,
    )
    if not matches:
        return {
            "mode": "explicit",
            "requested_blocks": requested,
            "admitted": False,
            "reason": "Server did not report its automatic capacity before override",
        }
    probes = [
        {"automatic_blocks": int(a), "override_blocks": int(b)} for a, b in matches
    ]
    admitted = all(
        p["override_blocks"] == requested and requested <= p["automatic_blocks"]
        for p in probes
    )
    return {
        "mode": "explicit",
        "requested_blocks": requested,
        "probes": probes,
        "admitted": admitted,
        "reason": "Within every reported automatic capacity"
        if admitted
        else "Override exceeds automatic capacity or disagrees with request",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--images", type=Path, nargs="+", required=True)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--requests", type=int, default=16)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--max-model-len", type=int, default=2048)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.6)
    parser.add_argument(
        "--kv-cache-blocks",
        type=int,
        help="Explicit cache block count; omit for vLLM's automatic capacity sizing",
    )
    parser.add_argument("--startup-timeout", type=int, default=240)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.requests < 1 or any(c < 1 or c > 64 for c in args.concurrency):
        parser.error("requests must be positive; concurrency must be 1..64")
    if args.kv_cache_blocks is not None and args.kv_cache_blocks < 1:
        parser.error("kv-cache-blocks must be positive")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    command = [
        str(args.python.absolute()),
        "-m",
        "vllm.entrypoints.openai.api_server",
        "--model",
        str(args.model_dir.resolve()),
        "--served-model-name",
        "ocr-benchmark",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--dtype",
        "bfloat16",
        "--max-model-len",
        str(args.max_model_len),
        "--max-num-seqs",
        str(max(args.concurrency)),
        "--max-num-batched-tokens",
        "4096",
        "--gpu-memory-utilization",
        str(args.gpu_memory_utilization),
        "--no-enable-prefix-caching",
        "--mm-processor-cache-gb",
        "0",
        "--limit-mm-per-prompt",
        '{"image":1,"video":0}',
        "--no-enable-log-requests",
    ]
    if args.kv_cache_blocks is not None:
        command += ["--num-gpu-blocks-override", str(args.kv_cache_blocks)]
    versions = json.loads(
        subprocess.check_output(
            [
                str(args.python.absolute()),
                "-c",
                'import importlib.metadata as m,json; print(json.dumps({p:m.version(p) for p in ["vllm","vllm-metal","mlx","mlx-vlm","mlx-lm","transformers","torch"]}))',
            ],
            text=True,
        )
    )
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017 (stock macOS Python 3.9)
        "hardware": hardware(),
        "versions": versions,
        "command": command,
        "kv_cache_block_override": args.kv_cache_blocks,
        "benchmark_script_sha256": digest(Path(__file__)),
        "model": checkpoint(args.model_dir.resolve()),
        "images": [
            {"path": str(p.resolve()), "sha256": digest(p)} for p in args.images
        ],
        "scope": "One resident vLLM Metal server, original BF16 weights, image-only prompt, thinking disabled, greedy decoding. Includes preprocessing, serving queue, inference and loopback HTTP. Excludes loading and serial reference reads. Prefix and processor caches disabled; every image gets a unique media UUID to prevent encoder reuse. Other machine workloads uncontrolled.",
        "results": [],
        "status": "starting",
    }
    report_path = args.output_dir / "report.json"

    def save():
        report_path.write_text(json.dumps(report, indent=2) + "\n")

    payloads = [
        json.dumps(
            {
                "model": "ocr-benchmark",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": "data:image/png;base64,"
                                    + base64.b64encode(path.read_bytes()).decode()
                                },
                            }
                        ],
                    }
                ],
                "temperature": 0,
                "max_tokens": args.max_tokens,
                "chat_template_kwargs": {"enable_thinking": False},
            }
        ).encode()
        for path in args.images
    ]

    def request(index):
        start = time.perf_counter()
        payload = json.loads(payloads[index % len(payloads)])
        payload["messages"][0]["content"][0]["uuid"] = uuid.uuid4().hex
        req = urllib.request.Request(
            base + "/v1/chat/completions",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=600) as response:
                result = json.load(response)
        except urllib.error.HTTPError as error:
            raise RuntimeError(error.read().decode()) from error
        choice = result["choices"][0]
        if choice["finish_reason"] == "length":
            raise RuntimeError("OCR output truncated")
        return {
            "image_index": index % len(payloads),
            "text": choice["message"]["content"],
            "usage": result.get("usage"),
            "latency_ms": (time.perf_counter() - start) * 1000,
        }

    save()
    env = os.environ.copy()
    env.update(
        VLLM_METAL_BACKEND="mlx",
        VLLM_METAL_MULTIMODAL_MODE="auto",
        TOKENIZERS_PARALLELISM="false",
    )
    with (args.output_dir / "server.txt").open("w") as log:
        process = subprocess.Popen(
            command,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            deadline = time.monotonic() + args.startup_timeout
            while True:
                if process.poll() is not None:
                    raise RuntimeError(
                        f"Server exited with {process.returncode}; see server.txt"
                    )
                try:
                    with urllib.request.urlopen(base + "/health", timeout=2):
                        break
                except (urllib.error.URLError, TimeoutError):
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Server startup timeout; see server.txt")
                    time.sleep(0.5)
            log.flush()
            admission = cache_override_admission(
                (args.output_dir / "server.txt").read_text(), args.kv_cache_blocks
            )
            report["cache_override_admission"] = admission
            save()
            if not admission["admitted"]:
                raise RuntimeError(
                    "Unsafe or unverifiable KV cache override: " + admission["reason"]
                )
            references = [request(i) for i in range(len(payloads))]
            report["references"] = references
            report["status"] = "running"
            save()
            for concurrency in args.concurrency:
                start = time.perf_counter()
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    outputs = list(executor.map(request, range(args.requests)))
                elapsed = time.perf_counter() - start
                latencies = sorted(output["latency_ms"] for output in outputs)
                parity = all(
                    output["text"] == references[output["image_index"]]["text"]
                    for output in outputs
                )
                result = {
                    "concurrency": concurrency,
                    "completed": len(outputs),
                    "elapsed_seconds": elapsed,
                    "pages_per_second": len(outputs) / elapsed,
                    "p50_ms": latencies[len(outputs) // 2],
                    "p95_ms": latencies[
                        min(len(outputs) - 1, len(outputs) * 95 // 100)
                    ],
                    "serial_reference_parity": parity,
                    "outputs": outputs,
                }
                report["results"].append(result)
                save()
                print(
                    json.dumps(
                        {
                            key: value
                            for key, value in result.items()
                            if key != "outputs"
                        }
                    ),
                    flush=True,
                )
                if not parity:
                    raise RuntimeError(
                        "Concurrent output differs from the serial reference"
                    )
            report["status"] = "ok"
        except Exception as error:
            report.update(status="failed", error=str(error))
            raise
        finally:
            save()
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()


if __name__ == "__main__":
    main()
