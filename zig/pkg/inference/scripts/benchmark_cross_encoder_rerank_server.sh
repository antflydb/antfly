#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
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

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

antfly_bin="${ANTFLY_BIN:-$repo_root/zig-out/bin/antfly}"
model_dir="${ANTFLY_INFERENCE_RERANK_MODEL_DIR:-/Users/tim/.cache/bge-reranker-base}"
query="${ANTFLY_INFERENCE_RERANK_QUERY:-what is Antfly inference}"
document="${ANTFLY_INFERENCE_RERANK_DOCUMENT:-Antfly inference is a Zig inference runtime with native model runtimes}"
repeat="${ANTFLY_INFERENCE_RERANK_SERVER_BENCH_REPEAT:-8}"
world_size="${ANTFLY_INFERENCE_RERANK_TP_WORLD_SIZE:-2}"
models_root="${ANTFLY_INFERENCE_MODELS_DIR:-$repo_root}"
request_timeout="${ANTFLY_INFERENCE_RERANK_SERVER_REQUEST_TIMEOUT_SECS:-600}"
startup_settle_ms="${ANTFLY_INFERENCE_RERANK_SERVER_STARTUP_SETTLE_MS:-1000}"

if [[ ! -x "$antfly_bin" ]]; then
  echo "missing antfly binary at $antfly_bin" >&2
  echo "build it first, for example: zigup run master build" >&2
  exit 1
fi

if [[ ! -f "$model_dir/model.safetensors" ]]; then
  echo "missing model weights at $model_dir/model.safetensors" >&2
  exit 1
fi

python3 - "$antfly_bin" "$models_root" "$model_dir" "$query" "$document" "$repeat" "$world_size" "$request_timeout" "$startup_settle_ms" <<'PY'
import atexit
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time

import requests

antfly_bin, models_root, model_dir, query, document, repeat_s, world_size_s, request_timeout_s, startup_settle_ms_s = sys.argv[1:]
repeat = int(repeat_s)
world_size = int(world_size_s)
request_timeout = float(request_timeout_s)
startup_settle = float(startup_settle_ms_s) / 1000.0


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_for_server(url: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{url}/api/version", timeout=1.0)
            if r.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.25)
    raise RuntimeError(f"server failed to start: {url}")


procs = []


def stop_all() -> None:
    for proc in reversed(procs):
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)


atexit.register(stop_all)


def start_server(env: dict[str, str], port: int, log_path: str) -> str:
    with open(log_path, "wb") as log_file:
        proc = subprocess.Popen(
            [antfly_bin, "inference", "run", "--host", "127.0.0.1", "--port", str(port), "--models-dir", models_root],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env=env,
        )
    procs.append(proc)
    url = f"http://127.0.0.1:{port}"
    wait_for_server(url)
    return url


def stats(name: str, times_ms: list[float], score: float) -> None:
    warm = times_ms[1:] if len(times_ms) > 1 else times_ms
    print(f"{name}:")
    print(f"  repeat={len(times_ms)}")
    print(f"  score={score:.6f}")
    print(f"  last_ms={times_ms[-1]:.1f}")
    print(f"  min_ms={min(times_ms):.1f}")
    print(f"  max_ms={max(times_ms):.1f}")
    print(f"  avg_ms={sum(times_ms)/len(times_ms):.1f}")
    print(f"  warm_avg_ms={sum(warm)/len(warm):.1f}")


def post_rerank(url: str) -> tuple[float, list[float]]:
    payload = {"model": model_dir, "query": query, "prompts": [document]}
    start = time.perf_counter()
    resp = requests.post(f"{url}/api/rerank", json=payload, timeout=request_timeout)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    resp.raise_for_status()
    data = resp.json()
    return elapsed_ms, data["scores"]


tmpdir = os.path.join("/tmp", f"termite-rerank-server-bench-{os.getpid()}")
os.makedirs(tmpdir, exist_ok=True)

# BLAS server benchmark
blas_port = free_port()
blas_env = dict(os.environ)
blas_env["TERMITE_PREFERRED_BACKEND"] = "blas"
blas_log = os.path.join(tmpdir, "blas.log")
blas_url = start_server(blas_env, blas_port, blas_log)
time.sleep(startup_settle)
# Untimed warmup to pay model-load cost outside the measured loop.
_, scores = post_rerank(blas_url)
blas_score = scores[0]
blas_times = []
for _ in range(repeat):
    elapsed_ms, scores = post_rerank(blas_url)
    blas_times.append(elapsed_ms)
    blas_score = scores[0]

print("== Server BLAS ==")
stats("server_blas", blas_times, float(blas_score))

# TP server benchmark
hostfile = os.path.join(tmpdir, "hosts.json")
tp_ports = [free_port() for _ in range(world_size)]
with open(hostfile, "w", encoding="utf-8") as f:
    json.dump([[f"127.0.0.1:{port}"] for port in tp_ports], f)

tp_urls = []
for rank, port in enumerate(tp_ports):
    env = dict(os.environ)
    env["TERMITE_PREFERRED_BACKEND"] = "mlx"
    env["TERMITE_MLX_DISTRIBUTED_ENABLE"] = "1"
    env["TERMITE_MLX_DISTRIBUTED_MODE"] = "tensor_parallel"
    env["TERMITE_MLX_DISTRIBUTED_BACKEND"] = "ring"
    env["TERMITE_MLX_WORLD_SIZE"] = str(world_size)
    env["TERMITE_MLX_RANK"] = str(rank)
    env["TERMITE_MLX_LOCAL_RANK"] = str(rank)
    env["MLX_WORLD_SIZE"] = str(world_size)
    env["MLX_RANK"] = str(rank)
    env["MLX_HOSTFILE"] = hostfile
    env["TERMITE_MLX_ALLOW_CPU_STREAM_WITHOUT_METAL"] = "1"
    log_path = os.path.join(tmpdir, f"tp_rank_{rank}.log")
    tp_urls.append(start_server(env, port, log_path))

time.sleep(startup_settle)

def post_tp_round() -> tuple[float, float]:
    results: list[tuple[float, list[float]] | Exception | None] = [None] * world_size

    def runner(index: int) -> None:
        try:
            results[index] = post_rerank(tp_urls[index])
        except Exception as exc:  # noqa: BLE001
            results[index] = exc

    threads = [threading.Thread(target=runner, args=(i,)) for i in range(world_size)]
    wall_start = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    wall_ms = (time.perf_counter() - wall_start) * 1000.0

    for result in results:
        if isinstance(result, Exception):
            raise result
        assert result is not None
    scores = [result[1][0] for result in results]  # type: ignore[index]
    if max(scores) - min(scores) > 5e-4:
        raise RuntimeError(f"tp ranks disagree on score: {scores}")
    return wall_ms, scores[0]

# Untimed warmup to pay model-load / first-collective cost outside the measured loop.
_, tp_score = post_tp_round()
tp_times = []
for _ in range(repeat):
    wall_ms, tp_score = post_tp_round()
    tp_times.append(wall_ms)

print()
print(f"== Server MLX TP ({world_size} ranks) ==")
stats("server_mlx_tp", tp_times, float(tp_score))
print()
print(f"tmpdir={tmpdir}")
PY
