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
import signal
import socket
import statistics
import subprocess
import sys
import time
import urllib.request
from urllib.parse import urlsplit

import qualify_transcription as q
import preflight_text_tokens as token_preflight


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


def request_options(engine, supplied, model=None):
    """Disable reuse explicitly; unsupported reference-server fields stay off wire."""
    options = dict(supplied)
    if "model" in options and model is not None and options["model"] != model:
        raise ValueError("--request-json cannot override the selected --model")
    if engine == "native":
        if "backend" in options and options["backend"] != "cuda":
            raise ValueError("native benchmarks require backend=cuda; --request-json cannot override it")
        options.setdefault("prompt_cache", False)
    elif engine == "llama":
        options.pop("cache_dtype", None)
        options.setdefault("cache_prompt", False)
    else:
        for key in ("cache_dtype", "prompt_cache", "cache_prompt", "backend"):
            if key in options:
                raise ValueError(f"{key} is not a vLLM request option; configure cache policy at server launch")
    return options


def server_command(args, port):
    binary = str(args.binary.resolve())
    if args.engine == "native":
        command = [binary, "run", "--host", "127.0.0.1", "--port", str(port),
                   "--models-dir", str(args.models_dir.resolve()), "--preload-model", f"generator:cuda:{args.model}",
                   "--max-work-units", "256", "--max-concurrent-requests", "2"]
        if args.process_memory_budget_mb is not None:
            command += ["--process-memory-budget-mb", str(args.process_memory_budget_mb)]
    elif args.engine == "llama":
        model_file = args.model_file or args.models_dir.resolve() / "generators" / args.model / "gemma-4-E4B_q4_0-it.gguf"
        cache = json.loads(args.request_json).get("cache_dtype", "f32")
        command = [binary, "-m", str(model_file.resolve()),
                   "--host", "127.0.0.1", "--port", str(port), "-ngl", "99", "-np", "1",
                   "-c", "8192", "-b", "2048", "-ub", "2048", "-ctk", cache, "-ctv", cache,
                   "-t", str(args.threads or 8), "--reasoning-format", "none"]
    else:
        model = str(args.model_file.resolve()) if args.model_file else args.vllm_model or args.model
        command = [binary, "serve", model, "--host", "127.0.0.1", "--port", str(port),
                   "--served-model-name", args.model, "--max-model-len", str(args.context),
                   "--max-num-seqs", "1", "--no-enable-prefix-caching", "--generation-config", "vllm",
                   "--dtype", "half", "--kv-cache-dtype", "auto",
                   "--gpu-memory-utilization", str(args.gpu_memory_utilization)]
        if args.tokenizer:
            command += ["--tokenizer", args.tokenizer]
    return command + args.server_arg


def native_checkpoint_binding(args):
    """Verify the local named target instead of hashing an unrelated path.

    server.zig tries models_dir/name before models_dir/generators/name.
    Only exact canonical owner/name references are supported here; aliases,
    managed variants and fallback searches require runtime loaded-file proof.
    """
    if args.base_url:
        if args.model_file is not None or args.token_preflight:
            raise ValueError("external native servers cannot verify a local --model-file or token preflight; use an owned server for qualification")
        return None
    parts = args.model.split("/")
    if len(parts) != 2 or any(not part or part in (".", "..") or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for c in part) for part in parts):
        raise ValueError("native checkpoint binding requires an exact owner/name model, without aliases, variants or traversal")
    for argument in args.server_arg:
        if argument.split("=", 1)[0] in ("--models-dir", "--config", "--preload-model"):
            raise ValueError("native checkpoint binding rejects model-affecting --server-arg overrides")
    root = args.models_dir.resolve(strict=True)
    direct = root / args.model
    directory = direct if direct.exists() else root / "generators" / args.model
    directory = directory.resolve(strict=True)
    if not directory.is_relative_to(root):
        raise ValueError("native model directory escapes --models-dir")
    checkpoint = token_preflight.native_gguf_checkpoint(directory, args.model_file)
    args.model_file = checkpoint
    return {"verified": True, "method": "exact local directory and unique GGUF decoder", "model_directory": str(directory),
            "checkpoint": str(checkpoint)}


def run_token_preflight(args, base, report):
    q.save(args.output / "report.json", report)
    command = [sys.executable, str(Path(__file__).with_name("preflight_text_tokens.py")),
               "--engine", args.engine, "--base-url", base,
               "--request-file", str(args.output / "report.json"),
               "--output", str(args.output / "preflight.json")]
    if args.native_token_reference:
        command += ["--native-reference", str(args.native_token_reference)]
    if args.native_cli:
        command += ["--native-cli", str(args.native_cli), "--native-cli-prefix-json", args.native_cli_prefix_json,
                    "--native-cli-backend", args.native_cli_backend, "--native-cli-args-json", args.native_cli_args_json,
                    "--native-cli-source-root", str(args.native_cli_source_root),
                    "--native-server-source-root", str(args.native_server_source_root), "--model-file", str(args.model_file)]
    with (args.output / "preflight.log").open("wb") as preflight_log:
        subprocess.run(command, stdout=preflight_log, stderr=subprocess.STDOUT, check=True, timeout=600)
    proof = json.loads((args.output / "preflight.json").read_text())
    if proof.get("passed") is not True or proof.get("prompt_identity_sha256") != report["prompt_identity_sha256"]:
        raise RuntimeError("Exact-token preflight did not verify this generation request")
    report["token_preflight"] = proof


def stop_owned_process(process):
    """Only stop a server group created by this run, including its GPU workers."""
    if process is None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait(timeout=10)
        return
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
    # A supervisor may exit before a worker that has not drained yet.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def reference_manifest(path):
    if path is None:
        return None
    metadata = json.loads(path.read_text())
    for name, expected in metadata.get("verified_files", {}).items():
        if q.file_sha(Path(name)) != expected:
            raise ValueError(f"reference compatibility/source artifact changed: {name}")
    return {"file": str(path.resolve()), "sha256": q.file_sha(path), "metadata": metadata}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=["native", "llama", "vllm"], default="native")
    parser.add_argument("--binary", type=Path, help="Server executable; not required with --base-url")
    parser.add_argument("--models-dir", type=Path)
    parser.add_argument("--base-url", help="Existing server origin, e.g. http://127.0.0.1:8000; never stopped by this harness")
    parser.add_argument("--server-pid", type=int, action="append", default=[], help="Host PID of an external server/supervisor for GPU isolation auditing (repeatable)")
    parser.add_argument("--server-metadata-json", default="{}", help="Provenance for an external server: exact version, command, cache policy and precision")
    parser.add_argument("--reference-manifest", type=Path, help="JSON reference compatibility/version/qualification receipt; verify linked files and embed it in report")
    parser.add_argument("--server-arg", action="append", default=[], help="Extra server argument; use --server-arg=--flag (repeat for each argument)")
    parser.add_argument("--threads", type=int, help="Explicit CPU/BLAS threads; unset preserves the environment and llama's existing default of 8")
    parser.add_argument("--process-memory-budget-mb", type=int, help="Explicit native process resource envelope")
    parser.add_argument("--model-file", type=Path, help="Exact reference GGUF/checkpoint file; its SHA-256 is recorded")
    parser.add_argument("--vllm-model", help="vLLM model path or Hugging Face ID, defaulting to --model")
    parser.add_argument("--tokenizer", help="vLLM tokenizer path or Hugging Face ID")
    parser.add_argument("--context", type=int, default=8192, help="vLLM maximum model length")
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.15, help="vLLM bounded residency fraction")
    parser.add_argument("--startup-timeout", type=float, default=180)
    parser.add_argument("--token-preflight", action="store_true", help="Before warmups, verify exact prompt token IDs using native CLI evidence")
    parser.add_argument("--native-token-reference", type=Path, help="Passing native CLI preflight.json with matching prompt, binary and model provenance")
    parser.add_argument("--native-cli", type=Path, help="Existing native CLI for untimed token capture; initializes a model, so schedule outside GPU work")
    parser.add_argument("--native-cli-prefix-json", default="[]")
    parser.add_argument("--native-cli-backend", choices=("native", "cuda"), default="native")
    parser.add_argument("--native-cli-args-json", default="[]")
    parser.add_argument("--native-cli-source-root", type=Path)
    parser.add_argument("--native-server-source-root", type=Path)
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
    if (args.threads is not None and args.threads < 1) or args.startup_timeout <= 0 or args.context < 1 or not 0 < args.gpu_memory_utilization <= 1:
        parser.error("invalid thread count, timeout, context or GPU memory fraction")
    if args.process_memory_budget_mb is not None and args.process_memory_budget_mb < 1:
        parser.error("process memory budget must be positive")
    if args.base_url:
        url = urlsplit(args.base_url)
        if url.scheme not in ("http", "https") or not url.netloc or url.path not in ("", "/") or url.query or url.fragment:
            parser.error("--base-url must be an HTTP(S) server origin without a path, query or fragment")
        if args.server_arg:
            parser.error("--server-arg cannot configure an external server")
        if args.require_quiet_gpu and not args.server_pid:
            parser.error("external GPU isolation requires --server-pid (host PID)")
    elif args.binary is None or (args.engine in ("native", "llama") and args.models_dir is None):
        parser.error("starting a server requires --binary and, for native/llama, --models-dir")
    if any(pid <= 0 for pid in args.server_pid):
        parser.error("--server-pid must be positive")
    if args.token_preflight and args.engine != "native" and args.native_token_reference is None:
        parser.error("reference --token-preflight requires --native-token-reference")
    if args.native_token_reference is not None and not args.token_preflight:
        parser.error("--native-token-reference requires --token-preflight")
    if args.native_cli and (not args.token_preflight or args.engine != "native" or args.native_token_reference or not all((args.native_cli_source_root, args.native_server_source_root, args.model_file))):
        parser.error("--native-cli requires native token-preflight, model-file and both source roots; cannot combine with native-token-reference")
    if args.token_preflight and args.engine == "native" and not (args.native_cli or args.native_token_reference):
        parser.error("native token-preflight requires --native-cli or --native-token-reference; checkpoint capture is not portable")
    supplied = json.loads(args.request_json)
    server_metadata = json.loads(args.server_metadata_json)
    manifest = reference_manifest(args.reference_manifest)
    if not isinstance(supplied, dict) or not isinstance(server_metadata, dict):
        parser.error("request and server metadata must be JSON objects")
    options = request_options(args.engine, supplied, model=args.model)
    native_binding = native_checkpoint_binding(args) if args.engine == "native" else None
    prompt = args.prompt_file.read_text() if args.prompt_file else None
    args.output.mkdir(parents=True, exist_ok=False)
    if args.base_url:
        base = args.base_url.rstrip("/")
        command = None
    else:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        command = server_command(args, port)
    route, health = ("/ai/v1/generate", "/healthz") if args.engine == "native" else ("/v1/chat/completions", "/health")
    server_env = dict(os.environ)
    if args.threads is not None:
        server_env.update({name: str(args.threads) for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")})
    report = {"engine": args.engine, "model": args.model, "binary_sha256": q.file_sha(args.binary) if args.binary else None,
              "harness_sha256": q.file_sha(Path(__file__)), "command": command,
              "base_url": base, "external_server": bool(args.base_url), "server_metadata": server_metadata,
              "reference_manifest": manifest,
              "native_model_binding": native_binding,
              "model_sha256": q.file_sha(args.model_file) if args.model_file else None,
              "threads": args.threads, "warmups": args.warmups, "repeats": args.repeats,
              "threads_applied_to_launched_server": args.threads is not None and not args.base_url,
              "environment": {k: v for k, v in os.environ.items() if k.startswith(("ANTFLY_", "TERMITE_", "CUDA_", "GGML_"))},
              "request_options": options, "prompt": prompt, "gpu_before": q.gpu_snapshot(), "generation": []}
    report["request"] = q.generation_request(args.model, args.tokens, prompt=prompt, backend=None if args.engine == "vllm" else "cuda", extra_body=options)
    report["prompt_identity_sha256"] = hashlib.sha256(json.dumps({key: report["request"][key] for key in ("messages", "chat_template_kwargs")}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    process = monitor = None
    owned_pids = set()
    for pid in args.server_pid:
        owned_pids.update(process_tree(pid))
    with (args.output / "gpu-activity.log").open("wb") as gpu_log, (args.output / "server.log").open("wb") as log:
        try:
            # CLI capture initializes a model. Finish it before starting a GPU
            # monitor/server, even though its requests are excluded from timing.
            if args.token_preflight and args.engine == "native":
                run_token_preflight(args, base, report)
            if args.monitor_gpu or args.require_quiet_gpu:
                monitor = subprocess.Popen(["nvidia-smi", "pmon", "-s", "um", "-d", "1", "-o", "DT"],
                    stdout=gpu_log, stderr=subprocess.STDOUT, env={**os.environ, "TZ": "UTC"})
            if command:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=server_env, start_new_session=True)
            deadline = time.monotonic() + args.startup_timeout
            while True:
                if process is not None and process.poll() is not None:
                    raise RuntimeError(f"Server exited {process.returncode}")
                try:
                    with urllib.request.urlopen(base + health, timeout=1):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise RuntimeError("Server startup timed out")
                    time.sleep(0.25)
            if process is not None:
                owned_pids.update(process_tree(process.pid))
            if args.token_preflight and args.engine != "native":
                run_token_preflight(args, base, report)
            for i in range(args.warmups + args.repeats):
                started = time.time()
                result = q.sse_generate(base, args.model, args.tokens, route=route, extra_body=options, prompt=prompt,
                                        backend=None if args.engine == "vllm" else "cuda")
                result["started_at_unix"] = started
                result["finished_at_unix"] = time.time()
                result["warmup"] = i < args.warmups
                report["generation"].append(result)
                q.save(args.output / "report.json", report)
                print(f"sample {i}: {result['decode_tokens_per_second']:.3f} decode tok/s; {result['usage']}", flush=True)
            samples = [r for r in report["generation"] if not r["warmup"]]
            report["summary"] = {k: statistics.median(r[k] for r in samples) for k in [
                "decode_tokens_per_second", "end_to_end_tokens_per_second", "ttft_seconds"]}
            report["summary"]["seconds"] = statistics.median(r["seconds"] for r in samples)
            report["summary"]["decode_metric"] = "SSE visible-content-based; inspect generation[].stream_accounting for assumptions"
            report["summary"]["text_hashes"] = sorted({hashlib.sha256(r["text"].encode()).hexdigest() for r in samples})
            report["summary"]["deterministic_text"] = len(report["summary"]["text_hashes"]) == 1
            report["summary"]["completion_tokens"] = [r["usage"]["completion_tokens"] for r in samples]
            if not all(r["usage"]["completion_tokens"] == args.tokens for r in samples):
                raise RuntimeError("Measured completion did not reach the requested token budget")
            if not report["summary"]["deterministic_text"]:
                raise RuntimeError("Greedy output changed across repeated requests")
            prompt_tokens = {r["usage"]["prompt_tokens"] for r in samples}
            if len(prompt_tokens) != 1:
                raise RuntimeError("Prompt token count changed across repeated requests")
            for sample in samples:
                details = sample["usage"].get("prompt_tokens_details") or {}
                if details.get("cached_tokens", 0) != 0:
                    raise RuntimeError("Reference reused cached prompt tokens")
            report["summary"]["prompt_tokens"] = prompt_tokens.pop()
            if args.token_preflight and report["token_preflight"]["logical_tokens"] != report["summary"]["prompt_tokens"]:
                raise RuntimeError("Timed prompt token count differs from exact-token preflight")
            report["cache_policy"] = "disabled by request" if args.engine != "vllm" else ("disabled by launch flag" if command else "external operator assertion; inspect server_metadata")
        except Exception as error:
            report["error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            stop_owned_process(process)
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
