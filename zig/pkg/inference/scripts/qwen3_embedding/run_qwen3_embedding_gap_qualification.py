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

"""Own local servers, run strict AB/BA cells, and record isolated capacity soak.

No downloading, compilation, CI dispatch or publication. Reports include hashes,
argv/PID attestation, vector parity and explicit failures; gates are never waived.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, ExitStack, nullcontext
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import shlex
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

import benchmark_qwen3_embedding_endpoint as benchmark


@contextmanager
def server(executable, argv, env, health, log_path):
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [str(executable), *argv],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(
                        f"server exited {process.returncode}; see {log_path}"
                    )
                try:
                    with urllib.request.urlopen(health, timeout=1) as response:
                        if response.status == 200:
                            break
                except (urllib.error.URLError, TimeoutError):
                    time.sleep(0.25)
            else:
                raise RuntimeError(f"server readiness timed out; see {log_path}")
            yield process
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=20)


def process_tree_memory(pid):
    # The serving executable supervises a replaceable child. Measuring only
    # Popen.pid would report the idle supervisor instead of the model worker.
    result = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,rss="], text=True, capture_output=True, check=True
    )
    rows = [
        tuple(map(int, line.split()))
        for line in result.stdout.splitlines()
        if line.strip()
    ]
    owned = {pid}
    while True:
        descendants = {child for child, parent, _ in rows if parent in owned}
        if descendants <= owned:
            break
        owned.update(descendants)
    processes = [
        {"pid": child, "rss_bytes": rss * 1024}
        for child, _, rss in rows
        if child in owned
    ]
    if not any(row["pid"] == pid for row in processes):
        raise RuntimeError("owned supervisor disappeared")
    return {
        "rss_bytes": sum(row["rss_bytes"] for row in processes),
        "processes": processes,
    }


def swap_used_bytes(output):
    match = re.search(r"\bused\s*=\s*([0-9.]+)([KMG])", output)
    if match is None:
        raise RuntimeError("could not parse host swap usage")
    return round(float(match[1]) * {"K": 1024, "M": 1024**2, "G": 1024**3}[match[2]])


def baseline_regression(current, baseline):
    """Independent runs require an unpaired bootstrap, not paired AB/BA CI."""
    for field in (
        "fixture_token_count",
        "batch_sizes",
        "task_type",
        "query_prefix",
        "seed",
        "iters",
        "warmup",
        "antfly_server_args",
    ):
        if current["args"][field] != baseline["args"][field]:
            raise ValueError(f"baseline workload mismatch: {field}")
    for report in (current, baseline):
        contract = report["comparison_contract"]
        if (
            not contract["strict"]
            or not contract["model_files"]["identical"]
            or not all(row["pass"] for row in report["comparisons"])
        ):
            raise ValueError(
                "baseline comparison requires strict attestation and vector parity"
            )
    for target in ("antfly", "reference"):
        if (
            current["comparison_contract"]["model_files"][target]["sha256"]
            != baseline["comparison_contract"]["model_files"][target]["sha256"]
        ):
            raise ValueError("baseline model hash mismatch")
    new = next(row for row in current["results"] if row["target"] == "antfly")
    old = next(row for row in baseline["results"] if row["target"] == "antfly")
    for field in ("batch", "dimensions", "input_tokens"):
        if new[field] != old[field]:
            raise ValueError(f"baseline shape mismatch: {field}")
    new_samples, old_samples = new["samples_ms"], old["samples_ms"]
    if min(len(new_samples), len(old_samples)) < 20 or not all(
        math.isfinite(value) and value > 0 for value in new_samples + old_samples
    ):
        raise ValueError(
            "baseline comparison requires at least 20 positive finite samples"
        )
    rng = random.Random(current["args"]["seed"])
    samples = sorted(
        statistics.fmean(rng.choices(old_samples, k=len(old_samples)))
        / statistics.fmean(rng.choices(new_samples, k=len(new_samples)))
        for _ in range(2000)
    )
    lower = samples[49]
    return {
        "estimate": statistics.fmean(old_samples) / statistics.fmean(new_samples),
        "lower_95": lower,
        "upper_95": samples[1949],
        "threshold": 0.95,
        "method": "unpaired bootstrap of mean latency ratio",
        "pass": lower >= 0.95,
    }


def correctness(args, url, cases):
    texts = [
        "",
        "hello",
        "北京是中华人民共和国的首都。",
        "Rocket 🚀 café ✨",
        "def compute(x): return x*x",
    ]
    texts += [
        benchmark.select_fixture_token_count(cases, length)[0]["text"]
        for length in (20, 32, 64, 256, 511)
    ]
    checks = []

    def check(name, passed, **detail):
        checks.append({"name": name, "pass": bool(passed), **detail})
        print(f"correctness {name}: pass={bool(passed)}", flush=True)

    for policy in ("fail_fast", "per_item"):
        _, singles, _, _ = benchmark.request_embeddings(
            url,
            args.model_dir.name,
            texts,
            args.timeout,
            request_options={"error_policy": policy},
        )
        individual = [
            benchmark.request_embeddings(
                url, args.model_dir.name, [text], args.timeout
            )[1][0]
            for text in texts
        ]
        parity = benchmark.cross_check(singles, individual, 0.9999)
        check(f"ragged_order_and_single_equivalence_{policy}", parity["pass"], **parity)
        check(
            f"finite_full_vectors_and_unit_norms_{policy}",
            all(
                len(vector) == 1024
                and abs(math.sqrt(sum(value * value for value in vector)) - 1) <= 0.001
                for vector in singles
            ),
        )
        for dimensions in (32, 256):
            _, reduced, _, _ = benchmark.request_embeddings(
                url,
                args.model_dir.name,
                texts,
                args.timeout,
                request_options={"dimensions": dimensions, "error_policy": policy},
            )
            expected = []
            for vector in singles:
                prefix = vector[:dimensions]
                norm = math.sqrt(sum(value * value for value in prefix))
                expected.append([value / norm for value in prefix])
            parity = benchmark.cross_check(reduced, expected, 0.99999)
            check(
                f"truncate_then_renormalize_{dimensions}_{policy}",
                parity["pass"],
                **parity,
            )

    prefix = "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery:"
    for instruction in (None, "Retrieve relevant documents"):
        options = {"task_type": "RETRIEVAL_QUERY"}
        rendered_prefix = prefix
        if instruction:
            options["instruction"] = instruction
            rendered_prefix = f"Instruct: {instruction}\nQuery:"
        _, queries, _, _ = benchmark.request_embeddings(
            url, args.model_dir.name, texts[:5], args.timeout, request_options=options
        )
        _, rendered, _, _ = benchmark.request_embeddings(
            url,
            args.model_dir.name,
            [rendered_prefix + text for text in texts[:5]],
            args.timeout,
        )
        parity = benchmark.cross_check(queries, rendered, 0.99999)
        check(f"query_prefix_{instruction or 'default'}", parity["pass"], **parity)

    def request_json(body):
        request = urllib.request.Request(
            url,
            json.dumps({"model": args.model_dir.name, **body}).encode(),
            {"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=args.timeout) as response:
                status, payload = response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            status, payload = exc.code, json.load(exc)
        return status, payload

    def reject(name, body, expected):
        status, payload = request_json(body)
        check(name, status == expected, status=status, response=payload)

    for dimensions in (0, 31, 1025):
        reject(
            f"invalid_dimensions_{dimensions}",
            {"input": "hello", "dimensions": dimensions},
            400,
        )
    reject("too_many_items", {"input": ["hello"] * 129}, 413)
    overlong = (
        benchmark.select_fixture_token_count(cases, 20)[0]["text"] + " token" * 32768
    )
    # Context truncation is an existing model behavior. At this lane's bounded
    # budget, the resulting full-context frame must reject before allocation;
    # execution errors preserve the API's per-item partial-response policy.
    status, rejected = request_json({"input": overlong, "error_policy": "fail_fast"})
    check(
        "oversized_frame_budget_fail_fast",
        status == 400 and rejected.get("error") == "MODEL_RESOURCE_LIMIT",
        status=status,
        response=rejected,
    )
    status, rejected = request_json({"input": overlong, "error_policy": "per_item"})
    failures = rejected.get("errors", [])
    check(
        "oversized_frame_budget_per_item",
        status == 200
        and rejected.get("data") == []
        and rejected.get("summary") == {"total": 1, "succeeded": 0, "failed": 1}
        and len(failures) == 1
        and failures[0].get("index") == 0
        and failures[0].get("code") == "MODEL_RESOURCE_LIMIT"
        and failures[0].get("retryable") is False
        and failures[0].get("status") == 400,
        status=status,
        response=rejected,
    )
    mixed = {
        "input": ["hello", {"type": "unsupported"}, "world"],
        "error_policy": "fail_fast",
    }
    reject("invalid_item_fail_fast", mixed, 400)
    mixed["error_policy"] = "per_item"
    status, partial = request_json(mixed)
    check(
        "partial_success_preserves_original_indexes",
        status == 200
        and [row["index"] for row in partial.get("data", [])] == [0, 2]
        and [row["index"] for row in partial.get("errors", [])] == [1]
        and partial.get("summary") == {"total": 3, "succeeded": 2, "failed": 1},
        status=status,
    )
    if status == 200:
        _, expected, _, _ = benchmark.request_embeddings(
            url, args.model_dir.name, ["hello", "world"], args.timeout
        )
        parity = benchmark.cross_check(
            [row["embedding"] for row in partial["data"]], expected, 0.99999
        )
        check("partial_success_vector_parity", parity["pass"], **parity)

    # Cancel a real passage request by disconnecting its owned socket. The
    # following request must recover with the same vectors and no worker exit.
    passage, _ = benchmark.fixture_batch(
        benchmark.select_fixture_token_count(cases, 256), 32
    )
    body = json.dumps({"model": args.model_dir.name, "input": passage}).encode()
    endpoint = urlsplit(url)
    with socket.create_connection(
        (endpoint.hostname, endpoint.port), timeout=args.timeout
    ) as connection:
        header = f"POST {endpoint.path} HTTP/1.1\r\nHost: {endpoint.hostname}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
        connection.sendall(header + body)
        time.sleep(0.05)
    _, recovered, _, _ = benchmark.request_embeddings(
        url, args.model_dir.name, texts[:5], args.timeout
    )
    _, control, _, _ = benchmark.request_embeddings(
        url, args.model_dir.name, texts[:5], args.timeout
    )
    parity = benchmark.cross_check(recovered, control, 0.99999)
    check("recovery_after_rejections_and_client_disconnect", parity["pass"], **parity)
    # Occupy the model with one passage request while exceeding the 32-request
    # ingress cap. Accepted rows must remain correct, denials must be retryable,
    # and the following normal request must recover without restarting.
    short = benchmark.select_fixture_token_count(cases, 20)[0]["text"]
    _, expected_short, _, _ = benchmark.request_embeddings(
        url, args.model_dir.name, [short], args.timeout
    )
    barrier = threading.Barrier(35)

    def overloaded(index):
        barrier.wait(timeout=30)
        status, payload = request_json({"input": passage if index == 0 else [short]})
        if status == 200:
            if index == 0:
                return status, len(payload.get("data", [])) == 32
            parity = benchmark.cross_check(
                [row["embedding"] for row in payload["data"]], expected_short, 0.99999
            )
            return status, parity["pass"]
        return status, status == 503 and payload.get("retryable") is True

    with ThreadPoolExecutor(max_workers=35) as pool:
        outcomes = list(pool.map(overloaded, range(35)))
    check(
        "overload_denials_are_retryable_and_accepted_vectors_match",
        all(passed for _, passed in outcomes)
        and any(status == 503 for status, _ in outcomes),
        accepted=sum(status == 200 for status, _ in outcomes),
        denied=sum(status == 503 for status, _ in outcomes),
    )
    _, recovered, _, _ = benchmark.request_embeddings(
        url, args.model_dir.name, texts[:5], args.timeout
    )
    parity = benchmark.cross_check(recovered, control, 0.99999)
    check("recovery_after_overload", parity["pass"], **parity)
    if args.eviction_model:
        benchmark.request_embeddings(url, args.eviction_model, ["hello"], args.timeout)
        boundary_manifest = (
            args.model_dir.parent / args.eviction_model / "model_manifest.json"
        )
        if (
            boundary_manifest.exists()
            and "inference.limits.max_input_tokens_per_item=4"
            in json.loads(boundary_manifest.read_text()).get("capabilities", [])
        ):
            # An explicit four-token boundary bundle must count its query
            # prefix toward the limit under both error policies.
            for policy in ("fail_fast", "per_item"):
                reject(
                    f"limited_query_prefix_{policy}",
                    {
                        "model": args.eviction_model,
                        "input": "hello",
                        "task_type": "RETRIEVAL_QUERY",
                        "error_policy": policy,
                    },
                    413,
                )
        _, reloaded, _, _ = benchmark.request_embeddings(
            url, args.model_dir.name, texts[:5], args.timeout
        )
        parity = benchmark.cross_check(reloaded, control, 0.99999)
        check("eviction_and_reload_preserve_vectors", parity["pass"], **parity)
    return {"checks": checks, "pass": all(row["pass"] for row in checks)}


def soak(args, process, url, cases):
    texts, _ = benchmark.fixture_batch(
        benchmark.select_fixture_token_count(cases, 256), 32
    )
    # Warm every geometry before the plateau measurement. Compare each returned
    # row with this same-binary control to detect order/state corruption.
    _, expected, _, _ = benchmark.request_embeddings(
        url, args.model_dir.name, texts, args.timeout
    )
    deadline = time.monotonic() + args.soak_seconds
    started = time.monotonic()
    lock = threading.Lock()
    counts = {"requests": 0, "vectors": 0, "failures": 0, "min_cosine": 1.0}
    failures, memory = [], []
    stop = threading.Event()
    initial_processes = {
        row["pid"] for row in process_tree_memory(process.pid)["processes"]
    }

    def worker(worker_id):
        while not stop.is_set() and time.monotonic() < deadline:
            offset = worker_id % len(texts)
            rotated = texts[offset:] + texts[:offset]
            reference = expected[offset:] + expected[:offset]
            try:
                _, vectors, _, _ = benchmark.request_embeddings(
                    url, args.model_dir.name, rotated, args.timeout
                )
                parity = benchmark.cross_check(vectors, reference, 0.99999)
                if not parity["pass"]:
                    raise RuntimeError(f"soak vector/order parity failed: {parity}")
                with lock:
                    counts["requests"] += 1
                    counts["vectors"] += len(vectors)
                    counts["min_cosine"] = min(
                        counts["min_cosine"], parity["min_cosine"]
                    )
            except Exception as exc:
                detail = str(exc)
                if isinstance(exc, urllib.error.HTTPError):
                    detail += ": " + exc.read(4096).decode(errors="replace")
                with lock:
                    counts["failures"] += 1
                    if len(failures) < 64:
                        failures.append({"worker": worker_id, "error": detail})
                print(f"soak worker={worker_id} failure={detail}", flush=True)
                stop.set()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(worker, index) for index in range(args.workers)]
        try:
            while not stop.is_set() and time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("Antfly exited during soak")
                swap = subprocess.run(
                    ["sysctl", "vm.swapusage"],
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
                sample = process_tree_memory(process.pid)
                if {row["pid"] for row in sample["processes"]} != initial_processes:
                    raise RuntimeError("owned serving worker restarted during soak")
                memory.append(
                    {
                        "elapsed_s": time.monotonic() - started,
                        **sample,
                        "host_swap": swap,
                        "host_swap_used_bytes": swap_used_bytes(swap),
                    }
                )
                print(
                    f"soak {memory[-1]['elapsed_s']:.0f}s requests={counts['requests']} failures={counts['failures']} rss={memory[-1]['rss_bytes']}",
                    flush=True,
                )
                stop.wait(min(15, max(0, deadline - time.monotonic())))
        finally:
            stop.set()
        for future in futures:
            future.result()
    # Host swap growth invalidates qualification without attributing it to
    # this process; competing applications can also invalidate isolation.
    tail = [row["rss_bytes"] for row in memory[len(memory) // 2 :]]
    plateau = len(tail) >= 4 and max(tail) - min(tail) <= max(
        32 * 1024 * 1024, statistics.fmean(tail) * 0.05
    )
    swap_growth = (
        max(row["host_swap_used_bytes"] for row in memory)
        - memory[0]["host_swap_used_bytes"]
        if memory
        else None
    )
    elapsed = time.monotonic() - started
    return {
        **counts,
        "duration_s": elapsed,
        "workers": args.workers,
        "batch": 32,
        "tokens_per_row": 256,
        "memory": memory,
        "memory_plateau_pass": plateau,
        "host_swap_growth_bytes": swap_growth,
        "failures_detail": failures,
        "pass": counts["failures"] == 0
        and counts["requests"] > 0
        and plateau
        and swap_growth == 0
        and args.workers >= 8
        and min(elapsed, args.soak_seconds) >= 1800,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--antfly", type=Path, required=True)
    parser.add_argument("--llama", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--query-fixture", type=Path)
    parser.add_argument(
        "--baseline-dir",
        type=Path,
        help="same-budget fixed-baseline passage reports for the separate 5 percent regression gate",
    )
    parser.add_argument(
        "--baseline-antfly",
        type=Path,
        help="alternate requests against a live fixed Antfly baseline in the regression stage",
    )
    parser.add_argument(
        "--eviction-model",
        help="optional sibling model for max-loaded-models=1 teardown/reload qualification",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--stage",
        choices=(
            "short",
            "passage",
            "regression",
            "capacity",
            "correctness",
            "soak",
            "all",
        ),
        default="all",
    )
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--iters", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--soak-seconds", type=int, default=1800)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--budget-mb", type=int, default=4096)
    parser.add_argument(
        "--scratch-budget-mb",
        type=int,
        help="explicit operator scratch cap; defaults to the server's derived cap",
    )
    parser.add_argument(
        "--combined-budget-mb",
        type=int,
        help="explicit operator combined cap; process limit remains independent",
    )
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--antfly-port", type=int, default=18100)
    parser.add_argument("--reference-port", type=int, default=18101)
    parser.add_argument(
        "--reference-ubatch",
        type=int,
        default=2048,
        help="llama.cpp physical microbatch cap, recorded with its live arguments",
    )
    args = parser.parse_args()
    if (
        any(
            value < 1
            for value in (
                args.rounds,
                args.iters,
                args.soak_seconds,
                args.workers,
                args.budget_mb,
            )
        )
        or args.warmup < 0
    ):
        parser.error(
            "positive rounds, iterations, soak duration, workers and budget required"
        )
    if args.scratch_budget_mb is not None and args.scratch_budget_mb < 1:
        parser.error("scratch budget must be positive")
    if args.combined_budget_mb is not None and args.combined_budget_mb < 1:
        parser.error("combined budget must be positive")
    if args.reference_ubatch < 1:
        parser.error("reference microbatch must be positive")
    if args.baseline_antfly and (
        args.stage != "regression" or args.baseline_dir or args.eviction_model
    ):
        parser.error(
            "baseline-antfly requires the regression stage without baseline-dir or eviction-model"
        )
    if args.antfly_port == args.reference_port:
        parser.error("owned servers require distinct ports")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    weights = list(args.model_dir.glob("*.gguf"))
    if len(weights) != 1:
        parser.error("model directory must contain exactly one GGUF")
    weight = weights[0]
    cases = benchmark.load_fixture(args.fixture, benchmark.sha256_file(weight))
    antfly_args = [
        "run",
        "--host",
        "127.0.0.1",
        "--port",
        str(args.antfly_port),
        "--models-dir",
        str(args.model_dir.parent.resolve()),
        "--max-loaded-models",
        "1",
        "--max-concurrent-requests",
        "32",
        "--process-memory-budget-mb",
        str(args.budget_mb),
    ]
    # Startup preloads are pinned. A real eviction/reload test must instead
    # load through requests, with no pinned publication preventing eviction.
    if not args.eviction_model:
        antfly_args += ["--preload-model", f"embedder:metal:{args.model_dir.name}"]
    if args.scratch_budget_mb is not None:
        antfly_args += ["--scratch-budget-mb", str(args.scratch_budget_mb)]
    if args.combined_budget_mb is not None:
        antfly_args += ["--combined-budget-mb", str(args.combined_budget_mb)]
    url = f"http://127.0.0.1:{args.antfly_port}/ai/v1/embeddings"
    reference = f"http://127.0.0.1:{args.reference_port}"
    env = dict(
        os.environ,
        ANTFLY_INFERENCE_REQUIRED_BACKEND="metal",
        TERMITE_EMBED_RESIDENT_FAIL_CLOSED="1",
    )
    (args.output_dir / "provenance.json").write_text(
        json.dumps(
            {
                "args": {
                    k: str(v) if isinstance(v, Path) else v
                    for k, v in vars(args).items()
                },
                "platform": platform.platform(),
                "machine": platform.machine(),
                "weights_sha256": benchmark.sha256_file(weight),
                "antfly_sha256": benchmark.sha256_file(args.antfly),
                "llama_sha256": benchmark.sha256_file(args.llama),
                "fixture_sha256": benchmark.sha256_file(args.fixture),
                "query_fixture_sha256": benchmark.sha256_file(args.query_fixture)
                if args.query_fixture
                else None,
                "controls": {
                    k: v
                    for k, v in env.items()
                    if k.startswith(("TERMITE_", "ANTFLY_INFERENCE_"))
                },
            },
            indent=2,
        )
        + "\n"
    )
    if args.baseline_dir:
        current = json.loads((args.output_dir / "provenance.json").read_text())
        baseline = json.loads((args.baseline_dir / "provenance.json").read_text())
        for field in (
            "platform",
            "machine",
            "weights_sha256",
            "llama_sha256",
            "fixture_sha256",
        ):
            if current[field] != baseline[field]:
                raise ValueError(f"baseline provenance mismatch: {field}")
        for field in ("budget_mb", "scratch_budget_mb", "combined_budget_mb"):
            if current["args"][field] != baseline["args"][field]:
                raise ValueError(f"baseline budget mismatch: {field}")
        if current["args"]["reference_ubatch"] != baseline["args"].get(
            "reference_ubatch", 8192
        ):
            raise ValueError("baseline reference microbatch mismatch")
    results = []
    with ExitStack() as stack:
        antfly = stack.enter_context(
            server(
                args.antfly.resolve(),
                antfly_args,
                env,
                url.replace("embeddings", "models"),
                args.output_dir / "antfly.log",
            )
        )
        paired_baseline = None
        baseline_args = list(antfly_args)
        baseline_args[baseline_args.index("--port") + 1] = str(args.reference_port)
        if args.baseline_antfly:
            baseline_env = dict(env)
            for flag in (
                "TERMITE_METAL_ENABLE_Q8_0_SMALL_ROWS",
                "TERMITE_METAL_ENABLE_Q8_0_SMALL_ROWS_M64",
                "TERMITE_METAL_ENABLE_QWEN3_EMBED_BATCHING",
                "TERMITE_METAL_ENABLE_QWEN3_HEAD_NORM_SG",
            ):
                # Unset flags now select qualified device defaults. The fixed
                # comparison must explicitly disable these serving changes.
                baseline_env[flag] = "0"
            paired_baseline = stack.enter_context(
                server(
                    args.baseline_antfly.resolve(),
                    baseline_args,
                    baseline_env,
                    reference + "/ai/v1/models",
                    args.output_dir / "baseline.log",
                )
            )
            (args.output_dir / "paired-baseline.json").write_text(
                json.dumps(
                    {
                        "executable_sha256": benchmark.sha256_file(
                            args.baseline_antfly
                        ),
                        "argv": baseline_args,
                        "controls": {
                            k: v
                            for k, v in baseline_env.items()
                            if k.startswith(("TERMITE_", "ANTFLY_INFERENCE_"))
                        },
                        "method": "same-host alternating AB/BA, fixed baseline and candidate; no simultaneous GPU requests",
                        "threshold": 0.95,
                    },
                    indent=2,
                )
                + "\n"
            )
        cells = []
        if args.stage in ("short", "all"):
            cells += [(args.fixture, tokens, 1, 1.0, False) for tokens in (20, 32, 64)]
            if args.query_fixture:
                cells += [
                    (args.query_fixture, tokens, 1, 1.0, True) for tokens in (32, 64)
                ]
        if args.stage in ("passage", "all"):
            cells += [
                (
                    args.fixture,
                    tokens,
                    batch,
                    0.9 if (tokens, batch) == (256, 32) else 0.95,
                    False,
                )
                for tokens, batch in (
                    (256, 1),
                    (511, 1),
                    (2551, 1),
                    (20, 8),
                    (20, 32),
                    (256, 8),
                    (256, 32),
                )
            ]
        if args.stage == "regression":
            # This separate gate compares already-supported shapes with the
            # fixed audit build. Reference parity/attestation still apply;
            # the 32x256 reference target belongs to the capacity stage.
            cells += [
                (
                    args.fixture,
                    tokens,
                    batch,
                    0.95 if args.baseline_antfly else None,
                    False,
                )
                for tokens, batch in (
                    (256, 1),
                    (511, 1),
                    (2551, 1),
                    (20, 8),
                    (20, 32),
                    (256, 8),
                )
            ]
        if args.stage == "capacity":
            # Qualify the advertised API batch at the default derived limits
            # independently of long singleton operator-budget qualification.
            cells += [(args.fixture, 256, 32, 0.9, False)]
        for fixture, tokens, batch, threshold, query in cells:
            llama_args = [
                "-m",
                str(weight.resolve()),
                "--alias",
                "model",
                "--embeddings",
                "--pooling",
                "last",
                "-c",
                "16384" if batch > 1 else "8192",
                "-b",
                "8192",
                "-ub",
                str(max(tokens, args.reference_ubatch)),
                "--flash-attn",
                "on",
                "--cache-ram",
                "0",
                "--parallel",
                "32" if batch > 1 else "1",
                "--host",
                "127.0.0.1",
                "--port",
                str(args.reference_port),
                "--slot-save-path",
                str(args.output_dir.resolve()),
            ]
            reference_executable = args.baseline_antfly or args.llama
            reference_args = baseline_args if paired_baseline else llama_args
            reference_embeddings = reference + (
                "/ai/v1/embeddings" if paired_baseline else "/v1/embeddings"
            )
            reference_context = (
                nullcontext(paired_baseline)
                if paired_baseline
                else server(
                    args.llama.resolve(),
                    llama_args,
                    env,
                    reference + "/health",
                    args.output_dir / f"llama_{tokens}_{batch}_{query}.log",
                )
            )
            with reference_context as llama:
                for round_index in range(args.rounds):
                    output = (
                        args.output_dir
                        / f"{'query' if query else 'document'}_{tokens}_{batch}_r{round_index + 1}.json"
                    )
                    command = [
                        sys.executable,
                        str(Path(benchmark.__file__)),
                        "--url",
                        url,
                        "--reference-url",
                        reference + "/v1/embeddings",
                        "--model",
                        args.model_dir.name,
                        "--reference-model",
                        "model",
                        "--fixture",
                        str(fixture),
                        "--fixture-token-count",
                        str(tokens),
                        "--antfly-reported-token-offset",
                        "-1",
                        "--batch-sizes",
                        str(batch),
                        "--iters",
                        str(args.iters),
                        "--warmup",
                        str(args.warmup),
                        "--require-comparable",
                        "--cosine-threshold",
                        "0.995",
                        "--antfly-model-file",
                        str(weight),
                        "--reference-model-file",
                        str(weight),
                        "--antfly-build-id",
                        benchmark.sha256_file(args.antfly),
                        "--reference-build-id",
                        benchmark.sha256_file(args.llama),
                        "--antfly-build-file",
                        str(args.antfly.resolve()),
                        "--reference-build-file",
                        str(args.llama.resolve()),
                        "--antfly-server-pid",
                        str(antfly.pid),
                        "--reference-server-pid",
                        str(llama.pid),
                        "--antfly-server-args",
                        shlex.join(antfly_args),
                        "--reference-server-args",
                        shlex.join(llama_args),
                        "--output",
                        str(output),
                    ]
                    if threshold is not None:
                        command += ["--fail-below-ratio", str(threshold)]
                    if paired_baseline:
                        overrides = {
                            "--reference-url": reference_embeddings,
                            "--reference-model": args.model_dir.name,
                            "--reference-build-id": benchmark.sha256_file(
                                reference_executable
                            ),
                            "--reference-build-file": str(
                                reference_executable.resolve()
                            ),
                            "--reference-server-args": shlex.join(reference_args),
                        }
                        for option, value in overrides.items():
                            command[command.index(option) + 1] = value
                        command += ["--reference-reported-token-offset", "-1"]
                    if query:
                        prefix = json.loads(fixture.read_text())["query_prefix"]
                        command += [
                            "--task-type",
                            "query",
                            "--query-prefix",
                            prefix,
                            "--reference-slots-url",
                            reference + "/slots",
                        ]
                    with output.with_suffix(".log").open("w") as log:
                        result = subprocess.run(
                            command, stdout=log, stderr=subprocess.STDOUT
                        )
                    results.append(
                        {"report": output.name, "exit_code": result.returncode}
                    )
                    if output.exists() and tokens <= 64 and batch == 1:
                        report = json.loads(output.read_text())
                        latencies = {
                            row["target"]: row["latency"] for row in report["results"]
                        }
                        p95_pass = (
                            latencies["antfly"]["p95_ms"]
                            <= latencies["reference"]["p95_ms"] * 1.05
                        )
                        results[-1]["p95_within_5_percent_pass"] = p95_pass
                        if not p95_pass:
                            results[-1]["exit_code"] = 1
                    if (
                        args.baseline_dir
                        and args.stage in ("passage", "regression", "all")
                        and (tokens, batch) != (256, 32)
                        and not query
                        and (tokens > 64 or batch > 1)
                    ):
                        try:
                            regression = baseline_regression(
                                json.loads(output.read_text()),
                                json.loads(
                                    (args.baseline_dir / output.name).read_text()
                                ),
                            )
                        except (OSError, ValueError, KeyError, StopIteration) as exc:
                            regression = {"pass": False, "error": str(exc)}
                        results[-1]["baseline_regression"] = regression
                        if not regression["pass"]:
                            results[-1]["exit_code"] = 1
                    print(f"{output.name}: exit={results[-1]['exit_code']}", flush=True)
        if args.stage in ("correctness", "all"):
            try:
                result = correctness(args, url, cases)
            except Exception as exc:
                detail = str(exc)
                if isinstance(exc, urllib.error.HTTPError):
                    detail += ": " + exc.read(4096).decode(errors="replace")
                result = {"pass": False, "error": detail}
            print(f"correctness: pass={result['pass']}", flush=True)
            (args.output_dir / "correctness.json").write_text(
                json.dumps(result, indent=2) + "\n"
            )
            results.append(
                {"report": "correctness.json", "exit_code": 0 if result["pass"] else 1}
            )
        if args.stage in ("soak", "all"):
            # Reference has exited before isolated capacity qualification.
            try:
                result = soak(args, antfly, url, cases)
            except Exception as exc:
                detail = str(exc)
                if isinstance(exc, urllib.error.HTTPError):
                    detail += ": " + exc.read(4096).decode(errors="replace")
                result = {"pass": False, "error": detail}
            (args.output_dir / "soak.json").write_text(
                json.dumps(result, indent=2) + "\n"
            )
            results.append(
                {"report": "soak.json", "exit_code": 0 if result["pass"] else 1}
            )
    (args.output_dir / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    return int(any(row["exit_code"] for row in results))


if __name__ == "__main__":
    raise SystemExit(main())
