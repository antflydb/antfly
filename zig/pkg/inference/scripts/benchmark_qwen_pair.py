#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run serial, alternating requests against two resident Antfly Qwen builds.

Launch this script under benchmark_resources.py to bound both servers and their
workers. A passing report means output parity; latency retention also requires
the independent runs and confirmation described in QWEN_PERFORMANCE.md.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
import os
import platform
from pathlib import Path
import shlex
import statistics
import subprocess
import sys
import time
import urllib.request

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS / "qwen3_embedding"))
sys.path.insert(0, str(SCRIPTS / "qwen3vl"))
import benchmark_qwen3_embedding_endpoint as embedding  # noqa: E402
import benchmark_qwen3vl_ocr_endpoint as ocr  # noqa: E402
from qualify_qwen3vl_metal import write_json_atomic  # noqa: E402


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def start_server(stack, binary, port, args, label):
    argv = [
        "run",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--models-dir",
        str(args.models_dir),
        "--max-loaded-models",
        "1",
        "--max-concurrent-requests",
        "32",
        "--kernel-jit-mode",
        "off",
        "--process-memory-budget-mb",
        "8192",
        "--host-budget-mb",
        "4096",
        "--backend-budget-mb",
        "6144",
        "--combined-budget-mb",
        "8192",
        "--kv-budget-mb",
        "512",
        "--scratch-budget-mb",
        "1024",
    ]
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("TERMITE_", "ANTFLY_EMBED_TRACE"))
    }
    env["TERMITE_EMBED_RESIDENT_FAIL_CLOSED"] = "1"
    for setting in getattr(args, label + "_env"):
        key, value = setting.split("=", 1)
        if not key.startswith("TERMITE_"):
            raise ValueError("benchmark overrides must be TERMITE_ variables")
        env[key] = value
    stdout = stack.enter_context((args.output / (label + ".stdout")).open("wb"))
    stderr = stack.enter_context((args.output / (label + ".stderr")).open("wb"))
    started = time.monotonic()
    process = subprocess.Popen(
        [str(binary), *argv], env=env, stdout=stdout, stderr=stderr
    )
    stack.callback(stop, process)
    url = f"http://127.0.0.1:{port}"
    while True:
        if process.poll() is not None:
            raise RuntimeError(f"{label} exited with {process.returncode}")
        try:
            with urllib.request.urlopen(url + "/healthz", timeout=1) as response:
                if response.status == 200:
                    break
        except OSError:
            if time.monotonic() - started > 45:
                raise
            time.sleep(0.25)
    provenance = embedding.process_provenance(process.pid, binary, shlex.join(argv))
    provenance["environment"] = {
        k: v for k, v in env.items() if k.startswith("TERMITE_")
    }
    provenance["startup_to_health_ms"] = (time.monotonic() - started) * 1000
    return url + "/ai/v1/", provenance


def measure_embeddings(lengths, cases, targets, args, on_pair):
    selected = [embedding.select_fixture_token_count(cases, n) for n in lengths]
    if any(len(items) < args.warmup + args.iters for items in selected):
        raise ValueError("insufficient distinct fixture cases for the requested pairs")
    samples = {label: [] for label in targets}
    pairs = []
    bitwise_equal = True
    max_abs_error = 0.0
    min_cosine = 1.0
    for iteration in range(args.warmup + args.iters):
        batch = [items[iteration] for items in selected]
        pair = {"iteration": iteration, "warmup": iteration < args.warmup, "ms": {}}
        vectors = {}
        order = list(targets)
        if iteration % 2:
            order.reverse()
        for label in order:
            elapsed, vectors[label], model, tokens = embedding.request_embeddings(
                targets[label] + "embeddings",
                args.model,
                [case["text"] for case in batch],
                args.timeout,
            )
            if model != args.model or tokens != sum(lengths) - len(lengths):
                raise ValueError("embedding model or exact token-count mismatch")
            for vector in vectors[label]:
                norm = sum(value * value for value in vector) ** 0.5
                if len(vector) != 1024 or not 0.9999 <= norm <= 1.0001:
                    raise ValueError("embedding dimension, finiteness, or L2 mismatch")
            pair["ms"][label] = elapsed
            if not pair["warmup"]:
                samples[label].append(elapsed)
        parity = embedding.cross_check(
            vectors["candidate"], vectors["baseline"], 0.9999
        )
        if not parity["pass"]:
            raise ValueError(f"baseline embedding parity failed: {parity}")
        min_cosine = min(min_cosine, parity["min_cosine"])
        max_abs_error = max(max_abs_error, parity["max_abs_error"])
        bitwise_equal = bitwise_equal and vectors["candidate"] == vectors["baseline"]
        pair["case_ids"] = [case["id"] for case in batch]
        pair["min_cosine"] = parity["min_cosine"]
        pairs.append(pair)
        on_pair(pair)
    return {
        "id": "tokens_" + "_".join(map(str, lengths)),
        "lengths": lengths,
        "pairs": pairs,
        "samples_ms": samples,
        "min_cosine": min_cosine,
        "max_abs_error": max_abs_error,
        "bitwise_equal": bitwise_equal,
        "latency": {k: embedding.latency_summary(v) for k, v in samples.items()},
        "median_ms": {k: statistics.median(v) for k, v in samples.items()},
        **{
            stat + "_speedup": embedding.bootstrap_ratio_ci(
                samples["candidate"], samples["baseline"], 2000, 1729, statistic=stat
            )
            for stat in ("median", "p95")
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument("--" + label, type=Path, required=True)
        parser.add_argument("--" + label + "-env", action="append", default=[])
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--model")
    parser.add_argument("--model-file", type=Path, help="exact GGUF for embedding runs")
    parser.add_argument("--model-dir", type=Path, help="split GGUF bundle for OCR runs")
    parser.add_argument("--phase", choices=("embedding", "ocr"), required=True)
    parser.add_argument(
        "--output", type=Path, required=True, help="new evidence directory"
    )
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--golden", type=Path)
    parser.add_argument("--case", action="append")
    parser.add_argument("--length", type=int, action="append")
    parser.add_argument(
        "--batch-lengths",
        action="append",
        default=[],
        help="comma-separated exact lengths for one ragged batch",
    )
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--iters", type=int, default=20)
    parser.add_argument("--port", type=int, default=18191)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if (
        args.warmup < 0
        or args.iters < 1
        or args.timeout <= 0
        or not 1024 <= args.port < 65535
    ):
        parser.error("invalid warmup, iteration, timeout, or port")
    args.baseline = args.baseline.resolve(strict=True)
    args.candidate = args.candidate.resolve(strict=True)
    args.models_dir = args.models_dir.resolve(strict=True)
    args.model = args.model or (
        "Qwen/Qwen3-Embedding-0.6B-GGUF"
        if args.phase == "embedding"
        else "Qwen/Qwen3-VL-2B-Instruct-GGUF"
    )
    if args.phase == "embedding" and not args.model_file:
        parser.error("embedding runs require --model-file")
    if args.phase == "ocr" and (not args.model_dir or not args.golden):
        parser.error("OCR runs require --model-dir and --golden")
    args.fixture = args.fixture or (
        SCRIPTS / "qwen3_embedding/fixtures/qwen3_embedding_0_6b_exact_tokens.json"
        if args.phase == "embedding"
        else SCRIPTS / "qwen3vl/fixtures/ocr/fixture.json"
    )
    args.output.mkdir(parents=True, exist_ok=False)
    report = {
        "schema": "antfly.qwen_paired_benchmark.v1",
        "pass": False,
        "phase": args.phase,
        "model": args.model,
        "warmup": args.warmup,
        "iters": args.iters,
        "fixture_sha256": embedding.sha256_file(args.fixture),
        "runner_sha256": embedding.sha256_file(Path(__file__)),
        "order": "alternating AB/BA, one active request",
        "servers": {},
        "cases": [],
        "system": {"os": platform.platform(), "architecture": platform.machine()},
    }
    if sys.platform == "darwin":
        model, memory, cpu = subprocess.check_output(
            ["sysctl", "-n", "hw.model", "hw.memsize", "machdep.cpu.brand_string"],
            text=True,
        ).splitlines()
        report["system"].update(model=model, memory_bytes=int(memory), cpu=cpu)

    def save():
        write_json_atomic(
            args.output / "report.json",
            embedding.portable_report(report, workdir=Path.cwd(), home=Path.home()),
        )

    def on_pair(pair):
        report["current_pairs"].append(pair)
        save()
        print(json.dumps({"case": report["current_case"], **pair}), flush=True)

    try:
        with ExitStack() as stack:
            targets = {}
            for index, label in enumerate(("baseline", "candidate")):
                targets[label], report["servers"][label] = start_server(
                    stack, getattr(args, label), args.port + index, args, label
                )
            if args.phase == "embedding":
                report["model_sha256"] = embedding.sha256_file(args.model_file)
                cases = embedding.load_fixture(args.fixture)
                lengths = args.length or (
                    [] if args.batch_lengths else [20, 256, 511, 2551, 4096, 8192]
                )
                workloads = [[n] for n in lengths] + [
                    [int(n) for n in item.split(",")] for item in args.batch_lengths
                ]
                for lengths in workloads:
                    report["current_case"], report["current_pairs"] = lengths, []
                    report["cases"].append(
                        measure_embeddings(lengths, cases, targets, args, on_pair)
                    )
                    save()
            else:
                golden = json.loads(args.golden.read_text())
                if (
                    golden["schema"] != ocr.GOLDEN_SCHEMA
                    or golden["fixture_sha256"] != report["fixture_sha256"]
                ):
                    raise ValueError("OCR golden fixture identity mismatch")
                if ocr.model_artifacts(args.model_dir) != golden["artifacts"]:
                    raise ValueError("OCR model bundle differs from golden")
                report["artifacts"] = golden["artifacts"]
                cases = ocr.load_cases(args.fixture)
                if args.case and not set(args.case) <= {case["id"] for case in cases}:
                    raise ValueError("unknown selected OCR case")
                for case in cases:
                    if args.case and case["id"] not in args.case:
                        continue
                    report["current_case"], report["current_pairs"] = case["id"], []
                    report["cases"].append(
                        ocr.measure_case(
                            case,
                            {
                                "reference": (targets["baseline"] + "read", args.model),
                                "candidate": (
                                    targets["candidate"] + "read",
                                    args.model,
                                ),
                            },
                            golden["outputs"][case["id"]],
                            warmup=args.warmup,
                            iters=args.iters,
                            timeout=args.timeout,
                            on_pair=on_pair,
                        )
                    )
                    save()
            del report["current_case"], report["current_pairs"]
            report["pass"] = True
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(report["error"], file=sys.stderr)
    finally:
        save()
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
