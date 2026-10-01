#!/usr/bin/env python3
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

"""Fail-closed L4 qualification for native Laya CUDA inference.

Build inference-laya-cuda-test-build and antfly first. --prepare downloads only
pinned sources/checkpoints into --work-dir. A passing report requires the complete
numerical suite, real CUDA sessions, HTTP parity, concurrency, and a 30-minute soak.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import select
import shutil
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODEL = "convaiinnovations/laya"
REVISION = "c5d78730f3493e4fe16d61507ef4b78eef7318cf"
COMMON_REVISION = "6a5819129eb220570792e417e49723d697efd76f"
COMMON_SHA256 = "f948ee606abe2ed2463f830c051f1a60dccc1b9f5ca1fdc15635cfcbe0cff7b2"
DATASETS = {
    "ag-news.jsonl": "sh0416/ag_news/resolve/70e3fa1915be9a8daebec5e840f20df9a8e18793/test.jsonl",
    "boolq.parquet": "google/boolq/resolve/35b264d03638db9f4ce671b711558bf7ff0f80d5/data/validation-00000-of-00001.parquet",
    "sst5.jsonl": "SetFit/sst5/resolve/e51bdcd8cd3a30da231967c1a249ba59361279a3/test.jsonl",
}


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run(argv, *, env=None, log=None, timeout=3600):
    with Path(log).open("w") if log else nullcontext(subprocess.PIPE) as destination:
        result = subprocess.run(
            [str(v) for v in argv],
            env=env,
            text=True,
            stdout=destination,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    output = Path(log).read_text() if log else result.stdout
    if result.returncode:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {argv}\n{output[-8000:]}"
        )
    return output


def download(url, path):
    if not path.exists():
        temporary = path.with_suffix(path.suffix + ".part")
        try:
            urllib.request.urlretrieve(url, temporary)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def validate_test_run(output, backend):
    summaries = re.findall(r"(\d+) selected; (\d+) passed; (\d+) skipped", output)
    if len(summaries) != 1:
        raise ValueError("Qualification requires exactly one test summary")
    selected, passed, skipped = map(int, summaries[0])
    if selected == 0 or skipped != 0 or selected != passed:
        raise ValueError("Qualification tests skipped or failed")
    required = ["laya released checkpoint accuracy parity batching and performance"]
    if backend == "cuda":
        required += [
            "laya forward preprocessing and batching match the PyTorch reference",
            "laya CUDA batched RoPE matches independent positions and native rotation",
            "laya CUDA local attention matches explicit bidirectional window bias",
            "laya CUDA marker gather and action features preserve padding ties and row boundaries",
            "laya CUDA warp attention matches legacy at shape and window boundaries",
            "laya CUDA packed exact GELU matches unfused projection slices",
            "laya extraction v2 serves typed decisions over HTTP and embedded calls",
            "readiness inventory starts with no async worker capacity",
        ]
    for name in required:
        if not re.search(
            r"^\d+/\d+ [^\n]*\.test\." + re.escape(name) + r"\.\.\.",
            output,
            re.MULTILINE,
        ):
            raise ValueError(f"Qualification did not run required test: {name}")
    if f"Laya qualification backend={backend} " not in output:
        raise ValueError("Qualification did not select the required backend")
    return {"selected": selected, "passed": passed, "skipped": skipped}


def prepare(work, uv):
    common = work / "common.py"
    download(
        f"https://raw.githubusercontent.com/NandhaKishorM/laya/{COMMON_REVISION}/laya/common.py",
        common,
    )
    if sha256(common) != COMMON_SHA256:
        raise ValueError("Upstream common.py checksum mismatch")
    py = [
        uv,
        "run",
        "--index",
        "https://download.pytorch.org/whl/cpu",
        "--index-strategy",
        "unsafe-best-match",
        "--with",
        "torch==2.6.0+cpu",
    ]
    if not (work / "synthetic/reference.json").exists():
        run(
            [
                *py,
                ROOT / "scripts/laya_reference.py",
                "--common",
                common,
                "--output",
                work / "synthetic",
            ],
            log=work / "prepare-synthetic.log",
        )
    if not (work / "released/model").exists():
        run(
            [
                uv,
                "run",
                ROOT / "scripts/prepare_laya.py",
                MODEL,
                "--revision",
                REVISION,
                "--output",
                work / "released/model",
            ],
            log=work / "prepare-model.log",
        )
    for filename, suffix in DATASETS.items():
        download(f"https://huggingface.co/datasets/{suffix}", work / filename)
    if not (work / "released/qualification.json").exists():
        run(
            [
                *py,
                ROOT / "scripts/laya_qualify.py",
                "--model",
                work / "released/model",
                "--common",
                common,
                "--ag-news",
                work / "ag-news.jsonl",
                "--boolq",
                work / "boolq.parquet",
                "--sst5",
                work / "sst5.jsonl",
                "--output",
                work / "released/qualification.json",
                "--threads",
                str(max(1, min(4, (os.cpu_count() or 1) // 2))),
            ],
            log=work / "prepare-reference.log",
        )


def validate_fixtures(work):
    oracle = json.loads((work / "released/qualification.json").read_text())
    manifest = json.loads((work / "released/model/model_manifest.json").read_text())
    source = {"repository": MODEL, "revision": REVISION}
    if manifest.get("source") != source or oracle.get("source") != source:
        raise ValueError("Qualification requires the pinned English Laya checkpoint")
    if oracle.get("file_sha256", {}).get("common.py") != COMMON_SHA256:
        raise ValueError("Qualification requires the pinned upstream oracle")
    if sha256(work / "common.py") != COMMON_SHA256:
        raise ValueError("Upstream comparison source checksum mismatch")
    if len(oracle["rows"]) != 192:
        raise ValueError("Qualification requires all 192 reference examples")
    synthetic = json.loads((work / "synthetic/reference.json").read_text())
    if not synthetic.get("intermediates"):
        raise ValueError(
            "Regenerate the synthetic fixture with intermediate references"
        )
    return oracle


def request_body(rows, copies=1):
    inputs = []
    for index, row in enumerate(rows):
        task = row["task"]
        question = task["question"]
        classification = {
            "name": question["name"],
            "instruction": question["instruction"],
            "mode": {"choice": "single", "score": "ordinal", "noul": "boolean"}[
                question["kind"]
            ],
            "labels": question["labels"],
        }
        definitions = {
            label: {"description": description}
            for label, description in zip(question["labels"], question["descriptions"])
            if description
        }
        if definitions:
            classification["label_definitions"] = definitions
        inputs.append(
            {
                "id": str(index),
                "content": task["text"],
                "schema": {
                    "classifications": [
                        dict(
                            classification,
                            name=question["name"]
                            if copies == 1
                            else f"{question['name']}_{copy}",
                        )
                        for copy in range(copies)
                    ]
                },
            }
        )
    return {"model": "model", "schema_version": 2, "schema": {}, "inputs": inputs}


def check_response(response, rows, copies=1):
    data = response["data"]
    if len(data) != len(rows):
        raise ValueError("HTTP result count mismatch")
    for index, (item, row) in enumerate(zip(data, rows)):
        if item.get("id") != str(index) or len(item["decisions"]) != copies:
            raise ValueError("HTTP result grouping mismatch")
        for copy, decision in enumerate(item["decisions"]):
            labels = row["task"]["question"]["labels"]
            entries = decision["probabilities"]
            if [entry["label"] for entry in entries] != labels:
                raise ValueError("Probability label order mismatch")
            actual = [entry["probability"] for entry in entries]
            if any(
                not math.isfinite(value) or abs(value - expected) > 5e-5
                for value, expected in zip(actual, row["probabilities"])
            ):
                raise ValueError("HTTP probabilities exceed the strict parity bound")
            expected_label = labels[
                max(range(len(labels)), key=row["probabilities"].__getitem__)
            ]
            if decision["label"] != expected_label:
                raise ValueError("HTTP decision differs from the oracle")
            act = decision["act_probability"]
            if not math.isfinite(act) or abs(act - row["act_probability"]) > 5e-5:
                raise ValueError("HTTP action probability differs from the oracle")
            question = row["task"]["question"]
            expected_name = (
                question["name"] if copies == 1 else f"{question['name']}_{copy}"
            )
            kind = question["kind"]
            if decision["name"] != expected_name or decision["type"] != (
                "boolean" if kind == "noul" else kind
            ):
                raise ValueError("HTTP decision identity mismatch")
            probabilities = row["probabilities"]
            entropy = -sum(p * math.log(max(p, 1e-12)) for p in probabilities)
            confidence = (
                max(probabilities)
                if kind == "noul"
                else max(0, min(1, 1 - entropy / math.log(len(labels))))
            )
            derived = {"confidence": confidence}
            if kind == "score":
                derived["expected_value"] = sum(
                    i * p for i, p in enumerate(probabilities)
                )
            if kind == "noul":
                derived["true_probability"] = probabilities[1]
            for name, expected in derived.items():
                actual = decision[name]
                if not math.isfinite(actual) or abs(actual - expected) > 5e-5:
                    raise ValueError(f"HTTP {name} differs from the oracle")
            method = (
                "max_probability" if kind == "noul" else "normalized_inverse_entropy"
            )
            if decision["confidence_method"] != method:
                raise ValueError("HTTP confidence method mismatch")


def http_json(url, body=None, timeout=300):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        error.msg = f"{error.msg}: {error.read(2048).decode(errors='replace')}"
        raise


def memory_sample(pid, nvidia_smi):
    def process_rss(process_id):
        try:
            status = Path(f"/proc/{process_id}/status").read_text()
            match = re.search(r"VmRSS:\s+(\d+)", status)
            rss = int(match[1]) * 1024 if match else 0
            children = set()
            for task in Path(f"/proc/{process_id}/task").iterdir():
                children.update((task / "children").read_text().split())
            return rss + sum(process_rss(child) for child in children)
        except FileNotFoundError:
            return 0

    rss = process_rss(pid)
    device = run(
        [nvidia_smi, "--query-gpu=memory.used", "--format=csv,noheader,nounits"]
    )
    return {
        "host_rss_bytes": rss,
        "device_used_bytes": int(device.strip().splitlines()[0]) * 1024**2,
    }


def cancel_and_recover(port, rows, nvidia_smi, execute, process):
    def utilization():
        return int(
            run(
                [
                    nvidia_smi,
                    "--query-gpu=utilization.gpu",
                    "--format=csv,noheader,nounits",
                ]
            )
            .strip()
            .splitlines()[0]
        )

    idle_deadline = time.monotonic() + 30
    while utilization() != 0:
        if time.monotonic() >= idle_deadline:
            raise RuntimeError("GPU did not become idle before cancellation test")
        time.sleep(0.1)
    body = json.dumps(request_body(rows[:128], copies=4)).encode()
    with socket.create_connection(("127.0.0.1", port), timeout=10) as client:
        client.sendall(
            b"POST /ai/v1/extract HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\n\r\n"
            + body
        )
        deadline = time.monotonic() + 60
        while utilization() == 0:
            if select.select([client], [], [], 0)[0]:
                response = client.recv(4096, socket.MSG_PEEK).decode(errors="replace")
                raise RuntimeError(
                    f"Cancellation request finished before GPU activity: {response}"
                )
            if time.monotonic() >= deadline:
                raise RuntimeError("No GPU activity observed for cancellation request")
            time.sleep(0.05)
        # An HTTP/1 FIN can mean the client is waiting for a response. RST is
        # required to exercise the production disconnect cancellation path.
        client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    began = time.monotonic()
    deadline = began + 120
    while True:
        if process.poll() is not None:
            raise RuntimeError("Supervisor exited after client cancellation")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("Cancellation recovery exceeded 120 seconds")
        try:
            execute(rows[:1], timeout=remaining)
            if time.monotonic() > deadline:
                raise RuntimeError("Cancellation recovery exceeded 120 seconds")
            break
        except urllib.error.HTTPError as error:
            if error.code != 503 or time.monotonic() >= deadline:
                raise
        except (ConnectionError, urllib.error.URLError):
            if time.monotonic() >= deadline:
                raise
        time.sleep(0.1)
    return {
        "disconnect": "TCP RST",
        "gpu_activity_observed": True,
        "parity_after_recovery": True,
        "recovery_ms": (time.monotonic() - began) * 1000,
    }


def serve(args, env, oracle, report):
    (args.work_dir / "ml").mkdir(exist_ok=True)
    report["server_config"] = {
        "max_concurrent_requests": 4,
        "max_loaded_models": 1,
        "scratch_budget_mib": 3072,
    }
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    log_path = args.work_dir / "server.log"
    device_log_path = args.work_dir / "http-device-memory.csv"
    with (
        tempfile.TemporaryDirectory(
            prefix="laya-reload-", dir=args.work_dir / "released"
        ) as reload_dir,
        log_path.open("w") as log,
        device_log_path.open("w") as device_log,
    ):
        shutil.copytree(
            args.work_dir / "released/model",
            reload_dir,
            copy_function=os.link,
            dirs_exist_ok=True,
        )
        process = subprocess.Popen(
            [
                str(args.binary),
                *([] if args.standalone_inference else ["inference"]),
                "run",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--models-dir",
                str(args.work_dir / "released"),
                "--ml-dir",
                str(args.work_dir / "ml"),
                "--max-concurrent-requests",
                "4",
                "--max-loaded-models",
                "1",
                # Each HTTP request reserves 512 MiB of preprocessing scratch.
                # Leave room for four requests and their per-model run permits.
                "--scratch-budget-mb",
                "3072",
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        device_monitor = None
        try:
            device_monitor = subprocess.Popen(
                [
                    args.nvidia_smi,
                    "--query-gpu=memory.used",
                    "--format=csv,noheader,nounits",
                    "--loop-ms=200",
                ],
                stdout=device_log,
                stderr=subprocess.STDOUT,
            )
            ready = time.monotonic() + 120
            while True:
                if process.poll() is not None:
                    raise RuntimeError("Inference server exited during startup")
                try:
                    http_json(base + "/healthz", timeout=2)
                    break
                except (OSError, ValueError):
                    if time.monotonic() >= ready:
                        raise RuntimeError(
                            "Inference server startup timed out"
                        ) from None
                    time.sleep(0.2)
            rows = oracle["rows"]

            def execute(chunk, copies=1, model_name="model", timeout=300):
                began = time.monotonic()
                body = request_body(chunk, copies)
                body["model"] = model_name
                response = http_json(base + "/ai/v1/extract", body, timeout=timeout)
                check_response(response, chunk, copies)
                return (time.monotonic() - began) * 1000

            first_use = execute(rows[:1])
            for start in range(0, len(rows), 8):
                execute(rows[start : start + 8])
            invalid = request_body(rows[:1])
            invalid["inputs"][0]["schema"]["classifications"][0]["mode"] = "multi"
            try:
                http_json(base + "/ai/v1/extract", invalid)
                raise ValueError("Unsupported multi-label request succeeded")
            except urllib.error.HTTPError as error:
                if error.code != 400:
                    raise
            report["concurrency_cases"] = []
            for concurrency in (1, 2, 4):
                for size in (1, 8):
                    with ThreadPoolExecutor(max_workers=concurrency) as pool:
                        list(pool.map(execute, [rows[:size]] * concurrency))
                    report["concurrency_cases"].append(
                        {
                            "requests": concurrency,
                            "tasks_per_request": size,
                            "parity": True,
                        }
                    )
            # Verify the cancellation workload is accepted before testing a
            # disconnect, where no normal response can be inspected.
            execute(rows[:128], copies=4)
            reload_ms = execute(rows[:1], model_name=Path(reload_dir).name)
            return_ms = execute(rows[:1])
            report["model_reload"] = {
                "alternate_model_ms": reload_ms,
                "original_model_ms": return_ms,
                "parity": True,
            }
            report["cancellation"] = cancel_and_recover(
                port, rows, args.nvidia_smi, execute, process
            )
            # Warm the largest HTTP shape before checking retained memory growth.
            execute(rows[:128], copies=4)
            for _ in range(5):
                execute(rows[:8])
            memory_before = memory_sample(process.pid, args.nvidia_smi)
            latencies = []
            began = time.monotonic()
            next_progress = began + 60
            index = 0
            while time.monotonic() - began < args.soak_seconds or len(latencies) < 30:
                size = (1, 3, 8)[index % 3]
                start = index % (len(rows) - size + 1)
                latencies.append(execute(rows[start : start + size]))
                index += 1
                if time.monotonic() >= next_progress:
                    print(
                        f"Laya soak: elapsed={time.monotonic() - began:.0f}s requests={len(latencies)} last_ms={latencies[-1]:.2f}",
                        flush=True,
                    )
                    next_progress = time.monotonic() + 60
            memory_after = memory_sample(process.pid, args.nvidia_smi)
            if device_monitor.poll() is not None:
                raise RuntimeError(
                    "GPU memory sampling stopped during HTTP qualification"
                )
            for domain in memory_before:
                if memory_after[domain] - memory_before[domain] > 256 * 1024**2:
                    raise ValueError(f"Unbounded memory growth: {domain}")
            ordered = sorted(latencies)
            report["http"] = {
                "first_use_ms": first_use,
                "requests": len(latencies),
                "mixed_p50_ms": ordered[len(ordered) // 2],
                "mixed_p95_ms": ordered[math.ceil(len(ordered) * 0.95) - 1],
                "soak_seconds": time.monotonic() - began,
                "memory_before": memory_before,
                "memory_after": memory_after,
            }
        finally:
            if device_monitor is not None:
                device_monitor.terminate()
                try:
                    device_monitor.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    device_monitor.kill()
                    device_monitor.wait()
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    device_samples = [int(value) for value in device_log_path.read_text().splitlines()]
    if not device_samples:
        raise ValueError("HTTP qualification produced no GPU memory samples")
    report["http"]["device_memory_sampling"] = {
        "interval_ms": 200,
        "samples": len(device_samples),
        "peak_device_used_bytes": max(device_samples) * 1024**2,
        "scope": "whole GPU during HTTP cold start, reload, stress, and soak",
    }
    selections = []
    for line in log_path.read_text().splitlines():
        try:
            message = json.loads(line).get("msg", line)
        except (ValueError, AttributeError):
            message = line
        match = re.search(r"selected backend (\w+) for (.+)", message)
        if match:
            selections.append(match.groups())
    if len(selections) < 3 or any(backend != "cuda" for backend, _ in selections):
        raise ValueError("HTTP qualification did not exclusively select CUDA")
    report["backend_selections"] = selections


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--standalone-inference",
        action="store_true",
        help="Binary is antfly-inference rather than the unified antfly CLI",
    )
    parser.add_argument("--tests", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--uv", default="uv")
    parser.add_argument("--nvidia-smi", default="nvidia-smi")
    parser.add_argument("--soak-seconds", type=float, default=1800)
    args = parser.parse_args(argv)
    if args.soak_seconds < 0 or not math.isfinite(args.soak_seconds):
        parser.error("--soak-seconds must be finite and nonnegative")
    args.work_dir = args.work_dir.resolve()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.binary = args.binary.resolve()
    args.tests = args.tests.resolve()
    report = {
        "schema": "antfly.laya.cuda_qualification.v1",
        "status": "failed",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    try:
        report["gpu"] = run(
            [
                args.nvidia_smi,
                "--query-gpu=name,uuid,driver_version,compute_cap,memory.total",
                "--format=csv,noheader",
            ]
        )
        if "NVIDIA L4" not in report["gpu"] or "8.9" not in report["gpu"]:
            raise ValueError("This qualification gate requires an NVIDIA L4")
        report["artifact_identity"] = run(
            [
                args.binary,
                *([] if args.standalone_inference else ["inference"]),
                "cuda-info",
                "--artifact-identity",
            ]
        )
        report["cuda_runtime"] = run(
            [
                args.binary,
                *([] if args.standalone_inference else ["inference"]),
                "cuda-info",
            ]
        )
        if "capability_laya: true" not in report["cuda_runtime"]:
            raise ValueError("CLI does not expose the required Laya CUDA capability")
        report["binary_sha256"] = sha256(args.binary)
        report["tests_sha256"] = sha256(args.tests)
        report["git_commit"] = run(["git", "-C", ROOT, "rev-parse", "HEAD"]).strip()
        report["working_tree_dirty"] = bool(
            run(["git", "-C", ROOT, "status", "--porcelain"]).strip()
        )
        if args.prepare:
            prepare(args.work_dir, args.uv)
        oracle = validate_fixtures(args.work_dir)
        report["oracle"] = {
            key: value for key, value in oracle.items() if key != "rows"
        }
        report["fixture_sha256"] = sha256(args.work_dir / "released/qualification.json")
        report["model_sha256"] = sha256(
            args.work_dir / "released/model/model.safetensors"
        )
        env = {
            **os.environ,
            "ANTFLY_LAYA_BACKEND": "cuda",
            "ANTFLY_LAYA_REQUIRE_TESTS": "1",
            "ANTFLY_LAYA_REFERENCE": str(args.work_dir / "synthetic"),
            "ANTFLY_LAYA_QUALIFICATION": str(args.work_dir / "released"),
            "ANTFLY_INFERENCE_REQUIRED_BACKEND": "cuda",
            "ANTFLY_INFERENCE_PREFERRED_BACKEND": "cuda",
            "ANTFLY_CUDA_ALLOW_HOST_ATTENTION_FALLBACK": "0",
        }
        env.pop("ANTFLY_LAYA_METAL", None)
        env.pop("ANTFLY_LAYA_COMPARISON_ONLY", None)
        env.pop("ANTFLY_LAYA_PERFORMANCE_ONLY", None)
        for key in (
            "ANTFLY_CUDA_LAYA_OPTIMIZATIONS",
            "ANTFLY_CUDA_LAYA_FUSION",
            "ANTFLY_CUDA_LAYA_BUCKETING",
        ):
            env.pop(key, None)
        cpu_env = {
            **env,
            "ANTFLY_LAYA_BACKEND": "native",
            "ANTFLY_INFERENCE_REQUIRED_BACKEND": "native",
            "ANTFLY_INFERENCE_PREFERRED_BACKEND": "native",
            "ANTFLY_LAYA_COMPARISON_ONLY": "1",
        }
        print("Laya CUDA qualification: native CPU comparison", flush=True)
        cpu = run(
            [args.tests, "--test-filter", "laya released"],
            env=cpu_env,
            log=args.work_dir / "cpu-comparison.log",
        )
        report["cpu_tests"] = validate_test_run(cpu, "native")
        report["cpu_measurements"] = re.findall(r"Laya [^\n]+", cpu)
        print("Laya CUDA qualification: PyTorch CUDA comparison", flush=True)
        comparison = args.work_dir / "pytorch-cuda.json"
        run(
            [
                args.uv,
                "run",
                "--index",
                "https://download.pytorch.org/whl/cu124",
                "--index-strategy",
                "unsafe-best-match",
                ROOT / "scripts/laya_cuda_benchmark.py",
                "--work-dir",
                args.work_dir,
                "--output",
                comparison,
            ],
            log=args.work_dir / "pytorch-cuda.log",
        )
        report["pytorch_cuda"] = json.loads(comparison.read_text())
        # Laya tests require CUDA explicitly through their shared helper. The
        # unrelated readiness tests do not construct a supervised model worker,
        # so they must not inherit the production server's global CUDA policy.
        test_env = dict(env)
        test_env.pop("ANTFLY_INFERENCE_REQUIRED_BACKEND", None)
        test_env.pop("ANTFLY_INFERENCE_PREFERRED_BACKEND", None)
        print("Laya CUDA qualification: kernel, parity, and batching tests", flush=True)
        output = run([args.tests], env=test_env, log=args.work_dir / "tests.log")
        report["cuda_tests"] = validate_test_run(output, "cuda")
        report["pipeline_measurements"] = re.findall(r"Laya [^\n]+", output)
        print("Laya CUDA qualification: HTTP parity, recovery, and soak", flush=True)
        serve(args, env, oracle, report)
        report["status"] = "passed" if args.soak_seconds >= 1800 else "incomplete_soak"
    except (
        OSError,
        ValueError,
        RuntimeError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
    ) as error:
        report["error"] = str(error)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "report": str(args.report)}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
