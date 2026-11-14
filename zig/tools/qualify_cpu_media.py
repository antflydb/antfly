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

"""Run bounded native CPU /read checks against a fresh, owned local worker.

Cases are JSON objects with model, kind (image/audio/document), fixture, and
expected (nonempty text fragments). Optional checkpoint_dir hashes local weights;
options supplies /read options, such as max_tokens or selected document pages.
No models are downloaded and no shared service is mutated. First-request timings
include lazy preparation but are not a claim of a cold OS page cache.
"""

import argparse
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normalize(value):
    return " ".join(value.casefold().split())


def validate_read(response, expected):
    items = response.get("data")
    if not isinstance(items, list) or not items:
        raise ValueError("read returned no items")
    for item in items:
        if item.get("error") or item.get("status") != "complete":
            raise ValueError("read was incomplete: " + str(item.get("status")))
        if item.get("truncated") or item.get("finish_reason") == "length":
            raise ValueError("read output was truncated")
    text = normalize(" ".join(item.get("text", "") for item in items))
    if not expected or any(normalize(fragment) not in text for fragment in expected):
        raise ValueError("read omitted expected fixture text")
    return text


def validate_timeout(status, response):
    # Native document/audio reads expose the document contract's Timeout;
    # direct generation uses the inference envelope's INFERENCE_TIMEOUT.
    # DocumentBudgetExhausted can also be 504 and must never pass this probe.
    if status != 504 or response.get("error") not in ("INFERENCE_TIMEOUT", "Timeout"):
        raise ValueError("deadline probe did not return the expected timeout")


def request(port, route, body, timeout, *, budget_ms=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        connection.request(
            "POST" if body is not None else "GET",
            "/ai/v1" + route,
            None if body is None else json.dumps(body),
            {
                "Content-Type": "application/json",
                "X-Antfly-Inference-Timeout-Ms": str(
                    budget_ms or min(3600000, max(1, int(timeout * 1000)))
                ),
            },
        )
        response = connection.getresponse()
        payload = response.read(16 * 1024 * 1024 + 1)
        if len(payload) > 16 * 1024 * 1024:
            raise ValueError("probe response exceeded 16 MiB")
        return response.status, json.loads(payload)
    finally:
        connection.close()


def process_sample(pid):
    """Linux-only evidence for our process tree, including supervised workers."""
    totals = dict(pss_bytes=0, minor_faults=0, major_faults=0, cpu_ticks=0)
    pending, seen = [pid], set()
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        root = Path("/proc") / str(current)
        try:
            for task in (root / "task").iterdir():
                pending.extend(
                    int(child) for child in (task / "children").read_text().split()
                )
            fields = (root / "stat").read_text().rpartition(")")[2].split()
            totals["minor_faults"] += int(fields[7])
            totals["major_faults"] += int(fields[9])
            totals["cpu_ticks"] += int(fields[11]) + int(fields[12])
            for line in (root / "smaps_rollup").read_text().splitlines():
                if line.startswith("Pss:"):
                    totals["pss_bytes"] += int(line.split()[1]) * 1024
        except (OSError, ValueError, IndexError):
            continue
    totals["process_ids"] = sorted(seen)
    return totals


def run_case(args, case, index):
    kind = case["kind"]
    if kind not in ("image", "audio", "document") or not case.get("expected"):
        raise ValueError("each case needs a kind and nonempty expected fragments")
    fixture = Path(case["fixture"]).resolve()
    mime = {"image": "image/png", "audio": "audio/wav", "document": "application/pdf"}[
        kind
    ]
    uri = "data:" + mime + ";base64," + base64.b64encode(fixture.read_bytes()).decode()
    body = dict(case.get("options", {}), model=case["model"])
    source = {"id": "fixture", "url": uri, "media_type": mime}
    if case.get("pages") is not None:
        if kind != "document":
            raise ValueError("pages is supported only for document cases")
        source["pages"] = case["pages"]
    body.update(
        {"images": [{"url": uri}]}
        if kind == "image"
        else {"audio": uri}
        if kind == "audio"
        else {"documents": [source]}
    )
    result = dict(
        checkpoint_revision=case.get("revision"),
        model=case["model"],
        kind=kind,
        fixture_sha256=sha256(fixture),
        trials=[],
    )
    result["weights"] = (
        [
            {"name": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)}
            for path in sorted(Path(case["checkpoint_dir"]).glob("*.safetensors"))
        ]
        if case.get("checkpoint_dir")
        else []
    )
    if case.get("checkpoint_dir") and not result["weights"]:
        raise ValueError("checkpoint_dir contains no safetensors weights")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = os.environ.copy()
    env.update(
        ANTFLY_INFERENCE_REQUIRED_BACKEND="native",
        ANTFLY_INFERENCE_PREFERRED_BACKEND="native",
        ANTFLY_INFERENCE_CPU_THREADS=str(args.threads),
        ANTFLY_INFERENCE_BLAS=args.blas,
        OMP_NUM_THREADS=str(args.threads),
        OPENBLAS_NUM_THREADS=str(args.threads),
    )
    result["acceleration"] = {
        key: env.get(key)
        for key in (
            "ANTFLY_INFERENCE_BLAS",
            "ANTFLY_OPENBLAS_LIBRARY",
            "ANTFLY_INFERENCE_CPU_THREADS",
        )
    }
    result["cpu_affinity"] = (
        sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
    )
    result["host_load_before"] = os.getloadavg()
    library = env.get("ANTFLY_OPENBLAS_LIBRARY")
    if library:
        result["openblas_sha256"] = sha256(library)
    stopped = threading.Event()
    samples = []
    process = None
    thread = None
    started = time.monotonic()
    with (args.report.parent / f"cpu-media-{index}.log").open("w") as log:
        try:
            process = subprocess.Popen(
                [
                    str(args.binary),
                    "run",
                    "--config",
                    str(args.config),
                    "--models-dir",
                    str(args.models_dir),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--max-loaded-models",
                    "1",
                    "--process-memory-budget-mb",
                    str(args.process_memory_mib),
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

            def monitor():
                while not stopped.wait(1):
                    sample = dict(
                        seconds=round(time.monotonic() - started, 3),
                        **process_sample(process.pid),
                    )
                    samples.append(sample)
                    if (
                        args.abort_pss_mib
                        and sample["pss_bytes"] > args.abort_pss_mib * 1024**2
                    ):
                        os.killpg(process.pid, signal.SIGTERM)
                        return

            thread = threading.Thread(target=monitor, daemon=True)
            thread.start()
            ready_deadline = started + args.startup_timeout
            while True:
                if process.poll() is not None:
                    raise RuntimeError("owned server exited before readiness")
                try:
                    status, _ = request(port, "/models/instances", None, 1)
                    if status == 200:
                        break
                except (OSError, ValueError, http.client.HTTPException):
                    pass
                if time.monotonic() >= ready_deadline:
                    raise TimeoutError("server startup deadline exceeded")
                time.sleep(0.1)
            ready = time.monotonic()
            status, load = request(
                port,
                "/models/instances",
                {
                    "model": case["model"],
                    "task": "transcriber" if kind == "audio" else "generator",
                },
                args.timeout,
            )
            result["load_seconds"] = time.monotonic() - ready
            if status != 200:
                raise ValueError(f"model load failed: {status} {load}")
            for trial in range(args.trials):
                tick = time.monotonic()
                status, response = request(port, "/read", body, args.timeout)
                elapsed = time.monotonic() - tick
                row = dict(
                    trial=trial, seconds=elapsed, status=status, response=response
                )
                result["trials"].append(row)
                if status != 200:
                    raise ValueError(f"read failed: {status} {response}")
                validate_read(response, case["expected"])
                if trial == 0:
                    result["startup_to_first_complete_seconds"] = (
                        time.monotonic() - started
                    )
                status, instances = request(port, "/models/instances", None, 5)
                if (
                    status != 200
                    or not instances.get("data")
                    or any(
                        item.get("backend") != "native" for item in instances["data"]
                    )
                ):
                    raise ValueError(
                        "native execution was not confirmed by model instances"
                    )
                row["instances"] = instances["data"]
            if args.cancel_after_ms:
                status, canceled = request(
                    port, "/read", body, args.timeout, budget_ms=args.cancel_after_ms
                )
                result["cancellation"] = dict(status=status, response=canceled)
                validate_timeout(status, canceled)
                status, recovered = request(port, "/read", body, args.timeout)
                if status != 200:
                    raise ValueError("worker did not recover after cancellation")
                validate_read(recovered, case["expected"])
                result["cancellation"]["recovery_complete"] = True
            result["success"] = True
        except Exception as error:
            result.update(success=False, error=str(error))
        finally:
            stopped.set()
            if process is not None:
                # Kill our process group even if its supervisor already exited.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                # A dead supervisor does not prove its worker exited.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                if thread is not None:
                    thread.join(timeout=2)
    result["samples"] = samples
    result["peak_pss_bytes"] = max(
        (sample["pss_bytes"] for sample in samples), default=0
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "config", "models-dir", "cases", "report"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--blas", choices=("auto", "openblas", "off"), default="auto")
    parser.add_argument("--process-memory-mib", type=int, default=32768)
    parser.add_argument("--abort-pss-mib", type=int, default=30720)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--startup-timeout", type=float, default=30)
    parser.add_argument("--cancel-after-ms", type=int, default=0)
    args = parser.parse_args()
    if (
        min(
            args.threads,
            args.process_memory_mib,
            args.trials,
            args.timeout,
            args.startup_timeout,
        )
        <= 0
    ):
        parser.error("budgets, threads, trials and deadlines must be positive")
    if args.abort_pss_mib < 0:
        parser.error("abort-pss-mib must be nonnegative")
    if not 0 <= args.cancel_after_ms <= 3600000:
        parser.error("cancel-after-ms must be in 0..3600000")
    for key in ("binary", "config", "models_dir", "cases", "report"):
        setattr(args, key, getattr(args, key).resolve())
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report = dict(
        binary_sha256=sha256(args.binary),
        config_sha256=sha256(args.config),
        os_page_cache="uncontrolled; checkpoint hashing can warm cache; first request in a fresh owned process",
        cases=[],
    )
    for index, case in enumerate(json.loads(args.cases.read_text())):
        try:
            result = run_case(args, case, index)
        except Exception as error:
            # Missing fixtures/checkpoints must remain explicit failed cases;
            # never leave a partial report looking like the complete campaign.
            result = dict(
                model=case.get("model"),
                kind=case.get("kind"),
                success=False,
                error=str(error),
            )
        report["cases"].append(result)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(
            json.dumps(
                {
                    key: report["cases"][-1].get(key)
                    for key in (
                        "model",
                        "kind",
                        "success",
                        "error",
                        "load_seconds",
                        "startup_to_first_complete_seconds",
                        "peak_pss_bytes",
                    )
                }
            ),
            flush=True,
        )
    return (
        0 if report["cases"] and all(case["success"] for case in report["cases"]) else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
