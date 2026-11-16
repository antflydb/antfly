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

"""Alternate device/host PLE placement in one server binary on identical inputs.

Requires psutil and nvidia-ml-py, like benchmark_cuda_http_pair.py. Refuses GPU
cotenants by default. Unified-memory NVML/PSS values are retained separately;
only the CUDA allocation ledger is used for the device-allocation comparison.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import socket
import statistics
import subprocess
import time

from benchmark_cuda_http_pair import (
    MemorySampler,
    body_for,
    host_snapshot,
    request,
    sha256,
)


def active_compute():
    return (
        subprocess.check_output(
            [
                "nvidia-smi",
                "--query-compute-apps=pid",
                "--format=csv,noheader",
            ],
            text=True,
        )
        .strip()
        .splitlines()
    )


def run(args, cells, placement, lifecycle):
    active = active_compute()
    if active and not args.allow_cotenant:
        raise RuntimeError(f"GPU compute processes already active: {active}")
    tag = f"lc{lifecycle}-{placement}"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = args.output / f"{tag}.config.json"
    config.write_text(json.dumps({"inference": {"keep_alive": "0"}}))
    command = [str(args.server.resolve())]
    if args.server.name == "antfly":
        command.append("inference")
    command += [
        "run",
        "--models-dir",
        str(args.model.resolve().parent.parent.parent),
        "--config",
        str(config.resolve()),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--host-budget-mb",
        str(args.host_budget_mb),
        "--scratch-budget-mb",
        "4096",
    ]
    env = os.environ.copy()
    env.update(dict(v.split("=", 1) for v in args.env))
    env.update(
        ANTFLY_INFERENCE_REQUIRED_BACKEND="cuda",
        ANTFLY_INFERENCE_WORKER=str(args.server.resolve()),
        ANTFLY_INFERENCE_CUDA_GEMMA4_PLE_HOST="1" if placement == "host" else "0",
        ANTFLY_INFERENCE_CUDA_GEMMA4_MEMORY_STATS="1",
    )
    meta = {
        "command": command,
        "placement": placement,
        "lifecycle": lifecycle,
        "cotenants_before": active,
        "host_before": host_snapshot(),
    }
    log_path = args.output / f"{tag}.log"
    rows = []
    with log_path.open("w") as log:
        process = subprocess.Popen(
            command,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        sampler = None
        try:
            sampler = MemorySampler(process, args.output / f"{tag}.memory.jsonl")
            sampler.thread.start()
            deadline = time.monotonic() + 600
            while True:
                if process.poll() is not None:
                    raise RuntimeError(
                        f"Server exited {process.returncode}: {log_path}"
                    )
                try:
                    request(port, "/ai/v1/models")
                    break
                except (OSError, RuntimeError):
                    if time.monotonic() > deadline:
                        raise TimeoutError("server startup")
                    time.sleep(0.25)
            for cell in cells:
                body = body_for(args, "antfly", cell, cell["calls"][0])
                request(port, "/ai/v1/generate", body)
                for index, call in enumerate(cell["calls"]):
                    result = request(
                        port, "/ai/v1/generate", body_for(args, "antfly", cell, call)
                    )
                    row = dict(
                        placement=placement,
                        lifecycle=lifecycle,
                        cell=cell["id"],
                        index=index,
                        **result,
                    )
                    rows.append(row)
                    with (args.output / "calls.jsonl").open("a") as out:
                        out.write(json.dumps(row) + "\n")
                    print(
                        f"{tag} {cell['id']}[{index}] {result['ms']:.2f} ms", flush=True
                    )
            # Emit the final measured request's allocation counters at the next
            # request boundary, before request-local arenas are reset.
            request(
                port,
                "/ai/v1/generate",
                body_for(args, "antfly", cells[0], cells[0]["calls"][0]),
            )
        finally:
            import psutil

            try:
                children = psutil.Process(process.pid).children(recursive=True)
            except psutil.NoSuchProcess:
                children = []
            if sampler is not None:
                meta["memory"] = sampler.stop()
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            _, alive = psutil.wait_procs(children, timeout=15)
            for child in alive:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            psutil.wait_procs(alive, timeout=5)
            meta["host_after"] = host_snapshot()
            meta["cotenants_after"] = active_compute()
            observations = re.findall(
                r"cuda_gemma4_memory ((?:[a-z_]+=[0-9]+ ?)+)", log_path.read_text()
            )
            meta["cuda_allocations"] = [
                dict((k, int(v)) for k, v in re.findall(r"([a-z_]+)=([0-9]+)", line))
                for line in observations
            ]
            (args.output / f"{tag}.json").write_text(json.dumps(meta, indent=2) + "\n")
    if not meta.get("memory", {}).get("valid"):
        raise RuntimeError(
            "memory sampler failed; retain diagnostics but do not report a memory result"
        )
    if not meta["cuda_allocations"]:
        raise RuntimeError("missing CUDA allocation ledger observations")
    expected_host = placement == "host"
    if not any((o["host_ple"] > 0) == expected_host for o in meta["cuda_allocations"]):
        raise RuntimeError("requested PLE placement was not observed")
    return rows, meta


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("server", "model", "fixtures", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--lifecycles", type=int, default=3)
    parser.add_argument("--host-budget-mb", type=int, default=12288)
    parser.add_argument(
        "--calls-per-cell",
        type=int,
        help="Diagnostic subset, selected evenly from each fixture cell",
    )
    parser.add_argument("--env", action="append", default=[])
    parser.add_argument(
        "--allow-cotenant",
        action="store_true",
        help="Diagnostic only; prevents an uncontended performance claim",
    )
    args = parser.parse_args()
    if (
        args.lifecycles < 1
        or args.host_budget_mb < 1
        or (args.calls_per_cell is not None and args.calls_per_cell < 1)
    ):
        parser.error("lifecycle, host budget and optional call limit must be positive")
    if active_compute() and not args.allow_cotenant:
        parser.error(
            "GPU has compute cotenants; wait for idle or explicitly request a diagnostic run"
        )
    args.output.mkdir(parents=True, exist_ok=False)
    cells = json.loads(args.fixtures.read_text())
    selected_indices = {}
    for cell in cells:
        count = min(len(cell["calls"]), args.calls_per_cell or len(cell["calls"]))
        indices = (
            [0]
            if count == 1
            else [
                round(i * (len(cell["calls"]) - 1) / (count - 1)) for i in range(count)
            ]
        )
        selected_indices[cell["id"]] = indices
        cell["calls"] = [cell["calls"][i] for i in indices]
    provenance = {
        "arguments": {
            k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()
        },
        "server_sha256": sha256(args.server),
        "model_sha256": sha256(args.model),
        "fixtures_sha256": sha256(args.fixtures),
        "harness_sha256": sha256(Path(__file__)),
        "inherited_cuda_env": {
            k: v
            for k, v in os.environ.items()
            if k.startswith(("ANTFLY_INFERENCE_CUDA_", "ANTFLY_CUDA_"))
        },
    }
    media = {
        call[kind]
        for cell in cells
        for call in cell["calls"]
        for kind in ("image", "audio")
        if kind in call
    }
    provenance["selected_call_indices"] = selected_indices
    provenance["gpu"] = subprocess.check_output(
        [
            "nvidia-smi",
            "--query-gpu=name,driver_version,memory.total,compute_cap",
            "--format=csv,noheader",
        ],
        text=True,
    ).strip()
    provenance["media_sha256"] = {
        name: sha256(args.fixtures.parent / name) for name in sorted(media)
    }
    provenance["mmproj_sha256"] = sha256(
        args.model.parent / "gemma-4-E4B-it-mmproj.gguf"
    )
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    rows, metadata = [], []
    for lifecycle in range(args.lifecycles):
        for placement in (
            ("device", "host") if lifecycle % 2 == 0 else ("host", "device")
        ):
            arm_rows, meta = run(args, cells, placement, lifecycle)
            rows.extend(arm_rows)
            metadata.append(meta)
    by_input = {}
    for row in rows:
        key = row["cell"], row["index"]
        contract = (
            row["text"],
            row["reasoning"],
            row["usage"]["prompt_tokens"],
            row["usage"]["completion_tokens"],
        )
        if key in by_input and by_input[key] != contract:
            raise RuntimeError(f"placement/repetition output parity failed: {key}")
        by_input[key] = contract
    report = {
        "exact_output_parity": True,
        "uncontended": not any(
            m["cotenants_before"] or m["cotenants_after"] for m in metadata
        ),
        "calls_per_placement": len(rows) // 2,
        "cells": [],
        "cuda_allocation_maxima": {},
    }
    for cell in cells:
        values = {
            p: [r for r in rows if r["placement"] == p and r["cell"] == cell["id"]]
            for p in ("device", "host")
        }
        data = {"cell": cell["id"]}
        for placement, group in values.items():
            data[placement] = {
                k: statistics.median(r[k] for r in group)
                for k in ("ms", "ttft_ms", "decode_tok_s")
            }
            data[placement]["lifecycle_median_ms"] = [
                statistics.median(r["ms"] for r in group if r["lifecycle"] == lc)
                for lc in range(args.lifecycles)
            ]
        data["host_over_device_latency_ratio"] = (
            data["host"]["ms"] / data["device"]["ms"]
        )
        report["cells"].append(data)
    for placement in ("device", "host"):
        observations = [
            o
            for m in metadata
            if m["placement"] == placement
            for o in m["cuda_allocations"]
        ]
        report["cuda_allocation_maxima"][placement] = {
            k: max(o[k] for o in observations) for k in observations[0]
        }
    report["managed_device_peak_saved_bytes"] = (
        report["cuda_allocation_maxima"]["device"]["device_peak"]
        - report["cuda_allocation_maxima"]["host"]["device_peak"]
    )
    (args.output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
