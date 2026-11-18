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

"""Compare unflagged L4 dispatch, the explicit profile and the legacy override."""

import argparse
import hashlib
import json
import os
import re
import signal
import statistics
import subprocess
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[6]
sys.path.insert(0, str(ROOT / "zig/lib/pjrt/tools"))
from media_campaign import metrics
from memory import MemorySampler

PREFIX = "ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_"
FLAGS = (
    "EXTENDED_FLASH",
    "PACKED_PROJECTIONS",
    "GRAPH_REPLAY",
    "FUSED_BACKBONE",
    "GLOBAL_GEMM",
)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for key in ("binary", "model", "suite", "out"):
        p.add_argument("--" + key, type=Path, required=True)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--iterations", type=int, default=8)
    a = p.parse_args()
    if not 1 <= a.rounds <= 10 or not 1 <= a.iterations <= 100:
        p.error("Use 1..10 rounds and 1..100 iterations")
    a.out.mkdir(parents=True, exist_ok=False)
    suite = json.loads(a.suite.read_text())
    inputs = [a.binary, a.suite, a.model / "model.safetensors"] + sorted(
        a.model.glob("*.json")
    )
    inputs += [
        a.suite.parent / part["path"]
        for case in suite["cases"]
        for part in case["parts"]
        if "path" in part
    ]
    identities = {str(path.resolve()): digest(path) for path in inputs}
    identities[str(Path(__file__).resolve())] = digest(Path(__file__))
    (a.out / "provenance.json").write_text(
        json.dumps(
            {
                "files": identities,
                "minimum_cosine": 0.9999,
                "maximum_absolute_error": 0.003,
                "maximum_latency_ratio_to_legacy": 1.05,
                "reference": "explicit fixed-arithmetic profile",
                "gpu": subprocess.check_output(
                    [
                        "nvidia-smi",
                        "--query-gpu=name,driver_version,memory.total",
                        "--format=csv,noheader",
                    ],
                    text=True,
                ).strip(),
            },
            indent=2,
        )
    )
    runs = []
    for round_index in range(a.rounds):
        order = (
            ("legacy", "automatic", "explicit"),
            ("explicit", "legacy", "automatic"),
            ("automatic", "explicit", "legacy"),
        )[round_index % 3]
        for role in order:
            active = subprocess.check_output(
                ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"],
                text=True,
            ).strip()
            if active:
                raise RuntimeError("GPU is occupied: " + active)
            env = {k: v for k, v in os.environ.items() if not k.startswith(PREFIX)}
            if role == "legacy":
                env[PREFIX + "AUTOMATIC"] = "0"
            if role == "explicit":
                env[PREFIX + "AUTOMATIC"] = "0"
                env.update({PREFIX + flag: "1" for flag in FLAGS})
            env[PREFIX + "STATS"] = "1"
            tag = f"r{round_index}-{role}"
            target = a.out / f"{tag}.json"
            command = [
                str(a.binary.resolve()),
                "--model",
                str(a.model.resolve()),
                "--suite",
                str(a.suite.resolve()),
                "--output",
                str(target.resolve()),
                "--warmups",
                "3",
                "--iterations",
                str(a.iterations),
            ]
            with (a.out / f"{tag}.log").open("w") as log:
                process = subprocess.Popen(
                    command,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                sampler = None
                try:
                    sampler = MemorySampler(process, a.out / f"{tag}.memory.jsonl")
                    sampler.thread.start()
                    code = process.wait(timeout=900)
                    if code:
                        raise RuntimeError(f"{tag} exited {code}")
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                    if sampler is not None:
                        memory = sampler.stop()
                        (a.out / f"{tag}.memory.json").write_text(
                            json.dumps(memory, indent=2)
                        )
            report = json.loads(target.read_text())
            global_calls = [
                int(n)
                for n in re.findall(
                    r"global_gemm_attention_calls_total=(\d+)",
                    (a.out / f"{tag}.log").read_text(),
                )
            ]
            expect_global = role != "legacy" and any(
                512 <= len(c["token_ids"]) <= 8192 for c in suite["cases"]
            )
            if not global_calls or (max(global_calls) > 0) != expect_global:
                raise RuntimeError("Unexpected global-attention dispatch counters")
            policy = report["execution_policy"]
            if not memory["valid"]:
                raise RuntimeError("Invalid memory sampling")
            if role == "automatic":
                if not policy["automatic_bits"] or any(
                    policy[flag.lower()] for flag in FLAGS
                ):
                    raise RuntimeError("Automatic arm contains explicit route settings")
            elif policy["automatic_bits"] or any(
                policy[flag.lower()] != (role == "explicit") for flag in FLAGS
            ):
                raise RuntimeError("Unexpected explicit/legacy recipe")
            if len(report["cases"]) != len(suite["cases"]):
                raise RuntimeError("Incomplete run")
            for result, fixture in zip(report["cases"], suite["cases"], strict=True):
                if result["name"] != fixture["name"] or any(
                    len(result[k]) != a.iterations
                    for k in ("vectors", "samples_seconds", "input_tokens")
                ):
                    raise RuntimeError("Unexpected case/sample geometry")
                if any(n != len(fixture["token_ids"]) for n in result["input_tokens"]):
                    raise RuntimeError("Rendered token counts differ")
                times = np.asarray(result["samples_seconds"], dtype=np.float64)
                if not np.isfinite(times).all() or (times <= 0).any():
                    raise RuntimeError("Invalid latency samples")
                for vector in result["vectors"]:
                    metrics(vector, vector)
            runs.append(
                {"round": round_index, "role": role, "report": report, "memory": memory}
            )
            print(
                tag,
                {
                    c["name"]: round(statistics.median(c["samples_seconds"]) * 1000, 2)
                    for c in report["cases"]
                },
                flush=True,
            )
    passed = True
    cases = []
    for fixture in suite["cases"]:
        groups = {
            role: [
                next(c for c in run["report"]["cases"] if c["name"] == fixture["name"])
                for run in runs
                if run["role"] == role
            ]
            for role in ("legacy", "automatic", "explicit")
        }
        reference = groups["explicit"][0]["vectors"][0]
        comparisons = [
            metrics(vector, reference)
            for case in groups["automatic"]
            for vector in case["vectors"]
        ]
        accepted = all(
            c["cosine"] >= 0.9999 and c["max_abs"] <= 0.003 for c in comparisons
        )
        passed &= accepted
        timings = {
            role: statistics.median(
                t * 1000 for case in cases for t in case["samples_seconds"]
            )
            for role, cases in groups.items()
        }
        latency_passed = timings["automatic"] <= timings["legacy"] * 1.05
        passed &= latency_passed
        cases.append(
            {
                "name": fixture["name"],
                "tokens": len(fixture["token_ids"]),
                "ms": timings,
                "legacy_speedup": timings["legacy"] / timings["automatic"],
                "automatic_explicit_min_cosine": min(c["cosine"] for c in comparisons),
                "automatic_explicit_max_abs": max(c["max_abs"] for c in comparisons),
                "automatic_explicit_bitwise_equal": all(
                    np.array_equal(vector, reference)
                    for c in groups["automatic"]
                    for vector in c["vectors"]
                ),
                "passed": accepted and latency_passed,
                "parity_passed": accepted,
                "latency_passed": latency_passed,
            }
        )
    if any(digest(Path(path)) != sha for path, sha in identities.items()):
        raise RuntimeError("Campaign artifacts changed")
    summary = {
        "passed": passed,
        "rounds": a.rounds,
        "iterations": a.iterations,
        "warmups": 3,
        "scope": runs[0]["report"]["scope"],
        "identities": identities,
        "cases": cases,
        "memory": {
            role: [r["memory"] for r in runs if r["role"] == role]
            for role in ("legacy", "automatic", "explicit")
        },
    }
    (a.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if not passed:
        raise RuntimeError("Automatic dispatch qualification failed; see summary.json")


if __name__ == "__main__":
    main()
