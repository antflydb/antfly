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

"""Alternate native CUDA policies; measure full forwards, vectors and memory."""

import argparse
import hashlib
import json
import os
import signal
import statistics
import subprocess
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[6]
sys.path.insert(0, str(ROOT / "zig/lib/pjrt/tools"))
from memory import MemorySampler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--suite", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=8)
    parser.add_argument(
        "--tuned-gemm",
        action="store_true",
        help="Allow process-specific BF16 projection autotuning; disabled for fixed-arithmetic comparisons",
    )
    args = parser.parse_args()
    if not 1 <= args.rounds <= 10 or not 1 <= args.iterations <= 100:
        parser.error("Use 1..10 rounds and 1..100 iterations")
    args.out.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    env = dict(os.environ)
    prefix = "ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_"
    # Hold all other experimental routes constant in the two arms.
    for name in list(env):
        if name.startswith(prefix):
            del env[name]
    for name in (
        "EXTENDED_FLASH",
        "PACKED_PROJECTIONS",
        "GRAPH_REPLAY",
        "FUSED_BACKBONE",
    ):
        env[prefix + name] = "1"
    env[prefix + "TUNED_GEMM"] = str(int(args.tuned_gemm))
    runs = []
    for round_index in range(args.rounds):
        for candidate in [False, True] if round_index % 2 == 0 else [True, False]:
            name = f"r{round_index}-{'candidate' if candidate else 'control'}"
            path = args.out / f"{name}.json"
            env[prefix + "GLOBAL_GEMM"] = str(int(candidate))
            command = [
                str(binary),
                "--model",
                str(args.model.resolve()),
                "--suite",
                str(args.suite.resolve()),
                "--backend",
                "cuda",
                "--warmups",
                "4",
                "--iterations",
                str(args.iterations),
                "--long-iterations",
                str(args.iterations),
                "--output",
                str(path.resolve()),
            ]
            active = subprocess.check_output(
                ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"],
                text=True,
            ).strip()
            if active:
                raise RuntimeError(f"GPU already has compute processes: {active}")
            sampler = None
            with (args.out / f"{name}.log").open("w") as log:
                process = subprocess.Popen(
                    command,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                try:
                    sampler = MemorySampler(process, args.out / f"{name}.memory.jsonl")
                    sampler.thread.start()
                    code = process.wait(timeout=600)
                    if code:
                        raise RuntimeError(f"{name} exited {code}; see its log")
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
                        (args.out / f"{name}.memory.json").write_text(
                            json.dumps(memory, indent=2)
                        )
            if not memory["valid"]:
                raise RuntimeError(f"Invalid memory sample: {memory}")
            report = json.loads(path.read_text())
            runs.append(
                {
                    "candidate": candidate,
                    "round": round_index,
                    "report": report,
                    "memory": memory,
                }
            )
            print(
                name,
                {
                    case["name"]: round(
                        statistics.median(case["samples_seconds"]) * 1000, 3
                    )
                    for case in report["cases"]
                },
                flush=True,
            )
    replay_checks = []
    for run in runs:
        cohort = {case["name"]: case for case in run["report"]["cases"]}
        if "replay_0_2048" in cohort and "replay_2_2048" in cohort:
            before = cohort["replay_0_2048"]["vector"]
            after = cohort["replay_2_2048"]["vector"]
            if before != after:
                raise RuntimeError("Vector changed after shared workspace growth")
            replay_checks.append(
                {
                    "candidate": run["candidate"],
                    "round": run["round"],
                    "bitwise_equal": True,
                }
            )
    cases = []
    for index, case in enumerate(runs[0]["report"]["cases"]):
        arms = {False: [], True: []}
        for run in runs:
            arms[run["candidate"]].append(run["report"]["cases"][index])
        baseline = np.asarray(arms[False][0]["vector"], dtype=np.float64)
        quality = []
        for run in arms[True]:
            vector = np.asarray(run["vector"], dtype=np.float64)
            cosine = float(
                np.dot(baseline, vector)
                / (np.linalg.norm(baseline) * np.linalg.norm(vector))
            )
            max_abs = float(np.max(np.abs(baseline - vector)))
            if (
                vector.shape != (768,)
                or not np.isfinite(vector).all()
                or not np.isfinite(cosine)
                or abs(float(np.linalg.norm(vector)) - 1) > 0.001
                or cosine < 0.9999
                or max_abs > 0.003
            ):
                raise RuntimeError(
                    f"Vector mismatch {case['name']}: cosine={cosine}, max_abs={max_abs}"
                )
            quality.append({"cosine": cosine, "max_abs": max_abs})
        ms = {
            arm: statistics.median(
                [value * 1000 for c in values for value in c["samples_seconds"]]
            )
            for arm, values in arms.items()
        }
        cases.append(
            {
                "name": case["name"],
                "tokens": case["tokens"],
                "control_ms": ms[False],
                "candidate_ms": ms[True],
                "speedup": ms[False] / ms[True],
                "quality": quality,
            }
        )
    summary = {
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "suite_sha256": hashlib.sha256(args.suite.read_bytes()).hexdigest(),
        "rounds": args.rounds,
        "warmups": 4,
        "iterations": args.iterations,
        "scope": runs[0]["report"]["timing_scope"],
        "policy": {
            k: v
            for k, v in env.items()
            if k.startswith(prefix) and not k.endswith("GLOBAL_GEMM")
        },
        "cases": cases,
        "replay_after_growth": replay_checks,
        "memory": {
            str(arm): [run["memory"] for run in runs if run["candidate"] == arm]
            for arm in (False, True)
        },
    }
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
