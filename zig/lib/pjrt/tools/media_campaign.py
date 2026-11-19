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

"""Rotate full native media pipelines and native PJRT media with frozen gates."""

import argparse
import hashlib
import importlib.metadata
import json
import os
import signal
import statistics
import subprocess
import sys
from pathlib import Path

import numpy as np
import transformers
from media_validation import validate_oracle
from memory import MemorySampler


def digest(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def metrics(actual, expected):
    a = np.asarray(actual, np.float64)
    b = np.asarray(expected, np.float64)
    if (
        a.shape != (768,)
        or b.shape != a.shape
        or not np.isfinite(a).all()
        or not np.isfinite(b).all()
        or abs(np.linalg.norm(a) - 1) > 0.001
        or abs(np.linalg.norm(b) - 1) > 0.001
    ):
        raise ValueError("Invalid embedding")
    return {
        "cosine": float(a @ b / (np.linalg.norm(a) * np.linalg.norm(b))),
        "max_abs": float(np.max(np.abs(a - b))),
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for arg in (
        "binary",
        "runner",
        "plugin",
        "model",
        "manifest",
        "suite",
        "oracle",
        "acceptance",
        "out",
    ):
        p.add_argument("--" + arg, type=Path, required=True)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--iterations", type=int, default=8)
    p.add_argument("--warmups", type=int, default=3)
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=False)
    suite = json.loads(a.suite.read_text())
    manifest = json.loads(a.manifest.read_text())
    gates = json.loads(a.acceptance.read_text())
    if (a.rounds, a.warmups, a.iterations) != (
        gates["timing"]["rounds"],
        gates["timing"]["warmups"],
        gates["timing"]["samples_per_case_per_arm_per_round"],
    ):
        raise ValueError("Campaign dimensions differ from frozen acceptance criteria")
    oracle = json.loads(a.oracle.read_text())
    expected = validate_oracle(
        oracle,
        [c["name"] for c in suite["cases"]],
        gates["static_wrapper_vs_unmodified_upstream_cpu"]["max_abs"],
    )
    paths = (
        [
            a.binary,
            a.runner,
            a.plugin,
            a.manifest,
            a.suite,
            a.oracle,
            a.acceptance,
            a.model / "model.safetensors",
        ]
        + [a.manifest.parent / c["program"] for c in manifest["cases"]]
        + [a.suite.parent / f for f in suite["files"]]
        + sorted(a.model.glob("*.json"))
        + sorted(Path(__file__).parent.glob("*media*.py"))
        + [Path(__file__).with_name("memory.py")]
        + [
            Path(transformers.__file__).parent / name
            for name in (
                "models/embedding_gemma2/modeling_embedding_gemma2.py",
                "models/embedding_gemma2/processing_embedding_gemma2.py",
                "models/gemma4/modeling_gemma4.py",
                "models/gemma4/image_processing_gemma4.py",
                "models/gemma4/feature_extraction_gemma4.py",
            )
        ]
    )
    identities = {str(f.resolve()): digest(f) for f in paths}
    executable_cache = a.out / "pjrt-executables"
    executable_cache.mkdir()
    (a.out / "provenance.json").write_text(
        json.dumps(
            {
                "files": identities,
                "acceptance": gates,
                "versions": {
                    name: importlib.metadata.version(name)
                    for name in ("torch", "transformers", "torchax", "jax", "jaxlib")
                },
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
    results = []
    cached_executables = None
    for round_index in range(a.rounds):
        order = (
            ("control", "candidate", "pjrt"),
            ("pjrt", "control", "candidate"),
            ("candidate", "pjrt", "control"),
        )[round_index % 3]
        for role in order:
            tag = f"r{round_index}-{role}"
            directory = a.out / tag
            active = subprocess.check_output(
                ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"],
                text=True,
            ).strip()
            if active:
                raise RuntimeError("GPU has another compute process: " + active)
            env = dict(os.environ)
            prefix = "ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_"
            for key in list(env):
                if key.startswith(prefix):
                    del env[key]
            for flag in (
                "EXTENDED_FLASH",
                "PACKED_PROJECTIONS",
                "GRAPH_REPLAY",
                "FUSED_BACKBONE",
            ):
                env[prefix + flag] = "1"
            env[prefix + "TUNED_GEMM"] = "0"
            env[prefix + "GLOBAL_GEMM"] = str(int(role == "candidate"))
            if role == "pjrt":
                if cached_executables is not None and cached_executables != {
                    f.name: digest(f) for f in sorted(executable_cache.iterdir())
                }:
                    raise RuntimeError("PJRT executable cache changed between rounds")
                env.pop("XLA_FLAGS", None)
                command = [
                    sys.executable,
                    str(Path(__file__).with_name("benchmark_media_pjrt.py")),
                    "--external-memory-sampler",
                    "--executable-cache",
                    str(executable_cache),
                    "--runner",
                    str(a.runner),
                    "--plugin",
                    str(a.plugin),
                    "--model",
                    str(a.model),
                    "--manifest",
                    str(a.manifest),
                    "--suite",
                    str(a.suite),
                    "--out",
                    str(directory),
                    "--warmups",
                    str(a.warmups),
                    "--iterations",
                    str(a.iterations),
                ]
            else:
                directory.mkdir()
                command = [
                    str(a.binary.resolve()),
                    "--model",
                    str(a.model.resolve()),
                    "--suite",
                    str(a.suite.resolve()),
                    "--output",
                    str((directory / "results.json").resolve()),
                    "--warmups",
                    str(a.warmups),
                    "--iterations",
                    str(a.iterations),
                ]
            sampler = None
            process = None
            with (a.out / f"{tag}.log").open("w") as log:
                try:
                    process = subprocess.Popen(
                        command,
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                    sampler = MemorySampler(process, a.out / f"{tag}.memory.jsonl")
                    sampler.thread.start()
                    code = process.wait(timeout=1800)
                    if code:
                        raise RuntimeError(f"{tag} exited {code}; see its log")
                finally:
                    if process is not None and process.poll() is None:
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                    if sampler is not None:
                        (directory / "memory.json").write_text(
                            json.dumps(sampler.stop(), indent=2)
                        )
            report = json.loads((directory / "results.json").read_text())
            if role == "pjrt":
                current_cache = {
                    f.name: digest(f) for f in sorted(executable_cache.iterdir())
                }
                if len(current_cache) != len(manifest["cases"]):
                    raise RuntimeError("Incomplete PJRT executable cache")
                if (
                    cached_executables is not None
                    and cached_executables != current_cache
                ):
                    raise RuntimeError("PJRT executable cache changed during a round")
                cached_executables = current_cache
            memory = json.loads((directory / "memory.json").read_text())
            if not memory["valid"]:
                raise RuntimeError("Invalid memory sampling")
            if {c["name"] for c in report["cases"]} != {
                c["name"] for c in suite["cases"]
            } or len(report["cases"]) != len(suite["cases"]):
                raise ValueError("Incomplete media run")
            if any(
                len(c[key]) != a.iterations
                for c in report["cases"]
                for key in ("vectors", "input_tokens", "samples_seconds")
            ):
                raise ValueError("Incomplete measured samples")
            if len(report["cases"]) != len(suite["cases"]) or any(
                len(c[key]) != a.iterations
                for c in report["cases"]
                for key in ("samples_seconds", "vectors", "input_tokens")
            ):
                raise ValueError("Incorrect measured sample count")
            results.append(
                {"role": role, "round": round_index, "report": report, "memory": memory}
            )
            print(
                tag,
                {
                    c["name"]: round(statistics.median(c["samples_seconds"]) * 1000, 2)
                    for c in report["cases"]
                },
                flush=True,
            )
    cases = []
    passed = True
    for fixture in suite["cases"]:
        name = fixture["name"]
        arms = {r: [] for r in ("control", "candidate", "pjrt")}
        for result in results:
            arms[result["role"]].append(
                next(c for c in result["report"]["cases"] if c["name"] == name)
            )
        control = arms["control"][0]["vectors"][0]
        parity = {}
        timings = {}
        case_passed = True
        breakdown = {}
        for role, values in arms.items():
            timings[role] = statistics.median(
                t * 1000 for c in values for t in c["samples_seconds"]
            )
            comparisons = [
                metrics(v, expected[name]) for c in values for v in c["vectors"]
            ]
            parity[role] = {
                "min_oracle_cosine": min(c["cosine"] for c in comparisons),
                "max_oracle_abs": max(c["max_abs"] for c in comparisons),
            }
            for key in (
                "preprocess_seconds",
                "native_receive_upload_seconds",
                "native_execute_readback_seconds",
            ):
                if all(key in c for c in values):
                    breakdown.setdefault(role, {})[key.replace("_seconds", "_ms")] = (
                        statistics.median(t * 1000 for c in values for t in c[key])
                    )
            oracle_passed = True
            for comparison in comparisons:
                gate = gates["native_vs_independent_bf16_reference"]
                oracle_passed &= (
                    comparison["cosine"] >= gate["min_cosine"]
                    and comparison["max_abs"] <= gate["max_abs"]
                )
            parity[role]["bf16_oracle_passed"] = bool(oracle_passed)
            case_passed &= oracle_passed
        deltas = [metrics(v, control) for c in arms["candidate"] for v in c["vectors"]]
        gate = gates["native_candidate_vs_control"]
        candidate_control_passed = all(
            c["cosine"] >= gate["min_cosine"] and c["max_abs"] <= gate["max_abs"]
            for c in deltas
        )
        case_passed &= candidate_control_passed
        passed &= case_passed
        for role, values in arms.items():
            if any(
                any(t != len(fixture["token_ids"]) for t in c["input_tokens"])
                for c in values
            ):
                raise ValueError("Expanded token counts differ")
        cases.append(
            {
                "name": name,
                "tokens": len(fixture["token_ids"]),
                "ms": timings,
                "native_speedup": timings["control"] / timings["candidate"],
                "native_speedup_vs_pjrt": timings["pjrt"] / timings["candidate"],
                "parity": parity,
                "passed": bool(case_passed),
                "candidate_control_passed": candidate_control_passed,
                "pjrt_host_and_execution_breakdown_ms": breakdown.get("pjrt", {}),
                "candidate_control_min_cosine": min(c["cosine"] for c in deltas),
                "candidate_control_max_abs": max(c["max_abs"] for c in deltas),
            }
        )
    if any(digest(Path(f)) != h for f, h in identities.items()):
        raise RuntimeError("Inputs/programs/binaries changed during campaign")
    summary = {
        "passed": bool(passed),
        "cases": cases,
        "rounds": a.rounds,
        "iterations": a.iterations,
        "warmups": a.warmups,
        "acceptance": gates,
        "pjrt_executable_cache": {
            "scope": "Created fresh for this campaign on this device/plugin; first reference round compiles and serializes, later fresh processes deserialize. Startup memory includes the work performed in each round; warm timing excludes both compilation and deserialization.",
            "files": {f.name: digest(f) for f in sorted(executable_cache.iterdir())},
        },
        "memory": {
            role: [r["memory"] for r in results if r["role"] == role] for role in arms
        },
        "scopes": {
            role: next(r["report"]["scope"] for r in results if r["role"] == role)
            for role in arms
        },
    }
    (a.out / "summary.json").write_text(json.dumps(summary, indent=2))
    if not passed:
        raise RuntimeError(
            "End-to-end media qualification gates failed; see summary.json"
        )


if __name__ == "__main__":
    main()
