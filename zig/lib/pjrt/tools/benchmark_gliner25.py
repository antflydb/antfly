#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Alternate native CUDA/PJRT GLiNER2.5 prepared-tensor forwards on an idle GPU."""

import argparse
import hashlib
import json
import os
import select
import signal
import statistics
import struct
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
from memory import MemorySampler
from safetensors import safe_open


def digest(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def read_exact(stream, size, timeout=600):
    result = bytearray()
    deadline = time.monotonic() + timeout
    while len(result) < size:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([stream], [], [], remaining)[0]:
            raise TimeoutError("Native PJRT worker response timed out")
        chunk = os.read(stream.fileno(), size - len(result))
        if not chunk:
            raise RuntimeError(
                "Native PJRT worker closed its output; inspect worker log"
            )
        result.extend(chunk)
    return bytes(result)


def terminate(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def stream_parameters(stream, manifest, model):
    import ml_dtypes

    kinds = {"float32": np.float32, "bfloat16": ml_dtypes.bfloat16, "int64": np.int64}
    with safe_open(model / "model.safetensors", framework="numpy") as weights:
        for spec in manifest["parameters"]:
            values = (
                weights.get_tensor(spec["source"])
                if "source" in spec
                else np.asarray(spec["data"])
            )
            values = np.asarray(values, dtype=kinds[spec["dtype"]]).reshape(
                spec["shape"]
            )
            payload = memoryview(np.ascontiguousarray(values).view(np.uint8)).cast("B")
            while payload:
                sent = stream.write(payload[: 4 * 1024 * 1024])
                if not sent:
                    raise RuntimeError("Worker closed its parameter stream")
                payload = payload[sent:]
    stream.flush()


def pjrt_run(a, manifest, output, log):
    with log.open("wb") as stderr:
        process = subprocess.Popen(
            [
                str(a.runner.resolve()),
                str(a.plugin.resolve()),
                str(a.manifest.resolve()),
                str(a.cache.resolve()),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            start_new_session=True,
            bufsize=0,
        )
        # Include the common Python harness and parameter feeder for both arms.
        sampler = MemorySampler(
            SimpleNamespace(pid=os.getpid()), output.with_suffix(".memory.jsonl")
        )
        sampler.thread.start()
        try:
            stream_parameters(process.stdin, manifest, a.model)
            if read_exact(process.stdout, 6, timeout=1800) != b"READY\n":
                raise RuntimeError("Unexpected worker startup packet")
            cases = []
            for index, case in enumerate(manifest["cases"]):
                samples, uploads, logits = [], [], []
                count = int(np.prod(case["outputs"][0]["shape"]))
                for rep in range(3 + a.iterations):
                    process.stdin.write(struct.pack("<I", index))
                    receive_ns, execute_ns = struct.unpack(
                        "<QQ", read_exact(process.stdout, 16)
                    )
                    values = np.frombuffer(
                        read_exact(process.stdout, count * 4), dtype="<f4"
                    ).tolist()
                    if rep >= 3:
                        samples.append((receive_ns + execute_ns) / 1e9)
                        uploads.append(receive_ns / 1e9)
                        logits.append(values)
                cases.append(
                    {
                        "id": case["id"],
                        "samples_seconds": samples,
                        "input_upload_seconds": uploads,
                        "logits": logits,
                        "physical_forwards": 1,
                    }
                )
            process.stdin.write(struct.pack("<I", 0xFFFFFFFF))
            process.stdin.close()
            if process.wait(timeout=30):
                raise RuntimeError("Native PJRT worker failed")
            report = {
                "scope": manifest["scope"],
                "precision": manifest["precision"],
                "cases": cases,
            }
            output.write_text(json.dumps(report, allow_nan=False) + "\n")
            return report
        finally:
            terminate(process)
            memory = sampler.stop()
            output.with_suffix(".memory.json").write_text(
                json.dumps(memory, indent=2) + "\n"
            )


def native_run(a, output, log, binary=None, precision=None):
    with log.open("wb") as stderr:
        process = subprocess.Popen(
            [
                str((binary or a.native).resolve()),
                str(a.model.resolve()),
                str(a.manifest.resolve()),
                str(output.resolve()),
                "3",
                str(a.iterations),
                precision or a.native_precision,
            ],
            stdout=stderr,
            stderr=stderr,
            start_new_session=True,
        )
        sampler = MemorySampler(
            SimpleNamespace(pid=os.getpid()), output.with_suffix(".memory.jsonl")
        )
        sampler.thread.start()
        try:
            if process.wait(timeout=900):
                raise RuntimeError("Native CUDA worker failed; inspect " + str(log))
            return json.loads(output.read_text())
        finally:
            terminate(process)
            memory = sampler.stop()
            output.with_suffix(".memory.json").write_text(
                json.dumps(memory, indent=2) + "\n"
            )


def decisions(values):
    values = np.asarray(values).reshape(-1, 14)
    return [
        (
            int(row[:6].argmax()),
            tuple(np.where(row[6:12] >= np.log(0.4 / 0.6))[0]),
            int(row[12:14].argmax()),
        )
        for row in values
    ]


def probabilities(values):
    """Presentation diagnostic; it never relaxes the frozen raw-output gate."""
    values = np.asarray(values, dtype=np.float64).reshape(-1, 14)
    result = np.empty_like(values)
    for start, end in ((0, 6), (12, 14)):
        x = values[:, start:end]
        exps = np.exp(x - x.max(axis=1, keepdims=True))
        result[:, start:end] = exps / exps.sum(axis=1, keepdims=True)
    result[:, 6:12] = 1 / (1 + np.exp(-np.clip(values[:, 6:12], -700, 700)))
    return result


def canonical_output(vector, fixture):
    values = np.asarray(vector, dtype=np.float64)
    if fixture.get("kind", "classification") != "entities":
        return values
    b = fixture["inputs"][0]["shape"][0]
    q = fixture["inputs"][4]["shape"][1]
    index_start = fixture["candidate_indices_offset"]
    capacity = (len(values) - index_start) // (b * q * 3)
    pair_start = index_start - b * q * capacity
    pairs = values[pair_start:index_start].reshape(b, q, capacity)
    coords = values[index_start : index_start + 2 * b * q * capacity].reshape(
        b, q, capacity, 2
    )
    masks = values[index_start + 2 * b * q * capacity :].reshape(b, q, capacity)
    result = values.copy()
    pair_out = result[pair_start:index_start].reshape(b, q, capacity)
    coord_out = result[index_start : index_start + 2 * b * q * capacity].reshape(
        b, q, capacity, 2
    )
    mask_out = result[index_start + 2 * b * q * capacity :].reshape(b, q, capacity)
    for bi in range(b):
        for qi in range(q):
            order = np.lexsort(
                (coords[bi, qi, :, 1], coords[bi, qi, :, 0], -masks[bi, qi])
            )
            pair_out[bi, qi] = pairs[bi, qi, order]
            coord_out[bi, qi] = coords[bi, qi, order]
            mask_out[bi, qi] = masks[bi, qi, order]
            invalid = mask_out[bi, qi] == 0
            pair_out[bi, qi, invalid] = 0
            coord_out[bi, qi, invalid] = 0
    return result


def compare_to_cpu(vector, fixture, max_error):
    """A backend qualifies against its FP32 oracle, independently of peers."""
    if "cpu_logits" not in fixture:
        raise ValueError("Missing CPU FP32 oracle")
    expected_size = int(np.prod(fixture["outputs"][0]["shape"]))
    for values in (vector, fixture["cpu_logits"]):
        array = np.asarray(values)
        if array.ndim != 1 or array.size != expected_size or not np.isfinite(array).all():
            raise ValueError("Invalid qualification output")
    candidate = canonical_output(vector, fixture)
    reference = canonical_output(fixture["cpu_logits"], fixture)
    error = float(np.max(np.abs(candidate - reference)))
    if fixture.get("kind", "classification") == "classification":
        match = decisions(candidate) == decisions(reference)
    else:
        start = fixture["candidate_indices_offset"]
        match = bool(np.array_equal(candidate[start:], reference[start:]))
    return {"max_abs_output_error": error, "decisions_or_candidate_routing_match": match, "passed": error <= max_error and match}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("native", "runner", "plugin", "model", "manifest", "out"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument(
        "--control",
        type=Path,
        help="Optional frozen native control binary; rotate three fresh-process arms",
    )
    p.add_argument(
        "--native-precision", default="auto", choices=("auto", "fp32", "fp16", "bf16")
    )
    p.add_argument(
        "--control-precision",
        choices=("auto", "fp32", "fp16", "bf16"),
        help="Frozen control precision; defaults to the native candidate precision",
    )
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--iterations", type=int, default=8)
    p.add_argument("--cache", type=Path)
    p.add_argument(
        "--prepare-only",
        action="store_true",
        help="Compile and validate PJRT cache separately from warm inference measurement",
    )
    a = p.parse_args()
    if not 1 <= a.rounds <= 10 or not 1 <= a.iterations <= 100:
        p.error("Use 1..10 rounds and 1..100 iterations")
    a.out.mkdir(parents=True, exist_ok=False)
    a.cache = a.cache or a.out / "executables"
    a.cache.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(a.manifest.read_text())
    inputs = [
        a.runner,
        a.plugin,
        a.manifest,
        a.model / "model.safetensors",
        Path(__file__),
        Path(__file__).with_name("memory.py"),
    ]
    if not a.prepare_only:
        inputs.append(a.native)
        if a.control:
            inputs.append(a.control)
    inputs += [a.manifest.parent / c["program"] for c in manifest["cases"]]
    for name, expected in (
        manifest.get("preparation", {}).get("model_sidecars", {}).items()
    ):
        path = a.model / name
        if digest(path) != expected:
            raise RuntimeError("Model sidecar differs from the graph export: " + name)
        inputs.append(path)
    hashes = {str(path.resolve()): digest(path) for path in inputs}
    if (
        hashes[str((a.model / "model.safetensors").resolve())]
        != manifest["model_sha256"]
    ):
        raise RuntimeError("Model differs from the exported graph checkpoint")
    exact_fp32 = manifest["precision"] == "fp32" and a.native_precision == "fp32"
    max_error = 0.002 if exact_fp32 else 0.02
    gpu = subprocess.check_output(
        [
            "nvidia-smi",
            "--query-gpu=name,driver_version,memory.total",
            "--format=csv,noheader",
        ],
        text=True,
    ).strip()
    cache_identity = {
        "gpu": gpu,
        "plugin_sha256": hashes[str(a.plugin.resolve())],
        "runner_sha256": hashes[str(a.runner.resolve())],
        "manifest_sha256": hashes[str(a.manifest.resolve())],
    }
    cache_meta = a.cache / "identity.json"
    if cache_meta.exists() and json.loads(cache_meta.read_text()) != cache_identity:
        raise RuntimeError(
            "Executable cache belongs to a different benchmark environment"
        )
    cache_meta.write_text(json.dumps(cache_identity, indent=2) + "\n")
    reused_cache = any(a.cache.glob("*.pjrt"))
    if reused_cache and not a.prepare_only:
        hashes.update(
            {str(path.resolve()): digest(path) for path in a.cache.glob("*.pjrt")}
        )
    (a.out / "provenance.json").write_text(
        json.dumps(
            {
                "files": hashes,
                "scope": manifest["scope"],
                "pjrt_precision": manifest["precision"],
                "native_precision": a.native_precision,
                "control_binary_supplied": a.control is not None,
                "control_precision": (a.control_precision or a.native_precision)
                if a.control
                else None,
                "max_abs_logit_error": max_error,
                "exact_decisions_required": True,
                "prepared_cache_reused": reused_cache,
                "prepare_only": a.prepare_only,
                "memory_scope": "whole common Python harness process tree, including parameter feeder and native runtime; startup and inference; PSS plus NVML",
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
        + "\n"
    )
    if a.prepare_only:
        active = subprocess.check_output(
            ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"],
            text=True,
        ).strip()
        if active:
            raise RuntimeError("GPU is occupied: " + active)
        report = pjrt_run(a, manifest, a.out / "prepared.json", a.out / "prepared.log")
        checks = []
        for fixture, case in zip(manifest["cases"], report["cases"], strict=True):
            expected = fixture.get("cpu_logits")
            arrays = [np.asarray(v, dtype=np.float64) for v in case["logits"]]
            if any(
                v.size != np.prod(fixture["outputs"][0]["shape"])
                or not np.isfinite(v).all()
                for v in arrays
            ):
                raise RuntimeError("Invalid prepared outputs")
            error = (
                None
                if expected is None
                else max(
                    float(
                        np.max(
                            np.abs(
                                canonical_output(v, fixture)
                                - canonical_output(expected, fixture)
                            )
                        )
                    )
                    for v in arrays
                )
            )
            checks.append(
                {
                    "id": fixture["id"],
                    "cpu_max_abs": error,
                    "passed": error is None or error <= max_error,
                }
            )
        result = {
            "scope": "cache compilation and CPU numerical preflight; latency is not a warm performance qualification",
            "passed": all(c["passed"] for c in checks),
            "checks": checks,
        }
        (a.out / "summary.json").write_text(
            json.dumps(result, indent=2, allow_nan=False) + "\n"
        )
        print(json.dumps(result), flush=True)
        if not result["passed"]:
            raise RuntimeError(
                "CPU numerical preflight failed; retained outputs and executable cache"
            )
        return
    arms = ("control", "native", "pjrt") if a.control else ("native", "pjrt")
    runs = {arm: [] for arm in arms}
    for round_index in range(a.rounds):
        order = (
            arms[round_index % 3 :] + arms[: round_index % 3]
            if a.control
            else ("native", "pjrt")
            if round_index % 2 == 0
            else ("pjrt", "native")
        )
        for arm in order:
            active = subprocess.check_output(
                ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"],
                text=True,
            ).strip()
            if active:
                raise RuntimeError("GPU is occupied: " + active)
            target = a.out / f"r{round_index}-{arm}.json"
            report = (
                native_run(
                    a,
                    target,
                    target.with_suffix(".log"),
                    a.control if arm == "control" else a.native,
                    a.control_precision if arm == "control" else None,
                )
                if arm != "pjrt"
                else pjrt_run(a, manifest, target, target.with_suffix(".log"))
            )
            memory = json.loads(target.with_suffix(".memory.json").read_text())
            if not memory["valid"]:
                raise RuntimeError("Memory sampling failed")
            if [c["id"] for c in report["cases"]] != [
                c["id"] for c in manifest["cases"]
            ]:
                raise RuntimeError("Workload coverage differs")
            for case in report["cases"]:
                if (
                    len(case["samples_seconds"]) != a.iterations
                    or len(case["logits"]) != a.iterations
                ):
                    raise RuntimeError("Missing measured samples")
                if (
                    not np.isfinite(case["samples_seconds"]).all()
                    or min(case["samples_seconds"]) <= 0
                ):
                    raise RuntimeError("Invalid latency")
            runs[arm].append({"report": report, "memory": memory})
            print(
                round_index,
                arm,
                {
                    c["id"]: round(1000 * statistics.median(c["samples_seconds"]), 3)
                    for c in report["cases"]
                },
                flush=True,
            )
    results = []
    for index, fixture in enumerate(manifest["cases"]):
        cases = {
            arm: [run["report"]["cases"][index] for run in group]
            for arm, group in runs.items()
        }
        reference = canonical_output(cases["native"][0]["logits"][0], fixture)
        if (
            reference.size != np.prod(fixture["outputs"][0]["shape"])
            or not np.isfinite(reference).all()
        ):
            raise RuntimeError("Invalid reference logits")
        errors, matches = [], []
        probability_errors = []
        by_arm = {}
        for arm, group in cases.items():
            arm_errors, arm_matches = [], []
            for case in group:
                for vector in case["logits"]:
                    values = canonical_output(vector, fixture)
                    if values.shape != reference.shape or not np.isfinite(values).all():
                        raise RuntimeError("Invalid output geometry or logits")
                    error = float(np.max(np.abs(values - reference)))
                    errors.append(error)
                    arm_errors.append(error)
                    if fixture.get("kind", "classification") == "classification":
                        match = decisions(values) == decisions(reference)
                        matches.append(match)
                        arm_matches.append(match)
                        probability_errors.append(
                            float(
                                np.max(
                                    np.abs(
                                        probabilities(values) - probabilities(reference)
                                    )
                                )
                            )
                        )
                    else:
                        start = fixture["candidate_indices_offset"]
                        match = np.array_equal(values[start:], reference[start:])
                        matches.append(match)
                        arm_matches.append(match)
            by_arm[arm] = {
                "max_abs_output_error": max(arm_errors),
                "decisions_or_candidate_routing_match": all(arm_matches),
            }
        cpu_error = None
        if "cpu_logits" in fixture:
            cpu = canonical_output(fixture["cpu_logits"], fixture)
            cpu_error = float(np.max(np.abs(cpu - reference)))
        timings = {
            arm: statistics.median(
                t * 1000 for c in group for t in c["samples_seconds"]
            )
            for arm, group in cases.items()
        }
        results.append(
            {
                "id": fixture["id"],
                "kind": fixture.get("kind", "classification"),
                "ms": timings,
                "pjrt_speedup": timings["native"] / timings["pjrt"],
                "native_speedup_over_pjrt": timings["pjrt"] / timings["native"],
                "native_speedup_over_control": timings["control"] / timings["native"]
                if a.control
                else None,
                "max_abs_output_error": max(errors),
                "comparison_to_candidate_reference": by_arm,
                "max_probability_error_diagnostic": max(
                    probability_errors, default=None
                ),
                "decisions_or_candidate_routing_match": all(matches),
                "cpu_max_abs": cpu_error,
                "passed": max(errors) <= max_error
                and all(matches)
                and (cpu_error is None or cpu_error <= max_error),
            }
        )
    if any(digest(Path(path)) != value for path, value in hashes.items()):
        raise RuntimeError("Benchmark artifacts changed")
    summary = {
        "passed": all(c["passed"] for c in results),
        "scope": manifest["scope"],
        "rounds": a.rounds,
        "iterations": a.iterations,
        "results": results,
        "memory": {
            arm: [run["memory"] for run in group] for arm, group in runs.items()
        },
    }
    (a.out / "summary.json").write_text(
        json.dumps(summary, indent=2, allow_nan=False) + "\n"
    )
    if not summary["passed"]:
        raise RuntimeError("Numerical comparison failed; retained all measurements")


if __name__ == "__main__":
    main()
