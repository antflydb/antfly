# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Same-binary native change qualification against Transformers logits."""

import argparse
import json
import os
import statistics
import subprocess
from pathlib import Path

import numpy as np
from benchmark_gemma4 import digest, gpu, idle, native_suite
from memory import MemorySampler
from validate_gemma4 import compare


def accuracy_gate(before, after):
    # Improvement never substitutes for the original absolute reference gate.
    change = after["argmax_matches"] and after["relative_l2"] <= before["relative_l2"]
    absolute = after["argmax_matches"] and after["logit_tolerance_pass"]
    return {"change_accuracy_pass": change, "absolute_reference_pass": absolute}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("native", "gguf", "mmproj", "tokenizer", "template", "suite", "out"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--cpu", type=Path, action="append", required=True)
    p.add_argument(
        "--control", choices=("ple", "video-resize", "branch"), default="ple"
    )
    p.add_argument("--measure", action="store_true")
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--iterations", type=int, default=3)
    a = p.parse_args()
    if not 1 <= a.rounds <= 10 or not 1 <= a.iterations <= 100:
        p.error("Invalid campaign bounds")
    a.out.mkdir(parents=True, exist_ok=False)
    acceptance = {
        "control": a.control,
        "reference": "independent unmodified Transformers CPU FP32",
        "cosine_min": 0.9999,
        "relative_l2_max": 0.02,
        "change_relative_l2_ratio_max": 1.0,
        "require_reference_argmax": True,
        "latency_median_ratio_max": 1.05,
        "latency_p95_ratio_max": 1.10,
        "rounds": a.rounds,
        "iterations": a.iterations,
    }
    (a.out / "acceptance.json").write_text(json.dumps(acceptance, indent=2) + "\n")
    suite = json.loads(a.suite.read_text())
    for name, identity in suite["files"].items():
        path = a.suite.parent / name
        if (
            path.stat().st_size != identity["bytes"]
            or digest(path) != identity["sha256"]
        ):
            raise ValueError("Fixture identity differs: " + name)
    if a.control in ("video-resize", "branch") and not any(
        case.get("kind") == "video" for case in suite["cases"]
    ):
        raise ValueError("Video resize control needs a video case")
    refs, sources, reference_hashes = {}, {}, {}
    for directory in a.cpu:
        identity = json.loads((directory / "reference.json").read_text())
        if identity["forward_module"] != "transformers.models.gemma4.modeling_gemma4":
            raise ValueError("Reference must use the official Transformers forward")
        if identity.get("logit_position", "first_step") != "first_step":
            raise ValueError("Expected first-step CPU references")
        sources[str(directory)] = identity
        for path in directory.glob("*.f32"):
            if path.stem in refs:
                raise ValueError("Duplicate reference")
            refs[path.stem] = np.fromfile(path, dtype="<f4")
            reference_hashes[path.stem] = digest(path)
    ids = {case["id"] for case in suite["cases"]}
    if ids != set(refs) or len(ids) != len(suite["cases"]):
        raise ValueError("Suite/reference coverage differs")
    provenance = {
        "gpu": gpu(),
        "control": a.control,
        "arm_environment_overrides": {
            arm: {"ANTFLY_INFERENCE_CUDA_GEMMA4_VIDEO_BICUBIC": value}
            for arm, value in (("legacy", "0"), ("corrected", "1"))
        }
        if a.control in ("video-resize", "branch")
        else {},
        "reference_sources": sources,
        "reference_sha256": reference_hashes,
        "runtime_environment": {
            key: value
            for key, value in os.environ.items()
            if key.startswith("ANTFLY_INFERENCE_")
            or key in ("ANTFLY_CUDA_QMATMUL_VARIANT", "TERMITE_GEMMA4_AUDIO_HOST_OPS")
        },
        "files": {
            name: digest(getattr(a, name))
            for name in ("native", "gguf", "mmproj", "tokenizer", "template", "suite")
        },
    }
    (a.out / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")

    def run(arm, index, validation):
        idle()
        out = a.out / (("validation-" if validation else f"r{index}-") + arm)
        out.mkdir()
        selected = native_suite(
            suite, a.suite.parent, out, 1 if validation else a.iterations, validation
        )
        selected["legacy_ple_normalization"] = (
            a.control in ("ple", "branch") and arm == "legacy"
        )
        environment = dict(os.environ)
        if a.control in ("video-resize", "branch"):
            environment["ANTFLY_INFERENCE_CUDA_GEMMA4_VIDEO_BICUBIC"] = (
                "0" if arm == "legacy" else "1"
            )
        config = out / "suite.json"
        config.write_text(json.dumps(selected) + "\n")
        with (out / "worker.log").open("wb") as log:
            process = subprocess.Popen(
                [
                    str(a.native.resolve()),
                    str(a.gguf.resolve()),
                    str(a.tokenizer.resolve()),
                    str(a.template.resolve()),
                    str(a.mmproj.resolve()),
                    str(config.resolve()),
                    str((out / "result.json").resolve()),
                ],
                stdout=log,
                stderr=log,
                env=environment,
            )
            sampler = MemorySampler(process, out / "memory.jsonl")
            sampler.thread.start()
            try:
                process.wait(timeout=1800)
                if process.returncode:
                    raise RuntimeError("Native worker failed: " + str(out))
            except BaseException:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                raise
            finally:
                memory = sampler.stop()
                (out / "memory.json").write_text(json.dumps(memory, indent=2) + "\n")
        if not memory["valid"]:
            raise ValueError("Invalid memory sampling")
        report = json.loads((out / "result.json").read_text())
        if (
            report["legacy_ple_normalization"] != selected["legacy_ple_normalization"]
            or report["ple_control_calls"] == 0
        ):
            raise ValueError("Native worker did not apply the requested PLE control")
        rows = report["rows"]
        for case in suite["cases"]:
            selected_rows = [row for row in rows if row["id"] == case["id"]]
            if len(selected_rows) != (1 if validation else a.iterations):
                raise ValueError("Incomplete native measurements")
        return out, rows

    directories = {arm: run(arm, 0, True)[0] for arm in ("legacy", "corrected")}
    accuracy = []
    for case in suite["cases"]:
        before, after = [
            compare(
                np.fromfile(directories[arm] / (case["id"] + ".f32"), dtype="<f4"),
                refs[case["id"]],
            )
            for arm in ("legacy", "corrected")
        ]
        accuracy.append(
            {
                "id": case["id"],
                "legacy": before,
                "corrected": after,
                **accuracy_gate(before, after),
            }
        )
    timings = {arm: [] for arm in ("legacy", "corrected")}
    if a.measure:
        for index in range(a.rounds):
            for arm in (
                ("legacy", "corrected") if index % 2 == 0 else ("corrected", "legacy")
            ):
                _, rows = run(arm, index, False)
                timings[arm].extend(rows)
                print(index, arm, "complete", flush=True)
    latency = []
    if a.measure:
        for case in suite["cases"]:
            samples = {
                arm: [row["ms"] for row in rows if row["id"] == case["id"]]
                for arm, rows in timings.items()
            }
            median = {arm: statistics.median(values) for arm, values in samples.items()}
            p95 = {
                arm: float(np.percentile(values, 95)) for arm, values in samples.items()
            }
            ratio, tail_ratio = (
                median["corrected"] / median["legacy"],
                p95["corrected"] / p95["legacy"],
            )
            latency.append(
                {
                    "id": case["id"],
                    "samples_ms": samples,
                    "median_ms": median,
                    "p95_ms": p95,
                    "median_ratio": ratio,
                    "p95_ratio": tail_ratio,
                    "latency_pass": ratio <= 1.05 and tail_ratio <= 1.10,
                }
            )
    report = {
        "scope": (
            "same binary; "
            + {
                "ple": "legacy versus corrected PLE epsilon",
                "video-resize": "bilinear versus bicubic video resize",
                "branch": "legacy PLE epsilon and bilinear video versus corrected PLE epsilon and bicubic video",
            }[a.control]
            + "; direct generation, fixed budgets"
        ),
        "performance_measured": a.measure,
        "acceptance": acceptance,
        "provenance": provenance,
        "accuracy": accuracy,
        "latency": latency,
        "change_accuracy_pass": all(row["change_accuracy_pass"] for row in accuracy),
        "absolute_reference_pass": all(
            row["absolute_reference_pass"] for row in accuracy
        ),
        "latency_pass": all(row["latency_pass"] for row in latency)
        if a.measure
        else None,
    }
    report["passed"] = (
        report["absolute_reference_pass"]
        and report["change_accuracy_pass"]
        and report["latency_pass"] is not False
    )
    (a.out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
