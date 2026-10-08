#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Sequential, rotating prepared-text encoder comparison on a single GPU.

Archive the baseline executable before changing the encoder. This driver keeps
every attempt, rejects measured paging, and gates BOTH document lengths in EACH
process order. It does not qualify application quality or production deployment.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys

import embeddinggemma2_compare as compare

CASES = ("document_512", "document_8192")
ORDERS = (("mps", "baseline", "candidate"), ("baseline", "candidate", "mps"),
          ("candidate", "mps", "baseline"))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def paging_delta(run, case):
    if run.get("vm_counters_available") is False:
        raise ValueError("paging telemetry unavailable")
    value = run["cases"][case]
    if "vm_measured_before" in value and "vm_measured_after" in value:
        before, after = value["vm_measured_before"], value["vm_measured_after"]
    else:
        # An older archived baseline lacks sample-window telemetry. Requiring
        # its entire process to be clean is conservative and preserves it.
        def counters(raw):
            result = []
            for key in ("Pageouts", "Swapins", "Swapouts"):
                match = re.search(r"^" + key + r":\s+(\d+)", raw, re.MULTILINE)
                if not match:
                    raise ValueError("missing paging counter")
                result.append(int(match[1]))
            return result
        before, after = counters(run["before"]["vm_stat"]), counters(run["after"]["vm_stat"])
    if len(before) != 3 or len(after) != 3 or any(y < x for x, y in zip(before, after)):
        raise ValueError("invalid paging counters")
    return [y - x for x, y in zip(before, after)]


def assess(runs, max_ratio=1.10, sample_counts=None):
    sample_counts = sample_counts or {"document_512": 20, "document_8192": 5}
    for role in ("mps", "baseline", "candidate"):
        if runs[role]["status"] != "pass" or set(runs[role]["cases"]) != set(CASES):
            raise ValueError("incomplete encoder run")
        for name, value in runs[role]["cases"].items():
            samples = value["timing"]["samples_seconds"]
            if len(samples) != sample_counts[name] or any(not math.isfinite(x) or x <= 0 for x in samples):
                raise ValueError("invalid encoder sample count or duration")
            if role != "mps":
                if value["host_live_after_release"] != 0 or value["host_peak_bytes"] > 512 * 1024 * 1024:
                    raise ValueError("host workspace release or admission failed")
                memory = value.get("gpu_memory")
                if role == "candidate" and not memory:
                    raise ValueError("candidate GPU memory telemetry missing")
                if memory and any(m["frame_retained_bytes"] or m["scratch_pool_pending_slots"] for m in memory):
                    raise ValueError("GPU frame did not drain")
    parity = {role: compare.compare_reference(runs[role], runs["mps"], 1e-5)
              for role in ("baseline", "candidate")}
    cases = {}
    for name in CASES:
        candidate = runs["candidate"]["cases"][name]["timing"]["p50_seconds"]
        baseline = runs["baseline"]["cases"][name]["timing"]["p50_seconds"]
        oracle = runs["mps"]["cases"][name]["timing"]["p50_seconds"]
        paging = {role: paging_delta(run, name) for role, run in runs.items()}
        eligible = all(delta == [0, 0, 0] for delta in paging.values())
        cases[name] = {"candidate_seconds": candidate, "baseline_seconds": baseline,
                       "mps_seconds": oracle, "candidate_over_mps": candidate / oracle,
                       "candidate_over_baseline": candidate / baseline,
                       "paging_delta": paging, "archived_baseline_gpu_telemetry_available": bool(runs["baseline"]["cases"][name].get("gpu_memory")), "timing_eligible": eligible,
                       "within_target": eligible and candidate / oracle <= max_ratio}
    return {"parity": parity, "cases": cases,
            "timing_eligible": all(case["timing_eligible"] for case in cases.values()),
            "within_target": all(case["within_target"] for case in cases.values())}


def verify_source(manifest_path, root):
    manifest = json.loads(manifest_path.read_text())
    for path, expected in manifest["files"].items():
        if sha(root / path) != expected:
            raise ValueError("source changed during campaign: " + path)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("model-dir", "suite", "baseline", "candidate", "output-dir", "source-manifest"):
        parser.add_argument("--" + flag, type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--long-iterations", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--paging-retries", type=int, default=2)
    args = parser.parse_args()
    if args.iterations < 20 or args.long_iterations < 5 or args.warmups < 2 or not 0 <= args.paging_retries <= 4:
        parser.error("qualification requires at least 20/5 samples, two warmups and 0..4 paging retries")
    root = Path(__file__).resolve().parents[4]
    manifest = verify_source(args.source_manifest, root)
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    hashes = {role: sha(path) for role, path in binaries.items()}
    if manifest.get("binary_sha256") != hashes["candidate"]:
        raise ValueError("candidate binary differs from source manifest")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {"version": 1, "status": "running", "production_qualified": False,
              "scope": "synchronized prepared text encoder; small parity suite",
              "target": {"max_candidate_over_mps": 1.10, "max_vector_error": 1e-5},
              "sample_counts": {"document_512": args.iterations, "document_8192": args.long_iterations},
              "warmups": args.warmups, "binary_sha256": hashes,
              "source_manifest_sha256": sha(args.source_manifest), "source": manifest,
              "suite_sha256": sha(args.suite), "tool_sha256": sha(Path(__file__)),
              "orders": [], "attempts": []}
    output = args.output_dir / "campaign.json"
    environment = dict(os.environ, VECLIB_MAXIMUM_THREADS="4", OMP_NUM_THREADS="4")
    try:
        for rotation, order in enumerate(ORDERS):
            accepted = None
            for attempt in range(args.paging_retries + 1):
                runs = {}
                artifacts = {}
                for role in order:
                    verify_source(args.source_manifest, root)
                    if any(sha(path) != hashes[key] for key, path in binaries.items()):
                        raise ValueError("binary changed during campaign")
                    path = args.output_dir / f"rotation-{rotation}-attempt-{attempt}-{role}.json"
                    command = [sys.executable, str(Path(compare.__file__).resolve()),
                               "--model-dir", str(args.model_dir.resolve()), "--suite", str(args.suite.resolve()),
                               "--backend", "mps" if role == "mps" else "metal", "--timing-mode", "encoder",
                               "--iterations", str(args.iterations), "--long-iterations", str(args.long_iterations),
                               "--warmups", str(args.warmups), "--max-vector-error", "0.00001", "--output", str(path.resolve())]
                    for name in CASES:
                        command.extend(["--case", name])
                    if role != "mps":
                        command.extend(["--binary", str(binaries[role])])
                    with path.with_suffix(".driver.log").open("w") as log:
                        subprocess.run(command, env=environment, stdout=log, stderr=log, check=True, timeout=1800)
                    runs[role] = json.loads(path.read_text())
                    artifacts[role] = {"path": str(path), "sha256": sha(path), "run": runs[role]}
                    print(f"rotation {rotation} attempt {attempt}: {role} complete", flush=True)
                assessment = assess(runs, sample_counts=report["sample_counts"])
                result = {"rotation": rotation, "attempt": attempt, "order": order,
                          "assessment": assessment, "artifacts": artifacts}
                report["attempts"].append(result)
                compare.publish(output, report)
                if assessment["timing_eligible"]:
                    accepted = result
                    break
                print("Measured paging: retaining evidence and repeating this rotation", flush=True)
            if accepted is None:
                raise ValueError("no paging-free rotation after retries")
            report["orders"].append(accepted)
        verify_source(args.source_manifest, root)
        report["performance_goal_achieved"] = all(order["assessment"]["within_target"] for order in report["orders"])
        if not report["performance_goal_achieved"]:
            raise ValueError("encoder performance target missed")
        report["status"] = "pass"
    finally:
        if report["status"] != "pass":
            report["status"] = "failed"
        compare.publish(output, report)


if __name__ == "__main__":
    main()
