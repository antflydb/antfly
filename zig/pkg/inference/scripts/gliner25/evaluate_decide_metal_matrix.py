#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Require speed, memory and correctness wins on every frozen matrix cell."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import statistics


def read(path):
    return json.loads(path.read_text())


def percentile95(samples):
    return sorted(samples)[math.ceil(.95 * len(samples)) - 1]


def evaluate(folder, fixture, min_blocks=3, min_reps=20):
    failures = []
    fixture_cases = read(fixture)["cases"]
    expected = [c["id"] for c in fixture_cases]
    batch_counts = {c["id"]:len(c["texts"]) for c in fixture_cases}
    identity = read(folder / "identity.json")
    rows = read(folder / "summary.json") if (folder / "summary.json").exists() else []
    complete = read(folder / "completion.json") if (folder / "completion.json").exists() else {}
    if complete != dict(completed=True, source_unchanged=True, binary_unchanged=True, script_unchanged=True):
        failures.append("Incomplete campaign or source/binary/script changed")
    if sorted(r["id"] for r in rows) != sorted(expected):
        failures.append("Complete frozen case inventory required without duplicates")
    if identity["blocks"] < min_blocks or identity["reps"] < min_reps:
        failures.append("Insufficient paired processes or timed samples")
    if not identity["antfly_controlled"]:
        failures.append("Cancellation supervision required")
    if identity["cases_sha256"] != hashlib.sha256(fixture.read_bytes()).hexdigest():
        failures.append("Frozen fixture identity changed")
    if identity["overrides"]:
        failures.append("Default runtime policy required without tuning overrides")
    if identity.get("python_microbatch") not in (0, 8):
        failures.append("Python full-batch or upstream-default-8 baseline required")
    if identity["warmup"] < 3:
        failures.append("At least three warmups required")
    q8 = identity.get("antfly_quantization") == "q8_0"
    if q8:
        from prepare_decide_metal_q8_reference import PINS
        if {name: value["sha256"] for name, value in identity["model_weights"].items()} != PINS:
            failures.append("Exact published Q8 encoder and head required")
        if identity["python_weights"].get("model.safetensors", {}).get("sha256") != "40a5a23ff860dc3dff426cecd1048cacdd29c648c96db209dad818e9686dc997":
            failures.append("Original FP32 Python timing baseline required")
        if not identity.get("reference_weights") or not identity.get("reference_tools"):
            failures.append("Separate decoded Q8 reference provenance required")
    expected_policy = dict(batch_items=8, padded_batch_tokens=1000,
        reuse_batch_padding=True,
        packed_qkv_cache_bytes=512 * 1024 * 1024,
        attention_scratch_bytes=32 * 1024 * 1024)
    results = []
    for row in rows:
        problems = []
        if q8:
            reference_dir = folder / f'{row["id"]}-q8-reference'
            if not read(reference_dir / "receipt.json")["passed"]:
                problems.append("failed independent decoded-Q8 reference")
            reference_report = read(reference_dir / "report.json")
        antfly_samples, python_samples = [], []
        if len(row["pairs"]) != identity["blocks"]:
            problems.append("paired block inventory")
        for pair in row["pairs"]:
            block = pair["block"]
            if not pair["parity"]["passed"]:
                problems.append(f"block {block}: output parity")
            if pair["antfly_ms"] >= pair["python_ms"]:
                problems.append(f"block {block}: median latency")
            for kind in ("footprint", "rss"):
                if pair[f"antfly_{kind}"] >= pair[f"python_{kind}"]:
                    problems.append(f"block {block}: peak {kind}")
            for runtime, samples in (("antfly", antfly_samples), ("python", python_samples)):
                dest = folder / f'{row["id"]}-{block}-{runtime}'
                receipt, report = read(dest / "receipt.json"), read(dest / "report.json")
                if runtime == "antfly":
                    if q8:
                        from benchmark_decide_metal_matrix import compare
                        if report.get("model_variant") != "decide_deberta_q8":
                            problems.append(f"block {block}: authenticated Q8 model variant")
                        if not compare(report, reference_report)["passed"]:
                            problems.append(f"block {block}: decoded Q8 reference parity")
                    if report.get("metal_model_policy") != expected_policy:
                        problems.append(f"block {block}: authenticated Metal policy")
                    if (report.get("physical_batch_limit") != expected_policy["batch_items"] or
                        report.get("physical_batch_token_limit") != expected_policy["padded_batch_tokens"] or
                        report.get("physical_reuse_batch_padding") is not True):
                        problems.append(f"block {block}: physical batch policy")
                    stats = report.get("metal_runtime_stats") or {}
                    if (stats.get("dense_f32_weight_bytes", 0) <= 0 or
                        stats.get("dense_f16_weight_bytes") != 0 or
                        stats.get("packed_qkv_fallbacks") != 0 or
                        stats.get("packed_qkv_calls", 0) <= 0 or
                        stats.get("mps_linear_calls", 0) <= 0):
                        problems.append(f"block {block}: FP32 packed Metal execution")
                else:
                    limit = identity["python_microbatch"] or batch_counts[row["id"]]
                    if (report.get("physical_batch_limit") != limit or
                        report.get("physical_batch_size") != min(limit, batch_counts[row["id"]]) or
                        report.get("batch_size") != batch_counts[row["id"]]):
                        problems.append(f"block {block}: Python physical batch identity")
                if not receipt["passed"]:
                    problems.append(f"block {block} {runtime}: failed process")
                values = report["samples_ms_sorted"] if runtime == "antfly" else report["samples_ms"]
                if len(values) != identity["reps"] or any(not math.isfinite(x) or x <= 0 for x in values):
                    problems.append(f"block {block} {runtime}: sample inventory")
                samples.extend(values)
                before, after = (receipt[phase] for phase in ("host_before", "host_after"))
                swap = [re.search(r"used\s*=\s*([\d.]+)M", ctx["swap"]) for ctx in (before, after)]
                if not all(swap) or float(swap[1][1]) > float(swap[0][1]):
                    problems.append(f"block {block} {runtime}: missing swap telemetry or growth")
                powers = [re.search(r"drawing from '([^']+)'", ctx["power"]) for ctx in (before, after)]
                if not all(powers) or powers[0][1] != powers[1][1]:
                    problems.append(f"block {block} {runtime}: power source changed or missing")
                for ctx in (before, after):
                    if not ctx["thermal"] or re.search(r"CPU_Speed_Limit\s*=\s*(?:[0-9]|[1-9][0-9])(?:\D|$)", ctx["thermal"]):
                        problems.append(f"block {block} {runtime}: thermal telemetry unavailable or throttled")
        p95_a = percentile95(antfly_samples)
        p95_p = percentile95(python_samples)
        if p95_a >= p95_p:
            problems.append("pooled p95 latency")
        results.append(dict(id=row["id"], passed=not problems, failures=problems,
            antfly_median_ms=statistics.median(antfly_samples),
            python_median_ms=statistics.median(python_samples),
            antfly_p95_ms=p95_a, python_p95_ms=p95_p,
            footprint_ratio=row["antfly_footprint"] / row["python_footprint"],
            rss_ratio=row["antfly_rss"] / row["python_rss"]))
        failures.extend(f'{row["id"]}: {problem}' for problem in problems)
    return dict(passed=not failures, scope=identity["timing_scope"],
        expected_cases=len(expected), completed_cases=len(rows),
        required_blocks=min_blocks, required_reps=min_reps,
        policy=dict(effective=expected_policy,
                    requested_max_items=identity["antfly_microbatch"],
                    requested_max_padded_tokens=identity["antfly_batch_tokens"],
                    python_internal_batch_limit=identity["python_microbatch"],
                    environment=identity["execution_environment"]),
        failures=failures, cases=results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-blocks", type=int, default=3)
    parser.add_argument("--min-reps", type=int, default=20)
    args = parser.parse_args()
    result = evaluate(args.campaign, args.fixture, args.min_blocks, args.min_reps)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps(dict(passed=result["passed"], completed_cases=result["completed_cases"],
                         failures=result["failures"]), indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
