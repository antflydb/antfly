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

"""Per-case upstream parity and same-backend no-regression latency gates.

Consumes compare.py or video_compare.py receipts, retaining failed cases.
Request scope includes Antfly HTTP; the reference includes preprocessing and
inference without HTTP. Prepared encoder scope is a separate, matched gate.
No average across modalities can hide a regression. Peak memory, cold setup
and concurrent-load receipts are preserved separately, not equated across APIs.
"""

import argparse
import json
import math
import re
from pathlib import Path

if __package__:
    from . import compare, reference
else:
    import compare
    import reference


def paging_observation(report, case):
    first, last = case.get("vm_measured_before"), case.get("vm_measured_after")
    scope = "measured samples"
    if first is None or last is None:
        scope = "whole process including cold setup"

        def parse(when):
            state = report.get(when, report.get("conditions_" + when, {}))
            raw = state.get("vm_stat", state.get("vmstat", ""))
            if isinstance(raw, dict):
                raw = raw.get("output", "")
            keys = (
                ("Pageouts", "Swapins", "Swapouts")
                if "Pageouts:" in raw
                else ("pswpin", "pswpout")
            )
            values = []
            for key in keys:
                match = re.search(r"^" + key + r":?\s+(\d+)", raw, re.MULTILINE)
                if not match:
                    return None
                values.append(int(match[1]))
            return values

        first, last = parse("before"), parse("after")
    if first is None or last is None or len(first) != len(last) or not first:
        return {"scope": scope, "status": "unobserved", "delta": None}
    delta = [b - a for a, b in zip(first, last)]
    return {
        "scope": scope,
        "status": "quiet" if all(x == 0 for x in delta) else "paging_or_counter_reset",
        "delta": delta,
    }


def evaluate(
    candidate, baseline, *, boundary="request", limit=1.10, minimum_samples=10
):
    if (
        boundary not in ("request", "encoder")
        or not math.isfinite(limit)
        or limit <= 0
        or minimum_samples < 2
    ):
        raise ValueError("invalid performance gate policy")
    if (candidate.get("backend"), baseline.get("backend")) not in (
        ("native", "cpu"),
        ("metal", "mps"),
    ):
        raise ValueError("expected native/CPU or Metal/MPS pair")
    identity = "corpus_sha256" if "corpus_sha256" in candidate else "suite_sha256"
    for field in (identity, "checkpoint_receipt_sha256", "precision"):
        if not candidate.get(field) or candidate[field] != baseline.get(field):
            raise ValueError(f"unmatched {field}")
    if candidate["precision"] != "float32" or any(
        r.get("status") != "pass" for r in (candidate, baseline)
    ):
        raise ValueError("completed F32 receipts required")
    fingerprint = candidate.get("binary_sha256", "")
    if len(fingerprint) != 64 or any(c not in "0123456789abcdef" for c in fingerprint):
        raise ValueError("candidate binary fingerprint required")
    modes = [r.get("measurement_mode", "default") for r in (candidate, baseline)]
    if (boundary == "encoder" and modes != ["encoder", "encoder"]) or (
        boundary == "request" and "encoder" in modes
    ):
        raise ValueError("prepared and request timing boundaries cannot be mixed")
    cases = candidate.get("cases", {})
    if not cases or set(cases) != set(baseline.get("cases", {})):
        raise ValueError("identical nonempty case coverage required")
    rows = []
    for name, actual in cases.items():
        expected = baseline["cases"][name]
        # Container comparison records completed_timing; compact model
        # comparison records timing plus the reference preprocessing boundary.
        actual_time = actual.get("completed_timing", actual.get("timing"))
        expected_time = expected.get("completed_timing") or (
            expected.get("timing")
            if boundary == "encoder"
            else expected.get("preprocessing_and_inference_timing")
        )
        if actual_time is None or expected_time is None:
            raise ValueError(f"missing {boundary} timing for {name}")
        a = actual_time["samples_seconds"]
        b = expected_time["samples_seconds"]
        # Recompute medians from raw samples; never trust reported aggregates.
        stats_a, stats_b = compare.timing(a), compare.timing(b)
        parity = compare.agreement(actual["vector"], expected["vector"])
        sufficient = min(len(a), len(b)) >= minimum_samples
        ratio = stats_a["p50_seconds"] / stats_b["p50_seconds"]
        memory = {
            "candidate": paging_observation(candidate, actual),
            "reference": paging_observation(baseline, expected),
        }
        quiet = all(v["status"] == "quiet" for v in memory.values())
        passed = (
            sufficient
            and quiet
            and ratio <= limit
            and parity["cosine"] >= 0.9999
            and parity["max_abs"] <= 1e-5
            and abs(parity["actual_norm"] - 1) <= 1e-4
        )
        rows.append(
            dict(
                case=name,
                status="pass" if passed else "fail",
                sufficient_samples=sufficient,
                native=stats_a,
                reference=stats_b,
                median_ratio=ratio,
                p95_ratio=stats_a["p95_seconds"] / stats_b["p95_seconds"],
                parity=parity,
                paging=memory,
                quiet_memory=quiet,
            )
        )
    return dict(
        version=1,
        status="pass" if all(r["status"] == "pass" for r in rows) else "fail",
        boundary=boundary,
        median_limit=limit,
        minimum_samples=minimum_samples,
        backend=candidate["backend"],
        binary_sha256=fingerprint,
        identity={
            k: candidate[k]
            for k in (identity, "checkpoint_receipt_sha256", "precision")
        },
        cases=rows,
        timing_scope="matched prepared encoder"
        if boundary == "encoder"
        else "Antfly HTTP request / reference preprocessing plus inference; HTTP excluded from reference",
        production_qualified=False,
    )


def digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def valid_samples(record):
    samples = record.get("samples_seconds") if isinstance(record, dict) else None
    if not isinstance(samples, list) or not samples or any(
        isinstance(x, bool) or not isinstance(x, (int, float))
        or not math.isfinite(x) or x <= 0 for x in samples
    ):
        raise ValueError("positive finite recorded timing samples required")
    return samples


def matching_environment(candidate, baseline):
    facts = None
    order = None
    for report in (candidate, baseline):
        for when in ("before", "after"):
            snapshot = report.get(when, {})
            host = snapshot.get("host_identity")
            budget = snapshot.get("thread_budget")
            device = snapshot.get("device_identity")
            if (not isinstance(host, dict) or not host.get("node") or not host.get("machine")
                or not host.get("release") or type(host.get("cpu_count")) is not int or host["cpu_count"] <= 0
                or not snapshot.get("hardware") or not isinstance(budget, dict)
                or not isinstance(budget.get("environment"), dict)
                or not budget.get("affinity") or not budget.get("cgroup_cpu_max")
                or not isinstance(device, dict) or device.get("status") != "observed"):
                raise ValueError("matching observed host/device/thread-budget snapshots required")
            thread_environment = budget["environment"]
            if (set(thread_environment) != set(compare.THREAD_ENVIRONMENT_KEYS)
                or any(value is not None and not isinstance(value, str) for value in thread_environment.values())
                or type(budget.get("cpu_count")) is not int or budget["cpu_count"] != host["cpu_count"]):
                raise ValueError("complete configured thread controls and CPU budget required")
            platform_name = host.get("platform", "")
            expected_order = (["pageouts", "swapins", "swapouts"] if platform_name == "darwin"
                              else ["pswpin", "pswpout"] if platform_name.startswith("linux") else None)
            if expected_order is None or report.get("vm_counter_order") != expected_order:
                raise ValueError("explicit platform paging counter order required")
            if platform_name.startswith("linux"):
                if (not isinstance(host.get("boot_id"), str) or not host["boot_id"].strip()
                    or type(host.get("memory_bytes")) is not int or host["memory_bytes"] <= 0):
                    raise ValueError("actual Linux boot and memory identity required")
                affinity = budget["affinity"]
                if (not isinstance(affinity, list) or any(type(cpu) is not int or cpu < 0 for cpu in affinity)
                    or len(affinity) != len(set(affinity))):
                    raise ValueError("actual Linux process CPU affinity required")
                quotas = budget.get("cgroup_cpu_max")
                if not isinstance(quotas, list) or not quotas:
                    raise ValueError("actual Linux cgroup CPU quota observations required")
                paths = set()
                for quota in quotas:
                    path = quota.get("path") if isinstance(quota, dict) else None
                    value = quota.get("value") if isinstance(quota, dict) else None
                    if (not isinstance(path, str) or not path.startswith("/sys/fs/cgroup/")
                        or Path(path).name != "cpu.max" or ".." in Path(path).parts
                        or path in paths or not isinstance(value, str)):
                        raise ValueError("invalid Linux cgroup CPU quota observation")
                    tokens = value.split()
                    if (len(tokens) != 2 or not tokens[1].isascii() or not tokens[1].isdigit()
                        or int(tokens[1]) <= 0 or (tokens[0] != "max" and
                            (not tokens[0].isascii() or not tokens[0].isdigit() or int(tokens[0]) <= 0))):
                        raise ValueError("invalid Linux cpu.max quota tokens")
                    paths.add(path)
            kind = {"native": "cpu", "metal": "metal", "cuda": "cuda"}[report["backend"]]
            if device.get("kind") != kind:
                raise ValueError("observed device backend differs")
            if kind == "cuda":
                if any(not device.get(k) for k in ("uuid", "name", "driver_version", "pci_bus_id", "query_output")):
                    raise ValueError("observed CUDA device and driver identity required")
                capacity = device.get("memory_mib")
                capacity_status = device.get("memory_capacity_status")
                if not ((capacity_status == "observed" and type(capacity) is int and capacity > 0)
                        or (capacity_status == "unavailable" and capacity is None)):
                    raise ValueError("explicit valid or unavailable CUDA memory observation required")
            if kind == "metal" and not device.get("device", {}).get("sppci_model"):
                raise ValueError("observed Metal GPU identity required")
            current = {k: snapshot[k] for k in ("host_identity", "hardware", "thread_budget", "device_identity")}
            if facts is not None and current != facts:
                raise ValueError("host, device or thread budget changed within/between arms")
            facts, order = current, expected_order
    return facts, order


def evaluate_no_regression(
    candidate, baseline, *, boundary="request", median_limit=1.05,
    p95_limit=1.10, minimum_samples=10, quality_reference=None,
    quality_reference_sha256=None,
):
    """Compare independent completed Antfly runs; receipt order implies no pairing.

    Checkpoint receipt hashes bind the producer-verified model/revision/files;
    suite hashes bind the complete workload. No relaxed upstream precision or
    quality policy is inferred from the two latency distributions.
    """
    if (boundary not in ("request", "encoder") or
        any(isinstance(x, bool) or not isinstance(x, (int, float)) or
            not math.isfinite(x) or x <= 0 for x in (median_limit, p95_limit)) or
        isinstance(minimum_samples, bool) or not isinstance(minimum_samples, int)
        or minimum_samples < 2):
        raise ValueError("invalid no-regression gate policy")
    backend = candidate.get("backend")
    if backend not in ("native", "metal", "cuda") or baseline.get("backend") != backend:
        raise ValueError("same Native, Metal or CUDA backend required")
    if any(r.get("status") != "pass" for r in (candidate, baseline)):
        raise ValueError("completed successful receipts required")
    # compare.py owns this quality contract. Container/video receipts use a
    # different reference contract and cannot borrow it from a matching digest.
    if any("corpus_sha256" in r for r in (candidate, baseline)):
        raise ValueError("blocked: container corpus quality contract is distinct")
    for field in ("suite_sha256", "checkpoint_receipt_sha256", "tool_sha256"):
        if not digest(candidate.get(field)) or candidate[field] != baseline.get(field):
            raise ValueError(f"missing or unmatched {field}")
    for r in (candidate, baseline):
        if not digest(r.get("binary_sha256")):
            raise ValueError("both binary fingerprints required")
    environment, counter_order = matching_environment(candidate, baseline)
    precision = candidate.get("precision")
    if precision not in ("float32", "bfloat16") or baseline.get("precision") != precision:
        raise ValueError("matching explicit F32/BF16 precision required")
    compare.execution_precision(backend, precision)
    if any(r.get("pooling_precision") != "float32" for r in (candidate, baseline)):
        raise ValueError("explicit F32 pooling required")
    modes = [r.get("measurement_mode") for r in (candidate, baseline)]
    required_mode = "encoder" if boundary == "encoder" else "default"
    if modes != [required_mode, required_mode]:
        raise ValueError("matching explicit timing boundary required")
    if (not isinstance(candidate.get("timing_scope"), str) or not candidate["timing_scope"]
        or candidate["timing_scope"] != baseline.get("timing_scope")):
        raise ValueError("matching recorded timing scopes required")
    for field in ("implementation_controls", "reference_tool_sha256"):
        if candidate.get(field) != baseline.get(field):
            raise ValueError(f"unmatched {field}")
    if boundary == "request" and (not candidate.get("model_identity") or
        candidate["model_identity"] != baseline.get("model_identity")):
        raise ValueError("matching served model identities required")
    if boundary == "encoder" and backend == "cuda":
        raise ValueError("blocked: CUDA producer has no encoder timing contract")
    cases = candidate.get("cases")
    if not isinstance(cases, dict) or not cases or set(cases) != set(baseline.get("cases", {})):
        raise ValueError("identical nonempty case coverage required")
    selection = candidate.get("case_selection")
    if (not isinstance(selection, list) or any(not isinstance(x, str) for x in selection)
        or len(selection) != len(set(selection)) or set(selection) != set(cases)
        or selection != baseline.get("case_selection")):
        raise ValueError("matching explicit case selections required")
    if not isinstance(candidate.get("implementation_controls"), dict):
        raise ValueError("recorded implementation controls required")
    if candidate.get("reference_tool_sha256") is not None and not digest(candidate["reference_tool_sha256"]):
        raise ValueError("invalid reference tool fingerprint")
    max_error = 1e-5 if precision == "float32" else 1e-4
    if precision == "bfloat16":
        # BF16 producers otherwise self-compare their output. Require recorded
        # independent matched-BF16 parity from BOTH runs, not merely status=pass.
        reference_hash = candidate.get("reference_result_sha256")
        if not digest(reference_hash) or reference_hash != baseline.get("reference_result_sha256"):
            raise ValueError("blocked: BF16 requires the same independent reference receipt")
        if (quality_reference_sha256 != reference_hash or not isinstance(quality_reference, dict)
            or quality_reference.get("backend") != "torch-cuda"
            or quality_reference.get("status") != "pass"
            or quality_reference.get("precision") != "bfloat16"
            or quality_reference.get("pooling_precision") != "float32"):
            raise ValueError("blocked: actual matched Torch CUDA BF16 reference receipt required")
        for r in (candidate, baseline):
            # Recheck actual reference vectors/choices/rankings, not only the
            # producer's summary; the CLI hashes the supplied receipt bytes.
            try:
                compare.compare_reference(r, quality_reference, max_error)
            except (ValueError, KeyError, TypeError) as error:
                raise ValueError("blocked: BF16 independent reference parity failed") from error
            tolerance = r.get("max_vector_error")
            vectors = r.get("parity", {}).get("vectors", {})
            if (isinstance(tolerance, bool) or not isinstance(tolerance, (int, float))
                or not math.isfinite(tolerance) or not 0 < tolerance <= max_error
                or set(vectors) != set(cases)):
                raise ValueError("blocked: BF16 matched-reference quality evidence incomplete")
            for value in vectors.values():
                if not quality_passes(value, max_error):
                    raise ValueError("blocked: BF16 reference quality contract failed")
    quality_error = None
    try:
        # Preserve choices, routing scores/margins and retrieval rankings too.
        compare.compare_reference(candidate, baseline, max_error)
    except ValueError as error:
        quality_error = str(error)
    rows = []
    for name, actual in cases.items():
        expected = baseline["cases"][name]
        if actual.get("tokens") != expected.get("tokens") or not isinstance(actual.get("tokens"), int) or isinstance(actual["tokens"], bool) or actual["tokens"] <= 0:
            raise ValueError(f"matching positive token counts required for {name}")
        a, b = valid_samples(actual.get("timing")), valid_samples(expected.get("timing"))
        stats_a, stats_b = compare.timing(a), compare.timing(b)
        parity = compare.agreement(actual["vector"], expected["vector"])
        reverse = compare.agreement(expected["vector"], actual["vector"])
        enough = min(len(a), len(b)) >= minimum_samples
        median_ratio = stats_a["p50_seconds"] / stats_b["p50_seconds"]
        p95_ratio = stats_a["p95_seconds"] / stats_b["p95_seconds"]
        if any(not math.isfinite(x) or x <= 0 for x in (median_ratio, p95_ratio)):
            raise ValueError("finite positive latency ratios required")
        for case in (actual, expected):
            for key in ("vm_measured_before", "vm_measured_after"):
                counters = case.get(key)
                if (not isinstance(counters, list) or len(counters) != len(counter_order)
                    or any(type(x) is not int or x < 0 for x in counters)):
                    raise ValueError("measured paging counters must match actual platform order")
        memory = {"candidate": paging_observation(candidate, actual),
                  "baseline": paging_observation(baseline, expected)}
        quiet = all(value["status"] == "quiet" for value in memory.values())
        passed = (enough and quiet and quality_error is None
                  and quality_passes(parity, max_error) and quality_passes(reverse, max_error)
                  and median_ratio <= median_limit and p95_ratio <= p95_limit)
        rows.append(dict(case=name, status="pass" if passed else "fail",
                         sufficient_samples=enough, candidate=stats_a, baseline=stats_b,
                         median_ratio=median_ratio, p95_ratio=p95_ratio, parity=parity,
                         paging=memory, quiet_memory=quiet))
    return dict(version=1, mode="no-regression",
                status="pass" if all(r["status"] == "pass" for r in rows) else "fail",
                backend=backend, boundary=boundary, median_limit=median_limit,
                p95_limit=p95_limit, minimum_samples=minimum_samples,
                identity={k: candidate[k] for k in
                          ("suite_sha256", "checkpoint_receipt_sha256", "precision", "pooling_precision", "timing_scope")},
                binaries={"candidate": candidate["binary_sha256"], "baseline": baseline["binary_sha256"]},
                cases=rows, quality_error=quality_error, environment=environment,
                vm_counter_order=counter_order,
                sample_relationship="independent distributions; no recorded pairing",
                timing_scope=candidate["timing_scope"], physical_memory_qualified=False, production_qualified=False)


def quality_passes(value, max_error):
    fields = ("cosine", "max_abs", "actual_norm")
    if not isinstance(value, dict) or any(
        isinstance(value.get(k), bool) or not isinstance(value.get(k), (int, float))
        or not math.isfinite(value[k]) for k in fields
    ):
        return False
    return (0.9999 <= value["cosine"] <= 1.0000001 and 0 <= value["max_abs"] <= max_error
            and abs(value["actual_norm"] - 1) <= 1e-4)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--mode", choices=("upstream", "no-regression"), default="upstream")
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--reference", type=Path)
    inputs.add_argument("--baseline", type=Path)
    parser.add_argument("--quality-reference", type=Path, help="Actual Torch CUDA BF16 receipt required by BF16 no-regression mode")
    parser.add_argument("--boundary", choices=("request", "encoder"), default="request")
    parser.add_argument("--limit", type=float, default=1.10)
    parser.add_argument("--median-limit", type=float, default=1.05)
    parser.add_argument("--p95-limit", type=float, default=1.10)
    parser.add_argument("--minimum-samples", type=int, default=10)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "upstream" and args.reference is None:
        parser.error("upstream mode requires --reference")
    if args.mode == "no-regression" and args.baseline is None:
        parser.error("no-regression mode requires --baseline")
    if args.mode == "upstream" and args.quality_reference is not None:
        parser.error("--quality-reference belongs to no-regression mode")
    other = args.reference if args.mode == "upstream" else args.baseline
    candidate, baseline = json.loads(args.candidate.read_text()), json.loads(other.read_text())
    if args.mode == "upstream":
        result = evaluate(candidate, baseline, boundary=args.boundary,
                          limit=args.limit, minimum_samples=args.minimum_samples)
    else:
        try:
            result = evaluate_no_regression(candidate, baseline, boundary=args.boundary,
                        median_limit=args.median_limit, p95_limit=args.p95_limit,
                        minimum_samples=args.minimum_samples,
                        quality_reference=json.loads(args.quality_reference.read_text()) if args.quality_reference else None,
                        quality_reference_sha256=reference.sha256(args.quality_reference) if args.quality_reference else None)
        except (ValueError, KeyError, TypeError) as error:
            result = dict(version=1, mode="no-regression", status="blocked",
                          reason=str(error), cases=[], production_qualified=False)
    result["inputs"] = {"candidate": reference.sha256(args.candidate),
                        "reference" if args.mode == "upstream" else "baseline": reference.sha256(other)}
    if args.quality_reference:
        result["inputs"]["quality_reference"] = reference.sha256(args.quality_reference)
    compare.publish(args.output, result)
    for row in result["cases"]:
        comparator = "baseline" if args.mode == "no-regression" else "reference"
        print(f"{row['case']}: {row['median_ratio']:.3f}x {comparator}, {row['status']}")
    raise SystemExit(0 if result["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
