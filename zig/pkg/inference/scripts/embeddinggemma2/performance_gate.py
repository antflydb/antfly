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

"""Per-case CPU/PyTorch and Metal/MPS parity and latency gates.

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--boundary", choices=("request", "encoder"), default="request")
    parser.add_argument("--limit", type=float, default=1.10)
    parser.add_argument("--minimum-samples", type=int, default=10)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = evaluate(
        json.loads(args.candidate.read_text()),
        json.loads(args.reference.read_text()),
        boundary=args.boundary,
        limit=args.limit,
        minimum_samples=args.minimum_samples,
    )
    result["inputs"] = {
        "candidate": reference.sha256(args.candidate),
        "reference": reference.sha256(args.reference),
    }
    compare.publish(args.output, result)
    for row in result["cases"]:
        print(f"{row['case']}: {row['median_ratio']:.3f}x reference, {row['status']}")
    raise SystemExit(0 if result["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
