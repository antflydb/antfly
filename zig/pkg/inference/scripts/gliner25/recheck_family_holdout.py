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

"""Re-evaluate a pinned capture under the explicit original-source span policy.

No inference or timing is performed. The report identifies the original binary
and immutable captures, retains strict comparisons, and enumerates every
excluded prediction. This does not grant release qualification.
"""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path

from check_family_holdout import compare_rows, extraction_coverage, slice_summary
from check_family_reference_holdout import pinned_bytes
from family_oracle import FIXTURES
from source_span_policy import original_text_reference


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    report_bytes = pinned_bytes(args.report, manifest)
    baseline = json.loads(report_bytes)
    task = baseline.get("task")
    if (
        baseline["status"] != "complete"
        or task not in ("entities", "structured")
        or baseline["model"] != manifest["models"]["multi"]
    ):
        raise ValueError("expected complete Multi extraction capture")
    if baseline.get("source_span_policy", "strict") != "strict":
        raise ValueError("expected the original strict comparison")
    reference = baseline["reference"]
    if (
        reference["dtype"] != "fp32"
        or reference.get("weight_dtype", "fp32") != "fp32"
        or reference["model_files"] != baseline["model"]["files"]
    ):
        raise ValueError("expected pinned FP32 oracle")
    capture = baseline["raw_capture"]
    if (
        capture.get("encoding", "gzip" if capture["path"].endswith(".gz") else None)
        != "gzip"
    ):
        raise ValueError("expected compressed pinned capture")
    capture_path = args.report.parent / capture["path"]
    data = gzip.decompress(pinned_bytes(capture_path, manifest))
    if hashlib.sha256(data).hexdigest() != capture["uncompressed_sha256"]:
        raise ValueError("capture hash mismatch")
    source = json.loads(pinned_bytes(FIXTURES / "holdout/source.json", manifest))
    if source != baseline["dataset"]:
        raise ValueError("dataset mismatch")
    captured = {}
    for line in data.splitlines():
        row = json.loads(line)
        key = row["locale"], row["case"]
        if key in captured:
            raise ValueError("duplicate capture")
        captured[key] = row
    report = dict(
        baseline,
        slices=[],
        source_span_policy="original_text",
        qualification=False,
        strict_report=str(args.report.resolve().relative_to(FIXTURES.resolve())),
        strict_report_sha256=hashlib.sha256(report_bytes).hexdigest(),
        comparison_sources={
            name: hashlib.sha256(
                Path(__file__).with_name(name).read_bytes()
            ).hexdigest()
            for name in (
                "recheck_family_holdout.py",
                "check_family_holdout.py",
                "check_family_decisions.py",
                "source_span_policy.py",
            )
        },
    )
    report.pop("failure", None)
    report.pop("disposition", None)
    report["disposition"] = (
        "Re-evaluated under the approved original-text span policy. Only predictions containing Fastino's appended terminal period are excluded; strict results and every excluded prediction remain recorded. No release qualification is granted."
    )
    report["raw_capture"] = dict(
        capture,
        encoding="gzip",
        path=os.path.relpath(capture_path.resolve(), args.output.resolve()),
    )
    schema = baseline.get("extraction_schema") or dict(
        entities=baseline["entity_types"]
    )
    visited = set()
    for filename in source["files"]:
        locale = filename.split("/")[0]
        fixture_bytes = pinned_bytes(
            FIXTURES / "holdout/cases" / (locale + ".json"), manifest
        )
        fixture = json.loads(fixture_bytes)
        rows = []
        coverage = dict(entities=0, attributes=0, relations=0, records=0)
        for case in fixture["cases"]:
            key = locale, case["id"]
            visited.add(key)
            captured_row = captured[key]
            request = dict(
                id=case["id"],
                texts=case["texts"],
                source_ids=case["source_ids"],
                schema=schema,
            )
            rows += compare_rows(
                request,
                captured_row["native"],
                captured_row["reference"],
                "multi",
                5e-4 if baseline["precision"] == "fp32" else 5e-3,
                "original_text",
            )
            valid = [
                original_text_reference(text, output)[0]
                for text, output in zip(
                    case["texts"], captured_row["reference"]["outputs"], strict=True
                )
            ]
            for head, count in extraction_coverage(valid).items():
                coverage[head] += count
        report["slices"].append(
            dict(
                locale=locale,
                fixture_sha256=hashlib.sha256(fixture_bytes).hexdigest(),
                **slice_summary(rows),
                rows=rows,
                strict_comparison=slice_summary([r["strict_comparison"] for r in rows]),
                reference_prediction_counts=coverage,
                excluded_reference_predictions=sum(
                    len(r["reference_exclusions"]) for r in rows
                ),
            )
        )
    if visited != captured.keys():
        raise ValueError("capture does not match complete public holdout")
    counts = {
        head: sum(s["reference_prediction_counts"][head] for s in report["slices"])
        for head in coverage
    }
    report["reference_prediction_counts"] = counts
    if task == "structured":
        report["all_heads_exercised"] = all(counts.values())
    report["measured_slices_passed"] = all(s["passed"] for s in report["slices"]) and (
        task != "structured" or all(counts.values())
    )
    report["strict_slices_passed"] = all(
        s["strict_comparison"]["passed"] for s in report["slices"]
    )
    report["excluded_reference_predictions"] = sum(
        s["excluded_reference_predictions"] for s in report["slices"]
    )
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    )
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "measured_slices_passed",
                    "strict_slices_passed",
                    "excluded_reference_predictions",
                )
            }
        )
    )
    if not report["measured_slices_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
