#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

import json
import sys
import tempfile
from pathlib import Path


EVIDENCE_CONTRACT = "antfly.quant_kernel_metal_evidence.v1"
SUMMARY_SCHEMA = "antfly.quant_kernel_metal_bench_summary.v3"

LINEAR_REDUCE_BUCKETS = {
    "q4_0_linear_reduce": (
        "q4_0_linear_reduce_rows_1",
        "q4_0_linear_reduce_rows_2_8",
        "q4_0_linear_reduce_rows_9_64",
        "q4_0_linear_reduce_rows_65_plus",
    ),
    "q4_linear_reduce": (
        "q4_linear_reduce_rows_1",
        "q4_linear_reduce_rows_2_8",
        "q4_linear_reduce_rows_9_64",
        "q4_linear_reduce_rows_65_plus",
    ),
    "q6_linear_reduce": (
        "q6_linear_reduce_rows_1",
        "q6_linear_reduce_rows_2_8",
        "q6_linear_reduce_rows_9_64",
        "q6_linear_reduce_rows_65_plus",
    ),
}

STORED_TOTALS = {
    "q4_0_linear_reduce": "q4_0_linear_reduce_row_total",
    "q4_linear_reduce": "q4_linear_reduce_row_total",
    "q6_linear_reduce": "q6_linear_reduce_row_total",
}

FALLBACK_FIELDS = (
    ("generated_artifact_missing", "quant_plan_generated_artifact_missing"),
    ("generated_runtime_not_wired", "quant_plan_generated_runtime_not_wired"),
    ("unsupported_format", "quant_plan_unsupported_format"),
    ("unsupported_shape", "quant_plan_unsupported_shape"),
    ("unsupported_epilogue", "quant_plan_unsupported_epilogue"),
    ("unsupported_backend", "quant_plan_unsupported_backend"),
    ("tensor_core_repack_required", "quant_plan_tensor_core_repack_required"),
)

QUANT_PLAN_TOTAL_FIELDS = (
    "quant_plan_planned",
    "quant_plan_handwritten_production",
    "quant_plan_generated_production",
    "quant_plan_unsupported_routes",
    "quant_plan_generated_candidates",
    "quant_plan_generated_artifact_missing",
    "quant_plan_generated_runtime_not_wired",
    "quant_plan_unsupported",
    "quant_plan_unsupported_format",
    "quant_plan_unsupported_shape",
    "quant_plan_unsupported_epilogue",
    "quant_plan_unsupported_backend",
    "quant_plan_tensor_core_repack_required",
    "quant_mapped_failures",
)

RUNTIME_FALLBACK_TOTAL_FIELDS = (
    ("decode_fallbacks", "decode_fallback"),
    ("command_operator_fallbacks", "command_operator_fallback"),
    ("prefill_execute_failures", "prefill_execute_fail"),
    ("quant_mapped_failures", "quant_mapped_failures"),
)


def fail(message):
    raise SystemExit(message)


def int_field(row, key, path, label):
    if key not in row:
        fail(f"{path}: row {label}: missing {key}")
    value = row[key]
    if isinstance(value, bool) or not isinstance(value, int):
        fail(f"{path}: row {label}: {key} must be an integer, got {value!r}")
    return value


def string_field(row, key, path, label):
    if key not in row:
        fail(f"{path}: row {label}: missing {key}")
    value = row[key]
    if not isinstance(value, str):
        fail(f"{path}: row {label}: {key} must be a string, got {value!r}")
    return value


def bool_field(row, key, path, label):
    if key not in row:
        fail(f"{path}: row {label}: missing {key}")
    value = row[key]
    if not isinstance(value, bool):
        fail(f"{path}: row {label}: {key} must be a boolean, got {value!r}")
    return value


def measured_rows(summary, path):
    if summary.get("evidence_contract") != EVIDENCE_CONTRACT:
        fail(f"{path}: missing or unsupported evidence_contract")
    if summary.get("schema") != SUMMARY_SCHEMA:
        fail(f"{path}: missing or unsupported schema")
    rows = summary.get("rows")
    if not isinstance(rows, list):
        fail(f"{path}: summary rows must be a list")
    measured = []
    for row in rows:
        if not isinstance(row, dict):
            fail(f"{path}: summary row must be an object")
        label = row.get("label", "")
        if isinstance(label, str) and label.startswith("run-"):
            measured.append(row)
    if not measured:
        fail(f"{path}: summary has no measured run-* rows")
    return measured


def check_plan_counters(row, path, label):
    planned = int_field(row, "quant_plan_planned", path, label)
    handwritten = int_field(row, "quant_plan_handwritten_production", path, label)
    generated = int_field(row, "quant_plan_generated_production", path, label)
    unsupported_routes = int_field(row, "quant_plan_unsupported_routes", path, label)
    route_total = handwritten + generated + unsupported_routes
    if planned != route_total:
        fail(
            f"{path}: row {label}: quant_plan_planned={planned} "
            f"does not match route total {route_total}"
        )

    generated_candidates = int_field(row, "quant_plan_generated_candidates", path, label)
    if generated_candidates > planned:
        fail(
            f"{path}: row {label}: generated candidates {generated_candidates} "
            f"exceed planned ops {planned}"
        )

    unsupported = int_field(row, "quant_plan_unsupported", path, label)
    unsupported_subtotal = sum(
        int_field(row, key, path, label)
        for key in (
            "quant_plan_unsupported_format",
            "quant_plan_unsupported_shape",
            "quant_plan_unsupported_epilogue",
            "quant_plan_unsupported_backend",
        )
    )
    if unsupported != unsupported_subtotal:
        fail(
            f"{path}: row {label}: quant_plan_unsupported={unsupported} "
            f"does not match unsupported reason total {unsupported_subtotal}"
        )

    top_reason = string_field(row, "quant_plan_top_fallback_reason", path, label)
    top_count = int_field(row, "quant_plan_top_fallback_count", path, label)
    expected_reason = "none"
    expected_count = 0
    for reason, key in FALLBACK_FIELDS:
        count = int_field(row, key, path, label)
        if count > expected_count:
            expected_reason = reason
            expected_count = count
    if top_reason != expected_reason or top_count != expected_count:
        fail(
            f"{path}: row {label}: top fallback {top_reason}/{top_count} "
            f"does not match {expected_reason}/{expected_count}"
        )


def row_bucket_dispatches(row, path, label):
    return sum(int_field(row, STORED_TOTALS[key], path, label) for key in LINEAR_REDUCE_BUCKETS)


def quant_plan_totals(rows, path):
    totals = {key: 0 for key in QUANT_PLAN_TOTAL_FIELDS}
    totals["measured_rows"] = len(rows)
    totals["row_bucket_dispatches"] = 0
    for row in rows:
        label = row.get("label", "<unknown>")
        for key in QUANT_PLAN_TOTAL_FIELDS:
            totals[key] += int_field(row, key, path, label)
        totals["row_bucket_dispatches"] += row_bucket_dispatches(row, path, label)

    expected_reason = "none"
    expected_count = 0
    for reason, key in FALLBACK_FIELDS:
        count = totals[key]
        if count > expected_count:
            expected_reason = reason
            expected_count = count
    totals["quant_plan_top_fallback_reason"] = expected_reason
    totals["quant_plan_top_fallback_count"] = expected_count
    return totals


def check_quant_plan_totals(summary, measured, path):
    actual = summary.get("quant_plan_totals")
    if not isinstance(actual, dict):
        fail(f"{path}: missing quant_plan_totals")
    expected = quant_plan_totals(measured, path)
    for key, want in expected.items():
        got = actual.get(key)
        if got != want:
            fail(f"{path}: quant_plan_totals.{key}={got!r} does not match {want!r}")
    return expected


def runtime_fallback_totals(rows, path):
    totals = {
        "measured_rows": len(rows),
        "backend_metal_rows": 0,
        "non_metal_rows": 0,
        "timing_invalid_rows": 0,
    }
    for total_key, _ in RUNTIME_FALLBACK_TOTAL_FIELDS:
        totals[total_key] = 0

    for row in rows:
        label = row.get("label", "<unknown>")
        if string_field(row, "backend", path, label) == "metal":
            totals["backend_metal_rows"] += 1
        else:
            totals["non_metal_rows"] += 1
        if not bool_field(row, "timing_valid", path, label):
            totals["timing_invalid_rows"] += 1
        for total_key, row_key in RUNTIME_FALLBACK_TOTAL_FIELDS:
            totals[total_key] += int_field(row, row_key, path, label)
    return totals


def check_runtime_fallback_totals(summary, measured, path):
    actual = summary.get("runtime_fallback_totals")
    if not isinstance(actual, dict):
        fail(f"{path}: missing runtime_fallback_totals")
    expected = runtime_fallback_totals(measured, path)
    for key, want in expected.items():
        got = actual.get(key)
        if got != want:
            fail(f"{path}: runtime_fallback_totals.{key}={got!r} does not match {want!r}")
    return expected


def check_summary(path):
    try:
        summary = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        fail(f"{path}: invalid JSON: {err}")

    total_bucket_dispatches = 0
    checked_rows = 0
    measured = measured_rows(summary, path)
    totals = check_quant_plan_totals(summary, measured, path)
    runtime_totals = check_runtime_fallback_totals(summary, measured, path)
    for row in measured:
        label = row.get("label", "<unknown>")
        checked_rows += 1
        row_bucket_dispatches = 0
        for aggregate_key, bucket_keys in LINEAR_REDUCE_BUCKETS.items():
            aggregate = int_field(row, aggregate_key, path, label)
            bucket_total = sum(int_field(row, key, path, label) for key in bucket_keys)
            if aggregate != bucket_total:
                fail(
                    f"{path}: row {label}: {aggregate_key}={aggregate} "
                    f"does not match bucket total {bucket_total}"
                )
            stored_total_key = STORED_TOTALS[aggregate_key]
            stored_total = int_field(row, stored_total_key, path, label)
            if stored_total != bucket_total:
                fail(
                    f"{path}: row {label}: {stored_total_key}={stored_total} "
                    f"does not match bucket total {bucket_total}"
                )
            row_bucket_dispatches += bucket_total

        if row_bucket_dispatches:
            planned = int_field(row, "quant_plan_planned", path, label)
            handwritten = int_field(row, "quant_plan_handwritten_production", path, label)
            if planned < row_bucket_dispatches or handwritten < row_bucket_dispatches:
                fail(
                    f"{path}: row {label}: quant row bucket dispatches "
                    f"{row_bucket_dispatches} exceed plan counters "
                    f"planned={planned} handwritten_production={handwritten}"
                )
        check_plan_counters(row, path, label)
        total_bucket_dispatches += row_bucket_dispatches

    return checked_rows, total_bucket_dispatches, totals, runtime_totals


def write_summary(path, rows):
    path.write_text(
        json.dumps(
            {
                "evidence_contract": EVIDENCE_CONTRACT,
                "schema": SUMMARY_SCHEMA,
                "quant_plan_totals": quant_plan_totals(rows, path),
                "runtime_fallback_totals": runtime_fallback_totals(rows, path),
                "rows": rows,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def self_test():
    good_row = {
        "label": "run-1",
        "q4_0_linear_reduce": 3,
        "q4_0_linear_reduce_rows_1": 1,
        "q4_0_linear_reduce_rows_2_8": 2,
        "q4_0_linear_reduce_rows_9_64": 0,
        "q4_0_linear_reduce_rows_65_plus": 0,
        "q4_0_linear_reduce_row_total": 3,
        "q4_linear_reduce": 4,
        "q4_linear_reduce_rows_1": 0,
        "q4_linear_reduce_rows_2_8": 4,
        "q4_linear_reduce_rows_9_64": 0,
        "q4_linear_reduce_rows_65_plus": 0,
        "q4_linear_reduce_row_total": 4,
        "q6_linear_reduce": 5,
        "q6_linear_reduce_rows_1": 5,
        "q6_linear_reduce_rows_2_8": 0,
        "q6_linear_reduce_rows_9_64": 0,
        "q6_linear_reduce_rows_65_plus": 0,
        "q6_linear_reduce_row_total": 5,
        "quant_plan_planned": 12,
        "quant_plan_handwritten_production": 12,
        "quant_plan_generated_production": 0,
        "quant_plan_unsupported_routes": 0,
        "quant_plan_generated_candidates": 0,
        "quant_plan_generated_artifact_missing": 0,
        "quant_plan_generated_runtime_not_wired": 0,
        "quant_plan_unsupported": 0,
        "quant_plan_unsupported_format": 0,
        "quant_plan_unsupported_shape": 0,
        "quant_plan_unsupported_epilogue": 0,
        "quant_plan_unsupported_backend": 0,
        "quant_plan_tensor_core_repack_required": 0,
        "quant_plan_top_fallback_reason": "none",
        "quant_plan_top_fallback_count": 0,
        "quant_mapped_failures": 0,
        "backend": "metal",
        "decode_fallback": 0,
        "command_operator_fallback": 0,
        "prefill_execute_fail": 0,
        "timing_valid": True,
    }
    with tempfile.TemporaryDirectory(prefix="antfly-metal-quant-summary-check.") as tmp:
        tmp_path = Path(tmp)
        good = tmp_path / "good.json"
        write_summary(good, [good_row])
        check_summary(good)

        missing_contract = tmp_path / "missing-contract.json"
        missing_contract.write_text(json.dumps({"rows": [good_row]}) + "\n", encoding="utf-8")
        try:
            check_summary(missing_contract)
        except SystemExit as err:
            if "evidence_contract" not in str(err):
                raise
        else:
            fail("self-test expected evidence contract failure")

        missing_schema = tmp_path / "missing-schema.json"
        missing_schema.write_text(
            json.dumps({"evidence_contract": EVIDENCE_CONTRACT, "rows": [good_row]}) + "\n",
            encoding="utf-8",
        )
        try:
            check_summary(missing_schema)
        except SystemExit as err:
            if "schema" not in str(err):
                raise
        else:
            fail("self-test expected schema failure")

        mismatch = dict(good_row)
        mismatch["q4_linear_reduce_rows_2_8"] = 3
        mismatch_path = tmp_path / "mismatch.json"
        write_summary(mismatch_path, [mismatch])
        try:
            check_summary(mismatch_path)
        except SystemExit as err:
            if "does not match bucket total" not in str(err):
                raise
        else:
            fail("self-test expected bucket mismatch failure")

        missing_plan = dict(good_row)
        missing_plan["quant_plan_planned"] = 11
        missing_plan_path = tmp_path / "missing-plan.json"
        write_summary(missing_plan_path, [missing_plan])
        try:
            check_summary(missing_plan_path)
        except SystemExit as err:
            if "exceed plan counters" not in str(err):
                raise
        else:
            fail("self-test expected plan counter failure")

        bad_route_total = dict(good_row)
        bad_route_total["quant_plan_generated_production"] = 1
        bad_route_total_path = tmp_path / "bad-route-total.json"
        write_summary(bad_route_total_path, [bad_route_total])
        try:
            check_summary(bad_route_total_path)
        except SystemExit as err:
            if "route total" not in str(err):
                raise
        else:
            fail("self-test expected route total failure")

        bad_unsupported_total = dict(good_row)
        bad_unsupported_total["quant_plan_unsupported_shape"] = 1
        bad_unsupported_total_path = tmp_path / "bad-unsupported-total.json"
        write_summary(bad_unsupported_total_path, [bad_unsupported_total])
        try:
            check_summary(bad_unsupported_total_path)
        except SystemExit as err:
            if "unsupported reason total" not in str(err):
                raise
        else:
            fail("self-test expected unsupported subtotal failure")

        bad_top_fallback = dict(good_row)
        bad_top_fallback["quant_plan_top_fallback_reason"] = "unsupported_shape"
        bad_top_fallback_path = tmp_path / "bad-top-fallback.json"
        write_summary(bad_top_fallback_path, [bad_top_fallback])
        try:
            check_summary(bad_top_fallback_path)
        except SystemExit as err:
            if "top fallback" not in str(err):
                raise
        else:
            fail("self-test expected top fallback failure")

        bad_totals = tmp_path / "bad-totals.json"
        bad_totals.write_text(
            json.dumps(
                {
                    "evidence_contract": EVIDENCE_CONTRACT,
                    "schema": SUMMARY_SCHEMA,
                    "quant_plan_totals": {
                        **quant_plan_totals([good_row], bad_totals),
                        "quant_plan_planned": 11,
                    },
                    "runtime_fallback_totals": runtime_fallback_totals([good_row], bad_totals),
                    "rows": [good_row],
                }
            )
            + "\n",
            encoding="utf-8",
        )
        try:
            check_summary(bad_totals)
        except SystemExit as err:
            if "quant_plan_totals.quant_plan_planned" not in str(err):
                raise
        else:
            fail("self-test expected quant plan totals failure")

        bad_runtime_totals = tmp_path / "bad-runtime-totals.json"
        bad_runtime_totals.write_text(
            json.dumps(
                {
                    "evidence_contract": EVIDENCE_CONTRACT,
                    "schema": SUMMARY_SCHEMA,
                    "quant_plan_totals": quant_plan_totals([good_row], bad_runtime_totals),
                    "runtime_fallback_totals": {
                        **runtime_fallback_totals([good_row], bad_runtime_totals),
                        "decode_fallbacks": 1,
                    },
                    "rows": [good_row],
                }
            )
            + "\n",
            encoding="utf-8",
        )
        try:
            check_summary(bad_runtime_totals)
        except SystemExit as err:
            if "runtime_fallback_totals.decode_fallbacks" not in str(err):
                raise
        else:
            fail("self-test expected runtime fallback totals failure")

    print("metal quant summary checker self-test passed")


def main(argv):
    if len(argv) == 2 and argv[1] == "--self-test":
        self_test()
        return 0
    if len(argv) < 2:
        print(f"usage: {argv[0]} [--self-test] summary.json [...]", file=sys.stderr)
        return 2
    for arg in argv[1:]:
        path = Path(arg)
        checked_rows, bucket_dispatches, totals, runtime_totals = check_summary(path)
        print(
            f"metal quant summary check ok path={path} "
            f"measured_rows={checked_rows} "
            f"planned_ops={totals['quant_plan_planned']} "
            f"handwritten={totals['quant_plan_handwritten_production']} "
            f"generated={totals['quant_plan_generated_production']} "
            f"unsupported_routes={totals['quant_plan_unsupported_routes']} "
            f"row_bucket_dispatches={bucket_dispatches} "
            f"top_fallback={totals['quant_plan_top_fallback_reason']}/{totals['quant_plan_top_fallback_count']} "
            f"runtime_fallbacks={runtime_totals['decode_fallbacks'] + runtime_totals['command_operator_fallbacks'] + runtime_totals['prefill_execute_failures']} "
            f"residency_misses={runtime_totals['quant_mapped_failures']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
