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

import json
import unittest

import stored_validation_trace as trace


def fixture(count=36):
    coordinates = [
        {"owner_layer": (4, 9, 14)[i // 12], "head": 0, "span": i % 12,
         "logical_start": 512 * (1 + i % 12), "logical_count": 512,
         "retained_rows": 352 if i < 12 else 288, "head_dim": 512}
        for i in range(count)
    ]
    value = {"schema_name": trace.SCHEMA, "attempted_count": count,
             "report_count": count, "accepted_count": count, "overflowed": False,
             "entries": [trace.unused_entry() for _ in range(trace.CAPACITY)]}
    for coordinate, entry in zip(coordinates, value["entries"]):
        entry.update(coordinate=coordinate, disposition="accepted", accepted=True,
                     failure="none", report_present=True, expected_validation_groups=4)
        entry["report"].update(evaluated=7, validation_groups=4)
        for name in trace.METRICS:
            entry["report"][name] = 0.02
            entry["report"][name + "_bits"] = trace.float32_bits(0.02, name)
        for name, tolerance in (("mass", 0.05), ("output", 0.10)):
            entry[name + "_tolerance"] = tolerance
            entry[name + "_tolerance_bits"] = trace.float32_bits(tolerance, name)
    return value, coordinates


def validate(value, coordinates, **kwargs):
    return trace.validate_trace(value, coordinates, mass_tolerance=0.05, output_tolerance=0.10,
                                expected_validation_groups=[4] * len(coordinates),
                                status_readback_bytes=value["report_count"] * 32, **kwargs)


class StoredValidationTraceTests(unittest.TestCase):
    def test_all36_success_preserves_exact_float32_authority(self):
        value, coordinates = fixture()
        summary = validate(json.loads(json.dumps(value, allow_nan=False)), coordinates, require_success=True)
        self.assertTrue(summary["all_numerical_reports_accepted"])
        self.assertEqual(summary["status_readback_bytes"], 1152)
        metric = summary["entries"][0]["metrics"]["mass_relative_max_error"]
        expected = trace.decode_float32(trace.float32_bits(0.05, "mass"), "mass") - trace.decode_float32(
            trace.float32_bits(0.02, "observed"), "observed")
        self.assertEqual(metric["margin"], expected)
        self.assertFalse(summary["release_authority"])

    def test_quality_rejection_keeps_failed_coordinate_and_negative_margin(self):
        value, coordinates = fixture()
        value.update(attempted_count=2, report_count=2, accepted_count=1)
        value["entries"][2:] = [trace.unused_entry() for _ in range(34)]
        entry = value["entries"][1]
        entry.update(disposition="rejected_with_report", accepted=False, failure="quality_rejected")
        entry["report"].update(failure_flags=2, mass_relative_max_error=0.07,
                               mass_relative_max_error_bits=trace.float32_bits(0.07, "mass"))
        summary = validate(value, coordinates)
        self.assertFalse(summary["all_numerical_reports_accepted"])
        self.assertEqual(summary["entries"][-1]["coordinate"], coordinates[1])
        self.assertLess(summary["entries"][-1]["metrics"]["mass_relative_max_error"]["margin"], 0)
        with self.assertRaisesRegex(ValueError, "incomplete numerical success"):
            validate(value, coordinates, require_success=True)

    def test_pre_report_failure_cannot_inherit_previous_report(self):
        value, coordinates = fixture(2)
        value.update(report_count=1, accepted_count=1)
        entry = value["entries"][1]
        entry.update(disposition="failed_before_report", accepted=False, failure="other", report_present=False)
        with self.assertRaisesRegex(ValueError, "stale data"):
            validate(value, coordinates)
        entry["report"] = trace.unused_entry()["report"]
        self.assertEqual(validate(value, coordinates)["entries"][-1]["metrics"], {})

    def test_nonfinite_payload_bits_survive_strict_json_without_fabricated_margin(self):
        for bits in (0x7FC01234, 0x7F800000, 0xFF800000):
            with self.subTest(bits=bits):
                value, coordinates = fixture(1)
                value["accepted_count"] = 0
                entry = value["entries"][0]
                entry.update(disposition="rejected_with_report", accepted=False, failure="non_finite_input")
                entry["report"].update(failure_flags=1, mass_relative_max_error_bits=bits,
                                       mass_relative_max_error=None)
                result = validate(json.loads(json.dumps(value, allow_nan=False)), coordinates)
                metric = result["entries"][0]["metrics"]["mass_relative_max_error"]
                self.assertEqual(metric["bits"], bits)
                self.assertFalse(metric["finite"])
                self.assertIsNone(metric["observed"])
                self.assertIsNone(metric["margin"])

    def test_unevaluated_zero_is_not_reported_as_perfect_accuracy(self):
        value, coordinates = fixture(1)
        value["accepted_count"] = 0
        entry = value["entries"][0]
        entry.update(disposition="rejected_with_report", accepted=False, failure="quality_rejected")
        entry["report"].update(failure_flags=2, evaluated=1)
        result = validate(value, coordinates)
        self.assertIsNone(result["entries"][0]["metrics"]["output_relative_rmse"]["observed"])

    def test_wrong_coordinate_order_and_stale_padding_rejected(self):
        value, coordinates = fixture(2)
        value["entries"][0]["coordinate"], value["entries"][1]["coordinate"] = (
            value["entries"][1]["coordinate"], value["entries"][0]["coordinate"])
        with self.assertRaisesRegex(ValueError, "coordinate order"):
            validate(value, coordinates)
        value, coordinates = fixture(1)
        value["entries"][1]["report"]["failure_flags"] = 2
        with self.assertRaisesRegex(ValueError, "unused entry"):
            validate(value, coordinates)

    def test_counters_and_measured_readback_must_agree(self):
        value, coordinates = fixture(2)
        with self.assertRaisesRegex(ValueError, "readback"):
            trace.validate_trace(value, coordinates, mass_tolerance=0.05, output_tolerance=0.10,
                                 expected_validation_groups=[4] * len(coordinates),
                                 status_readback_bytes=32)
        value["accepted_count"] = 1
        with self.assertRaisesRegex(ValueError, "counters disagree"):
            validate(value, coordinates)

    def test_float_mirror_and_threshold_splicing_rejected(self):
        value, coordinates = fixture(1)
        value["entries"][0]["report"]["mass_relative_max_error"] = 0.01
        with self.assertRaisesRegex(ValueError, "mirror differs"):
            validate(value, coordinates)
        value, coordinates = fixture(1)
        value["entries"][0]["mass_tolerance_bits"] = trace.float32_bits(0.10, "changed")
        with self.assertRaisesRegex(ValueError, "tolerance changed"):
            validate(value, coordinates)

    def test_boolean_u32_alias_and_overflow_rejected(self):
        for field in ("attempted_count", "report_count", "accepted_count"):
            value, coordinates = fixture(1)
            value[field] = True
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate(value, coordinates)
        value, coordinates = fixture(1)
        value["overflowed"] = True
        with self.assertRaises(ValueError):
            validate(value, coordinates)

    def test_success_cannot_hide_unevaluated_or_over_threshold_metrics(self):
        value, coordinates = fixture(1)
        value["entries"][0]["report"]["evaluated"] = 3
        with self.assertRaisesRegex(ValueError, "accepted report status"):
            validate(value, coordinates)
        value, coordinates = fixture(1)
        report = value["entries"][0]["report"]
        report["output_relative_rmse"] = 0.12
        report["output_relative_rmse_bits"] = trace.float32_bits(0.12, "output")
        with self.assertRaisesRegex(ValueError, "exceeds thresholds"):
            validate(value, coordinates)

    def test_exact_group_count_and_native_invalid_report_predicate(self):
        value, coordinates = fixture(1)
        value["entries"][0]["report"]["validation_groups"] = 3
        with self.assertRaisesRegex(ValueError, "accepted report status"):
            validate(value, coordinates)
        value["accepted_count"] = 0
        value["entries"][0].update(disposition="rejected_with_report", accepted=False, failure="invalid_report")
        self.assertTrue(validate(value, coordinates)["trace_valid"])
        value["entries"][0]["report"]["validation_groups"] = 4
        with self.assertRaisesRegex(ValueError, "contradicts native predicates"):
            validate(value, coordinates)
        for groups in ([True], [0], [], [4, 4]):
            with self.subTest(groups=groups), self.assertRaises(ValueError):
                trace.validate_trace(value, coordinates, mass_tolerance=0.05, output_tolerance=0.10,
                                     expected_validation_groups=groups, status_readback_bytes=32)

    def test_native_group_expectation_is_bound_to_independent_reservation(self):
        for groups in (True, 0, 3, 0x100000000):
            value, coordinates = fixture(1)
            value["entries"][0]["expected_validation_groups"] = groups
            with self.subTest(groups=groups), self.assertRaises(ValueError):
                validate(value, coordinates)
        value, coordinates = fixture(1)
        with self.assertRaisesRegex(ValueError, "differ from reservation"):
            trace.validate_trace(value, coordinates, mass_tolerance=0.05, output_tolerance=0.10,
                                 expected_validation_groups=[3], status_readback_bytes=32)
        value["entries"][1]["expected_validation_groups"] = 4
        with self.assertRaisesRegex(ValueError, "unused entry"):
            validate(value, coordinates)


if __name__ == "__main__":
    unittest.main()
