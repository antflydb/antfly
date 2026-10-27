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

"""Contract tests for native replay benchmark evidence."""
import unittest

from source_replay import PHASES, paired_comparisons, parse_metrics


class ReplayMetricsTests(unittest.TestCase):
    record = "SOURCE_REPLAY documents=65 family=1 collect_ns=10 prepare_ns=20 publish_ns=30 read_ns=40 verified=65 production_allocator=1 optimize_mode=2"

    def test_validates_and_totals_phases(self):
        self.assertEqual(parse_metrics(self.record, 65, True)["write_ns"], 60)

    def test_rejects_wrong_workload_or_missing_verification(self):
        for old, new in (("family=1", "family=0"), ("verified=65", "verified=64"),
                         ("production_allocator=1", "production_allocator=0"),
                         ("documents=65", "documents=64"), ("optimize_mode=2", "optimize_mode=10"),
                         ("read_ns=40", "")):
            with self.subTest(old=old), self.assertRaises(RuntimeError):
                parse_metrics(self.record.replace(old, new), 65, True)

    def test_checks_sparse_selection_and_reader_policy(self):
        sparse = self.record.replace("verified=65", "verified=10") + " limit=10 forced_snapshot=0 probe_reads=1"
        self.assertEqual(parse_metrics(sparse, 65, True, 10)["verified"], 10)
        forced = sparse.replace("forced_snapshot=0", "forced_snapshot=1").replace("probe_reads=1", "probe_reads=0")
        self.assertEqual(parse_metrics(forced, 65, True, 10, True)["probe_reads"], 0)
        declined = sparse.replace("probe_reads=1", "probe_reads=0") + " point_eligible=0"
        self.assertEqual(parse_metrics(declined, 65, True, 10)["probe_reads"], 0)
        for bad in (sparse.replace("probe_reads=1", "probe_reads=0"), sparse.replace("limit=10", "limit=11")):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                parse_metrics(bad, 65, True, 10)

    def test_rejects_duplicate_malformed_and_multiple_records(self):
        for bad in (self.record + " collect_ns=20", self.record.replace("read_ns=40", "read_ns=-1"),
                    self.record + "\n" + self.record, ""):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                parse_metrics(bad, 65, True)


class ReplayComparisonTests(unittest.TestCase):
    def rows(self):
        return [dict(binary=label, documents=65, family=1, trial=trial,
                     **{phase: value for phase in (*PHASES, "write_ns", "cpu_seconds", "peak_rss_bytes")})
                for trial, values in enumerate(((10, 20), (30, 20), (10, 20)))
                for label, value in zip(("current", "snapshot"), values)]

    def test_pairs_trials_and_exposes_regression_variation(self):
        result = paired_comparisons(list(reversed(self.rows())), ["current", "snapshot"])[0]
        self.assertEqual(result["pairs"], 3)
        self.assertEqual(result["current_wins"], 2)
        self.assertEqual(result["median_ratio"], 0.5)
        self.assertEqual(result["max_ratio"], 1.5)
        self.assertEqual(result["median_delta"], -10)

    def test_rejects_duplicate_or_incomplete_trials(self):
        rows = self.rows()
        for invalid in (rows + rows[:1], rows[:-1]):
            with self.assertRaises(RuntimeError):
                paired_comparisons(invalid, ["current", "snapshot"])

    def test_zero_reference_is_visible_without_division(self):
        rows = self.rows()[:2]
        rows[1]["read_ns"] = 0
        result = next(item for item in paired_comparisons(rows, ["current", "snapshot"]) if item["metric"] == "read_ns")
        self.assertEqual(result["ratio_pairs"], 0)
        self.assertIsNone(result["median_ratio"])


if __name__ == "__main__":
    unittest.main()
