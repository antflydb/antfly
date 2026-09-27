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

import unittest

from run_dense_subgroup_scan_bench import summarize


class ScanReceiptTest(unittest.TestCase):
    def rows(self):
        return [
            {
                "round": r,
                "warmup": r == 0,
                "mode": mode,
                "checksum": 17 if mode == "control" else 42,
                "routing_ns": 1,
                "selection_ns": 2,
                "scan_ns": 3,
                "vectors_per_query": 75,
                "frontier_vectors_per_query": 100,
            }
            for mode in (
                "control",
                "sort_predicate",
                "partition_predicate",
                "partition_ranges",
            )
            for r in range(6)
        ]

    def test_complete_matching_receipt(self):
        self.assertEqual(summarize(self.rows())["partition_ranges"]["total_ns"], 6)

    def test_missing_round_and_candidate_mismatch_fail(self):
        with self.assertRaises(ValueError):
            summarize(self.rows()[:-1])
        rows = self.rows()
        rows[-1]["checksum"] = 43
        with self.assertRaises(ValueError):
            summarize(rows)


if __name__ == "__main__":
    unittest.main()
