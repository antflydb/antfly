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

"""Evidence contract for atomic source-cohort refresh comparisons."""
import unittest
from source_cohort_batch import parse_metrics


class CohortBatchMetricsTests(unittest.TestCase):
    record = "SOURCE_COHORT_BATCH documents=131 payload=0 sequential=0 cohorts=2 prepare_ns=1 publish_ns=2 verify_ns=3 barriers=1 verified=5 physical_shared=1 production_allocator=1 optimize_mode=2"

    def test_accepts_both_verified_publication_paths(self):
        self.assertEqual(parse_metrics(self.record, 131, 0, False)["barriers"], 1)
        sequential = self.record.replace("sequential=0", "sequential=1").replace("barriers=1", "barriers=2")
        self.assertEqual(parse_metrics(sequential, 131, 0, True)["barriers"], 2)

    def test_rejects_wrong_workload_or_missing_verification(self):
        for old, new in (("documents=131", "documents=4099"), ("payload=0", "payload=4096"),
                         ("verified=5", "verified=4"), ("physical_shared=1", "physical_shared=0"),
                         ("cohorts=2", "cohorts=1"), ("barriers=1", "barriers=2"),
                         ("production_allocator=1", "production_allocator=0"), ("optimize_mode=2", "optimize_mode=10"),
                         ("publish_ns=2", "")):
            with self.subTest(old=old), self.assertRaises(RuntimeError):
                parse_metrics(self.record.replace(old, new), 131, 0, False)

    def test_rejects_duplicate_truncated_and_multiple_records(self):
        for bad in ("", self.record + " prepare_ns=1", self.record + "\n" + self.record,
                    self.record.replace("publish_ns=2", "publish_ns=2ms")):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                parse_metrics(bad, 131, 0, False)


if __name__ == "__main__":
    unittest.main()
