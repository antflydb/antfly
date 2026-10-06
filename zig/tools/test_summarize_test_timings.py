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

"""Verify timings retain attribution through CI prefixes and noisy output."""

import unittest

from tools.summarize_test_timings import parse_timings


class TimingSummaryTests(unittest.TestCase):
    def test_prefixed_records_and_repeated_executions(self):
        text = (
            "2026-09-16T00:00:00Z [db-core-category] "
            "TIMING\t1000000\t2000000000\t3000000\t4000000\tstorage.test.example\n"
            "OK\nwarning: unrelated output\n"
            "TIMING\t0\t1000000000\t0\t0\tstorage.test.example\n"
        )
        rows = parse_timings(text)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["name"], "storage.test.example")
        self.assertAlmostEqual(rows[0]["total"], 2.008)
        self.assertEqual(rows[0]["body"], 2.0)
        self.assertEqual(rows[1]["total"], 1.0)

    def test_incomplete_record_is_not_reported(self):
        self.assertEqual(parse_timings("TIMING\t1\t2\t3\t"), [])


if __name__ == "__main__":
    unittest.main()
