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

import unittest

from measure_watch_rebuild import successful_summary


class WatchSummaryTests(unittest.TestCase):
    def test_missing_summary_is_not_a_successful_timing(self):
        self.assertFalse(successful_summary("1/9 test...OK\n"))

    def test_all_steps_must_succeed(self):
        self.assertTrue(successful_summary("Build Summary: 8/8 steps succeeded\n"))
        self.assertTrue(
            successful_summary("Build Summary: 8/8 steps succeeded; 0 failed\n")
        )
        with self.assertRaises(RuntimeError):
            successful_summary("Build Summary: 7/8 steps succeeded (1 failed)\n")

    def test_failed_test_counts_are_rejected_even_when_steps_match(self):
        with self.assertRaises(RuntimeError):
            successful_summary(
                "Build Summary: 8/8 steps succeeded; 8 passed; 1 failed\n"
            )

    def test_latest_summary_controls_result(self):
        with self.assertRaises(RuntimeError):
            successful_summary(
                "Build Summary: 8/8 steps succeeded\nBuild Summary: 2/8 steps succeeded (1 failed)\n"
            )


if __name__ == "__main__":
    unittest.main()
