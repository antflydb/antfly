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

from summarize_dense_delete_experiment import mixed_apply_work


def event(sequence, documents=100):
    return (
        "antfly_bench_derived_worker index=vec kind=dense_vector "
        f"sequence={sequence} overwritten={documents} documents={documents} "
        "total_ms=80 dense_delete_ms=60 dense_embedding_apply_ms=20"
    )


class MixedApplyTests(unittest.TestCase):
    def test_excludes_fresh_load_and_later_churn(self):
        result = mixed_apply_work([event(1, 0), event(2), event(3), event(4)], 3)
        self.assertEqual(result["windows"], 2)
        self.assertEqual(result["replayed_documents"], 200)
        self.assertEqual(result["apply_ms_per_1000_replayed_documents"], 800)

    def test_retries_and_missing_observations_are_not_speedups(self):
        for lines in ([], [event(2), event(2)], [event(3)]):
            with self.assertRaises(ValueError):
                mixed_apply_work(lines, 2)

    def test_invalid_timers_and_counts_fail_closed(self):
        for line in (
            event(1).replace("total_ms=80", "total_ms=nan"),
            event(1).replace("total_ms=80", "total_ms=-1"),
            event(1, -1),
            event(0),
        ):
            with self.assertRaises(ValueError):
                mixed_apply_work([line], 2)


if __name__ == "__main__":
    unittest.main()
