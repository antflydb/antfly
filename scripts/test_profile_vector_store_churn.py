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

"""Reject incomplete sync-wait evidence before accepting churn attribution."""

import unittest
from profile_vector_store_churn import parse_batch_profiles


class ProfileTests(unittest.TestCase):
    def test_exact_rows_and_fence(self):
        text = (
            "info: antfly_bench_batch sequence=1 writes=2 deletes=0 sync=write sync_wait_ms=0\n"
            "info: antfly_bench_batch sequence=2 writes=1 deletes=0 sync=full_index sync_wait_ms=31\n"
        )
        rows = parse_batch_profiles(text, 3, 2)
        self.assertEqual(sum(int(p["sync_wait_ms"]) for p in rows), 31)
        for invalid in [
            text.splitlines()[0],
            text + text,
            text.replace("full_index", "write"),
        ]:
            with self.assertRaises(RuntimeError):
                parse_batch_profiles(invalid, 3, 2)


if __name__ == "__main__":
    unittest.main()
