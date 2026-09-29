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
from unittest import mock

import prepare_laya_longcontext_teacher as teacher


class LayaLongContextTeacherTests(unittest.TestCase):
    def test_shared_prefix_leaves_every_prompt_a_token(self):
        self.assertEqual(teacher.shared_prefix_len([[1, 2, 3, 4], [1, 2, 5]]), 2)
        self.assertEqual(teacher.shared_prefix_len([[1, 2, 3], [1, 2, 3, 4]]), 2)
        self.assertEqual(teacher.shared_prefix_len([[1, 2, 3], [1, 2, 3]]), 2)
        self.assertEqual(teacher.shared_prefix_len([[7, 2], [1, 2]]), 0)
        self.assertEqual(teacher.shared_prefix_len([[1, 2, 3]]), 0)

    def test_prompt_puts_the_state_before_the_question(self):
        record = {"text": "the state", "kind": "choice", "instruction": "which?", "labels": ["a", "b"]}
        prompt = teacher.build_prompt(record)
        self.assertLess(prompt.index("the state"), prompt.index("which?"))

    def test_records_are_scored_per_case_in_input_order(self):
        records = [
            {"id": "x/1", "group_id": "x"},
            {"id": "y/1", "group_id": "y"},
            {"id": "x/2", "group_id": "x"},
            {"id": "lone"},
            {"id": "lone2"},
        ]
        calls = []

        def fake_group(model, tok, mx, cache_mod, group, share_prefix):
            calls.append([r["id"] for r in group])
            return [([0.0], [0.0], r["id"]) for r in group]

        with mock.patch.object(teacher, "score_group", fake_group):
            results = teacher.score_records(None, None, None, None, records, True)
        self.assertEqual(calls, [["x/1", "x/2"], ["y/1"], ["lone"], ["lone2"]])
        self.assertEqual([r[2] for r in results], [r["id"] for r in records])


if __name__ == "__main__":
    unittest.main()
