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
from pathlib import Path


class ContextCaptureTests(unittest.TestCase):
    def test_pinned_windows_exclude_aggregate_logits_and_reject_drift(self):
        from prepare_decide_quant_capture import prepare_windows

        path = (
            Path(__file__).resolve().parents[2]
            / "src/extractors/testdata/knowledge_graph/decide_1b_windows.json"
        )
        data = path.read_bytes()
        cases = prepare_windows(data)
        self.assertEqual([len(case["input_ids"]) for case in cases], [55, 65, 1024])
        self.assertEqual(len({case["id"] for case in cases}), 3)
        with self.assertRaisesRegex(ValueError, "pinned upstream capture"):
            prepare_windows(data + b" ")


if __name__ == "__main__":
    unittest.main()
