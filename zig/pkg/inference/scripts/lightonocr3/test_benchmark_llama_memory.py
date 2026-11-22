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

"""Quality gates for the llama.cpp OCR memory benchmark."""

import unittest

from benchmark_llama_memory import grounding_quality_passes, quality_passes, score_case


class QualificationTest(unittest.TestCase):
    def test_ocr_with_every_sentinel_but_high_cer_fails(self):
        case = {
            "expected": "Invoice heading, description, and balance USD 42.00",
            "required": ["42.00"],
        }
        row = {"finish": "stop", "text": "42.00"}
        score_case(row, case)
        self.assertEqual(row["missing"], [])
        self.assertFalse(quality_passes(row))

    def test_changing_one_table_row_fails_even_at_low_cer(self):
        rows = [
            "Item Reference Quantity",
            "Replacement part SKU-1201 2",
            "Replacement part SKU-1202 3",
        ]
        case = {
            "expected": "\n".join(rows),
            "required": ["SKU-1201", "SKU-1202"],
            "expected_table_rows": rows,
        }
        table = "<table><tr><td>Item</td><td>Reference</td><td>Quantity</td></tr><tr><td>Replacement part</td><td>SKU-1201</td><td>2</td></tr><tr><td>Replacement part</td><td>SKU-1202</td><td>3</td></tr></table>"
        row = {"finish": "stop", "text": table}
        score_case(row, case)
        self.assertTrue(quality_passes(row))
        row["text"] = table.replace(">3<", ">8<")
        score_case(row, case)
        self.assertLess(row["cer"], 0.05)
        self.assertEqual(row["missing"], [])
        self.assertFalse(quality_passes(row))

    def test_layout_requires_distinct_boxes_at_fixture_coverage(self):
        case = {
            "expected": "Invoice 12345",
            "required": ["12345"],
            "min_grounding_boxes": 3,
        }
        box = "![title](1,2,3,4)"
        row = {"finish": "stop", "text": box * 3 + "Invoice 12345"}
        score_case(row, case)
        self.assertFalse(grounding_quality_passes(row, case))
        row["text"] = box + "![text](5,6,7,8)![table](9,10,11,12)Invoice 12345"
        score_case(row, case)
        self.assertTrue(grounding_quality_passes(row, case))


if __name__ == "__main__":
    unittest.main()
