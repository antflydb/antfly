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

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "eval_extract", Path(__file__).parents[1] / "eval_extract.py"
)
eval_extract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(eval_extract)


class EvalExtractTests(unittest.TestCase):
    def test_exact_metrics_include_negatives_and_duplicate_deduplication(self):
        report = eval_extract.metrics(
            [{("p", 0, 3)}, set()], [{("p", 0, 3), ("p", 4, 7)}, {("p", 0, 3)}]
        )
        self.assertEqual(report["micro"]["tp"], 1)
        self.assertEqual(report["micro"]["fp"], 2)
        self.assertEqual(report["micro"]["precision"], 1 / 3)
        self.assertEqual(report["micro"]["recall"], 1)
        self.assertIsNone(eval_extract.metrics([set()], [set()])["micro"]["precision"])
        empty = eval_extract.metrics([set()], [set()], ["person"])
        self.assertEqual(empty["per_label"]["person"]["mentions"], 0)
        with self.assertRaises(ValueError):
            eval_extract.metrics([set()], [])

    def test_unicode_validation_and_real_wire_response(self):
        class Inference:
            def extract(self, request):
                self.request = request
                return {
                    "data": [
                        {
                            "id": "1",
                            "offset_unit": "unicode_codepoints",
                            "entities": [
                                {"label": "person", "start": 1, "end": 4, "text": "Ada"}
                            ],
                        }
                    ]
                }

        inference = Inference()
        config = {
            "schema_version": 2,
            "model": "test",
            "schema": {"entities": ["person"]},
        }
        result = eval_extract.evaluate(
            inference,
            config,
            [
                {
                    "id": "1",
                    "text": "😀Ada",
                    "entities": [{"label": "person", "start": 1, "end": 4}],
                }
            ],
        )
        self.assertEqual(result["metrics"]["micro"]["precision"], 1)
        self.assertNotIn("options", config)
        self.assertEqual(
            inference.request["options"]["offset_unit"], "unicode_codepoints"
        )
        with self.assertRaises(ValueError):
            eval_extract.spans([{"label": "person", "start": 4, "end": 7}], "😀Ada")


if __name__ == "__main__":
    unittest.main()
