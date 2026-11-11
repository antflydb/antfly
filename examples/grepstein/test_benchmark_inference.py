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

from benchmark_inference import validate


class CompletedWorkTests(unittest.TestCase):
    def test_truncated_or_failed_ocr_is_not_completed_work(self):
        for item in (
            {"text": "partial", "finish_reason": "length"},
            {"text": "partial", "status": "incomplete"},
            {"text": "", "finish_reason": "stop"},
            {"error": "INFERENCE_TIMEOUT"},
        ):
            with self.subTest(item=item), self.assertRaises(ValueError):
                validate("ocr", {"data": [item]}, 1)

    def test_required_text_and_item_count_are_checked(self):
        response = {"data": [{"text": "Invoice 40872", "finish_reason": "stop"}]}
        validate("ocr", response, 1, ["40872"])
        with self.assertRaises(ValueError):
            validate("ocr", response, 1, ["different evidence"])
        with self.assertRaises(ValueError):
            validate("ocr", response, 2)

    def test_wrong_dimension_and_nonfinite_embeddings_fail(self):
        for vector in ([0.0], [float("nan")] * 768):
            with self.assertRaises(ValueError):
                validate("embed", {"data": [{"embedding": vector}]}, 1)

    def test_empty_extraction_does_not_qualify_the_model_sample(self):
        with self.assertRaises(ValueError):
            validate("extract", {"data": [{"entities": []}]}, 1)


if __name__ == "__main__":
    unittest.main()
