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
from qualify_cpu_media import validate_read


class CompletionAssertions(unittest.TestCase):
    def test_success_requires_complete_output_and_known_answer(self):
        for response in (
            {},
            {"data": []},
            {"data": [{"text": "last phrase"}]},
            {"data": [{"status": "partial", "text": "last phrase"}]},
            {
                "data": [
                    {"status": "complete", "text": "last phrase", "error": "provider"}
                ]
            },
            {
                "data": [
                    {"status": "complete", "text": "last phrase", "truncated": True}
                ]
            },
            {
                "data": [
                    {
                        "status": "complete",
                        "text": "last phrase",
                        "finish_reason": "length",
                    }
                ]
            },
            {"data": [{"status": "complete", "text": "first window only"}]},
        ):
            with self.subTest(response=response), self.assertRaises(ValueError):
                validate_read(response, ["last phrase"])
        result = {
            "data": [
                {"status": "complete", "text": "FIRST window"},
                {"status": "complete", "text": "last\nphrase"},
            ]
        }
        self.assertEqual(
            "first window last phrase",
            validate_read(result, ["first window", "last phrase"]),
        )
        with self.assertRaises(ValueError):
            validate_read(result, [])


if __name__ == "__main__":
    unittest.main()
