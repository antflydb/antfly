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

import json
import tempfile
import unittest
from pathlib import Path

import quality_corpus_pack_advance_v8_tool_history as advance


class DriverErrorTests(unittest.TestCase):
    @staticmethod
    def report(**updates):
        value = {"schema_version": 2, "completed": False, "request_index": 7,
                 "error": "ModelNotLoaded"}
        value.update(updates)
        return value

    def test_preserves_exact_structured_driver_error_before_raising(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.jsonl"
            with path.open("x", encoding="utf-8", newline="\n") as stream:
                with self.assertRaisesRegex(advance.MaterializationDriverError,
                                            "materialization driver error: ModelNotLoaded"):
                    advance._reject_driver_error(stream, self.report(), 7, 2, "case-2", 1)
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(rows, [{
                "schema_version": 1,
                "stage": "quality_loaded_tokenizer_driver_failure",
                "completed": False,
                "case_index": 2,
                "case_id": "case-2",
                "iteration": 1,
                "request_index": 7,
                "driver_report": self.report(),
            }])

    def test_success_continues_and_malformed_error_envelopes_reject(self):
        self.assertIsNone(advance._driver_error_report(
            {"schema_version": 2, "completed": True, "request_index": 7}, 7))
        bad = [
            self.report(request_index=8),
            dict(self.report(), detail="untrusted"),
            self.report(completed=0),
            self.report(schema_version=True),
            self.report(error="Model Not Loaded"),
        ]
        for report in bad:
            with self.subTest(report=report):
                with self.assertRaises(ValueError):
                    advance._driver_error_report(report, 7)


if __name__ == "__main__":
    unittest.main()
