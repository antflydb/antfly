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

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from qualify_cpu_media import validate_read, main


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


class ReportAssertions(unittest.TestCase):
    def test_missing_checkpoint_is_an_explicit_failed_campaign_case(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, cases, report = (
                root / name for name in ("config.json", "cases.json", "report.json")
            )
            config.write_text("{}")
            cases.write_text(json.dumps([{"model": "reader", "kind": "image"}]))
            argv = [
                "probe",
                "--binary",
                __file__,
                "--config",
                str(config),
                "--models-dir",
                str(root),
                "--cases",
                str(cases),
                "--report",
                str(report),
            ]
            with (
                patch("sys.argv", argv),
                patch(
                    "qualify_cpu_media.run_case",
                    side_effect=ValueError("missing checkpoint"),
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(1, main())
            recorded = json.loads(report.read_text())["cases"]
            self.assertEqual(1, len(recorded))
            self.assertFalse(recorded[0]["success"])
            self.assertEqual("missing checkpoint", recorded[0]["error"])


if __name__ == "__main__":
    unittest.main()
