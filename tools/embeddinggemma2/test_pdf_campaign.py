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

import copy
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pdf_campaign import compare_vectors, main, validate_report


class PdfCampaignTests(unittest.TestCase):
    def setUp(self):
        self.report = {
            "scope": "pipeline",
            "items": [{"document": 0, "page": 1}, {"document": 1, "page": 1}],
            "samples": [
                {
                    "gpu_pages": 1,
                    "cpu_pages": 1,
                    "pages": 2,
                    "retained_raster_bytes": 4096,
                    "seconds": 1.0,
                }
            ],
            "pixel_parity": [{"max_error": 0}],
            "output": [
                {"document": 0, "page": 1, "values": [1.0] + [0.0] * 767},
                {"document": 1, "page": 1, "values": [0.0, 1.0] + [0.0] * 766},
            ],
        }

    def test_distinct_cross_document_rows_must_stay_ordered(self):
        candidate = copy.deepcopy(self.report)
        self.assertEqual(len(compare_vectors(self.report, candidate)), 2)
        candidate["output"].reverse()
        with self.assertRaisesRegex(ValueError, "order"):
            compare_vectors(self.report, candidate)

    def test_same_identity_with_wrong_vector_fails(self):
        candidate = copy.deepcopy(self.report)
        candidate["output"][0]["values"] = candidate["output"][1]["values"]
        with self.assertRaisesRegex(ValueError, "parity"):
            compare_vectors(self.report, candidate)

    def test_missing_page_output_fails(self):
        candidate = copy.deepcopy(self.report)
        candidate["output"].pop()
        with self.assertRaisesRegex(ValueError, "page outputs"):
            validate_report(candidate)

    def test_nonfinite_and_zero_vectors_fail(self):
        for value in [float("nan"), float("inf"), 0.0]:
            candidate = copy.deepcopy(self.report)
            candidate["output"][0]["values"][0] = value
            with self.assertRaises(ValueError):
                compare_vectors(self.report, candidate)

    def test_accounting_bounds_and_pixel_gate(self):
        validate_report(self.report, self.report["items"])
        for key, value in [
            ("gpu_pages", 2),
            ("retained_raster_bytes", 257 * 1024 * 1024),
            ("seconds", float("nan")),
        ]:
            candidate = copy.deepcopy(self.report)
            candidate["samples"][0][key] = value
            with self.assertRaises(ValueError):
                validate_report(candidate)
        candidate = copy.deepcopy(self.report)
        candidate["pixel_parity"][0]["max_error"] = 2
        with self.assertRaisesRegex(ValueError, "pixel"):
            validate_report(candidate)

    def test_bad_gpu_pixels_abort_before_any_model_case(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "bench"
            binary.write_bytes(b"test executable identity")
            pdf = root / "input.pdf"
            pdf.write_bytes(b"test input identity")
            model = root / "model"
            model.mkdir()
            argv = [
                "pdf_campaign.py",
                "--binary",
                str(binary),
                "--model",
                str(model),
                "--pdf",
                str(pdf),
                "--output-dir",
                str(root / "output"),
            ]

            def run(command, **_):
                self.assertEqual(command[command.index("--scope") + 1], "render")
                self.assertEqual(command[command.index("--renderer") + 1], "gpu")
                report = copy.deepcopy(self.report)
                report["scope"] = "render"
                report["pixel_parity"][0]["max_error"] = 255
                Path(command[command.index("--output") + 1]).write_text(
                    json.dumps(report)
                )

            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch("pdf_campaign.platform.platform", return_value="test"),
                mock.patch("pdf_campaign.platform.processor", return_value="test"),
                mock.patch("pdf_campaign.subprocess.run", side_effect=run) as execute,
            ):
                with (
                    contextlib.redirect_stdout(io.StringIO()),
                    self.assertRaisesRegex(ValueError, "pixel"),
                ):
                    main()
                self.assertEqual(execute.call_count, 1)


if __name__ == "__main__":
    unittest.main()
