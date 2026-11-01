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

"""Fail-closed reference contracts for private OCR performance qualification."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from qualify_reader_page_latency import normalized_text, reference_expectations


class ReaderReferenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "reference.json"
        self.report = {
            "status": "ok",
            "images": [{"sha256": "fixture-hash"}],
            "references": [{"image_index": 0, "text": "Café 123!"}],
        }

    def load(self):
        self.path.write_text(json.dumps(self.report))
        return reference_expectations(self.path)

    def test_success_retains_exact_and_unicode_normalized_identity(self):
        exact, whitespace, normalized = self.load()["fixture-hash"]
        self.assertEqual(exact, hashlib.sha256("Café 123!".encode()).hexdigest())
        self.assertEqual(whitespace, hashlib.sha256("Café123!".encode()).hexdigest())
        self.assertEqual(normalized, hashlib.sha256("café123".encode()).hexdigest())
        self.assertEqual(normalized_text("CAFÉ 123!"), "café123")
        self.assertNotEqual(exact, normalized)

    def test_failed_or_empty_reference_cannot_qualify(self):
        self.report["status"] = "failed"
        with self.assertRaisesRegex(ValueError, "successful completed"):
            self.load()
        self.report["status"] = "ok"
        self.report["references"] = []
        with self.assertRaisesRegex(ValueError, "successful completed"):
            self.load()

    def test_conflicting_output_for_the_same_image_is_rejected(self):
        self.report["references"].append({"image_index": 0, "text": "different"})
        with self.assertRaisesRegex(ValueError, "conflicting outputs"):
            self.load()

    def test_reference_cannot_select_a_different_image_with_negative_index(self):
        for index in (-1, 1, True):
            with self.subTest(index=index):
                self.report["references"][0]["image_index"] = index
                with self.assertRaisesRegex(ValueError, "invalid image index"):
                    self.load()


if __name__ == "__main__":
    unittest.main()
