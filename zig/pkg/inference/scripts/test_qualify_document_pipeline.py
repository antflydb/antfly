#!/usr/bin/env python3
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

"""Contract tests for the OCR comparison's binary requests and parity gate."""

import json
import struct
import unittest

import qualify_document_pipeline as probe


class DocumentPipelineProbeTests(unittest.TestCase):
    def test_binary_envelope_matches_shared_wire_layout(self):
        image = b"\x89PNG\r\n\x1a\n\x00\xff"
        body = probe.envelope("reader@revision", image, 2048)
        self.assertEqual(body[:8], b"AFATT001")
        metadata_len, count, reserved = struct.unpack_from("<QII", body, 8)
        mime_len, descriptor_reserved, image_len = struct.unpack_from("<IIQ", body, 24)
        self.assertEqual((count, reserved, descriptor_reserved), (1, 0, 0))
        metadata = json.loads(body[40 : 40 + metadata_len])
        self.assertEqual(
            metadata,
            {
                "model": "reader@revision",
                "images": [{"url": "attachment:0"}],
                "max_tokens": 2048,
            },
        )
        start = 40 + metadata_len
        self.assertEqual(body[start : start + mime_len], b"image/png")
        self.assertEqual(body[start + mime_len :], image)
        self.assertEqual(image_len, len(image))

    def test_success_requires_content_and_complete_singleton(self):
        self.assertEqual(
            probe.validate(
                {"data": [{"text": "Invoice 12345\nTotal 42.00"}]}, ["12345", "42.00"]
            ),
            "invoice 12345 total 42.00",
        )
        for invalid in (
            {"error": "Unavailable"},
            {"data": []},
            {"data": [{"text": "ok"}, {"text": "extra"}]},
            {"data": [{"text": "ok", "error": "Failed"}]},
            {"data": [{"text": "ok", "status": "partial"}]},
            {"data": [{"text": "12345", "finish_reason": "length"}]},
            {"data": [{"text": " "}]},
            {"data": [{"text": "Missing invoice values"}]},
        ):
            with self.subTest(response=invalid), self.assertRaises(ValueError):
                probe.validate(invalid, ["12345"])


if __name__ == "__main__":
    unittest.main()
