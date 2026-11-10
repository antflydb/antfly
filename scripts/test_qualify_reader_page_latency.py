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

from qualify_reader_page_latency import (
    image_request_payload,
    normalized_text,
    qualified_request,
    reference_expectations,
    response_texts_and_statuses,
)


class ReaderWireTests(unittest.TestCase):
    def test_chat_reference_uses_the_same_image_only_greedy_workload(self):
        body = image_request_payload(
            "openai-chat", "reader", 2048, "data:image/png;base64,fixture"
        )
        self.assertEqual(body["max_tokens"], 2048)
        self.assertEqual(body["temperature"], 0)
        self.assertEqual(body["chat_template_kwargs"], {"enable_thinking": False})
        self.assertEqual(
            body["messages"],
            [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": "data:image/png;base64,fixture"},
                        }
                    ],
                }
            ],
        )
        native = image_request_payload(
            "antfly-reader", "reader", 2048, "data:image/png;base64,fixture"
        )
        self.assertEqual(
            native,
            {
                "model": "reader",
                "max_tokens": 2048,
                "images": [{"url": "data:image/png;base64,fixture"}],
            },
        )

    def test_both_wire_shapes_preserve_the_complete_text(self):
        text = "Café 123!\n\n---"
        native = response_texts_and_statuses(
            "antfly-reader", {"data": [{"text": text, "status": "completed"}]}
        )
        chat = response_texts_and_statuses(
            "openai-chat",
            {"choices": [{"message": {"content": text}, "finish_reason": "stop"}]},
        )
        self.assertEqual(native, ([text], ["completed"]))
        self.assertEqual(chat, native)

    def test_chat_length_and_unknown_finish_reasons_cannot_qualify(self):
        for reason, status in [
            ("length", "truncated"),
            (None, "unresolved"),
            ("content_filter", "unresolved"),
        ]:
            with self.subTest(reason=reason):
                texts, statuses = response_texts_and_statuses(
                    "openai-chat",
                    {
                        "choices": [
                            {
                                "message": {"content": "complete tail words"},
                                "finish_reason": reason,
                            }
                        ]
                    },
                )
                self.assertEqual(texts, ["complete tail words"])
                self.assertEqual(statuses, [status])

    def test_native_completion_is_canonical_and_missing_status_is_unresolved(self):
        for status, expected in (
            ("complete", "completed"),
            (None, "unresolved"),
            ("unknown", "unresolved"),
        ):
            self.assertEqual(
                response_texts_and_statuses(
                    "antfly-reader", {"data": [{"text": "tail", "status": status}]}
                ),
                (["tail"], [expected]),
            )

    def test_nontext_chat_result_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "contain text"):
            response_texts_and_statuses(
                "openai-chat",
                {"choices": [{"message": {"content": None}, "finish_reason": "stop"}]},
            )


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


class NativeBatchEvidenceTests(unittest.TestCase):
    def row(self, width):
        execution = {"native_items": 1, "native_batches": 1}
        if width is not None:
            execution["max_native_step_items"] = width
        return {
            "http_status": 200,
            "items": 1,
            "tail_pass": True,
            "reference_exact": True,
            "statuses": ["completed"],
            "execution": execution,
        }

    def test_serialized_native_singletons_cannot_qualify_as_a_batch(self):
        # A serialized server may truthfully report native executor use.
        # Attribution, epoch counts, and text parity do not prove co-execution.
        for width in (None, 0, 1, True, "4"):
            with self.subTest(width=width):
                self.assertFalse(
                    qualified_request(
                        self.row(width),
                        require_reference_parity=True,
                        min_native_step_items=2,
                    )
                )
        self.assertTrue(
            qualified_request(
                self.row(4), require_reference_parity=True, min_native_step_items=4
            )
        )
        self.assertFalse(qualified_request(self.row(2), min_native_step_items=4))

    def test_width_does_not_override_correctness_or_completion(self):
        for key, value in (
            ("http_status", 503),
            ("tail_pass", False),
            ("reference_exact", False),
            ("statuses", []),
            ("statuses", ["truncated"]),
        ):
            row = self.row(4)
            row[key] = value
            self.assertFalse(
                qualified_request(
                    row, require_reference_parity=True, min_native_step_items=2
                )
            )


if __name__ == "__main__":
    unittest.main()
