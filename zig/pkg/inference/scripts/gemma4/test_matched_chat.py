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
import io
import json
import time
import unittest
from unittest.mock import patch

from matched_chat import read_chat, validate_chat, verify_template


def events():
    return [
        {"choices": [{"index": 0, "delta": {"role": "assistant", "content": None}}]},
        {"choices": [{"index": 0, "delta": {"content": "violet-729"}}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
        {
            "choices": [],
            "usage": {
                "prompt_tokens": 8189,
                "completion_tokens": 6,
                "total_tokens": 8195,
                "prompt_tokens_details": {"cached_tokens": 0},
            },
            "timings": {
                "prompt_n": 8189,
                "cache_n": 0,
                "predicted_n": 6,
                "prompt_ms": 800.0,
                "predicted_ms": 100.0,
            },
        },
    ]


def read(rows, done=True):
    wire = "".join("data: " + json.dumps(row) + "\n\n" for row in rows)
    if done:
        wire += "data: [DONE]\n\n"
    return read_chat(io.BytesIO(wire.encode()), time.monotonic())


class ChatTests(unittest.TestCase):
    def test_stream_requires_done_and_preserves_raw_counts(self):
        value = read(events())
        self.assertEqual(value["result"]["content"], "violet-729")
        self.assertEqual(value["result"]["usage"]["completion_tokens"], 6)
        self.assertGreater(value["total_seconds"], value["first_content_seconds"])
        validate_chat(
            value["result"], 8189, {"content": "violet-729", "tokens_predicted": 6}
        )
        with self.assertRaises(AssertionError):
            read(events(), done=False)

    def test_missing_usage_timing_or_content_and_duplicate_finish_fail(self):
        for index in (1, 2, 3):
            rows = events()
            rows.pop(index)
            with self.subTest(index=index), self.assertRaises(AssertionError):
                read(rows)
        rows = events()
        rows.insert(3, copy.deepcopy(rows[2]))
        with self.assertRaises(AssertionError):
            read(rows)

    def test_rejects_diagnostic_echo_and_extra_generation_work(self):
        for change in ({"reasoning_content": "thinking"}, {"tool_calls": [1]}):
            rows = events()
            rows[1]["choices"][0]["delta"].update(change)
            with self.assertRaises(AssertionError):
                read(rows)
        rows = events()
        rows[-1]["__verbose"] = {"prompt": "large prompt"}
        with self.assertRaises(AssertionError):
            read(rows)

    def test_rejects_hidden_cache_extra_tokens_and_nonfinite_timing(self):
        for field, changes in [
            ("timings", {"cache_n": 8188, "prompt_n": 1}),
            ("timings", {"predicted_ms": float("nan")}),
            ("usage", {"completion_tokens": 7}),
        ]:
            value = read(events())["result"]
            value[field].update(changes)
            with self.assertRaises(AssertionError):
                validate_chat(
                    value, 8189, {"content": "violet-729", "tokens_predicted": 6}
                )

    def test_template_requires_exact_ids_not_just_equal_count(self):
        body = {"messages": [], "chat_template_kwargs": {"enable_thinking": False}}
        for ids in ([1, 3], [True, 2]):
            with patch(
                "matched_chat.post_json",
                side_effect=[{"prompt": "text"}, {"tokens": ids}],
            ):
                with self.assertRaises(AssertionError):
                    verify_template(1234, body, [1, 2])
        with patch(
            "matched_chat.post_json",
            side_effect=[{"prompt": "text"}, {"tokens": [1, 2]}],
        ):
            self.assertTrue(verify_template(1234, body, [1, 2])["verified"])


if __name__ == "__main__":
    unittest.main()
