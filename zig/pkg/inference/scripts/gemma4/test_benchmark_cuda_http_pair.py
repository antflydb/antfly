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

import io
import json
import unittest
from unittest.mock import patch

import benchmark_cuda_http_pair as bench


class StreamTests(unittest.TestCase):
    def run_stream(self, events, done=True, ignore_eos=True):
        data = b"".join(b"data: " + json.dumps(event).encode() + b"\n\n" for event in events)
        if done:
            data += b"data: [DONE]\n\n"
        response = io.BytesIO(data)
        response.status = 200
        with patch.object(bench.http.client, "HTTPConnection") as connection:
            connection.return_value.getresponse.return_value = response
            return bench.request(12345, "/generate", {"max_tokens": 4, "ignore_eos": ignore_eos})

    def test_fixed_length_and_usage_after_content(self):
        result = self.run_stream([
            {"choices": [{"delta": {"content": "hello"}}]},
            {"choices": [{"delta": {}, "finish_reason": "length"}],
             "usage": {"prompt_tokens": 10, "completion_tokens": 4}},
        ])
        self.assertEqual(result["text"], "hello")
        self.assertEqual(result["usage"]["completion_tokens"], 4)
        self.assertGreater(result["ms"], result["ttft_ms"])

    def test_truncated_stream_is_not_a_success(self):
        with self.assertRaises(RuntimeError):
            self.run_stream([{"choices": [{"delta": {"content": "hello"}}],
                              "usage": {"completion_tokens": 4}}], done=False)

    def test_wrong_output_length_is_not_a_success(self):
        with self.assertRaises(RuntimeError):
            self.run_stream([{"choices": [{"delta": {"content": "hello"}}],
                              "usage": {"completion_tokens": 3}}])

    def test_quality_mode_accepts_only_a_clean_early_stop(self):
        event = {"choices": [{"delta": {"content": "answer"}, "finish_reason": "stop"}], "usage": {"completion_tokens": 2}}
        self.assertEqual(self.run_stream([event], ignore_eos=False)["text"], "answer")
        event["choices"][0]["finish_reason"] = "length"
        with self.assertRaises(RuntimeError):
            self.run_stream([event], ignore_eos=False)

    def test_error_event_is_not_a_success(self):
        with self.assertRaisesRegex(RuntimeError, "failed"):
            self.run_stream([{"error": "failed"}])

    def test_reasoning_is_retained_and_timed(self):
        result = self.run_stream([
            {"choices": [{"delta": {"reasoning_content": "thought"}}]},
            {"choices": [{"delta": {"content": "answer"}}], "usage": {"completion_tokens": 4}},
        ])
        self.assertEqual(result["reasoning"], "thought")
        self.assertEqual(result["text"], "answer")


if __name__ == "__main__":
    unittest.main()
