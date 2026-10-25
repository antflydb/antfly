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

from qualify_native_sse_abort import first_content_delta


def event(delta=None, finish=None, usage=None):
    value = {"choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish}]}
    if usage is not None:
        value["usage"] = usage
    return ("data: " + json.dumps(value) + "\n\n").encode()


class NativeSseAbortEvidenceTests(unittest.TestCase):
    def test_stops_at_actual_content_without_consuming_terminal_events(self):
        tail = event({"content": "later"}) + b"data: [DONE]\n\n"
        response = io.BytesIO(
            event({"role": "assistant"}) + event({"content": "first"}) + tail
        )
        result = first_content_delta(response)
        self.assertEqual(result["content"], "first")
        self.assertEqual(response.read(), b"\n" + tail)

    def test_completed_or_role_only_stream_cannot_qualify(self):
        for data in [
            event({"role": "assistant"}),
            b"data: [DONE]\n\n",
            event({"content": "already complete"}, finish="stop"),
        ]:
            with self.subTest(data=data), self.assertRaises(AssertionError):
                first_content_delta(io.BytesIO(data))

    def test_error_and_usage_cannot_count_as_content(self):
        for data in [
            b"event: error\ndata: failure\n\n",
            event({"content": "last"}, usage={"completion_tokens": 512}),
        ]:
            with self.subTest(data=data), self.assertRaises(AssertionError):
                first_content_delta(io.BytesIO(data))

    def test_limits_line_size_and_empty_events(self):
        for data in [b"x" * 65537 + b"\n", b"\n" * 256]:
            with self.subTest(size=len(data)), self.assertRaises(AssertionError):
                first_content_delta(io.BytesIO(data))


if __name__ == "__main__":
    unittest.main()
