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

"""Checks qualification scoring and stream accounting without model weights."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
import io
import json

spec = importlib.util.spec_from_file_location(
    "qualify_transcription", Path(__file__).with_name("qualify_transcription.py")
)
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)


class QualificationTests(unittest.TestCase):
    def test_word_error_counts_preserve_omissions_and_hallucinations(self):
        result = qualification.score("one two three four", "one changed four extra")
        self.assertEqual(result["errors"], 3)
        self.assertEqual(result["reference_words"], 4)
        self.assertEqual(result["wer"], 0.75)
        self.assertEqual(result["errors"], result["substitutions"] + result["deletions"] + result["insertions"])

    def test_normalization_preserves_non_english_words(self):
        self.assertEqual(qualification.normalized_words("BUENOS DÍAS! Don't—stop."), ["buenos", "días", "dont", "stop"])
        self.assertEqual(qualification.score("Hello world.", "HELLO, WORLD!")["wer"], 0)
        self.assertEqual(qualification.score("", "Invented speech")["insertions"], 2)
        self.assertIsNone(qualification.score("", "")["wer"])

    def test_stream_requires_authoritative_usage_and_completion(self):
        class Response(io.BytesIO):
            headers = {}
        def line(value):
            return b"data: " + json.dumps(value).encode() + b"\n\n"
        chunks = line({"choices": [{"delta": {"content": "hello"}}]})
        chunks += line({"choices": [], "usage": {"completion_tokens": 3, "prompt_tokens": 5}})
        with patch.object(qualification.urllib.request, "urlopen", return_value=Response(chunks + b"data: [DONE]\n\n")):
            with patch.object(qualification.time, "perf_counter", side_effect=[0.0, 1.0, 3.0, 4.0]):
                result = qualification.sse_generate("http://localhost", "model", 3)
        self.assertEqual(result["decode_tokens_per_second"], 1.0)
        self.assertEqual(result["end_to_end_tokens_per_second"], 0.75)
        self.assertEqual(result["stream_accounting"]["content_chunks"], 1)
        self.assertEqual(result["stream_accounting"]["first_content_characters"], 5)
        self.assertEqual(result["engine_reported_timings"], [])
        with patch.object(qualification.urllib.request, "urlopen", return_value=Response(chunks)):
            with self.assertRaisesRegex(RuntimeError, "DONE"):
                qualification.sse_generate("http://localhost", "model", 3)

    def test_stream_preserves_reported_timings_and_chunk_sizes_without_inferred_tokens(self):
        class Response(io.BytesIO):
            headers = {}
        events = [
            {"choices": [{"delta": {"reasoning_content": "hidden"}}]},
            {"choices": [{"delta": {"content": "a whole chunk"}}]},
            {"choices": [{"delta": {"content": "é"}}]},
            {"choices": [], "timings": {"predicted_n": 3, "predicted_ms": 2000},
             "usage": {"completion_tokens": 3, "prompt_tokens": 5}},
        ]
        data = b"".join(b"data: " + json.dumps(event).encode() + b"\n\n" for event in events) + b"data: [DONE]\n\n"
        with patch.object(qualification.urllib.request, "urlopen", return_value=Response(data)):
            with patch.object(qualification.time, "perf_counter", side_effect=[0.0, 1.0, 3.0, 4.0]):
                result = qualification.sse_generate("http://localhost", "model", 3)
        self.assertEqual(result["text"], "a whole chunké")
        self.assertEqual(result["stream_accounting"]["content_chunks"], 2)
        self.assertEqual(result["stream_accounting"]["first_content_characters"], 13)
        self.assertNotIn("tokens_per_chunk", result["stream_accounting"])
        self.assertEqual(result["engine_reported_timings"], [{"predicted_n": 3, "predicted_ms": 2000}])


    def test_stream_preserves_plain_text_generation_errors(self):
        class Response(io.BytesIO):
            headers = {}
        chunks = b"event: error\ndata: CudaGraphCaptureUnsafeTempAlloc\n\n"
        with patch.object(qualification.urllib.request, "urlopen", return_value=Response(chunks)):
            with self.assertRaisesRegex(RuntimeError, "CudaGraphCaptureUnsafeTempAlloc"):
                qualification.sse_generate("http://localhost", "model", 3)

    def test_reference_stream_explicitly_disables_prompt_cache_reuse(self):
        class Response(io.BytesIO):
            headers = {}
        chunks = b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n'
        chunks += b'data: {"usage":{"completion_tokens":3,"prompt_tokens":5}}\n\ndata: [DONE]\n\n'
        with patch.object(qualification.urllib.request, "urlopen", return_value=Response(chunks)) as call:
            with patch.object(qualification.time, "perf_counter", side_effect=[0.0, 1.0, 3.0, 4.0]):
                qualification.sse_generate("http://localhost", "model", 3,
                    route="/v1/chat/completions", extra_body={"cache_prompt": False})
        request = call.call_args.args[0]
        self.assertEqual(request.full_url, "http://localhost/v1/chat/completions")
        self.assertIs(json.loads(request.data)["cache_prompt"], False)


    def test_json_post_preserves_request_and_response(self):
        class Response(io.BytesIO):
            headers = {"Content-Type": "application/json"}
        with patch.object(qualification.urllib.request, "urlopen", return_value=Response(b'{"data": []}')) as call:
            with patch.object(qualification.time, "perf_counter", side_effect=[1.0, 2.0]):
                response, seconds, headers = qualification.post("http://localhost", "/read", {"model": "speech"})
        self.assertEqual(response, {"data": []})
        self.assertEqual(seconds, 1)
        self.assertEqual(call.call_args.args[0].full_url, "http://localhost/read")
        self.assertEqual(json.loads(call.call_args.args[0].data), {"model": "speech"})


if __name__ == "__main__":
    unittest.main()
