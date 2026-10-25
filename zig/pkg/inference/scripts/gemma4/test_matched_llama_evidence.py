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

from qualify_matched_llama import (
    percentiles,
    read_completion,
    validate_result,
    content_token_count,
)


def result():
    return {
        "content": "violet-729",
        "stop_type": "eos",
        "truncated": False,
        "tokens": [68002, 236772, 236832, 236778, 236819],
        "tokens_evaluated": 8189,
        "tokens_predicted": 5,
        "timings": {
            "prompt_n": 8189,
            "cache_n": 0,
            "predicted_n": 5,
            "prompt_ms": 2000.0,
            "predicted_ms": 100.0,
        },
    }


class MatchedLlamaEvidenceTests(unittest.TestCase):
    def test_terminal_turn_token_is_excluded_without_mutating_raw_counters(self):
        value = result()
        value["tokens"].append(106)
        value["tokens_predicted"] = 6
        value["timings"]["predicted_n"] = 6
        original = copy.deepcopy(value)
        validate_result(value, 8189, 5, "violet-729", "replay")
        self.assertEqual(content_token_count(value), 5)
        self.assertEqual(value, original)

    def test_eos_normalization_cannot_hide_extra_content_or_invalid_token_accounting(
        self,
    ):
        for tokens in (
            [68002, 236772, 236832, 236778, 236819, 7],
            [68002, 236772, 236832, 236778, 236819, 7, 106],
            [106, 68002, 236772, 236832, 236778, 236819],
            [106, 68002, 236772, 236832, 236778, 236819, 106],
            [68002, 236772, 236832, 236778, True, 106],
        ):
            value = result()
            value.update(tokens=tokens, tokens_predicted=len(tokens))
            value["timings"]["predicted_n"] = len(tokens)
            with self.subTest(tokens=tokens), self.assertRaises(AssertionError):
                validate_result(value, 8189, 5, "violet-729", "replay")

    def test_replay_cannot_hide_cached_prefix(self):
        value = result()
        validate_result(value, 8189, 5, "violet-729", "replay")
        value["timings"].update(prompt_n=1, cache_n=8188)
        with self.assertRaisesRegex(AssertionError, "cached work"):
            validate_result(value, 8189, 5, "violet-729", "replay")
        validate_result(value, 8189, 5, "violet-729", "resident")

    def test_resident_cannot_count_full_replay(self):
        with self.assertRaisesRegex(AssertionError, "did not reuse"):
            validate_result(result(), 8189, 5, "violet-729", "resident")

    def test_wrong_prompt_output_or_truncation_fails(self):
        for changes in [
            {"tokens_evaluated": 8190},
            {"tokens_predicted": 6},
            {"truncated": True},
            {"content": "violet-728"},
            {"stop_type": "limit"},
        ]:
            value = result()
            value.update(changes)
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                validate_result(value, 8189, 5, "violet-729", "replay")

    def test_invalid_or_unmeasured_engine_phase_fails(self):
        for timing in [float("nan"), float("inf"), -1.0, 0.0]:
            value = result()
            value["timings"]["predicted_ms"] = timing
            with self.subTest(timing=timing), self.assertRaises(AssertionError):
                validate_result(value, 8189, 5, "violet-729", "replay")
        value = result()
        value["timings"].update(prompt_n=0, cache_n=8189, prompt_ms=0.0)
        validate_result(value, 8189, 5, "violet-729", "resident")
        self.assertEqual(percentiles([0.0, 0.0], allow_zero=True)["p95"], 0.0)

    def test_stream_preserves_final_accounting_and_first_token(self):
        final = result()
        final.update(stop=True, content="", tokens=[])
        chunks = [
            {"index": 0, "stop": False, "content": "violet", "tokens": [68002]},
            {
                "index": 0,
                "stop": False,
                "content": "-729",
                "tokens": [236772, 236832, 236778, 236819],
            },
            final,
        ]
        wire = b"".join(
            ("data: " + json.dumps(row) + "\n\n").encode() for row in chunks
        )
        observation = read_completion(io.BytesIO(wire), time.monotonic())
        self.assertEqual(observation["result"]["content"], "violet-729")
        self.assertEqual(observation["result"]["tokens"], result()["tokens"])
        self.assertGreater(observation["ttft_seconds"], 0)
        validate_result(observation["result"], 8189, 5, "violet-729", "replay")

    def test_done_without_accounting_or_unstreamed_result_fails(self):
        for wire in [
            b"data: [DONE]\n\n",
            b'data: {"stop":true,"content":"violet-729"}\n\n',
        ]:
            with self.subTest(wire=wire), self.assertRaises(AssertionError):
                read_completion(io.BytesIO(wire), time.monotonic())


if __name__ == "__main__":
    unittest.main()
