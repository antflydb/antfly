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
import unittest

from qualify_native_stress import assert_equivalent, summarize_blocks, TEN_REQUESTS


def completed_blocks(metal=False):
    return [
        {
            "passed": True,
            "operations": [
                {
                    "kind": kind,
                    "passed": True,
                    **({"outcome": "compacted"} if kind == "prepare" else {}),
                }
                for kind in TEN_REQUESTS * 5
            ],
            "quiescent_memory": ({
                "group_rss_bytes": 5 * 1024**3,
                "free_memory_percent": 42.0,
                "swapout_bytes": 0,
            } if metal else {
                "host_rss_bytes": 5 * 1024**3,
                "gpu_memory_bytes": 11 * 1024**3,
            }),
        }
        for _ in range(20)
    ]


class StressEvidenceTests(unittest.TestCase):
    def test_metal_bounded_profile_proves_100_mixed_and_10_independent_compactions(self):
        result = summarize_blocks(completed_blocks(metal=True)[:2], "bounded")
        self.assertEqual(result["mixed_requests"], 100)
        self.assertEqual(result["actual_compactions"], 10)
        self.assertEqual(result["independent_repreparations"], 10)
        self.assertFalse(result["successive_compaction_cycles_qualified"])
        self.assertIn("public checkpoint protocol", result["successive_compaction_reason"])
        self.assertEqual(set(result["quiescent_growth"]), {"group_rss_bytes"})

    def test_metal_rejects_inconsistent_or_growing_quiescent_evidence(self):
        blocks = completed_blocks(metal=True)[:2]
        blocks[1]["quiescent_memory"].pop("swapout_bytes")
        with self.assertRaisesRegex(AssertionError, "inconsistent"):
            summarize_blocks(blocks, "bounded")
        blocks = completed_blocks(metal=True)[:2]
        blocks[1]["quiescent_memory"]["group_rss_bytes"] += 129 * 1024**2
        with self.assertRaisesRegex(AssertionError, "quiescent memory growth"):
            summarize_blocks(blocks, "bounded")

    def test_bounded_profile_reports_its_actual_scope(self):
        blocks = completed_blocks()[:2]
        result = summarize_blocks(blocks, "bounded")
        self.assertEqual(result["mixed_requests"], 100)
        self.assertEqual(result["actual_compactions"], 10)
        self.assertFalse(result["extended_run_qualified"])
        self.assertEqual(
            result["quiescent_growth"]["host_rss_bytes"]["window_blocks"], 1
        )
        with self.assertRaises(AssertionError):
            summarize_blocks(blocks)
        with self.assertRaises(AssertionError):
            summarize_blocks(blocks[:1], "bounded")
        blocks[-1]["quiescent_memory"]["host_rss_bytes"] += 129 * 1024**2
        with self.assertRaisesRegex(AssertionError, "quiescent memory growth"):
            summarize_blocks(blocks, "bounded")

    def test_bounded_profile_rejects_missing_work_and_fallbacks(self):
        blocks = completed_blocks()[:2]
        blocks[0]["operations"].pop()
        with self.assertRaises(AssertionError):
            summarize_blocks(blocks, "bounded")
        blocks = completed_blocks()[:2]
        blocks[0]["operations"][0]["outcome"] = "unchanged_insufficient_savings"
        with self.assertRaisesRegex(AssertionError, "actual compactions"):
            summarize_blocks(blocks, "bounded")

    def test_only_complete_campaign_counts(self):
        blocks = completed_blocks()
        result = summarize_blocks(blocks)
        self.assertEqual(result["mixed_requests"], 1000)
        self.assertEqual(result["actual_compactions"], 100)
        with self.assertRaises(AssertionError):
            summarize_blocks(blocks[:-1])
        blocks[-1]["operations"].pop()
        with self.assertRaises(AssertionError):
            summarize_blocks(blocks)

    def test_unchanged_fallback_cannot_satisfy_compaction_count(self):
        blocks = completed_blocks()
        blocks[7]["operations"][0]["outcome"] = "unchanged_insufficient_savings"
        with self.assertRaisesRegex(AssertionError, "actual compactions"):
            summarize_blocks(blocks)

    def test_failed_request_cannot_be_hidden_by_later_recovery(self):
        blocks = completed_blocks()
        blocks[7]["operations"][3]["passed"] = False
        with self.assertRaisesRegex(AssertionError, "successful expected outcomes"):
            summarize_blocks(blocks)

    def test_growth_below_hard_cap_still_fails_diagnostic(self):
        blocks = completed_blocks()
        for block in blocks[-5:]:
            block["quiescent_memory"]["host_rss_bytes"] += 129 * 1024**2
        with self.assertRaisesRegex(
            AssertionError, "sustained quiescent memory growth"
        ):
            summarize_blocks(blocks)

    def test_changed_usage_fails_even_if_answer_is_identical(self):
        expected = {
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "violet-729"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 8189,
                "cached_prompt_tokens": 8189,
                "completion_tokens": 5,
            },
        }
        actual = copy.deepcopy(expected)
        assert_equivalent(actual, expected)
        actual["usage"]["cached_prompt_tokens"] = 0
        with self.assertRaisesRegex(AssertionError, "token accounting"):
            assert_equivalent(actual, expected)


if __name__ == "__main__":
    unittest.main()
