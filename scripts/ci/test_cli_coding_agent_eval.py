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

"""Deterministic evaluator grading and usage attribution, without model calls."""

import unittest
from eval_cli_coding_agent import score, summarize, summarize_claude


class CodingAgentEvaluationTests(unittest.TestCase):
    def test_grade_preserves_integer_precision_and_omission(self):
        self.assertEqual(
            score("aggregation", {"value": "9007199254740993", "incomplete": False}),
            (True, True),
        )
        self.assertEqual(
            score("aggregation", {"value": "9007199254740992", "incomplete": False}),
            (False, True),
        )
        self.assertEqual(
            score("oversized_result", {"value": "100", "incomplete": False}),
            (True, False),
        )
        self.assertEqual(
            score("partial_shards", {"value": '["doc:1"]', "incomplete": True}),
            (True, True),
        )
        self.assertEqual(
            score("uncertain_write", {"value": "committed", "incomplete": True}),
            (False, True),
        )

    def test_usage_is_reported_not_estimated(self):
        self.assertEqual(summarize([]), (None, [], []))
        events = [
            {
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 20,
                    "cached_input_tokens": 10,
                    "output_tokens": 3,
                },
            },
            {
                "type": "item.completed",
                "item": {"type": "command_execution", "command": "./antfly sql"},
            },
            {
                "type": "item.started",
                "item": {"type": "command_execution", "command": "./antfly sql"},
            },
            {"type": "item.completed", "item": {"type": "agent_message", "text": "{}"}},
        ]
        usage, commands, messages = summarize(events)
        self.assertEqual(usage["input_tokens"], 20)
        self.assertEqual(usage["cached_input_tokens"], 10)
        self.assertEqual(len(commands), 1)
        self.assertEqual(messages, ["{}"])

    def test_claude_cache_usage_is_included_in_input_total(self):
        events = [
            {
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "Bash",
                            "input": {"command": "./antfly schema"},
                        }
                    ]
                },
            },
            {
                "type": "result",
                "usage": {
                    "input_tokens": 5,
                    "cache_read_input_tokens": 100,
                    "cache_creation_input_tokens": 20,
                    "output_tokens": 2,
                },
                "structured_output": {"value": "1", "incomplete": False},
                "total_cost_usd": 0.01,
                "num_turns": 3,
            },
        ]
        usage, commands, answer, result = summarize_claude(events)
        self.assertEqual(usage["input_tokens"], 125)
        self.assertEqual(usage["cached_input_tokens"], 100)
        self.assertEqual(len(commands), 1)
        self.assertEqual(answer["value"], "1")
        self.assertEqual(result["total_cost_usd"], 0.01)


if __name__ == "__main__":
    unittest.main()
