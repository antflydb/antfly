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
import qualify_attention_match_queries as qualification


class ToolQualityTests(unittest.TestCase):
    def choice(self):
        return {
            "index": 0,
            "finish_reason": "tool_calls",
            "message": {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_a",
                        "type": "function",
                        "function": {
                            "name": "lookup_access_record",
                            "arguments": '{"access_code":"violet-729"}',
                        },
                    }
                ],
            },
        }

    def test_tool_fixture_uses_real_tool_schema_and_unprotected_middle_fact(self):
        body = next(qualification.requests("model", "f16", [742], fixture="tool_call"))
        self.assertEqual(
            body["tool_choice"]["function"]["name"], "lookup_access_record"
        )
        self.assertEqual(
            body["tools"][0]["function"]["parameters"]["required"], ["access_code"]
        )
        before, after = body["messages"][0]["content"].split(
            "The access code is violet-729.\n"
        )
        self.assertEqual(before.count("This log entry"), 371)
        self.assertEqual(after.count("This log entry"), 371)
        with self.assertRaises(ValueError):
            next(
                qualification.requests(
                    "model", "f16", [742], stream=True, fixture="tool_call"
                )
            )

    def test_structured_tool_correctness_and_id_independent_parity(self):
        first = self.choice()
        qualification.validate_fixture_choice(first, "tool_call")
        second = copy.deepcopy(first)
        second["message"]["tool_calls"][0]["id"] = "call_b"
        self.assertEqual(
            qualification.canonical_choices([first]),
            qualification.canonical_choices([second]),
        )
        self.assertEqual(first["message"]["tool_calls"][0]["id"], "call_a")
        second["message"]["tool_calls"][0]["function"]["arguments"] = (
            '{"access_code":"wrong"}'
        )
        self.assertNotEqual(
            qualification.canonical_choices([first]),
            qualification.canonical_choices([second]),
        )

    def test_rejects_wrong_missing_duplicate_or_text_tool_answers(self):
        for arguments in (
            "{}",
            '{"access_code":"wrong"}',
            '{"access_code":"violet-729","extra":1}',
            '{"access_code":"wrong","access_code":"violet-729"}',
            "not json",
        ):
            row = self.choice()
            row["message"]["tool_calls"][0]["function"]["arguments"] = arguments
            with (
                self.subTest(arguments=arguments),
                self.assertRaises((AssertionError, ValueError)),
            ):
                qualification.validate_fixture_choice(row, "tool_call")
        row = self.choice()
        row["finish_reason"] = "stop"
        row["message"] = {"content": "violet-729"}
        with self.assertRaises(AssertionError):
            qualification.validate_fixture_choice(row, "tool_call")
        row = self.choice()
        row["message"]["tool_calls"][0]["function"]["name"] = "wrong"
        with self.assertRaises(AssertionError):
            qualification.validate_fixture_choice(row, "tool_call")
