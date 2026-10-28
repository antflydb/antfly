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

import unittest
from unittest.mock import patch
from qualify_reader_generator_pools import Probe


class ProbeAssertionsTest(unittest.TestCase):
    def test_policy_rejection_requires_the_exact_code(self):
        probe = Probe(1)
        for response in [
            (400, {"error": "INVALID_REQUEST"}),
            (413, {"error": "MODEL_RESOURCE_LIMIT"}),
            (413, {"error": "INPUT_TOKEN_LIMIT_EXCEEDED", "retryable": False}),
        ]:
            with patch.object(probe, "request", return_value=response):
                with self.assertRaises(AssertionError):
                    probe.rejected("", "/read", {}, "OUTPUT_TOKEN_LIMIT_EXCEEDED")
        with patch.object(
            probe,
            "request",
            return_value=(
                413,
                {"error": "OUTPUT_TOKEN_LIMIT_EXCEEDED", "retryable": False},
            ),
        ):
            probe.rejected("", "/read", {}, "OUTPUT_TOKEN_LIMIT_EXCEEDED")

    def test_overload_requires_concurrency_reason_and_valid_success(self):
        probe = Probe(1)
        valid = {
            "choices": [{"message": {"content": "READY"}, "finish_reason": "stop"}]
        }
        correct = {
            "error": "SERVICE_UNAVAILABLE",
            "reason": "inference_admission",
            "retryable": True,
        }
        for success, rejection in [
            (valid, {"error": "MODEL_RESOURCE_BUSY"}),
            ({"choices": []}, correct),
            ({"choices": [{"message": {"content": "wrong"}}]}, correct),
        ]:
            with patch.object(
                probe,
                "request",
                side_effect=[(200, success), (503, rejection), (200, {})],
            ):
                with self.assertRaises((AssertionError, IndexError)):
                    probe.overload("", "/generate", {})
        with patch.object(
            probe, "request", side_effect=[(200, valid), (503, correct), (200, {})]
        ):
            probe.overload("", "/generate", {})


if __name__ == "__main__":
    unittest.main()
