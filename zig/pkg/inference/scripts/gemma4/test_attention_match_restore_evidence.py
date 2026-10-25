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

import sys, unittest
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "zig/pkg/inference/scripts/gemma4"))
from qualify_attention_match_queries import validate_restore_evidence


class RestoreEvidence(unittest.TestCase):
    def setUp(self):
        self.row = dict(
            source_tokens=8189,
            logical_tokens=8194,
            greedy_comparisons=6,
            appended_tokens=5,
            attention_calls=210,
            donor_appends=120,
            checkpoint_roundtrip=1,
            fresh_owner=1,
            replayed_tokens=0,
            bitwise_logits=1,
            source_unchanged=1,
            invalid=0,
        )
        self.requests = [{"response": {"usage": {"prompt_tokens": 8189}}}]

    def test_complete(self):
        validate_restore_evidence([self.row], self.requests)

    def test_v4_cannot_be_satisfied_by_fp16_fallback_restore(self):
        with self.assertRaises(AssertionError):
            validate_restore_evidence([self.row], self.requests, 4)
        validate_restore_evidence([{**self.row, "wire_version": 4}], self.requests, 4)

    def test_every_contract_flag(self):
        for key in (
            "checkpoint_roundtrip",
            "fresh_owner",
            "replayed_tokens",
            "bitwise_logits",
            "source_unchanged",
            "invalid",
        ):
            changed = dict(self.row)
            changed[key] ^= 1
            with self.assertRaises(AssertionError):
                validate_restore_evidence([changed], self.requests)

    def test_no_execution_or_bad_chain(self):
        for key, value in [
            ("source_tokens", 8000),
            ("logical_tokens", 8193),
            ("greedy_comparisons", 4),
            ("appended_tokens", 0),
            ("attention_calls", 0),
            ("donor_appends", 4),
        ]:
            changed = dict(self.row)
            changed[key] = value
            with self.assertRaises(AssertionError):
                validate_restore_evidence([changed], self.requests)
        with self.assertRaises(AssertionError):
            validate_restore_evidence([], self.requests)
        with self.assertRaises(AssertionError):
            validate_restore_evidence([], [])


if __name__ == "__main__":
    unittest.main()
