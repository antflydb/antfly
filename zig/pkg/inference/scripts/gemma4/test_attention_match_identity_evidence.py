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
from qualify_attention_match_queries import validate_identity_evidence, parse_capture


class IdentityEvidenceTests(unittest.TestCase):
    def row(self):
        return dict(
            cache_hit=0,
            elapsed_ns=100,
            weights_sha256="1" * 64,
            tokenizer_sha256="2" * 64,
            template_sha256="3" * 64,
            layout_sha256="4" * 64,
        )

    def test_loader_identity_and_cached_repeat_keep_exact_digests(self):
        row = self.row()
        parsed = parse_capture(" ".join(f"{key}={value}" for key, value in row.items()))
        self.assertEqual(parsed, row)
        validate_identity_evidence([row, row | {"cache_hit": 1, "elapsed_ns": 10}], 2)

    def test_missing_sentinel_changed_and_invalid_identity_fail_closed(self):
        row = self.row()
        for rows, count in (
            ([], 1),
            ([], 0),
            ([row], 2),
            ([row | {"weights_sha256": "0" * 64}], 1),
            ([row | {"layout_sha256": "bad"}], 1),
            ([row | {"elapsed_ns": 0}], 1),
            ([row | {"cache_hit": 2}], 1),
            ([row, row | {"tokenizer_sha256": "5" * 64}], 2),
        ):
            with (
                self.subTest(rows=rows, count=count),
                self.assertRaises(AssertionError),
            ):
                validate_identity_evidence(rows, count)
