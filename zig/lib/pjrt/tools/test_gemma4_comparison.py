# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""A speed comparison must reject unequal work and unstable greedy output."""

import copy
import unittest

from compare_gemma4 import check_rows


class ComparisonCoverageTests(unittest.TestCase):
    def setUp(self):
        self.cases = [{"id": "text", "expected_prompt_tokens": 4, "max_tokens": 3}]
        self.rows = [
            {
                "id": "text",
                "repeat": i,
                "prompt_tokens": 4,
                "completion_tokens": 3,
                "tokens": [5, 6, 7],
                "ms": 1.0,
            }
            for i in range(2)
        ]

    def test_equal_work_passes(self):
        check_rows(self.rows, self.cases, 2)

    def test_short_output_cannot_win(self):
        self.rows[0].update(completion_tokens=2, tokens=[5, 6])
        with self.assertRaisesRegex(ValueError, "fixed-budget"):
            check_rows(self.rows, self.cases, 2)

    def test_duplicate_repeat_cannot_fill_missing_sample(self):
        self.rows[1] = copy.deepcopy(self.rows[0])
        with self.assertRaisesRegex(ValueError, "coverage"):
            check_rows(self.rows, self.cases, 2)

    def test_unstable_greedy_output_is_rejected(self):
        self.rows[1]["tokens"] = [5, 6, 8]
        with self.assertRaisesRegex(ValueError, "Nondeterministic"):
            check_rows(self.rows, self.cases, 2)

    def test_different_prompt_is_rejected(self):
        self.rows[0]["prompt_tokens"] = 5
        with self.assertRaisesRegex(ValueError, "fixed-budget"):
            check_rows(self.rows, self.cases, 2)
