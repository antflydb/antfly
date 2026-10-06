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

"""Regression tests for prospective contributor sign-off enforcement."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from check_contributor_signoffs import POLICY_MARKER, check


class SignoffTests(unittest.TestCase):
    def test_policy_is_prospective(self):
        with patch("check_contributor_signoffs.git", return_value="old policy") as git:
            self.assertEqual(check("base", "head"), [])
            git.assert_called_once_with("show", "base:CONTRIBUTING.md")

    def test_requires_matching_author_signoff(self):
        def fake_git(*args):
            if args[0] == "rev-list":
                return "abc123\n"
            if args[-1] == "abc123":
                return "Alice\nalice@example.org\nChange\nSigned-off-by: Bob <bob@example.org>\n"
            return POLICY_MARKER

        with patch("check_contributor_signoffs.git", side_effect=fake_git):
            errors = check("base", "head")
        self.assertEqual(len(errors), 1)
        self.assertIn("Signed-off-by: Alice <alice@example.org>", errors[0])

    def test_accepts_matching_author_signoff(self):
        def fake_git(*args):
            if args[0] == "rev-list":
                return "abc123\n"
            if args[-1] == "abc123":
                return "Alice\nalice@example.org\nChange\n\nSigned-off-by: Alice <alice@example.org>\n"
            return POLICY_MARKER

        with patch("check_contributor_signoffs.git", side_effect=fake_git):
            self.assertEqual(check("base", "head"), [])


if __name__ == "__main__":
    unittest.main()
