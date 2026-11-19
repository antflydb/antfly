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

"""Independently verify the typed numeric oracle and its complete case domains."""

import json
import unittest

from generate_sql_numeric_functions_reference import FIXTURE, reference
from generate_sql_postgres_reference import postgres


class NumericFunctionsReferenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with postgres() as db:
            cls.actual = reference(db)
        cls.entries = cls.actual["entries"]

    def entry(self, domain, function, value, scale=None):
        matches = [
            entry
            for entry in self.entries
            if (entry["domain"], entry["function"], entry["input"], entry["scale"])
            == (domain, function, value, scale)
        ]
        self.assertEqual(len(matches), 1)
        return matches[0]

    def test_fixture_reproduces_independently(self):
        self.assertEqual(self.actual, json.loads(FIXTURE.read_text()))

    def test_complete_matrix_and_strict_errors(self):
        self.assertEqual(len(self.entries), 210)
        self.assertEqual(len(self.actual["strict_errors"]), 4)
        self.assertEqual(
            {entry["sqlstate"] for entry in self.actual["strict_errors"]}, {"22012"}
        )
        identities = [
            (e["domain"], e["function"], e["input"], e["scale"]) for e in self.entries
        ]
        self.assertEqual(len(set(identities)), len(identities))

    def test_numeric_ties_are_away_and_float_ties_are_even(self):
        self.assertEqual(self.entry("numeric", "round", "2.5", 0)["expected"], "3")
        self.assertEqual(self.entry("numeric", "round", "-2.5", 0)["expected"], "-3")
        self.assertEqual(
            self.entry("float64", "round", "2.5")["expected"], "4000000000000000"
        )
        self.assertEqual(
            self.entry("float64", "round", "-2.5")["expected"], "c000000000000000"
        )

    def test_large_numeric_never_detours_through_float(self):
        self.assertEqual(
            self.entry("numeric", "round", "9007199254740993.5", 0)["expected"],
            "9007199254740994",
        )
        self.assertEqual(
            self.entry("numeric", "trunc", "9007199254740993.5", 0)["expected"],
            "9007199254740993",
        )

    def test_scale_limits_are_postgres_limits(self):
        self.assertEqual(
            self.entry("numeric", "round", "1.25", -2147483648)["expected"], "0"
        )
        self.assertEqual(
            len(self.entry("numeric", "round", "1.25", 2147483647)["expected"]), 16385
        )

    def test_signed_zero_and_special_values(self):
        self.assertEqual(
            self.entry("float64", "round", "-0")["expected"], "8000000000000000"
        )
        self.assertEqual(self.entry("numeric", "sign", "NaN")["expected"], "NaN")
        self.assertEqual(
            self.entry("float64", "sign", "NaN")["expected"], "0000000000000000"
        )

    def test_negative_square_roots_preserve_sqlstate(self):
        for domain in ("numeric", "float64"):
            self.assertEqual(self.entry(domain, "sqrt", "-2.5")["sqlstate"], "2201F")

    def test_sql_null_is_not_numeric_zero_or_nan(self):
        nulls = [entry for entry in self.entries if entry["input"] is None]
        self.assertEqual(len(nulls), 20)
        self.assertTrue(
            all(e["expected"] is None and e["sqlstate"] is None for e in nulls)
        )

    def test_postfix_cast_precedes_sign_without_rejecting_parenthesized_minima(self):
        entries = self.actual["cast_precedence"]
        self.assertEqual(len(entries), 11)
        self.assertEqual(
            [entry["sqlstate"] for entry in entries[:6]],
            ["22003", None, "22003", None, "22003", None],
        )
        self.assertEqual(entries[6]["expected"], "-0")
        self.assertEqual(entries[7]["expected"], "0")
        self.assertTrue(all(entry["expected"] == "0" for entry in entries[8:]))


if __name__ == "__main__":
    unittest.main()
