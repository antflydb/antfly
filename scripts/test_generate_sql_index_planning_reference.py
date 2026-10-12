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

"""PostgreSQL-only immutability, membership, and planning-order contracts."""

import unittest

import psycopg

from generate_sql_index_planning_reference import reference
from generate_sql_postgres_reference import postgres


class IndexPlanningReferenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = postgres()
        cls.db = cls.server.__enter__()
        cls.result = reference(cls.db)
        cls.entries = {entry["expression"]: entry for entry in cls.result["entries"]}

    @classmethod
    def tearDownClass(cls):
        cls.server.__exit__(None, None, None)

    def test_complete_independent_contracts(self):
        self.assertEqual(24, len(self.entries))
        self.assertEqual(
            18, sum(entry["sqlstate"] is None for entry in self.entries.values())
        )

    def test_reachable_stable_keys_and_predicates_are_rejected(self):
        for expression in (
            "concat(s, 'x')",
            "concat_ws('-', s, 'x')",
            "to_jsonb(s)",
            "n > 0 AND concat(s, 'x') IS NULL",
        ):
            self.assertEqual("42P17", self.entries[expression]["sqlstate"])

    def test_dead_stable_key_calls_do_not_change_values(self):
        self.assertEqual(
            [None, "", "ALPHA"],
            self.entries["CASE WHEN false THEN concat(s, 'x') ELSE s END"]["rows"],
        )
        self.assertEqual(
            ["ok", "", "ALPHA"],
            self.entries["coalesce(s, 'ok', concat(s, 'x'))"]["rows"],
        )
        self.assertEqual([None, "", "alpha"], self.entries["lower(s)"]["rows"])

    def test_false_and_unknown_membership_is_empty(self):
        for expression in ("false", "NULL::boolean", "NOT NULL::boolean", "NOT true"):
            self.assertEqual([], self.entries[expression]["rows"])

    def test_true_membership_is_full_including_null_keys(self):
        for expression in (
            "true",
            "s IS NULL OR concat(s, 'x') IS NULL OR true",
            "true OR (1 / 0 = 0)",
            "NOT false",
        ):
            self.assertEqual([None, "0", "3"], self.entries[expression]["rows"])

    def test_neutral_boolean_and_dead_branches_keep_range_membership(self):
        for expression in (
            "CASE WHEN false THEN concat(s, 'x') IS NULL ELSE n > 0 END",
            "n > 0 AND true",
            "n > 0 OR false",
        ):
            self.assertEqual(["3"], self.entries[expression]["rows"])

    def test_binding_precedes_pruning_and_constant_failures_preserve_order(self):
        self.assertEqual("42703", self.entries["false AND missing IS NULL"]["sqlstate"])
        self.assertEqual("22012", self.entries["(1 / 0 = 0) OR true"]["sqlstate"])
        self.assertIsNone(
            self.db.execute("SELECT to_regclass('public.index_probe')").fetchone()[0]
        )

    def test_constant_membership_retains_foreign_key_target_eligibility(self):
        # PostgreSQL canonicalizes WHERE true to a full unique index, but an
        # empty unique index must never claim whole-table referential coverage.
        for predicate, expected in (
            ("", None),
            (" WHERE true", None),
            (" WHERE false", "42830"),
        ):
            state = None
            try:
                with self.db.transaction(force_rollback=True):
                    self.db.execute("CREATE TABLE fk_parent_probe (n int)")
                    self.db.execute(
                        "CREATE UNIQUE INDEX fk_parent_idx ON fk_parent_probe(n)"
                        + predicate
                    )
                    self.db.execute(
                        "CREATE TABLE fk_child_probe (n int REFERENCES fk_parent_probe(n))"
                    )
            except psycopg.Error as error:
                state = error.sqlstate
            self.assertEqual(expected, state)
            self.assertIsNone(
                self.db.execute(
                    "SELECT to_regclass('public.fk_parent_probe')"
                ).fetchone()[0]
            )


if __name__ == "__main__":
    unittest.main()
