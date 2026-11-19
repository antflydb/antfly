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

"""Independent generated-expression planning and failure-timing contracts."""

import json
import unittest

import psycopg

from generate_sql_generated_reference import FIXTURE, reference
from generate_sql_postgres_reference import postgres


class GeneratedReferenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = postgres()
        cls.db = cls.server.__enter__()
        cls.result = reference(cls.db)
        cls.entries = {entry["expression"]: entry for entry in cls.result["entries"]}

    @classmethod
    def tearDownClass(cls):
        cls.server.__exit__(None, None, None)

    def test_original_multi_column_generation_contract(self):
        fixture = json.loads(
            FIXTURE.with_name("sql_generated_original_reference.json").read_text()
        )
        inventory = json.loads(
            FIXTURE.with_name("sql_parity_inventory.json").read_text()
        )
        original = next(
            case for case in inventory["entries"] if case["id"] == fixture["id"]
        )
        self.assertEqual("PostgreSQL exact SQL", fixture["reference"])
        with self.db.transaction():
            self.db.execute(original["sql"])
            with self.db.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO generated_expr_records (id,status,amount,fee,metadata) "
                    "VALUES (%s::uuid,%s,%s::numeric,%s::numeric,%s::jsonb)",
                    [
                        (
                            row["id"],
                            row["status"],
                            row["amount"],
                            row["fee"],
                            row["metadata_json"],
                        )
                        for row in fixture["inputs"]
                    ],
                )
            rows = self.db.execute(
                "SELECT id::text,status_slug,amount_cents::text,source_key FROM generated_expr_records ORDER BY id"
            ).fetchall()
            self.assertEqual(fixture["rows"], [list(row) for row in rows])
            self.db.execute("DROP TABLE generated_expr_records")

    def test_complete_admission_and_row_contracts(self):
        self.assertEqual(42, len(self.result["entries"]))
        admitted = [e for e in self.entries.values() if e["sqlstate"] is None]
        self.assertEqual(27, len(admitted))
        for entry in admitted:
            self.assertEqual(3, len(entry["rows"]))

    def test_stable_functions_and_formatting_casts_are_not_immutable(self):
        for expression in (
            "concat(s, 'x')",
            "concat_ws('-', s, 'x')",
            "now()",
            "transaction_timestamp()",
            "CURRENT_TIMESTAMP(6)",
            "s::timestamptz",
            "t::text",
            "to_jsonb(s)",
            "jsonb_build_object('s', s)",
        ):
            with self.subTest(expression=expression):
                self.assertEqual("42P17", self.entries[expression]["sqlstate"])

    def test_dead_stable_calls_are_removed_before_immutability_checks(self):
        for expression in (
            "CASE WHEN false THEN concat(s, 'x') ELSE 'ok' END",
            "coalesce('ok', concat(s, 'x'))",
            "coalesce(NULL::text, 'ok', concat(s, 'x'))",
        ):
            self.assertEqual(["ok"] * 3, self.entries[expression]["rows"])

    def test_binding_checks_precede_dead_branch_elimination(self):
        self.assertEqual(
            "42703",
            self.entries["CASE WHEN false THEN missing ELSE 'ok' END"]["sqlstate"],
        )
        self.assertEqual(
            "42P17", self.entries["CASE WHEN false THEN g ELSE 'ok' END"]["sqlstate"]
        )

    def test_generated_constant_errors_differ_from_default_write_errors(self):
        self.assertEqual("22012", self.entries["1 / 0"]["sqlstate"])
        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE default_probe(n int DEFAULT (1 / 0))")
            with self.assertRaises(psycopg.errors.DivisionByZero):
                with self.db.transaction():
                    self.db.execute("INSERT INTO default_probe DEFAULT VALUES")
            self.assertEqual(
                0, self.db.execute("SELECT count(*) FROM default_probe").fetchone()[0]
            )

    def test_boolean_order_and_null_semantics(self):
        self.assertEqual("22012", self.entries["(1 / 0 = 0) OR true"]["sqlstate"])
        self.assertEqual("22012", self.entries["(1 / 0 = 0) AND false"]["sqlstate"])
        self.assertEqual(["true"] * 3, self.entries["true OR (1 / 0 = 0)"]["rows"])
        self.assertEqual(["false"] * 3, self.entries["false AND (1 / 0 = 0)"]["rows"])
        self.assertEqual(
            ["true", None, None], self.entries["NULL::boolean OR n IS NULL"]["rows"]
        )
        self.assertEqual(
            [None, "false", "false"],
            self.entries["NULL::boolean AND n IS NULL"]["rows"],
        )

    def test_timestamp_overloads_have_distinct_volatility(self):
        observed = self.db.execute(
            "SELECT provolatile FROM pg_proc WHERE oid='to_timestamp(double precision)'::regprocedure"
        ).fetchone()[0]
        self.assertEqual("i", observed)
        observed = self.db.execute(
            "SELECT provolatile FROM pg_proc WHERE oid='to_timestamp(text,text)'::regprocedure"
        ).fetchone()[0]
        self.assertEqual("s", observed)


if __name__ == "__main__":
    unittest.main()
