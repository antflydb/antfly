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

"""Run with uv run --no-project --with 'psycopg[binary]==3.3.6' python -m
unittest discover -s scripts -p test_generate_sql_postgres_reference.py.
These tests intentionally require PostgreSQL 18+: no skip or substitute oracle.
"""

from copy import deepcopy
import unittest
from unittest.mock import patch

from generate_sql_postgres_reference import (
    document_reference,
    execute,
    postgres,
    read_reference,
    SEEDS,
    validate_ordered_groups,
)


class PostgresReferenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = postgres()
        cls.db = cls.server.__enter__()
        cls.addClassCleanup(cls.server.__exit__, None, None, None)

    def test_original_catalog_subquery_defaults_are_not_postgres_features(self):
        import json
        from pathlib import Path

        fixtures = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/local/sql/fixtures"
        )
        cases = json.loads((fixtures / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        selected = [case for case in cases if "sql-1109" <= case["id"] <= "sql-1140"]
        self.assertEqual(len(selected), 32)
        for case in selected:
            with self.subTest(id=case["id"]):
                with self.db.transaction(force_rollback=True):
                    if case["sql"].upper().startswith("ALTER"):
                        self.db.execute(
                            "CREATE TABLE usage_records(id uuid, status text, amount bigint)"
                        )
                    import psycopg

                    with self.assertRaises(psycopg.errors.FeatureNotSupported) as error:
                        with self.db.transaction(force_rollback=True):
                            self.db.execute(case["sql"])
                    self.assertEqual(error.exception.sqlstate, "0A000")

    def case(self, sql, params=()):
        return {"id": "sql-0001", "sql": sql, "params": params}

    def profile(self):
        return {
            "schema": {
                "default_type": "row",
                "document_schemas": {
                    "row": {
                        "schema": {
                            "properties": {
                                "id": {"type": "integer"},
                                "metadata": {"type": "json"},
                            }
                        }
                    }
                },
            },
            "rows": [
                {
                    "key": "a",
                    "value": {"id": 9007199254740993, "metadata": {"source": "api"}},
                }
            ],
        }

    def test_postgres_version_and_private_listener(self):
        self.assertGreaterEqual(self.db.info.server_version, 180000)
        self.assertEqual("", self.db.execute("SHOW listen_addresses").fetchone()[0])

    def test_exact_raw_parameter_reuse_bigint_and_json_null_provenance(self):
        result = execute(
            self.db,
            self.case(
                "SELECT $1::bigint AS exact, $1::bigint AS repeated, NULL::text AS missing, 'null'::jsonb AS json_null",
                [{"integer": "9007199254740993"}],
            ),
            read=True,
        )
        self.assertEqual(
            [[9007199254740993, 9007199254740993, None, None]], result["rows"]
        )
        self.assertEqual([[False, False, True, False]], result["sql_nulls"])
        self.assertEqual([20, 20, 25, 3802], result["column_oids"])

    def test_default_null_order_and_unicode_are_postgres_owned(self):
        result = execute(
            self.db,
            self.case(
                "SELECT value FROM (VALUES ('é'), (NULL::text), ('a')) AS v(value) ORDER BY value"
            ),
            read=True,
        )
        self.assertEqual([["a"], ["é"], [None]], result["rows"])
        result = execute(
            self.db,
            self.case(
                "SELECT bit_length('é') AS bits, strpos('aé🍎z','🍎') AS position, concat_ws(':','a',NULL,'',3) AS combined"
            ),
            read=True,
        )
        self.assertEqual([[16, 3, "a::3"]], result["rows"])
        result = execute(
            self.db,
            self.case(
                "SELECT lpad('é',4,'🍎x'),rpad('é',4,'🍎x'),lpad('é🍎',1,'x'),repeat('é🍎',2),reverse('aé🍎')"
            ),
            read=True,
        )
        self.assertEqual([["🍎x🍎é", "é🍎x🍎", "é", "é🍎é🍎", "🍎éa"]], result["rows"])

    def test_read_only_oracle_cannot_mutate_and_failed_case_does_not_poison_next(self):
        result = read_reference(
            self.db,
            [
                self.case("DELETE FROM usage_records RETURNING id"),
                self.case("SELECT id FROM public.usage_records"),
            ],
            self.profile(),
        )
        self.assertEqual(1, len(result["excluded"]))
        self.assertEqual([[9007199254740993]], result["entries"][0]["rows"])
        self.assertIsNone(
            self.db.execute("SELECT to_regclass('public.usage_records')").fetchone()[0]
        )

    def test_sqlite_only_or_emulated_functions_do_not_receive_postgres_credit(self):
        result = read_reference(
            self.db,
            [
                self.case("SELECT ends_with('abc','c') FROM usage_records"),
                self.case("SELECT id FROM usage_records"),
            ],
            self.profile(),
        )
        self.assertEqual(1, len(result["excluded"]))
        self.assertEqual(1, len(result["entries"]))
        for sql in [
            "SELECT concat_ws(':') FROM usage_records",
            "SELECT concat() FROM usage_records",
        ]:
            invalid = read_reference(self.db, [self.case(sql)], self.profile())
            self.assertEqual([], invalid["entries"])
            self.assertIn("does not exist", invalid["excluded"][0]["reason"])

    def test_exact_json_parameter_is_not_passed_as_text(self):
        result = execute(
            self.db,
            self.case(
                "SELECT jsonb_typeof($1) AS kind, $1->>'source' AS source",
                [{"json": '{"source":"api"}'}],
            ),
            read=True,
        )
        self.assertEqual([["object", "api"]], result["rows"])

    def test_document_final_state_preserves_undeclared_and_untouched_data(self):
        schema = {
            "default_type": "doc",
            "storage_mode": "document",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "properties": {
                            "title": {"type": "text"},
                            "status": {"type": "keyword"},
                        }
                    }
                }
            },
        }
        case = self.case(
            "UPDATE docs SET title='Changed' WHERE _id='doc:a' RETURNING _id,title"
        )
        result = document_reference(self.db, [case], {case["id"]: schema})
        self.assertEqual([], result["excluded"])
        entry = result["entries"][0]
        self.assertEqual([["doc:a", "Changed"]], entry["rows"])
        self.assertEqual(1, entry["affected"])
        expected = deepcopy(SEEDS)
        expected[0]["value"]["title"] = "Changed"
        self.assertEqual(expected, entry["final"])
        self.assertIsNone(
            self.db.execute("SELECT to_regclass('public.docs')").fetchone()[0]
        )

    def test_recursive_reference_is_resource_bounded(self):
        self.db.execute("SET statement_timeout = '50ms'")
        try:
            result = read_reference(
                self.db,
                [
                    self.case(
                        "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r) SELECT sum(x) FROM r"
                    )
                ],
                self.profile(),
            )
        finally:
            self.db.execute("SET statement_timeout = '2s'")
        self.assertEqual([], result["entries"])
        reason = result["excluded"][0]["reason"]
        self.assertTrue(
            "statement timeout" in reason or "temp_file_limit" in reason, reason
        )

    def test_large_reads_use_a_bounded_server_cursor_and_close_it_on_rejection(self):
        with patch(
            "psycopg.RawCursor",
            side_effect=AssertionError("read must not buffer the entire result"),
        ):
            with self.assertRaisesRegex(ValueError, "row budget"):
                execute(
                    self.db,
                    self.case("SELECT generate_series(1,100000000) AS n"),
                    read=True,
                )
        self.assertEqual(
            0,
            self.db.execute(
                "SELECT count(*) FROM pg_cursors WHERE name='antfly_reference'"
            ).fetchone()[0],
        )

    def test_explicit_null_assignment_is_not_confused_with_missing_property(self):
        schema = {
            "default_type": "doc",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "properties": {
                            "title": {"type": "text"},
                            "archived_at": {"type": "keyword"},
                        }
                    }
                }
            },
        }
        case = self.case("UPDATE docs SET archived_at=NULL WHERE _id='doc:a'")
        result = document_reference(self.db, [case], {case["id"]: schema})
        entry = result["entries"][0]
        self.assertIn("archived_at", entry["final"][0]["value"])
        self.assertIsNone(entry["final"][0]["value"]["archived_at"])
        self.assertNotIn("archived_at", entry["final"][1]["value"])

    def test_current_document_profile_retains_source_schema_without_granting_proofs(
        self,
    ):
        schema = {
            "default_type": "doc",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "properties": {
                            "title": {"type": "text"},
                            "metadata": {"type": "json"},
                        }
                    }
                }
            },
        }
        case = self.case(
            "UPDATE docs SET title='Changed' WHERE metadata->>'source'='api'"
        )
        result = document_reference(self.db, [case], {case["id"]: schema})
        entry = result["entries"][0]
        self.assertEqual(
            "json",
            schema["document_schemas"]["doc"]["schema"]["properties"]["metadata"][
                "type"
            ],
        )
        self.assertEqual(schema, entry["schema"])
        self.assertEqual(
            "object",
            entry["native_schema"]["document_schemas"]["doc"]["schema"]["properties"][
                "metadata"
            ]["type"],
        )


class OrderingContractTest(unittest.TestCase):
    def test_ordered_peer_frontier_accepts_ties_but_not_worse_or_duplicate_rows(self):
        entry = {
            "rows": [["first"], ["peer-b"]],
            "sql_nulls": [[False], [False]],
            "ordered_groups": [
                {"rows": [["first"]], "sql_nulls": [[False]]},
                {"rows": [["peer-a"], ["peer-b"]], "sql_nulls": [[False], [False]]},
                {"rows": [["worse"]], "sql_nulls": [[False]]},
            ],
        }
        validate_ordered_groups(entry)
        for rows in [
            [["first"], ["worse"]],
            [["peer-a"], ["peer-b"]],
            [["first"], ["first"]],
        ]:
            with self.assertRaises(ValueError):
                validate_ordered_groups({**entry, "rows": rows})


if __name__ == "__main__":
    unittest.main()
