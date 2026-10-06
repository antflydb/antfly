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
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
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

    def test_typed_array_quantifiers_against_exact_postgres_sql(self):
        import json
        from pathlib import Path

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_reference.json"
            ).read_text()
        )
        self.assertEqual(fixture["reference"], "PostgreSQL exact SQL")
        self.assertEqual(len(fixture["entries"]), 18)
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                with self.db.transaction(force_rollback=True):
                    self.db.execute("SET TRANSACTION READ ONLY")
                    cursor = self.db.execute("SELECT " + case["sql"])
                    self.assertEqual(cursor.fetchone(), (case["expected"],))
                    self.assertEqual(cursor.description[0].type_code, 16)

    def test_typed_array_shape_null_and_float_reference_boundaries(self):
        cases = {
            "array_ndims('{}'::int4[])": None,
            "cardinality('{}'::int4[])": 0,
            "array_length('{}'::int4[], 1)": None,
            "array_lower('[-1:0][0:1]={{1,NULL},{3,4}}'::int4[], 1)": -1,
            "array_upper('[-1:0][0:1]={{1,NULL},{3,4}}'::int4[], 1)": 0,
            "('[-1:0][0:1]={{1,NULL},{3,4}}'::int4[])[0][1]": 4,
            "ARRAY[NULL]::int4[] @> ARRAY[NULL]::int4[]": False,
            "ARRAY[NULL]::int4[] && ARRAY[NULL]::int4[]": False,
            "ARRAY[1]::int4[] @> ARRAY[1,1]::int4[]": True,
            "'[0:1]={1,2}'::int4[] < '[1:2]={1,2}'::int4[]": True,
            "'[0:1]={1,2}'::int4[] @> '[1:2]={1,2}'::int4[]": True,
            "ARRAY['NaN'::float8] = ARRAY['NaN'::float8]": True,
            "ARRAY['NaN'::float8] > ARRAY['Infinity'::float8]": True,
            "ARRAY[0.0::float8] = ARRAY[-0.0::float8]": True,
            "ARRAY['null'::jsonb] < ARRAY[NULL]::jsonb[]": True,
            "array_position('[-3:-1]={a,NULL,a}'::text[], 'a')": -3,
            "array_position('[-3:-1]={a,NULL,a}'::text[], NULL)": -2,
        }
        for expression, expected in cases.items():
            with self.subTest(sql=expression):
                with self.db.transaction(force_rollback=True):
                    self.db.execute("SET TRANSACTION READ ONLY")
                    actual = self.db.execute("SELECT " + expression).fetchone()
                    self.assertEqual(actual, (expected,))
        self.assertEqual(
            self.db.execute(
                "SELECT oid, typelem FROM pg_type WHERE oid = ANY(%s) ORDER BY oid",
                [[1000, 1005, 1007, 1009, 1016, 1021, 1022, 2951, 3807]],
            ).fetchall(),
            [
                (1000, 16),
                (1005, 21),
                (1007, 23),
                (1009, 25),
                (1016, 20),
                (1021, 700),
                (1022, 701),
                (2951, 2950),
                (3807, 3802),
            ],
        )

    def test_typed_array_binary_server_goldens_and_native_parameter_payloads(self):
        import json
        from pathlib import Path

        import psycopg
        from psycopg.adapt import Dumper
        from psycopg.pq import Format

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_binary_reference.json"
            ).read_text()
        )
        self.assertEqual(fixture["reference"], "PostgreSQL 18+ binary array_send")
        self.assertEqual(len(fixture["entries"]), 11)

        class Payload:
            def __init__(self, data):
                self.data = data

        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                dumper = type(
                    "TypedArrayDumper",
                    (Dumper,),
                    {
                        "oid": case["array_oid"],
                        "format": Format.BINARY,
                        "dump": lambda self, obj: obj.data,
                    },
                )
                self.db.adapters.register_dumper(Payload, dumper)
                with self.db.transaction(force_rollback=True):
                    self.db.execute("SET TRANSACTION READ ONLY")
                    with self.db.cursor(binary=True) as cursor:
                        cursor.execute("SELECT " + case["sql"])
                        self.assertEqual(cursor.pgresult.fformat(0), 1)
                        self.assertEqual(cursor.pgresult.ftype(0), case["array_oid"])
                        self.assertEqual(
                            cursor.pgresult.get_value(0, 0).hex(), case["binary"]
                        )
                    payload = Payload(
                        bytes.fromhex(case.get("native_binary", case["binary"]))
                    )
                    with psycopg.RawCursor(self.db) as cursor:
                        cursor.execute("SELECT $1 = (" + case["sql"] + ")", [payload])
                        self.assertEqual(cursor.fetchone(), (True,))
                        if case["element_type"] == "jsonb":
                            cursor.execute(
                                "SELECT e IS NULL, jsonb_typeof(e) FROM unnest($1) AS e",
                                [payload],
                            )
                            self.assertEqual(
                                cursor.fetchall(),
                                [
                                    (False, "null"),
                                    (True, None),
                                    (False, "object"),
                                    (False, "array"),
                                ],
                            )

    def test_typed_array_scalar_expression_contracts(self):
        import json
        from pathlib import Path

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_expression_reference.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["entries"]), 98)
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                with self.db.transaction(force_rollback=True):
                    self.db.execute("SET TRANSACTION READ ONLY")
                    self.assertEqual(
                        self.db.execute("SELECT " + case["sql"]).fetchone(),
                        (case["value"],),
                    )

    def test_typed_array_cast_rejection_contracts(self):
        import json
        from pathlib import Path

        import psycopg

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_cast_errors.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["entries"]), 20)
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                with self.assertRaises(psycopg.Error) as caught:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute("SET TRANSACTION READ ONLY")
                        self.db.execute("SELECT " + case["sql"])
                self.assertEqual(caught.exception.sqlstate, case["code"])

    def test_typed_array_binary_receive_boundary_admission(self):
        import struct

        import psycopg
        from psycopg.adapt import Dumper
        from psycopg.pq import Format

        class Payload:
            def __init__(self, data):
                self.data = data

        cases = [
            (
                1000,
                struct.pack("!iiIiiiB", 1, 0, 16, 1, 1, 1, 2),
                ("[1:1]", 1, "{t}"),
                None,
            ),
            (
                1007,
                struct.pack("!iiIiiii", 1, 0, 23, 1, 2147483647, 4, 1),
                None,
                "54000",
            ),
            (
                1007,
                struct.pack("!iiIiiii", 1, 0, 23, 1, 2147483646, 4, 1),
                ("[2147483646:2147483646]", 1, "[2147483646:2147483646]={1}"),
                None,
            ),
            (
                1007,
                struct.pack("!iiIiiii", 2, 0, 23, 100000, 1, 0, 1),
                (None, 0, "{}"),
                None,
            ),
            (
                1007,
                struct.pack("!iiIiii", 1, 0, 23, 1, 1, -1),
                ("[1:1]", 1, "{NULL}"),
                None,
            ),
            (1009, struct.pack("!iiIiii", 1, 0, 25, 1, 1, 1) + b"\0", None, "22021"),
        ]
        for oid, data, expected, sqlstate in cases:
            with self.subTest(oid=oid, payload=data.hex()):
                dumper = type(
                    "TypedArrayDumper",
                    (Dumper,),
                    {
                        "oid": oid,
                        "format": Format.BINARY,
                        "dump": lambda self, obj: obj.data,
                    },
                )
                self.db.adapters.register_dumper(Payload, dumper)
                try:
                    with self.db.transaction(force_rollback=True):
                        with psycopg.RawCursor(self.db) as cursor:
                            cursor.execute(
                                "SELECT array_dims($1), cardinality($1), $1::text",
                                [Payload(data)],
                            )
                            if sqlstate is not None:
                                self.fail(
                                    "PostgreSQL unexpectedly admitted invalid binary data"
                                )
                            self.assertEqual(cursor.fetchone(), expected)
                except psycopg.Error as error:
                    self.assertEqual(error.sqlstate, sqlstate)

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
