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
from contextlib import redirect_stderr
from io import StringIO
import unittest
from unittest.mock import patch

from generate_sql_postgres_reference import (
    array_reference,
    array_seed_text,
    create_table,
    pg_type,
    FIXTURES,
    main,
    document_reference,
    execute,
    postgres,
    read_reference,
    aggregate_read_profile,
    aggregate_order_observer,
    normalize_ordered_contract,
    set_read_profile,
    mutation_reference,
    SEEDS,
    validate_ordered_groups,
)


class ReferenceExtensionTest(unittest.TestCase):
    def test_clock_functions_require_a_dedicated_native_clock_contract(self):
        for expression in (
            "now()",
            "NOW ()",
            "transaction_timestamp()",
            "statement_timestamp()",
            "clock_timestamp()",
            "timeofday()",
            "CURRENT_TIMESTAMP(6)",
            "CURRENT_DATE",
        ):
            # Reject before touching a connection: a wall-clock sample
            # must never become an apparently deterministic PG golden.
            with (
                self.subTest(expression=expression),
                self.assertRaisesRegex(ValueError, "clock profile required"),
            ):
                execute(None, {"sql": f"SELECT {expression}"}, read=True)

    def test_extension_rejects_missing_baseline_unknown_and_duplicate_ids(self):
        golden = str(FIXTURES / "sql_read_campaign_reference.json")
        for arguments in (
            ["read", "--include", "sql-0561"],
            ["read", "--check", golden, "--include", "sql-not-a-case"],
            ["read", "--check", golden, "--include", "sql-0561"],
            ["read", "--check", golden, "--only-id", "sql-0561"],
            ["mutation", "--only-id", "sql-unknown"],
            ["mutation", "--only-id", "sql-1455", "--only-id", "sql-1455"],
        ):
            with self.subTest(arguments=arguments):
                with (
                    patch("sys.argv", ["reference", *arguments]),
                    patch("generate_sql_postgres_reference.postgres") as server,
                    redirect_stderr(StringIO()),
                    self.assertRaises(SystemExit) as error,
                ):
                    main()
                self.assertEqual(2, error.exception.code)
                server.assert_not_called()


class CatalogReferenceTest(unittest.TestCase):
    def test_original_catalog_commands_and_native_diagnostic_contracts(self):
        import json
        import psycopg

        campaign = json.loads((FIXTURES / "sql_catalog_campaign.json").read_text())
        original = {
            case["id"]: case
            for case in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        ids = [
            "sql-0095",
            "sql-0101",
            "sql-0102",
            "sql-0103",
            "sql-0104",
            "sql-0106",
            "sql-0108",
            "sql-0157",
            "sql-0159",
            "sql-0676",
        ]
        self.assertEqual(ids, [case["id"] for case in campaign["entries"]])

        def reset(db):
            # This connection is owned by postgres(), never a user database.
            db.execute("DROP TABLE IF EXISTS usage_records, usage_stage CASCADE")
            db.execute("DROP SCHEMA IF EXISTS tenant_ops CASCADE")
            db.execute("DROP SCHEMA IF EXISTS tenant_ops_archive CASCADE")
            db.execute("DROP DATABASE IF EXISTS tenant_ops")

        def identity(db, kind, name):
            if kind == "namespace":
                row = db.execute(
                    "SELECT oid FROM pg_namespace WHERE nspname=%s", (name,)
                ).fetchone()
            elif kind == "database":
                row = db.execute(
                    "SELECT oid FROM pg_database WHERE datname=%s", (name,)
                ).fetchone()
            elif kind == "table":
                row = db.execute(
                    "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=%s",
                    (name,),
                ).fetchone()
            else:
                self.fail("unexpected catalog campaign kind")
            return row[0] if row else None

        with postgres() as db:
            for case in campaign["entries"]:
                with self.subTest(case=case["id"]):
                    reset(db)
                    for sql in case["setup"]:
                        db.execute(sql)
                    source = original[case["id"]]
                    self.assertEqual([], source["params"])
                    prior = identity(
                        db, case["kind"], case.get("prior_name", case["name"])
                    )
                    cursor = db.execute(source["sql"])
                    self.assertEqual(case["command_tag"], cursor.statusmessage)
                    observed = identity(db, case["kind"], case["name"])
                    self.assertEqual(case["present"], observed is not None)
                    if case.get("unchanged") or case.get("prior_name"):
                        self.assertEqual(prior, observed)
                    if case.get("prior_name"):
                        self.assertIsNone(
                            identity(db, case["kind"], case["prior_name"])
                        )
                    if case.get("repeat_noop"):
                        self.assertEqual(
                            case["command_tag"], db.execute(source["sql"]).statusmessage
                        )
                        self.assertIsNone(identity(db, case["kind"], case["name"]))
                    if case["kind"] == "table" and case["present"]:
                        self.assertEqual(
                            [
                                ("tenant_id", "text", "NO"),
                                ("id", "uuid", "NO"),
                                ("status", "text", "YES"),
                            ],
                            db.execute(
                                "SELECT column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema='public' AND table_name='usage_stage' ORDER BY ordinal_position"
                            ).fetchall(),
                        )
            for setup, sql, code in (
                (["CREATE SCHEMA tenant_ops"], "CREATE SCHEMA tenant_ops", "42P06"),
                (["CREATE DATABASE tenant_ops"], "CREATE DATABASE tenant_ops", "42P04"),
                (
                    ["CREATE TABLE usage_records(id uuid)"],
                    "CREATE TABLE usage_records(id uuid)",
                    "42P07",
                ),
                ([], "DROP SCHEMA tenant_ops", "3F000"),
                ([], "DROP DATABASE tenant_ops", "3D000"),
                ([], "DROP TABLE usage_records", "42P01"),
                (
                    [
                        "CREATE SCHEMA tenant_ops",
                        "CREATE TABLE tenant_ops.child(id uuid)",
                    ],
                    "DROP SCHEMA tenant_ops",
                    "2BP01",
                ),
            ):
                with self.subTest(sql=sql):
                    reset(db)
                    for statement in setup:
                        db.execute(statement)
                    with self.assertRaises(psycopg.Error) as caught:
                        db.execute(sql)
                    self.assertEqual(code, caught.exception.sqlstate)


class SetSpillReferenceTest(unittest.TestCase):
    def test_complete_high_cardinality_set_multiplicities(self):
        import json
        from collections import Counter

        fixture = json.loads((FIXTURES / "sql_set_spill_reference.json").read_text())
        self.assertEqual(1, fixture["format"])
        self.assertEqual(16384, fixture["input_rows"])
        self.assertEqual(5, len(fixture["entries"]))
        with postgres() as db:
            db.execute("CREATE TEMP TABLE items(n bigint)")
            db.execute(
                "INSERT INTO items SELECT i/2 FROM generate_series(0,16383) g(i)"
            )
            with db.transaction(force_rollback=True):
                db.execute("SET TRANSACTION READ ONLY")
                for case in fixture["entries"]:
                    with self.subTest(sql=case["sql"]):
                        cursor = db.execute(case["sql"])
                        self.assertEqual(20, cursor.description[0].type_code)
                        rows = cursor.fetchall()
                        self.assertLessEqual(len(rows), fixture["input_rows"])
                        expected = Counter(
                            {
                                n: case["multiplicity"]
                                for n in range(case["maximum"] + 1)
                            }
                        )
                        self.assertEqual(expected, Counter(row[0] for row in rows))
                        self.assertEqual(case["expected"], str(len(rows)))
                        self.assertEqual(case["sum"], str(sum(row[0] for row in rows)))


class PostgresReferenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = postgres()
        cls.db = cls.server.__enter__()
        cls.addClassCleanup(cls.server.__exit__, None, None, None)

    def test_index_namespace_ownership_search_path_and_diagnostics(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE SCHEMA index_path_target")
            self.db.execute("CREATE TABLE public.index_path_shadow(id bigint)")
            self.db.execute(
                "CREATE TABLE index_path_target.index_path_owner(id bigint PRIMARY KEY)"
            )
            self.db.execute(
                "CREATE INDEX namespace_key ON public.index_path_shadow(id)"
            )
            self.db.execute("SET LOCAL search_path TO public, index_path_target")
            # CREATE binds the table first; an unrelated index in an earlier
            # namespace cannot capture the new index's namespace or collide.
            self.db.execute("CREATE INDEX namespace_key ON index_path_owner(id)")
            self.assertEqual(
                [("index_path_target",), ("public",)],
                self.db.execute(
                    "SELECT n.nspname FROM pg_class c JOIN pg_namespace n "
                    "ON n.oid = c.relnamespace WHERE c.relname = 'namespace_key' "
                    "AND c.relkind = 'i' ORDER BY n.nspname"
                ).fetchall(),
            )
            # DROP resolves the index itself, so the earlier namespace wins.
            self.db.execute("DROP INDEX namespace_key")
            self.assertIsNone(
                self.db.execute(
                    "SELECT to_regclass('public.namespace_key')"
                ).fetchone()[0]
            )
            self.assertIsNotNone(
                self.db.execute(
                    "SELECT to_regclass('index_path_target.namespace_key')"
                ).fetchone()[0]
            )
            for sql, state in (
                ("DROP INDEX index_path_target.missing_key", "42704"),
                ("DROP INDEX index_path_target.index_path_owner", "42809"),
                ("DROP INDEX index_path_target.index_path_owner_pkey", "2BP01"),
                (
                    "CREATE INDEX index_path_target.qualified_key "
                    "ON index_path_target.index_path_owner(id)",
                    "42601",
                ),
            ):
                with self.subTest(sql=sql):
                    with self.assertRaises(psycopg.Error) as caught:
                        with self.db.transaction():
                            self.db.execute(sql)
                    self.assertEqual(state, caught.exception.sqlstate)
            self.db.execute("DROP INDEX index_path_target.namespace_key")

    def test_not_valid_check_still_enforces_new_writes(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TEMP TABLE not_valid_owner(id text PRIMARY KEY, amount bigint)"
            )
            self.db.execute("INSERT INTO not_valid_owner VALUES ('old', -1)")
            self.db.execute(
                "ALTER TABLE not_valid_owner ADD CONSTRAINT nonnegative CHECK (amount >= 0) NOT VALID"
            )
            self.assertEqual(
                [(False,)],
                self.db.execute(
                    "SELECT convalidated FROM pg_constraint WHERE conrelid = 'not_valid_owner'::regclass AND conname = 'nonnegative'"
                ).fetchall(),
            )
            # NOT VALID exempts historical rows from the installation scan,
            # never a newly inserted or updated row from enforcement.
            for sql in (
                "INSERT INTO not_valid_owner VALUES ('new', -1)",
                "UPDATE not_valid_owner SET amount = -2 WHERE id = 'old'",
                "ALTER TABLE not_valid_owner VALIDATE CONSTRAINT nonnegative",
            ):
                with self.subTest(sql=sql):
                    with self.assertRaises(psycopg.errors.CheckViolation) as caught:
                        with self.db.transaction():
                            self.db.execute(sql)
                    self.assertEqual("23514", caught.exception.sqlstate)
                    self.assertEqual(
                        [("old", -1)],
                        self.db.execute(
                            "SELECT id, amount FROM not_valid_owner"
                        ).fetchall(),
                    )
            self.db.execute(
                "INSERT INTO not_valid_owner VALUES ('valid', 1), ('nullable', NULL)"
            )
            self.assertEqual(
                3, self.db.execute("SELECT count(*) FROM not_valid_owner").fetchone()[0]
            )

    def test_array_overlap_and_string_output_match_shared_native_contracts(self):
        import json
        import psycopg

        fixture = json.loads((FIXTURES / "sql_array_string_reference.json").read_text())
        self.assertEqual(22, len(fixture["entries"]))
        for case in fixture["entries"]:
            with self.subTest(expression=case["expression"]):
                if "error" in case:
                    with self.assertRaises(psycopg.Error) as caught:
                        self.db.execute("SELECT " + case["expression"])
                    self.assertEqual(case["error"], caught.exception.sqlstate)
                    continue
                self.assertEqual(
                    case["expected"],
                    self.db.execute("SELECT " + case["expression"]).fetchone()[0],
                )

    def test_predicate_modifiers_match_shared_native_contracts(self):
        import json
        import psycopg

        fixture = json.loads((FIXTURES / "sql_predicate_reference.json").read_text())
        self.assertEqual(28, len(fixture["entries"]))
        for case in fixture["entries"]:
            with self.subTest(expression=case["expression"]):
                cursor = self.db.execute("SELECT " + case["expression"])
                self.assertEqual(16, cursor.description[0].type_code)
                self.assertEqual(case["expected"], cursor.fetchone()[0])
        for expression in fixture["type_errors"]:
            with (
                self.subTest(expression=expression),
                self.assertRaises(psycopg.errors.DatatypeMismatch) as failure,
            ):
                self.db.execute("SELECT " + expression)
            self.assertEqual("42804", failure.exception.sqlstate)

    def test_expression_defaults_and_stored_generation_assignment_contract(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TABLE exprs (g integer GENERATED ALWAYS AS "
                "(CASE WHEN n IS NULL THEN 0 ELSE CAST(n AS integer)+1 END) STORED, "
                "n smallint DEFAULT (2+3), overflow smallint DEFAULT (32767+1), "
                "label text DEFAULT lower('READY'), "
                "slug text GENERATED ALWAYS AS (lower(label)||'-ok') STORED, "
                "h smallint GENERATED ALWAYS AS (n+1) STORED)"
            )
            self.assertEqual(
                (5, 6, 6, "ready", "ready-ok"),
                self.db.execute(
                    "INSERT INTO exprs(overflow) VALUES (1) RETURNING n,g,h,label,slug"
                ).fetchone(),
            )
            self.db.execute(
                "ALTER TABLE exprs ALTER COLUMN n SET DEFAULT "
                "CASE WHEN true THEN 8 ELSE 9 END"
            )
            self.assertEqual(
                (8, 9, 9),
                self.db.execute(
                    "INSERT INTO exprs(overflow) VALUES (1) RETURNING n,g,h"
                ).fetchone(),
            )
            self.assertEqual(
                (None, 0, None),
                self.db.execute(
                    "INSERT INTO exprs(n,overflow) VALUES (NULL,1) RETURNING n,g,h"
                ).fetchone(),
            )
            for sql in (
                "INSERT INTO exprs DEFAULT VALUES",
                "INSERT INTO exprs(n,overflow) VALUES (2,1),(32767,1)",
            ):
                with self.assertRaises(psycopg.Error) as failure:
                    with self.db.transaction():
                        self.db.execute(sql)
                self.assertEqual("22003", failure.exception.sqlstate)
            self.assertEqual(
                3, self.db.execute("SELECT COUNT(*) FROM exprs").fetchone()[0]
            )
            self.db.execute(
                "ALTER TABLE exprs ADD COLUMN extra integer DEFAULT (4*5) NOT NULL"
            )
            self.assertEqual(
                [(20,), (20,), (20,)],
                self.db.execute("SELECT extra FROM exprs").fetchall(),
            )
            for sql in (
                "ALTER TABLE exprs ADD COLUMN bad integer GENERATED ALWAYS AS (g+1) STORED",
                "ALTER TABLE exprs ADD COLUMN bad integer GENERATED ALWAYS AS (bad+1) STORED",
            ):
                with self.assertRaises(psycopg.Error) as failure:
                    with self.db.transaction():
                        self.db.execute(sql)
                self.assertEqual("42P17", failure.exception.sqlstate)
            with self.assertRaises(psycopg.Error) as failure:
                with self.db.transaction():
                    self.db.execute("ALTER TABLE exprs ALTER COLUMN g SET DEFAULT 5")
            self.assertEqual("42601", failure.exception.sqlstate)

    def test_durable_membership_remainder_schema_contract(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TABLE exprs (n smallint, label text, bucket integer GENERATED ALWAYS AS (MOD(n,3)) STORED, CHECK (label IN ('ready','pending')), CHECK ((n>0) IS NOT FALSE OR n IN (-7,-3)))"
            )
            self.db.execute("CREATE INDEX by_total ON exprs ((n % 3)) INCLUDE (bucket)")
            self.db.execute(
                "INSERT INTO exprs(n,label) VALUES (-7,'ready'),(NULL,NULL)"
            )
            with self.assertRaises(psycopg.Error) as failure:
                with self.db.transaction():
                    self.db.execute(
                        "INSERT INTO exprs(n,label) VALUES (3,'pending'),(-1,'ready')"
                    )
            self.assertEqual("23514", failure.exception.sqlstate)
            self.assertEqual(
                [(-7, "ready", -1), (None, None, None)],
                self.db.execute(
                    "SELECT n,label,bucket FROM exprs ORDER BY n"
                ).fetchall(),
            )
            with self.assertRaises(psycopg.Error) as failure:
                with self.db.transaction():
                    self.db.execute("INSERT INTO exprs(n,label) VALUES (1,'invalid')")
            self.assertEqual("23514", failure.exception.sqlstate)
            self.assertEqual(
                (0,),
                self.db.execute(
                    "INSERT INTO exprs(n,label) VALUES (-3,'pending') RETURNING bucket"
                ).fetchone(),
            )

    def test_durable_numeric_expression_builtin_domains(self):
        import json
        import psycopg

        fixture = json.loads(
            (FIXTURES / "sql_numeric_expression_reference.json").read_text()
        )
        for entry in fixture["entries"]:
            with self.subTest(
                sql=entry["sql"], n=entry.get("n"), i=entry.get("i"), d=entry.get("d")
            ):
                with self.db.transaction(force_rollback=True):
                    self.db.execute(
                        "CREATE TABLE numeric_probe(n smallint,i integer,b bigint,f real,d double precision)"
                    )
                    self.db.execute(
                        "INSERT INTO numeric_probe VALUES (%s,%s,9007199254740993,16777216,%s)",
                        (entry.get("n", 1), entry.get("i", 1), entry.get("d", 2.5)),
                    )
                    if "error" in entry:
                        with self.assertRaises(psycopg.Error) as error:
                            with self.db.transaction():
                                self.db.execute(
                                    "SELECT " + entry["sql"] + " FROM numeric_probe"
                                ).fetchall()
                        self.assertEqual(entry["error"], error.exception.sqlstate)
                    else:
                        # float4 text is shortest-roundtrip for float4, not for
                        # Python's float64. Compare the actual binary value.
                        with self.db.cursor(binary=True) as cursor:
                            cursor.execute(
                                "SELECT " + entry["sql"] + " FROM numeric_probe"
                            )
                            self.assertEqual(entry["expected"], cursor.fetchone()[0])

    def test_nullable_catalog_check_index_and_precise_default_contract(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE items(n smallint, label text, cold bigint[])")
            self.db.execute(
                "ALTER TABLE items ADD CONSTRAINT positive CHECK (n > 0 AND lower(label) = 'ready')"
            )
            self.db.execute(
                "CREATE INDEX label_key ON items ((lower(label))) WHERE n > 0"
            )
            self.db.execute("ALTER TABLE items ALTER COLUMN n SET DEFAULT 7")
            self.db.execute(
                "INSERT INTO items(label,cold) VALUES ('READY','[-1:0]={9007199254740993,NULL}')"
            )
            self.db.execute("INSERT INTO items VALUES (NULL,'READY',NULL)")
            self.assertEqual(
                [(7, -1, 9007199254740993), (None, None, None)],
                self.db.execute(
                    "SELECT n,array_lower(cold,1),cold[-1] FROM items ORDER BY n NULLS LAST"
                ).fetchall(),
            )
            for n, label in ((-1, "READY"), (1, "wrong")):
                with self.subTest(n=n, label=label):
                    with self.assertRaises(psycopg.errors.CheckViolation):
                        with self.db.transaction():
                            self.db.execute(
                                "INSERT INTO items VALUES (%s,%s,NULL)", (n, label)
                            )
            # Assignment casts are retained and raise only when the default
            # is used, not while publishing the schema.
            self.db.execute("ALTER TABLE items ALTER COLUMN n SET DEFAULT 32768")
            with self.assertRaises(psycopg.errors.NumericValueOutOfRange):
                with self.db.transaction():
                    self.db.execute("INSERT INTO items(label) VALUES ('READY')")
            self.db.execute("ALTER TABLE items ALTER COLUMN n SET DEFAULT 7")
            # The durable native expression VM must retain this narrow domain.
            self.db.execute("INSERT INTO items VALUES (32767,'READY',NULL)")
            with self.assertRaises(psycopg.errors.NumericValueOutOfRange):
                with self.db.transaction():
                    self.db.execute("SELECT n+n FROM items WHERE n=32767").fetchall()

    def test_joined_returning_preserves_target_postimage_and_source_array_identity(
        self,
    ):
        import json
        from generate_sql_postgres_reference import execute

        fixture = json.loads(
            (FIXTURES / "sql_joined_returning_reference.json").read_text()
        )
        for entry in fixture["entries"]:
            with (
                self.subTest(sql=entry["sql"]),
                self.db.transaction(force_rollback=True),
            ):
                self.db.execute(
                    "CREATE TABLE target(id text, n bigint, a bigint[], j jsonb[], cold text)"
                )
                self.db.execute(
                    "CREATE TABLE source(id text, delta bigint, a smallint[])"
                )
                self.db.execute(
                    "INSERT INTO target VALUES ('a',1,'[-1:1]={9007199254740993,NULL,2}',ARRAY['null'::jsonb,NULL],'old'),('b',2,'[-1:1]={9007199254740993,NULL,2}',ARRAY['null'::jsonb,NULL],'old')"
                )
                self.db.execute(
                    "INSERT INTO source VALUES ('a',10,'[3:4]={3,NULL}'),(%s,20,'[3:4]={3,NULL}')",
                    ("a" if entry.get("duplicates") else "b",),
                )
                result = execute(
                    self.db,
                    {"id": "joined-returning", "sql": entry["sql"], "params": []},
                )
                target_values = (
                    ["3", None]
                    if entry["target_first"] == "3"
                    else ["9007199254740993", None, "2"]
                )
                expected = [
                    {
                        "dimensions": [
                            {
                                "length": len(target_values),
                                "lower_bound": entry["target_lower"],
                            }
                        ],
                        "values": target_values,
                        "sql_nulls": [False, True]
                        if len(target_values) == 2
                        else [False, True, False],
                    },
                    {
                        "dimensions": [{"length": 2, "lower_bound": 1}],
                        "values": [None, None],
                        "sql_nulls": [False, True],
                    },
                    {
                        "dimensions": [{"length": 2, "lower_bound": 3}],
                        "values": ["3", None],
                        "sql_nulls": [False, True],
                    },
                ]
                affected = 1 if entry.get("duplicates") else 2
                self.assertEqual(affected, result["affected"])
                self.assertEqual([1016, 3807, 1005], result["column_oids"])
                self.assertEqual(["a", "j", "a"], result["columns"])
                self.assertEqual([expected] * affected, result["rows"])
                self.assertEqual([[False] * 3] * affected, result["sql_nulls"])
                self.assertEqual(
                    0 if entry["sql"].startswith("DELETE") else 2,
                    self.db.execute("SELECT count(*) FROM target").fetchone()[0],
                )
                self.assertEqual(
                    2, self.db.execute("SELECT count(*) FROM source").fetchone()[0]
                )

    def test_joined_returning_subqueries_share_target_and_source_scope(self):
        import json
        from generate_sql_postgres_reference import execute

        fixture = json.loads(
            (FIXTURES / "sql_joined_returning_subquery_reference.json").read_text()
        )
        for entry in fixture["entries"]:
            with (
                self.subTest(sql=entry["sql"]),
                self.db.transaction(force_rollback=True),
            ):
                self.db.execute("CREATE TABLE target(id text,n bigint,cold text)")
                self.db.execute("CREATE TABLE source(id text,delta bigint)")
                self.db.execute("INSERT INTO target VALUES ('a',1,'old'),('b',2,'old')")
                self.db.execute("INSERT INTO source VALUES ('a',10),('b',20)")
                result = execute(
                    self.db,
                    {
                        "id": "joined-returning-subquery",
                        "sql": entry["sql"],
                        "params": [],
                    },
                )
                self.assertEqual(
                    [
                        [None if value is None else int(value) for value in row]
                        for row in entry["rows"]
                    ],
                    result["rows"],
                )
                self.assertEqual(2, result["affected"])
                self.assertEqual([20, 20, 20], result["column_oids"])
                self.assertEqual(["n", "delta", "d"], result["columns"])
                self.assertEqual(
                    [[value is None for value in row] for row in entry["rows"]],
                    result["sql_nulls"],
                )
                self.assertEqual(
                    []
                    if entry["sql"].startswith("DELETE")
                    else [(11, "new"), (22, "new")],
                    self.db.execute("SELECT n,cold FROM target ORDER BY id").fetchall(),
                )
                self.assertEqual(
                    [("a", 10), ("b", 20)],
                    self.db.execute("SELECT * FROM source ORDER BY id").fetchall(),
                )

    def test_joined_returning_subqueries_preserve_array_and_null_domains(self):
        from generate_sql_postgres_reference import execute

        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TABLE target(id text,a bigint[],j jsonb[],cold text)"
            )
            self.db.execute("CREATE TABLE source(id text,a smallint[])")
            self.db.execute(
                "INSERT INTO target VALUES ('a',ARRAY[1::bigint],ARRAY['null'::jsonb,NULL],'old')"
            )
            self.db.execute("INSERT INTO source VALUES ('a','[3:4]={3,NULL}')")
            result = execute(
                self.db,
                {
                    "id": "joined-returning-array-subquery",
                    "sql": "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t.id=s.id RETURNING t.a,t.j,(SELECT s.a),(SELECT x.a FROM source x WHERE x.id=s.id)",
                    "params": [],
                },
            )
            self.assertEqual([1016, 3807, 1005, 1005], result["column_oids"])
            self.assertEqual([[False] * 4], result["sql_nulls"])
            row = result["rows"][0]
            for index in (0, 2, 3):
                self.assertEqual(
                    {
                        "dimensions": [{"length": 2, "lower_bound": 3}],
                        "values": ["3", None],
                        "sql_nulls": [False, True],
                    },
                    row[index],
                )
            self.assertEqual([None, None], row[1]["values"])
            self.assertEqual([False, True], row[1]["sql_nulls"])

    def test_joined_returning_rejects_top_level_phase_functions(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE target(id text,n bigint)")
            self.db.execute("CREATE TABLE source(id text,delta bigint)")
            self.db.execute("INSERT INTO target VALUES ('a',1)")
            self.db.execute("INSERT INTO source VALUES ('a',10)")
            for expression, sqlstate in (
                ("row_number() OVER ()+(SELECT s.delta)", "42P20"),
                ("sum(t.n)+(SELECT s.delta)", "42803"),
            ):
                with self.subTest(expression=expression):
                    with self.assertRaises(psycopg.Error) as caught:
                        with self.db.transaction():
                            self.db.execute(
                                "UPDATE target t SET n=s.delta FROM source s WHERE t.id=s.id RETURNING "
                                + expression
                            )
                    self.assertEqual(sqlstate, caught.exception.sqlstate)
                    self.assertEqual(
                        [(1,)], self.db.execute("SELECT n FROM target").fetchall()
                    )

    def test_update_from_fanout_selects_one_coherent_source_match(self):
        from generate_sql_postgres_reference import execute

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE target(_id text,n bigint,cold text)")
            self.db.execute("CREATE TABLE source(id text,delta bigint)")
            self.db.execute("INSERT INTO target VALUES ('a',1,'old'),('b',2,'old')")
            self.db.execute("INSERT INTO source VALUES ('a',10),('a',20)")
            result = execute(
                self.db,
                {
                    "id": "joined-fanout",
                    "sql": "UPDATE target t SET n=s.delta,cold='new' FROM source s WHERE t._id=s.id RETURNING t.n,s.delta,s.delta+1",
                    "params": [],
                },
            )
            self.assertEqual(1, result["affected"])
            self.assertEqual([20, 20, 20], result["column_oids"])
            self.assertEqual([[False] * 3], result["sql_nulls"])
            row = [int(cell) for cell in result["rows"][0]]
            self.assertIn(row, [[10, 10, 11], [20, 20, 21]])
            self.assertEqual(
                [(row[0], "new"), (2, "old")],
                self.db.execute("SELECT n,cold FROM target ORDER BY _id").fetchall(),
            )

    def test_declared_array_columns_roundtrip_all_builtin_domains_and_bounds(self):
        examples = {
            "boolean": True,
            "int16": "32767",
            "int32": "2147483647",
            "int64": "9007199254740993",
            "float32": 1.5,
            "float64": -0.25,
            "text": 'NULL,{escaped}"\\雪',
            "uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "jsonb": None,
        }
        for element, first in examples.items():
            with (
                self.subTest(element=element),
                self.db.transaction(force_rollback=True),
            ):
                prop = {"type": "sql_array", "x-antfly-sql-type": element}
                value = {
                    "dimensions": [{"length": 2, "lower_bound": -3}],
                    "values": [first, None],
                    "sql_nulls": [False, True],
                }
                empty = {"dimensions": [], "values": [], "sql_nulls": []}
                create_table(
                    self.db,
                    "typed_seed",
                    {"id": {"type": "integer"}, "a": prop},
                    [
                        {"key": "a", "value": {"id": 1, "a": value}},
                        {"key": "b", "value": {"id": 2, "a": empty}},
                        {"key": "c", "value": {"id": 3, "a": None}},
                    ],
                )
                result = execute(
                    self.db,
                    self.case("SELECT a FROM typed_seed ORDER BY id"),
                    read=True,
                )
                self.assertEqual([[value], [empty], [None]], result["rows"])
                self.assertEqual([[False], [False], [True]], result["sql_nulls"])

    def test_declared_multidimensional_array_seed_retains_row_major_nulls(self):
        prop = {"type": "sql_array", "x-antfly-sql-type": "int64"}
        value = {
            "dimensions": [
                {"length": 2, "lower_bound": 0},
                {"length": 2, "lower_bound": -1},
            ],
            "values": ["1", None, "9007199254740993", "4"],
            "sql_nulls": [False, True, False, False],
        }
        with self.db.transaction(force_rollback=True):
            create_table(
                self.db,
                "matrix_seed",
                {"a": prop},
                [{"key": "a", "value": {"a": value}}],
            )
            result = execute(self.db, self.case("SELECT a FROM matrix_seed"), read=True)
            self.assertEqual([[value]], result["rows"])

    def test_array_seed_rejects_implicit_types_domains_and_noncanonical_shapes(self):
        prop = {"type": "sql_array", "x-antfly-sql-type": "int16"}
        good = {
            "dimensions": [{"length": 1, "lower_bound": 1}],
            "values": ["1"],
            "sql_nulls": [False],
        }
        for bad in (
            [1],
            {**good, "extra": 1},
            {**good, "values": [1]},
            {**good, "values": ["32768"]},
            {**good, "sql_nulls": [1]},
            {**good, "sql_nulls": [True]},
            {**good, "values": []},
            {**good, "dimensions": [{"length": 0, "lower_bound": 1}]},
            {**good, "dimensions": [{"length": 1, "lower_bound": 2**31}]},
        ):
            with self.subTest(value=bad), self.assertRaises(ValueError):
                array_seed_text(prop, bad)
        for declaration in (
            {"type": "sql_array"},
            {"type": "sql_array", "x-antfly-sql-type": "not-a-type"},
        ):
            with self.subTest(declaration=declaration), self.assertRaises(ValueError):
                pg_type(declaration)
        self.assertEqual("jsonb", pg_type({"type": "array"}))

    def test_array_result_reference_preserves_bounds_width_and_json_nulls(self):
        cases = (
            (
                "'[-1:1]={9007199254740993,NULL,2}'::bigint[]",
                1016,
                {
                    "dimensions": [{"length": 3, "lower_bound": -1}],
                    "values": ["9007199254740993", None, "2"],
                    "sql_nulls": [False, True, False],
                },
            ),
            (
                '\'[0:2]={"null",NULL,"{\\"x\\":1}"}\'::jsonb[]',
                3807,
                {
                    "dimensions": [{"length": 3, "lower_bound": 0}],
                    "values": [None, None, {"x": 1}],
                    "sql_nulls": [False, True, False],
                },
            ),
            (
                "ARRAY[]::integer[]",
                1007,
                {"dimensions": [], "values": [], "sql_nulls": []},
            ),
            (
                "ARRAY[[1,NULL],[2,3]]::smallint[]",
                1005,
                {
                    "dimensions": [
                        {"length": 2, "lower_bound": 1},
                        {"length": 2, "lower_bound": 1},
                    ],
                    "values": ["1", None, "2", "3"],
                    "sql_nulls": [False, True, False, False],
                },
            ),
        )
        for expression, oid, expected in cases:
            with self.subTest(sql=expression):
                result = execute(self.db, self.case("SELECT " + expression), read=True)
                self.assertEqual([oid], result["column_oids"])
                self.assertEqual([[expected]], result["rows"])
                self.assertEqual([[False]], result["sql_nulls"])
        result = execute(self.db, self.case("SELECT NULL::bigint[]"), read=True)
        self.assertEqual([[None]], result["rows"])
        self.assertEqual([[True]], result["sql_nulls"])

    def test_array_result_reference_rejects_corrupt_headers_and_partial_cells(self):
        import struct

        good = struct.pack("!iiiiiii", 1, 0, 20, 1, -1, 8, 0) + struct.pack("!i", 1)
        self.assertEqual(["1"], array_reference(good, 1016)["values"])
        for end in range(len(good)):
            with self.subTest(end=end):
                with self.assertRaises(ValueError):
                    array_reference(good[:end], 1016)
        for bad in (
            good + b"x",
            struct.pack("!iii", 7, 0, 20),
            struct.pack("!iii", 0, 0, 23),
            struct.pack("!iiiii", 1, 0, 20, 65537, 1),
        ):
            with self.assertRaises(ValueError):
                array_reference(bad, 1016)

    def test_native_array_mutation_sequence_preserves_bounds_and_null_provenance(self):
        cases = (
            (
                "INSERT INTO items (_id,a,j,n) VALUES ('a','[-1:1]={9007199254740993,NULL,2}'::bigint[],ARRAY['null'::jsonb,NULL],1) RETURNING a,j",
                -1,
                "9007199254740993",
            ),
            (
                "UPDATE items SET n=2 WHERE _id='a' RETURNING a,j",
                -1,
                "9007199254740993",
            ),
            (
                "INSERT INTO items (_id,a,j,n) SELECT 'b',a,j,n FROM items WHERE _id='a' RETURNING a,j",
                -1,
                "9007199254740993",
            ),
            (
                "UPDATE items SET a='[3:4]={9223372036854775807,NULL}'::bigint[] WHERE _id='b' RETURNING a,j",
                3,
                "9223372036854775807",
            ),
            ("DELETE FROM items WHERE _id='b' RETURNING a,j", 3, "9223372036854775807"),
            ("UPDATE items SET a=NULL WHERE _id='a' RETURNING a,j", None, None),
        )
        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TEMP TABLE items (_id text PRIMARY KEY,a bigint[],j jsonb[],n bigint)"
            )
            for statement, lower, first in cases:
                with self.subTest(sql=statement):
                    result = execute(self.db, self.case(statement))
                    self.assertEqual([1016, 3807], result["column_oids"])
                    self.assertEqual([[lower is None, False]], result["sql_nulls"])
                    self.assertEqual(1, len(result["rows"]))
                    array, json_array = result["rows"][0]
                    if lower is None:
                        self.assertIsNone(array)
                    else:
                        self.assertEqual(lower, array["dimensions"][0]["lower_bound"])
                        self.assertEqual(first, array["values"][0])
                        self.assertTrue(array["sql_nulls"][1])
                    self.assertEqual([None, None], json_array["values"])
                    self.assertEqual([False, True], json_array["sql_nulls"])

    def test_joined_array_mutations_preserve_domains_bounds_and_returning_scope(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TEMP TABLE target(_id text PRIMARY KEY,n bigint,cold text,a bigint[],j jsonb[]);"
                "CREATE TEMP TABLE source(id text,a smallint[],delta bigint)"
            )
            self.db.execute(
                "INSERT INTO target SELECT id,1,'old','[-1:1]={9007199254740993,NULL,2}',ARRAY['null'::jsonb,NULL] FROM (VALUES ('a'),('b')) v(id);"
                "INSERT INTO source SELECT _id,'[3:4]={3,NULL}',10 FROM target"
            )
            for sql, first, lower in (
                (
                    "UPDATE target t SET n=t.n+s.delta,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j",
                    9007199254740993,
                    -1,
                ),
                (
                    "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j",
                    3,
                    3,
                ),
                (
                    "UPDATE target t SET a='[-1:1]={9007199254740993,NULL,2}',cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j",
                    9007199254740993,
                    -1,
                ),
                (
                    "UPDATE target t SET a=$1,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j",
                    9223372036854775807,
                    5,
                ),
                (
                    "UPDATE target t SET a=ARRAY[1::smallint,NULL],cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j",
                    1,
                    1,
                ),
                (
                    "UPDATE target t SET a=NULL,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j",
                    None,
                    None,
                ),
                (
                    "DELETE FROM target t USING source s WHERE t._id=s.id RETURNING t.a,t.j",
                    9007199254740993,
                    -1,
                ),
            ):
                with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                    observer = sql + ",array_lower(t.a,1),t.j[1] IS NULL,t.j[2] IS NULL"
                    if "$1" in sql:
                        self.db.execute("PREPARE antfly_joined_array AS " + observer)
                        try:
                            rows = self.db.execute(
                                "EXECUTE antfly_joined_array('[5:6]={9223372036854775807,NULL}')"
                            ).fetchall()
                        finally:
                            self.db.execute("DEALLOCATE antfly_joined_array")
                    else:
                        rows = self.db.execute(observer).fetchall()
                    self.assertEqual(2, len(rows))
                    for row in rows:
                        if first is None:
                            self.assertIsNone(row[0])
                        else:
                            self.assertEqual(first, row[0][0])
                            self.assertIsNone(row[0][1])
                        self.assertEqual((lower, False, True), row[2:])
            for sql, state in (
                ("UPDATE target t SET a=s.a FROM source s RETURNING a", "42702"),
                ("DELETE FROM target t USING source s RETURNING a", "42702"),
                (
                    "UPDATE target t SET a=ARRAY['1'] FROM source s RETURNING t.a",
                    "42804",
                ),
                ("UPDATE target t SET a=ARRAY[] FROM source s RETURNING t.a", "42P18"),
            ):
                with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                    with self.assertRaises(psycopg.Error) as error:
                        self.db.execute("EXPLAIN " + sql)
                    self.assertEqual(state, error.exception.sqlstate)

    def test_merge_array_arm_assignment_domains(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TEMP TABLE target(_id text PRIMARY KEY DEFAULT 'generated',a bigint[],j jsonb[]);"
                "CREATE TEMP TABLE source(id text,a smallint[],text_values text[]);"
                "INSERT INTO target VALUES ('matched','{0}',ARRAY['null'::jsonb,NULL]);"
                "INSERT INTO source VALUES ('matched','[3:4]={3,NULL}','{1}'),"
                "('inserted','[3:4]={3,NULL}','{1}')"
            )
            prefix = "MERGE INTO target t USING source s ON t._id=s.id "
            for expression, first, lower in (
                ("s.a", 3, 3),
                ("'[-1:1]={9007199254740993,NULL,2}'", 9007199254740993, -1),
                ("ARRAY[1::smallint,NULL]", 1, 1),
                ("NULL", None, None),
                ("$1", 9223372036854775807, 5),
            ):
                with (
                    self.subTest(expression=expression),
                    self.db.transaction(force_rollback=True),
                ):
                    sql = (
                        prefix
                        + f"WHEN MATCHED THEN UPDATE SET a={expression} "
                        + f"WHEN NOT MATCHED THEN INSERT (a) VALUES ({expression}) "
                        + "RETURNING t.a,s.a,t.j,array_lower(t.a,1),"
                        + "pg_typeof(t.a)::text,pg_typeof(s.a)::text"
                    )
                    if expression == "$1":
                        self.db.execute("PREPARE antfly_merge_array AS " + sql)
                        try:
                            self.assertEqual(
                                "bigint[]",
                                self.db.execute(
                                    "SELECT parameter_types[1]::text FROM pg_prepared_statements "
                                    "WHERE name='antfly_merge_array'"
                                ).fetchone()[0],
                            )
                            rows = self.db.execute(
                                "EXECUTE antfly_merge_array('[5:6]={9223372036854775807,NULL}')"
                            ).fetchall()
                        finally:
                            self.db.execute("DEALLOCATE antfly_merge_array")
                    else:
                        rows = self.db.execute(sql).fetchall()
                    self.assertEqual(2, len(rows))
                    for row in rows:
                        if first is None:
                            self.assertIsNone(row[0])
                        else:
                            self.assertEqual(first, row[0][0])
                            self.assertIsNone(row[0][1])
                        self.assertEqual([3, None], row[1])
                        self.assertEqual((lower, "bigint[]", "smallint[]"), row[3:])
            for expression, state in (
                ("s.text_values", "42804"),
                ("ARRAY['1']", "42804"),
                ("ARRAY[]", "42P18"),
            ):
                with (
                    self.subTest(expression=expression),
                    self.db.transaction(force_rollback=True),
                ):
                    with self.assertRaises(psycopg.Error) as error:
                        self.db.execute(
                            "EXPLAIN "
                            + prefix
                            + f"WHEN MATCHED THEN UPDATE SET a={expression}"
                        )
                    self.assertEqual(state, error.exception.sqlstate)

    def test_array_conflict_mutations_preserve_typed_preimages_and_excluded(self):
        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TEMP TABLE items(_id text PRIMARY KEY,a bigint[],j jsonb[],n bigint)"
            )
            self.db.execute(
                "INSERT INTO items VALUES ('a','[-1:1]={9007199254740993,NULL,2}',ARRAY['null'::jsonb,NULL],1)"
            )
            self.db.execute("INSERT INTO items SELECT 'q',a,j,n FROM items")
            for sql, bounds, first in (
                (
                    "INSERT INTO items (_id,a,j,n) VALUES ('a',ARRAY[0],ARRAY[NULL::jsonb],9) ON CONFLICT (_id) DO UPDATE SET n=excluded.n RETURNING a,j",
                    -1,
                    9007199254740993,
                ),
                (
                    "INSERT INTO items (_id,a,j,n) VALUES ('a','[3:4]={9223372036854775807,NULL}',ARRAY['null'::jsonb,NULL],9) ON CONFLICT (_id) DO UPDATE SET a=excluded.a,j=excluded.j WHERE items.a='[-1:1]={9007199254740993,NULL,2}'::bigint[] RETURNING a,j",
                    3,
                    9223372036854775807,
                ),
                (
                    "INSERT INTO items (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a='[-1:1]={9007199254740993,NULL,2}' RETURNING a,j",
                    -1,
                    9007199254740993,
                ),
                (
                    "INSERT INTO items (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a=ARRAY[1::smallint,NULL] RETURNING a,j",
                    1,
                    1,
                ),
                (
                    "INSERT INTO items (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a=(SELECT a FROM items WHERE _id='q') RETURNING a,j",
                    -1,
                    9007199254740993,
                ),
                (
                    "INSERT INTO items (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a=coalesce((SELECT a FROM items WHERE _id='q'),excluded.a) RETURNING a,j",
                    -1,
                    9007199254740993,
                ),
            ):
                with self.subTest(sql=sql):
                    row = self.db.execute(sql).fetchone()
                    self.assertEqual(first, row[0][0])
                    self.assertIsNone(row[0][1])
                    observed = self.db.execute(
                        "SELECT array_lower(a,1),j[1] IS NULL,j[2] IS NULL FROM items WHERE _id='a'"
                    ).fetchone()
                    self.assertEqual((bounds, False, True), observed)
            row = self.db.execute(
                "INSERT INTO items (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a=(SELECT a FROM items WHERE _id='absent') RETURNING a,j"
            ).fetchone()
            self.assertIsNone(row[0])
            self.assertEqual([None, None], row[1])

    def test_builtin_array_assignment_matrix_and_parameter_contexts(self):
        import psycopg

        types = (
            "smallint",
            "integer",
            "bigint",
            "real",
            "double precision",
            "boolean",
            "text",
            "uuid",
            "jsonb",
        )
        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TEMP TABLE assignment_probe("
                + ",".join(f"c{i} {kind}[]" for i, kind in enumerate(types))
                + ")"
            )
            for source_index, source in enumerate(types):
                for target_index, target in enumerate(types):
                    allowed = (
                        source_index == target_index
                        or target == "text"
                        or (source_index < 5 and target_index < 5)
                    )
                    query = f"EXPLAIN INSERT INTO assignment_probe(c{target_index}) SELECT NULL::{source}[]"
                    with self.subTest(source=source, target=target):
                        with self.db.transaction(force_rollback=True):
                            if allowed:
                                self.db.execute(query)
                            else:
                                with self.assertRaises(
                                    psycopg.errors.DatatypeMismatch
                                ) as error:
                                    self.db.execute(query)
                                self.assertEqual("42804", error.exception.sqlstate)
            for query, state in (
                ("INSERT INTO assignment_probe(c2) VALUES (ARRAY[])", "42P18"),
                ("INSERT INTO assignment_probe(c2) VALUES (ARRAY['1'])", "42804"),
                (
                    "PREPARE antfly_array_conflict AS INSERT INTO assignment_probe(c0,c2) VALUES ($1,$1)",
                    "42P08",
                ),
            ):
                with self.subTest(sql=query), self.db.transaction(force_rollback=True):
                    with self.assertRaises(psycopg.Error) as error:
                        self.db.execute(query)
                    self.assertEqual(state, error.exception.sqlstate)
            self.db.execute(
                "PREPARE antfly_array_assignment AS INSERT INTO assignment_probe(c0) VALUES ($1)"
            )
            try:
                observed = self.db.execute(
                    "SELECT parameter_types::text FROM pg_prepared_statements WHERE name='antfly_array_assignment'"
                ).fetchone()[0]
                self.assertEqual("{smallint[]}", observed)
            finally:
                self.db.execute("DEALLOCATE antfly_array_assignment")

    def test_array_common_types_precede_set_identity(self):
        cases = (
            (
                "SELECT count(*) FROM (SELECT ARRAY[1,NULL]::int2[] a UNION SELECT ARRAY[1,NULL]::int8[]) q",
                [(1,)],
            ),
            (
                "SELECT count(*) FROM (SELECT ARRAY[1]::int4[] a INTERSECT SELECT ARRAY[1]::float8[]) q",
                [(1,)],
            ),
            (
                "SELECT count(*) FROM (SELECT ARRAY[1]::int4[] a EXCEPT SELECT ARRAY[1]::float8[]) q",
                [(0,)],
            ),
            (
                "SELECT cardinality(a),2.5=ANY(a) FROM (VALUES(ARRAY[1]::int2[]),(ARRAY[2.5]::float4[])) q(a) ORDER BY 1,2",
                [(1, False), (1, True)],
            ),
            (
                "SELECT count(*) FROM ((SELECT ARRAY[16777216]::int8[] a UNION SELECT ARRAY[16777217]::int8[]) UNION ALL SELECT ARRAY[1]::float4[]) q",
                [(3,)],
            ),
            (
                "SELECT count(*) FROM (SELECT ARRAY[16777216]::int8[] a UNION SELECT ARRAY[16777217]::float4[]) q",
                [(1,)],
            ),
            (
                "SELECT cardinality(a),1=ANY(a) FROM (SELECT '{1,NULL}' a UNION SELECT ARRAY[1,NULL]::int4[]) q",
                [(2, True)],
            ),
            (
                "SELECT count(*) FROM (SELECT NULL::int2[] a UNION SELECT NULL::float8[]) q",
                [(1,)],
            ),
            (
                "SELECT x FROM (VALUES(NULL),(NULL),(1)) q(x) ORDER BY x",
                [(1,), (None,), (None,)],
            ),
        )
        for sql, expected in cases:
            with self.subTest(sql=sql):
                self.assertEqual(expected, self.db.execute(sql).fetchall())

    def test_pairwise_set_parameter_domains(self):
        cases = (
            ("SELECT $1 AS x UNION SELECT 1 UNION SELECT 1.5", "{integer}"),
            ("SELECT 1.5 AS x UNION SELECT 1 UNION SELECT $1", "{numeric}"),
            ("SELECT 1 AS x UNION SELECT NULL UNION SELECT $1", "{integer}"),
            ("SELECT d.x FROM (SELECT $1::bigint AS x) d UNION SELECT 1", "{bigint}"),
        )
        for sql, expected in cases:
            with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                self.db.execute("PREPARE common_type_probe AS " + sql)
                actual = self.db.execute(
                    "SELECT parameter_types::text FROM pg_prepared_statements "
                    "WHERE name='common_type_probe'"
                ).fetchone()[0]
                self.assertEqual(expected, actual)
                self.db.execute("DEALLOCATE common_type_probe")

    def test_set_unknown_and_derived_boundaries_reject_invalid_promotion(self):
        import psycopg

        cases = (
            "SELECT NULL UNION SELECT NULL UNION SELECT 1",
            "SELECT '1'::text UNION SELECT 1",
            "SELECT ARRAY[1]::int4[] UNION SELECT ARRAY[TRUE]::boolean[]",
            "SELECT ARRAY[1]::int4[] UNION SELECT ARRAY['1']::text[]",
            "SELECT ARRAY[1]::int4[] UNION SELECT 1",
            "SELECT x FROM (SELECT NULL x) q UNION SELECT 1",
            "SELECT d.x FROM (SELECT $1 AS x) d UNION SELECT 1",
            "WITH a AS (SELECT $1 AS x) SELECT d.x FROM (SELECT x FROM a) d UNION SELECT 1",
            "SELECT NULL AS x UNION SELECT NULL UNION SELECT $1 UNION SELECT 1",
            "SELECT $1 AS x UNION SELECT NULL UNION SELECT 1",
            "SELECT COALESCE($1,NULL) AS x UNION SELECT 1",
            "WITH n AS (SELECT NULL AS x) SELECT COALESCE(n.x,$1) AS x FROM n UNION SELECT 1",
        )
        for sql in cases:
            with self.subTest(sql=sql), self.assertRaises(psycopg.Error) as error:
                with self.db.transaction(force_rollback=True):
                    self.db.execute("PREPARE invalid_type_probe AS " + sql)
            self.assertEqual(
                "42846" if "UNION SELECT ARRAY[" in sql else "42804",
                error.exception.sqlstate,
            )

    def test_assignment_and_recursive_unknown_boundaries(self):
        import psycopg

        cases = (
            (
                "INSERT INTO assignment_type_probe (_id,n,j) SELECT 'a',COALESCE($1,NULL),'null'::jsonb",
                "42804",
            ),
            (
                "INSERT INTO assignment_type_probe (_id,n,j) SELECT 'a',$1,'null'::jsonb UNION ALL SELECT 'b',$1,'null'::jsonb",
                "42804",
            ),
            (
                "INSERT INTO assignment_type_probe (_id,n,j) SELECT 'a',9007199254740993,NULL UNION ALL SELECT 'b',9007199254740993,NULL",
                "42804",
            ),
            (
                "INSERT INTO assignment_type_probe (_id,n,j) WITH q AS(SELECT 'a' k,9007199254740993 n,NULL j) SELECT k,n,j FROM q",
                "42804",
            ),
            (
                "INSERT INTO assignment_type_probe (_id,n) VALUES('a',(SELECT $1)),('b',8)",
                "42804",
            ),
            (
                "WITH RECURSIVE r(n) AS (SELECT $1 UNION ALL SELECT n+1 FROM r WHERE n<$2) SELECT n FROM r",
                "42883",
            ),
            (
                "SELECT (SELECT $1 FROM (SELECT 1 AS y) i WHERE i.y=o.x)+1 FROM (SELECT $2 AS x) o WHERE o.x=1",
                "42883",
            ),
        )
        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TEMP TABLE assignment_type_probe (_id text,n bigint,j jsonb)"
            )
            for sql, state in cases:
                with self.subTest(sql=sql), self.assertRaises(psycopg.Error) as error:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute("PREPARE assignment_type_statement AS " + sql)
                self.assertEqual(state, error.exception.sqlstate)

    def test_ordered_set_streaming_reducer_rank_and_tie_contracts(self):
        for direction in ("ASC", "DESC"):
            with self.subTest(direction=direction):
                order = f"WITHIN GROUP (ORDER BY x {direction})"
                values = self.db.execute(
                    "SELECT "
                    + ",".join(
                        f"{function} {order}"
                        for function in (
                            "mode()",
                            "percentile_cont(0.5)",
                            "percentile_disc(0.5)",
                            "percentile_cont(0.25)",
                            "percentile_cont(0.75)",
                            "percentile_cont(NULL::float8)",
                            "percentile_disc(0)",
                            "percentile_disc(1)",
                        )
                    )
                    + " FROM (SELECT ((511-n)%16)::bigint x "
                    "FROM generate_series(0,511)n UNION ALL SELECT NULL) t"
                ).fetchone()
                self.assertEqual(
                    (0, 7.5, 7, 3.75, 11.25, None, 0, 15)
                    if direction == "ASC"
                    else (15, 7.5, 8, 11.25, 3.75, None, 15, 0),
                    values,
                )

    def test_ordered_set_empty_direct_arguments_and_exact_discrete_values(self):
        import psycopg

        with self.assertRaises(psycopg.errors.NumericValueOutOfRange) as error:
            with self.db.transaction(force_rollback=True):
                self.db.execute(
                    "SELECT percentile_cont(-1) WITHIN GROUP (ORDER BY x) "
                    "FROM (SELECT 1 AS x WHERE FALSE) t"
                )
        self.assertEqual("22003", error.exception.sqlstate)
        self.assertEqual(
            (9007199254740993,),
            self.db.execute(
                "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) "
                "FROM (VALUES(9007199254740993::bigint),(9007199254740995))t(x)"
            ).fetchone(),
        )
        self.assertEqual(
            ("b", "b", None, "z"),
            self.db.execute(
                "SELECT mode() WITHIN GROUP(ORDER BY x),"
                "percentile_disc(0.5) WITHIN GROUP(ORDER BY x),"
                "percentile_disc(NULL::float8) WITHIN GROUP(ORDER BY x),"
                "percentile_disc(1) WITHIN GROUP(ORDER BY x) "
                "FROM (VALUES('z'),('a'),('a'),('b'),('b'),('b'))t(x)"
            ).fetchone(),
        )

    def test_ordered_set_binding_domains_and_clause_kind(self):
        import psycopg

        cases = (
            ("SELECT COUNT(*) WITHIN GROUP (ORDER BY 1)", "42809"),
            ("SELECT percentile_cont(0.5)", "42883"),
            ("SELECT percentile_cont(0.5,1.0)", "42809"),
            ("SELECT mode()", "42883"),
            ("SELECT mode(1)", "42809"),
            (
                "SELECT percentile_cont(x) WITHIN GROUP (ORDER BY x) "
                "FROM (SELECT 1 AS x,2 AS g) t GROUP BY g",
                "42803",
            ),
            (
                "SELECT percentile_cont(SUM(x)) WITHIN GROUP (ORDER BY x) "
                "FROM (SELECT 1 AS x) t",
                "42803",
            ),
            ("SELECT mode() WITHIN GROUP (ORDER BY 1) OVER ()", "0A000"),
            (
                "SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY true)",
                "42883",
            ),
        )
        for sql, code in cases:
            with self.subTest(sql=sql):
                with self.assertRaises(psycopg.Error) as error:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(sql)
                self.assertEqual(code, error.exception.sqlstate)
        self.assertEqual(
            [(1, 2.2)],
            self.db.execute(
                "SELECT g,percentile_cont(g/10.0) WITHIN GROUP (ORDER BY x) "
                "FROM (SELECT 1 AS g,2 AS x UNION ALL SELECT 1,4) t GROUP BY g"
            ).fetchall(),
        )
        self.assertEqual(
            [([1.5, None, 2.5],)],
            self.db.execute(
                "SELECT percentile_cont(ARRAY[0.25,NULL,0.75]) "
                "WITHIN GROUP (ORDER BY x) "
                "FROM (SELECT 1 AS x UNION ALL SELECT 3) t"
            ).fetchall(),
        )

    def test_ordered_set_grouped_execution_domains(self):
        cases = (
            (
                "SELECT mode() WITHIN GROUP (ORDER BY t.x DESC) FILTER (WHERE t.x>0) FROM (SELECT 1 AS x UNION ALL SELECT 3) t",
                [(3,)],
            ),
            (
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x),percentile_disc(0.5) WITHIN GROUP (ORDER BY x),mode() WITHIN GROUP (ORDER BY x),COUNT(*) FROM (SELECT 1 AS x UNION ALL SELECT 3 UNION ALL SELECT 3 UNION ALL SELECT 7) t",
                [(3.0, 3, 3, 4)],
            ),
            (
                "SELECT g,percentile_disc(NULL) WITHIN GROUP (ORDER BY x),mode() WITHIN GROUP (ORDER BY x) FROM (SELECT 2 AS g,10 AS x UNION ALL SELECT 1,3 UNION ALL SELECT 2,20 UNION ALL SELECT 1,1) t GROUP BY g ORDER BY g",
                [(1, None, 1), (2, None, 10)],
            ),
            (
                "SELECT g,mode() WITHIN GROUP (ORDER BY x) FILTER (WHERE x>5),percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM (SELECT 2 AS g,10 AS x UNION ALL SELECT 1,3 UNION ALL SELECT 2,20 UNION ALL SELECT 1,1) t GROUP BY g HAVING COUNT(*)=2 ORDER BY g",
                [(1, None, 2.0), (2, 10, 15.0)],
            ),
            (
                "SELECT percentile_cont(NULL) WITHIN GROUP (ORDER BY x),mode() WITHIN GROUP (ORDER BY x),COUNT(*) FROM (SELECT 1 AS x) t WHERE false",
                [(None, None, 0)],
            ),
            (
                "SELECT array_length(percentile_cont(ARRAY[0.25,NULL,0.75]) WITHIN GROUP (ORDER BY x),1) FROM (SELECT 1 AS x UNION ALL SELECT 3) t",
                [(3,)],
            ),
            (
                "SELECT percentile_cont(ARRAY[0.25,NULL,0.75]) WITHIN GROUP (ORDER BY x) IS NOT DISTINCT FROM ARRAY[1.5,NULL,2.5]::float8[] FROM (SELECT 1 AS x UNION ALL SELECT 3) t",
                [(True,)],
            ),
            (
                "SELECT percentile_disc('[0:2]={0.25,NULL,0.75}'::float8[]) WITHIN GROUP (ORDER BY x) IS NOT DISTINCT FROM '[0:2]={1,NULL,3}'::bigint[] FROM (SELECT 1::bigint AS x UNION ALL SELECT 3::bigint) t",
                [(True,)],
            ),
            (
                "SELECT percentile_cont('{}'::float8[]) WITHIN GROUP (ORDER BY x) IS NOT DISTINCT FROM '{}'::float8[] FROM (SELECT 1 AS x UNION ALL SELECT 3) t",
                [(True,)],
            ),
            (
                "SELECT array_length(percentile_disc(ARRAY[[0.25,NULL],[0.75,1.0]]) WITHIN GROUP (ORDER BY x),2) FROM (SELECT 1 AS x UNION ALL SELECT 3) t",
                [(2,)],
            ),
            (
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FILTER(WHERE x>10),mode() WITHIN GROUP(ORDER BY x) FROM (SELECT 1 AS x UNION ALL SELECT 3) t",
                [(None, 1)],
            ),
        )
        for sql, rows in cases:
            with self.subTest(sql=sql):
                self.assertEqual(rows, self.db.execute(sql).fetchall())

    def test_original_grouped_output_labels_are_not_visible_to_having(self):
        import json
        from pathlib import Path
        import psycopg

        inventory = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_parity_inventory.json"
            ).read_text()
        )
        cases = [
            case
            for case in inventory["entries"]
            if case["id"] in {"sql-0020", "sql-1254", "sql-1255"}
        ]
        self.assertEqual(3, len(cases))
        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE usage_records(status text)")
            for case in cases:
                with self.subTest(id=case["id"]):
                    with self.assertRaises(psycopg.errors.UndefinedColumn) as error:
                        with self.db.transaction(force_rollback=True):
                            self.db.execute(case["sql"])
                    self.assertEqual("42703", error.exception.sqlstate)

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

    def test_original_generated_subqueries_are_not_postgres_features(self):
        import json
        import psycopg

        entries = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        selected = [case for case in entries if case["id"] in ("sql-0672", "sql-0673")]
        self.assertEqual(2, len(selected))
        for case in selected:
            with self.subTest(id=case["id"]), self.db.transaction(force_rollback=True):
                if case["sql"].startswith("ALTER"):
                    self.db.execute("CREATE TABLE generated_usage_records(id text)")
                with self.assertRaises(psycopg.errors.FeatureNotSupported) as failure:
                    with self.db.transaction():
                        self.db.execute(case["sql"])
                self.assertEqual("0A000", failure.exception.sqlstate)

    def case(self, sql, params=()):
        return {"id": "sql-0001", "sql": sql, "params": params}

    def test_window_order_aliases_are_standalone_not_expression_variables(self):
        import psycopg

        rejected = [
            "SELECT row_number() OVER () AS n ORDER BY n+1",
            "SELECT row_number() OVER () AS n ORDER BY CAST(n AS bigint)",
            "SELECT row_number() OVER () AS n ORDER BY coalesce(n,0)",
            "SELECT row_number() OVER () AS n ORDER BY CASE WHEN TRUE THEN n ELSE 0 END",
            "SELECT row_number() OVER () ORDER BY row_number+1",
            'SELECT row_number() OVER () AS "n.total" ORDER BY "n.total"+1',
            "SELECT x,row_number() OVER (ORDER BY x) AS n FROM (SELECT 1 AS x) t ORDER BY n+1",
        ]
        for sql in rejected:
            with self.subTest(sql=sql):
                with self.db.transaction(force_rollback=True):
                    with self.assertRaises(psycopg.errors.UndefinedColumn) as error:
                        self.db.execute(sql)
                    self.assertEqual(error.exception.sqlstate, "42703")
        prefix = "SELECT -x AS x,row_number() OVER (ORDER BY x) AS n FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY "
        self.assertEqual(
            self.db.execute(prefix + "x DESC").fetchall(),
            [(-1, 1), (-2, 2), (-3, 3)],
        )
        self.assertEqual(
            self.db.execute(prefix + "(x+0) DESC").fetchall(),
            [(-3, 3), (-2, 2), (-1, 1)],
        )
        for sql in [
            "SELECT row_number() OVER (ORDER BY 1),row_number() OVER (ORDER BY 2) ORDER BY row_number",
            "SELECT row_number() OVER () AS n,rank() OVER () AS n ORDER BY n",
        ]:
            with self.subTest(sql=sql):
                with self.assertRaises(psycopg.errors.AmbiguousColumn) as error:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(sql)
                self.assertEqual("42702", error.exception.sqlstate)

    def test_original_window_output_expression_orders_are_not_postgres_features(self):
        import json
        from pathlib import Path
        import psycopg
        from generate_sql_postgres_reference import create_table, properties

        fixtures = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
        )
        profile = json.loads((fixtures / "sql_read_campaign_profile.json").read_text())
        inventory = json.loads((fixtures / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        cases = [case for case in inventory if case["id"] in {"sql-1219", "sql-1373"}]
        self.assertEqual(2, len(cases))
        self.assertNotIn("row_num", properties(profile["schema"]))
        with self.db.transaction(force_rollback=True):
            create_table(
                self.db, "usage_records", properties(profile["schema"]), profile["rows"]
            )
            for case in cases:
                with self.subTest(id=case["id"]):
                    with self.assertRaises(psycopg.errors.UndefinedColumn) as error:
                        with self.db.transaction(force_rollback=True):
                            self.db.execute(case["sql"])
                    self.assertEqual("42703", error.exception.sqlstate)
                    self.assertIn('"row_num"', str(error.exception))

    def mutation_profile(self):
        return {
            "schema": {
                "default_type": "row",
                "document_schemas": {
                    "row": {
                        "schema": {
                            "properties": {
                                "id": {"type": "keyword"},
                                "amount": {"type": "integer"},
                                "metadata": {"type": "json"},
                            }
                        }
                    }
                },
            },
            "primary_key": ["id"],
            "rows": [
                {
                    "key": "a",
                    "value": {"id": "u1", "amount": 5, "metadata": {"source": "api"}},
                },
                {"key": "b", "value": {"id": "u2", "amount": 9, "metadata": None}},
            ],
        }

    def test_mutation_stream_records_authentic_counts_nulls_and_fresh_state(self):
        cases = [
            self.case(
                "UPDATE usage_records SET amount=amount+$1 RETURNING id,amount", [1]
            ),
            self.case("DELETE FROM usage_records WHERE id='u1'"),
            self.case(
                "UPDATE usage_records SET metadata='null'::jsonb WHERE id='u1' RETURNING metadata"
            ),
            self.case(
                "UPDATE usage_records SET metadata=NULL WHERE id='u1' RETURNING metadata"
            ),
            self.case(
                "INSERT INTO usage_records (id,amount) VALUES ('u3',9007199254740993) RETURNING id,amount"
            ),
        ]
        result = mutation_reference(self.db, cases, self.mutation_profile())
        self.assertEqual([], result["excluded"])
        entries = result["entries"]
        self.assertEqual(
            ["UPDATE", "DELETE", "UPDATE", "UPDATE", "INSERT"],
            [entry["command_tag"] for entry in entries],
        )
        self.assertEqual([2, 1, 1, 1, 1], [entry["affected"] for entry in entries])
        self.assertEqual([["u1", 6], ["u2", 10]], entries[0]["rows"])
        self.assertEqual([25, 20], entries[0]["column_oids"])
        self.assertEqual([], entries[1]["rows"])
        self.assertEqual(
            [["u2", 9, None]], entries[1]["final_tables"]["usage_records"]["rows"]
        )
        self.assertEqual([[False]], entries[2]["sql_nulls"])
        self.assertEqual([[True]], entries[3]["sql_nulls"])
        self.assertEqual(
            [[False, False, False], [False, False, True]],
            entries[2]["final_tables"]["usage_records"]["sql_nulls"],
        )
        self.assertEqual([["u3", 9007199254740993]], entries[4]["rows"])
        self.assertEqual(3, len(entries[4]["final_tables"]["usage_records"]["rows"]))
        self.assertIsNone(
            self.db.execute("SELECT to_regclass('public.usage_records')").fetchone()[0]
        )

    def test_mutation_stream_joined_and_conflict_profiles_capture_every_table(self):
        profile = self.mutation_profile()
        profile["unique"] = [["amount"]]
        profile["additional_tables"] = [
            {
                "name": "archived_records",
                "schema": profile["schema"],
                "primary_key": ["id"],
                "rows": [
                    {
                        "key": "archived",
                        "value": {
                            "id": "u1",
                            "amount": 3,
                            "metadata": {"archive": True},
                        },
                    }
                ],
            }
        ]
        cases = [
            self.case(
                "UPDATE usage_records AS target SET amount=source.amount FROM archived_records AS source WHERE target.id=source.id RETURNING target.id,target.amount"
            ),
            self.case(
                "INSERT INTO usage_records (id,amount) VALUES ('u1',2) ON CONFLICT (id) DO UPDATE SET amount=usage_records.amount+excluded.amount RETURNING id,amount"
            ),
            self.case(
                "INSERT INTO usage_records (id,amount) VALUES ('u3',5) ON CONFLICT (amount) DO UPDATE SET metadata='null'::jsonb RETURNING id,metadata"
            ),
        ]
        result = mutation_reference(self.db, cases, profile)
        self.assertEqual([], result["excluded"])
        self.assertEqual([["u1", 3]], result["entries"][0]["rows"])
        self.assertEqual([["u1", 7]], result["entries"][1]["rows"])
        self.assertEqual([["u1", None]], result["entries"][2]["rows"])
        self.assertEqual([[False, False]], result["entries"][2]["sql_nulls"])
        for entry in result["entries"]:
            self.assertEqual(
                {"usage_records", "archived_records"}, set(entry["final_tables"])
            )
            self.assertEqual(
                [["u1", 3, {"archive": True}]],
                entry["final_tables"]["archived_records"]["rows"],
            )

    def test_mutation_stream_quota_and_constraint_failures_recover_the_connection(self):
        cases = [
            self.case("INSERT INTO usage_records (id,amount) VALUES ('u1',1)"),
            self.case(
                "INSERT INTO usage_records (id,amount) SELECT 'new_'||n, n FROM generate_series(1,20) n RETURNING id,amount"
            ),
            self.case(
                "INSERT INTO usage_records (id,amount) SELECT 'new_'||n, n FROM generate_series(1,20) n"
            ),
            self.case("SELECT 1"),
            self.case(
                "UPDATE usage_records SET amount=amount+1 WHERE id='u1' RETURNING amount"
            ),
        ]
        result = mutation_reference(
            self.db, cases, self.mutation_profile(), row_limit=2
        )
        self.assertEqual(4, len(result["excluded"]))
        self.assertEqual("23505", result["excluded"][0]["sqlstate"])
        self.assertIn("RETURNING exceeds row budget", result["excluded"][1]["reason"])
        self.assertIn("exceeds row budget", result["excluded"][2]["reason"])
        self.assertIn("not a mutation", result["excluded"][3]["reason"])
        self.assertEqual([[6]], result["entries"][0]["rows"])
        self.assertEqual(
            [["u1", 6, {"source": "api"}], ["u2", 9, None]],
            result["entries"][0]["final_tables"]["usage_records"]["rows"],
        )

    def test_conflict_defaults_are_owner_and_predicate_masked_and_regenerate_columns(
        self,
    ):
        import psycopg

        cases = (
            ("existing", "n=DEFAULT,g=DEFAULT", "", [(1, 2)], True),
            ("existing", "g=DEFAULT", "", [(4, 8)], False),
            ("new", "n=DEFAULT", "", [(7, 14)], False),
            ("existing", "n=DEFAULT", " WHERE FALSE", [], False),
            ("existing", "g=DEFAULT,n=items.n+excluded.n", "", [(11, 22)], False),
        )
        for key, assignment, predicate, expected, called in cases:
            with self.subTest(assignment=assignment, key=key, predicate=predicate):
                with self.db.transaction(force_rollback=True):
                    self.db.execute("CREATE TEMP SEQUENCE conflict_default_sequence")
                    self.db.execute(
                        "CREATE TEMP TABLE items (_id text PRIMARY KEY, "
                        "n bigint NOT NULL DEFAULT nextval('conflict_default_sequence'), "
                        "g bigint GENERATED ALWAYS AS (n*2) STORED)"
                    )
                    self.db.execute("INSERT INTO items(_id,n) VALUES('existing',4)")
                    result = self.db.execute(
                        f"INSERT INTO items(_id,n) VALUES(%s,7) ON CONFLICT(_id) "
                        f"DO UPDATE SET {assignment}{predicate} RETURNING n,g",
                        (key,),
                    ).fetchall()
                    self.assertEqual(expected, result)
                    self.assertEqual(
                        called,
                        self.db.execute(
                            "SELECT is_called FROM conflict_default_sequence"
                        ).fetchone()[0],
                    )
                    rows = self.db.execute(
                        "SELECT n,g FROM items ORDER BY _id"
                    ).fetchall()
                    self.assertEqual(
                        [(4, 8), (7, 14)] if key == "new" else expected or [(4, 8)],
                        rows,
                    )
        for assignment, state in (("n=NULL", "23502"), ("g=7", "428C9")):
            with self.subTest(assignment=assignment):
                with self.db.transaction(force_rollback=True):
                    self.db.execute(
                        "CREATE TEMP TABLE items (_id text PRIMARY KEY, n bigint NOT NULL, "
                        "g bigint GENERATED ALWAYS AS (n*2) STORED)"
                    )
                    self.db.execute("INSERT INTO items(_id,n) VALUES('existing',4)")
                    with self.assertRaises(psycopg.Error) as caught:
                        with self.db.transaction():
                            self.db.execute(
                                "INSERT INTO items(_id,n) VALUES('existing',7) "
                                f"ON CONFLICT(_id) DO UPDATE SET {assignment}"
                            )
                    self.assertEqual(state, caught.exception.sqlstate)
                    self.assertEqual(
                        [(4, 8)], self.db.execute("SELECT n,g FROM items").fetchall()
                    )

    def test_named_conflict_arbiters_select_exact_constraint_and_preserve_error_timing(
        self,
    ):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TEMP SEQUENCE named_default_sequence")
            self.db.execute(
                "CREATE TEMP TABLE named_items (_id text PRIMARY KEY, "
                "n bigint DEFAULT nextval('named_default_sequence'), other bigint, "
                'CONSTRAINT "Selected Unique" UNIQUE(n), '
                "CONSTRAINT other_key UNIQUE(other), "
                "CONSTRAINT positive_n CHECK(n>0), "
                "CONSTRAINT parent_fk FOREIGN KEY(other) REFERENCES named_items(n), "
                "CONSTRAINT deferred_key UNIQUE(n) DEFERRABLE INITIALLY IMMEDIATE)"
            )
            self.db.execute("INSERT INTO named_items(_id,n) VALUES('existing',3)")
            # A named target ignores an equivalent deferrable constraint.
            self.assertEqual(
                [("existing", 6)],
                self.db.execute(
                    "INSERT INTO named_items(_id,n) VALUES('proposed',3) "
                    'ON CONFLICT ON CONSTRAINT "Selected Unique" '
                    "DO UPDATE SET n=named_items.n+excluded.n RETURNING _id,n"
                ).fetchall(),
            )
            self.assertEqual(
                [],
                self.db.execute(
                    "INSERT INTO named_items(_id,n) VALUES('skipped',6) "
                    'ON CONFLICT ON CONSTRAINT "Selected Unique" DO NOTHING RETURNING n'
                ).fetchall(),
            )
            for name, state in (
                ("absent_key", "42704"),
                ("positive_n", "42809"),
                ("parent_fk", "42809"),
                ("selected unique", "42704"),
            ):
                with self.subTest(name=name):
                    with self.assertRaises(psycopg.Error) as caught:
                        with self.db.transaction():
                            self.db.execute(
                                "INSERT INTO named_items(_id) VALUES('invalid') "
                                f'ON CONFLICT ON CONSTRAINT "{name}" DO NOTHING'
                            )
                    self.assertEqual(state, caught.exception.sqlstate)
                    self.assertFalse(
                        self.db.execute(
                            "SELECT is_called FROM named_default_sequence"
                        ).fetchone()[0]
                    )
            # Deferrability is checked by execution, after proposed defaults.
            with self.assertRaises(psycopg.Error) as caught:
                with self.db.transaction():
                    self.db.execute(
                        "INSERT INTO named_items(_id) VALUES('deferred') "
                        "ON CONFLICT ON CONSTRAINT deferred_key DO NOTHING"
                    )
            self.assertEqual("55000", caught.exception.sqlstate)
            self.assertTrue(
                self.db.execute(
                    "SELECT is_called FROM named_default_sequence"
                ).fetchone()[0]
            )
            self.assertEqual(
                [("existing", 6)],
                self.db.execute("SELECT _id,n FROM named_items").fetchall(),
            )

    def test_relation_names_share_one_namespace_and_constraint_index_ownership(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE SCHEMA relation_scope_a")
            self.db.execute("CREATE SCHEMA relation_scope_b")
            self.db.execute(
                "CREATE TABLE relation_scope_a.items(id integer, email text)"
            )
            self.db.execute(
                "CREATE TABLE relation_scope_a.other(id integer, email text)"
            )
            self.db.execute(
                "CREATE TABLE relation_scope_b.items(id integer, email text)"
            )
            self.db.execute("CREATE INDEX access_key ON relation_scope_a.items(email)")
            self.db.execute("CREATE INDEX access_key ON relation_scope_b.items(email)")
            for ddl in (
                "CREATE INDEX access_key ON relation_scope_a.other(email)",
                "CREATE TABLE relation_scope_a.access_key(id integer)",
                "CREATE INDEX items ON relation_scope_a.other(email)",
            ):
                with self.subTest(ddl=ddl), self.assertRaises(psycopg.Error) as caught:
                    with self.db.transaction():
                        self.db.execute(ddl)
                self.assertEqual("42P07", caught.exception.sqlstate)
            self.db.execute(
                "ALTER TABLE relation_scope_a.items ADD CONSTRAINT email_owner UNIQUE(email)"
            )
            with self.assertRaises(psycopg.Error) as caught:
                with self.db.transaction():
                    self.db.execute("DROP INDEX relation_scope_a.email_owner")
            self.assertEqual("2BP01", caught.exception.sqlstate)
            self.db.execute(
                "ALTER TABLE relation_scope_a.items DROP CONSTRAINT email_owner"
            )
            self.db.execute("CREATE INDEX email_owner ON relation_scope_a.other(email)")
            self.db.execute(
                'CREATE INDEX "quoted.index:名" ON relation_scope_a.items(id)'
            )
            self.assertEqual(
                [
                    ("relation_scope_a", "access_key"),
                    ("relation_scope_b", "access_key"),
                ],
                self.db.execute(
                    "SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='access_key' AND n.nspname IN ('relation_scope_a','relation_scope_b') ORDER BY n.nspname"
                ).fetchall(),
            )

    def test_unique_indexes_are_inference_arbiters_not_named_constraints(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute(
                "CREATE TEMP TABLE owner_items (id text PRIMARY KEY, email text, "
                "tenant_id text, status text, name text)"
            )
            self.db.execute(
                "INSERT INTO owner_items VALUES "
                "('u1','a@example.test','t1','active','old'),"
                "('u2','b@example.test','t2','closed','other')"
            )
            for keys, predicate, email in (
                ("email", "", "a@example.test"),
                ("email", " WHERE status='active'", "a@example.test"),
                ("lower(email)", "", "A@EXAMPLE.TEST"),
                ("tenant_id,lower(email)", "", "A@EXAMPLE.TEST"),
            ):
                with self.subTest(keys=keys, predicate=predicate):
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(
                            f"CREATE UNIQUE INDEX access_key ON owner_items ({keys}){predicate}"
                        )
                        result = self.db.execute(
                            "INSERT INTO owner_items VALUES ('u2',%s,'t1','active','new') "
                            f"ON CONFLICT ({keys}){predicate} DO UPDATE "
                            "SET name=excluded.name RETURNING id,name",
                            (email,),
                        ).fetchall()
                        self.assertEqual([("u1", "new")], result)
                        self.assertEqual(
                            [("u1", "new"), ("u2", "other")],
                            self.db.execute(
                                "SELECT id,name FROM owner_items ORDER BY id"
                            ).fetchall(),
                        )
                        for sql in (
                            "INSERT INTO owner_items(id) VALUES('u3') "
                            "ON CONFLICT ON CONSTRAINT access_key DO NOTHING",
                            "ALTER TABLE owner_items DROP CONSTRAINT access_key",
                            "ALTER TABLE owner_items VALIDATE CONSTRAINT access_key",
                            "SET CONSTRAINTS access_key IMMEDIATE",
                        ):
                            with self.subTest(sql=sql):
                                with self.assertRaises(psycopg.Error) as caught:
                                    with self.db.transaction():
                                        self.db.execute(sql)
                                self.assertEqual("42704", caught.exception.sqlstate)
                        self.db.execute("DROP INDEX access_key")
                        self.assertEqual(
                            [("u1",)],
                            self.db.execute(
                                "INSERT INTO owner_items(id) VALUES('u1') "
                                "ON CONFLICT ON CONSTRAINT owner_items_pkey "
                                "DO UPDATE SET name='primary' RETURNING id"
                            ).fetchall(),
                        )

    def test_unique_mutation_profile_keeps_base_seeds_and_original_case_contracts(self):
        import json
        from generate_sql_postgres_reference import mutation_profile, properties

        base = mutation_profile()
        profile = mutation_profile("unique-email")
        self.assertNotIn("unique", base)
        self.assertNotIn("next_status", properties(base["schema"]))
        self.assertEqual([["email"]], profile["unique"])
        self.assertEqual(base["rows"], profile["rows"])
        self.assertEqual(base["additional_tables"], profile["additional_tables"])
        self.assertEqual(base, mutation_profile())
        golden = json.loads(
            (FIXTURES / "sql_unique_mutation_postgres_reference.json").read_text()
        )
        inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        ids = {case["id"] for case in golden["entries"]}
        self.assertEqual(10, len(ids))
        self.assertTrue({"sql-1509", "sql-1514"} <= ids)
        cases = [case for case in inventory if case["id"] in ids]
        result = mutation_reference(self.db, cases, profile)
        self.assertEqual([], result["excluded"])
        self.assertEqual(profile, golden["profile"])
        self.assertEqual(golden["entries"], result["entries"])
        self.assertEqual(
            {"usage_records", "archived_records", "source_records"},
            set(result["entries"][0]["final_tables"]),
        )

    def test_original_partial_and_expression_arbiters_require_declared_index_owners(
        self,
    ):
        import json
        from generate_sql_postgres_reference import mutation_profile

        inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        base = mutation_profile()
        for owner, case_ids, fixture in (
            ("partial-active-email", {"sql-1455", "sql-1510", "sql-1515"}, "partial"),
            ("lower-email", {"sql-1458"}, "lower"),
            ("tenant-lower-email", {"sql-1460"}, "mixed"),
            ("upper-email", {"sql-1461"}, "upper"),
        ):
            with self.subTest(owner=owner):
                profile = mutation_profile(owner)
                self.assertEqual(base["rows"], profile["rows"])
                self.assertEqual(base["schema"], profile["schema"])
                self.assertEqual(
                    base["additional_tables"], profile["additional_tables"]
                )
                cases = [case for case in inventory if case["id"] in case_ids]
                self.assertEqual(len(case_ids), len(cases))
                result = mutation_reference(self.db, cases, profile)
                self.assertEqual([], result["excluded"])
                golden = json.loads(
                    (
                        FIXTURES / f"sql_{fixture}_mutation_postgres_reference.json"
                    ).read_text()
                )
                self.assertEqual(profile, golden["profile"])
                self.assertEqual(result["entries"], golden["entries"])
                entry = result["entries"][0]
                self.assertEqual([["u1", "new"]], entry["rows"])
                self.assertEqual([25, 25], entry["column_oids"])
                self.assertEqual(1, entry["affected"])
                self.assertEqual(
                    {"usage_records", "archived_records", "source_records"},
                    set(entry["final_tables"]),
                )
                # No ordinary uniqueness may accidentally stand in for this
                # partial/expression owner; its declaration is authoritative.
                for changed in (
                    {"index_owner_ddl": "SELECT 1"},
                    {"index_owner_profile": "unknown"},
                    {"index_owner_profile": None},
                    {"unique": [["email"]]},
                ):
                    with self.assertRaisesRegex(ValueError, "declared index owner"):
                        mutation_reference(self.db, cases, profile | changed)

    def test_unique_mutation_admission_probes_fail_closed(self):
        from generate_sql_postgres_reference import mutation_profile

        for probe in (
            {
                "sql": "INSERT INTO usage_records (id,email) VALUES ('unique_probe','a@example.test')",
                "sqlstate": "23502",
            },
            {
                "sql": "UPDATE usage_records SET status='changed' WHERE id='u1'",
                "sqlstate": "23505",
            },
            {"sql": 42, "sqlstate": "23505"},
            {"sql": "SELECT 1", "sqlstate": "invalid"},
        ):
            with self.subTest(probe=probe):
                profile = mutation_profile("unique-email")
                profile["admission_probes"] = [probe]
                with self.assertRaises(ValueError):
                    mutation_reference(self.db, [], profile)
        profile = mutation_profile("unique-email")
        profile["admission_probes"] *= 129
        with self.assertRaises(ValueError):
            mutation_reference(self.db, [], profile)
        with self.assertRaises(ValueError):
            mutation_profile("unknown")

    def test_original_mutation_profiles_enforce_logical_primary_keys(self):
        import json

        cases = [
            self.case("INSERT INTO usage_records (id) VALUES ('u1')"),
            self.case("INSERT INTO usage_records (id) VALUES (NULL)"),
            self.case(
                "UPDATE usage_records SET status='verified' WHERE id='u1' RETURNING id,status"
            ),
        ]
        for name in (
            "sql_mutation_postgres_reference.json",
            "sql_correlated_mutation_postgres_reference.json",
        ):
            with self.subTest(profile=name):
                profile = json.loads((FIXTURES / name).read_text())["profile"]
                result = mutation_reference(self.db, cases, profile)
                self.assertEqual(
                    ["23505", "23502"],
                    [entry["sqlstate"] for entry in result["excluded"]],
                )
                self.assertEqual(1, len(result["entries"]))
                self.assertEqual([["u1", "verified"]], result["entries"][0]["rows"])

    def test_original_postgres_mutation_campaign_goldens_are_complete_and_repeatable(
        self,
    ):
        import json
        from pathlib import Path

        fixtures = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
        )
        manifest = json.loads((fixtures / "sql_mutation_campaign.json").read_text())
        profile = json.loads(
            (fixtures / "sql_mutation_campaign_profile.json").read_text()
        )
        golden = json.loads(
            (fixtures / "sql_mutation_postgres_reference.json").read_text()
        )
        inventory = json.loads((fixtures / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        requested = {case["id"] for case in manifest["entries"]}
        self.assertEqual(235, len(requested))
        cases = [case for case in inventory if case["id"] in requested]
        result = mutation_reference(self.db, cases, profile)
        self.assertEqual(48, len(result["entries"]))
        self.assertEqual(187, len(result["excluded"]))
        self.assertEqual(
            requested, {case["id"] for case in result["entries"] + result["excluded"]}
        )
        self.assertEqual(profile, golden["profile"])
        self.assertEqual(golden["entries"], result["entries"])
        for entry in result["entries"]:
            self.assertGreater(entry["affected"], 0)
            self.assertEqual(
                {"usage_records", "archived_records", "source_records"},
                set(entry["final_tables"]),
            )
        # Classifications are profile-scoped discovery, never disposition credit.
        # In particular a missing arbiter is not a PostgreSQL syntax rejection.
        excluded = {case["id"]: case for case in result["excluded"]}
        self.assertEqual("42601", excluded["sql-0574"]["sqlstate"])
        self.assertEqual("42P10", excluded["sql-1394"]["sqlstate"])

    def test_mutation_stream_byte_quota_covers_returning_and_post_state(self):
        cases = [
            self.case(
                "UPDATE usage_records SET amount=amount+1 WHERE id='u1' RETURNING repeat('x',4096)"
            ),
            self.case(
                "UPDATE usage_records SET metadata=jsonb_build_object('payload',repeat('x',4096)) WHERE id='u1'"
            ),
            self.case(
                "UPDATE usage_records SET amount=amount+1 WHERE id='u1' RETURNING amount"
            ),
        ]
        result = mutation_reference(
            self.db, cases, self.mutation_profile(), byte_limit=128
        )
        self.assertEqual(2, len(result["excluded"]))
        self.assertIn("RETURNING exceeds byte budget", result["excluded"][0]["reason"])
        self.assertIn(
            "final state exceeds byte budget", result["excluded"][1]["reason"]
        )
        self.assertEqual([[6]], result["entries"][0]["rows"])
        self.assertEqual(
            [["u1", 6, {"source": "api"}], ["u2", 9, None]],
            result["entries"][0]["final_tables"]["usage_records"]["rows"],
        )

    def test_mutation_profile_gaps_fail_closed_before_case_execution(self):
        for change in [
            lambda profile: profile.update(primary_key=["missing"]),
            lambda profile: profile.update(unique=[[]]),
            lambda profile: profile.update(checks=["amount > 0"]),
            lambda profile: profile["schema"]["document_schemas"]["row"]["schema"][
                "properties"
            ]["metadata"].update(type="array"),
            lambda profile: profile["schema"]["document_schemas"]["row"]["schema"][
                "properties"
            ]["metadata"].update(type="sql_array", **{"x-antfly-sql-type": "jsonb"}),
            lambda profile: profile["schema"]["document_schemas"]["row"]["schema"][
                "properties"
            ]["amount"].update(default=1),
            lambda profile: profile.update(
                additional_tables=[
                    {"name": "usage_records", "schema": profile["schema"], "rows": []}
                ]
            ),
        ]:
            profile = self.mutation_profile()
            change(profile)
            with self.assertRaises(ValueError):
                mutation_reference(
                    self.db, [self.case("DELETE FROM usage_records")], profile
                )
        self.assertIsNone(
            self.db.execute("SELECT to_regclass('public.usage_records')").fetchone()[0]
        )

    def test_like_explicit_escape_contracts(self):
        cases = [
            ("'bot_agent' LIKE 'bot!_%' ESCAPE '!'", True),
            ("'bot_agent' NOT LIKE 'bot!_%' ESCAPE '!'", False),
            ("'BOT_agent' ILIKE 'bot!_%' ESCAPE '!'", True),
            ("'a_b' LIKE 'aé_b' ESCAPE 'é'", True),
            ("'a%b' LIKE 'a%%b' ESCAPE '%'", True),
            ("'a_b' LIKE 'a__b' ESCAPE '_'", True),
            ("'a!xb' LIKE 'a!_b' ESCAPE ''", True),
            ("'a' LIKE 'a' ESCAPE NULL", None),
        ]
        for expression, expected in cases:
            with self.subTest(expression=expression):
                self.assertEqual(
                    self.db.execute("SELECT " + expression).fetchone()[0], expected
                )

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

    def test_lateral_parent_scopes_materialization_and_recursion(self):
        cases = [
            (
                "SELECT l.n FROM (SELECT 1 AS n UNION ALL SELECT 2) p "
                "CROSS JOIN LATERAL (SELECT p.*) l ORDER BY l.n",
                [(1,), (2,)],
            ),
            (
                "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p "
                "CROSS JOIN LATERAL (WITH c AS MATERIALIZED (SELECT p.n AS x) "
                "SELECT a.x+b.x AS x FROM c a CROSS JOIN c b) l ORDER BY p.n",
                [(1, 2), (2, 4)],
            ),
            (
                "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p "
                "CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n "
                "UNION ALL SELECT n+1 FROM r WHERE n<p.n+1) "
                "SELECT sum(n) AS x FROM r) l ORDER BY p.n",
                [(1, 3), (2, 5)],
            ),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                self.assertEqual(expected, self.db.execute(sql).fetchall())

    def test_lateral_limit_offset_is_per_parent(self):
        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE edges(src bigint, dst bigint)")
            self.db.execute(
                "INSERT INTO edges SELECT n, n*10+k "
                "FROM generate_series(1,128) n CROSS JOIN generate_series(0,1) k"
            )
            rows = self.db.execute(
                "WITH RECURSIVE p(n) AS (SELECT 1 UNION ALL SELECT n+1 "
                "FROM p WHERE n<128) SELECT p.n,l.x FROM p LEFT JOIN LATERAL "
                "(SELECT e.dst AS x FROM edges e WHERE e.src=p.n "
                "ORDER BY e.dst DESC LIMIT 1 OFFSET 1) l ON true ORDER BY p.n"
            ).fetchall()
            self.assertEqual([(n, n * 10) for n in range(1, 129)], rows)

    def test_wildcard_ordinals_select_rows_before_unneeded_scalar_outputs(self):
        import psycopg

        for sql, expected in (
            (
                "SELECT o.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 1 DESC LIMIT 1",
                [(2, None)],
            ),
            (
                "SELECT o.*,o.x+10 AS rank,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 2 DESC LIMIT 1",
                [(2, 12, None)],
            ),
            (
                "SELECT o.*,(SELECT o.x+10) AS rank FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 2 DESC LIMIT 1",
                [(2, 12)],
            ),
            (
                "SELECT o.*,o.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 2 DESC LIMIT 1",
                [(2, 2, None)],
            ),
            (
                "WITH c(x) AS (SELECT 1 UNION ALL SELECT 2) SELECT c.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE c.x=1) AS v FROM c ORDER BY 1 DESC LIMIT 1",
                [(2, None)],
            ),
            (
                "SELECT p.k,l.x,l.v FROM (SELECT 1 AS k) p CROSS JOIN LATERAL (SELECT q.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE q.x=p.k) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) q ORDER BY 1 DESC LIMIT 1) l",
                [(1, 2, None)],
            ),
            (
                "SELECT * FROM ((SELECT o.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 1 DESC LIMIT 1) UNION ALL SELECT 3,CAST(NULL AS BIGINT)) s ORDER BY 1",
                [(2, None), (3, None)],
            ),
            (
                "SELECT o.*,(SELECT o.x+10) AS rank FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 1 ASC LIMIT 1 OFFSET 1",
                [(2, 12)],
            ),
        ):
            with self.subTest(sql=sql):
                self.assertEqual(expected, self.db.execute(sql).fetchall())
        with (
            self.assertRaises(psycopg.Error) as error,
            self.db.transaction(force_rollback=True),
        ):
            self.db.execute(
                "SELECT o.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 1 ASC LIMIT 1 OFFSET 1"
            )
        self.assertEqual("21000", error.exception.sqlstate)

    def test_window_input_subqueries_retain_partition_ordering_and_filter_domains(self):
        import psycopg

        for sql, expected in (
            (
                "SELECT t.x,row_number() OVER (PARTITION BY (SELECT t.x%2) ORDER BY (SELECT t.x) DESC) AS n FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t ORDER BY t.x",
                [(1, 2), (2, 1), (3, 1)],
            ),
            (
                "SELECT t.x,row_number() OVER w AS n FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t WINDOW w AS (PARTITION BY (SELECT t.x%2) ORDER BY (SELECT t.x) DESC) ORDER BY t.x",
                [(1, 2), (2, 1), (3, 1)],
            ),
            (
                "SELECT t.x,SUM(t.x) FILTER (WHERE t.x>1) OVER (ORDER BY (SELECT t.x) ROWS UNBOUNDED PRECEDING) AS n FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t ORDER BY t.x",
                [(1, None), (2, 2), (3, 5)],
            ),
            (
                "SELECT row_number() OVER (ORDER BY CASE WHEN t.x=1 THEN (SELECT t.x) ELSE (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i) END) FROM (SELECT 1 AS x) t",
                [(1,)],
            ),
            (
                "SELECT COUNT((SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i)) FILTER (WHERE false) OVER (ORDER BY (SELECT t.x)) FROM (SELECT 1 AS x) t",
                [(0,)],
            ),
            (
                "SELECT row_number() OVER (ORDER BY (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i)) FROM (SELECT 1 AS x) t WHERE false",
                [],
            ),
        ):
            with self.subTest(sql=sql):
                self.assertEqual(expected, self.db.execute(sql).fetchall())
        with (
            self.assertRaises(psycopg.Error) as error,
            self.db.transaction(force_rollback=True),
        ):
            self.db.execute(
                "SELECT COUNT(*) FILTER (WHERE false) OVER (ORDER BY (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i)) FROM (SELECT 1 AS x) t"
            )
        self.assertEqual("21000", error.exception.sqlstate)

        with (
            self.assertRaises(psycopg.Error) as error,
            self.db.transaction(force_rollback=True),
        ):
            self.db.execute(
                "SELECT 1 WINDOW unused AS (ORDER BY (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i))"
            )
        self.assertEqual("21000", error.exception.sqlstate)

    def test_phase_outputs_evaluate_scalar_children_after_grouping_and_windows(self):
        for sql, expected in (
            (
                "SELECT t.x,(SELECT t.x+10) AS v FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x",
                [(1, 11), (2, 12)],
            ),
            (
                "SELECT SUM(t.x),(SELECT 1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) t",
                [(3, 1)],
            ),
            (
                "SELECT t.x,row_number() OVER (ORDER BY t.x) AS n,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE t.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY t.x DESC LIMIT 1",
                [(2, 2, None)],
            ),
            (
                "SELECT t.x,COUNT(*)+(SELECT t.x) AS n FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x HAVING (SELECT t.x)>1 ORDER BY t.x",
                [(2, 3)],
            ),
            (
                "SELECT t.x+1 AS k,COUNT(*),(SELECT 9) AS v FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY 1 ORDER BY k",
                [(2, 2, 9), (3, 1, 9)],
            ),
            (
                "SELECT t.x+1 AS k,COUNT(*),(SELECT 9) AS v FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY k ORDER BY k",
                [(2, 2, 9), (3, 1, 9)],
            ),
            (
                "SELECT t.x+1 AS k,COUNT(*),(SELECT 9) AS v FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY x+1 ORDER BY k",
                [(2, 2, 9), (3, 1, 9)],
            ),
            (
                "SELECT t.x,COUNT(*),row_number() OVER (ORDER BY COUNT(*) DESC),(SELECT t.x) FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x",
                [(1, 2, 1, 1), (2, 1, 2, 2)],
            ),
            ("SELECT COUNT(*),(SELECT 7) FROM (SELECT 1 AS x) t WHERE false", [(0, 7)]),
            (
                "SELECT t.x,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i) FROM (SELECT 1 AS x) t WHERE false GROUP BY t.x",
                [],
            ),
            (
                "SELECT t.x,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE t.x=1) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x DESC LIMIT 1",
                [(2, None)],
            ),
            (
                "SELECT t.x,(SELECT t.x FROM (SELECT 3 AS x) t) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x",
                [(1, 3), (2, 3)],
            ),
            (
                "SELECT t.x,(SELECT x+10) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x",
                [(1, 11), (2, 12)],
            ),
            (
                "SELECT CASE WHEN COUNT(*)=0 THEN (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i) ELSE 1 END FROM (SELECT 1 AS x) t",
                [(1,)],
            ),
            (
                "WITH c(x) AS (SELECT 1 UNION ALL SELECT 2) SELECT t.x,(SELECT c.x FROM c WHERE c.x=t.x) FROM c t GROUP BY t.x ORDER BY t.x",
                [(1, 1), (2, 2)],
            ),
            (
                "SELECT p.k,l.x,l.v FROM (SELECT 1 AS k) p CROSS JOIN LATERAL (SELECT q.x,(SELECT q.x+p.k) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) q GROUP BY q.x ORDER BY q.x DESC LIMIT 1) l",
                [(1, 2, 3)],
            ),
            (
                "SELECT t.x,COUNT(*),row_number() OVER (ORDER BY t.x),(SELECT t.x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x HAVING t.x>1 ORDER BY t.x",
                [(2, 1, 1, 2)],
            ),
            (
                "SELECT t.x,COUNT(*),row_number() OVER (ORDER BY t.x),(SELECT t.x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x HAVING (SELECT t.x)>1 ORDER BY t.x",
                [(2, 1, 1, 2)],
            ),
            (
                "SELECT t.x,CASE WHEN row_number() OVER (ORDER BY t.x)=1 THEN (SELECT t.x+10) ELSE 0 END FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY t.x",
                [(1, 11), (2, 0)],
            ),
            (
                "SELECT t.x,SUM((SELECT t.x)) OVER (ORDER BY t.x)+(SELECT 1) FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY t.x",
                [(1, 2), (2, 4)],
            ),
            (
                "SELECT * FROM ((SELECT t.x,(SELECT t.x+10) FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x DESC LIMIT 1) UNION ALL SELECT 3,13) s ORDER BY 1",
                [(2, 12), (3, 13)],
            ),
        ):
            with self.subTest(sql=sql):
                self.assertEqual(expected, self.db.execute(sql).fetchall())

    def test_phase_output_grouping_and_cardinality_diagnostics(self):
        import psycopg

        for sql, code in (
            ("SELECT COUNT(*),(SELECT t.x) FROM (SELECT 1 AS x) t", "42803"),
            (
                "SELECT t.x,(SELECT t.y) FROM (SELECT 1 AS x,2 AS y) t GROUP BY t.x",
                "42803",
            ),
            (
                "SELECT t.x+1,(SELECT t.x+1) FROM (SELECT 1 AS x) t GROUP BY t.x+1",
                "42803",
            ),
            (
                "SELECT t.x,(SELECT t.missing) FROM (SELECT 1 AS x) t GROUP BY t.x",
                "42703",
            ),
            (
                "SELECT t.x,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE t.x=1) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x ASC LIMIT 1 OFFSET 1",
                "21000",
            ),
        ):
            with self.subTest(sql=sql):
                with (
                    self.assertRaises(psycopg.Error) as error,
                    self.db.transaction(force_rollback=True),
                ):
                    self.db.execute(sql)
                self.assertEqual(code, error.exception.sqlstate)

        with psycopg.RawCursor(self.db) as cursor:
            result = cursor.execute(
                "SELECT t.x,COUNT(*)+(SELECT t.x+$1) AS n FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x HAVING (SELECT t.x)>$2 ORDER BY n DESC,t.x DESC LIMIT $3 OFFSET $4",
                (2, 0, 1, 1),
            ).fetchall()
        self.assertEqual([(1, 5)], result)

        cursor = self.db.execute(
            "SELECT COUNT(*),EXISTS(SELECT 1) FROM (SELECT 1 AS x) t"
        )
        self.assertEqual(
            ["count", "exists"], [column.name for column in cursor.description]
        )
        self.assertEqual([(1, True)], cursor.fetchall())

    def test_sorted_scalar_callbacks_evaluate_prefix_before_offset(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE SEQUENCE scalar_calls")
            self.db.execute(
                "CREATE FUNCTION scalar_callback(n bigint) RETURNS bigint LANGUAGE plpgsql VOLATILE AS $$ BEGIN PERFORM nextval('scalar_calls'); RETURN n; END $$"
            )
            for offset in (0, 1, 130, 512):
                self.db.execute("SELECT setval('scalar_calls',1,false)")
                with psycopg.RawCursor(self.db) as cursor:
                    rows = cursor.execute(
                        "SELECT o.*,(SELECT scalar_callback(o.x)) FROM generate_series(1,512) o(x) ORDER BY 1 DESC LIMIT $1 OFFSET $2",
                        (2, offset),
                    ).fetchall()
                self.assertEqual(
                    [(n, n) for n in range(512 - offset, max(0, 510 - offset), -1)],
                    rows,
                )
                value, called = self.db.execute(
                    "SELECT last_value,is_called FROM scalar_calls"
                ).fetchone()
                self.assertEqual(min(offset + 2, 512), value if called else 0)

    def test_quantified_correlated_boundaries_preserve_three_valued_truth(self):
        for sql, expected in (
            (
                "SELECT o.x IN (SELECT i.y FROM (SELECT 1 AS k, 2 AS y) i WHERE i.k=o.x ORDER BY i.y LIMIT 1) FROM (SELECT 1 AS x) o",
                False,
            ),
            (
                "SELECT o.x < ANY (SELECT SUM(i.y) FROM (SELECT 1 AS k, 2 AS y) i WHERE i.k=o.x GROUP BY i.k) FROM (SELECT 1 AS x) o",
                True,
            ),
            (
                "SELECT o.x = ALL (SELECT ROW_NUMBER() OVER (ORDER BY i.y) FROM (SELECT 1 AS k, 2 AS y) i WHERE i.k=o.x) FROM (SELECT 1 AS x) o",
                True,
            ),
            (
                "SELECT o.x IN (SELECT i.y FROM (SELECT 1 AS k, CAST(NULL AS BIGINT) AS y) i WHERE i.k=o.x LIMIT 1) FROM (SELECT 1 AS x) o",
                None,
            ),
            (
                "SELECT o.x <> ALL (SELECT i.y FROM (SELECT 1 AS k, CAST(NULL AS BIGINT) AS y) i WHERE i.k=o.x LIMIT 0) FROM (SELECT 1 AS x) o",
                True,
            ),
            (
                "SELECT o.x = ANY (SELECT i.y FROM (SELECT 1 AS k, CAST(NULL AS BIGINT) AS y) i WHERE i.k=o.x LIMIT 0) FROM (SELECT 1 AS x) o",
                False,
            ),
            (
                "SELECT o.x < ALL (SELECT i.y FROM (SELECT 1 AS k, 2 AS y UNION ALL SELECT 1,NULL) i WHERE i.k=o.x ORDER BY i.y LIMIT 2) FROM (SELECT 1 AS x) o",
                None,
            ),
            (
                "SELECT o.x > ALL (SELECT i.y FROM (SELECT 1 AS k, 2 AS y UNION ALL SELECT 1,NULL) i WHERE i.k=o.x ORDER BY i.y LIMIT 2) FROM (SELECT 1 AS x) o",
                False,
            ),
            (
                "SELECT o.x < ANY (SELECT i.y FROM (SELECT 1 AS k, 2 AS y UNION ALL SELECT 1,NULL) i WHERE i.k=o.x ORDER BY i.y LIMIT 2) FROM (SELECT 1 AS x) o",
                True,
            ),
            (
                "SELECT o.x LIKE ANY (SELECT i.y FROM (SELECT 1 AS k, 'a%' AS y) i WHERE i.k=o.k LIMIT 1) FROM (SELECT 1 AS k, 'abc' AS x) o",
                True,
            ),
            (
                "SELECT o.x NOT ILIKE ALL (SELECT i.y FROM (SELECT 1 AS k, 'A%' AS y) i WHERE i.k=o.k LIMIT 1) FROM (SELECT 1 AS k, 'abc' AS x) o",
                False,
            ),
            (
                "SELECT o.x IN (SELECT x LIMIT 1) FROM (SELECT 9007199254740993 AS x) o",
                True,
            ),
            (
                "SELECT o.x <> ALL (SELECT i.y FROM (SELECT 9007199254740992 AS y) i WHERE i.y<o.x LIMIT 1) FROM (SELECT 9007199254740993 AS x) o",
                True,
            ),
            (
                "SELECT o.x IN (SELECT i.y FROM (SELECT CAST('null' AS JSONB) AS y) i WHERE o.k=1 LIMIT 1) FROM (SELECT 1 AS k,CAST('null' AS JSONB) AS x) o",
                True,
            ),
            (
                "SELECT o.x IN (SELECT i.y FROM (SELECT CAST(NULL AS JSONB) AS y) i WHERE o.k=1 LIMIT 1) FROM (SELECT 1 AS k,CAST('null' AS JSONB) AS x) o",
                None,
            ),
            (
                "SELECT CASE WHEN FALSE THEN o.x IN (SELECT 1/(i.y-2) FROM (SELECT 1 AS k,2 AS y) i WHERE i.k=o.x LIMIT 1) ELSE TRUE END FROM (SELECT 1 AS x) o",
                True,
            ),
        ):
            with self.subTest(sql=sql):
                self.assertEqual([(expected,)], self.db.execute(sql).fetchall())

    def test_quantified_boundary_comparison_matrix_and_parameter_inference(self):
        import operator
        import psycopg

        sets = (
            ("SELECT 1 AS y WHERE false", []),
            ("SELECT CAST(NULL AS BIGINT) AS y", [None]),
            ("SELECT 1 AS y", [1]),
            ("SELECT 1 AS y UNION ALL SELECT 1", [1, 1]),
            ("SELECT 1 AS y UNION ALL SELECT 2", [1, 2]),
            ("SELECT 1 AS y UNION ALL SELECT NULL", [1, None]),
            ("SELECT 1 AS y UNION ALL SELECT 2 UNION ALL SELECT NULL", [1, 2, None]),
        )
        for op, compare in (
            ("=", operator.eq),
            ("<>", operator.ne),
            ("<", operator.lt),
            ("<=", operator.le),
            (">", operator.gt),
            (">=", operator.ge),
        ):
            for every in (False, True):
                for source, values in sets:
                    query = f"SELECT x {op} {'ALL' if every else 'ANY'} (SELECT y FROM ({source}) i WHERE o.x IS NULL OR o.x IS NOT NULL ORDER BY i.y LIMIT 100) FROM (SELECT 0 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT NULL) o"
                    expected = []
                    for operand in (0, 1, 2, None):
                        results = [
                            None
                            if operand is None or value is None
                            else compare(operand, value)
                            for value in values
                        ]
                        decisive = any(
                            value is not None and value != every for value in results
                        )
                        truth = (
                            not every
                            if decisive
                            else None
                            if None in results
                            else every
                        )
                        expected.append((truth,))
                    with self.subTest(sql=query):
                        self.assertEqual(expected, self.db.execute(query).fetchall())
        query = "SELECT $1 < ANY (SELECT i.y FROM (SELECT 1 AS k,2 AS y) i WHERE i.k=o.x ORDER BY i.y LIMIT $2 OFFSET $3) FROM (SELECT 1 AS x) o"
        with psycopg.RawCursor(self.db) as cursor:
            self.assertEqual([(True,)], cursor.execute(query, (1, 1, 0)).fetchall())
            self.assertEqual([(False,)], cursor.execute(query, (None, 0, 0)).fetchall())
        for alias in (
            "$quantified_input",
            "$quantified_input_1",
            "$quantified_demand_0",
        ):
            query = f'SELECT "{alias}".x IN (SELECT "{alias}".x LIMIT 1) FROM (SELECT 1 AS x) "{alias}"'
            self.assertEqual([(True,)], self.db.execute(query).fetchall())

    def test_row_assignment_expands_schema_bound_width_before_execution(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE target(n bigint,cold text)")
            self.db.execute("CREATE TABLE source(id text,delta bigint)")
            self.db.execute("INSERT INTO target VALUES(1,'old'),(2,'old')")
            self.db.execute("INSERT INTO source VALUES('a',10),('b',20)")
            for sql, expected in (
                (
                    "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta,'new' AS label FROM source ORDER BY delta DESC LIMIT 1) s) RETURNING n,cold",
                    [(20, "new")] * 2,
                ),
                (
                    "UPDATE target SET (n,cold)=(SELECT s.* FROM (SELECT delta,'new' AS label FROM source ORDER BY delta DESC LIMIT 1) s) RETURNING n,cold",
                    [(20, "new")] * 2,
                ),
                (
                    "UPDATE target SET (n,cold)=(SELECT s.*,'new' FROM (SELECT delta FROM source ORDER BY delta DESC LIMIT 1) s) RETURNING n,cold",
                    [(20, "new")] * 2,
                ),
                (
                    "WITH s AS (SELECT delta,'new' AS label FROM source ORDER BY delta DESC LIMIT 1) UPDATE target SET (n,cold)=(SELECT * FROM s) RETURNING n,cold",
                    [(20, "new")] * 2,
                ),
                (
                    "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta,'new' AS label FROM source WHERE false) s) RETURNING n,cold",
                    [(None, None)] * 2,
                ),
                (
                    "UPDATE target SET (n)=(SELECT count(*) FROM source),cold='new' RETURNING n,cold",
                    [(2, "new")] * 2,
                ),
            ):
                with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                    self.assertEqual(expected, self.db.execute(sql).fetchall())
            for sql, code in (
                (
                    "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta FROM source) s)",
                    "42601",
                ),
                (
                    "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta,'new' AS label,id FROM source) s)",
                    "42601",
                ),
                (
                    "UPDATE target SET (n,cold)=(SELECT s.*,'extra' FROM (SELECT delta,'new' AS label FROM source) s)",
                    "42601",
                ),
                (
                    "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta FROM source WHERE false) s)",
                    "42601",
                ),
                (
                    "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta,'new' AS label FROM source) s)",
                    "21000",
                ),
            ):
                with self.subTest(sql=sql):
                    with (
                        self.assertRaises(psycopg.Error) as error,
                        self.db.transaction(force_rollback=True),
                    ):
                        self.db.execute(sql)
                    self.assertEqual(code, error.exception.sqlstate)
                self.assertEqual(
                    [(1, "old"), (2, "old")],
                    self.db.execute("SELECT n,cold FROM target ORDER BY n").fetchall(),
                )

    def test_correlated_row_assignment_uses_one_cardinality_checked_tuple(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE target(_id text,n bigint,cold text)")
            self.db.execute("CREATE TABLE source(id text,delta bigint)")
            self.db.execute("INSERT INTO target VALUES('a',1,'old'),('b',2,'old')")
            self.db.execute("INSERT INTO source VALUES('a',10),('b',20)")
            for sql, expected in (
                (
                    "UPDATE target SET (n,cold)=(SELECT delta,'new' FROM source WHERE source.id=target._id) RETURNING n,cold",
                    [(10, "new"), (20, "new")],
                ),
                (
                    "UPDATE target t SET (n,cold)=(SELECT s.delta,'new' FROM source s WHERE s.id=t._id ORDER BY s.delta DESC LIMIT 1) RETURNING n,cold",
                    [(10, "new"), (20, "new")],
                ),
                (
                    "UPDATE target t SET (n,cold)=(SELECT q.* FROM (SELECT s.delta,'new' AS label FROM source s WHERE s.id=t._id) q) RETURNING n,cold",
                    [(10, "new"), (20, "new")],
                ),
                (
                    "UPDATE target t SET (n,cold)=(SELECT s.delta+t.n,'new' FROM source s WHERE s.id=t._id) RETURNING n,cold",
                    [(11, "new"), (22, "new")],
                ),
                (
                    "UPDATE target t SET (n,cold)=(SELECT COUNT(*),'new' FROM source s WHERE s.id=t._id) RETURNING n,cold",
                    [(1, "new"), (1, "new")],
                ),
                (
                    "UPDATE target t SET (n,cold)=(SELECT s.delta,'new' FROM source s WHERE s.id=t._id AND false) RETURNING n,cold",
                    [(None, None), (None, None)],
                ),
            ):
                with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                    self.assertEqual(expected, self.db.execute(sql).fetchall())
            self.db.execute("INSERT INTO source VALUES('a',30)")
            with (
                self.assertRaises(psycopg.Error) as error,
                self.db.transaction(force_rollback=True),
            ):
                self.db.execute(
                    "UPDATE target t SET (n,cold)=(SELECT s.delta,'new' FROM source s WHERE s.id=t._id)"
                )
            self.assertEqual("21000", error.exception.sqlstate)
            self.assertEqual(
                [(1, "old"), (2, "old")],
                self.db.execute("SELECT n,cold FROM target ORDER BY _id").fetchall(),
            )

    def test_original_aggregate_campaign_covers_match_nonmatch_null_witnesses(self):
        import json

        manifest = json.loads(
            (FIXTURES / "sql_aggregate_read_campaign.json").read_text()
        )
        ids = {entry["id"] for entry in manifest["entries"]}
        self.assertEqual(12, len(ids))
        inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        cases = [case for case in inventory if case["id"] in ids]
        profile = aggregate_read_profile()
        result = read_reference(self.db, cases, profile)
        self.assertEqual([], result["excluded"])
        self.assertEqual(ids, {entry["id"] for entry in result["entries"]})
        golden = json.loads(
            (FIXTURES / "sql_aggregate_read_reference.json").read_text()
        )
        self.assertEqual(profile, golden["profile"])
        actual, expected = deepcopy(result["entries"]), deepcopy(golden["entries"])
        for entry in actual + expected:
            normalize_ordered_contract(entry)
        self.assertEqual(actual, expected)
        by_id = {entry["id"]: entry for entry in result["entries"]}
        self.assertEqual([[6, 9, 4]], by_id["sql-1251"]["rows"])
        self.assertEqual([20, 20, 20], by_id["sql-1251"]["column_oids"])
        self.assertEqual([[44, 352]], by_id["sql-1250"]["rows"])
        self.assertEqual({"a": 1, "b": 1, "c": 1}, dict(by_id["sql-1245"]["rows"]))
        self.assertEqual({"a": 0, "b": 0, "c": 0}, dict(by_id["sql-1248"]["rows"]))
        self.assertEqual(1, len(by_id["sql-1248"]["ordered_groups"]))
        for case in cases:
            observer = aggregate_order_observer(case)
            if case["id"] < "sql-1250":
                self.assertIsNotNone(observer)
                self.assertNotIn("LIMIT", observer)
                self.assertIn(" AS order_key", observer)
            else:
                self.assertIsNone(observer)

    def test_original_set_campaign_preserves_duplicate_and_null_witnesses(self):
        import json
        from collections import Counter

        manifest = json.loads((FIXTURES / "sql_set_read_campaign.json").read_text())
        ids = {entry["id"] for entry in manifest["entries"]}
        self.assertEqual(21, len(ids))
        inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        cases = [case for case in inventory if case["id"] in ids]
        profile = set_read_profile()
        self.assertEqual(2, len(profile["additional_tables"]))
        result = read_reference(self.db, cases, profile)
        self.assertEqual([], result["excluded"])
        self.assertEqual(ids, {entry["id"] for entry in result["entries"]})
        empty_intersections = {"sql-0459", "sql-0516", "sql-0541"}
        self.assertEqual(
            empty_intersections,
            {entry["id"] for entry in result["entries"] if not entry["rows"]},
        )
        # Retain all eighteen nonempty multiplicity/NULL witnesses. Legitimate
        # contradictory or disjoint-expression intersections are separate
        # exact contracts, not a weakened empty-result acceptance rule.
        self.assertEqual(18, sum(bool(entry["rows"]) for entry in result["entries"]))
        golden = json.loads(
            (FIXTURES / "sql_set_read_campaign_reference.json").read_text()
        )
        self.assertEqual(profile, golden["profile"])
        expected = {entry["id"]: entry for entry in golden["entries"]}
        for entry in result["entries"]:
            self.assertEqual(expected[entry["id"]], entry)
        by_id = {entry["id"]: entry for entry in result["entries"]}
        self.assertEqual(
            Counter({"a": 3, "b": 1, "c": 1, "d": 1, None: 1}),
            Counter(row[0] for row in by_id["sql-0476"]["rows"]),
        )
        self.assertEqual({"a", None}, {row[0] for row in by_id["sql-0544"]["rows"]})
        self.assertEqual({"b", "c"}, {row[0] for row in by_id["sql-0543"]["rows"]})

    def test_negative_set_contracts_reject_missing_input_witnesses(self):
        import json

        ids = {"sql-0459", "sql-0516", "sql-0541"}
        cases = [
            case
            for case in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
            if case["id"] in ids
        ]
        profile = set_read_profile()
        profile["rows"] = []
        result = read_reference(self.db, cases, profile)
        self.assertEqual([], result["entries"])
        self.assertEqual(ids, {entry["id"] for entry in result["excluded"]})
        self.assertTrue(
            all(
                "empty result does not exercise" in entry["reason"]
                for entry in result["excluded"]
            )
        )

    def test_unlisted_empty_read_still_requires_a_witness(self):
        result = read_reference(
            self.db,
            [
                {
                    "id": "unlisted-empty-read",
                    "sql": "SELECT id FROM usage_records WHERE false",
                    "params": [],
                }
            ],
            set_read_profile(),
        )
        self.assertEqual([], result["entries"])
        self.assertEqual(
            "empty result does not exercise this shape", result["excluded"][0]["reason"]
        )

    def test_original_lateral_campaign_and_postgres_alias_scope(self):
        import json
        from pathlib import Path
        import psycopg
        from generate_sql_postgres_reference import create_table, properties

        fixtures = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
        )
        profile = json.loads(
            (fixtures / "sql_lateral_campaign_profile.json").read_text()
        )
        self.assertEqual(1, len(profile["additional_tables"]))
        self.assertEqual("balance_records", profile["additional_tables"][0]["name"])
        self.assertEqual(profile["schema"], profile["additional_tables"][0]["schema"])
        inventory = json.loads((fixtures / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        cases = [
            case
            for case in inventory
            if "sql-1345" <= case["id"] <= "sql-1365"
            or case["id"] in {"sql-0549", "sql-1217", "sql-1218"}
        ]
        self.assertEqual(24, len(cases))
        result = read_reference(self.db, cases, profile)
        self.assertEqual(23, len(result["entries"]))
        self.assertEqual(["sql-1357"], [case["id"] for case in result["excluded"]])
        self.assertTrue(all(case["rows"] for case in result["entries"]))
        # LIMIT must not make an all-unmatched result a vacuous proof of the
        # inner predicate. Every two-column original exposes a real match.
        for case in result["entries"]:
            if len(case["columns"]) == 2:
                self.assertTrue(
                    any(not flags[1] for flags in case["sql_nulls"]), case["id"]
                )
        with self.db.transaction(force_rollback=True):
            create_table(
                self.db, "usage_records", properties(profile["schema"]), profile["rows"]
            )
            invalid = next(case for case in cases if case["id"] == "sql-1357")
            with self.assertRaises(psycopg.errors.UndefinedColumn) as error:
                with self.db.transaction(force_rollback=True):
                    self.db.execute(invalid["sql"])
            self.assertEqual("42703", error.exception.sqlstate)

    def test_conditional_subquery_demand_reference(self):
        import json
        from pathlib import Path
        import psycopg

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_conditional_subquery_reference.json"
            ).read_text()
        )
        self.assertEqual(85, len(fixture["entries"]))
        self.assertEqual(44, len(fixture["errors"]))
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                self.assertEqual(
                    [(value,) for value in case["rows"]],
                    self.db.execute(case["sql"]).fetchall(),
                )
        for case in fixture["errors"]:
            with self.subTest(sql=case["sql"]):
                with self.assertRaises(psycopg.Error) as error:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(case["sql"]).fetchall()
                self.assertEqual(case["code"], error.exception.sqlstate)

    def test_sorted_scalar_outputs_retain_computed_aliases(self):
        for sql in (
            "SELECT o.x+1 AS rank,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY rank DESC LIMIT 1",
            "SELECT (SELECT o.x+1) AS rank,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY rank DESC LIMIT 1",
        ):
            with self.subTest(sql=sql):
                cursor = self.db.execute(sql)
                self.assertEqual(
                    ["rank", "v"], [column.name for column in cursor.description]
                )
                self.assertEqual([(3, None)], cursor.fetchall())

    def test_negative_row_bounds_validate_only_demanded_execution(self):
        import psycopg

        for sql, code in (
            ("SELECT 1 LIMIT $1", "2201W"),
            ("SELECT 1 OFFSET $1", "2201X"),
            ("SELECT 1 FETCH NEXT $1 ROWS ONLY", "2201W"),
            ("SELECT i.x FROM (SELECT 1 AS x) i LIMIT $1", "2201W"),
            ("SELECT i.x FROM (SELECT 1 AS x) i OFFSET $1", "2201X"),
            ("SELECT (SELECT i.x FROM (SELECT 1 AS x) i LIMIT $1)", "2201W"),
            ("SELECT (SELECT i.x FROM (SELECT 1 AS x) i OFFSET $1)", "2201X"),
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(psycopg.Error) as error:
                    with self.db.transaction(force_rollback=True):
                        with psycopg.RawCursor(self.db) as cursor:
                            cursor.execute(sql, [-1])
                            cursor.fetchall()
                self.assertEqual(code, error.exception.sqlstate)
        self.assertEqual(
            [(7,)],
            self.db.execute(
                "SELECT CASE WHEN FALSE THEN (SELECT 1 LIMIT -1) ELSE 7 END"
            ).fetchall(),
        )

    def test_scalar_cardinality_stops_before_third_row_value_errors(self):
        import psycopg

        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE scalar_cardinality_source(delta bigint)")
            self.db.execute(
                "INSERT INTO scalar_cardinality_source VALUES (10),(20),(30)"
            )
            for suffix, parameters in [
                ("", []),
                (" LIMIT 1000", []),
                (" LIMIT $1", [1000]),
                (" LIMIT $1", [None]),
            ]:
                sql = (
                    "SELECT (SELECT CASE WHEN delta=30 THEN 1/(delta-30) ELSE delta END "
                    "FROM scalar_cardinality_source" + suffix + ")"
                )
                with self.subTest(suffix=suffix, parameters=parameters):
                    with self.assertRaises(
                        psycopg.errors.CardinalityViolation
                    ) as error:
                        with self.db.transaction(force_rollback=True):
                            with psycopg.RawCursor(self.db) as cursor:
                                cursor.execute(sql, parameters)
                                cursor.fetchall()
                    self.assertEqual("21000", error.exception.sqlstate)
            self.assertEqual(
                [(0,), (None,)],
                self.db.execute(
                    "SELECT (SELECT 1/(s.delta-20) FROM scalar_cardinality_source s WHERE s.delta=o.x) "
                    "FROM (VALUES (10),(11)) o(x) ORDER BY o.x"
                ).fetchall(),
            )

    def test_correlated_aggregate_ownership_requires_outer_query_execution(self):
        # These are still native admission boundaries, not resolved parity
        # cases. Record the oracle result so a per-parent SUM cannot silently
        # replace PostgreSQL's outer-owned aggregate in a later activation.
        for sql in [
            "SELECT (SELECT SUM(o.x)) FROM (SELECT 1 AS x UNION ALL SELECT 2) o",
            "SELECT CASE WHEN TRUE THEN (SELECT SUM(o.x)) ELSE 0 END FROM (SELECT 1 AS x UNION ALL SELECT 2) o",
            "SELECT (SELECT SUM(x)) FROM (SELECT 1 AS x UNION ALL SELECT 2) o",
        ]:
            with self.subTest(sql=sql):
                self.assertEqual([(3,)], self.db.execute(sql).fetchall())

    def test_logical_json_parameters_and_identity_casts_preserve_strings(self):
        from psycopg.types.json import Jsonb

        for value in ["pro", "null", "true", "12", "[1,2]", '{"x":1}', '"quoted"']:
            for expression in [
                "%s::jsonb",
                "CAST(%s::jsonb AS json)",
                "CAST(%s::jsonb AS jsonb)",
            ]:
                with self.subTest(value=value, expression=expression):
                    row = self.db.execute(
                        "SELECT " + expression, [Jsonb(value)]
                    ).fetchone()
                    self.assertEqual((value,), row)

    def test_jsonb_path_update_reference(self):
        import json
        from pathlib import Path
        import psycopg

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_json_path_update_reference.json"
            ).read_text()
        )
        self.assertEqual(32, len(fixture["entries"]))
        self.assertEqual(10, len(fixture["errors"]))
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                value, sql_null = self.db.execute(
                    "SELECT v,v IS NULL FROM (SELECT " + case["sql"] + " AS v) q"
                ).fetchone()
                self.assertEqual(case["value"], value)
                self.assertEqual(case.get("sql_null", False), sql_null)
        for case in fixture["errors"]:
            with self.subTest(sql=case["sql"]):
                with self.assertRaises(psycopg.Error) as error:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute("SELECT " + case["sql"])
                self.assertEqual(case["code"], error.exception.sqlstate)

    def test_jsonb_concatenation_reference(self):
        import json
        from pathlib import Path

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_json_concat_reference.json"
            ).read_text()
        )
        self.assertEqual(20, len(fixture["entries"]))
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                value, sql_null = self.db.execute(
                    "SELECT v,v IS NULL FROM (SELECT " + case["sql"] + " AS v) q"
                ).fetchone()
                self.assertEqual(case["value"], value)
                self.assertEqual(case.get("sql_null", False), sql_null)

    def test_returning_correlates_postimages_but_reads_the_statement_snapshot(self):
        cases = [
            (
                "UPDATE target SET n=n+10,cold='new' RETURNING n,(SELECT delta FROM source WHERE id='a') AS x",
                [(11, 10), (12, 10)],
            ),
            (
                "UPDATE target t SET n=n+9,cold='new' RETURNING n,(SELECT delta FROM source s WHERE s.delta=t.n) AS x",
                [(10, 10), (11, None)],
            ),
            (
                "DELETE FROM target RETURNING n,(SELECT delta FROM source WHERE id='a') AS x",
                [(1, 10), (2, 10)],
            ),
            (
                "UPDATE target SET n=n+10,cold='new' RETURNING n,(SELECT n FROM target WHERE _id='a') AS previous",
                [(11, 1), (12, 1)],
            ),
            (
                "UPDATE target SET cold='new' WHERE n<0 RETURNING (SELECT delta FROM source)",
                [],
            ),
            (
                "UPDATE target t SET n=n+10,cold=DEFAULT,g=DEFAULT RETURNING g,(SELECT delta FROM source s WHERE s.delta=t.g-2) AS matched",
                [(22, 20), (24, None)],
            ),
            (
                "INSERT INTO target(n,payload,cold) VALUES(9,'null'::jsonb,'new') RETURNING n,(SELECT delta FROM source WHERE id='a') AS x",
                [(9, 10)],
            ),
            (
                "INSERT INTO target(n,payload,cold) SELECT delta,'null'::jsonb,'new' FROM source WHERE id='a' RETURNING n,(SELECT delta FROM source WHERE id='b') AS x",
                [(10, 20)],
            ),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                self.db.execute(
                    "CREATE TABLE target(_id text PRIMARY KEY DEFAULT 'fresh',n bigint,payload jsonb,cold text DEFAULT 'default',g bigint GENERATED ALWAYS AS (n*2) STORED)"
                )
                self.db.execute(
                    "INSERT INTO target(_id,n,payload,cold) VALUES ('a',1,'null','old'),('b',2,'null','old')"
                )
                self.db.execute("CREATE TABLE source(id text,delta bigint)")
                self.db.execute("INSERT INTO source VALUES ('a',10),('b',20)")
                self.assertEqual(expected, self.db.execute(sql).fetchall())

    def test_returning_cardinality_and_projection_errors_abort_the_mutation(self):
        import psycopg

        for sql, failure in [
            (
                "UPDATE target SET cold='new' RETURNING (SELECT delta FROM source)",
                psycopg.errors.CardinalityViolation,
            ),
            (
                "DELETE FROM target RETURNING (SELECT delta/0 FROM source WHERE id='a')",
                psycopg.errors.DivisionByZero,
            ),
        ]:
            with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                self.db.execute(
                    "CREATE TABLE target(_id text PRIMARY KEY,n bigint,payload jsonb,cold text)"
                )
                self.db.execute(
                    "INSERT INTO target VALUES ('a',1,'null','old'),('b',2,'null','old')"
                )
                self.db.execute("CREATE TABLE source(id text,delta bigint)")
                self.db.execute("INSERT INTO source VALUES ('a',10),('b',20)")
                with self.assertRaises(failure):
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(sql)
                self.assertEqual(
                    [("a", 1, "old"), ("b", 2, "old")],
                    self.db.execute(
                        "SELECT _id,n,cold FROM target ORDER BY _id"
                    ).fetchall(),
                )

    def test_json_existence_sets_match_typed_native_contracts(self):
        import json
        import psycopg

        fixture = json.loads((FIXTURES / "sql_json_exists_reference.json").read_text())
        self.assertEqual(22, len(fixture["entries"]))
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                cursor = self.db.execute("SELECT " + case["sql"])
                self.assertEqual(16, cursor.description[0].type_code)
                self.assertEqual(case["value"], cursor.fetchone()[0])
        for expression in fixture["type_errors"]:
            with (
                self.subTest(sql=expression),
                self.assertRaises(psycopg.Error) as failure,
            ):
                self.db.execute("SELECT " + expression)
            self.assertEqual("42883", failure.exception.sqlstate)

    def test_json_and_typed_array_containment_reference(self):
        import json
        from pathlib import Path

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_containment_reference.json"
            ).read_text()
        )
        self.assertEqual(30, len(fixture["entries"]))
        for case in fixture["entries"]:
            with (
                self.subTest(sql=case["sql"]),
                self.db.transaction(force_rollback=True),
            ):
                self.assertEqual(
                    case["value"],
                    self.db.execute("SELECT " + case["sql"]).fetchone()[0],
                )

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

    def test_prepared_parameter_descriptor_and_execution_contracts(self):
        import json
        from pathlib import Path

        import psycopg

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_parameter_frame_reference.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["entries"]), 24)
        self.assertEqual(len(fixture["errors"]), 9)
        supported = {
            "smallint",
            "integer",
            "bigint",
            "real",
            "double precision",
            "boolean",
            "text",
            "uuid",
            "jsonb",
            "timestamptz",
        }
        for case in fixture["entries"] + fixture["errors"]:
            with self.subTest(sql=case["sql"]):
                for name in case["types"]:
                    self.assertIn(name.removesuffix("[]"), supported)
                try:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(
                            "PREPARE frame_contract("
                            + ",".join(case["types"])
                            + ") AS SELECT "
                            + case["sql"]
                        )
                        if "oids" in case:
                            self.assertEqual(
                                self.db.execute(
                                    "SELECT parameter_types::oid[] FROM pg_prepared_statements "
                                    "WHERE name='frame_contract'"
                                ).fetchone()[0],
                                case["oids"],
                            )
                        arguments = psycopg.sql.SQL(",").join(
                            psycopg.sql.Literal(value) for value in case["values"]
                        )
                        actual = self.db.execute(
                            psycopg.sql.SQL("EXECUTE frame_contract({})").format(
                                arguments
                            )
                        ).fetchone()[0]
                        self.assertNotIn("code", case)
                        self.assertEqual(actual, case["value"])
                except psycopg.Error as error:
                    self.assertIn("code", case)
                    self.assertEqual(error.sqlstate, case["code"])
                finally:
                    self.db.execute("DEALLOCATE ALL")

    def test_binary_array_input_oid_mismatch_uses_datatype_mismatch(self):
        import struct
        import psycopg
        from psycopg.adapt import Dumper
        from psycopg.pq import Format

        class Payload:
            pass

        class ArrayDumper(Dumper):
            oid = 1016
            format = Format.BINARY

            def dump(self, obj):
                # Declared bigint[] parameter, binary header claims int4 cells.
                return struct.pack("!iii", 0, 0, 23)

        self.db.adapters.register_dumper(Payload, ArrayDumper)
        with self.db.transaction(force_rollback=True):
            with self.assertRaises(psycopg.Error) as raised:
                self.db.execute("SELECT %s", (Payload(),))
            self.assertEqual("42804", raised.exception.sqlstate)

    def test_wire_parameter_descriptors_preserve_declared_and_inferred_array_oids(self):
        for sql_type, oid in (
            ("boolean", 1000),
            ("smallint", 1005),
            ("integer", 1007),
            ("bigint", 1016),
            ("real", 1021),
            ("double precision", 1022),
            ("text", 1009),
            ("uuid", 2951),
            ("jsonb", 3807),
        ):
            for declared in (False, True):
                with self.subTest(sql_type=sql_type, declared=declared):
                    types = f"({sql_type}[])" if declared else ""
                    self.db.execute(
                        f"PREPARE wire_array {types} AS SELECT cardinality($1::{sql_type}[])"
                    )
                    actual = self.db.execute(
                        "SELECT parameter_types[1]::oid FROM pg_prepared_statements "
                        "WHERE name='wire_array'"
                    ).fetchone()[0]
                    self.assertEqual(oid, actual)
                    self.db.execute("DEALLOCATE wire_array", prepare=False)
        self.db.execute(
            "PREPARE wire_array(bigint[]) AS SELECT $1 a,cardinality($1),array_lower($1,1)"
        )
        row = self.db.execute(
            "EXECUTE wire_array('[-1:0]={9007199254740993,NULL}')"
        ).fetchone()
        self.assertEqual(([9007199254740993, None], 2, -1), row)
        self.assertEqual(
            (None, None, None), self.db.execute("EXECUTE wire_array(NULL)").fetchone()
        )
        self.db.execute("DEALLOCATE wire_array", prepare=False)

    def test_prepared_parameter_inference_retains_builtin_widths(self):
        for expression, oids in (
            ("$1::smallint", [21]),
            ("$1 = ANY($2::smallint[])", [21, 1005]),
            (
                "ARRAY[cardinality($1::integer[]),cardinality($1::text[])]",
                [1007],
            ),
        ):
            with self.subTest(sql=expression):
                try:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(
                            "PREPARE inferred_frame AS SELECT " + expression
                        )
                        self.assertEqual(
                            self.db.execute(
                                "SELECT parameter_types::oid[] FROM pg_prepared_statements "
                                "WHERE name='inferred_frame'"
                            ).fetchone()[0],
                            oids,
                        )
                finally:
                    self.db.execute("DEALLOCATE ALL")

    def test_statement_parameter_frames_match_postgres_target_and_mutation_typing(self):
        import psycopg

        cases = (
            (
                "(bigint[])",
                "SELECT $1, cardinality($1::bigint[]) FROM frame_items "
                "WHERE needle = ANY($1) ORDER BY cardinality($1)",
                [1016],
            ),
            (
                "",
                "SELECT cardinality($1::bigint[]), $1 FROM frame_items "
                "WHERE needle = ANY($1) ORDER BY cardinality($1)",
                [1016],
            ),
            ("", "SELECT cardinality($1::integer[]), $1", [1007]),
            (
                "",
                "UPDATE frame_items SET needle = cardinality($1::integer[]) "
                "WHERE needle = $2",
                [1007, 20],
            ),
            ("", "UPDATE frame_items SET needle = $1 WHERE needle = $2", [20, 20]),
            (
                "",
                "INSERT INTO frame_items (needle) "
                "VALUES (cardinality($1::integer[])), ($2)",
                [1007, 20],
            ),
        )
        for declarations, sql, oids in cases:
            with self.subTest(sql=sql):
                try:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute("CREATE TABLE frame_items(needle bigint)")
                        self.db.execute(
                            "PREPARE statement_frame" + declarations + " AS " + sql
                        )
                        self.assertEqual(
                            self.db.execute(
                                "SELECT parameter_types::oid[] FROM pg_prepared_statements "
                                "WHERE name='statement_frame'"
                            ).fetchone()[0],
                            oids,
                        )
                finally:
                    self.db.execute("DEALLOCATE ALL")
        for sql in (
            "SELECT $1, cardinality($1::integer[])",
            "SELECT $1, $1::smallint",
            "SELECT $1, $1 + 1",
        ):
            with self.subTest(sql=sql):
                try:
                    with self.db.transaction(force_rollback=True):
                        with self.assertRaises(psycopg.Error) as error:
                            self.db.execute("PREPARE statement_frame AS " + sql)
                        self.assertEqual(error.exception.sqlstate, "42P08")
                finally:
                    self.db.execute("DEALLOCATE ALL")

    def test_statement_parameter_integer_width_is_an_ingress_contract(self):
        import psycopg

        try:
            self.db.execute("PREPARE width_frame AS SELECT $1 + 1, $1")
            self.assertEqual(
                [23],
                self.db.execute(
                    "SELECT parameter_types::oid[] FROM pg_prepared_statements "
                    "WHERE name='width_frame'"
                ).fetchone()[0],
            )
            self.assertEqual(
                [(42, 41)], self.db.execute("EXECUTE width_frame(41)").fetchall()
            )
            with self.db.transaction(force_rollback=True):
                with self.assertRaises(psycopg.Error) as error:
                    self.db.execute("EXECUTE width_frame(9007199254740993)")
                self.assertEqual("22003", error.exception.sqlstate)
        finally:
            self.db.execute("DEALLOCATE ALL")

    def test_shared_invocation_array_plans_and_navigation_promotions(self):
        queries = (
            "SELECT cardinality($1::bigint[]),array_lower($1,1)",
            "SELECT cardinality(q.a),array_lower(q.a,1) FROM (SELECT $1::bigint[] a) q",
            "WITH q AS MATERIALIZED (SELECT $1::bigint[] a) SELECT cardinality(l.a),array_lower(r.a,1) FROM q l CROSS JOIN q r",
            "SELECT max(cardinality($1::bigint[])),array_lower($1,1) FROM frame_items",
            "SELECT cardinality(a),array_lower(a,1) FROM (SELECT $1::bigint[] a UNION SELECT $1::bigint[]) q",
        )
        try:
            with self.db.transaction(force_rollback=True):
                self.db.execute("CREATE TABLE frame_items(_id text,id bigint)")
                self.db.execute(
                    "INSERT INTO frame_items VALUES('0',9007199254740993),('1',9007199254740993)"
                )
                for sql in queries:
                    with self.subTest(sql=sql):
                        self.db.execute("PREPARE array_frame AS " + sql)
                        self.assertEqual(
                            [(3, -1)],
                            self.db.execute(
                                "EXECUTE array_frame('[-1:1]={9007199254740993,NULL,2}')"
                            ).fetchall(),
                        )
                        self.db.execute("DEALLOCATE array_frame")
                self.db.execute(
                    "PREPARE array_frame AS SELECT lag($1::int2[],1,$2::float8[]) "
                    "OVER (ORDER BY _id) a FROM frame_items"
                )
                self.assertEqual(
                    [([3.5, None],), ([1.0, None, 2.0],)],
                    self.db.execute(
                        "EXECUTE array_frame('[-1:1]={1,NULL,2}','[0:1]={3.5,NULL}')"
                    ).fetchall(),
                )
                self.db.execute("DEALLOCATE array_frame")
                self.db.execute(
                    "PREPARE array_frame AS WITH RECURSIVE r(n) AS "
                    "(SELECT cardinality($1::integer[]) UNION ALL SELECT n-1 FROM r "
                    "WHERE n>1) SELECT n FROM r ORDER BY n"
                )
                self.assertEqual(
                    [(1,), (2,), (3,)],
                    self.db.execute("EXECUTE array_frame('{1,2,3}')").fetchall(),
                )
        finally:
            self.db.execute("DEALLOCATE ALL")

    def test_assignment_does_not_widen_an_operator_parameter_contract(self):
        import psycopg

        try:
            with self.db.transaction(force_rollback=True):
                self.db.execute("CREATE TABLE width_items(_id text,n bigint)")
                for cast, oid in (("", 23), ("::bigint", 20)):
                    self.db.execute(
                        "PREPARE assignment_frame AS INSERT INTO width_items(_id,n) "
                        f"VALUES(lower('A'),$1{cast}+1),('b',$1{cast}+1)"
                    )
                    self.assertEqual(
                        [oid],
                        self.db.execute(
                            "SELECT parameter_types::oid[] FROM pg_prepared_statements "
                            "WHERE name='assignment_frame'"
                        ).fetchone()[0],
                    )
                    if cast:
                        self.db.execute("EXECUTE assignment_frame(9007199254740992)")
                        self.assertEqual(
                            [(9007199254740993,), (9007199254740993,)],
                            self.db.execute("SELECT n FROM width_items").fetchall(),
                        )
                    else:
                        with self.db.transaction(force_rollback=True):
                            with self.assertRaises(psycopg.Error) as error:
                                self.db.execute(
                                    "EXECUTE assignment_frame(9007199254740992)"
                                )
                            self.assertEqual("22003", error.exception.sqlstate)
                    self.db.execute("DEALLOCATE assignment_frame")
                self.db.execute(
                    "PREPARE function_frame AS SELECT sqrt(16)>$1,power(2,3)>$1"
                )
                self.assertEqual(
                    [701],
                    self.db.execute(
                        "SELECT parameter_types::oid[] FROM pg_prepared_statements "
                        "WHERE name='function_frame'"
                    ).fetchone()[0],
                )
                self.assertEqual(
                    [(True, True)],
                    self.db.execute("EXECUTE function_frame(3)").fetchall(),
                )
        finally:
            self.db.execute("DEALLOCATE ALL")

    def test_materialized_relation_replay_preserves_self_join_multiplicity(self):
        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE items(n bigint)")
            self.db.execute("INSERT INTO items SELECT generate_series(0,4095)")
            self.assertEqual(
                self.db.execute(
                    "WITH cached AS MATERIALIZED (SELECT n FROM items) "
                    "SELECT count(*) FROM cached a JOIN cached b ON a.n=b.n"
                ).fetchone(),
                (4096,),
            )

    def test_typed_relation_array_scalar_and_grouped_contracts(self):
        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE items(a bigint[])")
            self.db.execute(
                "INSERT INTO items VALUES ('[-3:-2]={9007199254740993,NULL}')"
            )
            cases = [
                (
                    "SELECT cardinality(a), array_lower(a, 1), array_upper(a, 1), "
                    "9007199254740993 = ANY(a), 9007199254740992 = ANY(a) FROM items",
                    (2, -3, -2, True, None),
                ),
                (
                    "SELECT cardinality(t.a), array_lower(t.a, 1), "
                    "9007199254740993 = ANY(t.a) FROM items t CROSS JOIN items u",
                    (2, -3, True),
                ),
                (
                    "SELECT sum(cardinality(t.a)) FROM items t CROSS JOIN items u",
                    (2,),
                ),
            ]
            for sql, expected in cases:
                with self.subTest(sql=sql):
                    self.assertEqual(self.db.execute(sql).fetchall(), [expected])

    def test_internal_array_query_boundaries(self):
        cases = [
            (
                "WITH q AS (SELECT ARRAY[1,NULL,3]::bigint[] a) "
                "SELECT cardinality(a), array_length(a,1) FROM q",
                [(3, 3)],
            ),
            (
                "WITH q AS MATERIALIZED (SELECT ARRAY[1,NULL,3]::bigint[] a) "
                "SELECT cardinality(a), array_lower(a,1) FROM q",
                [(3, 1)],
            ),
            (
                "SELECT cardinality(a) FROM "
                "(SELECT ARRAY[1,NULL,3]::bigint[] a ORDER BY 1) q",
                [(3,)],
            ),
            (
                "SELECT cardinality(a) FROM (SELECT ARRAY[1,NULL,3]::bigint[] a "
                "UNION ALL SELECT ARRAY[4]::bigint[]) q ORDER BY 1",
                [(1,), (3,)],
            ),
            (
                "SELECT cardinality(p), array_length(p,1), 1.5 = ANY(p), "
                "2.5 = ANY(p), 2.0 = ANY(p) FROM "
                "(SELECT percentile_cont(ARRAY[0.25,NULL,0.75]) WITHIN GROUP "
                "(ORDER BY x) p FROM (SELECT 1.0 x UNION ALL SELECT 3.0 x) t) q",
                [(3, 3, True, True, None)],
            ),
            (
                "SELECT cardinality(a), row_number() OVER (ORDER BY cardinality(a)) "
                "FROM (SELECT ARRAY[1,2]::bigint[] a UNION ALL "
                "SELECT ARRAY[3]::bigint[]) q ORDER BY 1",
                [(1, 1), (2, 2)],
            ),
            (
                "SELECT cardinality((SELECT ARRAY[1,NULL,3]::bigint[]))",
                [(3,)],
            ),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                self.assertEqual(self.db.execute(sql).fetchall(), expected)

    def test_typed_array_scalar_expression_contracts(self):
        import json
        from pathlib import Path

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_expression_reference.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["entries"]), 164)
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
        self.assertEqual(len(fixture["entries"]), 45)
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                with self.assertRaises(psycopg.Error) as caught:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute("SET TRANSACTION READ ONLY")
                        self.db.execute("SELECT " + case["sql"])
                self.assertEqual(caught.exception.sqlstate, case["code"])

    def test_array_element_overflow_retains_numeric_sqlstate(self):
        import psycopg

        for kind, value in (
            ("int2", "32768"),
            ("int4", "2147483648"),
            ("int8", "9223372036854775808"),
            ("int8", "-9223372036854775809"),
            ("int8", "111111111111111111111111"),
        ):
            with self.subTest(kind=kind, value=value):
                with self.assertRaises(psycopg.Error) as error:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(
                            "SELECT %s::" + kind + "[]", ("{" + value + "}",)
                        )
                self.assertEqual("22003", error.exception.sqlstate)

    def test_public_array_result_producer_contracts(self):
        # Native and mounted HTTP tests run these producer shapes. The oracle
        # independently proves the expected element OID, bounds and NULL value.
        cases = [
            (
                "SELECT ARRAY[-9223372036854775808,NULL,9223372036854775807]::bigint[]",
                "bigint",
                1016,
                "{-9223372036854775808,NULL,9223372036854775807}",
            ),
            (
                "SELECT '[0:1][3:4]={{1,NULL},{3,4}}'::int4[]",
                "int4",
                1007,
                "[0:1][3:4]={{1,NULL},{3,4}}",
            ),
            ("SELECT ARRAY[]::text[]", "text", 1009, "{}"),
            ("SELECT NULL::int4[]", "int4", 1007, None),
            (
                "SELECT ARRAY['null'::jsonb,NULL,'{\"a\":[1,2]}'::jsonb]::jsonb[]",
                "jsonb",
                3807,
                r'{"null",NULL,"{\"a\":[1,2]}"}',
            ),
            (
                "SELECT a FROM (SELECT ARRAY[1,NULL]::int2[] a UNION SELECT ARRAY[1,NULL]::int8[]) q ORDER BY 1",
                "int8",
                1016,
                "{1,NULL}",
            ),
            (
                "SELECT a FROM (VALUES(ARRAY[1,NULL]::int2[]),(ARRAY[1,NULL]::float8[])) q(a) LIMIT 1",
                "float8",
                1022,
                "{1,NULL}",
            ),
            (
                "SELECT a,row_number() OVER (ORDER BY cardinality(a)) FROM (SELECT ARRAY[1,NULL]::int4[] a) q",
                "int4",
                1007,
                "{1,NULL}",
            ),
            ("SELECT (SELECT ARRAY[1,NULL]::int4[])", "int4", 1007, "{1,NULL}"),
        ]
        for sql, kind, oid, expected in cases:
            with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                cursor = self.db.execute(sql)
                self.assertEqual(oid, cursor.description[0].type_code)
                self.assertEqual(1, len(cursor.fetchall()))
                self.assertEqual(
                    (True,),
                    self.db.execute(
                        f"SELECT a IS NOT DISTINCT FROM %s::{kind}[] FROM ({sql}) q(a)",
                        (expected,),
                    ).fetchone(),
                )

    def test_typed_array_streamed_text_output_contracts(self):
        # These exact byte strings are also asserted against the native
        # streaming encoder. PostgreSQL independently decodes their escaping,
        # bounds, numeric domains and distinction between JSON and SQL null.
        cases = [
            (
                "text",
                r'[0:1][3:4]={{NULL,"NULL"},{"a\"b","c\\d"}}',
                r'[0:1][3:4]={{NULL,"NULL"},{"a\"b","c\\d"}}',
            ),
            (
                "int8",
                "{-9223372036854775808,9223372036854775807,NULL}",
                "{-9223372036854775808,9223372036854775807,NULL}",
            ),
            (
                "real",
                "{NaN,Infinity,-Infinity,-0,1.5}",
                "{NaN,Infinity,-Infinity,-0,1.5}",
            ),
            ("bool", "{true,false,NULL}", "{t,f,NULL}"),
            (
                "jsonb",
                r'{"null",NULL,"[1,2]","{\"k\":\"a\\\\b\"}"}',
                r'{"null",NULL,"[1,2]","{\"k\":\"a\\\\b\"}"}',
            ),
            ("text", "{}", "{}"),
        ]
        for kind, source, emitted in cases:
            with self.subTest(kind=kind), self.db.transaction(force_rollback=True):
                self.assertEqual(
                    (True,),
                    self.db.execute(
                        f"SELECT %s::{kind}[] IS NOT DISTINCT FROM %s::{kind}[]",
                        (source, emitted),
                    ).fetchone(),
                )

    def test_typed_array_text_input_contracts(self):
        import json
        from pathlib import Path

        import psycopg

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_text_reference.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["entries"]), 19)
        self.assertEqual(len(fixture["errors"]), 19)
        types = {
            "text",
            "int2",
            "int4",
            "int8",
            "real",
            "float8",
            "bool",
            "uuid",
            "jsonb",
        }
        for case in fixture["entries"]:
            with self.subTest(input=case["input"], type=case["sql_type"]):
                self.assertIn(case["sql_type"], types)
                with self.db.transaction(force_rollback=True):
                    self.assertEqual(
                        self.db.execute(
                            "SELECT encode(array_send(%s::"
                            + case["sql_type"]
                            + "[]),'hex')",
                            (case["input"],),
                        ).fetchone()[0],
                        case["binary"],
                    )
        for case in fixture["errors"]:
            with self.subTest(input=case["input"], type=case["sql_type"]):
                self.assertIn(case["sql_type"], types)
                with self.assertRaises(psycopg.Error) as caught:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(
                            "SELECT %s::" + case["sql_type"] + "[]", (case["input"],)
                        )
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
    def test_golden_comparison_normalizes_only_genuine_peer_members(self):
        entry = {
            "rows": [["first"], ["peer-b"]],
            "sql_nulls": [[False], [False]],
            "ordered_groups": [
                {"rows": [["first"]], "sql_nulls": [[False]]},
                {"rows": [["peer-a"], ["peer-b"]], "sql_nulls": [[False], [False]]},
                {"rows": [["worse"]], "sql_nulls": [[False]]},
            ],
        }
        permuted = deepcopy(entry)
        permuted["rows"][1] = ["peer-a"]
        permuted["ordered_groups"][1]["rows"].reverse()
        normalize_ordered_contract(entry)
        normalize_ordered_contract(permuted)
        self.assertEqual(entry, permuted)
        self.assertEqual(2, entry["row_count"])
        # Duplicates remain significant; normalization must not turn bags into
        # sets or erase distinct SQL NULL provenance.
        duplicated = deepcopy(permuted)
        duplicated["ordered_groups"][1]["rows"].append(["peer-a"])
        duplicated["ordered_groups"][1]["sql_nulls"].append([False])
        self.assertNotEqual(entry, duplicated)

    def test_aggregate_observer_is_not_a_general_query_rewriter(self):
        sql = "SELECT organization_id, COUNT(*) AS n FROM usage_records GROUP BY organization_id ORDER BY n DESC LIMIT 5"
        self.assertIsNone(aggregate_order_observer({"family": "read", "sql": sql}))
        self.assertIsNone(
            aggregate_order_observer(
                {"family": "aggregate", "sql": sql.replace("DESC", "ASC")}
            )
        )
        self.assertIsNone(
            aggregate_order_observer(
                {"family": "aggregate", "sql": sql.replace("COUNT(*)", "SUM(quantity)")}
            )
        )
        self.assertIsNone(
            aggregate_order_observer(
                {
                    "family": "aggregate",
                    "sql": sql.replace("LIMIT 5", "LIMIT 5 OFFSET 2"),
                }
            )
        )

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
