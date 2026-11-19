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

"""Independent catalog lifecycle oracle; requires PostgreSQL 18+, never skips.

Run with the scripts environment: python -m unittest discover -s scripts
-p test_sql_catalog_lifecycle.py. Only a temporary owned database is modified.
"""

import json
import unittest

import psycopg
from generate_sql_postgres_reference import FIXTURES, postgres
from psycopg import sql as pgsql


class IndexLifecycleTest(unittest.TestCase):
    def test_postgresql18_uuid_functions_types_shifts_and_null_contracts(self):
        # Independent oracle for the native UUID codec/evaluator/default tests.
        # Assertions describe properties; volatile bytes are never golden data.
        with postgres() as db:
            db.execute("SET TIME ZONE 'UTC'")
            self.assertEqual(
                ("uuid", "smallint", "timestamp with time zone", 4, 4, 7),
                db.execute(
                    "SELECT pg_typeof(uuidv7())::text,"
                    "pg_typeof(uuid_extract_version(uuidv7()))::text,"
                    "pg_typeof(uuid_extract_timestamp(uuidv7()))::text,"
                    "uuid_extract_version(uuidv4()),"
                    "uuid_extract_version(gen_random_uuid()),"
                    "uuid_extract_version(pg_catalog.uuidv7())"
                ).fetchone(),
            )
            self.assertEqual(
                (True, True, True, True, True),
                db.execute(
                    "SELECT uuidv7(NULL::interval) IS NULL,"
                    "uuid_extract_version(NULL::uuid) IS NULL,"
                    "uuid_extract_timestamp(uuidv4()) IS NULL,"
                    "uuid_extract_version('00000000-0000-7000-0000-000000000000') IS NULL,"
                    "uuid_extract_timestamp('00000000-0000-7000-0000-000000000000') IS NULL"
                ).fetchone(),
            )
            self.assertEqual(
                ("2025-02-24 02:46:24.503+00", "2022-02-22 19:22:22+00"),
                db.execute(
                    "SELECT uuid_extract_timestamp('019535d9-3df7-79fb-b466-fa907fa17f9e')::text,"
                    "uuid_extract_timestamp('c232ab00-9414-11ec-b3c8-9f6bdeced846')::text"
                ).fetchone(),
            )
            for shift in ("1 day", "-1 day", "1 month 2 days 3 hours"):
                # Bracket generation with real clock samples; no fixed timeout
                # or wall-clock precision is assumed by the oracle assertion.
                self.assertTrue(
                    db.execute(
                        "WITH before AS MATERIALIZED (SELECT clock_timestamp() AS t),"
                        "generated AS MATERIALIZED (SELECT t,uuidv7(%s::interval) AS id FROM before) "
                        "SELECT uuid_extract_timestamp(id) >= date_trunc('milliseconds',t + %s::interval) "
                        "AND uuid_extract_timestamp(id) <= clock_timestamp() + %s::interval FROM generated",
                        (shift, shift, shift),
                    ).fetchone()[0]
                )
            for expression, state in (
                ("uuidv7(INTERVAL 'infinity')", "22008"),
                ("uuidv7(INTERVAL '-100000 years')", "22008"),
                ("uuidv7(NULL::text)", "42883"),
                ("uuid_extract_version(NULL::text)", "42883"),
            ):
                with self.assertRaises(psycopg.Error) as invalid:
                    db.execute("SELECT " + expression)
                self.assertEqual(state, invalid.exception.sqlstate)

    def test_original_secondary_index_drop_retires_enforcement_and_reuses_name(self):
        originals = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        with postgres() as db:
            db.execute(
                "CREATE TABLE usage_records "
                "(id UUID PRIMARY KEY, email TEXT, tenant_id TEXT, status TEXT)"
            )
            db.execute(
                "INSERT INTO usage_records VALUES "
                "('00000000-0000-0000-0000-000000000001',NULL,'tenant','pending'),"
                "('00000000-0000-0000-0000-000000000002','owner@example.com','tenant','active')"
            )
            insert = (
                "INSERT INTO usage_records VALUES "
                "('00000000-0000-0000-0000-000000000003',NULL,'other','pending')"
            )
            for _ in range(2):
                db.execute(
                    "CREATE UNIQUE INDEX usage_records_status_idx "
                    "ON usage_records(status)"
                )
                with self.assertRaises(psycopg.Error) as duplicate:
                    db.execute(insert)
                self.assertEqual("23505", duplicate.exception.sqlstate)
                before = db.execute(
                    "SELECT * FROM usage_records ORDER BY id"
                ).fetchall()
                self.assertEqual(
                    "DROP INDEX", db.execute(originals["sql-0724"]["sql"]).statusmessage
                )
                self.assertIsNone(
                    db.execute(
                        "SELECT to_regclass('usage_records_status_idx')"
                    ).fetchone()[0]
                )
                self.assertEqual(
                    before,
                    db.execute("SELECT * FROM usage_records ORDER BY id").fetchall(),
                )
                db.execute(insert)
                self.assertEqual(
                    [(str(row[0]), *row[1:]) for row in before]
                    + [
                        (
                            "00000000-0000-0000-0000-000000000003",
                            None,
                            "other",
                            "pending",
                        )
                    ],
                    [
                        (str(row[0]), *row[1:])
                        for row in db.execute("SELECT * FROM usage_records ORDER BY id")
                    ],
                )
                db.execute(
                    "DELETE FROM usage_records "
                    "WHERE id='00000000-0000-0000-0000-000000000003'"
                )

    def test_original_drop_not_null_preserves_uuid_rows_and_unrelated_enforcement(self):
        original = next(
            entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
            if entry["id"] == "sql-0745"
        )
        self.assertEqual([], original["params"])
        first, second, third, fourth, probe = [
            f"00000000-0000-0000-0000-{n:012}" for n in range(1, 6)
        ]
        with postgres() as db:
            db.execute(
                "CREATE TABLE usage_records(id UUID PRIMARY KEY,"
                "status TEXT NOT NULL DEFAULT 'open',"
                "status_upper TEXT GENERATED ALWAYS AS (upper(status)) STORED,"
                "amount NUMERIC CHECK(amount >= 0))"
            )
            db.execute(
                "INSERT INTO usage_records(id,status,amount) VALUES "
                f"('{first}','ready',1),('{second}','open',2)"
            )
            physical_read = (
                "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
            )
            before = db.execute(physical_read).fetchall()
            identity = db.execute("SELECT 'usage_records'::regclass::oid").fetchone()[0]
            db.execute("CREATE ROLE relaxation_reader")
            db.execute("GRANT SELECT ON usage_records TO relaxation_reader")
            with self.assertRaises(psycopg.Error) as denied, db.transaction():
                db.execute("SET LOCAL ROLE relaxation_reader")
                db.execute(original["sql"])
            self.assertEqual("42501", denied.exception.sqlstate)
            with self.assertRaises(psycopg.Error) as required:
                db.execute(
                    f"INSERT INTO usage_records(id,status,amount) VALUES ('{third}',NULL,3)"
                )
            self.assertEqual("23502", required.exception.sqlstate)
            self.assertEqual(before, db.execute(physical_read).fetchall())
            for _ in range(2):
                self.assertEqual(
                    "ALTER TABLE", db.execute(original["sql"]).statusmessage
                )
                self.assertEqual(before, db.execute(physical_read).fetchall())
                self.assertEqual(
                    identity,
                    db.execute("SELECT 'usage_records'::regclass::oid").fetchone()[0],
                )
            for statement, sqlstate in [
                ("ALTER TABLE usage_records ALTER COLUMN id DROP NOT NULL", "42P16"),
                (
                    f"INSERT INTO usage_records(id,status,amount) VALUES ('{first}',NULL,3)",
                    "23505",
                ),
                (
                    f"INSERT INTO usage_records(id,status,amount) VALUES ('{third}',NULL,-1)",
                    "23514",
                ),
            ]:
                with self.assertRaises(psycopg.Error) as rejected:
                    db.execute(statement)
                self.assertEqual(sqlstate, rejected.exception.sqlstate)
                self.assertEqual(before, db.execute(physical_read).fetchall())
            db.execute(
                f"INSERT INTO usage_records(id,status,amount) VALUES ('{third}',NULL,3)"
            )
            db.execute(f"INSERT INTO usage_records(id,amount) VALUES ('{fourth}',4)")
            self.assertEqual(
                [
                    (first, "ready", "READY", "1"),
                    (second, "open", "OPEN", "2"),
                    (third, None, None, "3"),
                    (fourth, "open", "OPEN", "4"),
                ],
                db.execute(
                    "SELECT id::text,status,status_upper,amount::text "
                    "FROM usage_records ORDER BY id"
                ).fetchall(),
            )
            stable = db.execute(physical_read).fetchall()
            db.execute(
                f"INSERT INTO usage_records(id,status,amount) VALUES ('{probe}',NULL,5)"
            )
            db.execute(f"DELETE FROM usage_records WHERE id='{probe}'")
            self.assertEqual(stable, db.execute(physical_read).fetchall())

    def test_original_uuid_set_not_null_preserves_rows_defaults_and_constraints(self):
        original = next(
            entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
            if entry["id"] == "sql-0744"
        )
        self.assertEqual([], original["params"])
        first = "00000000-0000-0000-0000-000000000001"
        second = "00000000-0000-0000-0000-000000000002"
        with postgres() as db:
            db.execute(
                "CREATE TABLE usage_records(id UUID PRIMARY KEY DEFAULT uuidv7(),"
                "status TEXT DEFAULT 'open',status_upper TEXT GENERATED ALWAYS AS "
                "(upper(status)) STORED,amount NUMERIC CHECK(amount >= 0))"
            )
            db.execute(
                "INSERT INTO usage_records(id,status,amount) VALUES "
                "(%s,NULL,1),(%s,'open',2)",
                (first, second),
            )
            read = "SELECT id::text,status,status_upper,amount::text,ctid::text,xmin::text FROM usage_records ORDER BY id"
            before = db.execute(read).fetchall()
            with self.assertRaises(psycopg.Error) as rejected:
                db.execute(original["sql"])
            self.assertEqual("23502", rejected.exception.sqlstate)
            self.assertEqual(before, db.execute(read).fetchall())
            db.execute("UPDATE usage_records SET status='ready' WHERE id=%s", (first,))
            repaired = db.execute(read).fetchall()
            self.assertEqual(
                [(first, "ready", "READY", "1"), (second, "open", "OPEN", "2")],
                [row[:4] for row in repaired],
            )
            for _ in range(2):
                self.assertEqual(
                    "ALTER TABLE", db.execute(original["sql"]).statusmessage
                )
                self.assertEqual(repaired, db.execute(read).fetchall())
            for statement, code in (
                ("INSERT INTO usage_records(status) VALUES(NULL)", "23502"),
                ("INSERT INTO usage_records(amount) VALUES(-1)", "23514"),
                (f"INSERT INTO usage_records(id) VALUES('{first}')", "23505"),
            ):
                with self.assertRaises(psycopg.Error) as rejected:
                    db.execute(statement)
                self.assertEqual(code, rejected.exception.sqlstate)
            generated = db.execute(
                "INSERT INTO usage_records(amount) VALUES(3) RETURNING id,status,status_upper"
            ).fetchone()
            self.assertEqual(7, generated[0].version)
            self.assertEqual(("open", "OPEN"), generated[1:])
            db.execute("INSERT INTO usage_records(amount) VALUES(4)")
            self.assertEqual(
                4, db.execute("SELECT count(*) FROM usage_records").fetchone()[0]
            )
            self.assertEqual(
                [(False,)],
                db.execute(
                    "SELECT attnotnull FROM pg_attribute "
                    "WHERE attrelid='usage_records'::regclass AND attname='amount'"
                ).fetchall(),
            )

    def test_column_nullability_validation_preserves_rows_and_expressions(self):
        with postgres() as db:
            db.execute(
                "CREATE TABLE usage_records ("
                "id BIGINT PRIMARY KEY, status TEXT DEFAULT 'open', "
                "status_upper TEXT GENERATED ALWAYS AS (upper(status)) STORED)"
            )
            db.execute(
                "INSERT INTO usage_records (id,status) VALUES (1,NULL),(2,'open')"
            )
            rows = db.execute("SELECT * FROM usage_records ORDER BY id").fetchall()
            with self.assertRaises(psycopg.Error) as rejected:
                db.execute(
                    "ALTER TABLE usage_records ALTER COLUMN status SET NOT NULL;"
                )
            self.assertEqual("23502", rejected.exception.sqlstate)
            self.assertEqual(
                rows, db.execute("SELECT * FROM usage_records ORDER BY id").fetchall()
            )
            self.assertEqual(
                [(False,)],
                db.execute(
                    "SELECT attnotnull FROM pg_attribute "
                    "WHERE attrelid='usage_records'::regclass AND attname='status'"
                ).fetchall(),
            )
            db.execute("UPDATE usage_records SET status='ready' WHERE id=1")
            for _ in range(2):
                self.assertEqual(
                    "ALTER TABLE",
                    db.execute(
                        "ALTER TABLE usage_records ALTER COLUMN status SET NOT NULL;"
                    ).statusmessage,
                )
            with self.assertRaises(psycopg.Error) as rejected:
                db.execute("INSERT INTO usage_records (id,status) VALUES (3,NULL)")
            self.assertEqual("23502", rejected.exception.sqlstate)
            for _ in range(2):
                self.assertEqual(
                    "ALTER TABLE",
                    db.execute(
                        "ALTER TABLE usage_records ALTER COLUMN status DROP NOT NULL;"
                    ).statusmessage,
                )
            db.execute("INSERT INTO usage_records (id,status) VALUES (3,NULL)")
            db.execute("INSERT INTO usage_records (id) VALUES (4)")
            self.assertEqual(
                [
                    (1, "ready", "READY"),
                    (2, "open", "OPEN"),
                    (3, None, None),
                    (4, "open", "OPEN"),
                ],
                db.execute("SELECT * FROM usage_records ORDER BY id").fetchall(),
            )
            with self.assertRaises(psycopg.Error) as rejected:
                db.execute("ALTER TABLE usage_records ALTER COLUMN id DROP NOT NULL")
            self.assertEqual("42P16", rejected.exception.sqlstate)

    def test_anonymous_primary_key_validation_preserves_failed_source(self):
        cases = (
            ("pk_anonymous", [(1, "a"), (2, "b")], None, False),
            ("pk_anonymous_null", [(None, "null")], "23502", False),
            ("pk_anonymous_duplicate", [(7, "a"), (7, "b")], "23505", False),
            ("pk_anonymous_collision", [(1, "a"), (2, "b")], None, True),
        )
        with postgres() as db:
            db.execute("CREATE TABLE public.name_owner (id BIGINT)")
            for name, rows, expected_error, collision in cases:
                with self.subTest(name=name):
                    relation = pgsql.Identifier("public", name)
                    db.execute(
                        pgsql.SQL("CREATE TABLE {} (id BIGINT, note TEXT)").format(
                            relation
                        )
                    )
                    for row in rows:
                        db.execute(
                            pgsql.SQL("INSERT INTO {} VALUES (%s, %s)").format(
                                relation
                            ),
                            row,
                        )
                    if collision:
                        db.execute(
                            pgsql.SQL(
                                "CREATE INDEX {} ON public.name_owner (id)"
                            ).format(pgsql.Identifier(name + "_pkey"))
                        )
                    alter = pgsql.SQL("ALTER TABLE {} ADD PRIMARY KEY (id)").format(
                        relation
                    )
                    if expected_error is None:
                        self.assertEqual("ALTER TABLE", db.execute(alter).statusmessage)
                    else:
                        with self.assertRaises(psycopg.Error) as rejected:
                            db.execute(alter)
                        self.assertEqual(expected_error, rejected.exception.sqlstate)
                    self.assertEqual(
                        rows,
                        db.execute(
                            pgsql.SQL("SELECT id, note FROM {} ORDER BY note").format(
                                relation
                            )
                        ).fetchall(),
                    )
                    expected_name = name + ("_pkey1" if collision else "_pkey")
                    self.assertEqual(
                        [(expected_name, "p", True, True)]
                        if expected_error is None
                        else [],
                        db.execute(
                            "SELECT c.conname, c.contype, i.indisvalid, i.indisready "
                            "FROM pg_constraint c JOIN pg_index i "
                            "ON i.indexrelid=c.conindid WHERE c.conrelid=%s::regclass",
                            ("public." + name,),
                        ).fetchall(),
                    )
                    self.assertEqual(
                        [(expected_error is None,)],
                        db.execute(
                            "SELECT attnotnull FROM pg_attribute "
                            "WHERE attrelid=%s::regclass AND attname='id'",
                            ("public." + name,),
                        ).fetchall(),
                    )
                    self.assertEqual(
                        [(1 if expected_error is None else 0,)],
                        db.execute(
                            "SELECT count(*) FROM pg_index WHERE indrelid=%s::regclass",
                            ("public." + name,),
                        ).fetchall(),
                    )
                    if expected_error is None:
                        with self.assertRaises(psycopg.Error) as duplicate:
                            db.execute(
                                pgsql.SQL(
                                    "INSERT INTO {} VALUES (1, 'duplicate')"
                                ).format(relation)
                            )
                        self.assertEqual("23505", duplicate.exception.sqlstate)
                        self.assertEqual(
                            rows,
                            db.execute(
                                pgsql.SQL(
                                    "SELECT id, note FROM {} ORDER BY note"
                                ).format(relation)
                            ).fetchall(),
                        )

    def test_alter_generated_names_distinguish_index_and_constraint_ownership(self):
        cases = (
            (
                "CREATE UNIQUE INDEX t_id_check ON public.t (id)",
                "ADD CHECK (id > 0)",
                [("t_id_check", "c")],
                [("t_id_check",)],
            ),
            (
                "CREATE INDEX t_id_key ON public.t (id)",
                "ADD UNIQUE (id)",
                [("t_id_key1", "u")],
                [("t_id_key",), ("t_id_key1",)],
            ),
            (
                "ALTER TABLE public.t ADD CONSTRAINT t_id_fkey CHECK (true)",
                "ADD FOREIGN KEY (id) REFERENCES public.parent (id)",
                [("t_id_fkey", "c"), ("t_id_fkey1", "f")],
                [],
            ),
            (
                "ALTER TABLE public.t ADD CONSTRAINT t_id_check CHECK (id > 0)",
                "ADD CHECK (id < 100)",
                [("t_id_check", "c"), ("t_id_check1", "c")],
                [],
            ),
        )
        with postgres() as db:
            db.execute("CREATE TABLE public.parent (id BIGINT PRIMARY KEY)")
            db.execute("INSERT INTO public.parent VALUES (1)")
            for setup, addition, constraints, indexes in cases:
                with self.subTest(addition=addition, setup=setup):
                    db.execute("CREATE TABLE public.t (id BIGINT)")
                    db.execute("INSERT INTO public.t VALUES (1)")
                    db.execute(setup)
                    db.execute("ALTER TABLE public.t " + addition)
                    self.assertEqual(
                        constraints,
                        db.execute(
                            "SELECT conname, contype FROM pg_constraint "
                            "WHERE conrelid='public.t'::regclass ORDER BY conname"
                        ).fetchall(),
                    )
                    self.assertEqual(
                        indexes,
                        db.execute(
                            "SELECT c.relname FROM pg_index i JOIN pg_class c "
                            "ON c.oid=i.indexrelid WHERE i.indrelid='public.t'::regclass "
                            "ORDER BY c.relname"
                        ).fetchall(),
                    )
                    self.assertEqual(
                        [(1,)], db.execute("SELECT * FROM public.t").fetchall()
                    )
                    db.execute("DROP TABLE public.t")

    def test_equivalent_index_constraints_normalize_before_final_name_selection(self):
        # Name, kind, deferrable, initially deferred, NULLS NOT DISTINCT.
        cases = (
            ("id BIGINT PRIMARY KEY UNIQUE", [("t_pkey", "p", False, False, False)]),
            ("id BIGINT UNIQUE PRIMARY KEY", [("t_pkey", "p", False, False, False)]),
            (
                "id BIGINT PRIMARY KEY CONSTRAINT named UNIQUE",
                [("named", "p", False, False, False)],
            ),
            (
                "id BIGINT CONSTRAINT named UNIQUE PRIMARY KEY",
                [("named", "p", False, False, False)],
            ),
            (
                "id BIGINT CONSTRAINT first UNIQUE CONSTRAINT second UNIQUE",
                [("first", "u", False, False, False)],
            ),
            (
                "id BIGINT UNIQUE CONSTRAINT named UNIQUE",
                [("named", "u", False, False, False)],
            ),
            (
                "a BIGINT, b BIGINT, UNIQUE (a, b), UNIQUE (b, a)",
                [
                    ("t_a_b_key", "u", False, False, False),
                    ("t_b_a_key", "u", False, False, False),
                ],
            ),
            (
                "id BIGINT UNIQUE NULLS DISTINCT UNIQUE NULLS NOT DISTINCT",
                [
                    ("t_id_key", "u", False, False, False),
                    ("t_id_key1", "u", False, False, True),
                ],
            ),
            (
                "id BIGINT UNIQUE DEFERRABLE INITIALLY IMMEDIATE UNIQUE DEFERRABLE INITIALLY DEFERRED",
                [
                    ("t_id_key", "u", True, False, False),
                    ("t_id_key1", "u", True, True, False),
                ],
            ),
            (
                "id BIGINT UNIQUE UNIQUE DEFERRABLE",
                [
                    ("t_id_key", "u", False, False, False),
                    ("t_id_key1", "u", True, False, False),
                ],
            ),
        )
        with postgres() as db:
            for definition, expected in cases:
                with self.subTest(definition=definition):
                    db.execute("CREATE TABLE public.t (" + definition + ")")
                    try:
                        self.assertEqual(
                            expected,
                            db.execute(
                                "SELECT c.conname, c.contype, c.condeferrable, c.condeferred, "
                                "i.indnullsnotdistinct FROM pg_constraint c "
                                "JOIN pg_index i ON i.indexrelid=c.conindid "
                                "WHERE c.conrelid='public.t'::regclass "
                                "AND c.contype IN ('p', 'u') ORDER BY c.conname"
                            ).fetchall(),
                        )
                        self.assertEqual(
                            len(expected),
                            db.execute(
                                "SELECT count(*) FROM pg_index WHERE indrelid='public.t'::regclass"
                            ).fetchone()[0],
                        )
                    finally:
                        db.execute("DROP TABLE public.t")

    def test_generated_unique_names_clip_balanced_utf8_and_composite_prefixes(self):
        cases = (
            ("a" * 63, ("b" * 63,), False, False, "a" * 29 + "_" + "b" * 29 + "_key"),
            ("a" * 63, ("b" * 63,), False, True, "a" * 29 + "_" + "b" * 28 + "_key1"),
            ("é" * 31, ("中" * 21,), False, False, "é" * 14 + "_" + "中" * 9 + "_key"),
            (
                "items",
                ("a" * 40, "b" * 40),
                False,
                False,
                "items_" + "a" * 40 + "_" + "b" * 12 + "_key",
            ),
            ("a" * 63, ("id",), True, False, "a" * 58 + "_pkey"),
        )
        with postgres() as db:
            db.execute("CREATE TABLE public.reserved (id BIGINT)")
            for table, columns, primary, collision, expected in cases:
                with self.subTest(table=table, primary=primary, collision=collision):
                    occupied = "a" * 29 + "_" + "b" * 29 + "_key"
                    if collision:
                        db.execute(
                            pgsql.SQL("CREATE INDEX {} ON public.reserved (id)").format(
                                pgsql.Identifier(occupied)
                            )
                        )
                    definitions = pgsql.SQL(", ").join(
                        pgsql.SQL("{} TEXT").format(pgsql.Identifier(column))
                        for column in columns
                    )
                    keys = pgsql.SQL(", ").join(map(pgsql.Identifier, columns))
                    db.execute(
                        pgsql.SQL("CREATE TABLE public.{} ({}, {} ({}))").format(
                            pgsql.Identifier(table),
                            definitions,
                            pgsql.SQL("PRIMARY KEY" if primary else "UNIQUE"),
                            keys,
                        )
                    )
                    self.assertEqual(
                        [(expected,)],
                        db.execute(
                            "SELECT c.conname FROM pg_constraint c "
                            "JOIN pg_class t ON t.oid=c.conrelid "
                            "JOIN pg_namespace n ON n.oid=t.relnamespace "
                            "WHERE n.nspname='public' AND t.relname=%s "
                            "AND c.contype=%s",
                            (table, "p" if primary else "u"),
                        ).fetchall(),
                    )
                    db.execute(
                        pgsql.SQL("DROP TABLE public.{}").format(
                            pgsql.Identifier(table)
                        )
                    )
                    if collision:
                        db.execute(
                            pgsql.SQL("DROP INDEX public.{}").format(
                                pgsql.Identifier(occupied)
                            )
                        )

    def test_generated_unique_constraint_names_are_table_scoped_and_collision_safe(
        self,
    ):
        with postgres() as db:
            for table in ("first", "second"):
                db.execute(
                    f"CREATE TABLE public.{table} "
                    "(id BIGINT PRIMARY KEY, email TEXT UNIQUE)"
                )
            self.assertEqual(
                [
                    ("first", "first_email_key", "u"),
                    ("first", "first_pkey", "p"),
                    ("second", "second_email_key", "u"),
                    ("second", "second_pkey", "p"),
                ],
                db.execute(
                    "SELECT t.relname, c.conname, c.contype "
                    "FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid "
                    "JOIN pg_namespace n ON n.oid=t.relnamespace "
                    "WHERE n.nspname='public' AND c.contype IN ('p', 'u') "
                    "ORDER BY t.relname, c.conname"
                ).fetchall(),
            )
            db.execute("CREATE INDEX third_pkey ON public.first (id)")
            db.execute("CREATE TABLE public.third (id BIGINT PRIMARY KEY)")
            self.assertEqual(
                [("third_pkey1",)],
                db.execute(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid='public.third'::regclass AND contype='p'"
                ).fetchall(),
            )
            # Explicit user names must not be silently renamed like generated
            # ones. Failed admission must leave no partially created table.
            with self.assertRaises(psycopg.Error) as explicit_collision:
                db.execute(
                    "CREATE TABLE public.fourth "
                    "(id BIGINT CONSTRAINT third_pkey1 PRIMARY KEY)"
                )
            self.assertEqual("42P07", explicit_collision.exception.sqlstate)
            self.assertIsNone(
                db.execute("SELECT to_regclass('public.fourth')").fetchone()[0]
            )
            db.execute(
                "CREATE TABLE public.fifth "
                "(id BIGINT CONSTRAINT sixth_pkey CHECK (id > 0))"
            )
            self.assertIsNone(
                db.execute("SELECT to_regclass('public.sixth_pkey')").fetchone()[0]
            )
            db.execute("CREATE TABLE public.sixth (id BIGINT PRIMARY KEY)")
            self.assertEqual(
                [("sixth_pkey1",)],
                db.execute(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid='public.sixth'::regclass AND contype='p'"
                ).fetchall(),
            )

    def test_internal_constraint_label_spellings_do_not_reserve_public_names(self):
        cases = (
            (
                "id BIGINT PRIMARY KEY, email TEXT CONSTRAINT sql_primary_0 UNIQUE",
                [("sql_primary_0", "u"), ("t_pkey", "p")],
            ),
            (
                (
                    "id BIGINT UNIQUE, CONSTRAINT sql_unique_0 CHECK (id > 0), "
                    "CONSTRAINT sql_generated_0 CHECK (id < 100)"
                ),
                [("sql_generated_0", "c"), ("sql_unique_0", "c"), ("t_id_key", "u")],
            ),
            (
                "id BIGINT CHECK (id > 0), CONSTRAINT sql_inline_0 UNIQUE (id)",
                [("sql_inline_0", "u"), ("t_id_check", "c")],
            ),
            (
                (
                    "id BIGINT REFERENCES parent (id), "
                    "CONSTRAINT sql_inline_0 CHECK (id > 0)"
                ),
                [("sql_inline_0", "c"), ("t_id_fkey", "f")],
            ),
        )
        with postgres() as db:
            db.execute("CREATE TABLE public.parent (id BIGINT PRIMARY KEY)")
            for definition, expected in cases:
                with self.subTest(definition=definition):
                    db.execute("CREATE TABLE public.t (" + definition + ")")
                    try:
                        # PostgreSQL 18+ also catalogs implicit NOT NULL rules.
                        # This fixture compares only the declared rule kinds.
                        self.assertEqual(
                            expected,
                            db.execute(
                                "SELECT conname, contype FROM pg_constraint "
                                "WHERE conrelid='public.t'::regclass "
                                "AND contype IN ('p', 'u', 'c', 'f') ORDER BY conname"
                            ).fetchall(),
                        )
                    finally:
                        db.execute("DROP TABLE public.t")
            with self.assertRaises(psycopg.Error) as duplicate:
                db.execute(
                    "CREATE TABLE public.t (id BIGINT UNIQUE, "
                    "CONSTRAINT repeated CHECK (id > 0), "
                    "CONSTRAINT repeated CHECK (id < 100))"
                )
            self.assertEqual("42710", duplicate.exception.sqlstate)
            self.assertIsNone(
                db.execute("SELECT to_regclass('public.t')").fetchone()[0]
            )

    def test_generated_check_and_fk_names_use_distinct_columns_and_ordered_keys(self):
        cases = (
            ("CHECK (true)", "items_check"),
            ("CHECK (id > 0)", "items_id_check"),
            ("CHECK (id + id > 0)", "items_id_check"),
            ("CHECK (id > tenant)", "items_check"),
            ("FOREIGN KEY (id) REFERENCES parent (id)", "items_id_fkey"),
            (
                "FOREIGN KEY (tenant, id) REFERENCES parent (tenant, id)",
                "items_tenant_id_fkey",
            ),
        )
        with postgres() as db:
            db.execute(
                "CREATE TABLE public.parent (tenant BIGINT, id BIGINT UNIQUE, "
                "PRIMARY KEY (tenant, id))"
            )
            # CHECK/FK default names consult constraint memberships, not the
            # relation namespace used by PRIMARY/UNIQUE index names.
            for label in (
                "items_check",
                "items_id_check",
                "items_id_fkey",
                "items_tenant_id_fkey",
            ):
                db.execute(
                    pgsql.SQL("CREATE TABLE public.{} (spare BIGINT)").format(
                        pgsql.Identifier(label)
                    )
                )
            for rule, expected in cases:
                with self.subTest(rule=rule):
                    db.execute(
                        "CREATE TABLE public.items (tenant BIGINT, id BIGINT, "
                        + rule
                        + ")"
                    )
                    try:
                        self.assertEqual(
                            [(expected,)],
                            db.execute(
                                "SELECT conname FROM pg_constraint "
                                "WHERE conrelid='public.items'::regclass "
                                "AND contype IN ('c', 'f')"
                            ).fetchall(),
                        )
                    finally:
                        db.execute("DROP TABLE public.items")
            db.execute(
                "CREATE TABLE public.reservations (id BIGINT, "
                "CONSTRAINT items_id_check CHECK (id > 0), "
                "CONSTRAINT items_id_fkey CHECK (id < 100))"
            )
            for rule, expected in (
                ("CHECK (id > 0)", "items_id_check1"),
                ("FOREIGN KEY (id) REFERENCES parent (id)", "items_id_fkey1"),
            ):
                with self.subTest(membership_collision=rule):
                    db.execute("CREATE TABLE public.items (id BIGINT, " + rule + ")")
                    try:
                        self.assertEqual(
                            [(expected,)],
                            db.execute(
                                "SELECT conname FROM pg_constraint "
                                "WHERE conrelid='public.items'::regclass AND contype IN ('c', 'f')"
                            ).fetchall(),
                        )
                    finally:
                        db.execute("DROP TABLE public.items")

    def test_same_statement_generated_names_follow_ddl_phases_not_global_prereservation(
        self,
    ):
        rejected = (
            ("id BIGINT PRIMARY KEY, x BIGINT CONSTRAINT t_pkey UNIQUE", "42P07"),
            ("x BIGINT CONSTRAINT t_pkey UNIQUE, id BIGINT PRIMARY KEY", "42P07"),
            (
                "id BIGINT CHECK (id > 0), CONSTRAINT t_id_check CHECK (id < 100)",
                "42710",
            ),
        )
        accepted = (
            (
                "id BIGINT CONSTRAINT t_id_check CHECK (id < 100), CHECK (id > 0)",
                [("t_id_check", "c"), ("t_id_check1", "c")],
            ),
            (
                "id BIGINT UNIQUE, CONSTRAINT t_id_key CHECK (id > 0)",
                [("t_id_key", "c"), ("t_id_key1", "u")],
            ),
            (
                "id BIGINT CONSTRAINT t_id_key CHECK (id > 0), UNIQUE (id)",
                [("t_id_key", "c"), ("t_id_key1", "u")],
            ),
        )
        with postgres() as db:
            for definition, state in rejected:
                with self.subTest(rejected=definition):
                    with self.assertRaises(psycopg.Error) as failure:
                        db.execute("CREATE TABLE public.t (" + definition + ")")
                    self.assertEqual(state, failure.exception.sqlstate)
                    self.assertIsNone(
                        db.execute("SELECT to_regclass('public.t')").fetchone()[0]
                    )
            for definition, expected in accepted:
                with self.subTest(accepted=definition):
                    db.execute("CREATE TABLE public.t (" + definition + ")")
                    try:
                        self.assertEqual(
                            expected,
                            db.execute(
                                "SELECT conname, contype FROM pg_constraint "
                                "WHERE conrelid='public.t'::regclass "
                                "AND contype IN ('p', 'u', 'c', 'f') ORDER BY conname"
                            ).fetchall(),
                        )
                    finally:
                        db.execute("DROP TABLE public.t")

    def test_alter_table_pins_first_search_path_relation_and_qualified_names_bypass_it(
        self,
    ):
        with postgres() as db:
            db.execute("CREATE SCHEMA tenant")
            db.execute("CREATE TABLE tenant.items (id BIGINT)")
            db.execute("CREATE TABLE public.index_owner (id BIGINT)")
            db.execute("INSERT INTO tenant.items VALUES (1)")
            db.execute("SET search_path TO public, tenant")
            db.execute("ALTER TABLE items ADD COLUMN label TEXT")
            self.assertEqual(
                [("id",), ("label",)],
                db.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='tenant' AND table_name='items' "
                    "ORDER BY ordinal_position"
                ).fetchall(),
            )
            db.execute("CREATE INDEX items ON public.index_owner (id)")
            with self.assertRaises(psycopg.Error) as shadow:
                db.execute("ALTER TABLE items ADD COLUMN blocked TEXT")
            self.assertEqual("42809", shadow.exception.sqlstate)
            self.assertEqual(
                [(1, None)], db.execute("SELECT * FROM tenant.items").fetchall()
            )
            db.execute("SELECT set_config('search_path', '', false)")
            with self.assertRaises(psycopg.Error) as absent:
                db.execute("ALTER TABLE items ADD COLUMN missing TEXT")
            self.assertEqual("42P01", absent.exception.sqlstate)
            db.execute("ALTER TABLE tenant.items ADD COLUMN qualified TEXT")
            self.assertEqual(
                [(1, None, None)], db.execute("SELECT * FROM tenant.items").fetchall()
            )

    def test_original_null_policy_ddl_executes_unchanged(self):
        original = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        with postgres() as db:
            for case_id in ("sql-0692", "sql-0706", "sql-0731"):
                with self.subTest(case_id=case_id):
                    case = original[case_id]
                    self.assertEqual([], case["params"])
                    db.execute("DROP TABLE IF EXISTS public.usage_records CASCADE")
                    db.execute(
                        "CREATE TABLE public.usage_records "
                        "(id BIGINT PRIMARY KEY, email TEXT)"
                    )
                    result = db.execute(case["sql"])
                    constraint = case_id == "sql-0731"
                    self.assertEqual(
                        "ALTER TABLE" if constraint else "CREATE INDEX",
                        result.statusmessage,
                    )
                    self.assertEqual(
                        [(True, case_id == "sql-0692")],
                        db.execute(
                            "SELECT i.indisunique, i.indnullsnotdistinct FROM pg_index i "
                            "WHERE i.indrelid='public.usage_records'::regclass "
                            "AND NOT i.indisprimary"
                        ).fetchall(),
                    )
                    if constraint:
                        self.assertEqual(
                            [(False, False)],
                            db.execute(
                                "SELECT condeferrable, condeferred FROM pg_constraint "
                                "WHERE conrelid='public.usage_records'::regclass "
                                "AND contype='u'"
                            ).fetchall(),
                        )
                    db.execute("INSERT INTO public.usage_records VALUES (1, NULL)")
                    if case_id == "sql-0692":
                        with self.assertRaises(psycopg.Error) as duplicate_null:
                            db.execute(
                                "INSERT INTO public.usage_records VALUES (2, NULL)"
                            )
                        self.assertEqual("23505", duplicate_null.exception.sqlstate)
                        expected = [(1, None)]
                    else:
                        db.execute("INSERT INTO public.usage_records VALUES (2, NULL)")
                        expected = [(1, None), (2, None)]
                    self.assertEqual(
                        expected,
                        db.execute(
                            "SELECT id, email FROM public.usage_records ORDER BY id"
                        ).fetchall(),
                    )
                    db.execute("INSERT INTO public.usage_records VALUES (3, 'known')")
                    expected.append((3, "known"))
                    with self.assertRaises(psycopg.Error) as duplicate_value:
                        db.execute(
                            "INSERT INTO public.usage_records VALUES (4, 'known')"
                        )
                    self.assertEqual("23505", duplicate_value.exception.sqlstate)
                    self.assertEqual(
                        expected,
                        db.execute(
                            "SELECT id, email FROM public.usage_records ORDER BY id"
                        ).fetchall(),
                    )

    def test_alter_table_rejects_index_relation_kinds_without_changing_storage(self):
        with postgres() as db:
            db.execute(
                "CREATE TABLE public.items (id BIGINT PRIMARY KEY, email TEXT UNIQUE)"
            )
            db.execute("CREATE INDEX email_lookup ON public.items (email)")
            db.execute("INSERT INTO public.items VALUES (1, NULL), (2, 'known')")
            indexes = db.execute(
                "SELECT indexrelid, indisunique, indisprimary "
                "FROM pg_index WHERE indrelid='public.items'::regclass "
                "ORDER BY indexrelid"
            ).fetchall()
            for relation in ("email_lookup", "items_email_key", "items_pkey"):
                with self.subTest(relation=relation):
                    with self.assertRaises(psycopg.Error) as wrong_kind:
                        db.execute(
                            f"ALTER TABLE public.{relation} ADD COLUMN spare TEXT"
                        )
                    self.assertEqual("42809", wrong_kind.exception.sqlstate)
                    self.assertEqual(
                        [(1, None), (2, "known")],
                        db.execute(
                            "SELECT id, email FROM public.items ORDER BY id"
                        ).fetchall(),
                    )
                    self.assertEqual(
                        indexes,
                        db.execute(
                            "SELECT indexrelid, indisunique, indisprimary "
                            "FROM pg_index WHERE indrelid='public.items'::regclass "
                            "ORDER BY indexrelid"
                        ).fetchall(),
                    )

    def test_unique_null_semantics_share_constraint_and_index_enforcement(self):
        cases = (
            ("email TEXT UNIQUE NULLS NOT DISTINCT", None, True),
            ("email TEXT CONSTRAINT email_key UNIQUE NULLS NOT DISTINCT", None, True),
            (
                "email TEXT, CONSTRAINT email_key UNIQUE NULLS NOT DISTINCT (tenant, email)",
                None,
                True,
            ),
            (
                "email TEXT",
                "ALTER TABLE public.items ADD CONSTRAINT email_key UNIQUE NULLS NOT DISTINCT (tenant, email)",
                True,
            ),
            (
                "email TEXT",
                "CREATE UNIQUE INDEX email_key ON public.items (tenant, email) INCLUDE (id) NULLS NOT DISTINCT",
                True,
            ),
            (
                "email TEXT",
                "CREATE UNIQUE INDEX email_key ON public.items (lower(email)) NULLS NOT DISTINCT WHERE status='active'",
                True,
            ),
            (
                "email TEXT",
                "CREATE UNIQUE INDEX email_key ON public.items (email) NULLS DISTINCT",
                False,
            ),
            ("email TEXT UNIQUE NULLS DISTINCT", None, False),
        )
        with postgres() as db:
            for declaration, index, not_distinct in cases:
                with self.subTest(declaration=declaration, index=index):
                    db.execute("DROP TABLE IF EXISTS public.items CASCADE")
                    db.execute(
                        "CREATE TABLE public.items (id BIGINT, tenant BIGINT, status TEXT, "
                        + declaration
                        + ")"
                    )
                    if index is not None:
                        db.execute(index)
                    db.execute("INSERT INTO public.items VALUES (1, 7, 'active', NULL)")
                    duplicate = "INSERT INTO public.items VALUES (2, 7, 'active', NULL)"
                    if not_distinct:
                        with self.assertRaises(psycopg.Error) as conflict:
                            db.execute(duplicate)
                        self.assertEqual("23505", conflict.exception.sqlstate)
                    else:
                        self.assertEqual(1, db.execute(duplicate).rowcount)
                    expected = [1] if not_distinct else [1, 2]
                    self.assertEqual(
                        expected,
                        [
                            row[0]
                            for row in db.execute(
                                "SELECT id FROM public.items ORDER BY id"
                            )
                        ],
                    )
                    db.execute(
                        "INSERT INTO public.items VALUES (3, 7, 'active', 'same')"
                    )
                    with self.assertRaises(psycopg.Error) as conflict:
                        db.execute(
                            "INSERT INTO public.items VALUES (4, 7, 'active', 'same')"
                        )
                    self.assertEqual("23505", conflict.exception.sqlstate)
                    expected.append(3)
                    if index is not None and "WHERE" in index:
                        self.assertEqual(
                            2,
                            db.execute(
                                "INSERT INTO public.items VALUES "
                                "(5, 7, 'inactive', NULL), (6, 7, 'inactive', NULL)"
                            ).rowcount,
                        )
                        expected.extend((5, 6))
                    if "tenant, email" in declaration or (
                        index is not None and "tenant, email" in index
                    ):
                        db.execute(
                            "INSERT INTO public.items VALUES (7, 8, 'active', NULL)"
                        )
                        db.execute(
                            "INSERT INTO public.items VALUES (8, NULL, 'active', NULL)"
                        )
                        with self.assertRaises(psycopg.Error) as conflict:
                            db.execute(
                                "INSERT INTO public.items VALUES (9, NULL, 'active', NULL)"
                            )
                        self.assertEqual("23505", conflict.exception.sqlstate)
                        expected.extend((7, 8))
                    self.assertEqual(
                        expected,
                        [
                            row[0]
                            for row in db.execute(
                                "SELECT id FROM public.items ORDER BY id"
                            )
                        ],
                    )
            db.execute("DROP TABLE public.items")
            db.execute("CREATE TABLE public.items(email TEXT)")
            # PostgreSQL accepts the clause on a nonunique index but does not
            # turn the index into a uniqueness constraint.
            db.execute(
                "CREATE INDEX email_lookup ON public.items(email) NULLS NOT DISTINCT"
            )
            self.assertEqual(
                2, db.execute("INSERT INTO public.items VALUES(NULL), (NULL)").rowcount
            )

    def test_unique_null_clause_rejects_malformed_or_misplaced_forms(self):
        with postgres() as db:
            db.execute("CREATE TABLE public.items(email TEXT, id BIGINT)")
            for sql in (
                "CREATE UNIQUE INDEX bad ON public.items (email) NULLS NOT",
                "CREATE UNIQUE INDEX bad ON public.items (email) NULLS FIRST",
                "CREATE UNIQUE INDEX bad ON public.items (email) NULLS DISTINCT NULLS DISTINCT",
                "CREATE UNIQUE INDEX bad ON public.items (email) NULLS NOT DISTINCT INCLUDE (id)",
                "CREATE TABLE public.bad (email TEXT PRIMARY KEY NULLS NOT DISTINCT)",
                "ALTER TABLE public.items ADD CONSTRAINT bad UNIQUE (email) NULLS NOT DISTINCT",
            ):
                with self.subTest(sql=sql), self.assertRaises(psycopg.Error) as failure:
                    db.execute(sql)
                self.assertEqual("42601", failure.exception.sqlstate)

    def test_empty_search_path_preserves_conditional_absence(self):
        with postgres() as db:
            db.execute("CREATE TABLE public.items(id bigint)")
            db.execute("CREATE INDEX items_id ON public.items(id)")
            db.execute("SELECT set_config('search_path', '', false)")
            self.assertEqual(
                "DROP INDEX", db.execute("DROP INDEX IF EXISTS items_id").statusmessage
            )
            self.assertTrue(
                db.execute(
                    "SELECT to_regclass('public.items_id') IS NOT NULL"
                ).fetchone()[0]
            )
            with self.assertRaises(psycopg.Error) as missing:
                db.execute("DROP INDEX items_id")
            self.assertEqual("42704", missing.exception.sqlstate)
            with self.assertRaises(psycopg.Error) as create:
                db.execute("CREATE INDEX other_id ON items(id)")
            self.assertEqual("42P01", create.exception.sqlstate)
            self.assertEqual(
                "DROP INDEX", db.execute("DROP INDEX public.items_id").statusmessage
            )
            self.assertIsNone(
                db.execute("SELECT to_regclass('public.items_id')").fetchone()[0]
            )

    def test_original_unique_index_build_failure_is_atomic(self):
        originals = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        with postgres() as db:
            for case_id in ("sql-0692", "sql-0706", "sql-0705"):
                with self.subTest(case_id=case_id):
                    db.execute(
                        "CREATE TABLE usage_records "
                        "(id UUID PRIMARY KEY, email TEXT, tenant_id TEXT, status TEXT)"
                    )
                    email = None if case_id == "sql-0692" else "owner@example.com"
                    for ordinal in (1, 2):
                        db.execute(
                            "INSERT INTO usage_records VALUES (%s,%s,%s,%s)",
                            (
                                f"00000000-0000-0000-0000-{ordinal:012d}",
                                email,
                                f"tenant-{ordinal}",
                                "pending",
                            ),
                        )
                    row_query = (
                        "SELECT id::text,email,tenant_id,status "
                        "FROM usage_records ORDER BY id"
                    )
                    index_query = (
                        "SELECT c.relname,i.indisvalid,i.indisready "
                        "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                        "WHERE i.indrelid='usage_records'::regclass ORDER BY c.relname"
                    )
                    before_rows = db.execute(row_query).fetchall()
                    before_indexes = db.execute(index_query).fetchall()
                    with self.assertRaises(psycopg.errors.UniqueViolation) as failed:
                        db.execute(originals[case_id]["sql"])
                    self.assertEqual("23505", failed.exception.sqlstate)
                    self.assertEqual(before_rows, db.execute(row_query).fetchall())
                    self.assertEqual(before_indexes, db.execute(index_query).fetchall())
                    # The rejected declaration neither reserves its name nor
                    # poisons later writes. A deliberate repaired retry works.
                    db.execute(
                        "DELETE FROM usage_records "
                        "WHERE id='00000000-0000-0000-0000-000000000002'"
                    )
                    db.execute(originals[case_id]["sql"])
                    self.assertEqual(
                        len(before_indexes) + 1, len(db.execute(index_query).fetchall())
                    )
                    db.execute("DROP TABLE usage_records")

    def test_original_unique_index_owner_campaign(self):
        originals = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        columns = "id::text, email, tenant_id, status"
        seed = [
            ("00000000-0000-0000-0000-000000000001", None, "tenant", "pending"),
            (
                "00000000-0000-0000-0000-000000000002",
                "owner@example.com",
                "tenant",
                "pending",
            ),
        ]
        with postgres() as db:
            for case_id, namespace, nulls_not_distinct in (
                ("sql-0692", "nulls_not_distinct", True),
                ("sql-0706", "nulls_distinct", False),
                ("sql-0705", "included_columns", False),
            ):
                with self.subTest(case_id=case_id):
                    db.execute(
                        pgsql.SQL("CREATE SCHEMA {}").format(
                            pgsql.Identifier(namespace)
                        )
                    )
                    db.execute(
                        pgsql.SQL("SET search_path TO {}").format(
                            pgsql.Identifier(namespace)
                        )
                    )
                    db.execute(
                        "CREATE TABLE usage_records "
                        "(id UUID PRIMARY KEY, email TEXT, tenant_id TEXT, status TEXT)"
                    )
                    for row in seed:
                        db.execute(
                            "INSERT INTO usage_records VALUES (%s,%s,%s,%s)", row
                        )
                    original = originals[case_id]
                    self.assertEqual([], original["params"])
                    db.execute(original["sql"])
                    with self.assertRaises(psycopg.errors.UniqueViolation) as duplicate:
                        db.execute(
                            "INSERT INTO usage_records VALUES "
                            "('00000000-0000-0000-0000-000000000004',"
                            "'owner@example.com','other','changed')"
                        )
                    self.assertEqual("23505", duplicate.exception.sqlstate)
                    self.assertEqual(
                        seed,
                        db.execute(
                            f"SELECT {columns} FROM usage_records ORDER BY id"
                        ).fetchall(),
                    )
                    second_null = (
                        "00000000-0000-0000-0000-000000000003",
                        None,
                        "other",
                        "changed",
                    )
                    if nulls_not_distinct:
                        with self.assertRaises(psycopg.errors.UniqueViolation) as null:
                            db.execute(
                                "INSERT INTO usage_records VALUES (%s,%s,%s,%s)",
                                second_null,
                            )
                        self.assertEqual("23505", null.exception.sqlstate)
                    else:
                        db.execute(
                            "INSERT INTO usage_records VALUES (%s,%s,%s,%s)",
                            second_null,
                        )
                    self.assertEqual(
                        seed if nulls_not_distinct else seed + [second_null],
                        db.execute(
                            f"SELECT {columns} FROM usage_records ORDER BY id"
                        ).fetchall(),
                    )
                    if not nulls_not_distinct:
                        further_null = (
                            "00000000-0000-0000-0000-000000000004",
                            None,
                            "other",
                            "changed",
                        )
                        db.execute(
                            "INSERT INTO usage_records VALUES (%s,%s,%s,%s)",
                            further_null,
                        )
                        self.assertEqual(
                            seed + [second_null, further_null],
                            db.execute(
                                f"SELECT {columns} FROM usage_records ORDER BY id"
                            ).fetchall(),
                        )


if __name__ == "__main__":
    unittest.main()
