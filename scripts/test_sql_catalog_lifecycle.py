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


if __name__ == "__main__":
    unittest.main()
