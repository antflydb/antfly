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


class IndexLifecycleTest(unittest.TestCase):
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
                    db.execute("CREATE TABLE public.usage_records(email TEXT)")
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
                            "WHERE i.indrelid='public.usage_records'::regclass"
                        ).fetchall(),
                    )
                    if constraint:
                        self.assertEqual(
                            [(False, False)],
                            db.execute(
                                "SELECT condeferrable, condeferred FROM pg_constraint "
                                "WHERE conrelid='public.usage_records'::regclass"
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
