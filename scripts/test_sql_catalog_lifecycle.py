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

import unittest

import psycopg
from generate_sql_postgres_reference import postgres


class IndexLifecycleTest(unittest.TestCase):
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
