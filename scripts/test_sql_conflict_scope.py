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

"""Original conflict-scope contracts against an isolated PostgreSQL 18+ oracle."""

import json
import unittest

import psycopg
from generate_sql_postgres_reference import FIXTURES, postgres

AMBIGUOUS = (
    "sql-1388", "sql-1389", "sql-1391", "sql-1438",
    "sql-1444", "sql-1478", "sql-1479",
)


class ConflictScopeTest(unittest.TestCase):
    def test_original_conflict_references_reject_before_row_selection(self):
        originals = {
            item["id"]: item
            for item in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        with postgres() as db:
            db.execute(
                "CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT DEFAULT 'initial',"
                "quantity BIGINT,amount BIGINT,enabled BOOLEAN)"
            )
            for seeded in (False, True):
                if seeded:
                    db.execute(
                        "INSERT INTO usage_records VALUES('u1','ready',1,5,true)"
                    )
                before = db.execute(
                    "SELECT * FROM usage_records ORDER BY id"
                ).fetchall()
                for case_id in AMBIGUOUS:
                    with self.subTest(case=case_id, seeded=seeded):
                        case = originals[case_id]
                        self.assertEqual([], case["params"])
                        with self.assertRaises(psycopg.Error) as caught:
                            with db.transaction():
                                with psycopg.RawCursor(db) as cursor:
                                    cursor.execute(case["sql"])
                        self.assertEqual("42702", caught.exception.sqlstate)
                        self.assertEqual(
                            before,
                            db.execute("SELECT * FROM usage_records ORDER BY id").fetchall(),
                        )
            valid = originals["sql-1480"]
            self.assertEqual([], valid["params"])
            with psycopg.RawCursor(db) as cursor:
                cursor.execute(valid["sql"])
                self.assertEqual("INSERT 0 1", cursor.statusmessage)
                self.assertEqual([("u1", "ready")], cursor.fetchall())
            result = db.execute(
                "INSERT INTO usage_records AS u(id,status,quantity) "
                "VALUES('u1','open',2) ON CONFLICT(id) DO UPDATE SET "
                "quantity=u.quantity+excluded.quantity RETURNING id,status,quantity"
            )
            self.assertEqual([("u1", "ready", 3)], result.fetchall())
            result = db.execute(
                "INSERT INTO usage_records AS u(id,status,quantity) "
                "VALUES('u1','open',2) ON CONFLICT(id) DO UPDATE SET "
                "(status,quantity)=ROW(DEFAULT,u.quantity+excluded.quantity) "
                "RETURNING id,status,quantity"
            )
            self.assertEqual([("u1", "initial", 5)], result.fetchall())
            self.assertEqual(
                [("u1", "initial", 5, 5, True)],
                db.execute("SELECT * FROM usage_records").fetchall(),
            )
            before = db.execute("SELECT *,xmin::text FROM usage_records").fetchall()
            with psycopg.RawCursor(db) as cursor:
                cursor.execute(valid["sql"])
                self.assertEqual("INSERT 0 0", cursor.statusmessage)
                self.assertEqual([], cursor.fetchall())
            self.assertEqual(before, db.execute("SELECT *,xmin::text FROM usage_records").fetchall())
