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

"""Exact-source PostgreSQL oracle for default DDL and future mutation semantics."""

import json
import unittest

import psycopg
from generate_sql_postgres_reference import FIXTURES, postgres


class DefaultLifecycleTest(unittest.TestCase):
    def test_original_defaults_preserve_old_rows_nulls_scale_and_future_writes(self):
        original = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        with postgres() as db:
            db.execute(
                "CREATE TABLE usage_records (id TEXT, status TEXT DEFAULT 'initial', "
                "amount NUMERIC DEFAULT 2.50)"
            )
            db.execute("INSERT INTO usage_records(id) VALUES('before')")
            for case_id in ("sql-0741", "sql-0742", "sql-0743"):
                self.assertEqual([], original[case_id]["params"])
                self.assertEqual(
                    "ALTER TABLE", db.execute(original[case_id]["sql"]).statusmessage
                )
                # Neither setting nor dropping a default rewrites old rows.
                self.assertEqual(
                    [("before", "initial", "2.50")],
                    db.execute(
                        "SELECT id,status,amount::text FROM usage_records WHERE id='before'"
                    ).fetchall(),
                )
                if case_id == "sql-0741":
                    db.execute("INSERT INTO usage_records(id) VALUES('after_status')")
                    db.execute(
                        "INSERT INTO usage_records(id,status,amount) "
                        "VALUES('explicit',NULL,NULL),('default',DEFAULT,DEFAULT)"
                    )
                elif case_id == "sql-0742":
                    db.execute("INSERT INTO usage_records(id) VALUES('after_amount')")
                else:
                    db.execute("INSERT INTO usage_records(id) VALUES('after_drop')")
            expected = [
                ("after_amount", "pending", "7"),
                ("after_drop", None, "7"),
                ("after_status", "pending", "2.50"),
                ("before", "initial", "2.50"),
                ("default", "pending", "2.50"),
                ("explicit", None, None),
            ]
            self.assertEqual(
                expected,
                db.execute(
                    "SELECT id,status,amount::text FROM usage_records ORDER BY id"
                ).fetchall(),
            )
            with self.assertRaises(psycopg.Error) as failure:
                db.execute(
                    "ALTER TABLE usage_records ALTER COLUMN absent SET DEFAULT 'bad'"
                )
            self.assertEqual("42703", failure.exception.sqlstate)
            db.execute("INSERT INTO usage_records(id) VALUES('after_restart')")
            db.execute(
                "UPDATE usage_records SET status=DEFAULT,amount=DEFAULT WHERE id='before'"
            )
            expected.insert(2, ("after_restart", None, "7"))
            expected[4] = ("before", None, "7")
            self.assertEqual(
                expected,
                db.execute(
                    "SELECT id,status,amount::text FROM usage_records ORDER BY id"
                ).fetchall(),
            )
