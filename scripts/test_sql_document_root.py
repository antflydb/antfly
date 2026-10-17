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

"""Independent PostgreSQL JSONB oracle for the SQL document-root boundary."""

import json
import unittest

from generate_sql_postgres_reference import FIXTURES, postgres


class DocumentRootTest(unittest.TestCase):
    def test_original_document_root_insertions(self):
        originals = {
            entry["id"]: entry
            for entry in json.loads(
                (FIXTURES / "sql_parity_inventory.json").read_text()
            )["entries"]
        }
        with postgres() as db:
            db.execute("CREATE TABLE docs (_id TEXT PRIMARY KEY, _doc JSONB NOT NULL)")
            for case in ("sql-0881", "sql-0882"):
                with self.subTest(case=case):
                    db.execute("TRUNCATE docs")
                    original = originals[case]
                    self.assertEqual([], original["params"])
                    result = db.execute(original["sql"])
                    self.assertEqual(1, result.rowcount)
                    if case == "sql-0882":
                        self.assertEqual([("doc:a",)], result.fetchall())
                    self.assertEqual(
                        [("doc:a", {})],
                        db.execute("SELECT _id,_doc FROM docs ORDER BY _id").fetchall(),
                    )

    def test_root_source_and_replacement_postimages(self):
        with postgres() as db:
            db.execute("CREATE TABLE docs (_id TEXT PRIMARY KEY, _doc JSONB NOT NULL)")
            db.execute(
                """INSERT INTO docs VALUES ('doc:a',
                '{"title":"before","amount":1,
                  "unknown":{"n":9007199254740993.1250}}')"""
            )
            for statement in (
                "SELECT _doc FROM docs WHERE _id='doc:a'",
                "WITH c AS (SELECT _doc FROM docs) SELECT _doc FROM c",
                "SELECT d._doc FROM docs d CROSS JOIN (VALUES(1)) n(v)",
            ):
                with self.subTest(statement=statement):
                    cursor = db.execute(statement)
                    self.assertEqual(1, cursor.rowcount)
                    self.assertEqual("before", cursor.fetchone()[0]["title"])
            db.execute(
                "INSERT INTO docs (_id,_doc) SELECT 'doc:b',_doc "
                "FROM docs WHERE _id='doc:a' RETURNING _doc"
            )
            updated = db.execute(
                """UPDATE docs SET _doc=jsonb_set(_doc,'{title}','"after"',true)
                WHERE _id='doc:a' RETURNING _doc"""
            ).fetchone()[0]
            self.assertEqual("after", updated["title"])
            self.assertEqual(
                [("doc:a", "after"), ("doc:b", "before")],
                db.execute(
                    "SELECT _id,_doc->>'title' FROM docs ORDER BY _id"
                ).fetchall(),
            )
            self.assertEqual(
                [("9007199254740993.1250",)] * 2,
                db.execute(
                    "SELECT _doc#>>'{unknown,n}' FROM docs ORDER BY _id"
                ).fetchall(),
            )


    def test_joined_and_merge_root_literals_replace_complete_postimages(self):
        with postgres() as db:
            db.execute("CREATE TABLE docs (_id TEXT PRIMARY KEY, _doc JSONB NOT NULL)")
            db.execute(
                "INSERT INTO docs VALUES ('doc:a',"
                "'{\"title\":\"before\",\"unknown\":{\"keep\":true}}')"
            )
            for statement, expected in (
                (
                    "UPDATE docs d SET _doc='{\"title\":\"joined\"}' "
                    "FROM (VALUES('doc:a')) s(id) WHERE d._id=s.id RETURNING d._doc",
                    {"title": "joined"},
                ),
                (
                    "MERGE INTO docs d USING (VALUES('doc:a')) s(id) "
                    "ON d._id=s.id WHEN MATCHED THEN UPDATE "
                    "SET _doc='{\"title\":\"merged\"}' RETURNING d._doc",
                    {"title": "merged"},
                ),
            ):
                with self.subTest(statement=statement):
                    self.assertEqual([(expected,)], db.execute(statement).fetchall())
                    self.assertEqual(
                        [("doc:a", expected)],
                        db.execute("SELECT _id,_doc FROM docs").fetchall(),
                    )
            self.assertEqual(
                [({"title": "inserted"},)],
                db.execute(
                    "MERGE INTO docs d USING (VALUES('doc:b')) s(id) "
                    "ON d._id=s.id WHEN NOT MATCHED THEN INSERT (_id,_doc) "
                    "VALUES(s.id,'{\"title\":\"inserted\"}') RETURNING _doc"
                ).fetchall(),
            )
            self.assertEqual(
                [({"title": "inserted"},)],
                db.execute(
                    "MERGE INTO docs d USING (VALUES('doc:b')) s(id) "
                    "ON d._id=s.id WHEN MATCHED THEN DELETE RETURNING d._doc"
                ).fetchall(),
            )
            old = {"title": "before", "unknown": {"keep": True}}
            db.execute(
                "UPDATE docs SET _doc='{\"title\":\"before\","
                "\"unknown\":{\"keep\":true}}'"
            )
            self.assertEqual(
                [({"title": "merged"}, old)],
                db.execute(
                    "MERGE INTO docs d USING docs s ON d._id=s._id "
                    "WHEN MATCHED THEN UPDATE SET _doc='{\"title\":\"merged\"}' "
                    "RETURNING d._doc,s._doc"
                ).fetchall(),
            )


if __name__ == "__main__":
    unittest.main()
