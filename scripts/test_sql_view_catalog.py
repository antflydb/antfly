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

"""PostgreSQL oracle for stored-view dependency and delegation contracts.

Every test owns an isolated PostgreSQL cluster; roles and data never touch an
external database. This does not qualify Antfly stored-view execution by itself.
"""

import unittest

import psycopg
from generate_sql_postgres_reference import postgres


def attributes(db, relation):
    return db.execute(
        "SELECT attname,attnum,atttypid,atttypmod FROM pg_attribute "
        "WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        (relation,),
    ).fetchall()


class ViewCatalogTest(unittest.TestCase):
    def test_unused_ctes_and_discarded_inner_outputs_still_own_dependencies(self):
        with postgres() as db:
            db.execute("CREATE TABLE rows(id BIGINT,status TEXT)")
            for query in (
                "WITH unused AS (SELECT status FROM rows) SELECT 42 AS answer FROM rows",
                "WITH used AS (SELECT id,status FROM rows) SELECT id FROM used",
            ):
                db.execute("CREATE VIEW stored AS " + query)
                with self.assertRaises(psycopg.Error) as dependency:
                    db.execute("ALTER TABLE rows DROP COLUMN status")
                self.assertEqual("2BP01", dependency.exception.sqlstate)
                db.execute("DROP VIEW stored")

    def test_dependencies_precede_constant_folding_and_star_is_a_captured_shape(self):
        with postgres() as db:
            db.execute("CREATE TABLE rows(id BIGINT,status TEXT)")
            db.execute("INSERT INTO rows VALUES(1,'ready')")
            db.execute(
                "CREATE VIEW unreachable AS "
                "SELECT CASE WHEN false THEN status ELSE 'constant' END AS value FROM rows"
            )
            self.assertEqual(
                [("constant",)], db.execute("SELECT * FROM unreachable").fetchall()
            )
            with self.assertRaises(psycopg.Error) as folded:
                db.execute("ALTER TABLE rows DROP COLUMN status")
            self.assertEqual("2BP01", folded.exception.sqlstate)
            db.execute("DROP VIEW unreachable")
            db.execute("CREATE VIEW captured_star AS SELECT * FROM rows")
            db.execute("CREATE VIEW counted AS SELECT count(*) AS n FROM rows")
            before = attributes(db, "captured_star")
            db.execute("ALTER TABLE rows ADD COLUMN unrelated TEXT")
            self.assertEqual(before, attributes(db, "captured_star"))
            db.execute("ALTER TABLE rows DROP COLUMN unrelated")
            with self.assertRaises(psycopg.Error) as captured:
                db.execute("ALTER TABLE rows DROP COLUMN status")
            self.assertEqual("2BP01", captured.exception.sqlstate)
            db.execute("DROP VIEW captured_star")
            db.execute("ALTER TABLE rows DROP COLUMN status")
            self.assertEqual([(1,)], db.execute("SELECT * FROM counted").fetchall())

    def test_rename_retains_attributes_but_recreated_names_do_not(self):
        with postgres() as db:
            db.execute("CREATE TABLE rows(id BIGINT,status TEXT)")
            db.execute("INSERT INTO rows VALUES(1,'ready')")
            db.execute("CREATE VIEW stored AS SELECT status FROM rows")
            original = dict((row[0], row[1:]) for row in attributes(db, "rows"))
            db.execute("ALTER TABLE rows ADD COLUMN note TEXT")
            db.execute("ALTER TABLE rows RENAME COLUMN status TO state")
            renamed = dict((row[0], row[1:]) for row in attributes(db, "rows"))
            self.assertEqual(original["status"], renamed["state"])
            self.assertEqual(
                [("ready",)], db.execute("SELECT * FROM stored").fetchall()
            )
            self.assertEqual(["status"], [row[0] for row in attributes(db, "stored")])
            with self.assertRaises(psycopg.Error) as dependent:
                db.execute("ALTER TABLE rows DROP COLUMN state")
            self.assertEqual("2BP01", dependent.exception.sqlstate)
            self.assertEqual(
                [("ready",)], db.execute("SELECT * FROM stored").fetchall()
            )
            db.execute("DROP VIEW stored")
            db.execute("ALTER TABLE rows DROP COLUMN state")
            db.execute("ALTER TABLE rows ADD COLUMN status TEXT")
            recreated = dict((row[0], row[1:]) for row in attributes(db, "rows"))
            self.assertGreater(recreated["status"][0], original["status"][0])
            self.assertEqual(original["id"], recreated["id"])
            self.assertEqual(
                [(None,)], db.execute("SELECT status FROM rows").fetchall()
            )

    def test_exact_domains_allow_unrelated_columns_defaults_and_nullability(self):
        with postgres() as db:
            db.execute(
                "CREATE TABLE rows(status TEXT,amount NUMERIC(20,4),tags TEXT[])"
            )
            db.execute("INSERT INTO rows VALUES('ready',1.2500,ARRAY['one',NULL])")
            db.execute("CREATE VIEW stored AS SELECT status,amount,tags FROM rows")
            before = attributes(db, "stored")
            rows = db.execute("SELECT * FROM stored").fetchall()
            self.assertEqual([25, 1700, 1009], [row[2] for row in before])
            db.execute("ALTER TABLE rows ADD COLUMN unrelated BIGINT")
            db.execute("ALTER TABLE rows ALTER COLUMN status SET DEFAULT 'pending'")
            db.execute("ALTER TABLE rows ALTER COLUMN status SET NOT NULL")
            db.execute("ALTER TABLE rows ALTER COLUMN status DROP NOT NULL")
            self.assertEqual(before, attributes(db, "stored"))
            self.assertEqual(rows, db.execute("SELECT * FROM stored").fetchall())
            with self.assertRaises(psycopg.Error) as changed:
                db.execute("ALTER TABLE rows ALTER COLUMN amount TYPE NUMERIC(22,4)")
            self.assertEqual("0A000", changed.exception.sqlstate)
            with self.assertRaises(psycopg.Error) as dropped:
                db.execute("ALTER TABLE rows DROP COLUMN tags")
            self.assertEqual("2BP01", dropped.exception.sqlstate)
            self.assertEqual(before, attributes(db, "stored"))
            self.assertEqual(rows, db.execute("SELECT * FROM stored").fetchall())

    def test_owner_delegation_is_not_session_impersonation_and_checks_live_grants(self):
        with postgres() as db:
            db.execute("CREATE ROLE view_owner NOLOGIN")
            db.execute("CREATE ROLE view_reader NOLOGIN")
            db.execute("CREATE SCHEMA app")
            db.execute("GRANT USAGE ON SCHEMA app TO view_owner,view_reader")
            db.execute("GRANT CREATE ON SCHEMA app TO view_owner")
            db.execute("CREATE TABLE app.rows(id BIGINT,principal TEXT)")
            db.execute("INSERT INTO app.rows VALUES(1,'view_owner'),(2,'view_reader')")
            db.execute("ALTER TABLE app.rows ENABLE ROW LEVEL SECURITY")
            db.execute(
                "CREATE POLICY owner_visible ON app.rows TO view_owner USING(id = 1)"
            )
            db.execute(
                "CREATE POLICY reader_visible ON app.rows TO view_reader USING(id = 2)"
            )
            db.execute("GRANT SELECT ON app.rows TO view_owner,view_reader")
            db.execute("SET ROLE view_owner")
            db.execute(
                "CREATE VIEW app.owner_view AS "
                "SELECT id,current_user AS running_user FROM app.rows"
            )
            db.execute(
                "CREATE VIEW app.invoker_view WITH(security_invoker=true) AS "
                "SELECT id,current_user AS running_user FROM app.rows"
            )
            db.execute("RESET ROLE")
            db.execute("GRANT SELECT ON app.owner_view,app.invoker_view TO view_reader")
            db.execute("SET ROLE view_reader")
            self.assertEqual(
                [(1, "view_reader")],
                db.execute("SELECT * FROM app.owner_view").fetchall(),
            )
            self.assertEqual(
                [(2, "view_reader")],
                db.execute("SELECT * FROM app.invoker_view").fetchall(),
            )
            # Policy selection uses checkAsUser, but SQL functions inside the
            # selected policy still evaluate in the caller's session.
            db.execute("RESET ROLE")
            db.execute("DROP POLICY owner_visible ON app.rows")
            db.execute("DROP POLICY reader_visible ON app.rows")
            db.execute(
                "CREATE POLICY session_visible ON app.rows TO view_owner,view_reader USING(principal = current_user)"
            )
            db.execute("SET ROLE view_reader")
            self.assertEqual(
                [(2, "view_reader")],
                db.execute("SELECT * FROM app.owner_view").fetchall(),
            )
            db.execute("RESET ROLE")
            db.execute("REVOKE SELECT ON app.rows FROM view_reader")
            db.execute("SET ROLE view_reader")
            self.assertEqual(
                [(2, "view_reader")],
                db.execute("SELECT * FROM app.owner_view").fetchall(),
            )
            with self.assertRaises(psycopg.Error) as invoker:
                db.execute("SELECT * FROM app.invoker_view")
            self.assertEqual("42501", invoker.exception.sqlstate)
            db.execute("RESET ROLE")
            db.execute("REVOKE SELECT ON app.rows FROM view_owner")
            db.execute("SET ROLE view_reader")
            with self.assertRaises(psycopg.Error) as owner:
                db.execute("SELECT * FROM app.owner_view")
            self.assertEqual("42501", owner.exception.sqlstate)
