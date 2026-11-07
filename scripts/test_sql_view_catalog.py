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
    def test_record_table_function_validates_discarded_typed_outputs(self):
        with postgres() as db:
            for sql in (
                """SELECT count(*) FROM jsonb_to_record('{"n":"bad"}') AS r(n BIGINT)""",
                """SELECT 1 FROM jsonb_to_record('{"n":"bad"}') AS r(n BIGINT)""",
            ):
                with self.subTest(sql=sql):
                    with self.assertRaises(psycopg.Error) as invalid:
                        db.execute(sql)
                    self.assertEqual("22P02", invalid.exception.sqlstate)

    def test_document_view_projects_real_paths_and_typed_arrays(self):
        with postgres() as db:
            db.execute("CREATE TABLE docs(_id TEXT PRIMARY KEY,body JSONB)")
            db.execute(
                """INSERT INTO docs VALUES
                ('a','{"title":"alpha","metadata":{"plan":"pro","score":12},
                       "tags":["urgent",null,"vip"]}'),
                ('b','{"title":"beta","metadata":{"plan":"free","score":4},
                       "tags":[]}'),
                ('c','{"title":"gamma","metadata":{},"tags":null}')"""
            )
            db.execute(
                """CREATE VIEW support_view AS
                SELECT d._id,r.title AS title_text,r.metadata->>'plan' AS plan,
                       (r.metadata->>'score')::BIGINT AS score,r.tags AS tag_list
                FROM docs d CROSS JOIN LATERAL
                jsonb_to_record(d.body) AS r(title TEXT,metadata JSONB,tags TEXT[])"""
            )
            self.assertEqual(
                [
                    ("a", "pro", 12, ["urgent", None, "vip"]),
                    ("b", "free", 4, []),
                    ("c", None, None, None),
                ],
                db.execute(
                    "SELECT _id,plan,score,tag_list FROM support_view ORDER BY _id"
                ).fetchall(),
            )
            self.assertEqual(
                [("a", "urgent"), ("a", None), ("a", "vip")],
                db.execute(
                    "SELECT d._id,t.tag FROM support_view d,"
                    "LATERAL UNNEST(d.tag_list) WITH ORDINALITY AS t(tag,n) "
                    "ORDER BY d._id,t.n"
                ).fetchall(),
            )
            self.assertEqual(
                (None, None),
                db.execute(
                    "SELECT * FROM jsonb_to_record(NULL::jsonb) AS r(a JSONB,b TEXT[])"
                ).fetchone(),
            )
            self.assertEqual(
                (None, None),
                db.execute(
                    """SELECT * FROM jsonb_to_record('{"a":null,"b":null}')
                    AS r(a JSONB,b TEXT[])"""
                ).fetchone(),
            )

    def test_nested_invoker_view_restores_original_caller_authority(self):
        with postgres() as db:
            for role in ("outer_owner", "inner_owner", "view_reader"):
                db.execute("CREATE ROLE " + role)
            db.execute("CREATE SCHEMA app")
            db.execute(
                "GRANT USAGE,CREATE ON SCHEMA app TO "
                "outer_owner,inner_owner,view_reader"
            )
            db.execute("CREATE TABLE app.rows(id BIGINT)")
            db.execute("INSERT INTO app.rows VALUES(1)")
            db.execute("GRANT SELECT ON app.rows TO outer_owner")
            db.execute("SET ROLE inner_owner")
            db.execute(
                "CREATE VIEW app.inner_view WITH(security_invoker=true) AS "
                "SELECT id FROM app.rows"
            )
            db.execute("RESET ROLE")
            db.execute("GRANT SELECT ON app.inner_view TO outer_owner")
            db.execute("SET ROLE outer_owner")
            db.execute("CREATE VIEW app.outer_view AS SELECT id FROM app.inner_view")
            db.execute("RESET ROLE")
            db.execute("GRANT SELECT ON app.outer_view TO view_reader")
            db.execute("SET ROLE view_reader")
            with self.assertRaises(psycopg.Error) as denied:
                db.execute("SELECT * FROM app.outer_view")
            self.assertEqual("42501", denied.exception.sqlstate)
            db.execute("RESET ROLE")
            db.execute("GRANT SELECT ON app.rows TO view_reader")
            db.execute("REVOKE SELECT ON app.rows FROM outer_owner")
            db.execute("SET ROLE view_reader")
            self.assertEqual(
                [(1,)], db.execute("SELECT * FROM app.outer_view").fetchall()
            )

    def test_cyclic_replacement_is_admitted_but_expansion_is_rejected(self):
        with postgres() as db:
            db.execute("CREATE VIEW first_view AS SELECT 1 AS id")
            db.execute("CREATE VIEW second_view AS SELECT id FROM first_view")
            db.execute(
                "CREATE OR REPLACE VIEW first_view AS SELECT id FROM second_view"
            )
            with self.assertRaises(psycopg.Error) as recursive:
                db.execute("SELECT * FROM first_view")
            self.assertEqual("42P17", recursive.exception.sqlstate)
            with self.assertRaises(psycopg.Error) as dependent:
                db.execute("DROP VIEW first_view")
            self.assertEqual("2BP01", dependent.exception.sqlstate)
            db.execute("DROP VIEW first_view CASCADE")
            self.assertEqual(
                (None,), db.execute("SELECT to_regclass('second_view')").fetchone()
            )

    def test_replacement_retains_owner_and_output_ids_but_resets_omitted_options(self):
        with postgres() as db:
            db.execute("CREATE ROLE view_owner")
            db.execute("CREATE SCHEMA app")
            db.execute("CREATE TABLE app.rows(id BIGINT)")
            db.execute("GRANT USAGE,CREATE ON SCHEMA app TO view_owner")
            db.execute("SET ROLE view_owner")
            db.execute(
                "CREATE VIEW app.stored WITH "
                "(security_invoker=true,security_barrier=true) AS "
                "SELECT id,id AS other FROM app.rows"
            )
            original = attributes(db, "app.stored")
            owner = db.execute(
                "SELECT relowner FROM pg_class WHERE oid='app.stored'::regclass"
            ).fetchone()
            db.execute("RESET ROLE")
            for query in (
                "SELECT id AS renamed,id AS other FROM app.rows",
                "SELECT id::TEXT AS id,id AS other FROM app.rows",
                "SELECT id FROM app.rows",
            ):
                with self.assertRaises(psycopg.Error) as invalid:
                    db.execute("CREATE OR REPLACE VIEW app.stored AS " + query)
                self.assertEqual("42P16", invalid.exception.sqlstate)
            db.execute(
                "CREATE OR REPLACE VIEW app.stored AS "
                "SELECT id,id AS other,'note'::TEXT AS note FROM app.rows"
            )
            self.assertEqual(original, attributes(db, "app.stored")[:2])
            self.assertEqual(
                owner,
                db.execute(
                    "SELECT relowner FROM pg_class WHERE oid='app.stored'::regclass"
                ).fetchone(),
            )
            self.assertEqual(
                (None,),
                db.execute(
                    "SELECT reloptions FROM pg_class WHERE oid='app.stored'::regclass"
                ).fetchone(),
            )

    def test_creation_checks_schema_usage_not_select_and_does_not_recheck_source_usage(
        self,
    ):
        with postgres() as db:
            db.execute("CREATE ROLE view_creator")
            db.execute("CREATE SCHEMA source")
            db.execute("CREATE SCHEMA published")
            db.execute("CREATE TABLE source.rows(id BIGINT)")
            db.execute("INSERT INTO source.rows VALUES(1)")
            db.execute("GRANT USAGE,CREATE ON SCHEMA published TO view_creator")
            db.execute("SET ROLE view_creator")
            with self.assertRaises(psycopg.Error) as schema:
                db.execute("CREATE VIEW published.stored AS SELECT id FROM source.rows")
            self.assertEqual("42501", schema.exception.sqlstate)
            db.execute("RESET ROLE")
            db.execute("GRANT USAGE ON SCHEMA source TO view_creator")
            db.execute("SET ROLE view_creator")
            # Source SELECT is not a CREATE VIEW admission requirement.
            db.execute("CREATE VIEW published.stored AS SELECT id FROM source.rows")
            with self.assertRaises(psycopg.Error) as select:
                db.execute("SELECT * FROM published.stored")
            self.assertEqual("42501", select.exception.sqlstate)
            db.execute("RESET ROLE")
            db.execute("GRANT SELECT ON source.rows TO view_creator")
            db.execute("REVOKE USAGE ON SCHEMA source FROM view_creator")
            db.execute("SET ROLE view_creator")
            # Stored relation IDs do not perform source-name lookup again.
            self.assertEqual(
                [(1,)], db.execute("SELECT * FROM published.stored").fetchall()
            )

    def test_view_ddl_alias_replace_security_and_rename_contracts(self):
        with postgres() as db:
            db.execute("CREATE TABLE rows(id BIGINT,status TEXT)")
            db.execute("INSERT INTO rows VALUES(1,'ready')")
            db.execute(
                "CREATE VIEW stored(record_id) WITH "
                "(security_invoker=true,security_barrier=false) AS SELECT id FROM rows"
            )
            self.assertEqual(
                ["record_id"], [row[0] for row in attributes(db, "stored")]
            )
            db.execute(
                "CREATE OR REPLACE VIEW stored AS SELECT id AS record_id FROM rows"
            )
            db.execute("ALTER VIEW IF EXISTS stored RENAME TO renamed")
            self.assertEqual([(1,)], db.execute("SELECT * FROM renamed").fetchall())
            db.execute("DROP VIEW IF EXISTS renamed CASCADE")
            with self.assertRaises(psycopg.Error) as invalid:
                db.execute("CREATE OR REPLACE VIEW IF NOT EXISTS stored AS SELECT 1")
            self.assertEqual("42601", invalid.exception.sqlstate)
            with self.assertRaises(psycopg.Error) as duplicate:
                db.execute(
                    "CREATE VIEW duplicated WITH "
                    "(security_invoker=true,security_invoker=false) AS SELECT 1"
                )
            self.assertEqual("22023", duplicate.exception.sqlstate)

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
