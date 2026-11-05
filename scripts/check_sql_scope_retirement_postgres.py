#!/usr/bin/env python3
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

"""PostgreSQL oracle for ancestor retirement, dependencies and FORCE custody.

Executes sql-0105/sql-0108/sql-0109 unchanged. This independent oracle is not
native completion evidence; original dispositions stay open until native
publication, fault and cold-recovery gates qualify the same behavior.
"""

import json

from generate_sql_postgres_reference import FIXTURES, postgres


def check():
    import psycopg

    assertions = 0

    def equal(actual, expected):
        nonlocal assertions
        if actual != expected:
            raise AssertionError((actual, expected))
        assertions += 1

    def scalar(db, sql):
        return db.execute(sql).fetchone()[0]

    def rejected(db, sql, state):
        try:
            db.execute(sql)
        except psycopg.Error as error:
            equal(error.sqlstate, state)
        else:
            raise AssertionError(f"unexpected success: {sql}")

    originals = {
        entry["id"]: entry["sql"]
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    with postgres(max_prepared_transactions=8) as db:
        db.execute("SET statement_timeout = '30s'")
        db.execute("CREATE SCHEMA tenant_ops")
        db.execute("CREATE TABLE tenant_ops.parents(id bigint PRIMARY KEY)")
        db.execute("INSERT INTO tenant_ops.parents VALUES (1),(2)")
        db.execute(
            "CREATE TABLE survivors(id bigint PRIMARY KEY, parent_id bigint REFERENCES tenant_ops.parents ON DELETE CASCADE)"
        )
        db.execute("INSERT INTO survivors VALUES (3,1)")
        db.execute("CREATE VIEW outside_view AS SELECT * FROM tenant_ops.parents")
        db.execute("CREATE VIEW outside_nested AS SELECT * FROM outside_view")
        db.execute("CREATE SEQUENCE tenant_ops.counter")
        db.execute(
            "CREATE TABLE borrower(id bigint DEFAULT nextval('tenant_ops.counter'))"
        )
        db.execute("INSERT INTO borrower DEFAULT VALUES")
        db.execute(
            "CREATE FUNCTION tenant_ops.audit(text) RETURNS text LANGUAGE SQL AS 'SELECT $1'"
        )
        db.execute(
            "CREATE FUNCTION tenant_ops.audit(bigint) RETURNS bigint LANGUAGE SQL AS 'SELECT $1'"
        )
        db.execute(
            "CREATE FUNCTION tenant_ops.step(text,text) RETURNS text LANGUAGE SQL AS 'SELECT coalesce($1,$2)'"
        )
        db.execute(
            "CREATE FUNCTION tenant_ops.same(text,text) RETURNS boolean LANGUAGE SQL AS 'SELECT $1=$2'"
        )
        db.execute(
            "CREATE FUNCTION tenant_ops.convert(jsonb) RETURNS text LANGUAGE SQL AS $$SELECT 'native'$$"
        )
        db.execute(
            "CREATE FUNCTION scope_finish(text) RETURNS text LANGUAGE SQL AS 'SELECT upper($1)'"
        )
        db.execute(
            "CREATE FUNCTION scope_outside_step(text,text) RETURNS text LANGUAGE SQL AS 'SELECT coalesce($1,$2)'"
        )
        db.execute(
            "CREATE AGGREGATE scope_collect(text)(SFUNC=tenant_ops.step,STYPE=text,FINALFUNC=scope_finish)"
        )
        db.execute(
            "CREATE OPERATOR === (FUNCTION=tenant_ops.same,LEFTARG=text,RIGHTARG=text)"
        )
        db.execute(
            "CREATE CAST (jsonb AS text) WITH FUNCTION tenant_ops.convert(jsonb) AS ASSIGNMENT"
        )
        db.execute(
            "CREATE AGGREGATE scope_unrelated(text)(SFUNC=scope_outside_step,STYPE=text,FINALFUNC=scope_finish)"
        )
        rejected(db, "DROP SCHEMA tenant_ops", "2BP01")
        equal(scalar(db, "SELECT count(*) FROM tenant_ops.parents"), 2)
        db.execute("BEGIN")
        equal(db.execute(originals["sql-0105"]).statusmessage, "DROP SCHEMA")
        equal(scalar(db, "SELECT to_regnamespace('tenant_ops')"), None)
        equal(scalar(db, "SELECT to_regprocedure('scope_collect(text)')"), None)
        equal(scalar(db, "SELECT to_regoperator('===(text,text)')"), None)
        equal(
            scalar(
                db,
                "SELECT count(*) FROM pg_cast WHERE castsource='jsonb'::regtype AND casttarget='text'::regtype",
            ),
            0,
        )
        equal(
            scalar(db, "SELECT scope_unrelated(x) FROM (VALUES('kept'::text)) v(x)"),
            "KEPT",
        )
        equal(scalar(db, "SELECT to_regclass('outside_view')"), None)
        equal(scalar(db, "SELECT to_regclass('outside_nested')"), None)
        equal(db.execute("SELECT * FROM survivors").fetchall(), [(3, 1)])
        equal(
            scalar(
                db,
                "SELECT count(*) FROM pg_constraint WHERE conrelid='survivors'::regclass AND contype='f'",
            ),
            0,
        )
        equal(db.execute("SELECT * FROM borrower").fetchall(), [(1,)])
        equal(
            scalar(
                db, "SELECT count(*) FROM pg_attrdef WHERE adrelid='borrower'::regclass"
            ),
            0,
        )
        db.execute("ROLLBACK")
        equal(scalar(db, "SELECT count(*) FROM tenant_ops.parents"), 2)
        equal(scalar(db, "SELECT count(*) FROM outside_nested"), 2)
        equal(
            scalar(
                db,
                "SELECT count(*) FROM pg_constraint WHERE conrelid='survivors'::regclass AND contype='f'",
            ),
            1,
        )
        equal(scalar(db, "SELECT tenant_ops.audit(7::bigint)"), 7)
        equal(scalar(db, "SELECT tenant_ops.audit('kept'::text)"), "kept")
        equal(db.execute(originals["sql-0105"]).statusmessage, "DROP SCHEMA")
        equal(db.execute(originals["sql-0105"]).statusmessage, "DROP SCHEMA")
        db.execute("INSERT INTO survivors VALUES (4,999)")
        db.execute("INSERT INTO borrower DEFAULT VALUES")
        equal(
            db.execute("SELECT * FROM borrower ORDER BY id NULLS LAST").fetchall(),
            [(1,), (None,)],
        )

        db.execute("CREATE DATABASE tenant_ops")
        with psycopg.connect(
            db.info.dsn, dbname="tenant_ops", autocommit=True
        ) as target:
            target.execute("CREATE TABLE durable(id bigint PRIMARY KEY)")
            target.execute("INSERT INTO durable VALUES (1),(2)")
            rejected(target, originals["sql-0108"], "55006")
        with psycopg.connect(
            db.info.dsn, dbname="tenant_ops", autocommit=True
        ) as target:
            rejected(db, originals["sql-0108"], "55006")
            equal(scalar(target, "SELECT count(*) FROM durable"), 2)
        equal(db.execute(originals["sql-0108"]).statusmessage, "DROP DATABASE")
        equal(
            scalar(db, "SELECT count(*) FROM pg_database WHERE datname='tenant_ops'"), 0
        )
        db.execute("CREATE DATABASE tenant_ops")
        target = psycopg.connect(db.info.dsn, dbname="tenant_ops", autocommit=True)
        try:
            target.execute("CREATE TABLE durable(id bigint PRIMARY KEY)")
            target.execute("BEGIN")
            target.execute("INSERT INTO durable VALUES (1)")
            equal(db.execute(originals["sql-0109"]).statusmessage, "DROP DATABASE")
            equal(
                scalar(
                    db, "SELECT count(*) FROM pg_database WHERE datname='tenant_ops'"
                ),
                0,
            )
            try:
                target.execute("SELECT 1")
            except psycopg.Error as error:
                equal(error.sqlstate, "57P01")
            else:
                raise AssertionError("FORCE left an old backend usable")
        finally:
            target.close()
        equal(db.execute(originals["sql-0109"]).statusmessage, "DROP DATABASE")
        db.execute("CREATE DATABASE tenant_ops")
        with psycopg.connect(
            db.info.dsn, dbname="tenant_ops", autocommit=True
        ) as target:
            target.execute("CREATE TABLE durable(id bigint PRIMARY KEY)")
            target.execute("BEGIN")
            target.execute("INSERT INTO durable VALUES (1)")
            target.execute("PREPARE TRANSACTION 'retirement_prepared'")
        rejected(db, originals["sql-0109"], "55006")
        equal(
            scalar(
                db,
                "SELECT count(*) FROM pg_prepared_xacts WHERE gid='retirement_prepared'",
            ),
            1,
        )
        with psycopg.connect(
            db.info.dsn, dbname="tenant_ops", autocommit=True
        ) as target:
            target.execute("ROLLBACK PREPARED 'retirement_prepared'")
            equal(scalar(target, "SELECT count(*) FROM durable"), 0)
        db.execute("BEGIN")
        rejected(db, originals["sql-0109"], "25001")
        db.execute("ROLLBACK")
        equal(db.execute(originals["sql-0109"]).statusmessage, "DROP DATABASE")
    print(f"PostgreSQL scope retirement oracle: {assertions} assertions passed")


if __name__ == "__main__":
    check()
