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

"""Qualify native table placement/name evolution against PostgreSQL."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from generate_sql_postgres_reference import FIXTURES, postgres

CREATE = "CREATE TABLE usage_records(id UUID,status TEXT NOT NULL)"
SEED = "INSERT INTO usage_records VALUES('00000000-0000-0000-0000-000000000001','retained')"
READ = "SELECT id::text AS id,status FROM renamed ORDER BY id"


def implicit_reference(db):
    import psycopg
    from psycopg import sql

    db.execute("SET search_path=public")
    statements = [
        (
            "CREATE TABLE public.implicit_rows(z TEXT UNIQUE,a BIGINT DEFAULT 7)",
            [],
            False,
            None,
        ),
        ("INSERT INTO implicit_rows VALUES('first') RETURNING z,a", [], False, None),
        (
            "INSERT INTO implicit_rows VALUES('second',DEFAULT),('third',3) RETURNING z,a",
            [],
            False,
            None,
        ),
        ("INSERT INTO implicit_rows SELECT 'fourth',4 RETURNING z,a", [], False, None),
        ("INSERT INTO implicit_rows SELECT 'fifth' RETURNING z,a", [], False, None),
        (
            "INSERT INTO implicit_rows SELECT $1,$2 RETURNING z,a",
            ["bound", 5],
            False,
            None,
        ),
        (
            "INSERT INTO implicit_rows SELECT * FROM (VALUES('sixth',6)) AS v(z,a) RETURNING z,a",
            [],
            False,
            None,
        ),
        (
            "INSERT INTO implicit_rows VALUES((SELECT 'seventh'),7) RETURNING z,a",
            [],
            False,
            None,
        ),
        (
            "INSERT INTO implicit_rows VALUES((SELECT 'eighth'),DEFAULT) RETURNING z,a",
            [],
            False,
            None,
        ),
        (
            "INSERT INTO implicit_rows VALUES('first',100) ON CONFLICT(z) DO UPDATE SET a=excluded.a RETURNING z,a",
            [],
            False,
            None,
        ),
        ("INSERT INTO implicit_rows VALUES('excess',8,9)", [], False, "42601"),
        (
            "CREATE TABLE public.implicit_generated(z BIGINT,twice BIGINT GENERATED ALWAYS AS(z*2) STORED)",
            [],
            False,
            None,
        ),
        (
            "INSERT INTO implicit_generated VALUES(3,DEFAULT) RETURNING z,twice",
            [],
            False,
            None,
        ),
        (
            "INSERT INTO implicit_generated VALUES((SELECT 5),DEFAULT) RETURNING z,twice",
            [],
            False,
            None,
        ),
        (
            "INSERT INTO implicit_generated VALUES(4,8) RETURNING z,twice",
            [],
            False,
            "428C9",
        ),
        ("SELECT z,twice FROM implicit_generated ORDER BY z", [], False, None),
        ("SELECT z,a FROM implicit_rows ORDER BY z", [], True, None),
        ("ALTER TABLE implicit_rows ADD COLUMN tail BIGINT DEFAULT 9", [], True, None),
        ("ALTER TABLE implicit_rows RENAME COLUMN a TO qty", [], True, None),
        (
            "INSERT INTO implicit_rows VALUES('cold',10,11) RETURNING z,qty,tail",
            [],
            True,
            None,
        ),
        ("SELECT z,qty,tail FROM implicit_rows ORDER BY z", [], True, None),
    ]
    output = []
    for statement, params, cold, code in statements:
        step = {"statement": statement, "params": params, "cold": cold}
        try:
            if params:
                db.execute("PREPARE implicit_parameters AS " + statement)
                query = sql.SQL("EXECUTE implicit_parameters({})").format(
                    sql.SQL(",").join(map(sql.Literal, params))
                )
                cursor = db.execute(query)
            else:
                cursor = db.execute(statement)
            assert code is None, f"expected {code}: {statement}"
            step["command_tag"] = cursor.statusmessage
            step["columns"] = (
                [column.name for column in cursor.description]
                if cursor.description
                else []
            )
            step["oids"] = (
                [column.type_code for column in cursor.description]
                if cursor.description
                else []
            )
            step["rows"] = (
                [list(row) for row in cursor.fetchall()] if cursor.description else []
            )
            step["sql_nulls"] = [
                [value is None for value in row] for row in step["rows"]
            ]
            if params:
                db.execute("DEALLOCATE implicit_parameters")
        except psycopg.Error as error:
            assert error.sqlstate == code, (statement, error.sqlstate, code)
            step["sqlstate"] = error.sqlstate
        output.append(step)
    return output


def path_reference(db):
    import psycopg

    # The HTTP connection explicitly owns public as its initial namespace.
    # Match that login baseline, including RESET/RESET ALL/DISCARD semantics.
    with psycopg.connect(
        db.info.dsn, options="-c search_path=public", autocommit=True
    ) as control:
        return path_connection_reference(control)


def path_connection_reference(db):
    db.execute("SET search_path=public")
    before = [
        "SHOW search_path",
        "SET search_path=empty_ns,public",
        "BEGIN",
        "SET LOCAL search_path=public",
        "SAVEPOINT path_guard",
        "SET search_path=empty_ns",
        "ROLLBACK TO path_guard",
        "RELEASE path_guard",
        "COMMIT",
        "BEGIN",
        "SET search_path=public",
        "ROLLBACK",
        "BEGIN",
        "SET search_path=public",
        "COMMIT",
        "SET LOCAL search_path=empty_ns",
        "SET search_path=empty_ns",
        "RESET search_path",
        "SET search_path=empty_ns,public",
        "RESET ALL",
        "SET search_path=empty_ns",
        "DISCARD ALL",
        "SET search_path=empty_ns,public",
        "BEGIN",
        "SAVEPOINT persisted_path",
        "SET LOCAL search_path=public",
    ]
    after = [
        "SHOW search_path",
        "ROLLBACK TO persisted_path",
        "SHOW search_path",
        "SET search_path=public",
        "COMMIT",
        "SHOW search_path",
        "SET search_path=empty_ns,public",
    ]
    output = []
    for cold, commands in ((False, before), (True, after)):
        for statement in commands:
            cursor = db.execute(statement)
            tag = cursor.statusmessage
            if cursor.description:
                cursor.fetchall()
            path = db.execute("SHOW search_path").fetchone()[0]
            output.append(
                {"statement": statement, "command_tag": tag, "path": path, "cold": cold}
            )
    return output


def reference():
    from psycopg import sql

    original = next(
        e
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        if e["id"] == "sql-1202"
    )
    assert not original["params"]
    with (
        postgres() as db,
        TemporaryDirectory(prefix="antfly-pg-move-", dir="/tmp") as directory,
    ):
        db.execute(
            sql.SQL("CREATE TABLESPACE fastspace LOCATION {}").format(
                sql.Literal(directory)
            )
        )
        db.execute("CREATE SCHEMA empty_ns")
        db.execute(CREATE)
        db.execute(SEED)
        before = db.execute(
            "SELECT oid FROM pg_class WHERE oid='usage_records'::regclass"
        ).fetchone()[0]
        db.execute("SET search_path=empty_ns,public")
        tag = db.execute(original["sql"]).statusmessage
        bound = db.execute(
            "SELECT reltablespace = (SELECT oid FROM pg_tablespace WHERE spcname='fastspace') FROM pg_class WHERE oid='usage_records'::regclass"
        ).fetchone()[0]
        assert bound
        rename = "ALTER TABLE usage_records RENAME TO renamed"
        rename_tag = db.execute(rename).statusmessage
        assert (
            db.execute(
                "SELECT oid FROM pg_class WHERE oid='renamed'::regclass"
            ).fetchone()[0]
            == before
        )
        rows = [list(r) for r in db.execute(READ).fetchall()]
        db.execute(original["sql"].replace("usage_records", "renamed"))
        assert [list(r) for r in db.execute(READ).fetchall()] == rows
        drop = "DROP TABLE renamed"
        drop_tag = db.execute(drop).statusmessage
        absent_tag = db.execute("DROP TABLE IF EXISTS renamed").statusmessage
        db.execute("DROP TABLESPACE fastspace")
        return {
            "implicit": implicit_reference(db),
            "paths": path_reference(db),
            "format": 1,
            "id": original["id"],
            "sql": original["sql"],
            "create": CREATE,
            "seed": SEED,
            "command_tag": tag,
            "rename": rename,
            "rename_tag": rename_tag,
            "read": READ,
            "rows": rows,
            "bound": bound,
            "drop": drop,
            "drop_tag": drop_tag,
            "absent_tag": absent_tag,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    expected = reference()
    if args.check:
        assert json.loads(args.check.read_text()) == expected, (
            "table evolution PostgreSQL drift"
        )
        print("verified original sql-1202 and table placement/rename/drop lifecycle")
    else:
        print(json.dumps(expected, indent=2))


if __name__ == "__main__":
    main()
