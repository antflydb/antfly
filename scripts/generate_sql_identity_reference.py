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

"""Original serial/identity declarations and complete PostgreSQL write lifecycles."""

import argparse
import json
import subprocess
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_temporal_key_reference import output


def reference():
    import psycopg

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    source = json.loads(
        subprocess.check_output(
            [
                "git",
                "show",
                inventory["source_commit"] + ":" + inventory["source_path"],
            ],
            text=True,
        )
    )["entries"]
    entries = []
    for ordinal in [318, 319, 320]:
        original = source[ordinal - 1]
        with postgres() as db:
            steps = []

            def step(sql, state=None, *, steps=steps):
                try:
                    cursor = db.execute(sql)
                    if state:
                        raise AssertionError((sql, state))
                    result = {
                        "sql": sql,
                        "sqlstate": None,
                        "tag": cursor.statusmessage,
                        "output": output(cursor),
                    }
                except psycopg.Error as error:
                    if error.sqlstate != state:
                        raise
                    result = {
                        "sql": sql,
                        "sqlstate": state,
                        "tag": None,
                        "output": None,
                    }
                steps.append(result)

            step(original["sql"])
            sequence_name = db.execute(
                "SELECT pg_get_serial_sequence('usage_records','id')"
            ).fetchone()[0]
            if sequence_name != "public.usage_records_id_seq":
                raise AssertionError(sequence_name)
            step(
                "INSERT INTO usage_records(status) VALUES('a'),('b') RETURNING id,status"
            )
            step(
                "INSERT INTO usage_records(id,status) VALUES(DEFAULT,'default') RETURNING id,status"
            )
            step(
                "INSERT INTO usage_records(id,status) VALUES(42,'explicit') RETURNING id,status",
                "428C9" if ordinal == 320 else None,
            )
            step(
                "INSERT INTO usage_records(id,status) OVERRIDING SYSTEM VALUE VALUES(43,'system') RETURNING id,status"
            )
            step(
                "INSERT INTO usage_records(id,status) OVERRIDING USER VALUE VALUES(44,'user') RETURNING id,status"
            )
            step(
                "INSERT INTO usage_records(id,status) OVERRIDING SYSTEM VALUE VALUES(NULL,'null')",
                "23502",
            )
            step(
                "UPDATE usage_records SET id=DEFAULT WHERE status='a' RETURNING id,status"
            )
            step(
                "UPDATE usage_records SET id=99 WHERE status='b' RETURNING id,status",
                "428C9" if ordinal == 320 else None,
            )
            step(
                "ALTER TABLE usage_records ADD CONSTRAINT status_check CHECK(status <> 'bad')"
            )
            step("INSERT INTO usage_records(status) VALUES('bad')", "23514")
            step(
                "INSERT INTO usage_records(status) VALUES('after_failure') RETURNING id,status"
            )
            step("BEGIN")
            step(
                "INSERT INTO usage_records(status) VALUES('rolled_back') RETURNING id,status"
            )
            step("ROLLBACK")
            step(
                "SELECT currval('usage_records_id_seq') AS current_value,lastval() AS last_value"
            )
            step(
                "INSERT INTO usage_records(status) VALUES('after_rollback') RETURNING id,status"
            )
            step("SELECT id,status FROM usage_records ORDER BY id")
            step("TRUNCATE usage_records CONTINUE IDENTITY")
            step(
                "INSERT INTO usage_records(status) VALUES('continued') RETURNING id,status"
            )
            step("TRUNCATE usage_records RESTART IDENTITY")
            step(
                "INSERT INTO usage_records(status) VALUES('restarted') RETURNING id,status"
            )
            entries.append(
                {"id": f"sql-{ordinal:04d}", "sql": original["sql"], "steps": steps}
            )
    # Standalone session/cache behavior is independent of identity syntax.
    with postgres() as db, psycopg.connect(db.info.dsn, autocommit=True) as other:
        db.execute("CREATE SEQUENCE first_seq START 100 INCREMENT 10 CACHE 4")
        db.execute("CREATE SEQUENCE second_seq START 1 CACHE 1")
        db.execute("CREATE TABLE sequence_anchor(id TEXT PRIMARY KEY,n BIGINT)")
        db.execute("INSERT INTO sequence_anchor(id,n) VALUES ('a',0),('b',0)")
        sessions = []
        for connection, sql, state in [
            (db, "SELECT currval('first_seq')", "55000"),
            (db, "SELECT lastval()", "55000"),
            (db, "SELECT nextval('first_seq')", None),
            (other, "SELECT nextval('first_seq')", None),
            (db, "SELECT nextval('first_seq')", None),
            (db, "SELECT setval('second_seq',500,true)", None),
            (db, "SELECT currval('second_seq'),lastval()", None),
            (db, "SELECT setval('first_seq',900,false)", None),
            (db, "SELECT currval('first_seq'),lastval()", None),
            (other, "SELECT nextval('first_seq')", None),
            (db, "SELECT nextval('first_seq')", None),
            (db, "ALTER SEQUENCE first_seq INCREMENT 7", None),
            (other, "SELECT nextval('first_seq')", None),
            (db, "SELECT nextval('first_seq')", None),
            (db, "DISCARD SEQUENCES", None),
            (db, "SELECT currval('first_seq')", "55000"),
            (db, "SELECT lastval()", "55000"),
            (db, "CREATE SEQUENCE retired_seq", None),
            (db, "SELECT nextval('retired_seq')", None),
            (db, "DROP SEQUENCE retired_seq", None),
            (db, "SELECT lastval()", "55000"),
            (db, "CREATE SEQUENCE retired_seq START 10", None),
            (db, "SELECT currval('retired_seq')", "55000"),
            (db, "SELECT nextval('retired_seq')", None),
            (db, "DROP SEQUENCE retired_seq", None),
            (db, "SELECT lastval()", "55000"),
            (db, "CREATE SEQUENCE ordered_seq CACHE 8", None),
            (
                db,
                "MERGE INTO sequence_anchor AS t USING (VALUES ('a'),('b')) AS s(id) ON t.id=s.id WHEN MATCHED AND nextval('ordered_seq')>0 THEN UPDATE SET n=currval('ordered_seq')",
                None,
            ),
            (db, "SELECT n FROM sequence_anchor ORDER BY n", None),
            (
                db,
                "SELECT nextval('ordered_seq'),currval('ordered_seq') FROM sequence_anchor ORDER BY id",
                None,
            ),
            (
                db,
                "SELECT nextval('ordered_seq'),currval('ordered_seq') FROM sequence_anchor ORDER BY n",
                None,
            ),
            (db, "CREATE SEQUENCE strict_seq", None),
            (db, "SELECT setval(NULL,nextval('strict_seq'),true)", None),
            (db, "SELECT nextval('strict_seq')", None),
        ]:
            try:
                cursor = connection.execute(sql)
                if state:
                    raise AssertionError((sql, state))
                result = {
                    "sql": sql,
                    "connection": "first" if connection is db else "second",
                    "sqlstate": None,
                    "output": output(cursor),
                }
            except psycopg.Error as error:
                if error.sqlstate != state:
                    raise
                result = {
                    "sql": sql,
                    "connection": "first" if connection is db else "second",
                    "sqlstate": state,
                    "output": None,
                }
            sessions.append(result)
    with postgres() as db:
        document_sql = source[1102]["sql"]
        db.execute("CREATE TABLE docs(_id TEXT PRIMARY KEY,title TEXT)")
        db.execute("INSERT INTO docs VALUES ('doc:a','before')")
        cursor = db.execute(document_sql)
        document_truncate = {
            "id": "sql-1103",
            "sql": document_sql,
            "tag": cursor.statusmessage,
            "empty": output(db.execute("SELECT _id,title FROM docs ORDER BY _id")),
        }
        db.execute("INSERT INTO docs VALUES ('doc:a','after')")
        document_truncate["final"] = output(
            db.execute("SELECT _id,title FROM docs ORDER BY _id")
        )
    with postgres() as db:
        db.execute("CREATE SEQUENCE runtime_null_probe")
        db.execute("SET plan_cache_mode=force_generic_plan")
        db.execute(
            "PREPARE runtime_null(regclass) AS SELECT setval($1,nextval('runtime_null_probe'),true)"
        )
        runtime_null = {"output": output(db.execute("EXECUTE runtime_null(NULL)"))}
        value, called = db.execute(
            "SELECT last_value,is_called FROM runtime_null_probe"
        ).fetchone()
        runtime_null["last_value"] = value
        runtime_null["is_called"] = called
    return {
        "format": 1,
        "reference": "PostgreSQL original serial and identity lifecycles",
        "entries": entries,
        "sessions": sessions,
        "document_truncate": document_truncate,
        "runtime_null": runtime_null,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    value = reference()
    if args.check:
        if json.loads(args.check.read_text()) != value:
            raise SystemExit("identity PostgreSQL witness drift")
        print(
            "verified three original declarations, identity writes and cross-session cache behavior"
        )
    elif args.output:
        args.output.write_text(json.dumps(value, indent=2) + "\n")
    else:
        print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
