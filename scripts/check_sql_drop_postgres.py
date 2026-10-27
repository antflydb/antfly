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

"""Independent PostgreSQL 18+ oracle for whole-table constraint retirement.

Run with uv run --offline --no-project --with 'psycopg[binary]==3.3.6'
python scripts/check_sql_drop_postgres.py. The existing oracle context owns an
isolated temporary server. No database URL or persistent user server is used.
This validates semantics; native crash/reply-loss and work-count evidence lives
in antfly-api-sql-drop-retirement-test. It does not change parity dispositions.
"""

import json

from generate_sql_postgres_reference import FIXTURES, postgres


def check():
    import psycopg

    originals = {
        row["id"]: row
        for row in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    assertions = 0
    with postgres() as db:

        def command(statement, tag=None, error=None):
            nonlocal assertions
            try:
                with db.transaction():
                    result = db.execute(statement)
                    actual = result.statusmessage
            except psycopg.Error as failure:
                if failure.sqlstate != error:
                    raise
            else:
                if error is not None or (tag is not None and actual != tag):
                    raise AssertionError((statement, actual, tag, error))
            assertions += 1

        def rows(statement, expected):
            nonlocal assertions
            actual = db.execute(statement).fetchall()
            if actual != expected:
                raise AssertionError((statement, actual, expected))
            assertions += 1

        def owner(name):
            return db.execute("SELECT %s::regclass::oid", (name,)).fetchone()[0]

        command("CREATE TABLE drop_parent(id BIGINT PRIMARY KEY)", "CREATE TABLE")
        command(
            "CREATE TABLE drop_child(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES drop_parent(id),email TEXT UNIQUE)",
            "CREATE TABLE",
        )
        child = owner("drop_child")
        parent = owner("drop_parent")
        command("INSERT INTO drop_parent VALUES(1),(2)", "INSERT 0 2")
        command("INSERT INTO drop_child VALUES(1,1,'a'),(2,2,'b')", "INSERT 0 2")
        command("DROP TABLE drop_parent", error="2BP01")
        command("CREATE VIEW drop_child_view AS SELECT id,email FROM drop_child")
        command("DROP TABLE drop_child", error="2BP01")
        if owner("drop_child") != child or owner("drop_parent") != parent:
            raise AssertionError("Rejected DROP replaced an owner")
        rows(
            "SELECT id,parent,email FROM drop_child ORDER BY id",
            [(1, 1, "a"), (2, 2, "b")],
        )
        command("INSERT INTO drop_child VALUES(3,1,'c')", "INSERT 0 1")
        command("DROP VIEW drop_child_view", "DROP VIEW")
        command("CREATE ROLE drop_child_owner")
        command("ALTER TABLE drop_child OWNER TO drop_child_owner")
        command("SET ROLE drop_child_owner")
        command("DROP TABLE drop_child", "DROP TABLE")
        command("RESET ROLE")
        rows("SELECT id FROM drop_parent ORDER BY id", [(1,), (2,)])
        command("DELETE FROM drop_parent WHERE id=1", "DELETE 1")
        command("DROP TABLE drop_parent", "DROP TABLE")
        command("CREATE TABLE drop_parent(id BIGINT PRIMARY KEY)")
        command(
            "CREATE TABLE drop_child(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES drop_parent(id),email TEXT UNIQUE)"
        )
        if owner("drop_child") == child or owner("drop_parent") == parent:
            raise AssertionError("Name reuse retained a retired owner")
        command("INSERT INTO drop_parent VALUES(1)", "INSERT 0 1")
        command("INSERT INTO drop_child VALUES(1,1,'a')", "INSERT 0 1")
        command("DROP TABLE drop_child")
        command("DROP TABLE drop_parent")
        command(
            "CREATE TABLE drop_self(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES drop_self(id))"
        )
        command("INSERT INTO drop_self VALUES(1,NULL),(2,1)", "INSERT 0 2")
        command("DROP TABLE drop_self", "DROP TABLE")
        command("CREATE TABLE mixed_parent(id BIGINT PRIMARY KEY)")
        command(
            "CREATE TABLE drop_mixed(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES drop_mixed(id),external_parent BIGINT REFERENCES mixed_parent(id))"
        )
        command("INSERT INTO mixed_parent VALUES(1)")
        command("INSERT INTO drop_mixed VALUES(1,NULL,1),(2,1,1)")
        command("DROP TABLE drop_mixed", "DROP TABLE")
        command("DELETE FROM mixed_parent WHERE id=1", "DELETE 1")
        command("DROP TABLE mixed_parent", "DROP TABLE")
        command("CREATE TABLE drop_policy(id BIGINT PRIMARY KEY)")
        command("INSERT INTO drop_policy VALUES(1)")
        command("CREATE POLICY hidden ON drop_policy USING (false)")
        command("ALTER TABLE drop_policy ENABLE ROW LEVEL SECURITY")
        command("DROP TABLE drop_policy", "DROP TABLE")
        rows("SELECT polname FROM pg_policy WHERE polname='hidden'", [])
        for case_id in ("sql-0157", "sql-0159"):
            entry = originals[case_id]
            if entry["params"]:
                raise AssertionError("DROP original acquired unexpected parameters")
            command(
                "CREATE TABLE usage_records(id BIGINT PRIMARY KEY,email TEXT UNIQUE)"
            )
            command("INSERT INTO usage_records VALUES(1,'a')")
            command(entry["sql"], "DROP TABLE")
            rows("SELECT to_regclass('usage_records')::text", [(None,)])
            command(
                entry["sql"],
                "DROP TABLE" if case_id == "sql-0159" else None,
                error="42P01" if case_id == "sql-0157" else None,
            )
    print(
        f"Verified {assertions} PostgreSQL constrained DROP assertions and exact originals sql-0157/sql-0159"
    )


if __name__ == "__main__":
    check()
