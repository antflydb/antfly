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

"""Standalone DDL regression using a real PostgreSQL client and one connection."""

import json
import time
import uuid

import psycopg


def exercise_sql_cached_plans(conn):
    # Use a connection with no driver-managed cache: psycopg DEALLOCATE ALL on
    # ALTER would otherwise also remove this manually managed SQL statement.
    with psycopg.ClientCursor(conn) as cursor:
        cursor.execute(
            "CREATE TABLE cached_sql (id TEXT PRIMARY KEY, n BIGINT NOT NULL)"
        )
        cursor.execute("INSERT INTO cached_sql (id, n) VALUES ('a', 1)")
        cursor.execute(
            "PREPARE cached_sql_plan AS SELECT id, n FROM cached_sql WHERE n = $1"
        )
        cursor.execute("EXECUTE cached_sql_plan(1)")
        assert cursor.fetchone() == ("a", 1)
        cursor.execute("ALTER TABLE cached_sql ADD COLUMN note TEXT")
        cursor.execute("EXECUTE cached_sql_plan(1)")
        assert cursor.fetchone() == ("a", 1)
        cursor.execute("CREATE INDEX cached_sql_n ON cached_sql(n)")
        cursor.execute("EXECUTE cached_sql_plan(1)")
        assert cursor.fetchone() == ("a", 1)
        cursor.execute("PREPARE cached_star AS SELECT * FROM cached_sql")
        cursor.execute("EXECUTE cached_star")
        assert cursor.fetchone() == ("a", 1, None)
        cursor.execute("ALTER TABLE cached_sql ADD COLUMN extra TEXT")
        try:
            cursor.execute("EXECUTE cached_star")
        except psycopg.Error as exc:
            assert exc.sqlstate == "0A000", exc
            assert "cached plan must not change result type" in str(exc)
        else:
            raise AssertionError("changed cached result shape was accepted")
    print("SQL PREPARE survives migrations: PASS", flush=True)


def exercise_cached_plans(conn, migrator):
    conn.execute("CREATE TABLE cached_python (id TEXT PRIMARY KEY, n BIGINT NOT NULL)")
    conn.execute("INSERT INTO cached_python (id, n) VALUES ('a', 1)")
    query = "SELECT id, n FROM cached_python WHERE n = %s"
    assert conn.execute(query, (1,), prepare=True).fetchone() == ("a", 1)
    migrator.execute("ALTER TABLE cached_python ADD COLUMN note TEXT")
    assert conn.execute(query, (1,), prepare=True).fetchone() == ("a", 1)
    migrator.execute("CREATE INDEX cached_python_n ON cached_python(n)")
    assert conn.execute(query, (1,), prepare=True).fetchone() == ("a", 1)
    # Rebinding writes is pre-admission; each EXECUTE still performs one write.
    conn.execute(
        "UPDATE cached_python SET n = n + %s WHERE id = %s", (1, "a"), prepare=True
    )
    migrator.execute("ALTER TABLE cached_python ADD COLUMN another TEXT")
    conn.execute(
        "UPDATE cached_python SET n = n + %s WHERE id = %s", (1, "a"), prepare=True
    )
    assert conn.execute("SELECT n FROM cached_python").fetchone() == (3,)
    print("psycopg cached plans survive migrations: PASS", flush=True)


def exercise_ddl(conn):
    prefix = "ddl_" + uuid.uuid4().hex[:12]

    def admit(statement, *, simple=False):
        deadline = time.monotonic() + 30
        while True:
            try:
                if simple:
                    with psycopg.ClientCursor(conn) as cursor:
                        cursor.execute(statement)
                else:
                    conn.execute(statement, prepare=True)
                return
            except psycopg.Error as exc:
                if exc.sqlstate == "55000":
                    receipt = json.loads(exc.diag.message_detail)
                    assert receipt["state"] == "pending", receipt
                    return  # Committed: poll visibility, never replay CREATE.
                if (
                    exc.sqlstate not in ("53300", "40001")
                    or time.monotonic() >= deadline
                ):
                    raise
                assert json.loads(exc.diag.message_detail)["retryable"] is True
                time.sleep(0.1)

    def visible(table):
        deadline = time.monotonic() + 30
        while True:
            try:
                conn.execute(f"SELECT id FROM {table} LIMIT 1")
                return
            except psycopg.Error as exc:
                if (
                    exc.sqlstate not in ("42P01", "55000", "53300")
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(0.1)

    parent = prefix + "_parent"
    second = prefix + "_second"
    admit(f"CREATE TABLE {parent} (id TEXT PRIMARY KEY, n BIGINT NOT NULL)")
    visible(parent)
    admit(f"CREATE TABLE {second} (id TEXT PRIMARY KEY)")
    visible(second)
    conn.execute(f"INSERT INTO {parent} (id, n) VALUES ('p', 7)")
    conn.execute(f"INSERT INTO {second} (id) VALUES ('p')")
    index = prefix + "_n"
    admit(f"CREATE INDEX IF NOT EXISTS {index} ON {parent}(n)", simple=True)
    # The index namespace must be republished after every schema mutation.
    admit(f"DROP INDEX {index}", simple=True)
    admit(f"CREATE INDEX {index} ON {parent}(n)")

    # Generated backing indexes must avoid existing namespace objects and
    # explicit names later in the declaration. Exercise the FK plan path too.
    collision = prefix + "_collision"
    for suffix in ("_pkey", "_pkey1"):
        admit(f"CREATE INDEX {collision + suffix} ON {parent}(n)")
    admit(
        f"CREATE TABLE {collision} (id TEXT PRIMARY KEY, p TEXT REFERENCES {parent}(id), "
        f"n BIGINT, CONSTRAINT {collision}_pkey2 UNIQUE(n))"
    )
    visible(collision)
    conn.execute(f"INSERT INTO {collision} (id, p, n) VALUES ('c', 'p', 1)")
    conn.execute(f"SELECT id FROM {collision}").fetchone()
    conn.execute(f"DELETE FROM {collision} WHERE id = 'c'")
    admit(f"CREATE TABLE IF NOT EXISTS {collision} (id TEXT PRIMARY KEY)")
    try:
        admit(f"CREATE INDEX {collision}_pkey3 ON {parent}(n)")
    except psycopg.Error as exc:
        assert exc.sqlstate == "42P07", exc
    else:
        raise AssertionError("generated primary-key index name was not reserved")
    # Clipping two distinct table identifiers can yield the same default name.
    long_prefix = prefix + "x" * (60 - len(prefix))
    for ending in ("aaa", "bbb"):
        long_table = long_prefix + ending
        admit(f"CREATE TABLE {long_table} (id TEXT PRIMARY KEY)", simple=True)
        visible(long_table)
        conn.execute(f"INSERT INTO {long_table} (id) VALUES ('c')")

    explicit = prefix + "_explicit"
    try:
        admit(
            f"CREATE TABLE IF NOT EXISTS {explicit} (id TEXT CONSTRAINT {collision}_pkey PRIMARY KEY)"
        )
    except psycopg.Error as exc:
        assert exc.sqlstate == "42P07", exc
    else:
        raise AssertionError("explicit index-name conflict was silently accepted")
    admit(f"CREATE TABLE {explicit} (id TEXT PRIMARY KEY)")
    visible(explicit)

    for columns in (10, 20, 30, 40, 50, 128, 256, 10):
        print(f"psycopg standalone DDL: {columns} columns", flush=True)
        child = prefix + "_child_" + str(columns) + "_" + uuid.uuid4().hex[:6]
        fields = [
            "id TEXT PRIMARY KEY",
            f"p1 TEXT REFERENCES {parent}(id) ON DELETE SET NULL",
            f"p2 TEXT REFERENCES {second}(id) ON DELETE SET NULL",
        ] + [f"c{i} TEXT" for i in range(columns - 3)]
        admit(f"CREATE TABLE {child} (" + ", ".join(fields) + ")", simple=columns == 50)
        visible(child)
        conn.execute(f"INSERT INTO {child} (id, p1, p2) VALUES ('c', 'p', 'p')")
        assert conn.execute(
            f"SELECT p1, p2 FROM {child} WHERE id = 'c'"
        ).fetchone() == ("p", "p")
        conn.execute(f"DELETE FROM {child} WHERE id = 'c'")

    # A rejected declaration must not reserve the name or lock the parent.
    recovery = prefix + "_recovery"
    try:
        conn.execute(
            f"CREATE TABLE {recovery} (id TEXT PRIMARY KEY, p BIGINT REFERENCES {parent}(id))"
        )
    except psycopg.Error as exc:
        assert exc.sqlstate not in ("40003", "55000"), exc
    else:
        raise AssertionError("incompatible foreign key accepted")
    admit(
        f"CREATE TABLE {recovery} (id TEXT PRIMARY KEY, p TEXT REFERENCES {parent}(id) ON DELETE SET NULL)"
    )
    visible(recovery)
    conn.execute(f"INSERT INTO {recovery} (id, p) VALUES ('c', 'p')")
    conn.execute(f"DELETE FROM {parent} WHERE id = 'p'")
    assert conn.execute(f"SELECT p FROM {recovery} WHERE id = 'c'").fetchone() == (
        None,
    )
    print("psycopg standalone DDL (indexes, 256 columns, FK rejection/recovery): PASS")
