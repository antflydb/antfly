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
