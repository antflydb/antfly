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

"""Independent PostgreSQL oracle for the three unchanged server COPY originals.

Owns /tmp/usage.csv exclusively and refuses to overwrite a pre-existing file.
Run sequentially with the native original-case gate, which owns the same path.
"""

import json
import os
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres


def check():
    import psycopg

    assertions = 0

    def equal(actual, expected):
        nonlocal assertions
        if actual != expected:
            raise AssertionError((actual, expected))
        assertions += 1

    def rejected(db, statement, expected):
        try:
            db.execute(statement)
        except psycopg.Error as error:
            equal(error.sqlstate, expected)
        else:
            raise AssertionError(f"statement unexpectedly succeeded: {statement}")

    originals = {
        entry["id"]: entry["sql"]
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    path = Path("/tmp/usage.csv")
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    inode = os.fstat(fd).st_ino
    try:

        def contents(data):
            os.ftruncate(fd, 0)
            os.pwrite(fd, data, 0)

        with postgres() as db:
            db.execute(
                "CREATE TABLE usage_records(id text PRIMARY KEY, status text NOT NULL)"
            )
            contents(b"a,queued\nb,ready\n")
            equal(db.execute(originals["sql-0375"]).statusmessage, "COPY 2")
            equal(
                db.execute("SELECT * FROM usage_records ORDER BY id").fetchall(),
                [("a", "queued"), ("b", "ready")],
            )
            db.execute("DELETE FROM usage_records")
            contents(b"a\tqueued\nb\tready\n")
            equal(db.execute(originals["sql-0376"]).statusmessage, "COPY 2")
            equal(db.execute(originals["sql-0377"]).statusmessage, "COPY 2")
            equal(sorted(path.read_bytes().splitlines()), [b"a,queued", b"b,ready"])
            equal(
                db.execute(
                    "COPY (SELECT 9007199254740993::bigint, 1.20::numeric, ARRAY[9007199254740993::bigint,NULL::bigint]) TO PROGRAM 'cat > /tmp/usage.csv' WITH (FORMAT csv)"
                ).statusmessage,
                "COPY 1",
            )
            equal(
                path.read_bytes(), b'9007199254740993,1.20,"{9007199254740993,NULL}"\n'
            )
            db.execute(
                "CREATE TABLE typed_copy(id integer PRIMARY KEY, status text NOT NULL)"
            )
            equal(
                db.execute(
                    "COPY typed_copy FROM PROGRAM 'printf \"bad\\tqueued\\n1\\tready\\n\"' WITH (ON_ERROR ignore)"
                ).statusmessage,
                "COPY 1",
            )
            equal(db.execute("SELECT * FROM typed_copy").fetchall(), [(1, "ready")])
            rejected(
                db, "COPY typed_copy FROM PROGRAM 'printf \"bad\\tqueued\\n\"'", "22P02"
            )
            db.execute("CREATE ROLE table_writer")
            db.execute("GRANT INSERT, SELECT ON usage_records TO table_writer")
            db.execute("SET ROLE table_writer")
            rejected(db, originals["sql-0375"], "42501")
            rejected(db, originals["sql-0376"], "42501")
            rejected(db, originals["sql-0377"], "42501")
            db.execute("RESET ROLE")
            db.execute("DELETE FROM usage_records")
            rejected(
                db,
                'COPY usage_records FROM PROGRAM \'i=0; while [ $i -lt 140 ]; do printf "p%s\\tqueued\\n" "$i"; i=$((i+1)); done; exit 7\'',
                "38000",
            )
            equal(db.execute("SELECT count(*) FROM usage_records").fetchone()[0], 0)
            rejected(
                db,
                "COPY usage_records FROM PROGRAM 'printf \"x\\tqueued\\nx\\tready\\n\"'",
                "23505",
            )
            equal(db.execute("SELECT count(*) FROM usage_records").fetchone()[0], 0)
            db.execute("BEGIN")
            db.execute("SAVEPOINT child")
            rejected(
                db,
                "COPY usage_records FROM PROGRAM 'printf \"x\\tqueued\\n\"; exit 7'",
                "38000",
            )
            rejected(db, "SELECT count(*) FROM usage_records", "25P02")
            db.execute("ROLLBACK TO child")
            equal(db.execute("SELECT count(*) FROM usage_records").fetchone()[0], 0)
            db.execute("COMMIT")
            rejected(
                db, "COPY usage_records TO PROGRAM 'cat > /dev/null; exit 9'", "38000"
            )
            equal(
                db.execute(
                    "COPY usage_records FROM PROGRAM 'printf \"kept\\tready\\n\"'"
                ).statusmessage,
                "COPY 1",
            )
            equal(
                db.execute(
                    "COPY (SELECT * FROM usage_records) TO PROGRAM 'cat > /dev/null'"
                ).statusmessage,
                "COPY 1",
            )
    finally:
        os.close(fd)
        if path.exists() and path.stat().st_ino == inode:
            path.unlink()
    print(f"PostgreSQL server COPY oracle: {assertions} assertions passed")


if __name__ == "__main__":
    check()
