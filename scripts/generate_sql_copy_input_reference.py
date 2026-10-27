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

"""PostgreSQL 18+ COPY input originals, complete postimages and atomic failures."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_temporal_key_reference import output

CREATE = "CREATE TABLE usage_records(id BIGINT PRIMARY KEY,status TEXT CHECK(status <> 'blocked'),amount NUMERIC(12,2) DEFAULT 3.50,normalized TEXT GENERATED ALWAYS AS (lower(status)) STORED)"
READ = "SELECT id,status,amount,normalized FROM usage_records ORDER BY id"


def reference():
    import psycopg

    inventory = {
        e["id"]: e
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        db.execute(CREATE)
        db.execute("CREATE TABLE copy_source(id BIGINT,status TEXT)")

        def payload(rows, codec):
            db.execute("DELETE FROM copy_source")
            with db.cursor() as cursor:
                cursor.executemany("INSERT INTO copy_source VALUES(%s,%s)", rows)
                with cursor.copy(
                    f"COPY (SELECT id,status FROM copy_source) TO STDOUT (FORMAT {codec})"
                ) as stream:
                    return b"".join(stream)

        def state():
            return output(db.execute(READ))

        for case_id, codec in [
            ("sql-0370", "csv"),
            ("sql-0371", "text"),
            ("sql-0379", "binary"),
        ]:
            original = inventory[case_id]
            db.execute("DELETE FROM usage_records")
            rows = [(i, ["READY", 'a\tb\nc\\d"é', "", None][i % 4]) for i in range(300)]
            data = payload(rows, codec)
            with db.cursor() as cursor:
                with cursor.copy(original["sql"]) as stream:
                    for offset in range(0, len(data), 7):
                        stream.write(data[offset : offset + 7])
                tag = cursor.statusmessage
            expected = state()
            faults = []
            for name, final, code in [
                ("duplicate", (0, "READY"), "23505"),
                ("check", (1128, "blocked"), "23514"),
                ("null", (None, "READY"), "23502"),
            ]:
                broken = payload(
                    [(1000 + i, "READY") for i in range(128)] + [final], codec
                )
                try:
                    with db.transaction():
                        with db.cursor().copy(original["sql"]) as stream:
                            stream.write(broken)
                except psycopg.Error as error:
                    if error.sqlstate != code:
                        raise
                else:
                    raise AssertionError((case_id, name, "accepted"))
                if state() != expected:
                    raise AssertionError((case_id, name, "partial import"))
                faults.append({"name": name, "hex": broken.hex(), "sqlstate": code})
            prefix = payload([(2000 + i, "READY") for i in range(128)], codec)
            malformed = (
                prefix[:-2] + b"\x00\x03"
                if codec == "binary"
                else prefix + (b"1,2,3\n" if codec == "csv" else b"1\t2\t3\n")
            )
            try:
                with db.transaction():
                    with db.cursor().copy(original["sql"]) as stream:
                        stream.write(malformed)
            except psycopg.Error as error:
                if error.sqlstate != "22P04":
                    raise
            else:
                raise AssertionError((case_id, "malformed accepted"))
            if state() != expected:
                raise AssertionError((case_id, "malformed partial import"))
            faults.append(
                {"name": "malformed", "hex": malformed.hex(), "sqlstate": "22P04"}
            )
            entries.append(
                {
                    "id": case_id,
                    "sql": original["sql"],
                    "format": codec,
                    "hex": data.hex(),
                    "tag": tag,
                    "output": expected,
                    "faults": faults,
                }
            )
        identity_create = "CREATE TABLE copy_identity(id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,status TEXT)"
        db.execute(identity_create)
        identity_copy = "COPY copy_identity(id,status) FROM STDIN"
        with db.cursor().copy(identity_copy) as stream:
            stream.write(b"42\tREADY\n")
        db.execute("INSERT INTO copy_identity(status) VALUES('default')")
        with db.cursor().copy("COPY copy_identity(status) FROM STDIN") as stream:
            stream.write(b"next\n")
        identity = {
            "create": identity_create,
            "copy": identity_copy,
            "hex": b"42\tREADY\n".hex(),
            "output": output(
                db.execute("SELECT id,status FROM copy_identity ORDER BY id")
            ),
        }
        db.execute("CREATE TABLE copy_rls(id BIGINT PRIMARY KEY,status TEXT)")
        db.execute("CREATE ROLE copy_writer")
        db.execute("GRANT INSERT ON copy_rls TO copy_writer")
        db.execute(
            "CREATE POLICY allow_all ON copy_rls FOR ALL USING(TRUE) WITH CHECK(TRUE)"
        )
        db.execute("ALTER TABLE copy_rls ENABLE ROW LEVEL SECURITY")
        db.execute("SET ROLE copy_writer")
        try:
            with db.transaction():
                with db.cursor().copy("COPY copy_rls FROM STDIN") as stream:
                    stream.write(b"1\tREADY\n")
        except psycopg.Error as error:
            if error.sqlstate != "0A000":
                raise
        else:
            raise AssertionError("COPY unexpectedly admitted RLS")
        db.execute("RESET ROLE")
        if db.execute("SELECT * FROM copy_rls").fetchall():
            raise AssertionError("RLS COPY wrote rows")
    return {
        "reference": "PostgreSQL exact COPY input",
        "create": CREATE,
        "read": READ,
        "entries": entries,
        "identity": identity,
        "rls_sqlstate": "0A000",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("COPY input PostgreSQL witness drift")
        print(
            "verified three original COPY input codecs and twelve atomic-failure postimages"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
