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

"""Original column COMMENT, removal and privilege semantics in PostgreSQL."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    import psycopg

    original = next(
        entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        if entry["id"] == "sql-0143"
    )
    assert not original["params"]
    statements = [
        original["sql"],
        "COMMENT ON COLUMN usage_records.status IS 'a ''quoted'' Ω'",
        "COMMENT ON COLUMN usage_records.status IS NULL",
        "COMMENT ON COLUMN usage_records.status IS ''",
        original["sql"],
    ]
    steps = []
    with postgres() as db:
        db.execute("CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT)")
        db.execute(
            "INSERT INTO usage_records(id,status) VALUES ('u1','before'),('u9','untouched')"
        )
        before = db.execute(
            "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
        ).fetchall()
        oid = db.execute("SELECT 'usage_records'::regclass::oid").fetchone()[0]
        for statement in statements:
            result = db.execute(statement)
            assert result.statusmessage == "COMMENT"
            description = db.execute(
                "SELECT col_description('usage_records'::regclass,2)"
            ).fetchone()[0]
            assert (
                oid == db.execute("SELECT 'usage_records'::regclass::oid").fetchone()[0]
            )
            assert (
                before
                == db.execute(
                    "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
                ).fetchall()
            )
            steps.append({"sql": statement, "description": description})
        db.execute("CREATE ROLE comment_reader")
        db.execute("GRANT SELECT ON usage_records TO comment_reader")
        try:
            with db.transaction():
                db.execute("SET LOCAL ROLE comment_reader")
                db.execute(original["sql"])
        except psycopg.Error as error:
            assert error.sqlstate == "42501"
        else:
            raise AssertionError("table read must not grant column COMMENT")
        assert (
            before
            == db.execute(
                "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
            ).fetchall()
        )
        assert (
            db.execute(
                "SELECT col_description('usage_records'::regclass,2)"
            ).fetchone()[0]
            == steps[-1]["description"]
        )
    return {
        "format": 1,
        "id": original["id"],
        "steps": steps,
        "denied_sqlstate": "42501",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--check", type=Path)
    target.add_argument("--output", type=Path)
    args = parser.parse_args()
    value = reference()
    if args.check:
        if json.loads(args.check.read_text()) != value:
            raise SystemExit("column comment PostgreSQL witness drift")
        print("verified original column COMMENT, removal, ownership and unchanged rows")
    elif args.output:
        args.output.write_text(json.dumps(value, indent=2) + "\n")
    else:
        print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
