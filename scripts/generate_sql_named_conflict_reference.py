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

"""Qualify named constraints separately from index names using exact originals.

The partial/expression objects are real UNIQUE indexes, not fabricated catalog
constraints. PostgreSQL must reject ON CONSTRAINT but accept index inference.
Complete postimages and subsequent owner enforcement accompany each outcome.
"""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, cursor_result, postgres

IDS = (1456, 1459, 1464)


def declaration(number):
    if number == 1464:
        return {
            "id": f"sql-{number:04}",
            "table": "usage_records",
            "setup": [
                {
                    "statement": "CREATE TABLE usage_records(id TEXT CONSTRAINT usage_records_id_pk PRIMARY KEY,status TEXT)",
                    "table": "usage_records",
                },
                {
                    "statement": "INSERT INTO usage_records(id,status) VALUES ('u1','before'),('u9','untouched')"
                },
            ],
            "read_sql": "SELECT id,status FROM usage_records ORDER BY id",
            "ddl_sqlstate": None,
            "probes": [
                {
                    "statement": "INSERT INTO usage_records(id,status) VALUES ('u1','collision')",
                    "sqlstate": "23505",
                    "repeat_on_reopen": True,
                },
                {
                    "statement": "INSERT INTO usage_records(id,status) VALUES ('u2','future')",
                    "sqlstate": None,
                },
                {
                    "statement": "INSERT INTO usage_records(id,status) VALUES ('u1','inferred') ON CONFLICT (id) DO UPDATE SET status=excluded.status",
                    "sqlstate": None,
                },
            ],
        }
    partial = number == 1456
    index = (
        "CREATE UNIQUE INDEX usage_records_active_email_key ON usage_records(email) WHERE status='active'"
        if partial
        else "CREATE UNIQUE INDEX usage_records_lower_email_key ON usage_records(lower(email))"
    )
    inference = "email) WHERE status='active'" if partial else "lower(email))"
    return {
        "id": f"sql-{number:04}",
        "table": "usage_records",
        "setup": [
            {
                "statement": "CREATE TABLE usage_records(id TEXT PRIMARY KEY,email TEXT,status TEXT,name TEXT)",
                "table": "usage_records",
            },
            {
                "statement": "INSERT INTO usage_records(id,email,status,name) VALUES ('u1','a@example.test','active','before'),('u9','z@example.test','inactive','untouched')"
            },
            {"statement": index},
        ],
        "read_sql": "SELECT id,email,status,name FROM usage_records ORDER BY id",
        "ddl_sqlstate": "42704",
        "probes": [
            {
                "statement": "INSERT INTO usage_records(id,email,status,name) VALUES ('u3','a@example.test','active','collision')",
                "sqlstate": "23505",
                "repeat_on_reopen": True,
            },
            {
                "statement": f"INSERT INTO usage_records(id,email,status,name) VALUES ('u2','a@example.test','active','inferred') ON CONFLICT ({inference} DO UPDATE SET name=excluded.name",
                "sqlstate": None,
            },
        ],
    }


def reference():
    import psycopg

    originals = {
        case["id"]: case
        for case in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    cases = []
    with postgres() as db:
        for number in IDS:
            case = declaration(number)
            with db.transaction(force_rollback=True):
                for step in case["setup"]:
                    db.execute(step["statement"])

                def snapshot(case=case):
                    with psycopg.RawCursor(db) as cursor:
                        return cursor_result(
                            cursor,
                            {"id": case["id"], "sql": case["read_sql"], "params": []},
                            read=True,
                        )

                before = snapshot()
                before_versions = db.execute(
                    "SELECT id,xmin::text FROM usage_records ORDER BY id"
                ).fetchall()
                expected = case["ddl_sqlstate"]
                try:
                    with db.transaction(), psycopg.RawCursor(db) as cursor:
                        original = originals[case["id"]]
                        outcome = cursor_result(cursor, original, read=False)
                except psycopg.Error as error:
                    if error.sqlstate != expected:
                        raise
                else:
                    if expected is not None:
                        raise ValueError("index name was accepted as a constraint")
                    case["outcome"] = {"command_tag": "INSERT", "result": outcome}
                case["after_ddl"] = snapshot()
                if expected is not None and (
                    case["after_ddl"] != before
                    or db.execute(
                        "SELECT id,xmin::text FROM usage_records ORDER BY id"
                    ).fetchall()
                    != before_versions
                ):
                    raise ValueError("rejected named arbiter modified rows")
                for probe in case["probes"]:
                    try:
                        with db.transaction():
                            db.execute(probe["statement"])
                    except psycopg.Error as error:
                        if error.sqlstate != probe["sqlstate"]:
                            raise
                    else:
                        if probe["sqlstate"] is not None:
                            raise ValueError("unique-owner enforcement was lost")
                    probe["after"] = snapshot()
                cases.append(case)
    return {"format": 1, "reference": "PostgreSQL exact SQL", "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.check and args.output:
        parser.error("a check must not overwrite its reference")
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("named-conflict PostgreSQL reference drift")
        print("verified three named-arbiter originals and future enforcement")
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
