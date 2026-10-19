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

"""Original indexed DML with UUID owners, real index DDL and cold enforcement."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres, cursor_result

IDS = [1509, 1510, 1511, 1512, 1514, 1515, 1516, 1517]


def declaration(number):
    base = "00000000-0000-0000-0000-"

    def identity(n):
        return base + f"{n:012}"

    predicate = (
        "status='active'"
        if number in {1510, 1515}
        else "deleted_at IS NULL"
        if number in {1511, 1516}
        else "tenant_id='t1' AND status='active'"
        if number == 1512
        else None
    )
    setup = [
        {
            "statement": "CREATE TABLE usage_records(id UUID PRIMARY KEY,email TEXT,tenant_id TEXT,status TEXT,amount BIGINT,deleted_at BIGINT)",
            "table": "usage_records",
        },
        {
            "statement": f"INSERT INTO usage_records(id,email,tenant_id,status,amount,deleted_at) VALUES ('{identity(1)}','a@example.test','t1','active',5,NULL),('{identity(2)}','b@example.test','t2','inactive',7,9)"
        },
    ]
    if predicate:
        inactive = (
            ("t1", "inactive", "NULL")
            if number in {1510, 1515}
            else ("t1", "active", "99")
            if number in {1511, 1516}
            else ("t2", "active", "NULL")
        )
        setup.append(
            {
                "statement": f"INSERT INTO usage_records(id,email,tenant_id,status,amount,deleted_at) VALUES ('{identity(5)}','a@example.test','{inactive[0]}','{inactive[1]}',12,{inactive[2]})"
            }
        )
    ddl = "CREATE UNIQUE INDEX usage_records_email_idx ON usage_records(email)"
    if predicate:
        ddl += " WHERE " + predicate
    setup.append({"statement": ddl})
    retained = number in {1509, 1511}
    insert = f"INSERT INTO usage_records(id,email,tenant_id,status,amount,deleted_at) VALUES ('{identity(3)}','a@example.test','t1','active',99,NULL)"
    probe = f"INSERT INTO usage_records(id,email,tenant_id,status,amount,deleted_at) VALUES ('{identity(4)}','a@example.test','t1','active',100,NULL)"
    return {
        "id": f"sql-{number:04}",
        "table": "usage_records",
        "setup": setup,
        "read_sql": "SELECT id::text AS id,email,tenant_id,status,amount::text AS amount,deleted_at FROM usage_records",
        "ddl_sqlstate": None,
        "probes": [
            {"statement": insert, "sqlstate": "23505" if retained else None},
            {"statement": probe, "sqlstate": "23505"},
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
                with psycopg.RawCursor(db) as cursor:
                    original = originals[case["id"]]
                    outcome = cursor_result(cursor, original, read=False)
                    case["outcome"] = {
                        "command_tag": original["sql"].split()[0],
                        "result": outcome,
                    }

                def snapshot():
                    with psycopg.RawCursor(db) as cursor:
                        return cursor_result(
                            cursor,
                            {"id": case["id"], "sql": case["read_sql"], "params": []},
                            read=True,
                        )

                case["after_ddl"] = snapshot()
                for probe in case["probes"]:
                    try:
                        with db.transaction():
                            db.execute(probe["statement"])
                    except psycopg.Error as error:
                        if error.sqlstate != probe["sqlstate"]:
                            raise
                    else:
                        if probe["sqlstate"] is not None:
                            raise ValueError("index enforcement probe succeeded")
                    probe["after"] = snapshot()
                cases.append(case)
    return {"format": 1, "reference": "PostgreSQL exact SQL", "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("indexed mutation PostgreSQL drift")
        print(
            "verified eight original indexed mutations, complete postimages and future owner enforcement"
        )
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
