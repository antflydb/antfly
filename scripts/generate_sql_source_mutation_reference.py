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

"""Unchanged source mutations with explicit constraints and full postimages."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, encoded, postgres


def profiles(identity):
    if identity == "sql-0591":
        return [
            {
                "name": "snapshot_swap",
                "tables": [
                    {
                        "name": "prices",
                        "create": "CREATE TABLE prices(sku text PRIMARY KEY,source_id text,status text)",
                        "seed": "INSERT INTO prices(sku,source_id,status) VALUES ('a','b','alpha'),('b','a','beta'),('c','missing','untouched')",
                        "read": "SELECT sku,source_id,status FROM prices ORDER BY sku",
                    }
                ],
            }
        ]
    if identity in {"sql-0011", "sql-0014"}:
        return [
            {
                "name": name,
                "tables": [
                    {
                        "name": "usage_records",
                        "create": "CREATE TABLE usage_records(id text PRIMARY KEY,organization_id text,status text)",
                        "seed": "INSERT INTO usage_records(id,organization_id,status) VALUES ('root',NULL,'ready'),('child',"
                        + ("'root'" if recursive else "'absent'")
                        + ",'child')",
                        "read": "SELECT id,organization_id,status FROM usage_records ORDER BY id",
                    },
                    {
                        "name": "archived_records",
                        "create": "CREATE TABLE archived_records(id text PRIMARY KEY)",
                        "seed": "INSERT INTO archived_records(id) VALUES ('retained')",
                        "read": "SELECT id FROM archived_records ORDER BY id",
                    },
                ],
            }
            for name, recursive in (("roots", False), ("recursive_duplicate", True))
        ]
    cases = (
        [("occupied_target", True), ("fresh_target", False)]
        if identity == "sql-0595"
        else [("matched_and_unmatched", True)]
    )
    return [
        {
            "name": name,
            "tables": [
                {
                    "name": "usage_records",
                    "create": "CREATE TABLE usage_records(id text PRIMARY KEY,status text DEFAULT 'queued',amount integer)",
                    "seed": "INSERT INTO usage_records(id,status,amount) VALUES ('u1','old',5)"
                    + (",('u2','untouched',9)" if occupied else ""),
                    "read": "SELECT id,status,amount FROM usage_records ORDER BY id",
                },
                {
                    "name": "archived_records",
                    "create": "CREATE TABLE archived_records(archive_id text PRIMARY KEY,id text,archive_status text,archive_amount integer)",
                    "seed": "INSERT INTO archived_records(archive_id,id,archive_status,archive_amount) VALUES ('u1','u1','archived',15),('u3','u3','inserted',30)",
                    "read": "SELECT archive_id,id,archive_status,archive_amount FROM archived_records ORDER BY archive_id",
                },
                {
                    "name": "source_records",
                    "create": "CREATE TABLE source_records(id text PRIMARY KEY,status text)",
                    "seed": "INSERT INTO source_records(id,status) VALUES ('u1','source_old'),('u3','source_new')",
                    "read": "SELECT id,status FROM source_records ORDER BY id",
                },
            ],
        }
        for name, occupied in cases
    ]


IDS = (
    "sql-0011",
    "sql-0014",
    "sql-0570",
    "sql-0580",
    "sql-0586",
    "sql-0591",
    "sql-0595",
)


def capture(cursor):
    return {
        "columns": [column.name for column in cursor.description]
        if cursor.description
        else [],
        "column_oids": [column.type_code for column in cursor.description]
        if cursor.description
        else [],
        "rows": [[encoded(cell) for cell in row] for row in cursor.fetchall()]
        if cursor.description
        else [],
    }


def reference():
    import psycopg

    originals = {
        case["id"]: case
        for case in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        for identity in IDS:
            original = originals[identity]
            if original["params"]:
                raise ValueError(
                    "source mutation profile must bind original parameters"
                )
            for profile in profiles(identity):
                with db.transaction(force_rollback=True):
                    for table in profile["tables"]:
                        db.execute(table["create"])
                        db.execute(table["seed"])
                    command_tag = "ERROR"
                    code, affected, output = (
                        None,
                        0,
                        {"columns": [], "column_oids": [], "rows": []},
                    )
                    try:
                        with db.transaction(), psycopg.RawCursor(db) as cursor:
                            cursor.execute(original["sql"])
                            affected = cursor.rowcount
                            output = capture(cursor)
                            if not cursor.statusmessage:
                                raise ValueError(
                                    "missing PostgreSQL terminal command tag"
                                )
                            command_tag = cursor.statusmessage.split()[0]
                    except psycopg.Error as error:
                        code = error.sqlstate
                    tables = []
                    for table in profile["tables"]:
                        with db.execute(table["read"]) as cursor:
                            tables.append({**table, "final": capture(cursor)})
                    if code is None and affected <= 0:
                        raise ValueError(
                            f"{identity}: source witness does not exercise a mutation"
                        )
                    entries.append(
                        {
                            "id": identity,
                            "profile": profile["name"],
                            "sqlstate": code,
                            "command_tag": command_tag,
                            "affected": affected,
                            "output": output,
                            "tables": tables,
                        }
                    )
    return {"format": 1, "reference": "PostgreSQL exact SQL", "entries": entries}


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
            raise SystemExit(
                "source mutation oracle differs from the checked reference"
            )
        print(f"verified {len(result['entries'])} source mutation witnesses")
    elif args.output:
        args.output.write_text(
            json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
        )
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
