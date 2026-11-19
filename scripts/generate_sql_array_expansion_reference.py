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

"""Exact PostgreSQL oracle for native typed array-expansion contracts.

These are new shared-operator contracts, not original-inventory case credit.
The PostgreSQL owner helper refuses foreign database connections.
"""

import argparse
from decimal import Decimal
import json
from pathlib import Path
from uuid import UUID

import psycopg

from generate_sql_postgres_reference import FIXTURES, postgres


def normalized(value, oid):
    if value is None:
        return None
    if oid in {20, 21, 23, 1700, 2950}:
        if not isinstance(value, (int, Decimal, UUID)):
            raise ValueError("unexpected exact scalar payload")
        return str(value)
    return value


def generate():
    campaign = json.loads((FIXTURES / "sql_array_expansion_campaign.json").read_text())
    cases = campaign["entries"]
    if campaign["format"] != 1 or not 1 <= len(cases) <= 128:
        raise ValueError("invalid array expansion campaign")
    if len({case["id"] for case in cases}) != len(cases):
        raise ValueError("duplicate array expansion contract")
    result = {"format": 1, "reference": "PostgreSQL exact SQL", "entries": []}
    with postgres() as db:
        db.execute("CREATE TABLE things (_id text, id bigint, tags text[])")
        with db.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO things VALUES (%s,%s,%s)",
                [
                    (
                        str(i),
                        9007199254740993,
                        ["urgent", None, "vip"] if i == 0 else [] if i == 1 else None,
                    )
                    for i in range(3)
                ],
            )
        for case in cases:
            if not 0 < len(case["sql"]) <= 16384:
                raise ValueError("array expansion SQL exceeds budget")
            with db.transaction(force_rollback=True):
                db.execute("SET TRANSACTION READ ONLY")
                with psycopg.RawServerCursor(db, "array_expansion") as cursor:
                    cursor.execute(case["sql"], binary=True)
                    rows = cursor.fetchmany(4097)
                    if len(rows) > 4096:
                        raise ValueError("array expansion rows exceed budget")
                    oids = [column.type_code for column in cursor.description]
                    result["entries"].append(
                        {
                            **case,
                            "columns": [column.name for column in cursor.description],
                            "column_oids": oids,
                            "rows": [
                                [
                                    normalized(cell, oid)
                                    for cell, oid in zip(row, oids, strict=True)
                                ]
                                for row in rows
                            ],
                            "sql_nulls": [
                                [
                                    cursor.pgresult.get_value(i, j) is None
                                    for j in range(len(row))
                                ]
                                for i, row in enumerate(rows)
                            ],
                        }
                    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", type=Path)
    mode.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = generate()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            parser.error("PostgreSQL array expansion reference changed")
        print(
            f"Verified {len(result['entries'])} exact PostgreSQL array expansion contracts"
        )
    else:
        args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")


if __name__ == "__main__":
    main()
