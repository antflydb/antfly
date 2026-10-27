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

"""Exact PostgreSQL document identity witnesses; native versions are checked separately."""

import argparse
import json
from copy import deepcopy
from pathlib import Path

from generate_sql_document_reference import SEEDS
from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    import psycopg
    from psycopg.types.json import Jsonb

    originals = {
        case["id"]: case
        for case in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    schemas = {
        case["id"]: case["schema"]
        for case in json.loads((FIXTURES / "sql_document_campaign.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        for identity in ("sql-0883", "sql-0927", "sql-0935"):
            for occupied in (False, True):
                seeds = deepcopy(SEEDS if occupied else SEEDS[1:])
                with db.transaction(force_rollback=True):
                    db.execute(
                        "CREATE TABLE docs(_id text PRIMARY KEY,title text,_doc jsonb,_version bigint NOT NULL DEFAULT 42)"
                    )
                    for row in seeds:
                        db.execute(
                            "INSERT INTO docs(_id,title,_doc) VALUES(%s,%s,%s)",
                            (row["key"], row["value"]["title"], Jsonb(row["value"])),
                        )
                    code, returned = None, []
                    columns, oids, affected = [], [], 0
                    try:
                        with db.transaction(), psycopg.RawCursor(db) as cursor:
                            cursor.execute(originals[identity]["sql"])
                            affected = cursor.rowcount
                            if cursor.description:
                                columns = [column.name for column in cursor.description]
                                oids = [
                                    column.type_code for column in cursor.description
                                ]
                                returned = [list(row) for row in cursor.fetchall()]
                    except psycopg.Error as error:
                        code = error.sqlstate
                    final = []
                    for key, value in db.execute(
                        "SELECT _id,_doc FROM docs ORDER BY _id"
                    ):
                        # PostgreSQL does not maintain the native document
                        # envelope for a scalar-column INSERT.
                        if value is None and identity == "sql-0927" and key == "doc:a":
                            value = {"title": "Launch"}
                        final.append({"key": key, "value": value})
                    entries.append(
                        {
                            "id": identity,
                            "occupied": occupied,
                            "schema": schemas[identity],
                            "seeds": seeds,
                            "sqlstate": code,
                            "command_tag": originals[identity]["sql"].split()[0],
                            "affected": affected,
                            "columns": columns,
                            "column_oids": oids,
                            "rows": returned,
                            "final": final,
                        }
                    )
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL; native versions verified independently",
        "seed_version": 42,
        "entries": entries,
    }


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
            raise SystemExit("document identity reference drift")
        print("verified", len(result["entries"]), "exact identity/conflict witnesses")
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
