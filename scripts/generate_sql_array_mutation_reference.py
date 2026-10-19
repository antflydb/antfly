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

"""Exact typed-array DML in explicit unconstrained and primary-owner domains.

Arrays retain binary element OIDs, dimensions, bounds and SQL NULL provenance.
Every original starts at a savepoint and records its complete native postimage;
no generic mutation adapter is permitted to guess arrays from JSON lists.
"""

import argparse
from copy import deepcopy
import json
from pathlib import Path
from uuid import UUID

from generate_sql_postgres_reference import (
    FIXTURES,
    create_table,
    cursor_result,
    mutation_profile,
    postgres,
    properties,
)

INSERT_IDS = {1436, 1437, 1445, 1447}


def profile(primary):
    result = mutation_profile()
    result["primary_key"] = ["id"] if primary else []
    result["additional_tables"] = []
    result["unique"] = []
    result["description"] = (
        "Explicit id PRIMARY KEY" if primary else "Explicit unconstrained table"
    ) + "; native stored text arrays with non-default bounds, NULL cells and arrays."
    properties(result["schema"])["tags"] = {
        "type": "sql_array",
        "x-antfly-sql-type": "text",
        "nullable": True,
    }
    for i, row in enumerate(result["rows"]):
        cells = [["hot", None], [], ["cold", "old", "extra", "last"], None][i]
        row["value"]["tags"] = (
            None
            if cells is None
            else {
                "dimensions": [{"length": len(cells), "lower_bound": -2 + i}]
                if cells
                else [],
                "values": cells,
                "sql_nulls": [value is None for value in cells],
            }
        )
    # OFFSET must consume a real ordered population, not turn INSERT SELECT
    # into a zero-effect claim. Logical IDs remain unique in the owner profile.
    for i in range(8):
        row = deepcopy(result["rows"][2])
        row["key"] = f"ready-{i}"
        row["value"]["id"] = f"ready-{i}"
        row["value"]["email"] = f"ready-{i}@example.test"
        row["value"]["amount"] = 30 + i
        result["rows"].append(row)
    return result


def reference():
    import psycopg

    originals = {
        case["id"]: case
        for case in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    campaigns = []
    uuid_contract = None
    with postgres() as db:
        for primary in (False, True):
            source = profile(primary)
            with db.transaction(force_rollback=True):
                columns = properties(source["schema"])
                create_table(db, "usage_records", columns, source["rows"])
                if primary:
                    db.execute("ALTER TABLE usage_records ADD PRIMARY KEY(id)")
                query = (
                    "SELECT " + ",".join(columns) + " FROM usage_records ORDER BY id"
                )

                def snapshot():
                    with psycopg.RawCursor(db) as cursor:
                        return cursor_result(
                            cursor,
                            {"id": "snapshot", "sql": query, "params": []},
                            read=True,
                        )

                before = snapshot()
                ready = db.execute(
                    "SELECT count(*) FROM usage_records WHERE status='ready'"
                ).fetchone()[0]
                if ready <= 2 + 5:
                    raise ValueError(
                        "ordered insertion needs rows beyond OFFSET plus LIMIT"
                    )
                entries = []
                numbers = sorted(
                    INSERT_IDS | ({1442, 1470, 1502} if primary else set())
                )
                for number in numbers:
                    case = originals[f"sql-{number:04}"]
                    expected_error = (
                        "23505"
                        if primary and number in INSERT_IDS
                        else "42702"
                        if number in {1442, 1470}
                        else None
                    )
                    try:
                        with db.transaction(force_rollback=True):
                            with psycopg.RawCursor(db) as cursor:
                                outcome = cursor_result(cursor, case, read=False)
                            if expected_error:
                                raise ValueError(
                                    "expected array DML rejection succeeded"
                                )
                            final = snapshot()
                    except psycopg.Error as error:
                        if error.sqlstate != expected_error:
                            raise
                        outcome = {
                            "id": case["id"],
                            "command_tag": "",
                            "affected": 0,
                            "columns": [],
                            "column_oids": [],
                            "rows": [],
                            "sql_nulls": [],
                            "sqlstate": expected_error,
                        }
                        final = before
                    else:
                        outcome["command_tag"] = case["sql"].split()[0]
                    outcome["final_tables"] = {"usage_records": final}
                    entries.append(outcome)
                campaigns.append(
                    {
                        "name": "primary-owner" if primary else "unconstrained",
                        "reference": {"profile": source, "entries": entries},
                    }
                )
                if primary:
                    original = originals["sql-1503"]
                    with db.transaction(force_rollback=True):
                        with psycopg.RawCursor(db) as cursor:
                            outcome = cursor_result(cursor, original, read=False)
                        value = UUID(outcome["rows"][0][0])
                        if (
                            value.version != 4
                            or value.variant != "specified in RFC 4122"
                        ):
                            raise ValueError("UUID producer lost its version/variant")
                        after = snapshot()
                        request = before["columns"].index("request_id")
                        identity = before["columns"].index("id")
                        changed = 0
                        for old, new in zip(before["rows"], after["rows"], strict=True):
                            if old[identity] == "u1":
                                if new[request] != str(value):
                                    raise ValueError(
                                        "RETURNING differs from stored UUID"
                                    )
                                new[request] = old[request]
                                changed += 1
                            if old != new:
                                raise ValueError("UUID mutation changed another field")
                        if changed != 1 or outcome["affected"] != 1:
                            raise ValueError(
                                "UUID mutation did not touch exactly its owner"
                            )
                        uuid_contract = {
                            "id": original["id"],
                            "columns": outcome["columns"],
                            "column_oids": outcome["column_oids"],
                            "affected": 1,
                            "target_id": "u1",
                            "target_key": "a",
                            "storage_column": "request_id",
                            "version": 4,
                            "variant": "RFC4122",
                        }
        assignment_contracts = []
        with db.transaction(force_rollback=True):
            db.execute("CREATE TABLE text_assignments(value TEXT)")
            for expression in (
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
                "42::bigint",
                "9007199254740993.1250::numeric",
                "true",
                "'[-2:0]={one,NULL,two}'::text[]",
                '\'{"bb":2,"a":1}\'::jsonb',
                "'null'::jsonb",
                "NULL::numeric",
            ):
                value = db.execute(
                    f"INSERT INTO text_assignments(value) VALUES ({expression}) RETURNING value"
                ).fetchone()[0]
                assignment_contracts.append(
                    {"expression": expression, "text": value, "sql_null": value is None}
                )
            db.execute("CREATE TABLE integer_assignments(value BIGINT)")
            try:
                with db.transaction(force_rollback=True):
                    db.execute("INSERT INTO integer_assignments VALUES ('42'::text)")
            except psycopg.Error as error:
                if error.sqlstate != "42804":
                    raise
            else:
                raise ValueError("typed text unexpectedly gained numeric assignment")
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL, binary typed arrays",
        "campaigns": campaigns,
        "uuid_contract": uuid_contract,
        "assignment_contracts": assignment_contracts,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("typed-array mutation PostgreSQL drift")
        print("verified 11 typed-array mutations and one UUID postimage invariant")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
