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

"""Bounded exact-SQL mutation reference for the fixed native campaign.

SQLite-compatible contracts only. No SQL rewriting, inferred locking semantics,
or inventory credit. Each statement starts from a fresh named fixture, and the
reference records its complete RETURNING result and complete final table state.
Index, locking, temporal and multi-table profiles need separate native gates.
"""

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3

from generate_sql_parity_read_reference import parameter

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "zig/pkg/antfly-embedded/src/sql/fixtures"
COLUMNS = {
    "id": "keyword",
    "status": "keyword",
    "quantity": "integer",
    "email": "keyword",
    "tenant_id": "keyword",
    "amount": "integer",
    "name": "keyword",
    "organization_id": "keyword",
    "priority": "integer",
    "expires_at": "integer",
    "enabled": "boolean",
    "metadata": "json",
    "tags": "json",
    "request_id": "keyword",
    "updated_at_ns": "integer",
}


def seeds():
    return [
        {
            "key": "a",
            "value": {
                "id": "u1",
                "status": "active",
                "quantity": 2,
                "email": "a@example.test",
                "tenant_id": "t1",
                "amount": 5,
                "name": "Ada",
                "organization_id": "o1",
                "priority": 8,
                "expires_at": 100,
                "enabled": True,
                "metadata": {"source": "api"},
                "tags": ["hot"],
                "request_id": "old",
                "updated_at_ns": 10,
            },
        },
        {
            "key": "b",
            "value": {
                "id": "u2",
                "status": "closed",
                "quantity": 7,
                "email": "b@example.test",
                "tenant_id": "t2",
                "amount": 9,
                "name": "Grace",
                "organization_id": "o2",
                "priority": 3,
                "expires_at": 1100,
                "enabled": False,
                "metadata": {"source": "internal"},
                "tags": [],
                "request_id": "other",
                "updated_at_ns": 20,
            },
        },
    ]


def reference(cases):
    entries, excluded = [], []
    sql_types = {
        "keyword": "TEXT",
        "integer": "INTEGER",
        "boolean": "BOOLEAN",
        "json": "TEXT",
    }
    for case in cases:
        sql = case["sql"]
        if any(
            word in case["name"]
            for word in ("unique selector", "primary key rewrite", "unvalidated check")
        ):
            excluded.append(
                {"id": case["id"], "reason": "constraint/index fixture required"}
            )
            continue
        if "returning scalar subquery" in case["name"]:
            excluded.append(
                {
                    "id": case["id"],
                    "reason": "native pre-statement snapshot oracle required",
                }
            )
            continue
        if "joined_source" in case["family"]:
            excluded.append(
                {
                    "id": case["id"],
                    "reason": "multi-table or recursive native profile required",
                }
            )
            continue
        # These contracts need their real arbiters or source/temporal profiles.
        if "on conflict" in sql.lower():
            excluded.append(
                {"id": case["id"], "reason": "native conflict-owner profile required"}
            )
            continue
        with closing(sqlite3.connect(":memory:")) as db:
            db.execute("ATTACH DATABASE ':memory:' AS public")
            db.execute("PRAGMA case_sensitive_like=ON")
            db.execute(
                "CREATE TABLE public.usage_records ("
                + ",".join(
                    f"{name} {sql_types[kind]}" for name, kind in COLUMNS.items()
                )
                + ")"
            )
            for seed in seeds():
                row = seed["value"]
                db.execute(
                    "INSERT INTO public.usage_records VALUES ("
                    + ",".join("?" for _ in COLUMNS)
                    + ")",
                    [
                        json.dumps(row[name], separators=(",", ":"))
                        if COLUMNS[name] == "json"
                        else row[name]
                        for name in COLUMNS
                    ],
                )
            db.commit()
            db.isolation_level = None

            # Native JSON functions return JSON values; SQLite carries them as
            # encoded JSON text, matching the existing read reference contract.
            def build_object(*args):
                if len(args) % 2:
                    raise ValueError("object requires key/value pairs")
                result = {}
                for key, value in zip(args[::2], args[1::2]):
                    if key is None:
                        raise ValueError("object keys cannot be NULL")
                    result[str(key)] = value
                return json.dumps(result, separators=(",", ":"))

            db.create_function("jsonb_build_object", -1, build_object)
            db.create_function(
                "to_jsonb",
                1,
                lambda value: (
                    None if value is None else json.dumps(value, separators=(",", ":"))
                ),
            )
            allowed = {
                sqlite3.SQLITE_SELECT,
                sqlite3.SQLITE_READ,
                sqlite3.SQLITE_FUNCTION,
                sqlite3.SQLITE_RECURSIVE,
                sqlite3.SQLITE_INSERT,
                sqlite3.SQLITE_UPDATE,
                sqlite3.SQLITE_DELETE,
            }
            db.set_authorizer(
                lambda action, name, _column, database, _trigger: (
                    sqlite3.SQLITE_OK
                    if action in allowed
                    and (
                        action
                        not in {
                            sqlite3.SQLITE_INSERT,
                            sqlite3.SQLITE_UPDATE,
                            sqlite3.SQLITE_DELETE,
                        }
                        or (database == "public" and name == "usage_records")
                    )
                    else sqlite3.SQLITE_DENY
                )
            )
            steps = 0

            def progress():
                nonlocal steps
                steps += 1000
                return steps > 100_000

            db.set_progress_handler(progress, 1000)
            try:
                cursor = db.execute(
                    sql,
                    {
                        str(i): parameter(value)
                        for i, value in enumerate(case["params"], 1)
                    },
                )
                rows = [list(row) for row in cursor.fetchmany(4097)]
                if len(rows) > 4096:
                    raise ValueError("RETURNING exceeds row budget")
                affected = db.execute("SELECT changes()").fetchone()[0]
                if affected == 0:
                    raise ValueError("empty mutation does not exercise this shape")
                final = [
                    list(row)
                    for row in db.execute(
                        "SELECT * FROM public.usage_records ORDER BY id,quantity,status"
                    ).fetchmany(4097)
                ]
                if len(final) > 4096:
                    raise ValueError("final state exceeds row budget")
                entries.append(
                    {
                        "id": case["id"],
                        "columns": [column[0] for column in cursor.description]
                        if cursor.description
                        else [],
                        "rows": rows,
                        "affected": affected,
                        "final": final,
                    }
                )
            except (sqlite3.Error, ValueError, TypeError) as error:
                excluded.append({"id": case["id"], "reason": str(error)})
    schema = {
        "version": 1,
        "storage_mode": "relational",
        "default_type": "row",
        "document_schemas": {
            "row": {
                "schema": {
                    "type": "object",
                    "properties": {
                        name: {"type": kind} for name, kind in COLUMNS.items()
                    },
                    "additionalProperties": False,
                }
            }
        },
    }
    return {
        "format": 1,
        "profile": "unconstrained-point",
        "schema": schema,
        "seeds": seeds(),
        "storage_columns": list(COLUMNS),
        "entries": entries,
        "excluded": excluded,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", type=Path, help="verify an explicit golden subset without writing"
    )
    args = parser.parse_args()
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    manifest = json.loads((FIXTURES / "sql_mutation_campaign.json").read_text())
    ids = {entry["id"] for entry in manifest["entries"]}
    if len(ids) != 235:
        parser.error("campaign requires 235 unique IDs")
    if args.check:
        expected = json.loads(args.check.read_text())
        selected = [entry["id"] for entry in expected["entries"]]
        if (
            not selected
            or len(set(selected)) != len(selected)
            or not set(selected) <= ids
        ):
            parser.error("golden IDs must be unique members of the campaign")
        ids = set(selected)
    result = reference([case for case in inventory if case["id"] in ids])
    if args.check:
        # Exclusions from discovery are not golden implementation assertions.
        expected.pop("excluded", None)
        result.pop("excluded", None)
        if expected != result:
            parser.error("mutation reference drift")
        print(f"Verified {len(result['entries'])} exact mutation reference contracts")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
