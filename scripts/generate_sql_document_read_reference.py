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

"""Exact document SQL reads with explicitly declared typed-array columns."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres, cursor_result

IDS = [
    *(f"sql-{n:04}" for n in range(824, 836)),
    "sql-1580",
    "sql-1581",
    "sql-1583",
    "sql-1584",
]
EMPTY_IDS = ["sql-1580", "sql-1581", "sql-1583", "sql-1584"]


def profile():
    properties = {
        "title": {"type": "keyword"},
        "status": {"type": "keyword"},
        "amount": {"type": "integer", "nullable": True},
        "tags": {"type": "array", "nullable": True},
        "labels": {"type": "array", "nullable": True},
    }
    schema = {
        "version": 1,
        "storage_mode": "document",
        "default_type": "doc",
        "document_schemas": {
            "doc": {
                "schema": {
                    "type": "object",
                    "properties": properties,
                    "additionalProperties": True,
                }
            }
        },
    }
    rows = [
        {
            "key": "doc:" + key,
            "value": {
                "title": key,
                "status": "active",
                "amount": amount,
                "tags": tags,
                "labels": labels,
            },
        }
        for key, amount, tags, labels in [
            ("a", 13, ["urgent", "vip", None], ["blue"]),
            ("b", -2, ["stale", "archived"], ["red", "green"]),
            ("c", None, [], ["none"]),
            ("d", 4, None, []),
            ("e", 0, ["abcdefg-hit"], ["rare"]),
            ("f", 7, ["zz"], ["tail"]),
        ]
    ]
    return {
        "description": "Explicit public typed SQL view over raw JSON document arrays; no implicit array coercion or index authority",
        "schema": schema,
        "rows": rows,
        "view_sql": "CREATE VIEW docs AS SELECT d._id,r.title,r.status,r.amount,r.tags,r.labels FROM document_source d CROSS JOIN LATERAL jsonb_to_record(d._doc) AS r(title TEXT,status TEXT,amount BIGINT,tags TEXT[],labels TEXT[])",
    }


def reference():
    import psycopg
    from psycopg.types.json import Jsonb

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    cases = {entry["id"]: entry for entry in inventory["entries"]}
    declared = profile()
    entries, empty = [], []
    with postgres() as db:
        db.execute("CREATE TABLE document_source(_id TEXT PRIMARY KEY,_doc JSONB)")
        for row in declared["rows"]:
            db.execute(
                "INSERT INTO document_source VALUES (%s,%s)",
                (row["key"], Jsonb(row["value"])),
            )
        db.execute(declared["view_sql"])
        for identity in IDS:
            with psycopg.RawCursor(db) as cursor:
                entry = cursor_result(cursor, cases[identity], read=True)
                entry["sql"] = cases[identity]["sql"]
                entries.append(entry)
        db.execute("DELETE FROM document_source")
        for identity in EMPTY_IDS:
            with psycopg.RawCursor(db) as cursor:
                entry = cursor_result(cursor, cases[identity], read=True)
                entry["sql"] = cases[identity]["sql"]
                empty.append(entry)
    return {
        "format": 2,
        "reference": "PostgreSQL exact SQL",
        "profile": declared,
        "entries": entries,
        "empty_reference": {"reference": "PostgreSQL exact SQL", "entries": empty},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("document read reference differs from PostgreSQL")
        print(
            f"verified {len(result['entries'])} document reads and {len(EMPTY_IDS)} empty aggregate contracts"
        )
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
