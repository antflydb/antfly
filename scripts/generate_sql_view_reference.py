#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exact original queries over a real PostgreSQL document-projection view.

This profile replaces the private index-JSON view mapping with public SQL DDL.
Original queries are never rewritten. Native full_text_search extensions and
malformed legacy mapping metadata require their own admission/search campaigns.
"""

import argparse
import json
from pathlib import Path

import psycopg
from generate_sql_postgres_reference import postgres, cursor_result

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "zig/pkg/antfly-embedded/src/sql/fixtures"
IDS = [
    *(f"sql-{n:04d}" for n in range(797, 824) if n not in {801, 814}),
    "sql-0836",
    "sql-0837",
    "sql-0857",
    "sql-0866",
    *(f"sql-{n:04d}" for n in range(872, 881)),
]
SCHEMA = {
    "version": 1,
    "storage_mode": "document",
    "default_type": "doc",
    "document_schemas": {
        "doc": {
            "schema": {
                "type": "object",
                "properties": {
                    "title": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "metadata": {"type": "object"},
                    "metrics": {"type": "object"},
                    "flags": {"type": "object"},
                    "tags": {"type": "array", "nullable": True},
                    "note": {"type": "keyword", "nullable": True},
                },
                "additionalProperties": True,
            }
        }
    },
}
SEEDS = [
    {
        "key": "doc:a",
        "value": {
            "title": "alpha",
            "status": "active",
            "metadata": {"plan": "pro", "region": "west", "billing": {}, "flags": []},
            "metrics": {"score": 60},
            "flags": {"published": True},
            "tags": ["urgent", None, "vip"],
            "note": "first",
        },
    },
    {
        "key": "doc:b",
        "value": {
            "title": "beta",
            "status": "active",
            "metadata": {"plan": "pro", "region": "west-coast"},
            "metrics": {"score": 20},
            "flags": {"published": False},
            "tags": ["vip", "stale"],
            "note": "second",
        },
    },
    {
        "key": "doc:c",
        "value": {
            "title": "gamma",
            "status": "active",
            "metadata": {"plan": "free", "region": "east"},
            "metrics": {"score": 15},
            "flags": {"published": True},
            "tags": [],
            "note": None,
        },
    },
    {
        "key": "doc:d",
        "value": {
            "title": "delta",
            "status": "draft",
            "metadata": {"plan": "team", "region": None},
            "metrics": {"score": 10},
            "flags": {"published": False},
            "tags": None,
            "note": None,
        },
    },
    {
        "key": "doc:e",
        "value": {
            "title": "epsilon",
            "status": "active",
            "metadata": {},
            "metrics": {},
            "flags": {"published": False},
            "tags": ["archived"],
            "note": "other",
        },
    },
    {
        "key": "doc:f",
        "value": {
            "title": "zeta",
            "status": "active",
            "metadata": {"plan": "pro", "region": "internal"},
            "metrics": {},
            "flags": {"published": True},
            "tags": ["urgent"],
            "note": "last",
        },
    },
]
VIEW = (
    "CREATE VIEW support_view AS SELECT d._id,r.title AS title_text,r.status,"
    "r.metadata->>'plan' AS plan,r.metadata->>'region' AS region,"
    "(r.metrics->>'score')::numeric AS score,"
    "(r.flags->>'published')::boolean AS published,r.tags AS tag_list,"
    "r.metadata,r.note FROM docs d CROSS JOIN LATERAL "
    "jsonb_to_record(d._doc) AS r(title TEXT,status TEXT,metadata JSONB,"
    "metrics JSONB,flags JSONB,tags TEXT[],note TEXT)"
)


def reference():
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    cases = {case["id"]: case for case in inventory["entries"]}
    entries = []
    with postgres() as db:
        db.execute("CREATE TABLE docs(_id TEXT PRIMARY KEY,_doc JSONB)")
        for seed in SEEDS:
            db.execute(
                "INSERT INTO docs VALUES(%s,%s::jsonb)",
                (seed["key"], json.dumps(seed["value"])),
            )
        db.execute(VIEW)
        for identity in IDS:
            case = cases[identity]
            # Missing/NULL cases are explicit contracts in this profile, not
            # silently excluded when an observer happens to produce no rows.
            with psycopg.RawCursor(db) as cursor:
                entry = cursor_result(
                    cursor, case, read=True, allow_empty=True, exact_numeric=True
                )
            entry["sql"] = case["sql"]
            entries.append(entry)
    return {
        "format": 2,
        "reference": "PostgreSQL exact SQL",
        "profile": {
            "description": "Public stored view over raw document paths and typed SQL arrays",
            "schema": SCHEMA,
            "rows": SEEDS,
            "view_sql": VIEW,
        },
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("stored-view reference differs from PostgreSQL")
        print(f"verified {len(result['entries'])} exact stored-view queries")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
