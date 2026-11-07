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

"""Independent PostgreSQL immutable-index admission and membership contracts."""

import argparse
import json
from pathlib import Path

import psycopg

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_index_planning_reference.json"
)
BASE_SQL = "CREATE TABLE index_probe (s text, n int)"
ROWS = ((None, None), ("", 0), ("ALPHA", 3))
CASES = (
    ("key", "concat(s, 'x')"),
    ("key", "concat_ws('-', s, 'x')"),
    ("key", "lower(s)"),
    ("key", "CASE WHEN false THEN concat(s, 'x') ELSE s END"),
    ("key", "coalesce(s, 'ok', concat(s, 'x'))"),
    ("key", "to_jsonb(s)"),
    ("predicate", "true"),
    ("predicate", "false"),
    ("predicate", "NULL::boolean"),
    ("predicate", "s IS NULL OR concat(s, 'x') IS NULL OR true"),
    ("predicate", "n > 0 AND concat(s, 'x') IS NULL"),
    ("predicate", "CASE WHEN false THEN concat(s, 'x') IS NULL ELSE n > 0 END"),
    ("predicate", "n > 0 AND true"),
    ("predicate", "n > 0 OR false"),
    ("predicate", "true OR (1 / 0 = 0)"),
    ("predicate", "(1 / 0 = 0) OR true"),
    ("predicate", "false AND missing IS NULL"),
    ("predicate", "NOT NULL::boolean"),
    ("predicate", "NOT false"),
    ("predicate", "NOT true"),
    ("key", "replace(s, 'A', 'x')"),
    ("key", "md5(s)"),
    ("key", "md5(replace(s, 'A', 'x'))"),
    ("key", "replace(s, lower(s), upper(s))"),
)


def reference(db):
    entries = []
    for kind, expression in CASES:
        suffix = f"(({expression}))" if kind == "key" else f"(n) WHERE {expression}"
        entry = {
            "kind": kind,
            "expression": expression,
            "sql": f"CREATE INDEX probe_idx ON index_probe {suffix}",
        }
        try:
            with db.transaction(force_rollback=True):
                db.execute(BASE_SQL)
                for s, n in ROWS:
                    db.execute("INSERT INTO index_probe(s,n) VALUES (%s,%s)", (s, n))
                db.execute(entry["sql"])
                query = (
                    f"SELECT ({expression})::text FROM index_probe ORDER BY n NULLS FIRST"
                    if kind == "key"
                    else f"SELECT n::text FROM index_probe WHERE {expression} ORDER BY n NULLS FIRST"
                )
                entry["rows"] = [row[0] for row in db.execute(query)]
                entry["sqlstate"] = None
        except psycopg.Error as error:
            entry["sqlstate"] = error.sqlstate
        if (
            db.execute("SELECT to_regclass('public.index_probe')").fetchone()[0]
            is not None
        ):
            raise AssertionError("index oracle escaped its rollback boundary")
        entries.append(entry)
    return {
        "format": 1,
        "oracle_major": db.info.server_version // 10000,
        "timezone": "UTC",
        "base_sql": BASE_SQL,
        "inputs": [{"s": s, "n": n} for s, n in ROWS],
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true")
    modes.add_argument("--write", action="store_true")
    args = parser.parse_args()
    with postgres() as db:
        result = reference(db)
    if args.check:
        if result != json.loads(FIXTURE.read_text()):
            raise AssertionError("PostgreSQL index-planning reference drift")
        print(f"Verified {len(result['entries'])} PostgreSQL index-planning contracts")
    elif args.write:
        FIXTURE.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
