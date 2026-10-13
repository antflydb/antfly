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

"""Independent PostgreSQL generated-expression planning and stored-row oracle.

Expected admission, SQLSTATE and row values are obtained only from PostgreSQL.
These component contracts do not change original-case inventory dispositions.
"""

import argparse
import json
from pathlib import Path

import psycopg

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_generated_expression_reference.json"
)
CASES = (
    ("text", "concat(s, 'x')"),
    ("text", "concat_ws('-', s, 'x')"),
    ("timestamptz", "now()"),
    ("timestamptz", "transaction_timestamp()"),
    ("timestamptz", "CURRENT_TIMESTAMP(6)"),
    ("timestamptz", "s::timestamptz"),
    ("text", "t::text"),
    ("jsonb", "to_jsonb(s)"),
    ("jsonb", "jsonb_build_object('s', s)"),
    ("text", "CASE WHEN false THEN concat(s, 'x') ELSE 'ok' END"),
    ("text", "coalesce('ok', concat(s, 'x'))"),
    ("text", "coalesce(NULL::text, 'ok', concat(s, 'x'))"),
    ("text", "coalesce(s, 'ok', concat(s, 'x'))"),
    (
        "text",
        "CASE WHEN s IS NULL THEN 'ok' WHEN true THEN 'yes' ELSE concat(s, 'x') END",
    ),
    ("boolean", "s IS NULL OR concat(s, 'x') IS NULL OR true"),
    ("boolean", "s IS NULL AND concat(s, 'x') IS NULL AND false"),
    ("boolean", "true OR (1 / 0 = 0)"),
    ("boolean", "false AND (1 / 0 = 0)"),
    ("boolean", "(1 / 0 = 0) OR true"),
    ("boolean", "(1 / 0 = 0) AND false"),
    ("int", "1 / 0"),
    ("int", "CASE WHEN s IS NULL THEN 1 ELSE 1 / 0 END"),
    ("int", "CASE WHEN false THEN 1 / 0 ELSE 1 END"),
    ("int", "coalesce(1, 1 / 0)"),
    ("text", "CASE WHEN false THEN missing ELSE 'ok' END"),
    ("text", "CASE WHEN false THEN g ELSE 'ok' END"),
    ("text", "lower(s)"),
    ("text", "s || '-x'"),
    ("int", "CASE WHEN n IS NULL THEN 7 ELSE n + 2 END"),
    ("int", "coalesce(n, 7, 1 / 0)"),
    ("boolean", "n IS NULL OR false"),
    ("boolean", "n IS NOT NULL AND true"),
    ("boolean", "NULL::boolean OR n IS NULL"),
    ("boolean", "NULL::boolean AND n IS NULL"),
    ("text", "replace(s, 'A', 'x')"),
    ("text", "md5(s)"),
    ("text", "md5(replace(s, 'A', 'x'))"),
    ("text", "replace(s, lower(s), upper(s))"),
    ("text", "replace(s, '', 'x')"),
    ("text", "replace(s, 'A', '')"),
    ("text", "coalesce(md5(s), 'missing')"),
    ("boolean", "md5(s) IS NULL"),
)
ROWS = ((None, None), ("", 0), ("ALPHA", 3))


def reference(db):
    entries = []
    for kind, expression in CASES:
        sql = f"CREATE TABLE generated_probe (s text, n int, t timestamptz, g {kind} GENERATED ALWAYS AS ({expression}) STORED)"
        entry = {"sql": sql, "type": kind, "expression": expression}
        try:
            with db.transaction(force_rollback=True):
                db.execute(sql)
                for s, n in ROWS:
                    db.execute(
                        "INSERT INTO generated_probe(s,n,t) VALUES (%s,%s,'2026-01-01 00:00:00+00')",
                        (s, n),
                    )
                entry["rows"] = [
                    row[0]
                    for row in db.execute(
                        "SELECT g::text FROM generated_probe ORDER BY n NULLS FIRST"
                    ).fetchall()
                ]
                entry["sqlstate"] = None
        except psycopg.Error as error:
            entry["sqlstate"] = error.sqlstate
        if (
            db.execute("SELECT to_regclass('public.generated_probe')").fetchone()[0]
            is not None
        ):
            raise AssertionError("oracle case escaped its rollback boundary")
        entries.append(entry)
    return {
        "format": 1,
        "oracle_major": db.info.server_version // 10000,
        "timezone": "UTC",
        "reference": "PostgreSQL generated expression planning and complete stored-row results",
        "inputs": [{"s": s, "n": n} for s, n in ROWS],
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    args = parser.parse_args()
    with postgres() as db:
        result = reference(db)
    if args.check:
        if result != json.loads(FIXTURE.read_text()):
            raise AssertionError("PostgreSQL generated-expression reference drift")
        print(
            f"Verified {len(result['entries'])} PostgreSQL generated-expression contracts"
        )
    elif args.write:
        FIXTURE.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
