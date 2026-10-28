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

"""PostgreSQL 18+ enum expression witnesses; original DDL qualification is separate."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import postgres


def reference():
    from psycopg import Error

    expressions = [
        f"v {op} '{label}'"
        for op in ("=", "<>", "<", "<=", ">", ">=")
        for label in ("z", "a", "")
    ]
    expressions += [
        "v",
        "v::text",
        "v = v",
        "v = NULL",
        "v IS NULL",
        "v IS NOT NULL",
        "v IS DISTINCT FROM NULL",
        "v IS NOT DISTINCT FROM 'z'",
        "v IN ('a', 'z')",
        "v NOT IN ('a', '')",
        "v IN ('a', NULL)",
        "coalesce(v, NULL)",
        "coalesce(NULL, v, 'a')",
        "coalesce('a', v)",
        "greatest(v, v)",
        "least(v, v)",
        "greatest(v, 'a')",
        "least('a', v)",
        "nullif(v, 'z')",
        "CASE WHEN true THEN v ELSE NULL END",
        "CASE WHEN true THEN v ELSE 'a' END",
        "CASE WHEN false THEN v ELSE 'a' END",
        "CASE WHEN true THEN 'a' ELSE v END",
        "CASE WHEN false THEN 'a' ELSE v END",
        "false AND v = 'Z'",
        "true OR v = 'Z'",
        "CASE WHEN false THEN 'Z' ELSE v END",
        "coalesce(v, 'Z')",
        "false AND v = 'a'",
        "true OR v = 'a'",
        "v = 'z'::text",
        "v::bigint",
        "v::boolean",
        "v + v",
        "v = 'Z'",
        "coalesce(v, 'z'::text)",
    ]
    entries = []
    with postgres() as db:
        db.execute("CREATE TYPE execution_state AS ENUM ('z','a','')")
        for value in ("z", "a", "", None):
            for expression in expressions:
                sql = (
                    "SELECT "
                    + expression
                    + " FROM (VALUES (%s::execution_state)) AS t(v)"
                )
                entry = {"expression": expression, "input": value}
                try:
                    entry["value"] = db.execute(sql, (value,)).fetchone()[0]
                except Error as exc:
                    entry["sqlstate"] = exc.sqlstate
                entries.append(entry)
    return {
        "format": 1,
        "reference": "PostgreSQL 18+ enum scalar execution",
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = reference()
    if args.check:
        assert json.loads(args.output.read_text()) == value, (
            "enum execution reference drift"
        )
    else:
        args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
