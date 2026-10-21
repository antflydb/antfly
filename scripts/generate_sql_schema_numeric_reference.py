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

"""Verify PostgreSQL NUMERIC schema-expression lowering; no binary-float oracle."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_schema_numeric_reference.json"
)


def cases():
    expressions = (
        "n+i",
        "n-i",
        "n*i",
        "n/i",
        "n%i",
        "mod(n,i)",
        "-n",
        "coalesce(n,i)",
        "CASE WHEN n>i THEN n ELSE i END",
        "CASE WHEN n<i THEN n END",
        "CASE WHEN n>i THEN n ELSE 1/(i-i) END",
        "n=i",
        "n<>i",
        "n>i",
        "n>=i",
        "n<i",
        "n<=i",
        "n IS DISTINCT FROM i",
        "n IS NOT DISTINCT FROM i",
        "n IN (i,n)",
        "n NOT IN (i,NULL)",
        "CAST(n AS bigint)",
        "CAST(n AS smallint)",
        "n/(i-i)",
        "mod(n,i-i)",
    )
    result = [{"sql": sql, "n": "9007199254740993.2500"} for sql in expressions]
    for value in ("2.5000", "NaN", "Infinity", "-Infinity", None):
        for sql in ("coalesce(n,i)", "n IS DISTINCT FROM i", "n IN (i,n)"):
            result.append({"sql": sql, "n": value})
    for sql in (
        "n+f",
        "n>f",
        "n=CAST(f AS real)",
        "n+CAST(f AS real)",
        "n IN (CAST(f AS real),i)",
        "coalesce(n,CAST(f AS real))",
        "CASE WHEN i>0 THEN n ELSE CAST(f AS real) END",
        "CAST(n AS real)",
        "CAST(n AS double precision)",
        "CAST(0.1+0.2 AS double precision)",
        "CAST(9007199254740993.5 AS bigint)",
        "CAST(32767.5 AS smallint)",
        "CAST(0.100000001490116119384765625 AS real)",
        "CAST(1e400 AS double precision)",
        "CAST('not-a-number' AS numeric)",
        "CAST('1e131072' AS numeric)",
        "CASE WHEN i>0 THEN n ELSE CAST('not-a-number' AS numeric) END",
        "CAST(16777217 AS integer) > CAST(16777216 AS real)",
        "CAST(16777217 AS integer) = CAST(16777216 AS real)",
        "CAST(16777217 AS integer) IN (CAST(16777216 AS real))",
        "CAST(16777217 AS integer) IN (CAST(16777216 AS real),NULL)",
        "n IN (CAST(16777218 AS real),i)",
    ):
        result.append({"sql": sql, "n": "16777217.2500"})
    return result


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    output = {"reference": "PostgreSQL 18 NUMERIC schema expressions", "entries": []}
    with postgres() as db:
        for case in cases():
            entry = dict(case)
            sql = case["sql"].replace("%", "%%")
            try:
                row = db.execute(
                    f"SELECT ({sql})::text FROM (SELECT %s::numeric AS n, "
                    "2::bigint AS i, 1.25::double precision AS f) AS inputs",
                    (case["n"],),
                ).fetchone()
                entry["expected"] = row[0]
            except psycopg.Error as error:
                entry["error"] = error.sqlstate
            output["entries"].append(entry)
    if args.generate:
        print(json.dumps(output, indent=2))
    else:
        if output != json.loads(FIXTURE.read_text()):
            raise ValueError("PostgreSQL NUMERIC schema-expression oracle drift")
        print(
            f"Verified {len(output['entries'])} PostgreSQL NUMERIC schema expressions"
        )


if __name__ == "__main__":
    main()
