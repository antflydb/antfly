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

"""Verify partial-index constant bounds against disposable PostgreSQL."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_partial_bound_reference.json"
)


def cases():
    groups = {
        "numeric": (
            "CAST(CAST(9007199254740993.5 AS bigint) AS numeric)",
            "CAST(1 AS numeric)",
            "9007199254740993.2500+0.0001",
            "-(9007199254740993.2500)",
            "1.00/8.0",
            "10.50%3.0",
            "COALESCE(NULL::numeric,1.2500)",
            "CASE WHEN true THEN 1.2500 ELSE 1.0/0.0 END",
            "CASE WHEN false THEN 1.0/0.0 ELSE 1.2500 END",
            "CAST(CAST(0.1 AS real) AS numeric)",
            "CAST(CAST(0.1 AS double precision) AS numeric)",
            "CAST('NaN' AS numeric)",
            "CAST('Infinity' AS numeric)",
            "CAST('-Infinity' AS numeric)",
            "CAST(NULL AS numeric)",
            "1e-99+1e-100",
            "1.0/0.0",
            "CAST('not-a-number' AS numeric)",
            "CAST('1e131072' AS numeric)",
        ),
        "int64": (
            "CAST(9007199254740993.5 AS bigint)",
            "CAST(-9007199254740993.5 AS bigint)",
            "CAST(2.5 AS bigint)",
            "CAST(-2.5 AS bigint)",
            "CAST(CAST(2.5 AS double precision) AS bigint)",
            "CAST(9223372036854775807.5 AS bigint)",
            "CAST(-9223372036854775808.5 AS bigint)",
            "CAST(NULL AS bigint)",
            "CAST(2 AS bigint)+CAST(3 AS bigint)",
        ),
        "int16": ("CAST(32767.4 AS smallint)", "CAST(32767.5 AS smallint)"),
        "int32": ("CAST(2147483647.5 AS integer)", "2147483647+1"),
        "float32": (
            "CAST(0.100000001490116119384765625 AS real)",
            "CAST(16777217 AS real)",
            "CAST(1e-45 AS real)",
            "CAST(1e-1000 AS real)",
        ),
        "float64": (
            "CAST(9007199254740993.5 AS double precision)",
            "CAST(0.1+0.2 AS double precision)",
            "CAST(1e400 AS double precision)",
        ),
        "text": ("lower('READY')", "'left'||'right'", "COALESCE(NULL::text,'ready')"),
        "boolean": ("true AND false", "CASE WHEN true THEN true ELSE false END"),
    }
    for identity, expressions in groups.items():
        for expression in expressions:
            yield {"identity": identity, "sql": expression}


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    output = {"reference": "PostgreSQL 18 partial-index constant bounds", "entries": []}
    with postgres() as db:
        for case in cases():
            entry = dict(case)
            try:
                entry["expected"] = db.execute(
                    f"SELECT ({case['sql']})::text"
                ).fetchone()[0]
            except psycopg.Error as error:
                entry["error"] = error.sqlstate
            output["entries"].append(entry)
    if args.generate:
        print(json.dumps(output, indent=2))
    else:
        if output != json.loads(FIXTURE.read_text()):
            raise ValueError("PostgreSQL partial-index constant-bound oracle drift")
        print(
            f"Verified {len(output['entries'])} PostgreSQL partial-index constant bounds"
        )


if __name__ == "__main__":
    main()
