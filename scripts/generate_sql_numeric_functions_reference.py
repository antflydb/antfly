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

"""PostgreSQL oracle for shared typed numeric functions; no native expectations."""

import argparse
import json
from pathlib import Path

import psycopg

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_numeric_functions_reference.json"
)
FUNCTIONS = ("abs", "ceil", "floor", "round", "trunc", "sign", "sqrt")
NUMERICS = (
    None,
    "0",
    "-0.000",
    "2.5",
    "-2.5",
    "1.255",
    "9007199254740993.5",
    "NaN",
    "Infinity",
    "-Infinity",
    "1e100",
)
FLOATS = (None, "0", "-0", "2.5", "-2.5", "NaN", "Infinity", "-Infinity", "1e100")
SCALES = (-3, 0, 2, 6)
STRICT = (
    "round(NULL::numeric, (1 / $1)::integer)",
    "trunc(NULL::numeric, (1 / $1)::integer)",
    "mod(NULL::numeric, (1 / $1)::numeric)",
    "mod((1 / $1)::numeric, NULL::numeric)",
)
CAST_PRECEDENCE = (
    "-32768::smallint",
    "(-32768)::smallint",
    "-2147483648::integer",
    "(-2147483648)::integer",
    "-9223372036854775808::bigint",
    "(-9223372036854775808)::bigint",
    "-0.0::double precision",
    "+0.0::double precision",
    "(-0.0)::double precision",
    "CAST(-0.0 AS double precision)",
    "(-0.0::numeric)::double precision",
)


def reference(db):
    entries = []
    cases = [
        (domain, function, value, scale)
        for domain, values in (("numeric", NUMERICS), ("float64", FLOATS))
        for function in FUNCTIONS
        for value in values
        for scale in (
            SCALES
            if domain == "numeric" and function in ("round", "trunc")
            else (None,)
        )
    ]
    cases += [
        ("numeric", function, "1.25", scale)
        for function in ("round", "trunc")
        for scale in (-2147483648, 2147483647)
    ]
    for domain, function, value, scale in cases:
        typename = "numeric" if domain == "numeric" else "double precision"
        call = f"{function}($1::{typename}" + (
            f", ({scale})::integer)" if scale is not None else ")"
        )
        projection = (
            f"({call})::text"
            if domain == "numeric"
            else (f"encode(float8send({call}), 'hex')")
        )
        entry = {
            "domain": domain,
            "function": function,
            "input": value,
            "scale": scale,
            "sql": call,
            "expected": None,
            "sqlstate": None,
        }
        try:
            entry["expected"] = db.execute(
                "SELECT " + projection.replace("$1", "%s"), (value,)
            ).fetchone()[0]
        except psycopg.Error as error:
            entry["sqlstate"] = error.sqlstate
        entries.append(entry)
    strict_errors = []
    for sql in STRICT:
        try:
            db.execute("SELECT " + sql.replace("$1", "%s"), (0,)).fetchone()
        except psycopg.Error as error:
            strict_errors.append({"sql": sql, "sqlstate": error.sqlstate})
        else:
            raise AssertionError("selected argument failure was hidden")
    cast_precedence = []
    for sql in CAST_PRECEDENCE:
        entry = {"sql": sql, "expected": None, "sqlstate": None}
        try:
            entry["expected"] = db.execute("SELECT (" + sql + ")::text").fetchone()[0]
        except psycopg.Error as error:
            entry["sqlstate"] = error.sqlstate
        cast_precedence.append(entry)
    return {
        "format": 1,
        "oracle_major": db.info.server_version // 10000,
        "reference": "PostgreSQL typed numeric function values and error timing",
        "entries": entries,
        "strict_errors": strict_errors,
        "cast_precedence": cast_precedence,
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
            raise AssertionError("PostgreSQL numeric-function reference drift")
        print(
            f"Verified {len(result['entries'])} PostgreSQL numeric-function contracts"
        )
    elif args.write:
        FIXTURE.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
