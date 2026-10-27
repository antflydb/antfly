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

"""PostgreSQL witness for operator publication and identity-bound scalar execution."""

import argparse
import json
from pathlib import Path

from generate_sql_document_alias_reference import output
from generate_sql_postgres_reference import FIXTURES, postgres

BEFORE = [
    "CREATE TABLE operator_rows(id integer PRIMARY KEY,left_value text,right_value text)",
    "INSERT INTO operator_rows VALUES(1,'One','one'),(2,'two','two'),(3,NULL,'three')",
    "CREATE FUNCTION text_eq(text,text) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$SELECT $1=$2$$",
    "sql-0133",
    "SELECT id,left_value===right_value AS same FROM operator_rows ORDER BY id",
    "SELECT 'one' OPERATOR(public.===) 'one' AS same,NULL::text===NULL::text AS absent",
    "CREATE FUNCTION int_ne(integer,integer) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$SELECT $1<>$2$$",
    "CREATE OPERATOR ===(FUNCTION=int_ne,LEFTARG=integer,RIGHTARG=integer)",
    "SELECT 1===2 AS different,1===1 AS same,'x'::text==='x'::text AS text_same",
    'CREATE FUNCTION "==="(text,text) RETURNS boolean LANGUAGE sql AS $$SELECT false$$',
    "SELECT 'x'==='x' AS operator_result,\"===\"('x','x') AS function_result",
    "CREATE OR REPLACE FUNCTION text_eq(text,text) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$SELECT lower($1)=lower($2)$$",
    "SELECT id,left_value===right_value AS same FROM operator_rows ORDER BY id",
    "UPDATE operator_rows SET right_value='changed' WHERE left_value===right_value RETURNING id,right_value",
    "SELECT id,left_value,right_value FROM operator_rows ORDER BY id",
    "sql-0133",
    "DROP FUNCTION text_eq(text,text)",
    "CREATE SCHEMA operator_tools",
    "CREATE FUNCTION operator_tools.equal_text(text,text) RETURNS boolean LANGUAGE sql AS $$SELECT $1=$2$$",
    "CREATE OPERATOR public.===(FUNCTION=text_eq,LEFTARG=text,RIGHTARG=text)",
    "CREATE OPERATOR public.==!(FUNCTION=operator_tools.equal_text,LEFTARG=text,RIGHTARG=text)",
    "ALTER SCHEMA operator_tools RENAME TO operator_renamed",
    "SELECT 'x' OPERATOR(public.==!) 'x' AS same",
]
COLD = [
    "SELECT id,left_value===right_value AS same FROM operator_rows ORDER BY id",
    "SELECT 1===2 AS different,'x'::text==='x'::text AS same,'x' OPERATOR(public.==!) 'x' AS renamed_same",
]
AFTER = [
    "sql-0134",
    "SELECT 'x'::text==='x'::text AS missing",
    "SELECT 1===2 AS retained_overload",
    "sql-0135",
    "CREATE OPERATOR ===(FUNCTION=text_eq,LEFTARG=text,RIGHTARG=text)",
    "DROP FUNCTION text_eq(text,text) CASCADE",
    "SELECT 'x'::text==='x'::text AS missing_after_cascade",
    "CREATE FUNCTION text_eq(text,text) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$SELECT $1<>$2$$",
    "SELECT 'x'::text==='x'::text AS name_reuse_does_not_retarget",
    "CREATE OPERATOR ===(FUNCTION=text_eq,LEFTARG=text,RIGHTARG=text)",
    "SELECT 'x'::text==='x'::text AS fresh_identity",
]


def reference():
    from psycopg import Error

    cases = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    notices = []
    credited = set()
    with postgres() as db:
        db.add_notice_handler(
            lambda item: notices.append(
                {"code": item.sqlstate, "message": item.message_primary}
            )
        )

        def run(item):
            sql = cases[item]["sql"] if item.startswith("sql-") else item
            notices.clear()
            result = {"sql": sql}
            if item.startswith("sql-") and item not in credited:
                credited.add(item)
                result.update(
                    {key: cases[item][key] for key in ("id", "source_entry_sha256")}
                )
            try:
                cursor = db.execute(sql)
                result["command_tag"] = cursor.statusmessage
                if cursor.description:
                    result["output"] = output(cursor)
                    # RETURNING order is unspecified without an explicit sort.
                    if sql.startswith("UPDATE"):
                        ordered = sorted(
                            zip(
                                result["output"]["rows"], result["output"]["sql_nulls"]
                            ),
                            key=lambda pair: pair[0][0],
                        )
                        result["output"]["rows"] = [pair[0] for pair in ordered]
                        result["output"]["sql_nulls"] = [pair[1] for pair in ordered]
            except Error as exc:
                result["sqlstate"] = exc.sqlstate
            result["notices"] = list(notices)
            return result

        before = [run(item) for item in BEFORE]
        cold = [run(item) for item in COLD]
        after = [run(item) for item in AFTER]
    assert credited == {"sql-0133", "sql-0134", "sql-0135"}
    return {
        "format": 1,
        "reference": "PostgreSQL 18 operators and function identity",
        "before": before,
        "cold": cold,
        "after": after,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = reference()
    if args.check:
        assert json.loads(args.output.read_text()) == value, "operator reference drift"
    else:
        args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
