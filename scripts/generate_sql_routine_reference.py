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

"""PostgreSQL witness for unchanged SQL-language routine declarations and calls."""

import argparse
import json
from pathlib import Path

from generate_sql_document_alias_reference import output
from generate_sql_postgres_reference import FIXTURES, postgres

QUERIES = [
    "SELECT normalize_status('ReAdY') AS value, normalize_status(NULL) AS absent",
    "SELECT second_status('ignored','Ready') AS value, second_status('ignored',NULL) AS absent",
    "SELECT status_label('Ready','Now') AS value, status_label(NULL,NULL) AS absent",
    "SELECT status_label_nested('ReAdY',NULL) AS value, status_label_nested(NULL,'Now') AS absent",
    "SELECT clamp_amount(10.25,0.5,5.75)::text AS value, clamp_amount(NULL,0.5,5.75)::text AS absent",
    "SELECT add_amounts(9007199254740993.0001,0.0002)::text AS value, add_amounts(NULL,1)::text AS absent",
    None,
    "SELECT normalize_named_status('ReAdY') AS value, normalize_named_status(NULL) AS absent",
]


def reference():
    from psycopg import Error

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    cases = {entry["id"]: entry for entry in inventory["entries"]}
    entries = []
    with postgres() as db:
        for number, query in zip(range(397, 405), QUERIES, strict=True):
            case = cases[f"sql-{number:04d}"]
            entry = {key: case[key] for key in ("id", "source_entry_sha256", "sql")}
            try:
                command = db.execute(case["sql"])
                entry["command_tag"] = command.statusmessage
            except Error as exc:
                entry["sqlstate"] = getattr(exc, "sqlstate", None)
                assert entry["sqlstate"] == "42703", exc
            if query:
                entry["query"] = query
                entry["output"] = output(db.execute(query))
            entries.append(entry)
        probes = []
        for sql in [
            "CREATE FUNCTION normalize_status(text) RETURNS text LANGUAGE sql AS $$SELECT lower($1)$$",
            "CREATE OR REPLACE FUNCTION normalize_status(text) RETURNS integer LANGUAGE sql AS $$SELECT 1$$",
            "CREATE OR REPLACE FUNCTION normalize_named_status(other_name text) RETURNS text LANGUAGE sql AS $$SELECT lower(other_name)$$",
            "CREATE FUNCTION missing_argument(text) RETURNS text LANGUAGE sql AS $$SELECT $2$$",
            "SELECT normalize_status()",
            "SELECT shift_amount(1)",
        ]:
            try:
                db.execute(sql)
                raise AssertionError(f"negative probe accepted: {sql}")
            except Error as exc:
                code = getattr(exc, "sqlstate", None)
                assert code is not None, exc
                probes.append({"sql": sql, "sqlstate": code})
        db.execute(
            "CREATE FUNCTION normalize_status(numeric) RETURNS numeric LANGUAGE sql AS $$SELECT $1+1$$"
        )
        overload = "SELECT normalize_status('ReAdY') AS value, normalize_status(2.25)::text AS amount"
        overloaded = output(db.execute(overload))
        db.execute(
            "CREATE OR REPLACE FUNCTION normalize_status(text) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT upper($1)$$"
        )
        replaced = output(db.execute(QUERIES[0]))
    return {
        "format": 1,
        "reference": "PostgreSQL 18 SQL-language routines",
        "entries": entries,
        "probes": probes,
        "overload": {"sql": overload, "output": overloaded},
        "replaced": replaced,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = reference()
    if args.check:
        assert json.loads(args.output.read_text()) == value, "routine reference drift"
    else:
        args.output.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
