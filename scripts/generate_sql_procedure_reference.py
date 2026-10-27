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

"""PostgreSQL witness for procedural declarations, demanded calls and notices."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres

IDS = [
    "sql-0414",
    "sql-0415",
    "sql-0416",
    "sql-0418",
    "sql-0417",
    "sql-0419",
    "sql-0420",
]
SETUP_CALLS = [
    "CALL rotate_usage_notice()",
    "CALL rotate_usage_perform()",
    "CALL rotate_usage_perform_arg()",
    "CREATE FUNCTION rotate_usage_now() RETURNS integer LANGUAGE sql AS $$SELECT 1/0$$",
    "CALL rotate_usage_perform()",
    "CREATE OR REPLACE FUNCTION rotate_usage_now() RETURNS integer LANGUAGE sql AS $$SELECT 1$$",
    "CREATE FUNCTION rotate_usage_now(integer) RETURNS integer LANGUAGE sql AS $$SELECT 10/$1$$",
    "CALL rotate_usage_perform()",
    "CALL rotate_usage_perform_arg()",
    "CREATE PROCEDURE notice_failure() LANGUAGE plpgsql AS $$BEGIN RAISE NOTICE 'before failure'; PERFORM 1/0; END$$",
    "CALL notice_failure()",
    "CREATE PROCEDURE return_early() LANGUAGE plpgsql AS $$BEGIN RETURN; PERFORM nonexistent_helper(); END$$",
    "CALL return_early()",
    "CREATE PROCEDURE typed_check(v numeric) LANGUAGE plpgsql AS $$BEGIN PERFORM 1/(CASE WHEN v+$1=14.50 THEN 1 ELSE 0 END); END$$",
    "CALL typed_check(7.25)",
    "CREATE FUNCTION rotate_usage() RETURNS integer LANGUAGE sql AS $$SELECT 1$$",
    "CREATE OR REPLACE FUNCTION rotate_usage() RETURNS integer LANGUAGE sql AS $$SELECT 1$$",
    "SELECT rotate_usage()",
    "CALL rotate_usage_now()",
]
COLD_CALLS = [
    "CALL rotate_usage()",
    "CALL rotate_usage_notice()",
    "CALL rotate_usage_perform()",
    "CALL rotate_usage_perform_arg()",
    "CALL return_early()",
    "CALL typed_check(7.25)",
    "CALL notice_failure()",
]


def reference():
    from psycopg import Error

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    cases = {entry["id"]: entry for entry in inventory["entries"]}
    notices = []
    with postgres() as db:
        db.add_notice_handler(
            lambda diagnostic: notices.append(
                {"code": diagnostic.sqlstate, "message": diagnostic.message_primary}
            )
        )

        def execute(sql):
            notices.clear()
            result = {"sql": sql}
            try:
                cursor = db.execute(sql)
                result["command_tag"] = cursor.statusmessage
            except Error as exc:
                result["sqlstate"] = exc.sqlstate
            result["notices"] = list(notices)
            return result

        entries = []
        for identity in IDS[:5]:
            case = cases[identity]
            result = execute(case["sql"])
            assert "sqlstate" not in result, result
            result.update({key: case[key] for key in ("id", "source_entry_sha256")})
            entries.append(result)
        calls = [execute(sql) for sql in SETUP_CALLS]
        cold = [execute(sql) for sql in COLD_CALLS]
        teardown = []
        result = execute(cases["sql-0419"]["sql"])
        result["id"] = "sql-0419"
        teardown.append(result)
        teardown.append(execute("CALL rotate_usage()"))
        teardown.append(
            execute(
                "CREATE PROCEDURE rotate_usage(text) LANGUAGE plpgsql AS $$BEGIN NULL; END$$"
            )
        )
        result = execute(cases["sql-0420"]["sql"])
        result["id"] = "sql-0420"
        teardown.append(result)
        teardown.append(execute("CALL rotate_usage('missing')"))
        teardown.append(execute(cases["sql-0420"]["sql"]))
    return {
        "format": 1,
        "reference": "PostgreSQL 18 procedural routines and notices",
        "entries": entries,
        "calls": calls,
        "cold": cold,
        "teardown": teardown,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = reference()
    if args.check:
        assert json.loads(args.output.read_text()) == value, "procedure reference drift"
    else:
        args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
