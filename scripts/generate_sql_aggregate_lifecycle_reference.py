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

"""Native aggregate lifecycle witnesses from independent PostgreSQL execution."""

import argparse
import json
from pathlib import Path

from generate_sql_document_alias_reference import output
from generate_sql_operator_reference import reference as campaign
from generate_sql_postgres_reference import postgres

BEFORE = [
    "CREATE TABLE aggregate_rows(id integer PRIMARY KEY,g text,value text,sort_key integer)",
    "INSERT INTO aggregate_rows VALUES(1,'a',NULL,1),(2,'a','first',2),(3,'b','later',3)",
    "CREATE FUNCTION first_sfunc(text,text) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT coalesce($1,$2)$$",
    "sql-0136",
    "SELECT first_value_text(value ORDER BY sort_key) AS first FROM aggregate_rows",
    "SELECT g,first_value_text(value ORDER BY sort_key) AS first FROM aggregate_rows GROUP BY g ORDER BY g",
    "SELECT first_value_text(value) AS empty FROM aggregate_rows WHERE id<0",
    "SELECT first_value_text(value) AS absent FROM aggregate_rows WHERE value IS NULL",
    "SELECT first_value_text(value ORDER BY sort_key) FILTER(WHERE id>1) AS filtered FROM aggregate_rows",
    "SELECT first_value_text(DISTINCT value ORDER BY value) AS distinct_value FROM aggregate_rows",
    "CREATE FUNCTION uppercase_result(text) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT upper($1)$$",
    "CREATE AGGREGATE upper_first(text)(SFUNC=first_sfunc,STYPE=text,FINALFUNC=uppercase_result)",
    "CREATE AGGREGATE seeded_first(text)(SFUNC=first_sfunc,STYPE=text,INITCOND='seed')",
    "SELECT upper_first(value ORDER BY sort_key) AS finalized,seeded_first(value) AS initialized FROM aggregate_rows",
    "SELECT upper_first(value) AS empty_finalized,seeded_first(value) AS empty_initialized FROM aggregate_rows WHERE id<0",
    "CREATE FUNCTION strict_first(text,text) RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$SELECT coalesce($1,$2)$$",
    "CREATE AGGREGATE strict_first_value(text)(SFUNC=strict_first,STYPE=text)",
    "SELECT strict_first_value(value ORDER BY sort_key) AS strict_value FROM aggregate_rows",
    "CREATE FUNCTION bad_transition(integer,text) RETURNS integer LANGUAGE sql STRICT AS $$SELECT $1$$",
    "CREATE AGGREGATE invalid_first(text)(SFUNC=bad_transition,STYPE=integer)",
    "CREATE FUNCTION first_value_text(text) RETURNS text LANGUAGE sql AS $$SELECT $1$$",
    "sql-0136",
    "DROP FUNCTION first_sfunc(text,text)",
    "DROP FUNCTION uppercase_result(text)",
    "CREATE OR REPLACE FUNCTION first_sfunc(text,text) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT coalesce($2,$1)$$",
    "SELECT first_value_text(value ORDER BY sort_key) AS replaced FROM aggregate_rows",
    "CREATE FUNCTION int_sum(integer,integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT coalesce($1,0)+coalesce($2,0)$$",
    "CREATE AGGREGATE int_total(integer)(SFUNC=int_sum,STYPE=integer,INITCOND='0')",
    "SELECT int_total(id) AS total,int_total(id) FILTER(WHERE id>1) AS filtered FROM aggregate_rows",
    "CREATE FUNCTION int_final(integer) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT $1::text$$",
    "CREATE AGGREGATE shown_total(integer)(SFUNC=int_sum,STYPE=integer,FINALFUNC=int_final,INITCOND='0')",
    "SELECT shown_total(id) AS finalized_integer FROM aggregate_rows",
    "SELECT shown_total(id) AS empty_integer FROM aggregate_rows WHERE id<0",
    "CREATE AGGREGATE invalid_seed(integer)(SFUNC=int_sum,STYPE=integer,INITCOND='bad')",
    "CREATE FUNCTION pair_first(text,text,text) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT coalesce($1,concat($2,':',$3))$$",
    "CREATE AGGREGATE first_pair(text,text)(SFUNC=pair_first,STYPE=text)",
    "SELECT first_pair(g,value ORDER BY sort_key) AS pair_value FROM aggregate_rows",
    "CREATE SCHEMA aggregate_tools",
    "CREATE FUNCTION aggregate_tools.first_text(text,text) RETURNS text LANGUAGE sql AS $$SELECT coalesce($1,$2)$$",
    "CREATE AGGREGATE renamed_first(text)(SFUNC=aggregate_tools.first_text,STYPE=text)",
    "ALTER SCHEMA aggregate_tools RENAME TO aggregate_renamed",
    "SELECT renamed_first(value ORDER BY sort_key) AS renamed FROM aggregate_rows",
]
COLD = [
    "SELECT int_total(id) AS integer_state,shown_total(id) AS integer_final,first_pair(g,value ORDER BY sort_key) AS pair_value FROM aggregate_rows",
    "SELECT g,first_value_text(value ORDER BY sort_key) AS first FROM aggregate_rows GROUP BY g ORDER BY g",
    "SELECT upper_first(value ORDER BY sort_key) AS finalized,seeded_first(value ORDER BY sort_key) AS initialized,strict_first_value(value ORDER BY sort_key) AS strict_value,renamed_first(value ORDER BY sort_key) AS renamed FROM aggregate_rows",
]
AFTER = [
    "sql-0137",
    "SELECT first_value_text(value) AS missing FROM aggregate_rows",
    "sql-0138",
    "sql-0136",
    "DROP FUNCTION uppercase_result(text) CASCADE",
    "SELECT upper_first(value) AS missing_final FROM aggregate_rows",
    "SELECT first_value_text(value ORDER BY sort_key) AS retained FROM aggregate_rows",
    "DROP AGGREGATE seeded_first(text)",
    "DROP FUNCTION first_sfunc(text,text) CASCADE",
    "SELECT first_value_text(value) AS missing_transition FROM aggregate_rows",
    "CREATE FUNCTION first_sfunc(text,text) RETURNS text LANGUAGE sql AS $$SELECT $2$$",
    "SELECT first_value_text(value) AS no_name_retarget FROM aggregate_rows",
    "sql-0136",
    "SELECT first_value_text(value ORDER BY sort_key) AS fresh FROM aggregate_rows",
]


def prepared_reference():
    setup = [
        "CREATE TABLE prepared_aggregate_rows(id integer PRIMARY KEY)",
        "INSERT INTO prepared_aggregate_rows VALUES(1),(2),(3)",
        "CREATE FUNCTION mixed(integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT $1+10$$",
        "CREATE FUNCTION add_big(bigint,bigint) RETURNS bigint LANGUAGE sql IMMUTABLE AS $$SELECT $1+$2$$",
        "CREATE AGGREGATE mixed(bigint)(SFUNC=add_big,STYPE=bigint,INITCOND='0')",
    ]
    statement = "WITH totals AS (SELECT mixed(id::bigint) AS total FROM prepared_aggregate_rows) SELECT id,mixed(id) AS ordinary,total FROM prepared_aggregate_rows CROSS JOIN totals ORDER BY id"
    mutation = "CREATE OR REPLACE FUNCTION add_big(bigint,bigint) RETURNS bigint LANGUAGE sql IMMUTABLE AS $$SELECT $1+$2+1$$"
    with postgres() as db:
        steps = []
        for sql in setup:
            cursor = db.execute(sql)
            steps.append(
                {"sql": sql, "command_tag": cursor.statusmessage, "notices": []}
            )
        db.execute("PREPARE aggregate_overloads AS " + statement)
        before = output(db.execute("EXECUTE aggregate_overloads"))
        cursor = db.execute(mutation)
        mutation_step = {
            "sql": mutation,
            "command_tag": cursor.statusmessage,
            "notices": [],
        }
        after = output(db.execute(statement))
    return {
        "setup": steps,
        "statement": statement,
        "before": before,
        "mutation": mutation_step,
        "after": after,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = campaign(
        BEFORE,
        COLD,
        AFTER,
        ("sql-0136", "sql-0137", "sql-0138"),
        "PostgreSQL 18 user-defined aggregate execution and dependency retirement",
    )
    value["prepared"] = prepared_reference()
    if args.check:
        assert json.loads(args.output.read_text()) == value, "aggregate reference drift"
    else:
        args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
