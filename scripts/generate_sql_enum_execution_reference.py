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
        "'z'::execution_state",
        "CAST('a' AS public.execution_state)",
        "(v::text)::execution_state",
        "CASE WHEN false THEN ('invalid'::text)::execution_state ELSE 'z'::execution_state END",
        "CASE WHEN true THEN 'z'::execution_state ELSE 'a' END",
        "CASE WHEN false THEN 'invalid'::execution_state ELSE v END",
        "'invalid'::execution_state",
        "(v::text) = 'z'::execution_state",
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
        # A cast retains its input label in PostgreSQL. Bare ORDER BY resolves
        # that output label first; qualification keeps the enum input domain.
        db.execute(
            "CREATE TYPE epoch_order_state AS ENUM ('queued','processing','done','archived')"
        )
        db.execute("CREATE TABLE enum_epoch_order(state epoch_order_state)")
        db.execute("INSERT INTO enum_epoch_order VALUES ('queued'),('archived')")
        ordering = []
        for projection, order_by in (
            ("state::text", "state"),
            ("e.state::text", "e.state"),
            ("e.state", "e.state"),
        ):
            ordering.append(
                {
                    "projection": projection,
                    "order_by": order_by,
                    "rows": [
                        row[0]
                        for row in db.execute(
                            f"SELECT {projection} FROM enum_epoch_order AS e ORDER BY {order_by}"
                        ).fetchall()
                    ],
                }
            )
        db.execute(
            "ALTER TYPE epoch_order_state ADD VALUE 'started' BEFORE 'processing'"
        )
        db.execute("INSERT INTO enum_epoch_order VALUES ('started')")
        positioned_ordering = [
            row[0]
            for row in db.execute(
                "SELECT e.state FROM enum_epoch_order AS e ORDER BY e.state"
            ).fetchall()
        ]
    with postgres() as db:
        notices = []
        db.add_notice_handler(
            lambda d: notices.append({"code": d.sqlstate, "message": d.message_primary})
        )
        db.execute("CREATE TYPE usage_status AS ENUM ('queued','done')")
        db.execute(
            "CREATE TABLE retirement_parent (id UUID PRIMARY KEY, state usage_status UNIQUE, payload TEXT, seq BIGINT GENERATED ALWAYS AS IDENTITY)"
        )
        db.execute(
            "CREATE TABLE retirement_child (id UUID PRIMARY KEY, state usage_status REFERENCES retirement_parent(state), payload TEXT)"
        )
        for table in ("retirement_parent", "retirement_child"):
            db.execute(
                f"INSERT INTO {table}(id,state,payload) VALUES('00000000-0000-0000-0000-000000000001','queued','retained')"
            )
        db.execute("DROP TYPE IF EXISTS usage_status CASCADE")
        retirement = {"notices": list(notices), "rows": {}, "columns": {}}
        for table in ("retirement_parent", "retirement_child"):
            retirement["columns"][table] = [
                row[0]
                for row in db.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
                    (table,),
                )
            ]
            retirement["rows"][table] = [
                row[0]
                for row in db.execute(f"SELECT payload FROM {table} ORDER BY payload")
            ]
            db.execute(
                f"INSERT INTO {table}(id,payload) VALUES('00000000-0000-0000-0000-000000000002','new')"
            )
        retirement["sequence_values"] = [
            row[0]
            for row in db.execute("SELECT seq FROM retirement_parent ORDER BY seq")
        ]
    return {
        "format": 1,
        "reference": "PostgreSQL 18+ enum scalar execution",
        "entries": entries,
        "ordering": ordering,
        "positioned_ordering": positioned_ordering,
        "retirement": retirement,
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
