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

"""Independent PostgreSQL arithmetic/cache oracle, not SQL DDL activation."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres


def profiles():
    return [
        {
            "name": "creation restart preserves a distinct declared start",
            "create": "CREATE SEQUENCE probe START 100 RESTART WITH 7 CACHE 4",
            "options": {"start": 100, "restart": {"value": 7}, "cache": 4},
            "steps": [
                ("reserve",),
                ("alter", "RESTART", {"restart": {}}),
                ("reserve",),
            ],
        },
        {
            "name": "cached ascending setval and restart",
            "create": "CREATE SEQUENCE probe CACHE 32",
            "options": {"cache": 32},
            "steps": [
                ("reserve",),
                ("reserve",),
                ("set", 100, False),
                ("reserve",),
                ("alter", "START WITH 5", {"start": 5}),
                ("reserve",),
                ("alter", "RESTART", {"restart": {}}),
                ("reserve",),
            ],
        },
        {
            "name": "descending bounded cycling cache",
            "create": "CREATE SEQUENCE probe INCREMENT -3 MINVALUE 1 MAXVALUE 10 CACHE 32 CYCLE",
            "options": {
                "increment": -3,
                "minimum": {"value": 1},
                "maximum": {"value": 10},
                "cache": 32,
                "cycle": True,
            },
            "steps": [("reserve",), ("reserve",)],
        },
        {
            "name": "full signed positive boundary",
            "create": "CREATE SEQUENCE probe START 9223372036854775806 CACHE 4",
            "options": {"start": 9223372036854775806, "cache": 4},
            "steps": [("reserve",), ("reserve",)],
        },
        {
            "name": "minimum signed increment and huge cache",
            "create": "CREATE SEQUENCE probe INCREMENT -9223372036854775808 MINVALUE -9223372036854775808 MAXVALUE 9223372036854775807 START 9223372036854775807 CACHE 9223372036854775807",
            "options": {
                "increment": -9223372036854775808,
                "minimum": {"value": -9223372036854775808},
                "maximum": {"value": 9223372036854775807},
                "start": 9223372036854775807,
                "cache": 9223372036854775807,
            },
            "steps": [("reserve",), ("reserve",)],
        },
        {
            "name": "narrowing allocated values and explicit restart",
            "create": "CREATE SEQUENCE probe",
            "options": {},
            "steps": [
                ("set", 60000, True),
                ("alter", "AS smallint", {"integer": "int16"}),
                ("alter", "MAXVALUE 10", {"maximum": {"value": 10}}),
                (
                    "alter",
                    "AS smallint RESTART WITH 1000",
                    {"integer": "int16", "restart": {"value": 1000}},
                ),
                ("reserve",),
                ("set", 32768, False),
                ("reserve",),
            ],
        },
        {
            "name": "descending default bounds follow integer width",
            "create": "CREATE SEQUENCE probe AS smallint INCREMENT -1",
            "options": {"integer": "int16", "increment": -1},
            "steps": [
                ("reserve",),
                ("alter", "AS integer", {"integer": "int32"}),
                ("reserve",),
                ("set", -2147483648, False),
                ("reserve",),
                ("reserve",),
            ],
        },
    ]


def snapshot(db):
    row = db.execute(
        "SELECT seqtypid::regtype::text,seqincrement,seqmin,seqmax,seqstart,seqcache,seqcycle "
        "FROM pg_sequence WHERE seqrelid='probe'::regclass"
    ).fetchone()
    value, called = db.execute("SELECT last_value,is_called FROM probe").fetchone()
    return {
        "integer": {"smallint": "int16", "integer": "int32", "bigint": "int64"}[row[0]],
        "increment": row[1],
        "minimum": row[2],
        "maximum": row[3],
        "start": row[4],
        "cache": row[5],
        "cycle": row[6],
        "last_value": value,
        "is_called": called,
    }


def reference():
    import psycopg

    result = []
    with postgres() as db:
        for profile in profiles():
            db.execute(profile["create"])
            initial = snapshot(db)
            steps = []
            for raw in profile["steps"]:
                kind = raw[0]
                if kind == "reserve":
                    statement = "SELECT nextval('probe')"
                    action = {"reserve": {}}
                elif kind == "set":
                    statement = f"SELECT setval('probe',{raw[1]},{str(raw[2]).lower()})"
                    action = {"set": {"value": raw[1], "called": raw[2]}}
                else:
                    statement = "ALTER SEQUENCE probe " + raw[1]
                    action = {"alter": raw[2]}
                before = snapshot(db)
                first, sqlstate = None, None
                try:
                    if kind == "reserve":
                        # Each backend owns a fresh cache. Closing it discards
                        # unused values; the persisted high-water is observable
                        # independently. Never loop CACHE times (it can be 2^63).
                        with psycopg.connect(db.info.dsn, autocommit=True) as owner:
                            first = owner.execute(statement).fetchone()[0]
                    else:
                        db.execute(statement)
                except psycopg.Error as error:
                    sqlstate = error.sqlstate
                after = snapshot(db)
                if sqlstate:
                    assert before == after, (profile["name"], statement)
                lease = None
                if first is not None:
                    count = (
                        abs(after["last_value"] - first) // abs(after["increment"]) + 1
                    )
                    assert 0 < count <= after["cache"]
                    lease = {
                        "first": first,
                        "count": count,
                        "last": after["last_value"],
                    }
                steps.append(
                    {
                        "sql": statement,
                        "action": action,
                        "sqlstate": sqlstate,
                        "expected": after,
                        "lease": lease,
                    }
                )
            result.append(
                {
                    "name": profile["name"],
                    "create": profile["create"],
                    "options": profile["options"],
                    "initial": initial,
                    "steps": steps,
                }
            )
            db.execute("DROP SEQUENCE probe")
    return {"format": 1, "profiles": result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--check", type=Path)
    target.add_argument("--output", type=Path)
    args = parser.parse_args()
    value = reference()
    if args.check:
        if json.loads(args.check.read_text()) != value:
            raise SystemExit("sequence PostgreSQL witness drift")
        print(
            f"verified {len(value['profiles'])} sequence arithmetic/cache profiles (not SQL activation)"
        )
    elif args.output:
        args.output.write_text(json.dumps(value, indent=2) + "\n")
    else:
        print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
