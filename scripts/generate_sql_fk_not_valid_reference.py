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

"""Original PostgreSQL NOT VALID FK: history, future writes and parent actions."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, cursor_result, postgres


def reference():
    import psycopg

    original = next(
        e
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        if e["id"] == "sql-0734"
    )
    assert not original["params"]
    first = "00000000-0000-0000-0000-000000000001"
    second = "00000000-0000-0000-0000-000000000002"
    probe_id = "00000000-0000-0000-0000-000000000003"
    deleted = "00000000-0000-0000-0000-000000000005"
    cold = "00000000-0000-0000-0000-000000000004"
    case = dict(
        id="sql-0734",
        table="usage_records",
        constraint_collection="foreign_keys",
        check_name="usage_records_user_id_fkey",
        preserve_ddl_primary=True,
        verify_primary_identity=True,
        setup=[
            dict(statement="CREATE TABLE users(id TEXT PRIMARY KEY)", table="users"),
            dict(
                statement="CREATE TABLE usage_records(id UUID PRIMARY KEY,user_id TEXT,status TEXT)",
                table="usage_records",
            ),
            dict(statement="INSERT INTO users(id) VALUES ('known')"),
            dict(
                statement=f"INSERT INTO usage_records(id,user_id,status) VALUES ('{first}','legacy-orphan','old'),('{second}','known','valid'),('{deleted}','deleted-orphan','old'),('{cold}','cold-orphan','retained-history')"
            ),
        ],
        read_sql="SELECT id::text AS id,user_id,status FROM usage_records ORDER BY id",
        ddl_sqlstate=None,
        probes=[],
    )
    statements = [
        (f"UPDATE usage_records SET user_id='missing' WHERE id='{first}'", "23503"),
        (
            f"INSERT INTO usage_records(id,user_id,status) VALUES ('{probe_id}','missing','new')",
            "23503",
        ),
        (f"UPDATE usage_records SET status='untouched-key' WHERE id='{first}'", None),
        (f"DELETE FROM usage_records WHERE id='{deleted}'", None),
        ("INSERT INTO users(id) VALUES ('legacy-orphan')", None),
        ("DELETE FROM users WHERE id='legacy-orphan'", "23503"),
        ("DELETE FROM users WHERE id='known'", "23503"),
        (f"UPDATE usage_records SET user_id='known' WHERE id='{first}'", None),
        ("DELETE FROM users WHERE id='legacy-orphan'", None),
        (f"DELETE FROM usage_records WHERE id='{second}'", None),
        (
            f"INSERT INTO usage_records(id,user_id,status) VALUES ('{probe_id}',NULL,'nullable')",
            None,
        ),
        (f"DELETE FROM usage_records WHERE id='{probe_id}'", None),
        ("INSERT INTO users(id) VALUES ('cold-orphan')", None),
        ("DELETE FROM users WHERE id='cold-orphan'", "23503"),
    ]
    parent_sql = "SELECT id FROM users ORDER BY id"
    with postgres() as db:
        for step in case["setup"]:
            db.execute(step["statement"])
        before = db.execute(
            "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
        ).fetchall()
        assert db.execute(original["sql"]).statusmessage == "ALTER TABLE"
        assert (
            before
            == db.execute(
                "SELECT xmin::text,ctid::text,* FROM usage_records ORDER BY id"
            ).fetchall()
        )

        def snapshot(sql):
            with psycopg.RawCursor(db) as cursor:
                return cursor_result(
                    cursor, dict(id=case["id"], sql=sql, params=[]), read=True
                )

        def validity():
            return not db.execute(
                "SELECT convalidated FROM pg_constraint WHERE conrelid='usage_records'::regclass AND conname='usage_records_user_id_fkey'"
            ).fetchone()[0]

        def parents():
            return [dict(statement=parent_sql, output=snapshot(parent_sql))]

        case["after_ddl"] = snapshot(case["read_sql"])
        case["related_after_ddl"] = parents()
        case["declared_not_valid"] = validity()
        assert case["declared_not_valid"]
        for statement, expected in statements:
            try:
                with db.transaction():
                    db.execute(statement)
            except psycopg.Error as err:
                if err.sqlstate != expected:
                    raise
            else:
                assert expected is None, (statement, expected)
            probe = dict(
                statement=statement,
                sqlstate=expected,
                after=snapshot(case["read_sql"]),
                related_after=parents(),
                declared_not_valid=validity(),
            )
            if statement == "DELETE FROM users WHERE id='legacy-orphan'" and expected:
                # This parent is deliberately removed after repairing its child.
                # The independent cold-orphan probe retains historical coverage.
                probe["reopen"] = dict(sqlstate=None)
            case["probes"].append(probe)
        final = snapshot(case["read_sql"])
        final_parents = parents()
        for probe in case["probes"]:
            if probe["sqlstate"] is None and "reopen" not in probe:
                continue
            expected = probe.get("reopen", probe)["sqlstate"]
            try:
                with db.transaction():
                    db.execute(probe["statement"])
            except psycopg.Error as err:
                assert err.sqlstate == expected, (
                    probe["statement"],
                    err.sqlstate,
                    expected,
                )
            else:
                assert expected is None, (probe["statement"], expected)
            assert snapshot(case["read_sql"]) == final
            assert parents() == final_parents
            assert validity()
        return dict(format=1, reference="PostgreSQL exact SQL", cases=[case])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--output", type=Path)
    group.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        assert json.loads(args.check.read_text()) == result, "FK NOT VALID oracle drift"
        print(
            "verified original FK NOT VALID, historical orphans, future enforcement, parent actions and complete postimages"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
