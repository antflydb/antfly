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

"""Exact transaction characteristics, retained snapshots and savepoint modes."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres

CREATE = "CREATE TABLE usage_records(id UUID PRIMARY KEY,status TEXT)"
READ = "SELECT id::text AS id,status FROM usage_records ORDER BY id"
FIRST = "00000000-0000-0000-0000-000000000001"
SECOND = "00000000-0000-0000-0000-000000000002"


def reference():
    import psycopg

    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    campaigns = []
    with postgres() as db, psycopg.connect(db.info.dsn, autocommit=True) as observer:
        for identity in ("sql-0084", "sql-0087"):
            db.execute(CREATE)
            original = originals[identity]
            assert not original["params"]
            commands = [
                ("BEGIN", None, False, None),
                (original["sql"], None, True, None),
                (
                    READ,
                    None,
                    True,
                    f"INSERT INTO usage_records(id,status) VALUES ('{FIRST}','outside')",
                ),
                ("SAVEPOINT guard", None, True, None),
                ("SET TRANSACTION READ WRITE", "25001", False, None),
                ("ROLLBACK TO guard", None, True, None),
                (
                    f"INSERT INTO usage_records(id,status) VALUES ('{SECOND}','forbidden')",
                    "25006",
                    False,
                    None,
                ),
                ("ROLLBACK", None, True, None),
                ("BEGIN", None, False, None),
                ("SAVEPOINT mode_guard", None, False, None),
                ("SET TRANSACTION READ ONLY", None, False, None),
                ("SET TRANSACTION READ WRITE", "25001", False, None),
                ("ROLLBACK TO mode_guard", None, False, None),
                (
                    f"INSERT INTO usage_records(id,status) VALUES ('{SECOND}','restored')",
                    None,
                    True,
                    None,
                ),
                ("COMMIT", None, True, None),
                ("BEGIN", None, False, None),
                (READ, None, True, None),
                (
                    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                    "25001",
                    False,
                    None,
                ),
                ("ROLLBACK", None, True, None),
            ]
            steps = []
            for (
                statement,
                expected_error,
                observe_inside,
                outside_statement,
            ) in commands:
                if outside_statement is not None:
                    observer.execute(outside_statement)
                try:
                    cursor = db.execute(statement)
                    if expected_error is not None:
                        raise AssertionError(f"{identity}: expected {expected_error}")
                    tag = cursor.statusmessage
                    code = None
                except psycopg.Error as error:
                    if error.sqlstate != expected_error:
                        raise
                    tag = ""
                    code = error.sqlstate
                steps.append(
                    {
                        "statement": statement,
                        "command_tag": tag,
                        "sqlstate": code,
                        "observe_inside": observe_inside,
                        "observer_statement": outside_statement,
                        "inside": [list(row) for row in db.execute(READ).fetchall()]
                        if observe_inside
                        else [],
                        "outside": [
                            list(row) for row in observer.execute(READ).fetchall()
                        ],
                    }
                )
            assert sum(step["statement"] == original["sql"] for step in steps) == 1
            campaigns.append(
                {
                    "format": 1,
                    "reference": "PostgreSQL exact SQL",
                    "create_sql": CREATE,
                    "read_sql": READ,
                    "cases": [{"id": identity, "steps": steps}],
                }
            )
            db.execute("DROP TABLE usage_records")
    return {"format": 1, "campaigns": campaigns}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("transaction characteristics PostgreSQL drift")
        print(f"verified {len(result['campaigns'])} original transaction campaigns")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
