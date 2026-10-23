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

"""Original deferred UNIQUE DDL/timing with transient conflicts and observers."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres

CREATE = "CREATE TABLE usage_records(id UUID PRIMARY KEY,email TEXT)"
READ = "SELECT id::text AS id,email AS status FROM usage_records ORDER BY id"
DECLARE = (
    "ALTER TABLE usage_records ADD CONSTRAINT usage_records_email_key "
    "UNIQUE(email) DEFERRABLE INITIALLY IMMEDIATE"
)


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
        for identity in ("sql-0016", "sql-0082", "sql-0018", "sql-0083"):
            table = "usage_stage" if identity == "sql-0018" else "usage_records"
            create = CREATE
            read = READ
            if identity == "sql-0018":
                create = "CREATE TABLE usage_stage(tenant_id TEXT,id UUID,status TEXT)"
                read = "SELECT id::text AS id,tenant_id||':'||status AS status FROM usage_stage ORDER BY status"
            db.execute(create)
            setup = []
            if identity == "sql-0083":
                setup = [
                    {
                        "statement": "CREATE TABLE parents(id TEXT PRIMARY KEY)",
                        "table": "parents",
                    },
                    {"statement": "INSERT INTO parents(id) VALUES ('valid')"},
                ]
                for step in setup:
                    db.execute(step["statement"])
            original = originals[identity]
            assert not original["params"]
            commands = (
                [original["sql"], "BEGIN"]
                if identity == "sql-0016"
                else [DECLARE, "BEGIN", original["sql"]]
            )
            commands += [
                "INSERT INTO usage_records(id,email) VALUES ('one','shared@example.test')",
                "INSERT INTO usage_records(id,email) VALUES ('two','shared@example.test')",
                "UPDATE usage_records SET email='distinct@example.test' WHERE id='two'",
                "COMMIT",
            ]
            commands += [
                "BEGIN",
                "SET CONSTRAINTS usage_records_email_key DEFERRED",
                "INSERT INTO usage_records(id,email) VALUES ('three','shared@example.test')",
                "COMMIT",
                "ROLLBACK",
            ]
            if identity == "sql-0018":
                commands = [
                    original["sql"],
                    "BEGIN",
                    "INSERT INTO usage_stage(tenant_id,id,status) VALUES ('acme','same','left')",
                    "INSERT INTO usage_stage(tenant_id,id,status) VALUES ('acme','same','right')",
                    "UPDATE usage_stage SET id='distinct' WHERE status='right'",
                    "COMMIT",
                    "BEGIN",
                    "SET CONSTRAINTS usage_stage_pk DEFERRED",
                    "INSERT INTO usage_stage(tenant_id,id,status) VALUES ('acme','same','pending')",
                    "COMMIT",
                    "ROLLBACK",
                ]
            if identity == "sql-0083":
                commands = [
                    "ALTER TABLE usage_records ADD CONSTRAINT usage_fk FOREIGN KEY(email) REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED",
                    "ALTER TABLE usage_records ADD CONSTRAINT usage_check CHECK(email <> 'bad') NOT VALID",
                    "BEGIN",
                    "INSERT INTO usage_records(id,email) VALUES ('one','missing')",
                    "UPDATE usage_records SET email='valid' WHERE id='one'",
                    original["sql"],
                    "COMMIT",
                    "BEGIN",
                    "SET CONSTRAINTS usage_fk IMMEDIATE",
                    "INSERT INTO usage_records(id,email) VALUES ('two','missing')",
                    "ROLLBACK",
                ]
            if identity == "sql-0018":
                commands.insert(
                    6,
                    "INSERT INTO usage_stage(tenant_id,id,status) VALUES (NULL,'null-probe','null')",
                )
            keys = {
                "one": "00000000-0000-0000-0000-000000000001",
                "two": "00000000-0000-0000-0000-000000000002",
                "three": "00000000-0000-0000-0000-000000000003",
                "same": "00000000-0000-0000-0000-000000000001",
                "distinct": "00000000-0000-0000-0000-000000000002",
                "null-probe": "00000000-0000-0000-0000-000000000003",
            }
            for index, statement in enumerate(commands):
                if statement == original["sql"]:
                    continue
                for label, key in keys.items():
                    statement = statement.replace(f"'{label}'", f"'{key}'")
                commands[index] = statement
            assert commands.count(original["sql"]) == 1
            steps = []
            for index, statement in enumerate(commands):
                expected_error = "23505" if index == len(commands) - 2 else None
                if "NULL" in statement:
                    expected_error = "23502"
                if identity == "sql-0083" and expected_error is not None:
                    expected_error = "23503"
                try:
                    cursor = db.execute(statement)
                except psycopg.Error as error:
                    if error.sqlstate != expected_error:
                        raise
                    outside = [list(row) for row in observer.execute(read).fetchall()]
                    steps.append(
                        {
                            "statement": statement,
                            "command_tag": "",
                            "sqlstate": error.sqlstate,
                            "observe_inside": False,
                            "inside": [],
                            "outside": outside,
                        }
                    )
                    continue
                if expected_error:
                    raise ValueError("unrepaired deferred UNIQUE committed")
                steps.append(
                    {
                        "statement": statement,
                        "command_tag": cursor.statusmessage,
                        "inside": [list(row) for row in db.execute(read).fetchall()],
                        "outside": [
                            list(row) for row in observer.execute(read).fetchall()
                        ],
                    }
                )
            campaigns.append(
                {
                    "format": 1,
                    "reference": "PostgreSQL exact SQL",
                    "create_sql": create,
                    "read_sql": read,
                    "table": table,
                    "setup": setup,
                    "cases": [{"id": identity, "steps": steps}],
                }
            )
            db.execute(f"DROP TABLE {table}")
            if identity == "sql-0083":
                db.execute("DROP TABLE parents")
    return {"format": 1, "campaigns": campaigns}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("constraint timing PostgreSQL drift")
        print(f"verified {len(result['campaigns'])} deferred constraint campaigns")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
