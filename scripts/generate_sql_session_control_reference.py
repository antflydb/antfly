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

"""Original transaction/savepoint controls with real isolated observer sessions."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres

IDS = [85, 86, 88, 764, 765, 766, 767, 768, 769, 770, 92, 93, 94]
CREATE = "CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT)"
READ = "SELECT id,status FROM usage_records ORDER BY id"


def commands(number, original):
    key = f"case-{number:04}"

    def insert(suffix):
        return f"INSERT INTO usage_records(id,status) VALUES ('{key}-{suffix}','kept')"

    if number in {85, 86, 88, 764, 765, 766}:
        return [
            original,
            insert("commit"),
            "COMMIT",
            "BEGIN",
            insert("abort"),
            "ROLLBACK",
        ]
    if number in {767, 768, 769, 770}:
        return ["BEGIN", insert("control"), original]
    if number == 92:
        return [
            "BEGIN",
            insert("before"),
            original,
            insert("after"),
            "ROLLBACK TO antfly_migration_step",
            "COMMIT",
        ]
    if number == 93:
        return [
            "BEGIN",
            "SAVEPOINT antfly_migration_step",
            insert("before"),
            original,
            insert("after"),
            "COMMIT",
        ]
    return [
        "BEGIN",
        "SAVEPOINT antfly_migration_step",
        insert("discard"),
        original,
        insert("after"),
        "COMMIT",
    ]


def reference():
    import psycopg

    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    result = []
    with postgres() as db:
        db.execute(CREATE)
        with psycopg.connect(db.info.dsn, autocommit=True) as observer:
            for number in IDS:
                identity = f"sql-{number:04}"
                steps = []
                for statement in commands(number, originals[identity]["sql"]):
                    cursor = db.execute(statement)
                    steps.append(
                        {
                            "statement": statement,
                            "command_tag": cursor.statusmessage,
                            "inside": [
                                list(row) for row in db.execute(READ).fetchall()
                            ],
                            "outside": [
                                list(row) for row in observer.execute(READ).fetchall()
                            ],
                        }
                    )
                result.append({"id": identity, "steps": steps})
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL",
        "create_sql": CREATE,
        "read_sql": READ,
        "cases": result,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("session control PostgreSQL drift")
        print(
            f"verified {len(result['cases'])} original controls and every inside/outside postimage"
        )
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
