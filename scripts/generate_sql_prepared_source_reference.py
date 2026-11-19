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

"""Exact original PREPARE/EXECUTE with full source/target postimages."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_source_mutation_reference import capture, profiles

IDS = ("sql-0005", "sql-0008", "sql-0009", "sql-0010")


def stamps(db, tables):
    return [
        list(
            db.execute(
                f"SELECT xmin::text,ctid::text FROM {table['name']} ORDER BY ctid"
            )
        )
        for table in tables
    ]


def reference():
    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        for identity in IDS:
            original = originals[identity]
            assert not original["params"]
            name = original["sql"].split()[1]
            assert original["sql"].startswith(f"PREPARE {name} AS ")
            for profile in profiles(
                "sql-0580" if identity == "sql-0008" else "sql-0011"
            ):
                db.execute("DEALLOCATE ALL")
                with db.transaction(force_rollback=True):
                    for table in profile["tables"]:
                        db.execute(table["create"])
                        db.execute(table["seed"])
                    before = stamps(db, profile["tables"])
                    prepared = db.execute(original["sql"])
                    assert prepared.statusmessage == "PREPARE"
                    assert before == stamps(db, profile["tables"])
                    cursor = db.execute(f"EXECUTE {name}")
                    output = capture(cursor)
                    assert cursor.statusmessage
                    command = cursor.statusmessage.split()[0]
                    assert cursor.rowcount > 0
                    affected = 0 if command == "SELECT" else cursor.rowcount
                    tables = []
                    for table in profile["tables"]:
                        with db.execute(table["read"]) as cursor:
                            tables.append({**table, "final": capture(cursor)})
                    entries.append(
                        {
                            "id": identity,
                            "profile": profile["name"],
                            "prepared_name": name,
                            "sqlstate": None,
                            "command_tag": command,
                            "affected": affected,
                            "output": output,
                            "tables": tables,
                        }
                    )
    return {"format": 1, "reference": "PostgreSQL exact SQL", "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--check", type=Path)
    target.add_argument("--output", type=Path)
    args = parser.parse_args()
    value = reference()
    if args.check:
        if json.loads(args.check.read_text()) != value:
            raise SystemExit("prepared source PostgreSQL witness drift")
        print(f"verified {len(value['entries'])} prepared source witnesses")
    elif args.output:
        args.output.write_text(json.dumps(value, indent=2) + "\n")
    else:
        print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
