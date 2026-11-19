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

"""Original invalid signatures and binding contracts on declared native columns."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import (
    FIXTURES,
    postgres,
    typed_array_read_profile,
    create_table,
    properties,
    execution_parameters,
)

CONTRACTS = {
    "sql-0215": "42883",
    "sql-0259": "42883",
    "sql-0261": "42883",
    "sql-0262": "42883",
    "sql-0263": "42883",
    "sql-0267": "42883",
    "sql-0268": "42883",
    "sql-0269": "42883",
    "sql-0270": "42883",
    "sql-0552": "42804",
    "sql-1370": "42P20",
    "sql-1501": "42601",
}


def reference():
    import psycopg

    originals = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    cases = [case for case in originals if case["id"] in CONTRACTS]
    if len(cases) != len(CONTRACTS):
        raise ValueError("binding rejection inventory drift")
    profile = typed_array_read_profile()
    entries = []
    with postgres() as db:
        create_table(
            db, "usage_records", properties(profile["schema"]), profile["rows"]
        )
        before = db.execute(
            "SELECT to_jsonb(r) FROM usage_records r ORDER BY id"
        ).fetchall()
        for case in cases:
            try:
                with (
                    db.transaction(force_rollback=True),
                    psycopg.RawCursor(db) as cursor,
                ):
                    cursor.execute(case["sql"], execution_parameters(db, case))
            except psycopg.Error as error:
                if error.sqlstate != CONTRACTS[case["id"]]:
                    raise
            else:
                raise ValueError(f"{case['id']}: expected rejection succeeded")
            if (
                db.execute(
                    "SELECT to_jsonb(r) FROM usage_records r ORDER BY id"
                ).fetchall()
                != before
            ):
                raise ValueError("binding rejection changed native values")
            entries.append({"id": case["id"], "sqlstate": CONTRACTS[case["id"]]})
    return {
        "format": 1,
        "reference": "PostgreSQL exact SQL",
        "profile": profile,
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("binding rejection PostgreSQL drift")
        print(
            f"verified {len(result['entries'])} exact pre-execution rejection contracts"
        )
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
