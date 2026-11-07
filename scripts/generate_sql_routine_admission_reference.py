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

"""Verify exact original invalid routine definitions with PostgreSQL.

This qualifies declaration admission, not support for executing SQL or PL/pgSQL
routines. The native gate proves rejection before any catalog/storage access.
"""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres

CONTRACTS = {
    **dict.fromkeys(
        [
            "sql-0156",
            *[f"sql-{n:04d}" for n in range(384, 394)],
            "sql-0396",
            "sql-0413",
        ],
        "42P13",
    ),
    "sql-0395": "42601",
}


def reference():
    import psycopg

    cases = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())["entries"]
    entries = []
    with postgres() as db:
        before = db.execute(
            "SELECT oid, proname FROM pg_proc WHERE pronamespace='public'::regnamespace ORDER BY oid"
        ).fetchall()
        for case in cases:
            if case["id"] not in CONTRACTS:
                continue
            try:
                with (
                    db.transaction(force_rollback=True),
                    psycopg.RawCursor(db) as cursor,
                ):
                    cursor.execute(case["sql"])
            except psycopg.Error as error:
                if error.sqlstate != CONTRACTS[case["id"]]:
                    raise
            else:
                raise ValueError(f"{case['id']}: invalid definition succeeded")
            if (
                before
                != db.execute(
                    "SELECT oid, proname FROM pg_proc WHERE pronamespace='public'::regnamespace ORDER BY oid"
                ).fetchall()
            ):
                raise ValueError("rejected definition changed the routine catalog")
            entries.append({"id": case["id"], "sqlstate": CONTRACTS[case["id"]]})
    if len(entries) != len(CONTRACTS):
        raise ValueError("routine inventory drift")
    return {"format": 1, "reference": "PostgreSQL exact SQL", "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("routine definition PostgreSQL drift")
        print(f"verified {len(result['entries'])} exact routine definition diagnostics")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
