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

"""Run unchanged maintenance originals against an owned PostgreSQL 18+ oracle."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "zig/pkg/antfly-embedded/src/sql/fixtures"
IDS = [f"sql-{i:04}" for i in range(72, 78)]
SETUP = [
    "CREATE TABLE usage_records (id BIGINT NOT NULL, status TEXT, amount BIGINT, PRIMARY KEY(id))",
    "INSERT INTO usage_records (id,status,amount) VALUES (1,'pending',10),(2,'done',20),(3,NULL,30)",
    "UPDATE usage_records SET amount=amount+1",
    "UPDATE usage_records SET amount=amount-1",
    "CREATE INDEX usage_records_status_idx ON usage_records (status)",
]


def reference():
    originals = {
        e["id"]: e
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    with postgres() as db:
        for sql in SETUP:
            db.execute(sql)
        steps = []
        for identity in IDS:
            sql = originals[identity]["sql"]
            cursor = db.execute(sql)
            steps.append(
                {"id": identity, "sql": sql, "command_tag": cursor.statusmessage}
            )
        rows = db.execute(
            "SELECT id,status,amount FROM usage_records ORDER BY id"
        ).fetchall()
        return {"format": 1, "cases": steps, "rows": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    target = FIXTURES / "sql_maintenance_reference.json"
    rendered = json.dumps(reference(), indent=2) + "\n"
    if args.check:
        if target.read_text() != rendered:
            raise SystemExit("SQL maintenance reference differs")
    else:
        target.write_text(rendered)


if __name__ == "__main__":
    main()
