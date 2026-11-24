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

"""Owned PostgreSQL witnesses for the unchanged partition membership cases."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import postgres

ROOT = Path(__file__).resolve().parents[1]


def reference():
    inventory = json.loads(
        (
            ROOT / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_parity_inventory.json"
        ).read_text()
    )
    cases = {case["id"]: case for case in inventory["entries"]}
    # The original CREATE parent has an invalid PK (sql-0421 is a rejection).
    # Membership cases therefore get an independent valid parent fixture;
    # their original statements are never rewritten to make them pass.
    setup = "CREATE TABLE usage_events(tenant_id text,id uuid,created_at timestamptz,PRIMARY KEY(tenant_id,id,created_at)) PARTITION BY RANGE(created_at)"
    import psycopg

    steps = []
    with postgres() as db:

        def execute(sql, case=None):
            cursor = db.execute(sql)
            result = {
                "sql": sql,
                "rows": cursor.fetchall() if cursor.description else None,
                "command_tag": cursor.statusmessage,
            }
            if case:
                result.update(
                    id=case["id"], source_entry_sha256=case["source_entry_sha256"]
                )
            steps.append(result)

        invalid = cases["sql-0421"]
        try:
            db.execute(invalid["sql"])
        except psycopg.errors.FeatureNotSupported as exc:
            assert exc.diag.message_primary == (
                "unique constraint on partitioned table must include all partitioning columns"
            )
            assert "created_at" in exc.diag.message_detail
            rejection = {
                "id": invalid["id"],
                "source_entry_sha256": invalid["source_entry_sha256"],
                "sql": invalid["sql"],
                "sqlstate": exc.sqlstate,
                "message": exc.diag.message_primary,
            }
        else:
            raise AssertionError(
                "PostgreSQL accepted a unique key omitting the partition column"
            )
        execute(setup)
        execute(cases["sql-0422"]["sql"], cases["sql-0422"])
        execute(
            "INSERT INTO usage_events VALUES('tenant','00000000-0000-0000-0000-000000000001','2026-06-01')"
        )
        execute("SELECT count(*) FROM usage_events")
        execute(cases["sql-0424"]["sql"], cases["sql-0424"])
        execute("SELECT count(*) FROM usage_events")
        execute("SELECT count(*) FROM usage_events_2026")
        execute(cases["sql-0423"]["sql"], cases["sql-0423"])
        execute("SELECT count(*) FROM usage_events")
        execute("SELECT count(*) FROM usage_events_2026")
        assert [step["rows"] for step in steps if step["rows"] is not None] == [
            [(1,)],
            [(0,)],
            [(1,)],
            [(1,)],
            [(1,)],
        ]
        try:
            db.execute(
                "INSERT INTO usage_events VALUES('tenant','00000000-0000-0000-0000-000000000002','2027-01-01')"
            )
        except psycopg.errors.CheckViolation as exc:
            assert exc.sqlstate == "23514"
        else:
            raise AssertionError("PostgreSQL routed a row at the exclusive upper bound")
        assert db.execute("SELECT count(*) FROM usage_events").fetchone() == (1,)
    return json.loads(
        json.dumps(
            {
                "format": 1,
                "reference": "PostgreSQL original partition membership",
                "rejection": rejection,
                "steps": steps,
            }
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--output", type=Path)
    group.add_argument("--check", type=Path)
    args = parser.parse_args()
    value = reference()
    if args.output:
        args.output.write_text(json.dumps(value, indent=2) + "\n")
    else:
        assert json.loads(args.check.read_text()) == value, (
            "PostgreSQL partition membership reference drift"
        )
        print(
            "verified PostgreSQL partition declaration, child birth, attach and detach witnesses"
        )


if __name__ == "__main__":
    main()
