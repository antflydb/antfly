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

"""PostgreSQL 18+ oracle for unchanged query-produced table originals."""

import argparse
import hashlib
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, create_table, postgres, properties

IDS = frozenset(
    (
        "sql-0351",
        "sql-0352",
        "sql-0353",
        "sql-0356",
        "sql-0357",
        "sql-0358",
        "sql-0359",
        "sql-0364",
        "sql-0365",
        "sql-0366",
        "sql-0567",
    )
)


def generate():
    inventory_bytes = (FIXTURES / "sql_parity_inventory.json").read_bytes()
    profile_bytes = (FIXTURES / "sql_read_campaign_profile.json").read_bytes()
    inventory = json.loads(inventory_bytes)
    profile = json.loads(profile_bytes)
    result = {
        "format": 1,
        "inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
        "profile_sha256": hashlib.sha256(profile_bytes).hexdigest(),
        "entries": [],
    }
    with postgres() as pg:
        result["postgres_version"] = pg.execute("SHOW server_version").fetchone()[0]
        for case in inventory["entries"]:
            if case["id"] not in IDS:
                continue
            with pg.transaction(force_rollback=True):
                create_table(
                    pg, "usage_records", properties(profile["schema"]), profile["rows"]
                )
                if case["id"] in ("sql-0356", "sql-0357"):
                    pg.execute(
                        "CREATE MATERIALIZED VIEW usage_records_mv AS SELECT id,status FROM usage_records"
                    )
                    pg.execute(
                        "UPDATE usage_records SET status='refreshed' WHERE id='u5'"
                    )
                    entry = {
                        key: case[key] for key in ("id", "source_entry_sha256", "sql")
                    }
                    code = None
                    command = ""
                    try:
                        with pg.transaction():
                            command = pg.execute(case["sql"]).statusmessage
                    except Exception as error:
                        code = getattr(error, "sqlstate", None)
                        if code is None:
                            raise
                    entry.update(
                        command_tag=command,
                        sqlstate=code,
                        columns=[],
                        rows=pg.execute(
                            "SELECT * FROM usage_records_mv ORDER BY 1"
                        ).fetchall(),
                    )
                    result["entries"].append(entry)
                    continue
                if case["id"] in ("sql-0358", "sql-0359"):
                    pg.execute(
                        "CREATE MATERIALIZED VIEW usage_records_mv AS SELECT id,status FROM usage_records"
                    )
                    entry = {
                        key: case[key] for key in ("id", "source_entry_sha256", "sql")
                    }
                    entry.update(
                        command_tag=pg.execute(case["sql"]).statusmessage,
                        columns=[],
                        rows=[],
                    )
                    entry["absent"] = pg.execute(
                        "SELECT to_regclass('usage_records_mv') IS NULL"
                    ).fetchone()[0]
                    if case["id"] == "sql-0359":
                        entry["repeat_command_tag"] = pg.execute(
                            case["sql"]
                        ).statusmessage
                    result["entries"].append(entry)
                    continue
                command = pg.execute(case["sql"]).statusmessage
                target = (
                    "usage_records_mv"
                    if case["id"] in ("sql-0351", "sql-0352", "sql-0353")
                    else "usage_archive_empty"
                    if case["id"] == "sql-0365"
                    else "usage_archive"
                )
                rows = pg.execute("SELECT * FROM " + target + " ORDER BY 1").fetchall()
                columns = pg.execute(
                    "SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull FROM pg_attribute a WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
                    (target,),
                ).fetchall()
                entry = {key: case[key] for key in ("id", "source_entry_sha256", "sql")}
                entry.update(command_tag=command, columns=columns, rows=rows)
                if case["id"] == "sql-0366":
                    entry["repeat_command_tag"] = pg.execute(case["sql"]).statusmessage
                    entry["repeat_rows"] = pg.execute(
                        "SELECT id,status FROM " + target + " ORDER BY id"
                    ).fetchall()
                result["entries"].append(entry)
        result["materialized_diagnostics"] = []
        for populated in (False, True):
            with pg.transaction(force_rollback=True):
                pg.execute(
                    "CREATE MATERIALIZED VIEW usage_records_mv AS SELECT 'one'::text AS id,'open'::text AS status"
                    + ("" if populated else " WITH NO DATA")
                )
                probes = []
                for query in (
                    "DROP VIEW usage_records_mv",
                    "DELETE FROM usage_records_mv",
                    "REFRESH MATERIALIZED VIEW CONCURRENTLY usage_records_mv WITH NO DATA",
                    "SELECT * FROM usage_records_mv ORDER BY 1",
                ):
                    code = None
                    try:
                        with pg.transaction(force_rollback=True):
                            pg.execute(query)
                    except Exception as error:
                        code = getattr(error, "sqlstate", None)
                        if code is None:
                            raise
                    probes.append({"sql": query, "code": code})
                result["materialized_diagnostics"].append(
                    {"populated": populated, "probes": probes}
                )
    if {entry["id"] for entry in result["entries"]} != IDS:
        raise ValueError("original population inventory changed")
    return json.loads(json.dumps(result))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    result = generate()
    if args.check:
        existing = json.loads(args.output.read_text())
        # Patch releases are provenance; SQL results and input hashes must agree.
        existing.pop("postgres_version", None)
        actual = dict(result)
        actual.pop("postgres_version", None)
        if existing != actual:
            raise SystemExit("PostgreSQL population reference changed")
        print(
            "PostgreSQL population reference: all eleven unchanged originals and materialized diagnostics match"
        )
    else:
        args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
