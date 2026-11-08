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

"""Original temporal-conflict diagnostics with valid range/UUID warm/cold witnesses."""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres, postgres_binary_dir


def reference():
    import psycopg

    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    source_bytes = subprocess.check_output([
        "git", "show", inventory["source_commit"] + ":" + inventory["source_path"]
    ])
    if hashlib.sha256(source_bytes).hexdigest() != inventory["source_sha256"]:
        raise RuntimeError("immutable source corpus digest mismatch")
    entry = json.loads(source_bytes)["entries"][1456]
    original = entry["sql"]
    original_setup = entry["apply_setup_sql"][0]
    create = "CREATE TABLE prices (id uuid PRIMARY KEY, sku text NOT NULL, valid_from numeric NOT NULL, valid_to numeric NOT NULL, price numeric, valid_time numrange NOT NULL DEFAULT '[5,15)', CONSTRAINT prices_sku_time_key UNIQUE (sku, valid_time WITHOUT OVERLAPS))"
    seed = "INSERT INTO prices VALUES ('00000000-0000-0000-0000-000000000001','sku:a',0,10,10,'[0,10)')"
    defaults_create = "CREATE TABLE temporal_defaults(id UUID DEFAULT 'A0EEBC999C0B4EF8BB6D6BB9BD380A11' PRIMARY KEY,valid_num NUMRANGE DEFAULT '[5,15)',valid_date DATERANGE DEFAULT '[2026-01-01,2026-01-04)')"
    corrected = original.replace("'p2'", "'00000000-0000-0000-0000-000000000002'", 1)

    def reject(db, statement, expected):
        try:
            db.execute(statement)
        except psycopg.Error as error:
            if error.sqlstate != expected:
                raise AssertionError((statement, expected, error.sqlstate)) from error
            return error.sqlstate
        raise AssertionError((statement, "unexpected acceptance"))

    def checks(db):
        reject(db, original, "22P02")
        reject(db, corrected, "42809")
        row = db.execute("SELECT id::text,sku,valid_time::text,price::text FROM prices").fetchall()
        assert row == [("00000000-0000-0000-0000-000000000001", "sku:a", "[0,10)", "10")]
        return "|".join(row[0])

    with postgres() as db:
        version = db.execute("SELECT version()").fetchone()[0]
        reject(db, original_setup, "42601")
        db.execute("CREATE EXTENSION btree_gist")
        db.execute(create)
        db.execute(seed)
        db.execute(defaults_create)
        db.execute("INSERT INTO temporal_defaults DEFAULT VALUES")
        defaults = db.execute("SELECT id::text,valid_num::text,valid_date::text FROM temporal_defaults").fetchall()
        assert defaults == [("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "[5,15)", "[2026-01-01,2026-01-04)")]
        row = checks(db)
        # The shared helper verified this connection against its own temporary
        # cluster. Restart only that same data directory and reconnect exactly
        # its Unix connection; the helper still owns finally-stop and cleanup.
        data = Path(db.execute("SHOW data_directory").fetchone()[0]).resolve()
        if not data.parent.name.startswith("antfly-sql-pg-") or data.name != "data":
            raise RuntimeError("refusing restart outside the owned oracle cluster")
        connection = db.info.dsn
        db.close()
        subprocess.run([postgres_binary_dir() / "pg_ctl", "-D", data, "-m", "fast", "-w", "restart", "-l", data.parent / "restart.log"], check=True, capture_output=True, timeout=30)
        with psycopg.connect(connection, autocommit=True) as cold:
            assert Path(cold.execute("SHOW data_directory").fetchone()[0]).resolve() == data
            assert cold.execute("SHOW listen_addresses").fetchone()[0] == ""
            cold.execute("SET statement_timeout='2s'")
            cold.execute("SET lock_timeout='250ms'")
            assert checks(cold) == row
            assert cold.execute("SELECT id::text,valid_num::text,valid_date::text FROM temporal_defaults").fetchall() == defaults
    return {
        "format": 1,
        "postgres_version": version,
        "id": "sql-1457",
        "source_entry_sha256": "6f1dacdc2b1959ca46998d9c24c9ff5dd226ca7c9833a0bcf27501d4b69fbddd",
        "scope": "Original setup diagnostic plus explicitly adapted valid PostgreSQL range/UUID temporal arbiter witnesses; not temporal DO UPDATE implementation.",
        "original_setup": original_setup,
        "original_setup_sqlstate": "42601",
        "create": create,
        "seed": seed,
        "defaults_create": defaults_create,
        "defaults_row": "|".join(defaults[0]),
        "original": original,
        "original_sqlstate": "22P02",
        "valid_typed": corrected,
        "valid_typed_sqlstate": "42809",
        "unchanged_row": row,
        "cold_original_sqlstate": "22P02",
        "cold_valid_typed_sqlstate": "42809",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] / "zig/pkg/antfly/src/api/fixtures/sql_temporal_conflict_reference.json")
    args = parser.parse_args()
    actual = reference()
    if args.check:
        expected = json.loads(args.check.read_text())
        # PostgreSQL18+ semantics are portable; compiler/platform/version text
        # records provenance and must not turn semantic checks into OS pins.
        expected.pop("postgres_version", None)
        actual.pop("postgres_version", None)
        if expected != actual:
            raise SystemExit("temporal conflict reference drift")
    else:
        args.output.write_text(json.dumps(actual, indent=2) + "\n")
    print("sql-1457 PostgreSQL warm/cold temporal arbiter PASS; owned cluster stopped")


if __name__ == "__main__":
    main()
