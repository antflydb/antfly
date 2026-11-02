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

"""Verify original prepared-transaction commands with an owned PostgreSQL 18+ restart."""

import argparse
import json
import subprocess
from pathlib import Path

from generate_sql_postgres_reference import postgres, postgres_binary_dir

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "zig/pkg/antfly-embedded/src/sql/fixtures"
IDS = ["sql-0771", "sql-0772", "sql-0773"]


def verify_boundaries(connection):
    import psycopg

    def rejects(statement, code):
        try:
            connection.execute(statement)
        except psycopg.Error as error:
            assert error.sqlstate == code, (statement, error.sqlstate, code)
        else:
            raise AssertionError(f"expected {code}: {statement}")

    assert (
        connection.execute("PREPARE TRANSACTION 'outside'").statusmessage == "ROLLBACK"
    )
    connection.execute("BEGIN")
    connection.execute("PREPARE TRANSACTION 'boundary_owner'")
    connection.execute("BEGIN")
    rejects("COMMIT PREPARED 'boundary_owner'", "25001")
    connection.execute("ROLLBACK")
    connection.execute("BEGIN")
    rejects("PREPARE TRANSACTION 'boundary_owner'", "42710")
    assert connection.info.transaction_status.name == "IDLE"
    connection.execute("CREATE ROLE prepared_observer")
    connection.execute("SET ROLE prepared_observer")
    try:
        rejects("ROLLBACK PREPARED 'boundary_owner'", "42501")
    finally:
        connection.execute("RESET ROLE")
    connection.execute("ROLLBACK PREPARED 'boundary_owner'")
    rejects("COMMIT PREPARED 'boundary_owner'", "42704")
    connection.execute("BEGIN")
    assert (
        connection.execute("PREPARE TRANSACTION ''").statusmessage
        == "PREPARE TRANSACTION"
    )
    assert connection.execute("COMMIT PREPARED ''").statusmessage == "COMMIT PREPARED"
    connection.execute("BEGIN")
    connection.execute("DECLARE held CURSOR WITH HOLD FOR SELECT 1")
    rejects("PREPARE TRANSACTION 'held'", "0A000")
    assert connection.info.transaction_status.name == "IDLE"
    rejects("FETCH NEXT FROM held", "34000")
    assert (
        connection.execute("PREPARE TRANSACTION AS SELECT 1").statusmessage == "PREPARE"
    )
    assert connection.execute("EXECUTE TRANSACTION").fetchall() == [(1,)]
    connection.execute("DEALLOCATE TRANSACTION")
    assert (
        connection.execute("PREPARE TRANSACTION(integer) AS SELECT $1").statusmessage
        == "PREPARE"
    )
    assert connection.execute("EXECUTE TRANSACTION(2)").fetchall() == [(2,)]
    connection.execute("DEALLOCATE TRANSACTION")
    assert connection.execute("SELECT gid FROM pg_prepared_xacts").fetchall() == []


def reference():
    import psycopg

    originals = {
        row["id"]: row
        for row in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    cases = []
    with postgres(max_prepared_transactions=32) as initial:
        version = int(initial.execute("SHOW server_version_num").fetchone()[0]) // 10000
        for table in ("prepared_a", "prepared_b"):
            initial.execute(
                f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL)"
            )
            initial.execute(f"INSERT INTO {table} VALUES(1,10)")
        initial.execute("BEGIN")
        for table in ("prepared_a", "prepared_b"):
            initial.execute(f"UPDATE {table} SET amount=11 WHERE id=1")
        cursor = initial.execute(originals[IDS[0]]["sql"])
        cases.append(
            {
                "id": IDS[0],
                "sql": originals[IDS[0]]["sql"],
                "command_tag": cursor.statusmessage,
            }
        )
        assert initial.info.transaction_status.name == "IDLE"
        before = initial.execute("SELECT amount FROM prepared_a").fetchall()
        assert initial.execute("SELECT amount FROM prepared_b").fetchall() == before
        assert initial.execute(
            "SELECT gid FROM pg_prepared_xacts ORDER BY gid"
        ).fetchall() == [("usage_batch",)]
        initial.execute("BEGIN")
        initial.execute("ROLLBACK")
        directory = initial.execute("SHOW data_directory").fetchone()[0]
        dsn = initial.info.dsn
        initial.close()
        # The generator owns this isolated server. Both connections disappear
        # before its real WAL-backed restart; the GID must remain independent.
        subprocess.run(
            [
                postgres_binary_dir() / "pg_ctl",
                "-D",
                directory,
                "-l",
                Path(directory).parent / "server.log",
                "-m",
                "fast",
                "-w",
                "restart",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
        with psycopg.connect(dsn, autocommit=True) as resumed:
            assert resumed.execute(
                "SELECT gid FROM pg_prepared_xacts ORDER BY gid"
            ).fetchall() == [("usage_batch",)]
            assert resumed.execute("SELECT amount FROM prepared_a").fetchall() == before
            cursor = resumed.execute(originals[IDS[1]]["sql"])
            cases.append(
                {
                    "id": IDS[1],
                    "sql": originals[IDS[1]]["sql"],
                    "command_tag": cursor.statusmessage,
                }
            )
            committed = resumed.execute("SELECT amount FROM prepared_a").fetchall()
            assert (
                resumed.execute("SELECT amount FROM prepared_b").fetchall() == committed
            )
            resumed.execute("BEGIN")
            for table in ("prepared_a", "prepared_b"):
                resumed.execute(f"UPDATE {table} SET amount=12 WHERE id=1")
            resumed.execute(originals[IDS[0]]["sql"])
            cursor = resumed.execute(originals[IDS[2]]["sql"])
            cases.append(
                {
                    "id": IDS[2],
                    "sql": originals[IDS[2]]["sql"],
                    "command_tag": cursor.statusmessage,
                }
            )
            rolled_back = resumed.execute("SELECT amount FROM prepared_a").fetchall()
            assert (
                resumed.execute("SELECT amount FROM prepared_b").fetchall()
                == rolled_back
            )
            assert resumed.execute("SELECT gid FROM pg_prepared_xacts").fetchall() == []
            assert (
                before == [(10,)] and committed == [(11,)] and rolled_back == committed
            )
            verify_boundaries(resumed)
    return {
        "format": 1,
        "postgres_major": version,
        "cases": cases,
        "preimage": before,
        "committed": committed,
        "rolled_back": rolled_back,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    target = FIXTURES / "sql_prepared_transaction_reference.json"
    rendered = json.dumps(reference(), indent=2) + "\n"
    if args.check:
        if target.read_text() != rendered:
            raise SystemExit("SQL prepared transaction reference differs")
    else:
        target.write_text(rendered)


if __name__ == "__main__":
    main()
