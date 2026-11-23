#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run the unchanged connected-subscription originals on owned PostgreSQL 18+."""

import argparse
import getpass
import json
import os
import socket
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, ROOT, postgres, postgres_binary_dir


@contextmanager
def reserve_port():
    # Keep the reservation until the postmaster is about to bind it. Nothing
    # connects to a foreign service: postgres() verifies data_directory first.
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        yield reservation.getsockname()[1], reservation


def await_value(read, expected, *, seconds=30):
    deadline = time.monotonic() + seconds
    actual = None
    while time.monotonic() < deadline:
        actual = read()
        if actual == expected:
            return actual
        time.sleep(0.05)
    raise AssertionError({"expected": expected, "actual": actual})


@contextmanager
def publisher():
    import psycopg

    with reserve_port() as (port, reservation):
        reservation.close()
        with postgres(logical_replication=True, listen_port=port) as admin:
            admin.execute("CREATE DATABASE usage")
            connection = dict(admin.info.get_parameters(), dbname="usage")
            with psycopg.connect(**connection, autocommit=True) as db:
                db.execute(
                    "CREATE TABLE usage_records(id integer PRIMARY KEY,payload text)"
                )
                db.execute(
                    "CREATE TABLE usage_events(id integer PRIMARY KEY,payload text)"
                )
                db.execute("INSERT INTO usage_records VALUES(1,'initial')")
                db.execute("INSERT INTO usage_events VALUES(9,'audit')")
                db.execute("CREATE PUBLICATION usage_pub FOR TABLE usage_records")
                db.execute("CREATE PUBLICATION usage_audit_pub FOR TABLE usage_events")
                yield db, port


def reference():
    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with publisher() as (upstream, upstream_port):
        # libpq inherits this in each owned subscriber worker. The actual SQL
        # originals remain host=localhost dbname=usage, with no substituted DSN.
        env = dict(os.environ, PGPORT=str(upstream_port), PGUSER=getpass.getuser())
        with postgres(logical_replication=True, server_env=env) as db:
            db.execute(
                "CREATE TABLE usage_records(id integer PRIMARY KEY,payload text)"
            )
            db.execute("CREATE TABLE usage_events(id integer PRIMARY KEY,payload text)")

            def rows(table="usage_records"):
                return [
                    list(row)
                    for row in db.execute(
                        f"SELECT id,payload FROM {table} ORDER BY id"
                    ).fetchall()
                ]

            def execute(case_id):
                case = originals[case_id]
                assert not case["params"]
                result = db.execute(case["sql"])
                entries.append(
                    {
                        "id": case_id,
                        "sql": case["sql"],
                        "command_tag": result.statusmessage,
                    }
                )
                return entries[-1]

            created = execute("sql-0119")
            created["initial_rows"] = await_value(rows, [[1, "initial"]])
            with upstream.transaction():
                upstream.execute(
                    "UPDATE usage_records SET payload='streamed' WHERE id=1"
                )
                upstream.execute("INSERT INTO usage_records VALUES(2,'second')")
            created["streamed_rows"] = await_value(
                rows, [[1, "streamed"], [2, "second"]]
            )
            disabled = execute("sql-0122")
            disabled["enabled"] = db.execute(
                "SELECT subenabled FROM pg_subscription WHERE subname='usage_sub'"
            ).fetchone()[0]
            assert disabled["enabled"] is False
            await_value(
                lambda: upstream.execute(
                    "SELECT active FROM pg_replication_slots WHERE slot_name='usage_sub'"
                ).fetchone()[0],
                False,
            )
            upstream.execute("INSERT INTO usage_records VALUES(3,'paused')")
            assert rows() == [[1, "streamed"], [2, "second"]]
            enabled = execute("sql-0123")
            enabled["caught_up_rows"] = await_value(
                rows, [[1, "streamed"], [2, "second"], [3, "paused"]]
            )
            upstream.execute("DELETE FROM usage_records WHERE id=2")
            enabled["after_delete_rows"] = await_value(
                rows, [[1, "streamed"], [3, "paused"]]
            )
            dropped = execute("sql-0124")
            dropped["slot_count"] = upstream.execute(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name='usage_sub'"
            ).fetchone()[0]
            assert dropped["slot_count"] == 0
            execute("sql-0125")
            # Distinct subscription, existing rows retired before its COPY.
            db.execute("TRUNCATE usage_records")
            multi = execute("sql-0121")
            multi["records"] = await_value(rows, enabled["after_delete_rows"])
            multi["events"] = await_value(lambda: rows("usage_events"), [[9, "audit"]])
            db.execute("DROP SUBSCRIPTION usage_sub_multi")
            assert (
                upstream.execute(
                    "SELECT count(*) FROM pg_replication_slots"
                ).fetchone()[0]
                == 0
            )
    return {
        "reference": "PostgreSQL 18+ owned publisher and subscriber",
        "entries": entries,
    }


def run_mounted_antfly():
    with publisher() as (_, port):
        subprocess.run(
            [
                os.environ.get("ZIG", "zig"),
                "build",
                "antfly-api-sql-connected-subscription-test",
                "-Doptimize=Debug",
                "-j2",
                "--summary",
                "failures",
            ],
            cwd=ROOT / "zig",
            env=dict(
                os.environ,
                PGPORT=str(port),
                PGUSER=getpass.getuser(),
                PGCONNECT_TIMEOUT="3",
                ANTFLY_TEST_SUBSCRIPTION_PSQL=str(postgres_binary_dir() / "psql"),
            ),
            check=True,
            timeout=1800,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output", type=Path)
    destination.add_argument("--check", type=Path)
    destination.add_argument("--mounted-antfly", action="store_true")
    args = parser.parse_args()
    if args.mounted_antfly:
        run_mounted_antfly()
        return
    value = reference()
    if args.check is not None:
        if json.loads(args.check.read_text()) != value:
            raise SystemExit("connected-subscription PostgreSQL witness changed")
    else:
        args.output.write_text(json.dumps(value, indent=2) + "\n")
    print(
        f"Qualified {len(value['entries'])} unchanged connected-subscription originals"
    )


if __name__ == "__main__":
    main()
