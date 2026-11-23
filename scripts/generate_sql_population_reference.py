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
import subprocess
import time
from pathlib import Path

from generate_sql_postgres_reference import (
    FIXTURES,
    create_table,
    encoded,
    postgres,
    postgres_binary_dir,
    properties,
)

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


PERSISTENCE_IDS = frozenset(
    f"sql-{number:04d}" for number in (360, 361, 362, 363, 367, 368, 369, 568, 569)
)


def persistence_rows(connection, target):
    from psycopg import sql

    cursor = connection.execute(
        sql.SQL("SELECT * FROM {} ORDER BY 1").format(sql.Identifier(target))
    )
    return {
        "columns": [[column.name, column.type_code] for column in cursor.description],
        "rows": [[encoded(value) for value in row] for row in cursor.fetchall()],
    }


def persistence_diagnostic(connection, statement, parameters=None, *, commit=False):
    import psycopg

    try:
        with connection.transaction(force_rollback=not commit):
            connection.execute(statement, parameters)
    except psycopg.Error as error:
        if error.sqlstate is None:
            raise
        return error.sqlstate
    return None


def like_semantics(connection):
    """Lifecycle oracle for declarations, ownership and authorization.

    These probes accompany the unchanged LIKE original. They must not become
    substitute originals or independently increase the implementation count.
    """
    from psycopg import sql

    result = {}
    connection.execute("CREATE TABLE like_parent (id integer PRIMARY KEY)")
    connection.execute("INSERT INTO like_parent VALUES (1),(2)")
    connection.execute(
        "CREATE TABLE like_source ("
        "id bigint GENERATED ALWAYS AS IDENTITY (START WITH 42 INCREMENT BY 3),"
        "serial_id bigserial, quantity integer NOT NULL DEFAULT 2,"
        "doubled integer GENERATED ALWAYS AS (quantity * 2) STORED,"
        "PRIMARY KEY(id), UNIQUE(quantity) DEFERRABLE INITIALLY DEFERRED,"
        "CHECK(quantity > 0), FOREIGN KEY(quantity) REFERENCES like_parent(id))"
    )
    connection.execute("COMMENT ON TABLE like_source IS 'source table only'")
    connection.execute("COMMENT ON COLUMN like_source.quantity IS 'copied quantity'")
    connection.execute(
        "COMMENT ON INDEX like_source_quantity_key IS 'copied index comment'"
    )
    connection.execute("ALTER TABLE like_source ENABLE ROW LEVEL SECURITY")
    connection.execute("CREATE POLICY like_source_deny ON like_source USING (false)")
    result["source_first"] = list(
        connection.execute(
            "INSERT INTO like_source DEFAULT VALUES RETURNING id,serial_id,doubled"
        ).fetchone()
    )
    connection.execute("CREATE TABLE like_copy (LIKE like_source INCLUDING ALL)")
    result["copied_security"] = list(
        connection.execute(
            "SELECT relrowsecurity,relforcerowsecurity,"
            "(SELECT count(*) FROM pg_policy WHERE polrelid=c.oid) "
            "FROM pg_class c WHERE c.oid='like_copy'::regclass"
        ).fetchone()
    )
    original_oid = connection.execute("SELECT 'like_copy'::regclass::oid").fetchone()[0]
    result["conditional_missing_source"] = connection.execute(
        "CREATE TABLE IF NOT EXISTS like_copy (LIKE absent_like_source INCLUDING ALL)"
    ).statusmessage
    result["conditional_preserves_identity"] = (
        connection.execute("SELECT 'like_copy'::regclass::oid").fetchone()[0]
        == original_oid
    )
    # A missing parent proves that LIKE did not copy the foreign key. The
    # identity starts independently, while SERIAL uses the original counter.
    result["copy_first"] = list(
        connection.execute(
            "INSERT INTO like_copy(quantity) VALUES (9) RETURNING id,serial_id,doubled"
        ).fetchone()
    )
    result["source_second"] = list(
        connection.execute(
            "INSERT INTO like_source(quantity) VALUES (1) RETURNING id,serial_id,doubled"
        ).fetchone()
    )
    result["constraints"] = [
        list(row)
        for row in connection.execute(
            "SELECT contype,condeferrable,condeferred,convalidated,"
            "pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid='like_copy'::regclass ORDER BY contype,conname"
        )
    ]
    result["comments"] = list(
        connection.execute(
            "SELECT obj_description('like_copy'::regclass,'pg_class'),"
            "col_description('like_copy'::regclass,3),"
            "obj_description('like_copy_quantity_key'::regclass,'pg_class')"
        ).fetchone()
    )
    result["not_null_sqlstate"] = persistence_diagnostic(
        connection, "INSERT INTO like_copy(quantity) VALUES (NULL)"
    )
    result["check_sqlstate"] = persistence_diagnostic(
        connection, "INSERT INTO like_copy(quantity) VALUES (-1)"
    )
    result["unique_sqlstate"] = persistence_diagnostic(
        connection, "INSERT INTO like_copy(quantity) VALUES (9)", commit=True
    )
    # Sequence allocations deliberately do not roll back. Record the shared
    # SERIAL counter before TRUNCATE, then prove only the fresh owned identity
    # was reset by RESTART IDENTITY.
    before_serial = connection.execute(
        "SELECT last_value FROM like_source_serial_id_seq"
    ).fetchone()[0]
    connection.execute("TRUNCATE like_copy RESTART IDENTITY")
    after = connection.execute(
        "INSERT INTO like_copy(quantity) VALUES (8) RETURNING id,serial_id,doubled"
    ).fetchone()
    result["restart_identity"] = {
        "identity": after[0],
        "serial_advanced": after[1] == before_serial + 1,
        "generated": after[2],
    }
    result["source_drop_restrict_sqlstate"] = persistence_diagnostic(
        connection, "DROP TABLE like_source"
    )
    connection.execute("CREATE ROLE like_reader")
    connection.execute("GRANT USAGE,CREATE ON SCHEMA public TO like_reader")
    connection.execute("GRANT SELECT(id) ON like_source TO like_reader")
    connection.execute("SET ROLE like_reader")
    try:
        result["column_select_sqlstate"] = persistence_diagnostic(
            connection, "CREATE TABLE column_only_copy (LIKE like_source)"
        )
    finally:
        connection.execute("RESET ROLE")
    connection.execute("GRANT SELECT ON like_source TO like_reader")
    connection.execute("SET ROLE like_reader")
    try:
        result["table_select_command"] = connection.execute(
            "CREATE TABLE table_select_copy (LIKE like_source)"
        ).statusmessage
        result["table_select_visible_rows"] = connection.execute(
            "SELECT count(*) FROM like_source"
        ).fetchone()[0]
    finally:
        connection.execute("RESET ROLE")
    connection.execute("CREATE TABLE like_left (left_id integer NOT NULL DEFAULT 7)")
    connection.execute("CREATE TABLE like_right (payload text DEFAULT 'right')")
    connection.execute(
        "CREATE TABLE like_ordered (prefix boolean,"
        "LIKE like_left INCLUDING DEFAULTS,middle text,"
        "LIKE like_right INCLUDING ALL EXCLUDING DEFAULTS,tail jsonb)"
    )
    connection.execute("INSERT INTO like_ordered DEFAULT VALUES")
    result["ordered_sources"] = persistence_rows(connection, "like_ordered")
    connection.execute(
        "CREATE TABLE like_excluded (LIKE like_source INCLUDING ALL "
        "EXCLUDING DEFAULTS EXCLUDING GENERATED EXCLUDING IDENTITY)"
    )
    connection.execute(
        "INSERT INTO like_excluded(id,serial_id,quantity,doubled) VALUES (900,901,1,123)"
    )
    result["excluded_values"] = persistence_rows(connection, "like_excluded")
    connection.execute("DROP TABLE like_copy")
    result["source_drop_after_reference_retirement"] = connection.execute(
        sql.SQL("DROP TABLE {} ").format(sql.Identifier("like_source"))
    ).statusmessage
    return result


def await_temporary_retirement(connection, qualified):
    # Sending Terminate closes the client before server-side catalog cleanup
    # finishes. Observe completion with a bounded deadline, not a fixed sleep.
    deadline = time.monotonic() + 2
    while True:
        if connection.execute(
            "SELECT to_regclass(%s) IS NULL", (qualified,)
        ).fetchone()[0]:
            return True
        if time.monotonic() >= deadline:
            raise AssertionError("temporary relation survived its owning session")
        time.sleep(0.005)


def restart_persistence_server(dsn, directory, mode):
    import psycopg

    if mode not in ("fast", "immediate"):
        raise ValueError("restart mode must be fast or immediate")
    subprocess.run(
        [
            postgres_binary_dir() / "pg_ctl",
            "-D",
            directory,
            "-l",
            directory.parent / "server.log",
            "-m",
            mode,
            "-w",
            "restart",
        ],
        check=True,
        capture_output=True,
        timeout=30,
    )
    resumed = psycopg.connect(dsn, autocommit=True)
    try:
        if (
            Path(resumed.execute("SHOW data_directory").fetchone()[0]).resolve()
            != directory
        ):
            raise RuntimeError("restart did not reconnect to the owned oracle server")
        resumed.execute("SET statement_timeout = '2s'")
        resumed.execute("SET lock_timeout = '250ms'")
        return resumed
    except BaseException:
        resumed.close()
        raise


def generate_persistence():
    """Real sessions and clean/crash restarts; no SQL is lowered or substituted."""
    import psycopg
    from psycopg import sql

    inventory_bytes = (FIXTURES / "sql_parity_inventory.json").read_bytes()
    inventory = json.loads(inventory_bytes)
    result = {
        "format": 1,
        "inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
        "entries": [],
    }
    for case in inventory["entries"]:
        if case["id"] not in PERSISTENCE_IDS:
            continue
        if case["params"]:
            raise ValueError("persistence original unexpectedly requires parameters")
        with postgres() as owner:
            major = int(owner.execute("SHOW server_version_num").fetchone()[0]) // 10000
            if major < 18:
                raise ValueError("persistence oracle requires PostgreSQL 18 or newer")
            result["postgres_major"] = major
            directory = Path(
                owner.execute("SHOW data_directory").fetchone()[0]
            ).resolve()
            if directory.parent != Path(owner.info.host).resolve():
                raise RuntimeError("refusing to restart a foreign PostgreSQL server")
            dsn = owner.info.dsn
            owner.execute(
                "CREATE TABLE usage_records (id uuid PRIMARY KEY, status text NOT NULL DEFAULT 'queued', CONSTRAINT status_nonempty CHECK (length(status)>0))"
            )
            for number, status in ((1, "closed"), (2, "open"), (3, "closed")):
                owner.execute(
                    "INSERT INTO usage_records VALUES (%s,%s)",
                    (f"00000000-0000-0000-0000-{number:012d}", status),
                )
            command = owner.execute(case["sql"]).statusmessage
            target = (
                "usage_session_archive"
                if case["id"] in ("sql-0367", "sql-0568")
                else "usage_ingest_archive"
                if case["id"] in ("sql-0368", "sql-0569")
                else "usage_records_copy"
                if case["id"] == "sql-0369"
                else "usage_session_records"
                if case["id"] in ("sql-0360", "sql-0361")
                else "usage_ingest_records"
            )
            oid, persistence, namespace = owner.execute(
                "SELECT c.oid,c.relpersistence,n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=%s::regclass",
                (target,),
            ).fetchone()
            entry = {key: case[key] for key in ("id", "source_entry_sha256", "sql")}
            entry.update(command_tag=command, persistence=persistence)
            if "IF NOT EXISTS" in case["sql"]:
                entry["repeat_command_tag"] = owner.execute(case["sql"]).statusmessage
                entry["repeat_preserves_identity"] = (
                    owner.execute("SELECT %s::regclass::oid", (target,)).fetchone()[0]
                    == oid
                )
            entry["populated"] = persistence_rows(owner, target)
            insert = (
                sql.SQL("INSERT INTO {} (id,payload) VALUES (%s,%s::jsonb)")
                if case["id"] in ("sql-0362", "sql-0363")
                else sql.SQL("INSERT INTO {} (id,status) VALUES (%s,%s)")
            )
            parameters = (
                "00000000-0000-0000-0000-000000000009",
                '{"kind":"ingest"}'
                if case["id"] in ("sql-0362", "sql-0363")
                else "retained",
            )
            owner.execute(insert.format(sql.Identifier(target)), parameters)
            entry["duplicate_sqlstate"] = persistence_diagnostic(
                owner, insert.format(sql.Identifier(target)), parameters
            )
            if case["id"] == "sql-0369":
                entry["default_probe"] = owner.execute(
                    "INSERT INTO usage_records_copy(id) VALUES ('00000000-0000-0000-0000-000000000010') RETURNING status"
                ).fetchone()[0]
                entry["check_sqlstate"] = persistence_diagnostic(
                    owner,
                    "INSERT INTO usage_records_copy(id,status) VALUES ('00000000-0000-0000-0000-000000000011','')",
                )
            # Both default TEMP persistence and UNLOGGED/PERMANENT tables
            # retain committed data; an aborted write never becomes visible.
            with owner.transaction(force_rollback=True):
                owner.execute(
                    insert.format(sql.Identifier(target)),
                    ("00000000-0000-0000-0000-000000000012", parameters[1]),
                )
            entry["before_disconnect"] = persistence_rows(owner, target)
            if case["id"] == "sql-0369":
                entry["like_semantics"] = like_semantics(owner)
            with psycopg.connect(dsn, autocommit=True) as observer:
                observer.execute("SET statement_timeout = '2s'")
                observer.execute("SET lock_timeout = '250ms'")
                if persistence == "t":
                    entry["other_session_sqlstate"] = persistence_diagnostic(
                        observer,
                        sql.SQL("SELECT * FROM {}").format(sql.Identifier(target)),
                    )
                    entry["other_session_qualified_sqlstate"] = persistence_diagnostic(
                        observer,
                        sql.SQL("SELECT * FROM {}.{}").format(
                            sql.Identifier(namespace), sql.Identifier(target)
                        ),
                    )
                    owner.close()
                    entry["disconnect_removes_relation"] = await_temporary_retirement(
                        observer, f"{namespace}.{target}"
                    )
                else:
                    entry["other_session"] = persistence_rows(observer, target)
                    owner.close()
            with restart_persistence_server(dsn, directory, "fast") as resumed:
                if persistence == "t":
                    entry["clean_restart_absent"] = resumed.execute(
                        "SELECT to_regclass(%s) IS NULL", (target,)
                    ).fetchone()[0]
                else:
                    entry["clean_restart"] = persistence_rows(resumed, target)
            with restart_persistence_server(dsn, directory, "immediate") as recovered:
                if persistence == "t":
                    entry["crash_restart_absent"] = recovered.execute(
                        "SELECT to_regclass(%s) IS NULL", (target,)
                    ).fetchone()[0]
                else:
                    entry["crash_restart"] = persistence_rows(recovered, target)
            result["entries"].append(entry)
    if {entry["id"] for entry in result["entries"]} != PERSISTENCE_IDS:
        raise ValueError("original persistence inventory changed")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--persistence",
        action="store_true",
        help="Verify original temporary, unlogged and LIKE table lifecycles",
    )
    args = parser.parse_args()
    result = generate_persistence() if args.persistence else generate()
    if args.check:
        existing = json.loads(args.output.read_text())
        # Patch releases are provenance; SQL results and input hashes must agree.
        existing.pop("postgres_version", None)
        actual = dict(result)
        actual.pop("postgres_version", None)
        if existing != actual:
            raise SystemExit("PostgreSQL population reference changed")
        print(
            "PostgreSQL table persistence reference: all nine unchanged originals match"
            if args.persistence
            else "PostgreSQL population reference: all eleven unchanged originals and materialized diagnostics match"
        )
    else:
        args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
