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

"""Real driver conformance; supply --binary to start an isolated authenticated node.

Build @antfly/sdk first for the Kysely client (pnpm --dir ts --filter @antfly/sdk build).

uv run --no-project --with requests --with pytest --with 'psycopg[binary]==3.3.6' \
  --with asyncpg==0.31.0 python scripts/test_pgwire_drivers.py --binary zig/zig-out/bin/antfly
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLIENTS = ROOT / "scripts" / "pgwire_drivers"


def exercise_cached_setting_plans(server, password):
    import psycopg
    import requests

    def define_setting(value):
        response = requests.post(
            server.api_url + "/settings",
            auth=("admin", password),
            json={
                "put": {
                    "name": "app.cached_driver",
                    "kind": "string",
                    "default": {"string": value},
                }
            },
            timeout=10,
        )
        response.raise_for_status()

    define_setting("before")
    url = f"postgres://admin:{password}@127.0.0.1:{server.pgwire_port}/default"
    query = "SELECT current_setting('app.cached_driver')"
    with (
        psycopg.connect(url, autocommit=True, prepare_threshold=5) as conn,
        psycopg.connect(url, autocommit=True, prepare_threshold=None) as migrator,
        psycopg.ClientCursor(conn) as cursor,
    ):
        cursor.execute("PREPARE cached_setting AS " + query)

        def assert_cached(value, expected_name=None):
            cursor.execute("EXECUTE cached_setting")
            assert cursor.fetchone() == (value,)
            assert conn.execute(query, prepare=True).fetchone() == (value,)
            # Inspect the pinned driver's cache: prepare=True alone cannot
            # prove reuse, because prepare_threshold=None overrides it.
            name = conn._prepared._names.get((query.encode(), ()))
            assert name is not None, "settings query was not cached by psycopg"
            if expected_name is not None:
                assert name == expected_name, "settings query was prepared again"
            return name

        prepared_name = assert_cached("before")
        # DDL on another connection keeps the driver's cache intact.
        migrator.execute("CREATE TABLE settings_unrelated (id TEXT PRIMARY KEY)")
        assert_cached("before", prepared_name)
        # A compatible setting definition update also requires a fresh guard.
        define_setting("after")
        assert_cached("after", prepared_name)
    print("SQL and driver cached settings survive catalog changes: PASS", flush=True)


def exercise_bound_portals(server, password):
    """Commit DDL after the client receives BindComplete and before Execute."""
    import socket
    import struct

    import psycopg
    import requests

    def message(tag, data=b""):
        return tag + struct.pack("!I", 4 + len(data)) + data

    def receive(reader):
        tag = reader.read(1)
        size = struct.unpack("!I", reader.read(4))[0]
        assert 4 <= size <= 65536, size
        data = reader.read(size - 4)
        assert len(data) == size - 4
        return tag, data

    url = f"postgres://admin:{password}@127.0.0.1:{server.pgwire_port}/default"
    with (
        psycopg.connect(url, autocommit=True, sslmode="disable") as session,
        psycopg.connect(url, autocommit=True) as migrator,
        socket.fromfd(
            session.pgconn.socket, socket.AF_INET, socket.SOCK_STREAM
        ) as wire,
        wire.makefile("rb") as reader,
    ):
        wire.settimeout(10)
        create = "CREATE TABLE portal_target (id TEXT, n BIGINT)"
        insert = "INSERT INTO portal_target (id, n) VALUES ('a', 0)"
        migrator.execute(create)
        migrator.execute(insert)

        def parse(name, sql):
            body = name.encode() + b"\0" + sql.encode() + bytes(3)
            wire.sendall(message(b"P", body) + message(b"H"))
            frame = receive(reader)
            assert frame == (b"1", b""), (sql, frame)

        def bind(name):
            body = b"portal\0" + name.encode() + bytes(7)
            wire.sendall(message(b"B", body) + message(b"H"))
            frame = receive(reader)
            assert frame == (b"2", b""), (name, frame)

        def execute(expected_error=None):
            wire.sendall(message(b"E", b"portal\0" + bytes(4)) + message(b"S"))
            errors = []
            while True:
                tag, data = receive(reader)
                if tag == b"E":
                    errors.append(data.split(b"\0"))
                if tag == b"Z":
                    break
            if expected_error:
                assert len(errors) == 1 and expected_error in errors[0], errors
            else:
                assert not errors, errors

        for name, query in (
            ("constant", "SELECT 1"),
            ("derived", "WITH c AS (SELECT 1 AS n) SELECT n FROM c"),
        ):
            parse(name, query)
            bind(name)
            execute()

        parse("update", "UPDATE portal_target SET n = 9 WHERE id = 'a'")
        for migration in ("alter", "replace", "reuse"):
            bind("update")
            if migration == "alter":
                migrator.execute("ALTER TABLE portal_target ADD COLUMN note TEXT")
            if migration == "replace":
                migrator.execute("DROP TABLE portal_target")
                migrator.execute(create)
                migrator.execute(insert)
            execute(b"C40001" if migration == "replace" else None)
            assert migrator.execute("SELECT n FROM portal_target").fetchone() == (
                0 if migration == "replace" else 9,
            )

        parse("drop", "DROP TABLE portal_target")
        bind("drop")
        migrator.execute("DROP TABLE portal_target")
        migrator.execute(create)
        migrator.execute(insert)
        execute(b"C40001")
        assert migrator.execute("SELECT n FROM portal_target").fetchone() == (0,)

        parse("conditional", "DROP TABLE IF EXISTS portal_absent")
        bind("conditional")
        migrator.execute("CREATE TABLE portal_absent (n BIGINT)")
        execute(b"C40001")
        assert migrator.execute("SELECT COUNT(*) FROM portal_absent").fetchone() == (0,)

        parse("truncate", "TRUNCATE TABLE portal_target, portal_absent")
        bind("truncate")
        migrator.execute("DROP TABLE portal_absent")
        migrator.execute("CREATE TABLE portal_absent (n BIGINT)")
        execute(b"C40001")
        assert migrator.execute("SELECT n FROM portal_target").fetchone() == (0,)

        parse("policy", "DROP POLICY IF EXISTS portal_policy ON portal_target")
        bind("policy")
        policy = "CREATE POLICY portal_policy ON portal_target USING (n >= 0)"
        migrator.execute(policy)
        execute(b"C40001")
        bind("policy")
        migrator.execute("DROP POLICY portal_policy ON portal_target")
        migrator.execute(policy)
        execute(b"C40001")
        migrator.execute("DROP POLICY portal_policy ON portal_target")

        migrator.execute("ALTER TABLE portal_target ADD COLUMN scratch TEXT")
        parse("column", "ALTER TABLE portal_target DROP COLUMN scratch")
        bind("column")
        migrator.execute("ALTER TABLE portal_target DROP COLUMN scratch")
        migrator.execute("ALTER TABLE portal_target ADD COLUMN scratch BIGINT")
        execute(b"C40001")
        assert migrator.execute("SELECT scratch FROM portal_target").fetchone() == (
            None,
        )

        constraint = (
            "ALTER TABLE portal_target ADD CONSTRAINT nonnegative CHECK (n >= 0)"
        )
        drop_constraint = "ALTER TABLE portal_target DROP CONSTRAINT nonnegative"
        migrator.execute(constraint)
        parse("constraint", drop_constraint)
        bind("constraint")
        migrator.execute(drop_constraint)
        migrator.execute(constraint)
        execute(b"C40001")
        migrator.execute(drop_constraint)

        migrator.execute("CREATE TABLE fk_destination (id TEXT PRIMARY KEY)")
        parse(
            "fk",
            "ALTER TABLE portal_target ADD CONSTRAINT bound_fk FOREIGN KEY (id) REFERENCES fk_destination(id)",
        )
        bind("fk")
        migrator.execute("DROP TABLE fk_destination")
        migrator.execute("CREATE TABLE fk_destination (id TEXT PRIMARY KEY)")
        execute(b"C40001")

        migrator.execute("CREATE DATABASE bound_placement")
        space = "CREATE TABLESPACE portal_space LOCATION 's3://bucket/path'"
        migrator.execute(space)
        parse("placement", "ALTER DATABASE bound_placement SET TABLESPACE portal_space")
        bind("placement")
        migrator.execute("DROP TABLESPACE portal_space")
        migrator.execute(space)
        execute(b"C40001")
        # Removing it also proves that the replacement never acquired the database.
        migrator.execute("DROP TABLESPACE portal_space")

        create_index = "CREATE INDEX portal_n ON portal_target (n)"
        migrator.execute(create_index)
        parse("index", "DROP INDEX portal_n")
        bind("index")
        migrator.execute("ALTER TABLE portal_target ADD COLUMN unrelated TEXT")
        execute()
        migrator.execute(create_index)
        bind("index")
        migrator.execute("DROP INDEX portal_n")
        migrator.execute(create_index)
        execute(b"C40001")
        migrator.execute("DROP INDEX portal_n")

        # A fresh Bind acquires the new identity and unrelated DDL is compatible.
        bind("drop")
        migrator.execute("CREATE TABLE graph_other (id TEXT, n BIGINT)")
        execute()
        migrator.execute(create)

        graph_url = server.api_url + "/graphs/portal_graph"

        def definition(table):
            return {"labels": [{"name": "D", "table": table}], "relationships": []}

        with requests.Session() as http:
            http.auth = ("admin", password)
            created = http.post(graph_url, json=definition("portal_target"), timeout=10)
            created.raise_for_status()
            parse(
                "graph",
                "SELECT q.n FROM GRAPH_TABLE(portal_graph MATCH(n IS D) "
                "COLUMNS(n.n AS n)) q",
            )
            bind("graph")
            changed = http.put(
                graph_url,
                json={
                    "expected_revision": "1",
                    "definition": definition("graph_other"),
                },
                timeout=10,
            )
            changed.raise_for_status()
            execute(b"C40001")
            bind("graph")
            execute()

    print("Bound DML, DDL and graph identity across catalog changes: PASS", flush=True)


def exercise_cached_search_path(server, password):
    import psycopg

    url = f"postgres://admin:{password}@127.0.0.1:{server.pgwire_port}/default"
    with (
        psycopg.connect(url, autocommit=True) as conn,
        psycopg.ClientCursor(conn) as sql,
    ):
        for schema, column, value in (
            ("path_first", "n", 1),
            ("path_second", "n", 2),
            ("path_renamed", "renamed", 3),
        ):
            sql.execute(f"CREATE SCHEMA {schema}")
            sql.execute(f"CREATE TABLE {schema}.cached_path (id TEXT, {column} BIGINT)")
            sql.execute(
                f"INSERT INTO {schema}.cached_path (id, {column}) VALUES ('a', {value})"
            )

        query = "SELECT * FROM cached_path"
        sql.execute("SET search_path TO path_first")
        sql.execute("PREPARE path_plan AS " + query)
        for schema, value in (("path_first", 1), ("path_second", 2)):
            sql.execute(f"SET search_path TO {schema}")
            sql.execute("EXECUTE path_plan")
            assert sql.fetchone() == ("a", value)
            assert conn.execute(query, prepare=True).fetchone() == ("a", value)

        sql.execute("CREATE TABLE path_second.only_here (n BIGINT)")
        sql.execute("SET search_path TO path_first, path_second")
        conn.execute("ALTER TABLE only_here ADD COLUMN k TEXT", prepare=True)
        sql.execute("SELECT k FROM path_second.only_here")
        conn.execute("DROP TABLE IF EXISTS only_here", prepare=True)
        try:
            sql.execute("SELECT k FROM path_second.only_here")
        except psycopg.errors.UndefinedTable:
            pass
        else:
            raise AssertionError("DROP TABLE skipped the second search_path entry")

        sql.execute("SET search_path TO path_renamed")
        for execute in (
            lambda: sql.execute("EXECUTE path_plan"),
            lambda: conn.execute(query, prepare=True),
        ):
            try:
                execute()
            except psycopg.Error as exc:
                assert exc.sqlstate == "0A000", exc
            else:
                raise AssertionError("cached result accepted a changed column label")
    print(
        "SQL and wire statements follow search_path and preserve column labels: PASS",
        flush=True,
    )


def run_clients(url, drivers, http_url=None):
    env = {**os.environ, "ANTFLY_PGWIRE_URL": url}
    if http_url:
        env["ANTFLY_HTTP_URL"] = http_url
    if any(driver in ("node", "kysely") for driver in drivers):
        subprocess.run(
            ["npm", "ci", "--ignore-scripts"],
            cwd=CLIENTS,
            env=env,
            check=True,
            timeout=300,
        )
    commands = {
        "python": [(CLIENTS, [sys.executable, "python.py"])],
        "node": [
            (CLIENTS, ["node", "node.mjs"]),
        ],
        "kysely": [
            (CLIENTS, ["node", "--test", "worker-progress.test.mjs"]),
            (CLIENTS, ["node", "kysely.mjs"]),
        ],
        "go": [(CLIENTS / "go", ["go", "run", "."])],
        "rust": [(CLIENTS / "rust", ["cargo", "run", "--locked"])],
    }
    for driver in drivers:
        for cwd, command in commands[driver]:
            subprocess.run(command, cwd=cwd, env=env, check=True, timeout=300)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary", type=Path)
    target.add_argument(
        "--url",
        help="PostgreSQL connection URL for an existing authenticated Antfly node",
    )
    parser.add_argument(
        "--http-url", help="Matching Antfly HTTP URL (required for Kysely with --url)"
    )
    parser.add_argument(
        "--drivers",
        nargs="+",
        choices=["python", "node", "go", "rust", "kysely"],
        default=["python", "node", "go", "rust", "kysely"],
    )
    args = parser.parse_args()
    if args.url:
        if "kysely" in args.drivers and not args.http_url:
            parser.error("--http-url is required for the Kysely driver with --url")
        run_clients(args.url, args.drivers, args.http_url)
        return
    sys.path.insert(0, str(ROOT / "zig" / "e2e" / "antfly"))
    import psycopg
    from conftest import AUTH_BOOTSTRAP_PASSWORD, StandaloneAntflyServer

    startup_ddl = iter(
        (
            "CREATE TABLE ready_immediately (id TEXT PRIMARY KEY)",
            "CREATE INDEX ready_after_restart ON ready_immediately(id)",
        )
    )

    def first_ddl(server):
        with psycopg.connect(
            f"postgres://admin:{AUTH_BOOTSTRAP_PASSWORD}@127.0.0.1:{server.pgwire_port}/default",
            autocommit=True,
        ) as conn:
            conn.execute(next(startup_ddl))

    server = StandaloneAntflyServer(
        str(args.binary.resolve()),
        "127.0.0.1",
        0,
        pgwire=True,
        on_first_ready=first_ddl,
    )
    failed = False
    try:
        server.restart()
        print("cold-start and restart DDL without retries: PASS", flush=True)
        exercise_bound_portals(server, AUTH_BOOTSTRAP_PASSWORD)
        exercise_cached_search_path(server, AUTH_BOOTSTRAP_PASSWORD)
        exercise_cached_setting_plans(server, AUTH_BOOTSTRAP_PASSWORD)
        run_clients(
            f"postgres://admin:{AUTH_BOOTSTRAP_PASSWORD}@127.0.0.1:{server.pgwire_port}/default",
            args.drivers,
            server.url,
        )
    except BaseException:
        failed = True
        print(server.log_path.read_text()[-16000:], file=sys.stderr)
        raise
    finally:
        server.stop(test_failed=failed)


if __name__ == "__main__":
    main()
