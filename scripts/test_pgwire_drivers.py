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


def run_clients(url, drivers):
    env = {**os.environ, "ANTFLY_PGWIRE_URL": url}
    commands = {
        "python": [(CLIENTS, [sys.executable, "python.py"])],
        "node": [
            (CLIENTS, ["npm", "ci", "--ignore-scripts"]),
            (CLIENTS, ["node", "node.mjs"]),
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
        "--drivers",
        nargs="+",
        choices=["python", "node", "go", "rust"],
        default=["python", "node", "go", "rust"],
    )
    args = parser.parse_args()
    if args.url:
        run_clients(args.url, args.drivers)
        return
    sys.path.insert(0, str(ROOT / "zig" / "e2e" / "antfly"))
    from conftest import AUTH_BOOTSTRAP_PASSWORD, StandaloneAntflyServer

    server = StandaloneAntflyServer(
        str(args.binary.resolve()), "127.0.0.1", 0, pgwire=True
    )
    failed = False
    try:
        # First SQL immediately after the fixture observes HTTP readiness: no
        # publication retry loop may hide a cold-start readiness regression.
        import psycopg

        with psycopg.connect(
            f"postgres://admin:{AUTH_BOOTSTRAP_PASSWORD}@127.0.0.1:{server.pgwire_port}/default",
            autocommit=True,
        ) as conn:
            conn.execute("CREATE TABLE ready_immediately (id TEXT PRIMARY KEY)")
        server.restart()
        with psycopg.connect(
            f"postgres://admin:{AUTH_BOOTSTRAP_PASSWORD}@127.0.0.1:{server.pgwire_port}/default",
            autocommit=True,
        ) as conn:
            conn.execute("CREATE INDEX ready_after_restart ON ready_immediately(id)")
        print("cold-start and restart DDL without retries: PASS", flush=True)
        exercise_cached_setting_plans(server, AUTH_BOOTSTRAP_PASSWORD)
        run_clients(
            f"postgres://admin:{AUTH_BOOTSTRAP_PASSWORD}@127.0.0.1:{server.pgwire_port}/default",
            args.drivers,
        )
    except BaseException:
        failed = True
        print(server.log_path.read_text()[-16000:], file=sys.stderr)
        raise
    finally:
        server.stop(test_failed=failed)


if __name__ == "__main__":
    main()
