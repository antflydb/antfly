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

"""Transaction ownership through the authenticated PostgreSQL listener."""

import os

import conftest as harness
import pytest


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("storage_engine", ["local", "lite"])
def test_pgwire_repeated_transaction_upserts(storage_engine):
    psycopg = pytest.importorskip("psycopg")
    binary = harness.resolve_binary_path(
        os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
    )
    server = harness.StandaloneAntflyServer(
        binary, "127.0.0.1", 0, pgwire=True, storage_engine=storage_engine
    )
    try:
        with psycopg.connect(
            host="127.0.0.1",
            port=server.pgwire_port,
            dbname="default",
            user="admin",
            password=harness.AUTH_BOOTSTRAP_PASSWORD,
            sslmode="disable",
            autocommit=True,
        ) as conn:
            conn.execute("CREATE TABLE repro (id TEXT PRIMARY KEY, v BIGINT)")
            upsert = (
                "INSERT INTO repro (id, v) VALUES (%s, %s) "
                "ON CONFLICT (id) DO UPDATE SET v = excluded.v"
            )
            # Exercise both ordinary and prepared extended-query statements on
            # the same connection; each transaction must release its budget.
            for prepare in (False, True):
                conn.execute("DELETE FROM repro")
                with conn.transaction():
                    conn.execute(upsert, ("k1", 1), prepare=prepare)
                    conn.execute(upsert, ("k2", 2), prepare=prepare)
                    conn.execute(upsert, ("k1", 3), prepare=prepare)
                assert conn.execute(
                    "SELECT id, v FROM repro ORDER BY id"
                ).fetchall() == [("k1", 3), ("k2", 2)]
                with conn.transaction():
                    conn.execute("INSERT INTO repro (id, v) VALUES ('k3', 4)")
                    conn.execute(upsert, ("k3", 5), prepare=prepare)
                    # Savepoints copy and restore staged requests. Neither view
                    # construction nor rollback can transfer scratch ownership.
                    conn.execute("SAVEPOINT before_update")
                    conn.execute(upsert, ("k3", 99), prepare=prepare)
                    conn.execute("ROLLBACK TO SAVEPOINT before_update")
                    conn.execute(upsert, ("k2", 6), prepare=prepare)
                assert conn.execute(
                    "SELECT id, v FROM repro ORDER BY id"
                ).fetchall() == [("k1", 3), ("k2", 6), ("k3", 5)]
                assert server.proc.poll() is None, harness._read_log_tail(
                    server.log_path
                )
    finally:
        server.stop()


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("storage_engine", ["local", "lite"])
@pytest.mark.parametrize("namespace", ["public", "allowed"])
def test_pgwire_scoped_filters_survive_mixed_qualified_bindings(
    storage_engine, namespace
):
    """Repeated bindings must retain one stable policy across Execute batches."""
    import requests

    psycopg = pytest.importorskip("psycopg")
    binary = harness.resolve_binary_path(
        os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
    )
    server = harness.StandaloneAntflyServer(
        binary, "127.0.0.1", 0, pgwire=True, storage_engine=storage_engine
    )
    try:
        connection = dict(
            host="127.0.0.1",
            port=server.pgwire_port,
            dbname="default",
            sslmode="disable",
            autocommit=True,
        )
        with psycopg.connect(
            **connection, user="admin", password=harness.AUTH_BOOTSTRAP_PASSWORD
        ) as admin:
            if namespace != "public":
                admin.execute(f"CREATE SCHEMA {namespace}")
            admin.execute(
                f"CREATE TABLE {namespace}.scope_docs "
                "(id TEXT PRIMARY KEY, tenant_id TEXT, status TEXT)"
            )
            admin.execute(
                f"INSERT INTO {namespace}.scope_docs (id, tenant_id, status) VALUES "
                "('own-open', 'a', 'open'), ('own-closed', 'a', 'closed'), "
                "('other-open', 'b', 'open')"
            )
        with requests.Session() as api:
            api.auth = ("admin", harness.AUTH_BOOTSTRAP_PASSWORD)
            response = api.post(
                server.url + "/auth/v1/users/scoped_reader",
                json={
                    "password": "reader-secret",
                    "initial_policies": [
                        {
                            "resource_type": "table",
                            "table_target": {
                                "database": "default",
                                "namespace": namespace,
                                "table": "scope_docs",
                            },
                            "type": "read",
                        }
                    ],
                },
                timeout=30,
            )
            response.raise_for_status()
            for scope, predicate in (
                ("*", {"term": {"tenant_id": "a"}}),
                (
                    f"scope_docs?database=default&namespace={namespace}",
                    {"term": {"status": "open"}},
                ),
            ):
                response = api.put(
                    server.url + f"/auth/v1/users/scoped_reader/row-filters/{scope}",
                    json=predicate,
                    timeout=30,
                )
                response.raise_for_status()
            if namespace != "public":
                response = api.put(
                    server.url + "/auth/v1/users/scoped_reader/row-filters/*"
                    "?database=default&namespace=public&all_tables=true",
                    json={"term": {"visibility": "public"}},
                    timeout=30,
                )
                response.raise_for_status()
        with psycopg.connect(
            **connection, user="scoped_reader", password="reader-secret"
        ) as reader:
            relation = (
                "scope_docs" if namespace == "public" else f"{namespace}.scope_docs"
            )
            query = (
                f'SELECT a.id FROM {relation} a JOIN "default"."{namespace}".scope_docs b '
                "ON a.id = b.id ORDER BY a.id"
            )
            for prepare in (False, True, True):
                assert reader.execute(query, prepare=prepare).fetchall() == [
                    ("own-open",)
                ]
    finally:
        server.stop()
