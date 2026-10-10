# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in managed PostgreSQL qualification against a native Antfly daemon."""

import json
import os
import socket
import subprocess
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError

import pytest

from antfly_lake_maintenance.managed_sources import (
    AntflyTarget,
    ManagedSources,
    PostgreSQLCDC,
)
from antfly_lake_maintenance.store import Store


@pytest.mark.skipif(
    not os.environ.get("ANTFLY_NATIVE_BINARY")
    or not os.environ.get("ANTFLY_MANAGED_PG_DSN"),
    reason="set native binary and disposable logical-replication PostgreSQL DSN",
)
def test_real_native_managed_postgres_cutover_updates_and_teardown(tmp_path):
    import psycopg
    from psycopg import sql

    dsn = os.environ["ANTFLY_MANAGED_PG_DSN"]
    namespace = "antfly_qualification_" + uuid.uuid4().hex
    with psycopg.connect(dsn) as connection:
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace))
        )
        connection.execute(
            sql.SQL("CREATE TABLE {}.items(id bigint PRIMARY KEY, body text)").format(
                sql.Identifier(namespace)
            )
        )
        connection.execute(
            sql.SQL("INSERT INTO {}.items VALUES(1,'original')").format(
                sql.Identifier(namespace)
            )
        )
    sockets, ports = [], []
    for _ in range(8):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        sockets.append(listener)
        ports.append(listener.getsockname()[1])
    for listener in sockets:
        listener.close()
    metadata_api, metadata_raft = ports[:2]
    port = ports[2]
    endpoint = f"http://127.0.0.1:{port}/db/v1"
    target = AntflyTarget(endpoint)
    env = {
        **os.environ,
        "ANTFLY_MANAGED_PG_DSN": dsn,
        "ANTFLY_INTERNAL_SERVICE_SECRET": "qualification_internal_service_secret_123456789",
        "ANTFLY_INTERNAL_SERVICE_ISSUER": "managed-qualification",
    }
    binary = str(Path(os.environ["ANTFLY_NATIVE_BINARY"]).resolve())
    logs, processes = [], []

    def node_config(name):
        path = tmp_path / (name + ".json")
        path.write_text(
            json.dumps(
                {
                    "deployment_mode": "distributed",
                    "storage": {
                        "engine": "local",
                        "local": {"base_dir": str(tmp_path / name)},
                    },
                }
            )
        )
        return path

    def start(name, arguments):
        log = (tmp_path / (name + ".log")).open("w")
        logs.append(log)
        process = subprocess.Popen(
            [
                binary,
                *arguments,
                "--config",
                str(node_config(name)),
                "--data-dir",
                str(tmp_path / name),
                "--health",
                "false",
                "--auth",
                "false",
            ],
            stdout=log,
            stderr=log,
            env=env,
        )
        processes.append(process)
        return process

    metadata_url = f"http://127.0.0.1:{metadata_api}"
    cluster = json.dumps(
        {
            "1": {
                "raft_url": f"http://127.0.0.1:{metadata_raft}",
                "orchestration_url": metadata_url,
            }
        }
    )
    start(
        "metadata",
        [
            "metadata",
            "--id",
            "1",
            "--raft-port",
            str(metadata_raft),
            "--api-port",
            str(metadata_api),
            "--cluster",
            cluster,
        ],
    )
    for ordinal in range(3):
        start(
            "data" + str(ordinal),
            [
                "data",
                "--node-id",
                str(ordinal + 2),
                "--store-id",
                str(ordinal + 2),
                "--metadata-api",
                metadata_url,
                "--api-port",
                str(ports[2 + ordinal * 2]),
                "--raft-port",
                str(ports[3 + ordinal * 2]),
                "--api-advertise-url",
                f"http://127.0.0.1:{ports[2 + ordinal * 2]}",
                "--raft-advertise-url",
                f"http://127.0.0.1:{ports[3 + ordinal * 2]}",
                "--failure-domain",
                "qualification-" + str(ordinal),
            ],
        )
    process = processes[1]

    def diagnostics():
        return "\n".join(path.read_text()[-3000:] for path in tmp_path.glob("*.log"))

    manager, state = None, None
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            assert process.poll() is None, diagnostics()
            try:
                target.request(
                    "POST",
                    "managed_hn",
                    "",
                    {
                        "schema": {
                            "document_schemas": {
                                "default": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "integer"},
                                            "body": {"type": "string"},
                                        },
                                    }
                                }
                            }
                        }
                    },
                )
                break
            except (URLError, HTTPError):
                time.sleep(0.2)
        else:
            pytest.fail("native daemon did not accept table creation")
        current = target.configuration("managed_hn")
        definition = {
            "kind": "postgres",
            "table": "managed_hn",
            "table_id": current["table_id"],
            "dsn_ref": "${secret:ANTFLY_MANAGED_PG_DSN}",
            "postgres_table": namespace + ".items",
        }
        provider = PostgreSQLCDC(lambda reference: psycopg.connect(dsn))
        manager = ManagedSources(
            Store(),
            (tmp_path / "authority").as_uri() + "/",
            target,
            {"postgres": provider},
        )
        state = manager.define("hn", definition)
        manager.reconcile("hn")

        def await_body(expected):
            end = time.monotonic() + 90
            while time.monotonic() < end:
                try:
                    result = target.request("GET", "managed_hn", "/documents/1")
                    if result.get("body") == expected:
                        return
                except HTTPError:
                    pass
                time.sleep(0.2)
            pytest.fail("native CDC did not converge; " + diagnostics())

        await_body("original")
        with psycopg.connect(dsn) as connection:
            connection.execute(
                sql.SQL("UPDATE {}.items SET body='updated' WHERE id=1").format(
                    sql.Identifier(namespace)
                )
            )
        await_body("updated")
        manager.remove("hn")
        end = time.monotonic() + 30
        while True:
            try:
                removed = manager.reconcile("hn")
                break
            except Exception:
                if time.monotonic() >= end:
                    raise
                time.sleep(0.2)
        assert removed["phase"] == "removed"
        assert not target.configuration("managed_hn")["replication_sources"]
        names = provider._names(state)
        with psycopg.connect(dsn) as connection:
            rows = connection.execute(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name=%s OR starts_with(slot_name,%s)",
                (names["slot"], names["slot"][:26] + "_af_"),
            ).fetchall()
            assert rows == []
    finally:
        for process in reversed(processes):
            process.terminate()
            try:
                process.wait(20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()
        # The daemon has drained, so no native claimant can recreate this
        # fixture's private resource namespace during cleanup.
        if manager is not None and state is not None:
            try:
                provider.teardown(
                    state,
                    type(
                        "StoppedTarget",
                        (),
                        {"configure_postgres": lambda *args, **kwargs: None},
                    )(),
                )
            except Exception:
                pass
        with psycopg.connect(dsn) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace))
            )
