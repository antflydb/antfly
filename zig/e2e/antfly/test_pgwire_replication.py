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

"""Exercise coordinated SQL writes with stock PostgreSQL logical replication."""

import os
import shutil
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

import conftest as harness
import pytest
from port_reservations import find_free_port


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize(
    "export_snapshot", [False, True], ids=["stream-only", "exported-snapshot"]
)
def test_pg_recvlogical_resume_after_restart(tmp_path, export_snapshot):
    binary = harness.resolve_binary_path(
        os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
    )
    assert Path(binary).is_file(), f"Antfly binary not found: {binary}"
    client = os.environ.get("ANTFLY_TEST_PG_RECVLOGICAL") or shutil.which(
        "pg_recvlogical"
    )
    psql = os.environ.get("ANTFLY_TEST_PSQL_BIN") or shutil.which("psql")
    if not client or not psql:
        pytest.skip("requires PostgreSQL 18.6 pg_recvlogical and psql")
    version = subprocess.check_output([client, "--version"], text=True, timeout=10)
    if version.split()[2] != "18.6":
        pytest.skip(f"qualified client is pg_recvlogical 18.6, found {version.strip()}")

    spool = tmp_path / "spool"
    spool.mkdir()
    namespace = subprocess.check_output(
        [binary, "storage", "provision-pgwire-spool", str(spool)], text=True, timeout=10
    ).strip()
    port = find_free_port()
    env = {
        **os.environ,
        "PGPASSWORD": harness.AUTH_BOOTSTRAP_PASSWORD,
        "PGSSLMODE": "disable",
        "PGGSSENCMODE": "disable",
        "PGCONNECT_TIMEOUT": "5",
    }
    connection = ["-h", "127.0.0.1", "-p", str(port), "-U", "admin", "-d", "default"]

    def sql(statement):
        subprocess.run(
            [psql, *connection, "-X", "-v", "ON_ERROR_STOP=1", "-c", statement],
            env=env,
            check=True,
            timeout=40,
        )

    def slot(*action):
        subprocess.run(
            [client, *connection, "--slot=smoke_slot", *action],
            env=env,
            check=True,
            timeout=40,
        )

    @contextmanager
    def stream(output):
        with output.with_suffix(".log").open("wb") as log:
            reader = subprocess.Popen(
                [
                    client,
                    *connection,
                    "--slot=smoke_slot",
                    "--start",
                    "--no-loop",
                    "--status-interval=1",
                    "--fsync-interval=1",
                    "-o",
                    "proto_version=1",
                    "-o",
                    "publication_names=changes",
                    "-o",
                    "messages=true",
                    "-f",
                    str(output),
                ],
                env=env,
                stderr=log,
            )
            try:
                yield reader
            finally:
                if reader.poll() is None:
                    reader.terminate()
                try:
                    reader.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    reader.kill()
                    reader.wait()
                assert reader.returncode == 0, output.with_suffix(".log").read_text()

    server = harness.StatefulAntflyServer(
        binary,
        "127.0.0.1",
        0,
        auth_enabled=True,
        data_config={
            "pgwire": {
                "enabled": True,
                "bind_port": port,
                "replication_publication": "changes",
                "replication_spool": {"directory": str(spool), "namespace": namespace},
            }
        },
    )
    try:
        sql("CREATE TABLE smoke (id BIGINT PRIMARY KEY, content TEXT)")
        if export_snapshot:
            sql("ALTER TABLE smoke REPLICA IDENTITY FULL")
        sql("CREATE PUBLICATION changes FOR TABLE smoke")
        if export_snapshot:
            sql("INSERT INTO smoke (id, content) VALUES (6, 'snapshot-before-cut')")
            exporter_connection = [
                *connection[:-1],
                "dbname=default replication=database",
            ]
            exported = subprocess.check_output(
                [
                    psql,
                    *exporter_connection,
                    "-XAt",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    "CREATE_REPLICATION_SLOT smoke_slot LOGICAL pgoutput EXPORT_SNAPSHOT",
                ],
                env=env,
                text=True,
                timeout=40,
            )
            name, point, snapshot, plugin = exported.strip().split("|")
            assert name == "smoke_slot"
            assert point and point != "0/0"
            assert snapshot.startswith("antfly_v1_")
            assert plugin == "pgoutput"
            sql("UPDATE smoke SET content = 'snapshot-after-cut' WHERE id = 6")
            copied = subprocess.run(
                [
                    psql,
                    *connection,
                    "-XAt",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-v",
                    "VERBOSITY=verbose",
                    "-c",
                    "BEGIN",
                    "-c",
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                    "-c",
                    f"SET TRANSACTION SNAPSHOT '{snapshot}'",
                    "-c",
                    "SELECT content FROM smoke",
                    "-c",
                    "COMMIT",
                ],
                env=env,
                text=True,
                capture_output=True,
                timeout=40,
            )
            assert copied.returncode == 0, copied.stdout + copied.stderr
            assert copied.stdout.splitlines() == [
                "BEGIN",
                "SET",
                "SET",
                "snapshot-before-cut",
                "COMMIT",
            ]
        else:
            slot("--create-slot", "--plugin=pgoutput")
        first = tmp_path / "first.bin"
        with stream(first) as reader:
            sql("INSERT INTO smoke (id, content) VALUES (7, 'retained-stream-smoke')")
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and reader.poll() is None:
                if first.exists() and b"retained-stream-smoke" in first.read_bytes():
                    break
                time.sleep(0.2)
            assert first.exists() and b"retained-stream-smoke" in first.read_bytes()
            if export_snapshot:
                payload = first.read_bytes()
                assert payload.count(b"snapshot-after-cut") == 1
                assert payload.count(b"snapshot-before-cut") == 1
                assert payload.index(b"snapshot-before-cut") < payload.index(
                    b"snapshot-after-cut"
                )

            # Give the client time to flush and report the complete frame.
            time.sleep(12)

        server.restart()
        resumed = tmp_path / "resumed.bin"
        with stream(resumed) as reader:
            time.sleep(4)
            assert reader.poll() is None
            assert resumed.is_file()
        assert resumed.read_bytes() == b""
        slot("--drop-slot")
    except Exception:
        print(server.debug_logs())
        raise
    finally:
        server.stop()
