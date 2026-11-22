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

import json
import os
import shutil
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote

import conftest as harness
import pytest
import requests
from helpers import wait_until
from port_reservations import find_free_port
from test_backup_restore import ThreeByThreeBackupCluster
from test_standalone_trusted_principal import _token


class PgwireFailoverCluster(ThreeByThreeBackupCluster):
    def __init__(self, binary, spool_config):
        self.spool_config = spool_config
        super().__init__(binary)

    def _write_config(self):
        super()._write_config()
        self.pgwire_ports = self.port_reservations.reserve_many(3)
        self.data_configs = []
        for index, port in enumerate(self.pgwire_ports):
            config = json.loads(self.config_path.read_text())
            config["pgwire"] = {
                "enabled": True,
                "bind_port": port,
                "replication_publication": "changes",
                "replication_spool": self.spool_config,
            }
            path = self.root / f"pgwire-data-{index}.json"
            path.write_text(json.dumps(config))
            self.data_configs.append(path)

    def _data_listener_ports(self, index):
        return (*super()._data_listener_ports(index), self.pgwire_ports[index])

    def _data_command(self, index):
        command = super()._data_command(index)
        command[command.index("--config") + 1] = str(self.data_configs[index])
        return command


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize(
    "export_snapshot, multi_table, cross_node, topology",
    [
        (False, False, False, False),
        (True, False, False, False),
        (False, True, False, False),
        (True, True, False, False),
        (False, False, True, False),
        (False, False, True, True),
    ],
    ids=[
        "stream-only",
        "exported-snapshot",
        "multi-table",
        "multi-table-snapshot",
        "cross-node",
        "split-merge",
    ],
)
def test_pg_recvlogical_resume_after_restart(
    tmp_path, monkeypatch, export_snapshot, multi_table, cross_node, topology
):
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

    def sql(*statements, retry_uncommitted=False):
        args = [
            psql,
            *connection,
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            "VERBOSITY=verbose",
            *(arg for statement in statements for arg in ("-c", statement)),
        ]
        expires = time.monotonic() + 12
        while True:
            result = subprocess.run(
                args, env=env, text=True, capture_output=True, timeout=40
            )
            if result.returncode == 0:
                return
            if (
                retry_uncommitted
                and (
                    "ERROR:  53300: A consistent SQL statement read" in result.stderr
                    or (
                        len(statements) == 1
                        and "ERROR:  40001: The mutation conflicted with a concurrent change and was not committed."
                        in result.stderr
                    )
                )
                and (
                    len(statements) == 1
                    or len(result.stdout.splitlines()) < len(statements) - 1
                )
                and time.monotonic() < expires
            ):
                # ON_ERROR_STOP ends this psql connection before COMMIT can be sent.
                time.sleep(0.2)
                continue
            raise AssertionError(result.stdout + result.stderr)

    def slot(*action, name="smoke_slot"):
        subprocess.run(
            [client, *connection, f"--slot={name}", *action],
            env=env,
            check=True,
            timeout=40,
        )

    def catalog_publication():
        return subprocess.check_output(
            [
                psql,
                *connection,
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate "
                "FROM pg_publication WHERE pubname = 'changes'",
            ],
            text=True,
            env=env,
            timeout=40,
        ).splitlines()

    def catalog_slot(name="smoke_slot", plugin="pgoutput"):
        result = subprocess.check_output(
            [psql, *connection, "-XAt", "-v", "ON_ERROR_STOP=1"],
            input=(
                "SELECT * FROM pg_replication_slots WHERE slot_name = $1 AND database = $2 AND plugin = $3 "
                f"\\bind {name} default {plugin}\n\\g\n"
            ),
            text=True,
            env=env,
            timeout=40,
        ).splitlines()
        columns = (0, 1, 6, 10, 11, 12)
        return [
            [fields[column] for column in columns]
            for fields in (line.split("|") for line in result)
        ]

    def stream_command(output, name="smoke_slot"):
        return [
            client,
            *connection,
            f"--slot={name}",
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
        ]

    @contextmanager
    def stream(output, *, expect_disconnect=False, name="smoke_slot"):
        with output.with_suffix(".log").open("wb") as log:
            reader = subprocess.Popen(stream_command(output, name), env=env, stderr=log)
            try:
                yield reader
            except Exception as exc:
                exc.add_note(output.with_suffix(".log").read_text())
                raise
            finally:
                if reader.poll() is None:
                    reader.terminate()
                try:
                    reader.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    reader.kill()
                    reader.wait()
            output_log = output.with_suffix(".log").read_text()
            if expect_disconnect:
                assert reader.returncode != 0 and "disconnected" in output_log
            else:
                assert reader.returncode == 0, output_log

    spool_config = {"directory": str(spool), "namespace": namespace}
    if cross_node:
        secret = "pgwire-failover-principal-authority-secret-v1"
        issuer = "pgwire-failover"
        monkeypatch.setenv("ANTFLY_TRUSTED_PRINCIPAL_SECRET", secret)
        monkeypatch.setenv("ANTFLY_TRUSTED_PRINCIPAL_ISSUER", issuer)
        server = PgwireFailoverCluster(binary, spool_config)
        connection[3] = str(server.pgwire_ports[0])
        connection[5] = "__antfly_trusted_principal__"
        env["PGPASSWORD"] = _token(secret, issuer=issuer, subject="admin", admin=True)
    else:
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
                    "replication_spool": spool_config,
                }
            },
        )

    def check_catalog_discovery():
        memberships = (
            "ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m "
            "JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid)"
        )
        role_query = (
            "SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, "
            "rolcanlogin, rolreplication FROM pg_roles "
            "WHERE pg_has_role('admin', oid, 'member')"
        )
        membership_query = (
            "SELECT r.rolcanlogin, r.rolreplication, "
            + ", ".join(
                f"CAST(array_position({memberships}, '{role}') AS BOOL) IS TRUE"
                for role in (
                    "rds_superuser",
                    "rdsadmin",
                    "rdsrepladmin",
                    "rds_replication",
                )
            )
            + " FROM pg_roles r WHERE r.rolname = current_user"
        )
        namespace_query = (
            "SELECT n.oid, n.nspname, t.typname FROM pg_catalog.pg_namespace n "
            "JOIN pg_catalog.pg_type t ON n.oid = t.typnamespace WHERE t.oid = 23"
        )
        type_query = (
            "SELECT oid, typname, typelem, typarray, typinput = 'array_in'::regproc "
            "FROM pg_catalog.pg_type WHERE oid IN (23, 1007) ORDER BY oid"
        )
        type_search_path_query = (
            "SELECT DISTINCT ON (typname) typname, typinput = 'array_in'::regproc, typtype, sp.r, pg_type.oid "
            "FROM pg_catalog.pg_type LEFT JOIN "
            "(SELECT ns.oid AS nspoid, ns.nspname, r.r FROM pg_namespace ns JOIN "
            "(SELECT s.r, (current_schemas(false))[s.r] AS nspname "
            "FROM generate_series(1, array_upper(current_schemas(false), 1)) s(r)) r USING (nspname)) sp "
            "ON sp.nspoid = typnamespace WHERE pg_type.oid IN (23, 1007) ORDER BY typname, sp.r, pg_type.oid"
        )
        type_registry_query = (
            "SELECT t.oid, t.typname, t.typelem, t.typbasetype, t.typtypmod, t.typcategory, e.values "
            "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid "
            "LEFT JOIN (SELECT t.enumtypid AS id, array_agg(t.enumlabel) AS values FROM pg_catalog.pg_enum t "
            "GROUP BY id) e ON t.oid = e.id WHERE n.nspname != 'pg_toast' AND t.oid IN (23,1007) ORDER BY t.oid"
        )
        discovery = subprocess.check_output(
            [
                psql,
                *connection,
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "SHOW wal_level",
                "-c",
                "SELECT version(), current_user, current_database(), current_catalog",
                "-c",
                type_query,
                "-c",
                role_query,
                "-c",
                "SELECT pg_has_role('other', oid, 'member') FROM pg_catalog.pg_roles",
                "-c",
                membership_query,
            ],
            env=env,
            text=True,
            timeout=40,
        ).splitlines()
        assert discovery[:4] == [
            "logical",
            "PostgreSQL 16.0 compatibility (Antfly)|admin|default|default",
            "23|int4|0|1007|f",
            "1007|_int4|23|0|t",
        ]
        oid, *role = discovery[4].split("|")
        assert int(oid) >= 16_384
        assert role == ["admin", "f", "f", "f", "f", "t", "t"]
        assert discovery[5:] == ["f", "t|t|f|f|f|f"]
        extended = subprocess.check_output(
            [psql, *connection, "-XAt", "-v", "ON_ERROR_STOP=1"],
            input="".join(
                f"{query} \\bind\n\\g\n"
                for query in (
                    role_query,
                    membership_query,
                    namespace_query,
                    "SELECT pg_is_in_recovery(), pg_catalog.pg_is_in_recovery()",
                    type_query,
                    type_search_path_query,
                    type_registry_query,
                    "SELECT current_schemas(false), pg_catalog.current_schemas(true), current_schemas(NULL)",
                    "SELECT (ARRAY['pg_catalog', 'public'])[oid - 2198] "
                    "FROM pg_catalog.pg_namespace WHERE oid = 2200",
                    "SELECT (ARRAY['public', 'app'])[s.r] "
                    "FROM pg_catalog.generate_series(1, array_upper(ARRAY['public', 'app'], 1)) s(r) ORDER BY s.r",
                    "SELECT oid,nspname FROM pg_catalog.pg_namespace WHERE nspname = 'public'",
                )
            ),
            env=env,
            text=True,
            timeout=40,
        ).splitlines()
        assert extended == [
            discovery[4],
            discovery[6],
            "11|pg_catalog|int4",
            "f|f",
            *discovery[2:4],
            "_int4|t|b||1007",
            "int4|f|b||23",
            "23|int4|0|0|-1|N|",
            "1007|_int4|23|0|-1|A|",
            '{"public"}|{"pg_catalog","public"}|',
            "public",
            "public",
            "app",
            "2200|public",
        ]

    try:
        if not cross_node:
            check_catalog_discovery()
        sql(
            "CREATE TABLE smoke (id BIGINT PRIMARY KEY, content TEXT, "
            "computed BIGINT GENERATED ALWAYS AS (id + 314159265) STORED)"
        )
        if multi_table:
            sql("CREATE TABLE smoke_other (id BIGINT PRIMARY KEY, content TEXT)")
        if export_snapshot:
            sql("ALTER TABLE smoke REPLICA IDENTITY FULL")
        replica_identity = subprocess.check_output(
            [
                psql,
                *connection,
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "SELECT c.relreplident FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid "
                "WHERE n.nspname='public' AND c.relname='smoke'",
            ],
            env=env,
            text=True,
            timeout=40,
        )
        assert replica_identity.strip() == ("f" if export_snapshot else "d")
        names = subprocess.check_output(
            [
                psql,
                *connection,
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "SELECT n.nspname, c.relname, c.relkind, d.description "
                "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "LEFT JOIN pg_catalog.pg_description d ON c.oid = d.objoid "
                "AND d.classoid = 'pg_catalog.pg_class'::regclass AND d.objsubid = 0 "
                "WHERE n.nspname = 'public' AND c.relname = 'smoke'",
            ],
            text=True,
            env=env,
            timeout=40,
        ).splitlines()
        columns = subprocess.check_output(
            [
                psql,
                *connection,
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "SELECT a.attname, t.typname, a.attnum, a.attnotnull, a.attgenerated, "
                "pg_catalog.pg_get_expr(d.adbin,d.adrelid) "
                "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
                "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid "
                "LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum "
                "WHERE c.relname = 'smoke' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            ],
            text=True,
            env=env,
            timeout=40,
        ).splitlines()
        assert columns == [
            "id|int8|1|t||",
            "content|text|2|f||",
            "computed|int8|3|f|s|",
        ]
        assert names == ["public|smoke|r|"]
        sql(
            "CREATE TABLE catalog_key_order (first_key BIGINT, second_key TEXT, "
            "CONSTRAINT ordered_primary PRIMARY KEY (second_key, first_key))"
        )
        primary_key = subprocess.check_output(
            [
                psql,
                *connection,
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "SELECT ci.relname, ci.relkind, i.indnkeyatts, i.indkey[0], i.indkey[1] "
                "FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_index i ON ct.oid = i.indrelid "
                "JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid "
                "WHERE ct.relname = 'catalog_key_order' AND i.indisprimary",
            ],
            text=True,
            env=env,
            timeout=40,
        ).splitlines()
        assert primary_key == ["ordered_primary|i|2|2|1"]
        jdbc_keys = subprocess.check_output(
            [
                psql,
                *connection,
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "SELECT r.column_name, r.key_seq, r.pk_name FROM "
                "(SELECT a.attname AS column_name, (information_schema._pg_expandarray(i.indkey)).n AS key_seq, "
                "ci.relname AS pk_name, information_schema._pg_expandarray(i.indkey) AS keys, "
                "a.attnum AS a_attnum, i.indnkeyatts AS key_count FROM pg_catalog.pg_class ct "
                "JOIN pg_catalog.pg_attribute a ON ct.oid = a.attrelid "
                "JOIN pg_catalog.pg_index i ON a.attrelid = i.indrelid "
                "JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid "
                "WHERE ct.relname = 'catalog_key_order' AND i.indisprimary) r "
                "WHERE r.a_attnum = (r.keys).x AND r.key_seq <= key_count ORDER BY r.key_seq",
            ],
            text=True,
            env=env,
            timeout=40,
        ).splitlines()
        assert jdbc_keys == [
            "second_key|1|ordered_primary",
            "first_key|2|ordered_primary",
        ]
        assert catalog_publication() == []
        published = "smoke, smoke_other" if multi_table else "smoke"
        sql(f"CREATE PUBLICATION changes FOR TABLE {published}")
        assert catalog_publication() == ["f|t|t|t|f"]
        assert catalog_slot() == []
        if export_snapshot:
            sql("INSERT INTO smoke (id, content) VALUES (6, 'snapshot-before-cut')")
            if multi_table:
                sql(
                    "INSERT INTO smoke_other (id, content) VALUES (5, 'other-before-cut')"
                )
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
            second_table = []
            if multi_table:
                sql("UPDATE smoke_other SET content = 'other-after-cut' WHERE id = 5")
                second_table = ["-c", "SELECT content FROM smoke_other"]
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
                    "SELECT CASE pg_is_in_recovery() WHEN 't' THEN pg_last_wal_receive_lsn() "
                    "ELSE pg_current_wal_lsn() END",
                    "-c",
                    "SELECT CASE pg_is_in_recovery() WHEN 't' THEN '0'::xid8 ELSE pg_current_xact_id() END",
                    "-c",
                    "SELECT pg_catalog.pg_current_xact_id()",
                    "-c",
                    "SELECT CURRENT_TIMESTAMP",
                    "-c",
                    "SELECT CURRENT_TIMESTAMP",
                    "-c",
                    "SELECT content FROM smoke",
                    *second_table,
                    "-c",
                    "COMMIT",
                ],
                env=env,
                text=True,
                capture_output=True,
                timeout=40,
            )
            assert copied.returncode == 0, copied.stdout + copied.stderr
            copied_lines = copied.stdout.splitlines()
            xid = int(copied_lines[4])
            timestamp = copied_lines[6]
            assert "T" not in timestamp and timestamp.endswith("+00")
            assert timestamp == copied_lines[7]
            assert 3 <= xid <= (1 << 63) - 1
            assert copied_lines == [
                "BEGIN",
                "SET",
                "SET",
                point,
                str(xid),
                str(xid),
                timestamp,
                timestamp,
                "snapshot-before-cut",
                *(["other-before-cut"] if multi_table else []),
                "COMMIT",
            ]
        else:
            slot("--create-slot", "--plugin=pgoutput")
        (created,) = catalog_slot()
        assert created[:3] == ["smoke_slot", "pgoutput", "f"]
        assert created[3] == created[4] and "/" in created[4]
        assert created[5] == "reserved"
        assert catalog_slot(plugin="wrong_plugin") == []
        first = tmp_path / "first.bin"
        with stream(first, expect_disconnect=cross_node and not topology) as reader:
            insert = (
                "INSERT INTO smoke (id, content) VALUES (7, 'retained-stream-smoke')"
            )
            # pgoutput text tuple field for the stored generated value of id 7.
            markers = [b"retained-stream-smoke", b"t\x00\x00\x00\x09314159272"]
            if multi_table:
                sql(
                    "BEGIN",
                    "INSERT INTO smoke (id, content) VALUES (8, 'retained-rolled-back-transaction')",
                    "ROLLBACK",
                    "BEGIN",
                    insert,
                    "SAVEPOINT discarded",
                    "INSERT INTO smoke_other (id, content) VALUES (10, 'retained-rolled-back-savepoint')",
                    "ROLLBACK TO SAVEPOINT discarded",
                    "INSERT INTO smoke_other (id, content) VALUES (9, 'retained-stream-other')",
                    "COMMIT",
                    retry_uncommitted=True,
                )
                markers.append(b"retained-stream-other")
            else:
                sql(insert)
            deadline = time.monotonic() + 30
            payload = b""
            while time.monotonic() < deadline and reader.poll() is None:
                if first.exists():
                    payload = first.read_bytes()
                    if all(marker in payload for marker in markers):
                        break
                time.sleep(0.2)
            assert all(payload.count(marker) == 1 for marker in markers)
            if multi_table:
                assert b"retained-rolled-back-" not in payload
            if export_snapshot:
                payload = first.read_bytes()
                assert payload.count(b"snapshot-after-cut") == 1
                assert payload.count(b"snapshot-before-cut") == 1
                assert payload.index(b"snapshot-before-cut") < payload.index(
                    b"snapshot-after-cut"
                )

            # Give the client time to flush and report the complete frame.
            time.sleep(12)
            if cross_node:
                (checkpointed,) = catalog_slot()
                assert checkpointed[2] == "t"
                assert checkpointed[4] != created[4]
                if not topology:
                    failed = server.data_procs[0]
                    failed.kill()
                    failed.wait(timeout=25)
                    reader.wait(timeout=10)

        if cross_node:
            connection[3] = str(server.pgwire_ports[1])
            rejected = subprocess.run(
                [psql, *connection, "-XAt", "-c", "SELECT 1"],
                env={**env, "PGPASSWORD": "invalid-failover-password"},
                capture_output=True,
                text=True,
                timeout=10,
            )
            assert rejected.returncode != 0
            assert "authentication failed" in rejected.stderr.lower()
            wrong_owner = subprocess.run(
                stream_command(tmp_path / "wrong-owner.bin"),
                env={
                    **env,
                    "PGPASSWORD": _token(
                        secret, issuer=issuer, subject="other", admin=True
                    ),
                },
                capture_output=True,
                text=True,
                timeout=10,
            )
            assert wrong_owner.returncode != 0
            assert "forbidden" in wrong_owner.stderr.lower()
            env["PGPASSWORD"] = _token(
                secret, issuer=issuer, subject="admin", admin=True
            )
        else:
            server.restart()
        assert catalog_publication() == ["f|t|t|t|f"]
        (recovered,) = catalog_slot()
        if cross_node:
            deadline = time.monotonic() + 65
            while recovered[2] == "t" and time.monotonic() < deadline:
                time.sleep(0.5)
                (recovered,) = catalog_slot()
            assert recovered[4] == checkpointed[4]

            def row_matches(statement, expected):
                result = subprocess.run(
                    [
                        psql,
                        *connection,
                        "-XAt",
                        "-v",
                        "ON_ERROR_STOP=1",
                        "-c",
                        statement,
                    ],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if result.returncode == 0:
                    return result.stdout.strip() == expected
                if "A consistent SQL statement read" in result.stderr:
                    return False
                raise AssertionError(result.stderr)

            assert wait_until(
                lambda: row_matches(
                    "SELECT content FROM smoke", "retained-stream-smoke"
                ),
                timeout_s=30,
                interval_s=0.5,
            )
        assert recovered[:3] == ["smoke_slot", "pgoutput", "f"]
        assert recovered[4] != created[4]
        resumed = tmp_path / "resumed.bin"
        with stream(resumed) as reader:
            time.sleep(4)
            assert reader.poll() is None
            assert resumed.is_file()
            assert resumed.read_bytes() == b""
            if cross_node:
                sql(
                    "BEGIN",
                    "INSERT INTO smoke (id, content) VALUES (101, 'after-node-failover')",
                    "COMMIT",
                    retry_uncommitted=True,
                )
                assert wait_until(
                    lambda: row_matches(
                        "SELECT content FROM smoke WHERE id = 101",
                        "after-node-failover",
                    ),
                    timeout_s=15,
                    interval_s=0.3,
                )
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline and reader.poll() is None:
                    if b"after-node-failover" in resumed.read_bytes():
                        break
                    time.sleep(0.2)
                assert resumed.read_bytes().count(b"after-node-failover") == 1
                assert all(marker not in resumed.read_bytes() for marker in markers)
                if topology:
                    catalog = requests.get(
                        f"{server.data_api_urls[1]}/tables",
                        headers={"X-Antfly-Trusted-Principal": env["PGPASSWORD"]},
                        timeout=10,
                    )
                    assert catalog.ok, catalog.text
                    tables = catalog.json()
                    names = {"smoke", "public.smoke", "default.public.smoke"}
                    ids = [
                        int(table["table_id"])
                        for table in tables
                        if table["name"] in names
                    ]
                    assert len(ids) == 1, [table["name"] for table in tables]
                    smoke_table_id = ids[0]

                    def topology_snapshot():
                        leader = server.metadata_stable_leader_id(timeout_s=30)
                        assert leader is not None
                        snapshot = server.metadata_snapshot(leader - 1)
                        table = next(
                            table
                            for table in snapshot["tables"]
                            if int(table["table_id"]) == smoke_table_id
                        )
                        ranges = sorted(
                            (
                                item
                                for item in snapshot["ranges"]
                                if int(item["table_id"]) == int(table["table_id"])
                            ),
                            key=lambda item: item.get("start_key") or "",
                        )
                        return leader, quote(table["name"], safe=""), ranges

                    def has_range_count(count):
                        try:
                            return len(topology_snapshot()[2]) == count
                        except (requests.Timeout, requests.ConnectionError):
                            # Leadership and catalog reads can briefly stall
                            # while the topology command is being applied.
                            return False

                    def request_topology(operation, body):
                        leader, physical_name, _ = topology_snapshot()
                        response = requests.post(
                            f"{server.metadata_admin_urls[leader - 1]}/internal/v1/tables/{physical_name}/{operation}",
                            headers=harness.internal_service_headers(),
                            json=body,
                            timeout=20,
                        )
                        assert response.ok, response.text

                    def lsn_value(lsn):
                        high, low = lsn.split("/")
                        return (int(high, 16) << 32) | int(low, 16)

                    def expect_markers(pairs, previous):
                        assert wait_until(
                            lambda: (
                                reader.poll() is None
                                and all(
                                    marker in resumed.read_bytes() for marker in pairs
                                )
                            ),
                            timeout_s=60,
                            interval_s=0.2,
                        ), resumed.with_suffix(".log").read_text()
                        assert all(
                            resumed.read_bytes().count(marker) == 1 for marker in pairs
                        )
                        assert wait_until(
                            lambda: lsn_value(catalog_slot()[0][4]) > previous,
                            timeout_s=30,
                            interval_s=0.2,
                        )
                        return lsn_value(catalog_slot()[0][4])

                    floor = lsn_value(recovered[4])
                    floor = expect_markers([b"after-node-failover"], floor)
                    # This internal endpoint takes a physical document key.
                    request_topology("split", {"split_key": "doc:m"})
                    assert wait_until(
                        lambda: has_range_count(2),
                        timeout_s=180,
                        interval_s=0.5,
                    ), server.debug_logs()
                    sql(
                        "INSERT INTO smoke (_id, id, content) VALUES ('a-left', 102, 'after-split-left')",
                        retry_uncommitted=True,
                    )
                    sql(
                        "INSERT INTO smoke (_id, id, content) VALUES ('z-right', 103, 'after-split-right')",
                        retry_uncommitted=True,
                    )
                    floor = expect_markers(
                        [b"after-split-left", b"after-split-right"], floor
                    )
                    slot("--create-slot", "--plugin=pgoutput", name="post_split")
                    slot("--drop-slot", name="post_split")
                    _, _, (left, right) = topology_snapshot()
                    request_topology(
                        "merge",
                        {
                            "donor_group_id": int(right["group_id"]),
                            "receiver_group_id": int(left["group_id"]),
                        },
                    )
                    assert wait_until(
                        lambda: has_range_count(1),
                        timeout_s=180,
                        interval_s=0.5,
                    ), server.debug_logs()
                    sql(
                        "INSERT INTO smoke (_id, id, content) VALUES ('z-merged', 104, 'after-merge')",
                        retry_uncommitted=True,
                    )
                    expect_markers([b"after-merge"], floor)
                    assert all(marker not in resumed.read_bytes() for marker in markers)
        active_slot = "smoke_slot"
        if not export_snapshot and not multi_table and not cross_node:
            sql("ALTER TABLE smoke REPLICA IDENTITY FULL")
            rejected_output = tmp_path / "rejected-identity.bin"
            rejected = subprocess.run(
                stream_command(rejected_output),
                env=env,
                text=True,
                capture_output=True,
                timeout=20,
            )
            assert rejected.returncode != 0
            assert "ChangefeedResynchronizationRequired" in rejected.stderr
            assert not rejected_output.exists() or not rejected_output.read_bytes()
            slot("--drop-slot")
            assert catalog_slot() == []
            sql("DROP PUBLICATION changes")
            sql("CREATE PUBLICATION changes FOR TABLE smoke")
            active_slot = "after_identity"
            slot("--create-slot", "--plugin=pgoutput", name=active_slot)
            sql("INSERT INTO smoke (id, content) VALUES (16, 'pre-schema-value')")
            sql("ALTER TABLE smoke ADD COLUMN additional TEXT")
            evolved = tmp_path / "evolved.bin"
            with stream(evolved, name=active_slot) as reader:
                sql(
                    "INSERT INTO smoke (id, content, additional) "
                    "VALUES (17, 'post-schema-content', 'post-schema-added-value')"
                )
                assert wait_until(
                    lambda: (
                        reader.poll() is None
                        and evolved.exists()
                        and b"post-schema-added-value" in evolved.read_bytes()
                    ),
                    timeout_s=25,
                )
                payload = evolved.read_bytes()
                assert (
                    payload.index(b"pre-schema-value")
                    < payload.index(b"additional\x00")
                    < payload.index(b"post-schema-added-value")
                )
            with pytest.raises(AssertionError, match="42601"):
                sql(
                    "CREATE PUBLICATION truncates FOR TABLE smoke WITH (publish = 'truncate')"
                )
        slot("--drop-slot", name=active_slot)
        assert catalog_slot(name=active_slot) == []
        sql("DROP PUBLICATION changes")
        assert catalog_publication() == []
    except Exception:
        print(server.debug_logs())
        raise
    finally:
        server.stop()
