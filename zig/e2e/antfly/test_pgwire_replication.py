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
        sql("CREATE TABLE smoke (id BIGINT PRIMARY KEY, content TEXT)")
        if export_snapshot:
            sql("ALTER TABLE smoke REPLICA IDENTITY FULL")
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
                "SELECT a.attname, t.typname, a.attnum, a.attnotnull, pg_catalog.pg_get_expr(d.adbin,d.adrelid) "
                "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
                "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid "
                "LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum "
                "WHERE c.relname = 'smoke' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            ],
            text=True,
            env=env,
            timeout=40,
        ).splitlines()
        assert columns == ["id|int8|1|t|", "content|text|2|f|"]
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
        sql("CREATE PUBLICATION changes FOR TABLE smoke")
        assert catalog_publication() == ["f|t|t|t|f"]
        assert catalog_slot() == []
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
                    "SELECT CASE pg_is_in_recovery() WHEN 't' THEN pg_last_wal_receive_lsn() "
                    "ELSE pg_current_wal_lsn() END",
                    "-c",
                    "SELECT CASE pg_is_in_recovery() WHEN 't' THEN '0'::xid8 ELSE pg_current_xact_id() END",
                    "-c",
                    "SELECT pg_catalog.pg_current_xact_id()",
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
            copied_lines = copied.stdout.splitlines()
            xid = int(copied_lines[4])
            assert 3 <= xid <= (1 << 63) - 1
            assert copied_lines == [
                "BEGIN",
                "SET",
                "SET",
                point,
                str(xid),
                str(xid),
                "snapshot-before-cut",
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
        assert catalog_publication() == ["f|t|t|t|f"]
        (recovered,) = catalog_slot()
        assert recovered[:3] == ["smoke_slot", "pgoutput", "f"]
        assert recovered[4] != created[4]
        resumed = tmp_path / "resumed.bin"
        with stream(resumed) as reader:
            time.sleep(4)
            assert reader.poll() is None
            assert resumed.is_file()
        assert resumed.read_bytes() == b""
        slot("--drop-slot")
        assert catalog_slot() == []
        sql("DROP PUBLICATION changes")
        assert catalog_publication() == []
    except Exception:
        print(server.debug_logs())
        raise
    finally:
        server.stop()
