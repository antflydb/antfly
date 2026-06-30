# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""E2E coverage for the pgwire SQL compatibility listener."""

from __future__ import annotations

import json
import os
import shutil
import socket
import struct
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import (
    DEFAULT_ANTFLY_BIN,
    SwarmAntflyServer,
    _metadata_command,
    _read_log_tail,
    find_free_port,
    maybe_preserve_tempdir,
    resolve_binary_path,
    wait_for_listener,
    wait_for_server,
)

PG_BOOL_OID = 16
PG_INT4_OID = 23
PG_TEXT_OID = 25
PG_BOOL_ARRAY_OID = 1000
PG_TEXT_ARRAY_OID = 1009
PG_TIMESTAMPTZ_ARRAY_OID = 1185
PG_NUMERIC_ARRAY_OID = 1231
PG_TIMESTAMPTZ_OID = 1184
PG_NUMERIC_OID = 1700
PG_JSONB_OID = 3802


@pytest.fixture(scope="module")
def pgwire_server():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")

    port = find_free_port()
    pgwire_port = find_free_port()
    server = SwarmAntflyServer(binary, "127.0.0.1", port, pgwire_port=pgwire_port)
    yield server
    server.stop()


@pytest.fixture(scope="module")
def auth_pgwire_server():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")

    port = find_free_port()
    pgwire_port = find_free_port()
    server = SwarmAntflyServer(binary, "127.0.0.1", port, pgwire_port=pgwire_port, auth_enabled=True)
    yield server
    server.stop()


@pytest.fixture(scope="module")
def antfly_bin() -> str:
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")
    return binary


@pytest.fixture(scope="module")
def metadata_pgwire_server(antfly_bin):
    host = "127.0.0.1"
    raft_port = find_free_port()
    admin_port = find_free_port()
    pgwire_port = find_free_port()
    tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-metadata-pgwire-e2e-")
    try:
        root = Path(tempdir.name)
        log_path = root / "metadata.log"
        with log_path.open("w") as log_file:
            command = _metadata_command(
                antfly_bin,
                host=host,
                raft_port=raft_port,
                admin_port=admin_port,
                root=root,
                pgwire_port=pgwire_port,
            )
            proc = subprocess.Popen(command, stdout=log_file, stderr=subprocess.STDOUT, cwd=root)
            admin_url = f"http://{host}:{admin_port}"
            pgwire_url = f"http://{host}:{pgwire_port}"
            try:
                if not wait_for_server(admin_url, path="/metadata/v1/status"):
                    raise RuntimeError(f"metadata server failed to start\n{_read_log_tail(log_path)}")
                if not wait_for_listener(pgwire_url):
                    raise RuntimeError(f"metadata pgwire listener failed to start\n{_read_log_tail(log_path)}")
                yield {"host": host, "pgwire_port": pgwire_port, "log_path": log_path}
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
    finally:
        if not maybe_preserve_tempdir(tempdir):
            tempdir.cleanup()


def _table_name(prefix: str) -> str:
    return f"{prefix}_{time.time_ns()}"


def _run_cli(
    binary: str,
    server: SwarmAntflyServer,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [binary, *args],
        capture_output=True,
        text=True,
        timeout=30,
        env=os.environ.copy(),
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"CLI failed (exit {result.returncode}): {binary} {' '.join(args)}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}\n"
            f"server logs:\n{server.debug_logs()[-4000:]}"
        )
    return result


def _json_values(stdout: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    values: list[dict[str, Any]] = []
    offset = 0
    while offset < len(stdout):
        while offset < len(stdout) and stdout[offset].isspace():
            offset += 1
        if offset >= len(stdout):
            break
        value, offset = decoder.raw_decode(stdout, offset)
        assert isinstance(value, dict)
        values.append(value)
    return values


def test_antfly_sql_can_use_http_host_port_and_pgwire(pgwire_server, antfly_bin):
    http_table = _table_name("pgwire_http_cli")

    created = _run_cli(
        antfly_bin,
        pgwire_server,
        "sql",
        "--host",
        pgwire_server.host,
        "--port",
        str(pgwire_server.port),
        "-c",
        f"CREATE TABLE {http_table} (id text PRIMARY KEY, status text, amount int);",
    )
    assert _json_values(created.stdout)[0]["kind"] == "ddl"

    table = _table_name("pgwire_cli")
    pgwire_created = _run_cli(
        antfly_bin,
        pgwire_server,
        "sql",
        "--pgwire-host",
        pgwire_server.host,
        "--pgwire-port",
        str(pgwire_server.pgwire_port),
        "-c",
        f"CREATE TABLE {table} (id text PRIMARY KEY, status text, amount int);",
    )
    pgwire_create_response = _json_values(pgwire_created.stdout)[0]
    assert pgwire_create_response["kind"] == "ddl"
    assert pgwire_create_response["statement_kind"] == "ddl"
    assert pgwire_create_response["result"] == {}

    inserted = _run_cli(
        antfly_bin,
        pgwire_server,
        "sql",
        "--pgwire-host",
        pgwire_server.host,
        "--pgwire-port",
        str(pgwire_server.pgwire_port),
        "-c",
        f"INSERT INTO {table} (id, status, amount) VALUES ('row:a', 'open', 10) RETURNING id;",
    )
    insert_response = _json_values(inserted.stdout)[0]
    assert insert_response["kind"] == "write"
    assert insert_response["statement_kind"] == "insert"
    assert insert_response["result"]["returning"] == [{"id": "row:a"}]

    selected = _run_cli(
        antfly_bin,
        pgwire_server,
        "sql",
        "--pgwire-host",
        pgwire_server.host,
        "--pgwire-port",
        str(pgwire_server.pgwire_port),
        "-c",
        f"SELECT id, amount FROM {table} WHERE status = 'open';",
    )
    select_response = _json_values(selected.stdout)[0]
    assert select_response["kind"] == "read"
    assert select_response["statement_kind"] == "query"
    assert select_response["result"]["rows"] == [{"id": "row:a", "amount": "10"}]


def test_pgx_default_extended_query_mode(pgwire_server):
    if shutil.which("go") is None:
        pytest.skip("go binary not available for pgx smoke")

    smoke_dir = Path(__file__).parent / "pgx_smoke"
    env = os.environ.copy()
    env["GOCACHE"] = env.get("GOCACHE", "/tmp/antfly-pgx-e2e-gocache")
    env["GOWORK"] = "off"
    env["GOPROXY"] = "off"
    env["GOSUMDB"] = "off"
    result = subprocess.run(
        ["go", "run", "-mod=readonly", ".", pgwire_server.host, str(pgwire_server.pgwire_port)],
        cwd=smoke_dir,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"pgx smoke failed (exit {result.returncode})\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}\n"
            f"server logs:\n{pgwire_server.debug_logs()[-4000:]}"
        )
    assert "pgx default extended query smoke passed" in result.stdout


def test_pgwire_simple_query_accepts_multiple_statements(pgwire_server):
    table = _table_name("pgwire_multi")
    sql = (
        f"CREATE TABLE {table} (id text PRIMARY KEY, status text);"
        f"INSERT INTO {table} (id, status) VALUES ('row:b', 'closed');"
        f"SELECT id, status FROM {table} WHERE id = 'row:b';"
    )

    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        messages = _pgwire_simple_query(sock, sql)

    commands = [message["tag"] for message in messages if message["type"] == "command"]
    rows = [message["values"] for message in messages if message["type"] == "row"]
    assert commands == ["CREATE TABLE", "INSERT 0 1", "SELECT 1"]
    assert rows == [["row:b", "closed"]]


def test_pgwire_auth_uses_public_api_user_manager(auth_pgwire_server, antfly_bin):
    table = _table_name("pgwire_auth")
    cli_table = _table_name("pgwire_auth_cli")

    with socket.create_connection((auth_pgwire_server.host, auth_pgwire_server.pgwire_port), timeout=5) as sock:
        with pytest.raises(AssertionError, match="28P01"):
            _pgwire_startup(sock, user="admin", password="wrong")

    with socket.create_connection((auth_pgwire_server.host, auth_pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock, user="admin", password="admin")
        messages = _pgwire_simple_query(sock, f"CREATE TABLE {table} (id text PRIMARY KEY);")

    assert [message["tag"] for message in messages if message["type"] == "command"] == ["CREATE TABLE"]

    cli_created = _run_cli(
        antfly_bin,
        auth_pgwire_server,
        "sql",
        "--pgwire-host",
        auth_pgwire_server.host,
        "--pgwire-port",
        str(auth_pgwire_server.pgwire_port),
        "--pgwire-user",
        "admin",
        "--pgwire-password",
        "admin",
        "-c",
        f"CREATE TABLE {cli_table} (id text PRIMARY KEY);",
    )
    assert _json_values(cli_created.stdout)[0]["kind"] == "ddl"


def test_pgwire_postgres_compatibility_probes_return_rows(pgwire_server):
    base_table = _table_name("pgwire_info_schema")
    table = f"{base_table}_a"
    later_table = f"{base_table}_b"
    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        _pgwire_simple_query(sock, f"CREATE TABLE {table} (id text PRIMARY KEY, amount numeric, active boolean);")
        _pgwire_simple_query(sock, f"CREATE TABLE {later_table} (id text PRIMARY KEY);")
        version_messages = _pgwire_simple_query(sock, "SELECT version();")
        catalog_version_messages = _pgwire_simple_query(sock, "SELECT pg_catalog.version();")
        server_version_messages = _pgwire_simple_query(sock, "SHOW server_version;")
        current_setting_messages = _pgwire_simple_query(sock, "SELECT current_setting('server_version_num');")
        catalog_current_setting_messages = _pgwire_simple_query(sock, "SELECT pg_catalog.current_setting('server_version_num');")
        search_path_messages = _pgwire_simple_query(sock, "SHOW search_path;")
        show_all_messages = _pgwire_simple_query(sock, "SHOW ALL;")
        information_schema_tables_messages = _pgwire_simple_query(
            sock,
            "SELECT table_schema, table_name, table_type FROM information_schema.tables "
            f"WHERE table_schema = 'public' AND table_name = '{table}';",
        )
        information_schema_columns_messages = _pgwire_simple_query(
            sock,
            "SELECT column_name, ordinal_position, data_type, is_nullable FROM information_schema.columns "
            f"WHERE table_catalog = 'default' AND table_name = '{table}' ORDER BY ordinal_position;",
        )
        information_schema_tables_desc_messages = _pgwire_simple_query(
            sock,
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = 'default' AND table_schema = 'public' ORDER BY table_name DESC;",
        )
        information_schema_columns_desc_messages = _pgwire_simple_query(
            sock,
            "SELECT column_name, ordinal_position FROM information_schema.columns "
            f"WHERE table_catalog = 'default' AND table_name = '{table}' ORDER BY ordinal_position DESC;",
        )

    assert [message for message in version_messages if message["type"] == "columns"] == [
        {"type": "columns", "columns": ["version"], "oids": [PG_TEXT_OID]}
    ]
    version_rows = [message["values"] for message in version_messages if message["type"] == "row"]
    assert len(version_rows) == 1
    assert "Antfly" in version_rows[0][0]
    assert [message["tag"] for message in version_messages if message["type"] == "command"] == ["SELECT 1"]

    assert [message for message in catalog_version_messages if message["type"] == "columns"] == [
        {"type": "columns", "columns": ["version"], "oids": [PG_TEXT_OID]}
    ]
    catalog_version_rows = [message["values"] for message in catalog_version_messages if message["type"] == "row"]
    assert len(catalog_version_rows) == 1
    assert "Antfly" in catalog_version_rows[0][0]
    assert [message["tag"] for message in catalog_version_messages if message["type"] == "command"] == ["SELECT 1"]

    assert [message for message in server_version_messages if message["type"] == "columns"] == [
        {"type": "columns", "columns": ["server_version"], "oids": [PG_TEXT_OID]}
    ]
    assert [message["values"] for message in server_version_messages if message["type"] == "row"] == [["16.0-antfly"]]
    assert [message["tag"] for message in server_version_messages if message["type"] == "command"] == ["SELECT 1"]

    assert [message for message in current_setting_messages if message["type"] == "columns"] == [
        {"type": "columns", "columns": ["current_setting"], "oids": [PG_TEXT_OID]}
    ]
    assert [message["values"] for message in current_setting_messages if message["type"] == "row"] == [["160000"]]
    assert [message["tag"] for message in current_setting_messages if message["type"] == "command"] == ["SELECT 1"]

    assert [message for message in catalog_current_setting_messages if message["type"] == "columns"] == [
        {"type": "columns", "columns": ["current_setting"], "oids": [PG_TEXT_OID]}
    ]
    assert [message["values"] for message in catalog_current_setting_messages if message["type"] == "row"] == [["160000"]]
    assert [message["tag"] for message in catalog_current_setting_messages if message["type"] == "command"] == ["SELECT 1"]

    assert [message["values"] for message in search_path_messages if message["type"] == "row"] == [["public"]]

    show_all_columns = [message for message in show_all_messages if message["type"] == "columns"]
    assert show_all_columns == [
        {"type": "columns", "columns": ["name", "setting", "description"], "oids": [PG_TEXT_OID, PG_TEXT_OID, PG_TEXT_OID]}
    ]
    show_all_rows = [message["values"] for message in show_all_messages if message["type"] == "row"]
    show_all_settings = {row[0]: row[1] for row in show_all_rows}
    assert show_all_settings["server_version"] == "16.0-antfly"
    assert show_all_settings["client_encoding"] == "UTF8"
    assert show_all_settings["search_path"] == "public"

    assert [message for message in information_schema_tables_messages if message["type"] == "columns"] == [
        {"type": "columns", "columns": ["table_schema", "table_name", "table_type"], "oids": [PG_TEXT_OID, PG_TEXT_OID, PG_TEXT_OID]}
    ]
    assert [message["values"] for message in information_schema_tables_messages if message["type"] == "row"] == [
        ["public", table, "BASE TABLE"]
    ]

    assert [message for message in information_schema_columns_messages if message["type"] == "columns"] == [
        {
            "type": "columns",
            "columns": ["column_name", "ordinal_position", "data_type", "is_nullable"],
            "oids": [PG_TEXT_OID, PG_TEXT_OID, PG_TEXT_OID, PG_TEXT_OID],
        }
    ]
    info_schema_columns = [message["values"] for message in information_schema_columns_messages if message["type"] == "row"]
    assert info_schema_columns == [
        ["id", "1", "text", "NO"],
        ["amount", "2", "numeric", "YES"],
        ["active", "3", "boolean", "YES"],
    ]
    info_schema_table_names_desc = [message["values"][0] for message in information_schema_tables_desc_messages if message["type"] == "row"]
    assert later_table in info_schema_table_names_desc
    assert table in info_schema_table_names_desc
    assert info_schema_table_names_desc.index(later_table) < info_schema_table_names_desc.index(table)
    assert [message["values"] for message in information_schema_columns_desc_messages if message["type"] == "row"] == [
        ["active", "3"],
        ["amount", "2"],
        ["id", "1"],
    ]


def test_pgwire_cancel_request_uses_backend_key_side_channel(pgwire_server):
    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        backend_pid, cancel_key = _pgwire_startup(sock)
        assert backend_pid != 0
        assert cancel_key != 0
        _pgwire_cancel_request(pgwire_server.host, pgwire_server.pgwire_port, backend_pid, cancel_key)
        messages = _pgwire_simple_query(sock, "SELECT version();")

    assert [message["tag"] for message in messages if message["type"] == "command"] == ["SELECT 1"]


def test_pgwire_ready_for_query_tracks_transaction_status(pgwire_server):
    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        begin_messages = _pgwire_simple_query(sock, "BEGIN;")
        commit_messages = _pgwire_simple_query(sock, "COMMIT;")
        begin_again_messages = _pgwire_simple_query(sock, "START TRANSACTION;")
        rollback_messages = _pgwire_simple_query(sock, "ROLLBACK;")

    assert [message["tag"] for message in begin_messages if message["type"] == "command"] == ["BEGIN"]
    assert [message["status"] for message in begin_messages if message["type"] == "ready"] == ["T"]
    assert [message["tag"] for message in commit_messages if message["type"] == "command"] == ["COMMIT"]
    assert [message["status"] for message in commit_messages if message["type"] == "ready"] == ["I"]
    assert [message["tag"] for message in begin_again_messages if message["type"] == "command"] == ["BEGIN"]
    assert [message["status"] for message in begin_again_messages if message["type"] == "ready"] == ["T"]
    assert [message["tag"] for message in rollback_messages if message["type"] == "command"] == ["ROLLBACK"]
    assert [message["status"] for message in rollback_messages if message["type"] == "ready"] == ["I"]


def test_pgwire_failed_transaction_reports_error_status_until_rollback(pgwire_server):
    table = _table_name("pgwire_failed_tx")

    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        begin_messages = _pgwire_simple_query(sock, "BEGIN;")
        missing_messages = _pgwire_simple_query_error(sock, "SELECT id FROM missing_pgwire_table;")
        blocked_messages = _pgwire_simple_query_error(sock, f"CREATE TABLE {table} (id text PRIMARY KEY);")
        rollback_messages = _pgwire_simple_query(sock, "ROLLBACK;")
        create_messages = _pgwire_simple_query(sock, f"CREATE TABLE {table} (id text PRIMARY KEY);")

    assert [message["status"] for message in begin_messages if message["type"] == "ready"] == ["T"]
    assert [message["status"] for message in missing_messages if message["type"] == "ready"] == ["E"]
    assert [message["sqlstate"] for message in missing_messages if message["type"] == "error"] == ["42P01"]
    assert [message["status"] for message in blocked_messages if message["type"] == "ready"] == ["E"]
    assert [message["sqlstate"] for message in blocked_messages if message["type"] == "error"] == ["25P02"]
    assert any("current transaction is aborted" in message["message"] for message in blocked_messages if message["type"] == "error")
    assert [message["status"] for message in rollback_messages if message["type"] == "ready"] == ["I"]
    assert [message["tag"] for message in create_messages if message["type"] == "command"] == ["CREATE TABLE"]
    assert [message["status"] for message in create_messages if message["type"] == "ready"] == ["I"]


def test_pgwire_extended_query_binds_text_parameters(pgwire_server):
    table = _table_name("pgwire_extended")

    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        create_messages = _pgwire_simple_query(sock, f"CREATE TABLE {table} (id text PRIMARY KEY, status text);")
        insert_messages = _pgwire_extended_query(
            sock,
            f"INSERT INTO {table} (id, status) VALUES ($1, $2);",
            ["row:extended", "ready"],
        )
        select_messages = _pgwire_extended_query(
            sock,
            f"SELECT id, status FROM {table} WHERE id = $1;",
            ["row:extended"],
        )

    assert [message["tag"] for message in create_messages if message["type"] == "command"] == ["CREATE TABLE"]
    assert [message["tag"] for message in insert_messages if message["type"] == "command"] == ["INSERT 0 1"]
    assert [message["tag"] for message in select_messages if message["type"] == "command"] == ["SELECT 1"]
    assert [message for message in select_messages if message["type"] == "columns"] == [
        {"type": "columns", "columns": ["id", "status"], "oids": [PG_TEXT_OID, PG_TEXT_OID]}
    ]
    assert [message["values"] for message in select_messages if message["type"] == "row"] == [["row:extended", "ready"]]


def test_pgwire_extended_query_binds_binary_parameters_and_results(pgwire_server):
    table = _table_name("pgwire_binary")

    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        _pgwire_simple_query(sock, f"CREATE TABLE {table} (id text PRIMARY KEY, amount numeric, active boolean, attrs jsonb);")
        insert_messages = _pgwire_extended_query_with_formats(
            sock,
            f"INSERT INTO {table} (id, amount, active, attrs) VALUES ($1, $2, $3, $4);",
            [PG_TEXT_OID, PG_INT4_OID, PG_BOOL_OID, PG_JSONB_OID],
            [1, 1, 1, 1],
            [b"row:binary", struct.pack("!i", 42), b"\x01", b'\x01{"mode":"binary"}'],
            [],
            raw_rows=False,
        )
        select_messages = _pgwire_extended_query_with_formats(
            sock,
            f"SELECT id, amount, active, attrs FROM {table} WHERE id = $1;",
            [PG_TEXT_OID],
            [1],
            [b"row:binary"],
            [1],
            raw_rows=True,
        )

    assert [message["tag"] for message in insert_messages if message["type"] == "command"] == ["INSERT 0 1"]
    assert [message for message in select_messages if message["type"] == "columns"] == [
        {"type": "columns", "columns": ["id", "amount", "active", "attrs"], "oids": [PG_TEXT_OID, PG_NUMERIC_OID, PG_BOOL_OID, PG_JSONB_OID]}
    ]
    rows = [message["values"] for message in select_messages if message["type"] == "row"]
    assert len(rows) == 1
    assert rows[0][0] == b"row:binary"
    assert rows[0][1] == b"\x00\x01\x00\x00\x00\x00\x00\x00\x00\x2a"
    assert rows[0][2] == b"\x01"
    assert rows[0][3][0] == 1
    assert json.loads(rows[0][3][1:].decode()) == {"mode": "binary"}


def test_pgwire_statement_describe_returns_parameters_and_row_description(pgwire_server):
    table = _table_name("pgwire_describe")

    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        _pgwire_simple_query(sock, f"CREATE TABLE {table} (id text PRIMARY KEY, status text);")

        read_messages = _pgwire_describe_statement(sock, f"SELECT id, status FROM {table};", [])
        parameterized_messages = _pgwire_describe_statement(sock, f"SELECT id FROM {table} WHERE status = $1;", [PG_TEXT_OID])

    assert read_messages == [
        {"type": "parameters", "oids": []},
        {"type": "columns", "columns": ["id", "status"], "oids": [PG_TEXT_OID, PG_TEXT_OID]},
    ]
    assert parameterized_messages == [
        {"type": "parameters", "oids": [PG_TEXT_OID]},
        {"type": "nodata"},
    ]


def test_pgwire_empty_select_preserves_row_description(pgwire_server):
    table = _table_name("pgwire_empty")

    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        _pgwire_simple_query(sock, f"CREATE TABLE {table} (id text PRIMARY KEY, status text, amount int);")
        messages = _pgwire_simple_query(sock, f"SELECT id, status, amount FROM {table} WHERE id = 'missing';")

    assert [message["columns"] for message in messages if message["type"] == "columns"] == [["id", "status", "amount"]]
    assert [message["oids"] for message in messages if message["type"] == "columns"] == [[PG_TEXT_OID, PG_TEXT_OID, PG_NUMERIC_OID]]
    assert [message["tag"] for message in messages if message["type"] == "command"] == ["SELECT 0"]
    assert [message["values"] for message in messages if message["type"] == "row"] == []


def test_pgwire_row_description_uses_relational_type_oids(pgwire_server):
    table = _table_name("pgwire_types")

    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        _pgwire_simple_query(
            sock,
            f"CREATE TABLE {table} ("
            "id text PRIMARY KEY, "
            "amount numeric, "
            "active boolean, "
            "created_at timestamptz, "
            "attrs jsonb, "
            "tags text[], "
            "amounts numeric[], "
            "flags boolean[], "
            "timestamps timestamptz[]"
            ");",
        )
        _pgwire_simple_query(
            sock,
            f"INSERT INTO {table} (id, amount, active, created_at, attrs) "
            "VALUES ('row:typed', 12.5, true, '2026-06-25T12:34:56Z', '{\"tier\":\"gold\"}'::jsonb);",
        )
        messages = _pgwire_simple_query(sock, f"SELECT id, amount, active, created_at, attrs, tags, amounts, flags, timestamps FROM {table} WHERE id = 'row:typed';")

    column_messages = [message for message in messages if message["type"] == "columns"]
    assert column_messages == [
        {
            "type": "columns",
            "columns": ["id", "amount", "active", "created_at", "attrs", "tags", "amounts", "flags", "timestamps"],
            "oids": [
                PG_TEXT_OID,
                PG_NUMERIC_OID,
                PG_BOOL_OID,
                PG_TIMESTAMPTZ_OID,
                PG_JSONB_OID,
                PG_TEXT_ARRAY_OID,
                PG_NUMERIC_ARRAY_OID,
                PG_BOOL_ARRAY_OID,
                PG_TIMESTAMPTZ_ARRAY_OID,
            ],
        }
    ]
    rows = [message["values"] for message in messages if message["type"] == "row"]
    assert len(rows) == 1
    assert rows[0][0:3] == ["row:typed", "12.5", "true"]
    assert rows[0][3].startswith("2026-06-25T12:34:56")
    assert json.loads(rows[0][4]) == {"tier": "gold"}
    assert rows[0][5:] == [None, None, None, None]


def test_pgwire_returning_row_description_uses_relational_type_oids(pgwire_server):
    table = _table_name("pgwire_ret_types")

    with socket.create_connection((pgwire_server.host, pgwire_server.pgwire_port), timeout=5) as sock:
        _pgwire_startup(sock)
        _pgwire_simple_query(sock, f"CREATE TABLE {table} (id text PRIMARY KEY, amount numeric, active boolean, attrs jsonb);")
        insert_messages = _pgwire_simple_query(
            sock,
            f"INSERT INTO {table} (id, amount, active, attrs) "
            "VALUES ('row:returning', 42, true, '{\"kind\":\"returning\"}'::jsonb) "
            "RETURNING id, amount, active, attrs, lower(id) AS id_key;",
        )
        update_messages = _pgwire_simple_query(
            sock,
            f"UPDATE {table} SET amount = amount + 1 WHERE id = 'row:returning' "
            "RETURNING id, amount + 1 AS next_amount;",
        )

    assert [message for message in insert_messages if message["type"] == "columns"] == [
        {
            "type": "columns",
            "columns": ["id", "amount", "active", "attrs", "id_key"],
            "oids": [PG_TEXT_OID, PG_NUMERIC_OID, PG_BOOL_OID, PG_JSONB_OID, PG_TEXT_OID],
        }
    ]
    insert_rows = [message["values"] for message in insert_messages if message["type"] == "row"]
    assert insert_rows == [["row:returning", "42", "true", '{"kind":"returning"}', "row:returning"]]

    assert [message for message in update_messages if message["type"] == "columns"] == [
        {"type": "columns", "columns": ["id", "next_amount"], "oids": [PG_TEXT_OID, PG_NUMERIC_OID]}
    ]
    assert [message["values"] for message in update_messages if message["type"] == "row"] == [["row:returning", "44"]]


def test_metadata_pgwire_simple_query_uses_public_api_sql(metadata_pgwire_server):
    table = _table_name("metadata_pgwire")

    with socket.create_connection((metadata_pgwire_server["host"], metadata_pgwire_server["pgwire_port"]), timeout=5) as sock:
        _pgwire_startup(sock)
        try:
            messages = _pgwire_simple_query(sock, f"CREATE TABLE {table} (id text PRIMARY KEY);")
        except AssertionError as err:
            raise AssertionError(f"{err}\nmetadata logs:\n{_read_log_tail(metadata_pgwire_server['log_path'])}") from err

    commands = [message["tag"] for message in messages if message["type"] == "command"]
    assert commands == ["CREATE TABLE"]


def _pgwire_startup(sock: socket.socket, *, user: str = "antfly", password: str | None = None) -> tuple[int, int]:
    payload = struct.pack("!i", 196608)
    for key, value in (("user", user),):
        payload += key.encode() + b"\x00" + value.encode() + b"\x00"
    payload += b"\x00"
    sock.sendall(struct.pack("!i", len(payload) + 4) + payload)
    backend_key: tuple[int, int] | None = None
    while True:
        tag, payload = _pgwire_read_message(sock)
        if tag == b"E":
            error = _pgwire_error_response(payload)
            raise AssertionError(f"pgwire startup error {error.get('sqlstate')}: {error.get('message')}")
        if tag == b"R":
            auth_code = struct.unpack("!i", payload[:4])[0]
            if auth_code == 0:
                continue
            if auth_code == 3:
                assert password is not None, "pgwire server requested a password"
                encoded = password.encode() + b"\x00"
                sock.sendall(b"p" + struct.pack("!i", len(encoded) + 4) + encoded)
                continue
            raise AssertionError(f"unsupported pgwire auth code: {auth_code}")
        if tag == b"K":
            assert len(payload) == 8
            backend_key = struct.unpack("!ii", payload)
        if tag == b"Z":
            assert backend_key is not None
            return backend_key


def _pgwire_cancel_request(host: str, port: int, backend_pid: int, cancel_key: int) -> None:
    payload = struct.pack("!iii", 80877102, backend_pid, cancel_key)
    with socket.create_connection((host, port), timeout=5) as cancel_sock:
        cancel_sock.sendall(struct.pack("!i", len(payload) + 4) + payload)
        cancel_sock.settimeout(5)
        try:
            assert cancel_sock.recv(1) == b""
        except ConnectionResetError:
            pass


def _pgwire_simple_query(sock: socket.socket, sql: str) -> list[dict[str, Any]]:
    payload = sql.encode() + b"\x00"
    sock.sendall(b"Q" + struct.pack("!i", len(payload) + 4) + payload)
    columns: list[str] = []
    messages: list[dict[str, Any]] = []
    while True:
        tag, payload = _pgwire_read_message(sock)
        if tag == b"T":
            description = _pgwire_row_description(payload)
            columns = [column["name"] for column in description]
            messages.append({"type": "columns", "columns": columns, "oids": [column["type_oid"] for column in description]})
        elif tag == b"D":
            messages.append({"type": "row", "values": _pgwire_data_row(payload)})
        elif tag == b"C":
            messages.append({"type": "command", "tag": payload.split(b"\x00", 1)[0].decode()})
        elif tag == b"E":
            raise AssertionError(f"pgwire query error: {payload!r}")
        elif tag == b"Z":
            messages.append({"type": "ready", "status": payload.decode()})
            return messages
        elif tag in {b"I", b"n"}:
            continue
        else:
            messages.append({"type": "message", "tag": tag.decode(errors="replace"), "columns": columns})


def _pgwire_simple_query_error(sock: socket.socket, sql: str) -> list[dict[str, Any]]:
    payload = sql.encode() + b"\x00"
    sock.sendall(b"Q" + struct.pack("!i", len(payload) + 4) + payload)
    messages: list[dict[str, Any]] = []
    while True:
        tag, payload = _pgwire_read_message(sock)
        if tag == b"E":
            messages.append({"type": "error", **_pgwire_error_response(payload)})
        elif tag == b"Z":
            messages.append({"type": "ready", "status": payload.decode()})
            return messages
        else:
            messages.append({"type": "message", "tag": tag.decode(errors="replace")})


def _pgwire_extended_query(sock: socket.socket, sql: str, params: list[str | None]) -> list[dict[str, Any]]:
    statement_name = b""
    portal_name = b""
    parse_payload = statement_name + b"\x00" + sql.encode() + b"\x00" + struct.pack("!h", 0)
    _pgwire_send_message(sock, b"P", parse_payload)

    bind_payload = portal_name + b"\x00" + statement_name + b"\x00" + struct.pack("!h", 0)
    bind_payload += struct.pack("!h", len(params))
    for param in params:
        if param is None:
            bind_payload += struct.pack("!i", -1)
        else:
            encoded = param.encode()
            bind_payload += struct.pack("!i", len(encoded)) + encoded
    bind_payload += struct.pack("!h", 0)
    _pgwire_send_message(sock, b"B", bind_payload)
    _pgwire_send_message(sock, b"D", b"P" + portal_name + b"\x00")
    _pgwire_send_message(sock, b"E", portal_name + b"\x00" + struct.pack("!i", 0))
    _pgwire_send_message(sock, b"S", b"")

    columns: list[str] = []
    messages: list[dict[str, Any]] = []
    while True:
        tag, payload = _pgwire_read_message(sock)
        if tag == b"T":
            description = _pgwire_row_description(payload)
            columns = [column["name"] for column in description]
            messages.append({"type": "columns", "columns": columns, "oids": [column["type_oid"] for column in description]})
        elif tag == b"t":
            messages.append({"type": "parameters", "oids": _pgwire_parameter_description(payload)})
        elif tag == b"D":
            messages.append({"type": "row", "values": _pgwire_data_row(payload)})
        elif tag == b"C":
            messages.append({"type": "command", "tag": payload.split(b"\x00", 1)[0].decode()})
        elif tag == b"E":
            raise AssertionError(f"pgwire extended query error: {payload!r}")
        elif tag == b"Z":
            messages.append({"type": "ready", "status": payload.decode()})
            return messages
        elif tag in {b"1", b"2", b"3", b"n", b"s"}:
            if tag == b"n":
                messages.append({"type": "nodata"})
            continue
        else:
            messages.append({"type": "message", "tag": tag.decode(errors="replace"), "columns": columns})


def _pgwire_extended_query_with_formats(
    sock: socket.socket,
    sql: str,
    parameter_oids: list[int],
    parameter_formats: list[int],
    params: list[bytes | None],
    result_formats: list[int],
    *,
    raw_rows: bool,
) -> list[dict[str, Any]]:
    statement_name = b""
    portal_name = b""
    parse_payload = statement_name + b"\x00" + sql.encode() + b"\x00" + struct.pack("!h", len(parameter_oids))
    for oid in parameter_oids:
        parse_payload += struct.pack("!i", oid)
    _pgwire_send_message(sock, b"P", parse_payload)

    bind_payload = portal_name + b"\x00" + statement_name + b"\x00" + struct.pack("!h", len(parameter_formats))
    for fmt in parameter_formats:
        bind_payload += struct.pack("!h", fmt)
    bind_payload += struct.pack("!h", len(params))
    for param in params:
        if param is None:
            bind_payload += struct.pack("!i", -1)
        else:
            bind_payload += struct.pack("!i", len(param)) + param
    bind_payload += struct.pack("!h", len(result_formats))
    for fmt in result_formats:
        bind_payload += struct.pack("!h", fmt)
    _pgwire_send_message(sock, b"B", bind_payload)
    _pgwire_send_message(sock, b"D", b"P" + portal_name + b"\x00")
    _pgwire_send_message(sock, b"E", portal_name + b"\x00" + struct.pack("!i", 0))
    _pgwire_send_message(sock, b"S", b"")

    messages: list[dict[str, Any]] = []
    while True:
        tag, payload = _pgwire_read_message(sock)
        if tag == b"T":
            description = _pgwire_row_description(payload)
            messages.append({"type": "columns", "columns": [column["name"] for column in description], "oids": [column["type_oid"] for column in description]})
        elif tag == b"D":
            messages.append({"type": "row", "values": _pgwire_data_row_raw(payload) if raw_rows else _pgwire_data_row(payload)})
        elif tag == b"C":
            messages.append({"type": "command", "tag": payload.split(b"\x00", 1)[0].decode()})
        elif tag == b"E":
            raise AssertionError(f"pgwire extended query error: {payload!r}")
        elif tag == b"Z":
            messages.append({"type": "ready", "status": payload.decode()})
            return messages
        elif tag in {b"1", b"2", b"3", b"n", b"s", b"t"}:
            continue
        else:
            messages.append({"type": "message", "tag": tag.decode(errors="replace")})


def _pgwire_describe_statement(sock: socket.socket, sql: str, parameter_oids: list[int]) -> list[dict[str, Any]]:
    statement_name = b"describe_stmt"
    parse_payload = statement_name + b"\x00" + sql.encode() + b"\x00" + struct.pack("!h", len(parameter_oids))
    for oid in parameter_oids:
        parse_payload += struct.pack("!i", oid)
    _pgwire_send_message(sock, b"P", parse_payload)
    _pgwire_send_message(sock, b"D", b"S" + statement_name + b"\x00")
    _pgwire_send_message(sock, b"S", b"")

    messages: list[dict[str, Any]] = []
    while True:
        tag, payload = _pgwire_read_message(sock)
        if tag == b"t":
            messages.append({"type": "parameters", "oids": _pgwire_parameter_description(payload)})
        elif tag == b"T":
            description = _pgwire_row_description(payload)
            messages.append(
                {
                    "type": "columns",
                    "columns": [column["name"] for column in description],
                    "oids": [column["type_oid"] for column in description],
                }
            )
        elif tag == b"n":
            messages.append({"type": "nodata"})
        elif tag == b"E":
            raise AssertionError(f"pgwire statement describe error: {payload!r}")
        elif tag == b"Z":
            return messages
        elif tag in {b"1", b"2", b"3"}:
            continue
        else:
            messages.append({"type": "message", "tag": tag.decode(errors="replace")})


def _pgwire_send_message(sock: socket.socket, tag: bytes, payload: bytes) -> None:
    sock.sendall(tag + struct.pack("!i", len(payload) + 4) + payload)


def _pgwire_read_message(sock: socket.socket) -> tuple[bytes, bytes]:
    tag = _recv_exact(sock, 1)
    length = struct.unpack("!i", _recv_exact(sock, 4))[0]
    assert length >= 4
    return tag, _recv_exact(sock, length - 4)


def _pgwire_row_description(payload: bytes) -> list[dict[str, Any]]:
    count = struct.unpack("!h", payload[:2])[0]
    offset = 2
    columns: list[dict[str, Any]] = []
    for _ in range(count):
        end = payload.index(b"\x00", offset)
        name = payload[offset:end].decode()
        offset = end + 1
        offset += 4
        offset += 2
        type_oid = struct.unpack("!i", payload[offset : offset + 4])[0]
        offset += 4
        offset += 2
        offset += 4
        offset += 2
        columns.append({"name": name, "type_oid": type_oid})
    return columns


def _pgwire_parameter_description(payload: bytes) -> list[int]:
    count = struct.unpack("!h", payload[:2])[0]
    offset = 2
    oids: list[int] = []
    for _ in range(count):
        oids.append(struct.unpack("!i", payload[offset : offset + 4])[0])
        offset += 4
    return oids


def _pgwire_error_response(payload: bytes) -> dict[str, str]:
    fields: dict[str, str] = {}
    offset = 0
    while offset < len(payload) and payload[offset] != 0:
        field = chr(payload[offset])
        offset += 1
        end = payload.index(b"\x00", offset)
        fields[field] = payload[offset:end].decode()
        offset = end + 1
    return {"severity": fields.get("S", ""), "sqlstate": fields.get("C", ""), "message": fields.get("M", "")}


def _pgwire_data_row(payload: bytes) -> list[str | None]:
    count = struct.unpack("!h", payload[:2])[0]
    offset = 2
    values: list[str | None] = []
    for _ in range(count):
        length = struct.unpack("!i", payload[offset : offset + 4])[0]
        offset += 4
        if length == -1:
            values.append(None)
            continue
        values.append(payload[offset : offset + length].decode())
        offset += length
    return values


def _pgwire_data_row_raw(payload: bytes) -> list[bytes | None]:
    count = struct.unpack("!h", payload[:2])[0]
    offset = 2
    values: list[bytes | None] = []
    for _ in range(count):
        length = struct.unpack("!i", payload[offset : offset + 4])[0]
        offset += 4
        if length == -1:
            values.append(None)
            continue
        values.append(payload[offset : offset + length])
        offset += length
    return values


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError("pgwire socket closed")
        data += chunk
    return data
