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
PG_TEXT_OID = 25
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
            "attrs jsonb"
            ");",
        )
        _pgwire_simple_query(
            sock,
            f"INSERT INTO {table} (id, amount, active, created_at, attrs) "
            "VALUES ('row:typed', 12.5, true, '2026-06-25T12:34:56Z', '{\"tier\":\"gold\"}'::jsonb);",
        )
        messages = _pgwire_simple_query(sock, f"SELECT id, amount, active, created_at, attrs FROM {table} WHERE id = 'row:typed';")

    column_messages = [message for message in messages if message["type"] == "columns"]
    assert column_messages == [
        {
            "type": "columns",
            "columns": ["id", "amount", "active", "created_at", "attrs"],
            "oids": [PG_TEXT_OID, PG_NUMERIC_OID, PG_BOOL_OID, PG_TIMESTAMPTZ_OID, PG_JSONB_OID],
        }
    ]
    rows = [message["values"] for message in messages if message["type"] == "row"]
    assert len(rows) == 1
    assert rows[0][0:3] == ["row:typed", "12.5", "true"]
    assert rows[0][3].startswith("2026-06-25T12:34:56")
    assert json.loads(rows[0][4]) == {"tier": "gold"}


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


def _pgwire_startup(sock: socket.socket) -> None:
    payload = struct.pack("!i", 196608)
    for key, value in (("user", "antfly"),):
        payload += key.encode() + b"\x00" + value.encode() + b"\x00"
    payload += b"\x00"
    sock.sendall(struct.pack("!i", len(payload) + 4) + payload)
    while True:
        tag, payload = _pgwire_read_message(sock)
        if tag == b"E":
            raise AssertionError(f"pgwire startup error: {payload!r}")
        if tag == b"Z":
            return


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
            return messages
        elif tag in {b"I", b"n"}:
            continue
        else:
            messages.append({"type": "message", "tag": tag.decode(errors="replace"), "columns": columns})


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
        elif tag == b"D":
            messages.append({"type": "row", "values": _pgwire_data_row(payload)})
        elif tag == b"C":
            messages.append({"type": "command", "tag": payload.split(b"\x00", 1)[0].decode()})
        elif tag == b"E":
            raise AssertionError(f"pgwire extended query error: {payload!r}")
        elif tag == b"Z":
            return messages
        elif tag in {b"1", b"2", b"3", b"n", b"s"}:
            continue
        else:
            messages.append({"type": "message", "tag": tag.decode(errors="replace"), "columns": columns})


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


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError("pgwire socket closed")
        data += chunk
    return data
