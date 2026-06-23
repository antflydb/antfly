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

"""E2E tests for the psql-style `antfly sql` CLI command."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import (
    ANTFLY_PUBLIC_API_ROOT,
    DEFAULT_ANTFLY_BIN,
    resolve_binary_path,
)
from helpers import wait_until


RELATIONAL_SQL_SCHEMA = {
    "version": 1,
    "storage_mode": "relational",
    "default_type": "row",
    "enforce_types": True,
    "document_schemas": {
        "row": {
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "amount": {"type": "numeric"},
                },
                "required": ["id"],
                "additionalProperties": False,
            }
        }
    },
    "primary_key": {"columns": ["id"]},
}

DOCUMENT_SQL_SCHEMA = {
    "version": 1,
    "storage_mode": "document",
    "default_type": "doc",
    "document_schemas": {
        "doc": {
            "schema": {
                "type": "object",
                "properties": {
                    "title": {"type": "text"},
                    "body": {"type": "text"},
                    "status": {"type": "keyword"},
                    "metadata": {"type": "json"},
                },
                "additionalProperties": True,
            }
        }
    },
}


@pytest.fixture(scope="function")
def sql_cli(stateful_api):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    env = os.environ.copy()
    env["ANTFLY_URL"] = _server_base_url_for_cli(stateful_api.url)

    def run_cli(
        *args: str,
        check: bool = True,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        cmd = [binary] + list(args)
        result = subprocess.run(
            cmd,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
        if check and result.returncode != 0:
            raise AssertionError(
                f"CLI failed (exit {result.returncode}): {' '.join(cmd)}\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}\n"
                f"server logs:\n{stateful_api._server.debug_logs()[-2000:] if stateful_api._server is not None else ''}"
            )
        return result

    return run_cli


def _server_base_url_for_cli(public_api_url: str) -> str:
    if public_api_url.endswith(ANTFLY_PUBLIC_API_ROOT):
        return public_api_url[: -len(ANTFLY_PUBLIC_API_ROOT)]
    return public_api_url


def _sql_table_name(prefix: str) -> str:
    return f"{prefix}_{time.time_ns()}"


def _create_relational_sql_table(stateful_api, table_name: str) -> None:
    created = stateful_api.create_table(table_name, num_shards=1, schema=RELATIONAL_SQL_SCHEMA)
    assert created["name"] == table_name


def _create_document_sql_table(stateful_api, table_name: str) -> None:
    created = stateful_api.create_table(table_name, num_shards=1, schema=DOCUMENT_SQL_SCHEMA)
    assert created["name"] == table_name


def _json_stream(stdout: str) -> list[dict[str, Any]]:
    cleaned = stdout.replace("antfly=> ", "").replace("antfly-> ", "")
    decoder = json.JSONDecoder()
    values: list[dict[str, Any]] = []
    offset = 0
    while offset < len(cleaned):
        while offset < len(cleaned) and cleaned[offset].isspace():
            offset += 1
        if offset >= len(cleaned):
            break
        value, offset = decoder.raw_decode(cleaned, offset)
        assert isinstance(value, dict)
        values.append(value)
    return values


def _first_sql_json(stdout: str) -> dict[str, Any]:
    values = _json_stream(stdout)
    assert values
    return values[0]


def _select_rows(sql_cli, sql: str) -> list[dict[str, Any]] | None:
    result = sql_cli("sql", "-c", sql, check=False)
    if result.returncode != 0:
        return None
    response = _first_sql_json(result.stdout)
    rows = response.get("result", {}).get("rows")
    if not rows:
        return None
    return rows


def test_sql_cli_help_does_not_require_server():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")

    result = subprocess.run(
        [binary, "sql", "--help"],
        capture_output=True,
        text=True,
        timeout=10,
        env=os.environ.copy(),
    )

    assert result.returncode == 0
    assert "usage: antfly sql" in result.stderr
    assert "psql-style REPL" in result.stderr


def test_sql_cli_command_and_file_execute_against_real_server(stateful_api, sql_cli, tmp_path):
    command_table = _sql_table_name("sql_cli_cmd")
    file_table = _sql_table_name("sql_cli_file")
    _create_relational_sql_table(stateful_api, command_table)
    _create_relational_sql_table(stateful_api, file_table)

    inserted_one = sql_cli(
        "sql",
        "-c",
        f"INSERT INTO {command_table} (id, status, amount) VALUES ('row:a', 'open', 10) RETURNING id;",
    )
    insert_response = _first_sql_json(inserted_one.stdout)
    assert insert_response["kind"] == "write"
    assert insert_response["statement_kind"] == "insert"
    assert insert_response["result"]["returning"] == [{"id": "row:a"}]

    script = tmp_path / "rows.sql"
    script.write_text(
        f"INSERT INTO {file_table} (id, status, amount) VALUES ('row:c', 'open', 20) RETURNING id;\n",
        encoding="utf-8",
    )

    inserted = _json_stream(sql_cli("sql", "-f", str(script)).stdout)
    assert [response["statement_kind"] for response in inserted] == ["insert"]
    assert [response["result"]["inserted"] for response in inserted] == [1]

    rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT id, amount FROM {command_table} WHERE status = 'open';",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert rows == [{"id": "row:a", "amount": 10}]

    file_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT id, amount FROM {file_table} WHERE status = 'open';",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert file_rows == [{"id": "row:c", "amount": 20}]


def test_sql_cli_repl_reads_stdin_and_reuses_session(stateful_api, sql_cli):
    table = _sql_table_name("sql_cli_repl")
    _create_relational_sql_table(stateful_api, table)

    repl_input = "\n".join(
        [
            f"INSERT INTO {table} (id, status, amount)",
            "VALUES ('repl:a', 'open', 7) RETURNING id;",
            f"SELECT id, amount FROM {table} WHERE status = 'open';",
            "\\q",
            "",
        ]
    )

    result = sql_cli("sql", input_text=repl_input)
    assert "antfly=> " in result.stdout
    assert "antfly-> " in result.stdout

    responses = _json_stream(result.stdout)
    assert [response["kind"] for response in responses] == ["write", "read"]
    session_ids = {response["session_id"] for response in responses}
    assert len(session_ids) == 1
    assert responses[0]["statement_kind"] == "insert"
    assert responses[1]["statement_kind"] == "query"
    assert responses[1]["result"]["rows"] == [{"id": "repl:a", "amount": 7}]


def test_sql_cli_queries_document_tables(stateful_api, sql_cli):
    table = _sql_table_name("sql_cli_docs")
    _create_document_sql_table(stateful_api, table)
    table_detail = stateful_api.get_table(table)
    assert table_detail["schema"]["storage_mode"] == "document"

    written = stateful_api.batch_write(
        table,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "alpha search document",
                "status": "active",
                "metadata": {"plan": "pro"},
            },
            "doc:b": {
                "title": "beta",
                "body": "beta archive document",
                "status": "archived",
                "metadata": {"plan": "free"},
            },
            "doc:c": {
                "title": "alpha followup",
                "body": "alpha search followup",
                "status": "active",
                "metadata": {"plan": "team"},
            },
        },
        sync_level="full_index",
    )
    assert written["inserted"] == 3

    direct_by_id = stateful_api.post(
        "/sql",
        {"sql": f"SELECT _id, title FROM {table} WHERE _id = 'doc:a';"},
    )
    assert direct_by_id["kind"] == "read"
    assert direct_by_id["statement_kind"] == "query"
    assert direct_by_id["result"]["rows"] == [{"_id": "doc:a", "title": "alpha"}]

    by_id = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT _id, title FROM {table} WHERE _id = 'doc:a';",
        ).stdout
    )
    assert by_id["kind"] == "read"
    assert by_id["statement_kind"] == "query"
    assert by_id["result"]["rows"] == [{"_id": "doc:a", "title": "alpha"}]

    by_json_path = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT _id, metadata->>'plan' AS plan FROM {table} WHERE _id = 'doc:a';",
        ).stdout
    )
    assert by_json_path["kind"] == "read"
    assert by_json_path["statement_kind"] == "query"
    assert by_json_path["result"]["rows"] == [{"_id": "doc:a", "plan": "pro"}]

    active_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT _id, status FROM {table} WHERE status = 'active' LIMIT 10;",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in active_rows) == ["doc:a", "doc:c"]

    ordered_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT _id, title FROM {table} WHERE status = 'active' ORDER BY title DESC LIMIT 2;",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert [row["_id"] for row in ordered_rows] == ["doc:c", "doc:a"]

    full_text_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT _id, title FROM {table} WHERE full_text_search('body:search') LIMIT 10;",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in full_text_rows) == ["doc:a", "doc:c"]

    counted = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT count(*) AS row_count FROM {table} WHERE full_text_search('body:search');",
        ).stdout
    )
    assert counted["kind"] == "read"
    assert counted["statement_kind"] == "aggregate"
    assert counted["result"]["rows"] == [{"row_count": 2}]
