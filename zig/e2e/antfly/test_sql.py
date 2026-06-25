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
                    "amount": {"type": "numeric"},
                    "note": {"type": "keyword"},
                    "tags": {"type": "array", "items": {"type": "keyword"}},
                    "metadata": {"type": "json"},
                },
                "additionalProperties": True,
            }
        }
    },
}

DOCUMENT_SQL_VIRTUAL_ROOT_SCHEMA = {
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
    created = stateful_api.create_table(
        table_name, num_shards=1, schema=RELATIONAL_SQL_SCHEMA
    )
    assert created["name"] == table_name


def _create_document_sql_table(stateful_api, table_name: str) -> None:
    created = stateful_api.create_table(
        table_name, num_shards=1, schema=DOCUMENT_SQL_SCHEMA
    )
    assert created["name"] == table_name


def _create_document_sql_virtual_root_table(stateful_api, table_name: str) -> None:
    created = stateful_api.create_table(
        table_name,
        num_shards=1,
        schema=DOCUMENT_SQL_VIRTUAL_ROOT_SCHEMA,
        typed_paths={"numeric": ["metrics.score"]},
    )
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


def _query_hit_ids(payload: dict[str, Any]) -> list[str] | None:
    responses = payload.get("responses")
    if not responses:
        return None
    hits = responses[0].get("hits", {}).get("hits")
    if not hits:
        return None
    ids: list[str] = []
    for hit in hits:
        doc_id = hit.get("_id") or hit.get("doc_id")
        if not isinstance(doc_id, str):
            return None
        ids.append(doc_id)
    return ids


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


def test_sql_cli_command_and_file_execute_against_real_server(
    stateful_api, sql_cli, tmp_path
):
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

    stateful_api.create_index(
        table,
        "amount_alg",
        {
            "type": "algebraic",
            "derive_from_schema": True,
        },
    )

    written = stateful_api.batch_write(
        table,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "alpha search document",
                "status": "active",
                "amount": 10,
                "category": "release",
                "tags": ["urgent", "search"],
                "metadata": {"plan": "pro", "billing": {"plan": "annual"}},
            },
            "doc:b": {
                "title": "beta",
                "body": "beta archive document",
                "status": "archived",
                "amount": 20,
                "category": "archive",
                "tags": ["archive"],
                "metadata": {"plan": "free", "billing": {"plan": "monthly"}},
            },
            "doc:c": {
                "title": "alpha followup",
                "body": "alpha search followup",
                "status": "active",
                "amount": 12,
                "note": "followup",
                "category": "release",
                "tags": ["search"],
                "metadata": {"plan": "team", "billing": {"plan": "annual"}},
            },
            "doc:d": {
                "title": "delta archived",
                "body": "delta search archive",
                "status": "archived",
                "amount": 30,
                "note": "closed",
                "category": "archive",
                "tags": ["archive", "search"],
                "metadata": {"plan": "enterprise", "billing": {"plan": "annual"}},
            },
        },
        sync_level="full_index",
    )
    assert written["inserted"] == 4

    stateful_api.create_index(
        table,
        "category_fts",
        {"name": "category_fts", "type": "full_text", "field": "category"},
    )

    direct_by_id = stateful_api.post(
        "/db/v1/sql",
        {"sql": f"SELECT _id, title FROM {table} WHERE _id = 'doc:a';"},
    )
    assert direct_by_id["kind"] == "read"
    assert direct_by_id["statement_kind"] == "query"
    assert direct_by_id["result"]["rows"] == [{"_id": "doc:a", "title": "alpha"}]

    write_response = stateful_api.s.post(
        f"{stateful_api.url}/db/v1/sql",
        json={"sql": f"INSERT INTO {table} (_id, _doc) VALUES ('doc:z', '{{}}');"},
        timeout=10,
    )
    assert write_response.status_code == 400
    assert write_response.text == "document_sql_write_unsupported"

    view_response = stateful_api.s.post(
        f"{stateful_api.url}/db/v1/sql",
        json={
            "sql": (
                f"CREATE VIEW {table}_view(doc_id, title) AS "
                f"SELECT _id, title FROM {table};"
            )
        },
        timeout=10,
    )
    assert view_response.status_code == 400
    assert view_response.text == "document_sql_view_mapping_unsupported"

    array_response = stateful_api.s.post(
        f"{stateful_api.url}/db/v1/sql",
        json={"sql": f"SELECT _id FROM {table} WHERE tags = 'urgent' LIMIT 10;"},
        timeout=10,
    )
    assert array_response.status_code == 400
    assert array_response.text == "document_sql_array_requires_unnest"

    unnested_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT d._id, tag FROM {table} AS d, UNNEST(d.tags) AS tag "
                "WHERE tag = 'search' LIMIT 10;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in unnested_rows) == ["doc:a", "doc:c", "doc:d"]
    assert {row["tag"] for row in unnested_rows} == {"search"}

    lookup_unnested_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT d._id, tag FROM {table} AS d, UNNEST(d.tags) AS tag "
                "WHERE d._id = 'doc:a' AND tag = 'urgent' LIMIT 10;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert lookup_unnested_rows == [{"_id": "doc:a", "tag": "urgent"}]

    indexed_unnested_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT d._id, tag FROM {table} AS d, UNNEST(d.tags) AS tag "
                "WHERE full_text_search('body:search') AND tag = 'archive' LIMIT 10;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert indexed_unnested_rows == [{"_id": "doc:d", "tag": "archive"}]

    ordered_indexed_unnested_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT d._id, tag FROM {table} AS d, UNNEST(d.tags) AS tag "
                "WHERE full_text_search('body:search') ORDER BY tag ASC LIMIT 4;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert ordered_indexed_unnested_rows == [
        {"_id": "doc:d", "tag": "archive"},
        {"_id": "doc:a", "tag": "search"},
        {"_id": "doc:c", "tag": "search"},
        {"_id": "doc:d", "tag": "search"},
    ]

    ordered_unnested_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT d._id, tag FROM {table} AS d, UNNEST(d.tags) AS tag "
                "ORDER BY tag ASC LIMIT 5;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert ordered_unnested_rows == [
        {"_id": "doc:b", "tag": "archive"},
        {"_id": "doc:d", "tag": "archive"},
        {"_id": "doc:a", "tag": "search"},
        {"_id": "doc:c", "tag": "search"},
        {"_id": "doc:d", "tag": "search"},
    ]

    join_response = stateful_api.s.post(
        f"{stateful_api.url}/db/v1/sql",
        json={
            "sql": (
                f"SELECT {table}._id FROM {table} "
                f"JOIN {table} AS other_docs ON {table}._id = other_docs._id;"
            )
        },
        timeout=10,
    )
    assert join_response.status_code == 400
    assert join_response.text == "document_sql_unsupported_join"

    unsupported_shape_cases = [
        (
            f"SELECT DISTINCT _id FROM {table} WHERE _id = 'doc:a';",
            "document_sql_projection_modifier_unsupported",
        ),
        (
            f"SELECT DISTINCT ON (status) _id FROM {table} WHERE status = 'active' LIMIT 10;",
            "document_sql_projection_modifier_unsupported",
        ),
        (
            f"SELECT ALL _id FROM {table} WHERE _id = 'doc:a';",
            "document_sql_projection_modifier_unsupported",
        ),
        (
            f"SELECT _id FROM {table} WHERE status = 'active' OFFSET 1;",
            "document_sql_pagination_unsupported",
        ),
        (
            f"SELECT _id FROM {table} FETCH FIRST 10 ROWS ONLY;",
            "document_sql_pagination_unsupported",
        ),
        (
            f"SELECT _id FROM {table} WHERE _id = 'doc:a' FOR UPDATE;",
            "document_sql_locking_unsupported",
        ),
        (
            f"SELECT _id FROM {table} WINDOW w AS () LIMIT 10;",
            "document_sql_window_unsupported",
        ),
    ]
    for sql, expected_body in unsupported_shape_cases:
        unsupported_response = stateful_api.s.post(
            f"{stateful_api.url}/db/v1/sql",
            json={"sql": sql},
            timeout=10,
        )
        assert unsupported_response.status_code == 400
        assert unsupported_response.text == expected_body

    native_search_predicate_response = stateful_api.s.post(
        f"{stateful_api.url}/db/v1/sql",
        json={
            "sql": (
                f"SELECT _id FROM {table} "
                "WHERE antfly.hybrid_search(table_name => '"
                f"{table}', query => 'search', limit => 10) LIMIT 10;"
            )
        },
        timeout=10,
    )
    assert native_search_predicate_response.status_code == 400
    assert (
        native_search_predicate_response.text
        == "document_sql_native_search_requires_table_function"
    )

    native_full_text_predicate_response = stateful_api.s.post(
        f"{stateful_api.url}/db/v1/sql",
        json={
            "sql": (
                f"SELECT _id FROM {table} "
                "WHERE antfly.full_text_search('body:search') LIMIT 10;"
            )
        },
        timeout=10,
    )
    assert native_full_text_predicate_response.status_code == 400
    assert (
        native_full_text_predicate_response.text
        == "document_sql_native_search_requires_table_function"
    )

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

    by_json_path_filter = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT _id, metadata->>'plan' AS plan FROM {table} "
                "WHERE metadata->>'plan' = 'pro' LIMIT 10;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert by_json_path_filter == [{"_id": "doc:a", "plan": "pro"}]

    by_nested_json_path_filter = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT _id, metadata#>>'{{billing,plan}}' AS billing_plan FROM {table} "
                "WHERE metadata#>>'{billing,plan}' = 'monthly' LIMIT 10;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert by_nested_json_path_filter == [
        {"_id": "doc:b", "billing_plan": "monthly"}
    ]

    active_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT _id, status FROM {table} WHERE status = 'active' LIMIT 10;",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in active_rows) == ["doc:a", "doc:c"]

    scalar_compat_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT _id, amount FROM {table} "
                "WHERE status IN ('active', 'pending') "
                "AND title LIKE 'alpha%' "
                "AND amount BETWEEN 5 AND 15 "
                "ORDER BY _id ASC LIMIT 10;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert scalar_compat_rows == [
        {"_id": "doc:a", "amount": 10},
        {"_id": "doc:c", "amount": 12},
    ]

    null_predicate_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT _id FROM {table} WHERE note IS NULL ORDER BY _id ASC LIMIT 10;",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert null_predicate_rows == [{"_id": "doc:a"}, {"_id": "doc:b"}]

    not_null_predicate_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT _id FROM {table} WHERE note IS NOT NULL ORDER BY _id ASC LIMIT 10;",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert not_null_predicate_rows == [{"_id": "doc:c"}, {"_id": "doc:d"}]

    category_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT _id, category FROM {table} WHERE category = 'release' LIMIT 10;",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in category_rows) == ["doc:a", "doc:c"]
    assert {row["category"] for row in category_rows} == {"release"}

    unbounded_scan_response = stateful_api.s.post(
        f"{stateful_api.url}/db/v1/sql",
        json={"sql": f"SELECT _id, status FROM {table} WHERE status = 'active';"},
        timeout=10,
    )
    assert unbounded_scan_response.status_code == 400
    assert unbounded_scan_response.text == "document_sql_requires_bounded_scan"

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
    assert sorted(row["_id"] for row in full_text_rows) == ["doc:a", "doc:c", "doc:d"]

    native_full_text_ids = wait_until(
        lambda: _query_hit_ids(
            stateful_api.query_table(
                table,
                {
                    "full_text_search": {"query": "body:search"},
                    "limit": 10,
                },
            )
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(native_full_text_ids) == sorted(row["_id"] for row in full_text_rows)

    function_full_text_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                "SELECT * FROM antfly.full_text_search("
                f"table_name => '{table}', query => 'body:search', limit => 10);"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in function_full_text_rows) == sorted(native_full_text_ids)
    assert all("_score" in row for row in function_full_text_rows)
    assert all("_source" in row for row in function_full_text_rows)

    projected_function_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                "SELECT _id, _score FROM antfly.full_text_search("
                f"table_name => '{table}', query => 'body:search', limit => 10);"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in projected_function_rows) == sorted(native_full_text_ids)
    assert all("_score" in row for row in projected_function_rows)
    assert all("_source" not in row for row in projected_function_rows)

    full_text_residual_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            f"SELECT _id, status FROM {table} WHERE full_text_search('body:search') AND status = 'active' LIMIT 10;",
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in full_text_residual_rows) == ["doc:a", "doc:c"]

    counted = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT count(*) AS row_count FROM {table} WHERE full_text_search('body:search');",
        ).stdout
    )
    assert counted["kind"] == "read"
    assert counted["statement_kind"] == "aggregate"
    assert counted["result"]["rows"] == [{"row_count": 3}]

    grouped_counted = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT count(*) AS row_count FROM {table} WHERE full_text_search('body:search') GROUP BY status LIMIT 10;",
        ).stdout
    )
    assert grouped_counted["kind"] == "read"
    assert grouped_counted["statement_kind"] == "aggregate"
    grouped_rows = sorted(
        grouped_counted["result"]["rows"], key=lambda row: row["status"]
    )
    assert grouped_rows == [
        {"status": "active", "row_count": 2},
        {"status": "archived", "row_count": 1},
    ]

    residual_counted = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT count(*) AS row_count FROM {table} WHERE full_text_search('body:search') AND status = 'active';",
        ).stdout
    )
    assert residual_counted["kind"] == "read"
    assert residual_counted["statement_kind"] == "aggregate"
    assert residual_counted["result"]["rows"] == [{"row_count": 2}]

    grouped_scan_counted = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT count(*) AS row_count FROM {table} WHERE status = 'archived' GROUP BY category LIMIT 10;",
        ).stdout
    )
    assert grouped_scan_counted["kind"] == "read"
    assert grouped_scan_counted["statement_kind"] == "aggregate"
    assert grouped_scan_counted["result"]["rows"] == [
        {"category": "archive", "row_count": 2}
    ]

    summed = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT sum(amount) AS total_amount FROM {table};",
        ).stdout
    )
    assert summed["kind"] == "read"
    assert summed["statement_kind"] == "aggregate"
    assert summed["result"]["rows"] == [{"total_amount": 72}]

    min_amount = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT min(amount) AS min_amount FROM {table};",
        ).stdout
    )
    assert min_amount["kind"] == "read"
    assert min_amount["statement_kind"] == "aggregate"
    assert min_amount["result"]["rows"] == [{"min_amount": 10}]

    avg_amount = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT avg(amount) AS avg_amount FROM {table};",
        ).stdout
    )
    assert avg_amount["kind"] == "read"
    assert avg_amount["statement_kind"] == "aggregate"
    assert avg_amount["result"]["rows"] == [{"avg_amount": 18}]

    max_amount = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT max(amount) AS max_amount FROM {table} WHERE full_text_search('body:search');",
        ).stdout
    )
    assert max_amount["kind"] == "read"
    assert max_amount["statement_kind"] == "aggregate"
    assert max_amount["result"]["rows"] == [{"max_amount": 30}]

    filtered_summed = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT sum(amount) AS total_amount FROM {table} WHERE full_text_search('body:search') AND status = 'active';",
        ).stdout
    )
    assert filtered_summed["kind"] == "read"
    assert filtered_summed["statement_kind"] == "aggregate"
    assert filtered_summed["result"]["rows"] == [{"total_amount": 22}]

    grouped_summed = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT sum(amount) AS total_amount FROM {table} WHERE full_text_search('body:search') GROUP BY status LIMIT 10;",
        ).stdout
    )
    assert grouped_summed["kind"] == "read"
    assert grouped_summed["statement_kind"] == "aggregate"
    grouped_sum_rows = sorted(
        grouped_summed["result"]["rows"], key=lambda row: row["status"]
    )
    assert grouped_sum_rows == [
        {"status": "active", "total_amount": 22},
        {"status": "archived", "total_amount": 30},
    ]

    materialized_grouped_avg = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT avg(amount) AS avg_amount FROM {table} GROUP BY status LIMIT 10;",
        ).stdout
    )
    assert materialized_grouped_avg["kind"] == "read"
    assert materialized_grouped_avg["statement_kind"] == "aggregate"
    materialized_grouped_avg_rows = sorted(
        materialized_grouped_avg["result"]["rows"], key=lambda row: row["status"]
    )
    assert materialized_grouped_avg_rows == [
        {"status": "active", "avg_amount": 11},
        {"status": "archived", "avg_amount": 25},
    ]

    grouped_maxed = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT max(amount) AS max_amount FROM {table} WHERE full_text_search('body:search') GROUP BY status LIMIT 10;",
        ).stdout
    )
    assert grouped_maxed["kind"] == "read"
    assert grouped_maxed["statement_kind"] == "aggregate"
    grouped_max_rows = sorted(
        grouped_maxed["result"]["rows"], key=lambda row: row["status"]
    )
    assert grouped_max_rows == [
        {"status": "active", "max_amount": 12},
        {"status": "archived", "max_amount": 30},
    ]

    grouped_averaged = _first_sql_json(
        sql_cli(
            "sql",
            "-c",
            f"SELECT avg(amount) AS avg_amount FROM {table} WHERE full_text_search('body:search') GROUP BY status LIMIT 10;",
        ).stdout
    )
    assert grouped_averaged["kind"] == "read"
    assert grouped_averaged["statement_kind"] == "aggregate"
    grouped_avg_rows = sorted(
        grouped_averaged["result"]["rows"], key=lambda row: row["status"]
    )
    assert grouped_avg_rows == [
        {"status": "active", "avg_amount": 11},
        {"status": "archived", "avg_amount": 30},
    ]

    virtual_field_by_id = stateful_api.post(
        "/db/v1/sql",
        {"sql": f"SELECT _id, category FROM {table} WHERE _id = 'doc:a';"},
    )
    assert virtual_field_by_id["kind"] == "read"
    assert virtual_field_by_id["statement_kind"] == "query"
    assert virtual_field_by_id["result"]["rows"] == [
        {"_id": "doc:a", "category": "release"}
    ]

    virtual_star_by_id = stateful_api.post(
        "/db/v1/sql",
        {"sql": f"SELECT * FROM {table} WHERE _id = 'doc:a';"},
    )
    assert virtual_star_by_id["kind"] == "read"
    assert virtual_star_by_id["statement_kind"] == "query"
    assert virtual_star_by_id["result"]["rows"][0]["category"] == "release"
    assert virtual_star_by_id["result"]["rows"][0]["_doc"]["category"] == "release"

    doc_root_path_rows = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT _id, _doc->>'category' AS category, "
                f"_doc#>>'{{metadata,billing,plan}}' AS billing_plan FROM {table} "
                "WHERE _doc->>'category' = 'release' ORDER BY _id ASC LIMIT 10;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert doc_root_path_rows == [
        {"_id": "doc:a", "category": "release", "billing_plan": "annual"},
        {"_id": "doc:c", "category": "release", "billing_plan": "annual"},
    ]

    virtual_root_table = _sql_table_name("sql_cli_docs_virtual_root")
    _create_document_sql_virtual_root_table(stateful_api, virtual_root_table)
    virtual_root_detail = stateful_api.get_table(virtual_root_table)
    virtual_root_props = (
        virtual_root_detail["schema"]["document_schemas"]["doc"]["schema"]["properties"]
    )
    assert "metadata" not in virtual_root_props

    virtual_root_written = stateful_api.batch_write(
        virtual_root_table,
        inserts={
            "doc:vr:a": {
                "title": "virtual alpha",
                "body": "virtual root alpha",
                "status": "active",
                "metadata": {"plan": "pro"},
                "metrics": {"score": 9},
            },
            "doc:vr:b": {
                "title": "virtual beta",
                "body": "virtual root beta",
                "status": "archived",
                "metadata": {"plan": "free"},
                "metrics": {"score": 3},
            },
            "doc:vr:c": {
                "title": "virtual gamma",
                "body": "virtual root gamma",
                "status": "active",
                "metadata": {"plan": "pro"},
                "metrics": {"score": 12},
            },
        },
        sync_level="full_index",
    )
    assert virtual_root_written["inserted"] == 3

    stateful_api.create_index(
        virtual_root_table,
        "metadata_plan_fts",
        {
            "name": "metadata_plan_fts",
            "type": "full_text",
            "field": "metadata.plan",
        },
    )
    virtual_root_projection = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT _id, metadata->>'plan' AS plan FROM {virtual_root_table} "
                "WHERE _id = 'doc:vr:a';"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert virtual_root_projection == [{"_id": "doc:vr:a", "plan": "pro"}]

    virtual_root_filter = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT _id, metadata->>'plan' AS plan FROM {virtual_root_table} "
                "WHERE metadata->>'plan' = 'pro' LIMIT 10;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in virtual_root_filter) == [
        "doc:vr:a",
        "doc:vr:c",
    ]
    assert {row["plan"] for row in virtual_root_filter} == {"pro"}

    virtual_root_numeric_filter = wait_until(
        lambda: _select_rows(
            sql_cli,
            (
                f"SELECT _id, metrics->>'score' AS score FROM {virtual_root_table} "
                "WHERE metrics->>'score' >= 7 LIMIT 10;"
            ),
        ),
        timeout_s=15.0,
        interval_s=0.5,
    )
    assert sorted(row["_id"] for row in virtual_root_numeric_filter) == [
        "doc:vr:a",
        "doc:vr:c",
    ]
    assert {row["score"] for row in virtual_root_numeric_filter} == {9, 12}

    virtual_root_ordered = _select_rows(
        sql_cli,
        (
            f"SELECT _id, metadata->>'plan' AS plan FROM {virtual_root_table} "
            "ORDER BY metadata->>'plan' ASC LIMIT 3;"
        ),
    )
    assert virtual_root_ordered == [
        {"_id": "doc:vr:b", "plan": "free"},
        {"_id": "doc:vr:a", "plan": "pro"},
        {"_id": "doc:vr:c", "plan": "pro"},
    ]
