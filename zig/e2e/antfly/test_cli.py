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

"""E2E tests for the antfly CLI client commands.

These tests start an antfly standalone server, then exercise the CLI binary
(table, insert, lookup, query, delete, internal) by shelling out via
subprocess and verifying stdout JSON and exit codes.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import pytest

from conftest import (
    DEFAULT_ANTFLY_BIN,
    InferenceEmbeddingServer,
    InferenceGeneratorServer,
    StandaloneAntflyServer,
    resolve_binary_path,
)
from helpers import wait_until
from port_reservations import find_free_port


@pytest.fixture(scope="module")
def cli_inference_servers():
    embedder = InferenceEmbeddingServer()
    generator = InferenceGeneratorServer()
    yield {"embedder": embedder.url, "generator": generator.url}
    generator.stop()
    embedder.stop()


@pytest.fixture(scope="module")
def cli_server(cli_inference_servers):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")

    port = find_free_port()
    server = StandaloneAntflyServer(binary, "127.0.0.1", port)
    server.cli_inference_urls = cli_inference_servers
    yield server
    server.stop()


@pytest.fixture(scope="module")
def cli(cli_server):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    env = os.environ.copy()
    env["ANTFLY_URL"] = cli_server.url

    def run_cli(
        *args: str, check: bool = True, timeout_s: float = 30.0
    ) -> subprocess.CompletedProcess[str]:
        cmd = [binary] + list(args)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
        )
        if check and result.returncode != 0:
            raise AssertionError(
                f"CLI failed (exit {result.returncode}): {' '.join(cmd)}\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}\n"
                f"server logs:\n{cli_server.debug_logs()[-2000:]}"
            )
        return result

    return run_cli


def parse_json(output: str) -> dict | list:
    return json.loads(output.strip())


# ---------------------------------------------------------------------------
# Table lifecycle
# ---------------------------------------------------------------------------


def test_table_create_list_get_drop(cli):
    table = f"cli_test_{int(time.time() * 1000)}"

    # create
    cli("table", "create", "--table", table, "--shards", "1")

    # list defaults to a compact summary rather than dumping full metadata
    result = cli("table", "list")
    lines = result.stdout.strip().splitlines()
    assert lines[0] == "NAME\tSHARDS\tINDEXES\tSTORAGE"
    assert any(line.startswith(f"{table}\t1\t") for line in lines[1:])

    # detailed machine-readable output remains available explicitly
    result = cli("table", "list", "--output", "json")
    tables = parse_json(result.stdout)
    assert isinstance(tables, list)
    names = [t["name"] for t in tables]
    assert table in names

    # get
    result = cli("table", "get", "--table", table)
    info = parse_json(result.stdout)
    assert info["name"] == table

    # drop
    cli("table", "drop", "--table", table)

    # list again — should be gone (eventually)
    def table_gone() -> bool:
        r = cli("table", "list", "--output", "json")
        tbl_list = parse_json(r.stdout)
        return table not in [t["name"] for t in tbl_list]

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if table_gone():
            break
        time.sleep(0.25)
    else:
        pytest.fail(f"table {table} still present after drop")


def test_cli_inline_create_load_wait_query_image_and_rag_pipeline(
    cli, cli_server, tmp_path
):
    """Exercise the documented CLI path across parsing, readiness, and retrieval."""
    table = f"cli_quickstart_{time.time_ns()}"
    embedder_url = cli_server.cli_inference_urls["embedder"]
    generator_url = cli_server.cli_inference_urls["generator"]
    inline_index = json.dumps(
        {
            "name": "title_body",
            "type": "embeddings",
            "template": "{{title}} {{body}}",
            "dimension": 3,
            "embedder": {
                "provider": "antfly",
                "model": "antfly-embed-v1",
                "api_url": embedder_url,
            },
        }
    )
    tiny_png = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9ZlS8AAAAASUVORK5CYII="
    records = tmp_path / "quickstart.jsonl"
    records.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "id": "doc:alpha",
                        "title": "Alpha",
                        "body": "alpha concept overview",
                        "thumbnail_url": tiny_png,
                    }
                ),
                json.dumps(
                    {
                        "id": "doc:beta",
                        "title": "Beta",
                        "body": "beta unrelated notes",
                    }
                ),
            ]
        )
        + "\n"
    )

    try:
        cli(
            "table",
            "create",
            "--table",
            table,
            "--shards",
            "1",
            "--index",
            inline_index,
        )
        cli(
            "load",
            "--table",
            table,
            "--file",
            str(records),
            "--id-field",
            "id",
            "--no-checkpoint",
        )

        text_wait = cli(
            "index",
            "wait",
            "--table",
            table,
            "--index",
            "title_body",
            "--timeout",
            "20s",
            "--poll-interval",
            "25ms",
            timeout_s=30.0,
        )
        assert "title_body\tembeddings\tready" in text_wait.stdout

        text_query = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "alpha concept",
            "--indexes",
            "title_body",
            "--limit",
            "2",
        )
        assert "warning:" not in text_query.stderr.lower()
        text_hits = parse_json(text_query.stdout)["responses"][0]["hits"]["hits"]
        assert text_hits[0]["_id"] == "doc:alpha"

        cli(
            "index",
            "create",
            "--table",
            table,
            "--index",
            "thumbnail",
            "--type",
            "embeddings",
            "--coverage-policy",
            "partial",
            "--template",
            "{{#if thumbnail_url}}{{remoteMedia url=thumbnail_url}}{{/if}}",
            "--dimension",
            "3",
            "--embedder",
            json.dumps(
                {
                    "provider": "antfly",
                    "model": "antflydb/clipclap",
                    "api_url": embedder_url,
                }
            ),
        )
        image_wait = cli(
            "index",
            "wait",
            "--table",
            table,
            "--index",
            "thumbnail",
            "--timeout",
            "20s",
            "--poll-interval",
            "25ms",
            timeout_s=30.0,
            check=False,
        )
        assert image_wait.returncode == 0, (
            f"{image_wait.stderr}\nindex diagnostics:\n"
            f"{cli('index', 'list', '--table', table, '--output', 'json').stdout}"
        )
        assert "thumbnail\tembeddings\tready" in image_wait.stdout
        image_status = parse_json(
            cli("index", "get", "--table", table, "--index", "thumbnail").stdout
        )
        image_coverage = image_status["status"]["coverage"]
        assert image_coverage["policy"] == "partial"
        assert image_coverage["source_total"] == 2
        assert image_coverage["produced"] == 1
        assert image_coverage["skipped"] == 1
        assert image_coverage["terminal_failed"] == 0
        assert image_coverage["complete"] is True
        assert image_coverage["healthy"] is True

        image_query = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "map of a country",
            "--indexes",
            "thumbnail",
            "--limit",
            "2",
        )
        assert "warning:" not in image_query.stderr.lower()
        image_hits = parse_json(image_query.stdout)["responses"][0]["hits"]["hits"]
        assert image_hits[0]["_id"] == "doc:alpha"

        # Creating another derived index can briefly advance the table's
        # catalog generation before every shard has republished sibling
        # coverage. Re-establish the documented readiness precondition before
        # exercising retrieval so the advisory remains meaningful and the
        # pipeline is deterministic across runners.
        text_wait_after_index_create = cli(
            "index",
            "wait",
            "--table",
            table,
            "--index",
            "title_body",
            "--timeout",
            "20s",
            "--poll-interval",
            "25ms",
            timeout_s=30.0,
        )
        assert "title_body\tembeddings\tready" in text_wait_after_index_create.stdout

        rag = cli(
            "agents",
            "retrieval",
            "--table",
            table,
            "--semantic-search",
            "alpha concept",
            "--indexes",
            "title_body",
            "--prompt",
            "Summarize the alpha document",
            "--fields",
            "title,body",
            "--generator",
            json.dumps(
                {
                    "provider": "antfly",
                    "model": "local-generator",
                    "api_url": generator_url,
                    "api_key": "test-key",
                }
            ),
            "--generate",
            "--no-streaming",
            timeout_s=60.0,
        )
        assert "warning:" not in rag.stderr.lower()
        rag_result = parse_json(rag.stdout)
        assert rag_result["status"] == "completed"
        assert rag_result["generation"]
        assert rag_result["hits"][0]["_id"] == "doc:alpha"
    finally:
        cli("table", "drop", "--table", table, check=False)


# ---------------------------------------------------------------------------
# Insert + Lookup + Delete
# ---------------------------------------------------------------------------


def test_insert_lookup_delete(cli):
    table = f"cli_crud_{int(time.time() * 1000)}"

    cli("table", "create", "--table", table, "--shards", "1")

    # insert
    doc = json.dumps({"title": "Hello", "body": "world"})
    cli("insert", "--table", table, "--key", "doc1", "--document", doc)

    # lookup
    def lookup_succeeds() -> dict | None:
        r = cli("lookup", "--table", table, "--key", "doc1", check=False)
        if r.returncode != 0:
            return None
        try:
            data = parse_json(r.stdout)
        except json.JSONDecodeError:
            return None
        if not data:
            return None
        return data

    result = wait_until(lookup_succeeds, timeout_s=10.0, interval_s=0.25)
    assert result is not None
    assert result["title"] == "Hello"
    assert result["body"] == "world"

    # delete
    cli("delete", "--table", table, "--key", "doc1")

    # verify gone
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        r = cli("lookup", "--table", table, "--key", "doc1", check=False)
        if r.returncode != 0 or not r.stdout.strip():
            break
        time.sleep(0.25)

    # cleanup
    cli("table", "drop", "--table", table)


@pytest.mark.parametrize(
    "args",
    [
        (
            "insert",
            "--table",
            "docs",
            "--key",
            "doc:a",
            "--document",
            "{}",
            "--typo",
            "value",
        ),
        ("delete", "--table", "docs", "--key", "doc:a", "--typo", "value"),
        ("lookup", "--table", "docs", "--key", "doc:a", "--typo", "value"),
        ("artifact", "list", "--table", "docs", "--typo", "value"),
        (
            "agents",
            "query-builder",
            "--intent",
            "find documents",
            "--generator",
            '{"provider":"openai","model":"test"}',
            "--typo",
            "value",
        ),
    ],
)
def test_client_commands_reject_unknown_options_before_network_work(cli, args):
    result = cli(*args, check=False)
    assert result.returncode != 0
    assert "unknown" in result.stderr.lower()


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (
            ("load", "--table", "docs", "--file", "missing.jsonl", "--checkpoint"),
            "--checkpoint requires a value",
        ),
        (
            ("load", "--table", "docs", "-t", "other", "--file", "missing.jsonl"),
            "-t may only be provided once",
        ),
        (
            (
                "load",
                "--table",
                "docs",
                "--file",
                "missing.jsonl",
                "--checkpoint",
                "state",
                "--no-checkpoint",
            ),
            "--checkpoint cannot be used with --no-checkpoint",
        ),
    ],
)
def test_load_rejects_missing_duplicate_and_conflicting_options_before_io(
    cli, args, message
):
    result = cli(*args, check=False)
    assert result.returncode != 0
    assert message in result.stderr


def test_semantic_query_requires_table_before_network_work(cli):
    result = cli("query", "--semantic-search", "alpha", check=False)
    assert result.returncode != 0
    assert "--table is required" in result.stderr


def test_semantic_preflight_reports_missing_selected_index(cli):
    table = f"cli_missing_semantic_{time.time_ns()}"
    try:
        cli("table", "create", "--table", table, "--shards", "1")
        missing = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "alpha",
            "--indexes",
            "missing_dense",
            check=False,
        )
        assert missing.returncode != 0
        assert (
            f"selected semantic index missing_dense was not found on table {table}"
            in missing.stderr
        )

        wrong_type = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "alpha",
            "--indexes",
            "full_text_index_v0",
            check=False,
        )
        assert wrong_type.returncode != 0
        assert (
            "selected semantic index full_text_index_v0 is type full_text, not embeddings"
            in wrong_type.stderr
        )

        none_available = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "alpha",
            check=False,
        )
        assert none_available.returncode != 0
        assert (
            f"table {table} has no embeddings indexes available for semantic search"
            in none_available.stderr
        )
    finally:
        cli("table", "drop", "--table", table, check=False)


# ---------------------------------------------------------------------------
# Query (full-text search via CLI)
# ---------------------------------------------------------------------------


def test_query_full_text_search(cli):
    table = f"cli_query_{int(time.time() * 1000)}"

    cli("table", "create", "--table", table, "--shards", "1")

    for key, body in [
        ("alpha", json.dumps({"content": "alpha retrieval architecture"})),
        ("beta", json.dumps({"content": "beta unrelated noise"})),
    ]:
        cli("insert", "--table", table, "--key", key, "--value", body)

    def query_hits() -> dict | None:
        r = cli(
            "query",
            "--table",
            table,
            "--full-text-search",
            "content:alpha",
            "--limit",
            "5",
            check=False,
        )
        if r.returncode != 0:
            return None
        try:
            data = parse_json(r.stdout)
        except json.JSONDecodeError:
            return None
        responses = data.get("responses", [])
        if not responses:
            return None
        hits = responses[0].get("hits", {}).get("hits", [])
        if not hits:
            return None
        return data

    result = wait_until(query_hits, timeout_s=15.0, interval_s=0.5)
    assert result is not None
    responses = result["responses"]
    hits = responses[0]["hits"]["hits"]
    assert hits[0]["_id"] == "alpha"

    cli("table", "drop", "--table", table)


# ---------------------------------------------------------------------------
# Internal metadata status
# ---------------------------------------------------------------------------


def test_internal_metadata_status(cli):
    result = cli("internal", "metadata", "status")
    status = parse_json(result.stdout)
    assert isinstance(status, dict)
