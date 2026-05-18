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

These tests start an antfly swarm server, then exercise the CLI binary
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
import requests

from conftest import (
    DEFAULT_ANTFLY_BIN,
    SwarmAntflyServer,
    find_free_port,
    resolve_binary_path,
    wait_for_server,
)
from helpers import wait_until


@pytest.fixture(scope="module")
def cli_server():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")

    port = find_free_port()
    server = SwarmAntflyServer(binary, "127.0.0.1", port)
    yield server
    server.stop()


@pytest.fixture(scope="module")
def cli(cli_server):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    env = os.environ.copy()
    env["ANTFLY_URL"] = cli_server.url

    def run_cli(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        cmd = [binary] + list(args)
        result = subprocess.run(
            cmd,
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

    # list — should contain the new table
    result = cli("table", "list")
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
        r = cli("table", "list")
        tbl_list = parse_json(r.stdout)
        return table not in [t["name"] for t in tbl_list]

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if table_gone():
            break
        time.sleep(0.25)
    else:
        pytest.fail(f"table {table} still present after drop")


# ---------------------------------------------------------------------------
# Insert + Lookup + Delete
# ---------------------------------------------------------------------------


def test_insert_lookup_delete(cli):
    table = f"cli_crud_{int(time.time() * 1000)}"

    cli("table", "create", "--table", table, "--shards", "1")

    # insert
    doc = json.dumps({"title": "Hello", "body": "world"})
    cli("insert", "--table", table, "--key", "doc1", "--value", doc)

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
            "--table", table,
            "--full-text-search", "content:alpha",
            "--limit", "5",
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
