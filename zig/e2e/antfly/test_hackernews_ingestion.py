# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Independent HN writer snapshots through native SQL/text and restart."""

import os
from pathlib import Path
import sys
import time

import pytest
import requests
from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    resolve_binary_path,
)
from test_iceberg_sql import _export_table

pytestmark = [pytest.mark.iceberg_integration, pytest.mark.fresh_antfly_process]


def test_hackernews_streaming_snapshots_roots_deletions_and_restart(tmp_path):
    from pyiceberg.catalog.sql import SqlCatalog

    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "examples/hackernews"))
    try:
        import ingest
    finally:
        sys.path.pop(0)
    directory = tmp_path / "state"
    directory.mkdir()
    warehouse = (tmp_path / "warehouse").as_uri()
    state = ingest.State(directory / "items.sqlite")
    with state.db:
        state.put(
            {"id": 1, "type": "story", "time": 1704067200, "title": "database story"}
        )
        state.put(
            {
                "id": 2,
                "type": "comment",
                "parent": 1,
                "time": 1704067200,
                "text": "database comment",
            }
        )
        state.resolve_roots()
    ingest.publish(state, directory, warehouse)
    catalog = SqlCatalog(
        "hn", uri=f"sqlite:///{directory / 'catalog.sqlite'}", warehouse=warehouse
    )
    source = tmp_path / "objects"
    _export_table(catalog.load_table("hackernews.items"), source)
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:

        def request(method, path, body=None):
            return requests.request(
                method,
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )

        def call(method, path, body=None):
            response = request(method, path, body)
            assert response.ok, response.text + server.debug_logs()
            value = response.json()
            return value["responses"][0] if "responses" in value else value

        call(
            "POST",
            "/tables/hn",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "hn",
                        "format": "iceberg",
                        "uri": source.as_uri(),
                    },
                },
                "indexes": {"body_text": {"type": "full_text", "field": "body"}},
            },
        )
        deadline = time.monotonic() + 90
        while not call("GET", "/tables/hn/indexes/body_text")["status"]["readiness"][
            "queryable"
        ]:
            assert time.monotonic() < deadline, server.debug_logs()
            time.sleep(0.1)
        query = {
            "full_text_search": {"match": "database", "field": "body"},
            "full_text_index": "body_text",
            "fields": ["hn_id", "root_story_id"],
            "limit": 10,
        }

        def rows(result):
            if "responses" in result:
                result = result["responses"][0]
            return sorted(
                (hit["_source"]["hn_id"], hit["_source"]["root_story_id"])
                for hit in result["hits"]["hits"]
            )

        assert rows(call("POST", "/tables/hn/query", query)) == [(1, 1), (2, 1)]
        with state.db:
            state.put({"id": 2, "deleted": True})
            state.put(
                {
                    "id": 3,
                    "type": "comment",
                    "parent": 1,
                    "time": 1704067200,
                    "text": "database replacement",
                }
            )
            state.resolve_roots()
        ingest.publish(state, directory, warehouse)
        _export_table(catalog.load_table("hackernews.items"), source)
        count = call(
            "POST",
            "/sql",
            {"statement": "SELECT hn_id,root_story_id FROM hn ORDER BY hn_id"},
        )
        assert count["rows"] == [["1", "1"], ["3", "1"]]
        server.restart()
        deadline = time.monotonic() + 90
        while True:
            response = request("POST", "/tables/hn/query", query)
            if response.ok:
                assert rows(response.json()) == [(1, 1), (3, 1)], response.text
                break
            assert time.monotonic() < deadline, response.text + server.debug_logs()
            assert response.status_code in (409, 422, 503), response.text
            time.sleep(0.2)
        failed = False
    finally:
        server.stop(test_failed=failed)
        state.db.close()
