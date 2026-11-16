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

"""Coordinator-side result transforms on distributed tables.

A one-shard table on a metadata + data cluster takes a single-group fast path.
Reranking and pruning must still apply there, exactly as they do when the
coordinator merges two shards.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
import requests
from conftest import DEFAULT_ANTFLY_BIN, StatefulAntflyServer, resolve_binary_path
from e2e_scheduler import e2e_resource
from helpers import wait_until
from port_reservations import find_free_port
from test_secrets import TableObjectStorage

QUERY = {"full_text_search": {"query": "text:Lovelace"}, "limit": 2, "fields": ["text"]}


@pytest.fixture(scope="module")
@e2e_resource("antfly_process")
def distributed_api_url():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists() or Path(binary).name != "antfly":
        pytest.skip(f"antfly binary not found: {binary}")
    # Distributed retained snapshots require shared S3/GCS artifact storage.
    objects = TableObjectStorage()
    try:
        server = StatefulAntflyServer(
            binary,
            "127.0.0.1",
            find_free_port(),
            data_config={
                "storage": {
                    "engine": "local",
                    "local": {},
                    "artifacts": {"connection": "snapshots", "bucket": "snapshots"},
                },
                "connections": {
                    "snapshots": {
                        "kind": "external_io",
                        "capabilities": ["storage.primary"],
                        "external_io": {
                            "protocol": "s3",
                            "endpoint": objects.endpoint,
                            "use_ssl": False,
                            "addressing_style": "path",
                            "region": "us-east-1",
                            "buckets": ["snapshots"],
                            "credentials": {
                                "source": "static",
                                "access_key_id": "fixture",
                                "secret_access_key": "fixture",
                            },
                        },
                    }
                },
            },
        )
        try:
            yield server.api_url
        finally:
            server.stop()
    finally:
        objects.close()


def _query(api_url: str, table: str, body: dict) -> dict:
    response = requests.post(f"{api_url}/tables/{table}/query", json=body, timeout=30)
    response.raise_for_status()
    return response.json()["responses"][0]


def _hits(api_url: str, table: str, body: dict) -> list[dict]:
    return _query(api_url, table, body)["hits"]["hits"]


def _table_with_two_hits(api_url: str, num_shards: int) -> str:
    table = f"finalize_{num_shards}_{time.time_ns()}"
    created = requests.post(
        f"{api_url}/tables/{table}", json={"num_shards": num_shards}, timeout=30
    )
    assert created.status_code == 200, created.text
    inserted = requests.post(
        f"{api_url}/tables/{table}/batch",
        json={
            "inserts": {
                "doc-alpha": {"text": "alpha Ada Lovelace"},
                "doc-beta": {"text": "beta Ada Lovelace"},
            }
        },
        timeout=30,
    )
    assert inserted.status_code == 201, inserted.text
    assert wait_until(lambda: len(_hits(api_url, table, QUERY)) == 2, timeout_s=30.0)
    return table


@pytest.mark.parametrize("num_shards", [1, 2])
def test_distributed_query_applies_reranker(
    distributed_api_url, inference_reranker, num_shards
):
    table = _table_with_two_hits(distributed_api_url, num_shards)
    hits = _hits(
        distributed_api_url,
        table,
        {
            **QUERY,
            "reranker": {
                "provider": "antfly",
                "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                "url": inference_reranker,
                "field": "text",
                "top_n": 2,
            },
        },
    )
    assert [hit["_id"] for hit in hits] == ["doc-beta", "doc-alpha"]
    assert hits[0]["_score"] == pytest.approx(0.95)
    assert hits[1]["_score"] == pytest.approx(0.6)


@pytest.mark.parametrize("num_shards", [1, 2])
def test_distributed_query_applies_pruner(distributed_api_url, num_shards):
    table = _table_with_two_hits(distributed_api_url, num_shards)
    hits = _hits(
        distributed_api_url, table, {**QUERY, "pruner": {"min_absolute_score": 1.0}}
    )
    assert hits == []


@pytest.mark.parametrize("num_shards", [1, 2])
def test_retained_query_finalizes_capture_replay_and_cursor_windows(
    distributed_api_url, inference_reranker, num_shards
):
    table = _table_with_two_hits(distributed_api_url, num_shards)
    # Stable retrieval order makes the cursor independent of provider scores.
    body = {**QUERY, "order_by": [{"field": "_id"}], "profile": True}
    initial = _query(distributed_api_url, table, body)
    assert initial["profile"]["shards"]["total"] == num_shards
    token = initial["remote_snapshot"]
    assert token.startswith("native2:")
    reranker = {
        "provider": "antfly",
        "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
        "url": inference_reranker,
        "field": "text",
        "top_n": 2,
    }
    for snapshot in ({}, {"remote_snapshot": token}):
        result = _query(
            distributed_api_url, table, {**body, **snapshot, "reranker": reranker}
        )
        assert result["profile"]["shards"]["total"] == num_shards
        reranked = result["hits"]["hits"]
        assert [hit["_id"] for hit in reranked] == ["doc-beta", "doc-alpha"]
        assert [hit["_score"] for hit in reranked] == pytest.approx([0.95, 0.6])
        assert (
            _hits(
                distributed_api_url,
                table,
                {**body, **snapshot, "pruner": {"min_absolute_score": 1.0}},
            )
            == []
        )

        reranked_and_pruned = _hits(
            distributed_api_url,
            table,
            {
                **body,
                **snapshot,
                "reranker": reranker,
                "pruner": {"min_absolute_score": 0.8},
            },
        )
        assert [hit["_id"] for hit in reranked_and_pruned] == ["doc-beta"]

    # Retrieval totals count the whole corpus, not just the remaining window.
    # The carrier's one-hit tail and empty tail must both finalize successfully.
    for cursor, expected in ((["doc-alpha"], ["doc-beta"]), (["doc-beta"], [])):
        retained = {
            **body,
            "remote_snapshot": token,
            "search_after": cursor,
            "reranker": reranker,
        }
        result = _query(distributed_api_url, table, retained)
        hits = result["hits"]["hits"]
        assert [hit["_id"] for hit in hits] == expected
        assert result["hits"]["total"]["value"] == 2
        if hits:
            assert hits[0]["_score"] == pytest.approx(0.95)
        assert (
            _hits(
                distributed_api_url,
                table,
                {**retained, "pruner": {"min_absolute_score": 1.0}},
            )
            == []
        )
