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
from helpers import wait_until
from port_reservations import find_free_port

QUERY = {"full_text_search": {"query": "text:Lovelace"}, "limit": 2, "fields": ["text"]}


@pytest.fixture(scope="module")
def distributed_api_url():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists() or Path(binary).name != "antfly":
        pytest.skip(f"antfly binary not found: {binary}")
    server = StatefulAntflyServer(binary, "127.0.0.1", find_free_port())
    try:
        yield server.api_url
    finally:
        server.stop()


def _hits(api_url: str, table: str, body: dict) -> list[dict]:
    response = requests.post(f"{api_url}/tables/{table}/query", json=body, timeout=30)
    response.raise_for_status()
    return response.json()["responses"][0]["hits"]["hits"]


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
