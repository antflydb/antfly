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

"""Stateful public API sparse and hybrid search tests."""

from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from helpers import wait_until


def _create_index(api, table_name: str, index_name: str, payload: dict) -> dict:
    return api.post(f"/tables/{table_name}/indexes/{index_name}", payload)


def _get_index(api, table_name: str, index_name: str) -> dict:
    return api.get(f"/tables/{table_name}/indexes/{index_name}")


def _index_stats(api, table_name: str, index_name: str) -> dict | None:
    try:
        return _get_index(api, table_name, index_name)["status"]
    except requests.HTTPError:
        return None


def _ready_index(api, table_name: str, index_name: str, *, expected_docs: int) -> dict | None:
    stats = _index_stats(api, table_name, index_name)
    if not stats:
        return None
    if stats.get("rebuilding", stats.get("backfill_active", False)):
        return None
    total_indexed = stats.get("total_indexed", stats.get("doc_count", 0))
    if total_indexed < expected_docs:
        return None
    return stats


def _retry_query_table(api, table_name: str, payload: dict):
    try:
        return api.query_table(table_name, payload)
    except requests.HTTPError:
        return None


def _settled_index(api, table_name: str, index_name: str) -> dict | None:
    stats = _index_stats(api, table_name, index_name)
    if not stats:
        return None
    if stats.get("rebuilding", stats.get("backfill_active", False)):
        return None
    return stats


def _top_hit_ids(result: dict) -> list[str]:
    responses = result.get("responses", [])
    if not responses:
        return []
    hits = responses[0].get("hits", {}).get("hits", [])
    return [hit["_id"] for hit in hits]


class _TextServer:
    def __init__(self, responses: dict[str, tuple[int, str, str]]):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                status, content_type, body = responses.get(
                    self.path,
                    (404, "text/plain", "missing"),
                )
                encoded = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, format: str, *args: object) -> None:
                _ = outer
                _ = format
                _ = args

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        host, port = self._server.server_address
        self.url = f"http://{host}:{port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def test_sparse_import_and_hybrid_query_with_external_embeddings(backup_api):
    table_name = f"sparse_hybrid_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    dense_index = "dense_idx"
    sparse_index = "sparse_idx"

    assert (
        _create_index(
            backup_api,
            table_name,
            dense_index,
            {
                "name": dense_index,
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )
    assert (
        _create_index(
            backup_api,
            table_name,
            sparse_index,
            {
                "name": sparse_index,
                "type": "embeddings",
                "external": True,
                "sparse": True,
            },
        )
        == {}
    )

    docs = {
        "doc1": {
            "title": "Database Indexing",
            "content": "B-tree indexes provide efficient ordered lookups for search systems.",
            "_embeddings": {
                dense_index: [0.92, 0.08, 0.0],
                sparse_index: {"10": 2.5, "20": 1.0, "30": 0.5},
            },
        },
        "doc2": {
            "title": "Garden Care",
            "content": "Water your garden early and mulch around plants to retain moisture.",
            "_embeddings": {
                dense_index: [0.0, 0.1, 0.99],
                sparse_index: {"90": 3.0, "91": 1.5},
            },
        },
        "doc3": {
            "title": "Vector Search Engines",
            "content": "Hybrid search combines vector similarity with traditional keyword matching.",
            "_embeddings": {
                dense_index: [0.85, 0.15, 0.0],
                sparse_index: {"10": 1.8, "20": 0.8, "60": 2.0},
            },
        },
        "doc4": {
            "title": "Search Engine Architecture",
            "content": "Search engines combine sparse lexical matching and dense semantic vectors.",
            "_embeddings": {
                dense_index: [1.0, 0.05, 0.0],
                sparse_index: {"10": 2.7, "20": 1.4, "30": 0.4},
            },
        },
    }

    batch = backup_api.batch_write(
        table_name,
        inserts=docs,
        sync_level="aknn",
    )
    assert batch["inserted"] == len(docs)

    sparse_query = wait_until(
        lambda: backup_api.query_table(
            table_name,
            {
                "embeddings": {
                    sparse_index: {
                        "indices": [10, 20, 30],
                        "values": [2.5, 1.0, 0.5],
                    }
                },
                "indexes": [sparse_index],
                "limit": 3,
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert sparse_query is not None
    sparse_top_ids = _top_hit_ids(sparse_query)
    assert sparse_top_ids
    assert sparse_top_ids[0] in {"doc1", "doc4"}
    assert len({"doc1", "doc3", "doc4"} & set(sparse_top_ids[:3])) >= 2

    dense_stats = wait_until(
        lambda: _settled_index(backup_api, table_name, dense_index),
        timeout_s=30.0,
        interval_s=0.5,
    )
    sparse_stats = wait_until(
        lambda: _ready_index(backup_api, table_name, sparse_index, expected_docs=len(docs)),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert dense_stats is not None
    assert sparse_stats is not None

    hybrid_query = wait_until(
        lambda: backup_api.query_table(
            table_name,
            {
                "full_text_search": {
                    "match": {
                        "field": "content",
                        "text": "search engines combine retrieval methods",
                    }
                },
                "embeddings": {
                    dense_index: [1.0, 0.0, 0.0],
                    sparse_index: {
                        "indices": [10, 20, 30],
                        "values": [2.5, 1.0, 0.5],
                    },
                },
                "indexes": [dense_index, sparse_index],
                "limit": 4,
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert hybrid_query is not None
    hybrid_top_ids = _top_hit_ids(hybrid_query)
    assert hybrid_top_ids
    assert len({"doc1", "doc3", "doc4"} & set(hybrid_top_ids[:3])) >= 2
    assert "doc2" not in hybrid_top_ids[:2]

    pruned_query = wait_until(
        lambda: backup_api.query_table(
            table_name,
            {
                "full_text_search": {
                    "match": {
                        "field": "content",
                        "text": "search engines combine retrieval methods",
                    }
                },
                "embeddings": {
                    dense_index: [1.0, 0.0, 0.0],
                    sparse_index: {
                        "indices": [10, 20, 30],
                        "values": [2.5, 1.0, 0.5],
                    },
                },
                "indexes": [dense_index, sparse_index],
                "merge_config": {
                    "strategy": "rsf",
                    "window_size": 10,
                },
                "pruner": {
                    "require_multi_index": True,
                    "min_score_ratio": 0.35,
                },
                "limit": 4,
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert pruned_query is not None
    pruned_top_ids = _top_hit_ids(pruned_query)
    assert pruned_top_ids
    assert len(pruned_top_ids) <= 3
    assert len({"doc1", "doc3", "doc4"} & set(pruned_top_ids)) >= 2
    assert "doc2" not in pruned_top_ids


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_named_embedding_queries_use_requested_indexes(table_api):
    table_name = f"named_embedding_indexes_{time.time_ns()}"
    created = table_api.create_table(table_name)
    assert created.get("name", table_name) == table_name

    assert (
        table_api.create_index(
            table_name,
            "semantic_a",
            {
                "name": "semantic_a",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )
    assert (
        table_api.create_index(
            table_name,
            "semantic_b",
            {
                "name": "semantic_b",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )
    assert (
        table_api.create_index(
            table_name,
            "sparse_a",
            {
                "name": "sparse_a",
                "type": "embeddings",
                "external": True,
                "sparse": True,
            },
        )
        == {}
    )
    assert (
        table_api.create_index(
            table_name,
            "sparse_b",
            {
                "name": "sparse_b",
                "type": "embeddings",
                "external": True,
                "sparse": True,
            },
        )
        == {}
    )

    batch = table_api.batch_write(
        table_name,
        inserts={
            "doc-1": {
                "text": "alpha",
                "_embeddings": {
                    "semantic_a": [1.0, 0.0, 0.0],
                    "semantic_b": [0.0, 1.0, 0.0],
                    "sparse_a": {"11": 1.5},
                    "sparse_b": {"22": 2.0},
                },
            },
            "doc-2": {
                "text": "beta",
                "_embeddings": {
                    "semantic_a": [0.0, 1.0, 0.0],
                    "semantic_b": [1.0, 0.0, 0.0],
                    "sparse_a": {"22": 2.0},
                    "sparse_b": {"11": 1.5},
                },
            },
        },
        sync_level="full_index" if table_api.backend == "stateful" else "write",
    )
    assert batch["inserted"] == 2
    table_api.publish_table(table_name)

    dense_b = wait_until(
        lambda: table_api.query_table(
            table_name,
            {
                "embeddings": {"semantic_b": [0.0, 1.0, 0.0]},
                "indexes": ["semantic_b"],
                "limit": 2,
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert dense_b is not None
    assert _top_hit_ids(dense_b)[0] == "doc-1"

    dense_a = wait_until(
        lambda: table_api.query_table(
            table_name,
            {
                "embeddings": {"semantic_a": [0.0, 1.0, 0.0]},
                "indexes": ["semantic_a"],
                "limit": 2,
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert dense_a is not None
    assert _top_hit_ids(dense_a)[0] == "doc-2"

    sparse_b = wait_until(
        lambda: table_api.query_table(
            table_name,
            {
                "embeddings": {
                    "sparse_b": {
                        "indices": [22],
                        "values": [2.0],
                    }
                },
                "indexes": ["sparse_b"],
                "limit": 2,
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert sparse_b is not None
    assert _top_hit_ids(sparse_b)[0] == "doc-1"

    sparse_a = wait_until(
        lambda: table_api.query_table(
            table_name,
            {
                "embeddings": {
                    "sparse_a": {
                        "indices": [22],
                        "values": [2.0],
                    }
                },
                "indexes": ["sparse_a"],
                "limit": 2,
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert sparse_a is not None
    assert _top_hit_ids(sparse_a)[0] == "doc-2"


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_semantic_query_embedding_template_supports_remote_text(table_api, openai_embedder):
    table_name = f"semantic_template_remote_text_{time.time_ns()}"
    created = table_api.create_table(table_name)
    assert created.get("name", table_name) == table_name

    index_name = "semantic_template_chunked_idx"

    text_server = _TextServer(
        {
            "/alpha.txt": (200, "text/plain", "alpha concept"),
            "/beta.txt": (200, "text/plain", "beta concept"),
        }
    )
    try:
        transcript_a = f"{text_server.url}/alpha.txt"
        transcript_b = f"{text_server.url}/beta.txt"

        assert (
            table_api.create_index(
                table_name,
                index_name,
                {
                    "name": index_name,
                    "type": "embeddings",
                    "template": "{{title}} {{remoteText url=transcript}}",
                    "dimension": 3,
                    "embedder": {
                        "provider": "openai",
                        "model": "text-embedding-3-small",
                        "url": openai_embedder,
                    },
                    "chunker": {
                        "provider": "antfly",
                        "model": "fixed-bert-tokenizer",
                        "text": {
                            "target_tokens": 4,
                            "overlap_tokens": 1,
                            "separator": " ",
                        },
                    },
                },
            )
            == {}
        )

        batch = table_api.batch_write(
            table_name,
            inserts={
                "doc:a": {
                    "title": "alpha",
                    "transcript": transcript_a,
                },
                "doc:b": {
                    "title": "beta",
                    "transcript": transcript_b,
                },
            },
            sync_level="full_index",
        )
        assert batch["inserted"] == 2
        table_api.publish_table(table_name)

        query_url = f"{text_server.url}/alpha.txt"
        result = wait_until(
            lambda: _retry_query_table(
                table_api,
                table_name,
                {
                    "semantic_search": query_url,
                    "embedding_template": "{{remoteText url=this}}",
                    "indexes": [index_name],
                    "limit": 2,
                },
            ),
            timeout_s=30.0,
            interval_s=0.5,
        )
        assert result is not None
        assert _top_hit_ids(result)[0] == "doc:a"
    finally:
        text_server.stop()


def test_managed_sparse_hybrid_query_with_termite_embeddings(backup_api, termite_embedder, termite_reranker):
    table_name = f"sparse_hybrid_managed_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    dense_index = "dense_idx"
    sparse_index = "sparse_idx"

    assert (
        _create_index(
            backup_api,
            table_name,
            dense_index,
            {
                "name": dense_index,
                "type": "embeddings",
                "field": "content",
                "dimension": 3,
                "embedder": {
                    "provider": "termite",
                    "model": "termite-embed-v1",
                    "api_url": termite_embedder,
                },
            },
        )
        == {}
    )
    assert (
        _create_index(
            backup_api,
            table_name,
            sparse_index,
            {
                "name": sparse_index,
                "type": "embeddings",
                "field": "content",
                "sparse": True,
                "embedder": {
                    "provider": "termite",
                    "model": "termite-sparse-v1",
                    "api_url": termite_embedder,
                },
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha retrieval",
                "content": "alpha body retrieval search",
            },
            "doc:b": {
                "title": "Beta retrieval",
                "content": "beta body retrieval search",
            },
            "doc:c": {
                "title": "Plain",
                "content": "plain unrelated text",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    dense_stats = wait_until(
        lambda: _ready_index(backup_api, table_name, dense_index, expected_docs=3),
        timeout_s=30.0,
        interval_s=0.5,
    )
    sparse_stats = wait_until(
        lambda: _ready_index(backup_api, table_name, sparse_index, expected_docs=3),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert dense_stats is not None
    assert sparse_stats is not None

    hybrid_query = wait_until(
        lambda: backup_api.query_table(
            table_name,
            {
                "full_text_search": {
                    "match": {
                        "field": "content",
                        "text": "retrieval search",
                    }
                },
                "semantic_search": "alpha concept",
                "embeddings": {
                    sparse_index: {
                        "indices": [7, 42],
                        "values": [1.5, 0.5],
                    }
                },
                "indexes": [dense_index, sparse_index],
                "merge_config": {
                    "strategy": "rsf",
                    "window_size": 10,
                },
                "pruner": {
                    "require_multi_index": True,
                },
                "reranker": {
                    "provider": "termite",
                    "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                    "url": termite_reranker,
                    "field": "content",
                    "top_n": 2,
                },
                "profile": True,
                "limit": 3,
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert hybrid_query is not None

    responses = hybrid_query["responses"]
    hits = responses[0]["hits"]["hits"]
    assert [hit["_id"] for hit in hits][:2] == ["doc:b", "doc:a"]

    profile = responses[0]["profile"]
    assert profile["merge"]["strategy"] == "rsf"
    assert profile["reranker"]["documents_reranked"] == 2
    assert profile["reranker"]["model"] == "cross-encoder/ms-marco-MiniLM-L-6-v2"


def test_sparse_hybrid_query_supports_reranker_and_pruner(backup_api, termite_reranker):
    table_name = f"sparse_hybrid_rerank_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    sparse_index = "sparse_idx"

    assert (
        _create_index(
            backup_api,
            table_name,
            sparse_index,
            {
                "name": sparse_index,
                "type": "embeddings",
                "external": True,
                "sparse": True,
            },
        )
        == {}
    )

    docs = {
            "doc:a": {
                "title": "Alpha",
                "content": "alpha body",
                "_embeddings": {
                    sparse_index: {"7": 1.5, "42": 0.5},
                },
            },
            "doc:b": {
                "title": "Beta",
                "content": "beta body",
                "_embeddings": {
                    sparse_index: {"7": 1.4, "42": 0.4},
                },
            },
            "doc:c": {
                "title": "Plain",
                "content": "body body",
                "_embeddings": {
                    sparse_index: {"99": 2.0},
                },
            },
        }

    batch = backup_api.batch_write(table_name, inserts=docs, sync_level="full_index")
    assert batch["inserted"] == len(docs)

    sparse_stats = wait_until(
        lambda: _ready_index(backup_api, table_name, sparse_index, expected_docs=len(docs)),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert sparse_stats is not None

    query_payload = {
        "full_text_search": {
            "match": {
                "field": "content",
                "text": "body",
            }
        },
        "embeddings": {
            sparse_index: {
                "indices": [7, 42],
                "values": [1.5, 0.5],
            },
        },
        "indexes": [sparse_index],
        "merge_config": {
            "strategy": "rsf",
            "window_size": 10,
        },
        "pruner": {
            "require_multi_index": True,
        },
        "reranker": {
            "provider": "termite",
            "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
            "url": termite_reranker,
            "field": "content",
            "top_n": 2,
        },
        "profile": True,
        "limit": 2,
    }

    reranked_query = wait_until(
            lambda: (
                response
                if (
                    (response := backup_api.query_table(table_name, query_payload))
                    and _top_hit_ids(response) == ["doc:b", "doc:a"]
                    and response["responses"][0]["profile"]["reranker"]["documents_reranked"] == 2
                )
                else None
            ),
            timeout_s=30.0,
            interval_s=0.5,
        )
    assert reranked_query is not None

    responses = reranked_query["responses"]
    assert responses
    hits = responses[0]["hits"]["hits"]
    assert [hit["_id"] for hit in hits] == ["doc:b", "doc:a"]

    profile = responses[0]["profile"]
    assert profile["reranker"]["model"] == "cross-encoder/ms-marco-MiniLM-L-6-v2"
    assert profile["reranker"]["documents_reranked"] == 2


def test_sparse_hybrid_query_rejects_invalid_reranker_provider(backup_api):
    table_name = f"sparse_hybrid_invalid_reranker_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    sparse_index = "sparse_idx"

    assert (
        _create_index(
            backup_api,
            table_name,
            sparse_index,
            {
                "name": sparse_index,
                "type": "embeddings",
                "external": True,
                "sparse": True,
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha",
                "content": "alpha body",
                "_embeddings": {
                    sparse_index: {"7": 1.5, "42": 0.5},
                },
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 1

    sparse_stats = wait_until(
        lambda: _ready_index(backup_api, table_name, sparse_index, expected_docs=1),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert sparse_stats is not None

    try:
        backup_api.query_table(
            table_name,
            {
                "full_text_search": {
                    "match": {
                        "field": "content",
                        "text": "body",
                    }
                },
                "embeddings": {
                    sparse_index: {
                        "indices": [7, 42],
                        "values": [1.5, 0.5],
                    },
                },
                "indexes": [sparse_index],
                "reranker": {
                    "provider": "bogus",
                    "model": "bad-model",
                    "field": "content",
                    "top_n": 2,
                },
                "limit": 3,
            },
        )
    except requests.HTTPError as exc:
        assert exc.response is not None
        assert exc.response.status_code == 400
    else:
        raise AssertionError("expected invalid reranker provider to fail")


def test_sparse_hybrid_query_rejects_invalid_reranker_config(backup_api, termite_reranker):
    table_name = f"sparse_hybrid_invalid_reranker_cfg_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    sparse_index = "sparse_idx"

    assert (
        _create_index(
            backup_api,
            table_name,
            sparse_index,
            {
                "name": sparse_index,
                "type": "embeddings",
                "external": True,
                "sparse": True,
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha",
                "content": "alpha body",
                "_embeddings": {
                    sparse_index: {"7": 1.5, "42": 0.5},
                },
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 1

    sparse_stats = wait_until(
        lambda: _ready_index(backup_api, table_name, sparse_index, expected_docs=1),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert sparse_stats is not None

    try:
        backup_api.query_table(
            table_name,
            {
                "full_text_search": {
                    "match": {
                        "field": "content",
                        "text": "body",
                    }
                },
                "embeddings": {
                    sparse_index: {
                        "indices": [7, 42],
                        "values": [1.5, 0.5],
                    },
                },
                "indexes": [sparse_index],
                "reranker": {
                    "provider": "termite",
                    "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                    "url": termite_reranker,
                    "field": "content",
                    "top_n": 0,
                },
                "limit": 3,
            },
        )
    except requests.HTTPError as exc:
        assert exc.response is not None
        assert exc.response.status_code == 400
    else:
        raise AssertionError("expected invalid reranker config to fail")
