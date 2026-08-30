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

"""Portable quickstart-style E2E tests for antfly-zig."""

import json

import pytest
import requests
from helpers import (
    assert_created_index,
    assert_single_top_hit,
    json_doc,
    upsert,
    wait_until,
)

pytestmark = pytest.mark.reuse_antfly_process


CLIPCLAP_MODEL = "antflydb/clipclap"


def test_text_quickstart_and_document_artifact(serverless_api):
    def _public_hit_ids(payload: dict) -> list[str]:
        responses = payload.get("responses", [])
        assert responses
        hits = responses[0]["hits"]["hits"]
        return [hit["_id"] for hit in hits]

    def published_query() -> dict | None:
        try:
            query = serverless_api.query_published("wikipedia")
        except requests.HTTPError:
            return None
        if query["document_count"] != 3:
            return None
        return query

    def search_results() -> dict | None:
        try:
            search = serverless_api.search_table(
                "wikipedia",
                {
                    "text": "relativity",
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        if not search.get("hits"):
            return None
        return search

    def public_search_results() -> dict | None:
        try:
            search = serverless_api.query_table(
                "wikipedia",
                {
                    "full_text_search": {"query": "body:relativity"},
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        responses = search.get("responses", [])
        if not responses or not responses[0]["hits"]["hits"]:
            return None
        return search

    def direct_match_search_results() -> dict | None:
        try:
            search = serverless_api.search_table(
                "wikipedia",
                {
                    "full_text_search": {
                        "match": {
                            "field": "body",
                            "text": "relativity",
                        }
                    },
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        if not search["hits"]:
            return None
        return search

    def direct_prefix_search_results() -> dict | None:
        try:
            search = serverless_api.search_table(
                "wikipedia",
                {
                    "full_text_search": {
                        "prefix": {
                            "field": "body",
                            "text": "rel",
                        }
                    },
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        if not search["hits"]:
            return None
        return search

    def filtered_public_search_results() -> dict | None:
        try:
            search = serverless_api.search_table(
                "wikipedia",
                {
                    "full_text_search": {"query": "body:relativity OR body:rome"},
                    "filter_query": {"query": "body:relativity OR body:rome"},
                    "exclusion_query": {"query": "body:rome"},
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        if not search["hits"]:
            return None
        return search

    def prefix_filtered_search_results() -> dict | None:
        try:
            search = serverless_api.search_table(
                "wikipedia",
                {
                    "text": "relativity",
                    "filter_prefix": "theory-",
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        if not search["hits"]:
            return None
        return search

    status = serverless_api.status()
    assert status["role"] == "combined"
    assert status["combined_mode"] is True
    assert status["validated"] is True

    serverless_api.ensure_table("wikipedia", created_at_ns=100)
    serverless_api.ingest_table(
        "wikipedia",
        timestamp_ns=123,
        mutations=[
            upsert(
                "theory-relativity",
                "relativity",
            ),
            upsert(
                "ancient-rome",
                "rome",
            ),
            upsert(
                "machine-learning",
                "learning",
            ),
        ],
    )
    try:
        serverless_api.build_table("wikipedia")
    except requests.HTTPError as exc:
        assert exc.response is not None
        assert exc.response.status_code == 409

    query = wait_until(published_query, timeout_s=10.0, interval_s=0.1)
    assert query is not None
    assert query["table_name"] == "wikipedia"
    assert query["document_count"] == 3

    search = wait_until(search_results, timeout_s=10.0, interval_s=0.1)
    assert search is not None
    assert_single_top_hit(search, "theory-relativity")

    public_search = wait_until(public_search_results, timeout_s=10.0, interval_s=0.1)
    assert public_search is not None
    assert _public_hit_ids(public_search)[0] == "theory-relativity"

    direct_match_search = wait_until(
        direct_match_search_results, timeout_s=10.0, interval_s=0.1
    )
    assert direct_match_search is not None
    assert_single_top_hit(direct_match_search, "theory-relativity")

    direct_prefix_search = wait_until(
        direct_prefix_search_results, timeout_s=10.0, interval_s=0.1
    )
    assert direct_prefix_search is not None
    assert_single_top_hit(direct_prefix_search, "theory-relativity")

    filtered_public_search = wait_until(
        filtered_public_search_results, timeout_s=10.0, interval_s=0.1
    )
    assert filtered_public_search is not None
    assert [hit["doc_id"] for hit in filtered_public_search["hits"]] == [
        "theory-relativity"
    ]

    prefix_filtered_search = wait_until(
        prefix_filtered_search_results, timeout_s=10.0, interval_s=0.1
    )
    assert prefix_filtered_search is not None
    assert [hit["doc_id"] for hit in prefix_filtered_search["hits"]] == [
        "theory-relativity"
    ]

    artifact = serverless_api.query_head_artifact("wikipedia", 1)
    assert artifact["artifact"]["kind"] == "document_segment"
    assert len(artifact["artifact"]["mutations"]) == 0
    assert len(artifact["artifact"]["documents"]) == 3
    assert artifact["artifact"]["documents"][0]["doc_id"] in {
        "ancient-rome",
        "machine-learning",
        "theory-relativity",
    }


def test_public_search_fields_projection(serverless_api):
    def projected_search() -> dict | None:
        try:
            search = serverless_api.search_table(
                "articles",
                {
                    "full_text_search": {"query": "body:alpha"},
                    "fields": ["title", "metadata.author"],
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        if not search["hits"]:
            return None
        return search

    serverless_api.ensure_table("articles", created_at_ns=100)
    serverless_api.ingest_table(
        "articles",
        timestamp_ns=123,
        mutations=[
            upsert(
                "doc-a",
                json_doc(
                    title="Alpha",
                    body="alpha",
                    metadata={"author": "Ada", "topic": "math"},
                    ignored="value",
                ),
            ),
        ],
    )
    try:
        serverless_api.build_table("articles")
    except requests.HTTPError as exc:
        assert exc.response is not None
        assert exc.response.status_code == 409

    search = wait_until(projected_search, timeout_s=10.0, interval_s=0.1)
    assert search is not None
    assert_single_top_hit(search, "doc-a")
    projected = json.loads(search["hits"][0]["body"])
    assert projected == {"metadata": {"author": "Ada"}, "title": "Alpha"}


def test_public_hybrid_quickstart_pipeline(backup_api, inference_reranker):
    table_name = f"quickstart_hybrid_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        backup_api.post(
            f"/tables/{table_name}/indexes/dense_idx",
            {
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        ),
        "dense_idx",
        "embeddings",
    )
    assert_created_index(
        backup_api.post(
            f"/tables/{table_name}/indexes/sparse_idx",
            {
                "type": "embeddings",
                "external": True,
                "sparse": True,
            },
        ),
        "sparse_idx",
        "embeddings",
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha",
                "body": "alpha retrieval architecture overview",
                "_embeddings": {
                    "dense_idx": [0.9, 0.1, 0.0],
                    "sparse_idx": {"7": 1.5, "42": 0.5},
                },
            },
            "doc:b": {
                "title": "Beta",
                "body": "beta retrieval architecture quickstart",
                "_embeddings": {
                    "dense_idx": [0.8, 0.2, 0.0],
                    "sparse_idx": {"7": 1.4, "42": 0.4},
                },
            },
            "doc:c": {
                "title": "Plain",
                "body": "plain body unrelated",
                "_embeddings": {
                    "dense_idx": [0.0, 0.0, 1.0],
                    "sparse_idx": {"99": 2.0},
                },
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    def hybrid_query() -> dict | None:
        try:
            result = backup_api.query_table(
                table_name,
                {
                    "full_text_search": {
                        "match": {
                            "field": "body",
                            "text": "retrieval architecture",
                        }
                    },
                    "embeddings": {
                        "dense_idx": [1.0, 0.0, 0.0],
                        "sparse_idx": {
                            "indices": [7, 42],
                            "values": [1.5, 0.5],
                        },
                    },
                    "indexes": ["dense_idx", "sparse_idx"],
                    "merge_config": {
                        "strategy": "rsf",
                        "window_size": 10,
                    },
                    "pruner": {
                        "require_multi_index": True,
                    },
                    "reranker": {
                        "provider": "antfly",
                        "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                        "url": inference_reranker,
                        "field": "body",
                        "top_n": 2,
                    },
                    "profile": True,
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        responses = result.get("responses", [])
        if not responses:
            return None
        hits = responses[0]["hits"]["hits"]
        if not hits:
            return None
        return result

    result = wait_until(hybrid_query, timeout_s=30.0, interval_s=0.5)
    assert result is not None
    responses = result["responses"]
    hits = responses[0]["hits"]["hits"]
    assert [hit["_id"] for hit in hits][:2] == ["doc:b", "doc:a"]

    profile = responses[0]["profile"]
    assert profile["reranker"]["documents_reranked"] == 2
    assert profile["reranker"]["model"] == "cross-encoder/ms-marco-MiniLM-L-6-v2"

    def hybrid_query_rrf() -> dict | None:
        try:
            result = backup_api.query_table(
                table_name,
                {
                    "full_text_search": {
                        "match": {
                            "field": "body",
                            "text": "retrieval architecture",
                        }
                    },
                    "embeddings": {
                        "dense_idx": [1.0, 0.0, 0.0],
                        "sparse_idx": {
                            "indices": [7, 42],
                            "values": [1.5, 0.5],
                        },
                    },
                    "indexes": ["dense_idx", "sparse_idx"],
                    "merge_config": {
                        "strategy": "rrf",
                        "rank_constant": 20,
                        "window_size": 10,
                    },
                    "pruner": {
                        "require_multi_index": True,
                        "min_score_ratio": 0.2,
                    },
                    "reranker": {
                        "provider": "antfly",
                        "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                        "url": inference_reranker,
                        "field": "body",
                        "top_n": 2,
                    },
                    "profile": True,
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        responses = result.get("responses", [])
        if not responses:
            return None
        hits = responses[0]["hits"]["hits"]
        if not hits:
            return None
        return result

    rrf_result = wait_until(hybrid_query_rrf, timeout_s=30.0, interval_s=0.5)
    assert rrf_result is not None
    rrf_responses = rrf_result["responses"]
    rrf_hits = rrf_responses[0]["hits"]["hits"]
    assert [hit["_id"] for hit in rrf_hits][:2] == ["doc:b", "doc:a"]
    rrf_profile = rrf_responses[0]["profile"]
    assert rrf_profile["reranker"]["documents_reranked"] == 2
    assert rrf_profile["reranker"]["model"] == "cross-encoder/ms-marco-MiniLM-L-6-v2"


def test_public_hybrid_quickstart_supports_weighted_merge_and_template_reranking(
    backup_api, inference_reranker
):
    table_name = f"quickstart_hybrid_template_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        backup_api.post(
            f"/tables/{table_name}/indexes/dense_idx",
            {
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        ),
        "dense_idx",
        "embeddings",
    )
    assert_created_index(
        backup_api.post(
            f"/tables/{table_name}/indexes/sparse_idx",
            {
                "type": "embeddings",
                "external": True,
                "sparse": True,
            },
        ),
        "sparse_idx",
        "embeddings",
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha Architecture",
                "body": "retrieval architecture overview",
                "_embeddings": {
                    "dense_idx": [1.0, 0.0, 0.0],
                    "sparse_idx": {"7": 1.5, "42": 0.5},
                },
            },
            "doc:b": {
                "title": "Beta Architecture",
                "body": "retrieval architecture overview",
                "_embeddings": {
                    "dense_idx": [0.9, 0.1, 0.0],
                    "sparse_idx": {"7": 1.4, "42": 0.4},
                },
            },
            "doc:c": {
                "title": "Plain",
                "body": "plain body unrelated",
                "_embeddings": {
                    "dense_idx": [0.0, 0.0, 1.0],
                    "sparse_idx": {"99": 2.0},
                },
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    def weighted_query() -> dict | None:
        try:
            result = backup_api.query_table(
                table_name,
                {
                    "full_text_search": {
                        "match": {
                            "field": "body",
                            "text": "retrieval architecture",
                        }
                    },
                    "embeddings": {
                        "dense_idx": [1.0, 0.0, 0.0],
                        "sparse_idx": {
                            "indices": [7, 42],
                            "values": [1.5, 0.5],
                        },
                    },
                    "indexes": ["dense_idx", "sparse_idx"],
                    "merge_config": {
                        "strategy": "rsf",
                        "window_size": 10,
                        "weights": {
                            "full_text": 0.2,
                            "dense_idx": 2.0,
                            "sparse_idx": 0.8,
                        },
                    },
                    "reranker": {
                        "provider": "antfly",
                        "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                        "url": inference_reranker,
                        "template": "title={{title}}\nbody={{body}}",
                        "top_n": 2,
                    },
                    "profile": True,
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        responses = result.get("responses", [])
        if not responses:
            return None
        hits = responses[0]["hits"]["hits"]
        if len(hits) < 2:
            return None
        return result

    result = wait_until(weighted_query, timeout_s=30.0, interval_s=0.5)
    assert result is not None
    responses = result["responses"]
    hits = responses[0]["hits"]["hits"]
    assert [hit["_id"] for hit in hits][:2] == ["doc:b", "doc:a"]

    profile = responses[0]["profile"]
    assert profile["merge"]["strategy"] == "rsf"
    assert profile["reranker"]["documents_reranked"] == 2
    assert profile["reranker"]["model"] == "cross-encoder/ms-marco-MiniLM-L-6-v2"


def test_public_managed_semantic_hybrid_quickstart_pipeline(
    backup_api, openai_embedder, inference_reranker
):
    table_name = f"quickstart_managed_hybrid_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        backup_api.create_index(
            table_name,
            "semantic_idx",
            {
                "name": "semantic_idx",
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": openai_embedder,
                },
            },
        ),
        "semantic_idx",
        "embeddings",
    )

    ready = wait_until(
        lambda: (
            status
            if (
                (
                    status := backup_api.get_index(table_name, "semantic_idx").get(
                        "status"
                    )
                )
                and not status.get("rebuilding", status.get("backfill_active", False))
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert ready is not None

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha",
                "body": "alpha concept overview",
            },
            "doc:b": {
                "title": "Beta",
                "body": "beta architecture quickstart",
            },
            "doc:c": {
                "title": "Plain",
                "body": "plain body unrelated",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    def hybrid_query() -> dict | None:
        try:
            result = backup_api.query_table(
                table_name,
                {
                    "full_text_search": {
                        "match": {
                            "field": "body",
                            "text": "quickstart",
                        }
                    },
                    "semantic_search": "alpha concept",
                    "indexes": ["semantic_idx"],
                    "merge_config": {
                        "strategy": "rsf",
                        "window_size": 10,
                        "weights": {
                            "full_text": 0.4,
                            "semantic_idx": 1.0,
                        },
                    },
                    "reranker": {
                        "provider": "antfly",
                        "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                        "url": inference_reranker,
                        "field": "body",
                        "top_n": 2,
                    },
                    "profile": True,
                    "limit": 3,
                },
            )
        except requests.HTTPError:
            return None
        responses = result.get("responses", [])
        if not responses:
            return None
        hits = responses[0]["hits"]["hits"]
        if len(hits) < 2:
            return None
        return result

    result = wait_until(hybrid_query, timeout_s=30.0, interval_s=0.5)
    assert result is not None
    responses = result["responses"]
    hits = responses[0]["hits"]["hits"]
    assert [hit["_id"] for hit in hits][:2] == ["doc:b", "doc:a"]

    profile = responses[0]["profile"]
    assert profile["merge"]["strategy"] == "rsf"
    assert profile["reranker"]["documents_reranked"] == 2
    assert profile["reranker"]["model"] == "cross-encoder/ms-marco-MiniLM-L-6-v2"


def test_public_managed_semantic_full_index_pipeline(backup_api, openai_embedder):
    table_name = f"quickstart_semantic_{__import__('time').time_ns()}"
    index_name = "semantic_idx"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        backup_api.create_index(
            table_name,
            index_name,
            {
                "name": index_name,
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": openai_embedder,
                },
            },
        ),
        index_name,
        "embeddings",
    )

    backup_api.wait_index_ready(table_name, index_name, timeout_s=30.0, interval_s=0.5)

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha",
                "body": "alpha concept overview",
            },
            "doc:b": {
                "title": "Beta",
                "body": "beta quickstart notes",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    backup_api.wait_index_ready(
        table_name,
        index_name,
        timeout_s=30.0,
        interval_s=0.25,
        until="complete",
    )
    index = backup_api.get_index(table_name, index_name)
    assert index["config"]["name"] == index_name
    status = index["status"]
    assert status["backfill_state"] == "ready"
    assert status["rebuilding"] is False
    assert status["coverage"]["observation_complete"] is True
    assert status["coverage"]["config_mismatch_group_count"] == 0
    assert status["enrichment_runtime"]["enabled"] is True
    assert status["enrichment_runtime"]["worker_started"] is True

    result = backup_api.query_table(
        table_name,
        {
            "semantic_search": "alpha concept",
            "indexes": [index_name],
            "limit": 5,
        },
    )
    responses = result["responses"]
    hits = responses[0]["hits"]["hits"]
    assert hits[0]["_id"] == "doc:a"


def test_inline_managed_index_create_load_ready_query_pipeline(
    backup_api, openai_embedder
):
    """Cover the published create-table path through query-visible readiness."""
    table_name = f"quickstart_inline_semantic_{__import__('time').time_ns()}"
    index_name = "title_body"
    created = backup_api.create_table(
        table_name,
        num_shards=1,
        indexes={
            index_name: {
                "name": index_name,
                "type": "embeddings",
                "template": "{{title}} {{body}}",
                "dimension": 3,
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": openai_embedder,
                },
            }
        },
    )
    assert created["name"] == table_name
    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "Alpha", "body": "alpha concept overview"},
            "doc:b": {"title": "Beta", "body": "beta unrelated notes"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    backup_api.wait_index_ready(
        table_name,
        index_name,
        timeout_s=30.0,
        interval_s=0.25,
        until="complete",
    )
    index = backup_api.get_index(table_name, index_name)
    assert index["config"]["name"] == index_name
    status = index["status"]
    assert status["backfill_state"] == "ready"
    assert status["rebuilding"] is False
    assert status["coverage"]["observation_complete"] is True
    assert status["coverage"]["config_mismatch_group_count"] == 0
    assert status["enrichment_runtime"]["enabled"] is True
    assert status["enrichment_runtime"]["worker_started"] is True

    result = backup_api.query_table(
        table_name,
        {
            "semantic_search": "alpha concept",
            "indexes": [index_name],
            "limit": 2,
        },
    )
    hits = result["responses"][0]["hits"]["hits"]
    assert hits
    assert hits[0]["_id"] == "doc:a"


def test_public_managed_chunked_semantic_full_index_pipeline(
    backup_api, openai_embedder
):
    table_name = f"quickstart_chunked_semantic_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        backup_api.create_index(
            table_name,
            "semantic_chunked_idx",
            {
                "name": "semantic_chunked_idx",
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": openai_embedder,
                },
                "chunker": {
                    "provider": "antfly",
                    "model": "fixed-bert-tokenizer",
                    "store_chunks": True,
                    "text": {
                        "target_tokens": 4,
                        "overlap_tokens": 1,
                        "separator": " ",
                    },
                },
            },
        ),
        "semantic_chunked_idx",
        "embeddings",
    )

    backup_api.wait_index_ready(
        table_name, "semantic_chunked_idx", timeout_s=30.0, interval_s=0.5
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha in chunks",
                "body": "alpha alpha alpha alpha beta beta beta beta beta beta",
            },
            "doc:b": {
                "title": "Beta only",
                "body": "beta beta beta beta beta beta beta beta",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = backup_api.query_table(
        table_name,
        {
            "semantic_search": "alpha concept",
            "indexes": ["semantic_chunked_idx"],
            "limit": 5,
        },
    )
    responses = result["responses"]
    hits = responses[0]["hits"]["hits"]
    assert hits[0]["_id"] == "doc:a"

    scan = backup_api.scan_keys(
        table_name,
        {
            "from": "doc:a",
            "to": "doc:a;",
            "inclusive_from": True,
            "fields": ["title", "_chunks"],
        },
    )
    assert len(scan) == 1
    assert scan[0]["title"] == "Alpha in chunks"
    chunks = scan[0]["_chunks"]["semantic_chunked_idx_chunks"]
    assert len(chunks) >= 2
    assert chunks[0]["body"].startswith("alpha")
    assert any(chunk["body"].startswith("beta") for chunk in chunks)


def test_progressive_index_is_semantically_queryable_before_full_coverage(
    single_item_enrichment_batches,
    backup_api,
    progressive_openai_embedder,
):
    """Time-to-first-result gate, separate from complete-generation readiness."""
    _ = single_item_enrichment_batches
    table_name = f"quickstart_progressive_{__import__('time').time_ns()}"
    index_name = "semantic_progressive"
    backup_api.create_table(table_name, num_shards=1)

    created = backup_api.create_index(
        table_name,
        index_name,
        {
            "name": index_name,
            "type": "embeddings",
            "field": "body",
            "dimension": 3,
            "execution": {"embedding": {"batch_items": 1}},
            "embedder": {
                "provider": "openai",
                "model": "text-embedding-3-small",
                "url": progressive_openai_embedder.url,
            },
        },
    )
    assert_created_index(created, index_name, "embeddings")
    # The public quickstart omits the policy. Verify the API and runtime apply
    # the v0.2 default instead of relying on an explicit test-only override.
    assert created["publication_policy"] == "progressive"
    backup_api.wait_index_ready(table_name, index_name, timeout_s=30.0)
    progressive_openai_embedder.rate_limit_after_next_requests(
        10, input_substring="progressive publication document"
    )

    # Match the quickstart's index-before-load ordering. Separate durable write
    # revisions make the first checkpoint queryable while later documents are
    # still being embedded.
    documents = {
        f"doc:{i:03d}": {
            "title": f"Alpha {i}",
            "body": (
                f"retrieval semantic progressive publication document {i}"
                if i < 10
                else f"alpha concept progressive publication document {i}"
                if i == 10
                else f"beta progressive publication document {i}"
            ),
        }
        for i in range(100)
    }
    started = __import__("time").monotonic()
    first_page = dict(list(documents.items())[:10])
    remaining = dict(list(documents.items())[10:])
    assert backup_api.batch_write(
        table_name,
        inserts=first_page,
        sync_level="write",
    )["inserted"] == len(first_page)
    assert backup_api.batch_write(
        table_name,
        inserts=remaining,
        sync_level="write",
    )["inserted"] == len(remaining)

    deadline = started + 30.0
    partial_status = None
    observed_states = {}
    while __import__("time").monotonic() < deadline:
        status = backup_api.get_index(table_name, index_name)["status"]
        readiness = status.get("readiness") or {}
        observed_states[readiness.get("state", "missing")] = status
        if readiness.get("state") == "queryable_partial":
            partial_status = status
            break
        __import__("time").sleep(0.05)
    assert partial_status is not None, __import__("json").dumps(
        observed_states, indent=2, sort_keys=True
    )
    time_to_queryable_s = __import__("time").monotonic() - started
    assert time_to_queryable_s < 30.0

    readiness = partial_status["readiness"]
    coverage = partial_status["coverage"]
    source_coverage = partial_status["source_coverage"]
    milestones = partial_status["milestones"]
    assert readiness["queryable"] is True
    assert readiness["complete"] is False
    assert readiness["state"] != "failed"
    assert "coverage" in readiness["pending_reasons"]
    assert readiness["incarnation"].startswith("g-")
    assert readiness["published_revision"] <= readiness["target_revision"]
    assert 0 < coverage["covered"] < coverage["source_total"] == 100
    assert coverage["complete"] is False
    assert source_coverage["total"] == 100
    assert source_coverage["covered"] == coverage["covered"]
    assert source_coverage["complete"] is False
    assert milestones["queryable"]["reached"] is True
    assert milestones["queryable"]["blockers"] == []
    assert milestones["complete"]["reached"] is False
    assert "source_coverage" in milestones["complete"]["blockers"]
    assert partial_status["searchable_vectors"] == partial_status["total_indexed"]

    # Activity is leader-local and can be briefly absent during handoff, but
    # the delivery pipeline must produce a heartbeat for the current index
    # incarnation while durable readiness remains independently queryable.
    def current_activity_status() -> dict | None:
        status = backup_api.get_index(table_name, index_name)["status"]
        readiness = status.get("readiness") or {}
        if readiness.get("incarnation") != partial_status["readiness"]["incarnation"]:
            return None
        if status.get("activity") is None:
            return None
        return status

    activity_status = wait_until(
        current_activity_status,
        timeout_s=10.0,
        interval_s=0.05,
    )
    assert activity_status is not None, "current incarnation emitted no activity heartbeat"
    activity = activity_status["activity"]
    assert activity["epoch"].startswith("a-")
    assert activity["phase"] in {
        "idle",
        "preparing",
        "embedding",
        "publishing",
        "waiting_retry",
    }
    assert activity["embeddings_computed"] > 0
    assert activity_status["readiness"]["queryable"] is True
    assert activity_status["readiness"]["complete"] is False

    result = backup_api.query_table(
        table_name,
        {
            # The first page embeds to [0.8, 0.2, 0.0], while doc:010 in the
            # unpublished remainder is the exact [1.0, 0.0, 0.0] match. This
            # proves the public query was served by the partial generation
            # while later enrichment remains throttled.
            "embeddings": {index_name: [1.0, 0.0, 0.0]},
            "indexes": [index_name],
            "limit": 5,
        },
    )
    hits = result["responses"][0]["hits"]["hits"]
    assert hits
    assert int(hits[0]["_id"].removeprefix("doc:")) < 10

    progressive_openai_embedder.allow_rate_limited_requests()
    complete = backup_api.wait_index_ready(
        table_name,
        index_name,
        timeout_s=120.0,
        interval_s=0.1,
        until="complete",
        require_query_fresh=True,
    )
    assert complete["readiness"]["state"] == "ready"
    assert complete["readiness"]["queryable"] is True
    assert complete["readiness"]["complete"] is True
    assert complete["coverage"]["covered"] == 100
    assert complete["source_coverage"]["covered"] == 100
    assert complete["source_coverage"]["complete"] is True
    assert complete["milestones"]["complete"]["reached"] is True
    assert complete["milestones"]["complete"]["blockers"] == []
    assert complete["searchable_vectors"] == 100
    assert complete["total_indexed"] == 100


@pytest.mark.slow
def test_500_document_chunked_backfill_is_bounded_idempotent_and_allows_second_index(
    backup_api, openai_embedder
):
    """Release gate for the launch regression reported against v0.2.1-rc0."""

    table_name = f"quickstart_500_chunked_{__import__('time').time_ns()}"
    docs = {
        f"doc:{number:04d}": {
            "title": f"Release document {number}",
            "body": (
                f"topic-{number} alpha beta gamma delta epsilon zeta eta theta "
                "iota kappa lambda mu nu xi omicron pi rho sigma tau"
            ),
        }
        for number in range(500)
    }

    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name
    doc_items = list(docs.items())
    for offset in range(0, len(doc_items), 50):
        batch = backup_api.batch_write(
            table_name,
            inserts=dict(doc_items[offset : offset + 50]),
            sync_level="write",
        )
        assert batch["inserted"] == min(50, len(doc_items) - offset)

    def index_config(name: str) -> dict:
        return {
            "name": name,
            "type": "embeddings",
            "template": "{{title}} {{body}}",
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
                    "target_tokens": 8,
                    "overlap_tokens": 2,
                    "separator": " ",
                },
            },
        }

    def ready_status(index_name: str) -> dict:
        status = backup_api.wait_index_ready(
            table_name,
            index_name,
            # A debug binary may spend several minutes finalizing the large
            # HBC replay window, especially when a second index consumes the
            # already-cached 2,500 chunk artifacts. Keep the release gate
            # bounded without mistaking productive 440/441 convergence for a
            # stalled backfill.
            timeout_s=600.0,
            interval_s=0.5,
            until="complete",
            require_query_fresh=True,
        )
        coverage = status.get("coverage")
        assert isinstance(coverage, dict), status
        assert coverage["source_total"] == 500, coverage
        assert coverage["covered"] == 500, coverage
        assert coverage["complete"] is True, coverage
        assert coverage["config_mismatch_group_count"] == 0, coverage
        assert status.get("backfill_state") in (None, "ready"), status
        return status

    first_name = "title_body"
    assert_created_index(
        backup_api.create_index(table_name, first_name, index_config(first_name)),
        first_name,
        "embeddings",
    )
    first_status = ready_status(first_name)
    first_count = first_status["total_indexed"]
    assert 500 <= first_count <= 5_000, first_status

    # Re-submit byte-identical source documents. Their source versions may
    # advance, but deterministic chunk/vector identities must not accumulate.
    for offset in range(0, len(doc_items), 50):
        batch = backup_api.batch_write(
            table_name,
            inserts=dict(doc_items[offset : offset + 50]),
            sync_level="write",
        )
        assert batch["inserted"] == min(50, len(doc_items) - offset)
    replay_status = ready_status(first_name)
    assert replay_status["total_indexed"] == first_count, replay_status

    second_name = "title_body_second"
    assert_created_index(
        backup_api.create_index(table_name, second_name, index_config(second_name)),
        second_name,
        "embeddings",
    )
    second_status = ready_status(second_name)
    assert second_status["total_indexed"] == first_count, second_status

    # Creating and backfilling a second dense index must not regress the first
    # index to pending or change its physical entry count.
    first_after_second = ready_status(first_name)
    assert first_after_second["total_indexed"] == first_count, first_after_second


def test_public_managed_antfly_chunked_semantic_full_index_pipeline(
    backup_api, inference_embedder
):
    table_name = f"quickstart_antfly_chunked_semantic_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        backup_api.create_index(
            table_name,
            "semantic_antfly_idx",
            {
                "name": "semantic_antfly_idx",
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "embedder": {
                    "provider": "antfly",
                    "model": "antfly-embed-v1",
                    "api_url": inference_embedder,
                },
                "chunker": {
                    "provider": "antfly",
                    "api_url": inference_embedder,
                    "model": "antfly-chunker-v1",
                    "store_chunks": True,
                    "text": {
                        "target_tokens": 4,
                        "overlap_tokens": 1,
                    },
                },
            },
        ),
        "semantic_antfly_idx",
        "embeddings",
    )

    backup_api.wait_index_ready(
        table_name, "semantic_antfly_idx", timeout_s=30.0, interval_s=0.5
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha in antfly chunks",
                "body": "alpha body chunk tail",
            },
            "doc:b": {
                "title": "Beta in antfly chunks",
                "body": "beta body chunk tail",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = backup_api.query_table(
        table_name,
        {
            "semantic_search": "alpha concept",
            "indexes": ["semantic_antfly_idx"],
            "limit": 5,
        },
    )
    responses = result["responses"]
    hits = responses[0]["hits"]["hits"]
    assert hits[0]["_id"] == "doc:a"

    scan = backup_api.scan_keys(
        table_name,
        {
            "from": "doc:a",
            "to": "doc:a;",
            "inclusive_from": True,
            "fields": ["title", "_chunks"],
        },
    )
    assert len(scan) == 1
    assert scan[0]["title"] == "Alpha in antfly chunks"
    chunks = scan[0]["_chunks"]["semantic_antfly_idx_chunks"]
    assert len(chunks) >= 2
    assert chunks[0]["body"] == "alpha body"
    assert chunks[1]["body"] == "chunk tail"


@pytest.mark.real_model
def test_public_managed_antfly_clipclap_gguf_embedder_smoke(real_clipclap_backup_api):
    backup_api = real_clipclap_backup_api
    table_name = f"quickstart_antfly_clipclap_semantic_{__import__('time').time_ns()}"

    try:
        warmup = backup_api.inference_embed(
            CLIPCLAP_MODEL, "alpha body", timeout_s=120.0
        )
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code in {400, 404}:
            pytest.skip(f"Embedded Antfly inference ClipClap model unavailable: {exc}")
        raise
    warmup_embedding = warmup["data"][0]["embedding"]
    assert len(warmup_embedding) == 512

    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        backup_api.create_index(
            table_name,
            "semantic_clipclap_idx",
            {
                "name": "semantic_clipclap_idx",
                "type": "embeddings",
                "field": "body",
                "dimension": 512,
                "embedder": {
                    "provider": "antfly",
                    "model": CLIPCLAP_MODEL,
                },
            },
        ),
        "semantic_clipclap_idx",
        "embeddings",
    )

    backup_api.wait_index_ready(
        table_name, "semantic_clipclap_idx", timeout_s=60.0, interval_s=0.5
    )


@pytest.mark.real_model
def test_public_managed_antfly_clipclap_gguf_chunked_full_index_pipeline(
    real_clipclap_backup_api,
):
    backup_api = real_clipclap_backup_api
    table_name = f"quickstart_antfly_clipclap_chunked_{__import__('time').time_ns()}"

    try:
        warmup = backup_api.inference_embed(
            CLIPCLAP_MODEL, "alpha body", timeout_s=120.0
        )
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code in {400, 404}:
            pytest.skip(f"Embedded Antfly inference ClipClap model unavailable: {exc}")
        raise
    warmup_embedding = warmup["data"][0]["embedding"]
    assert len(warmup_embedding) == 512

    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        backup_api.create_index(
            table_name,
            "semantic_clipclap_idx",
            {
                "name": "semantic_clipclap_idx",
                "type": "embeddings",
                "field": "body",
                "dimension": 512,
                "embedder": {
                    "provider": "antfly",
                    "model": CLIPCLAP_MODEL,
                },
                "chunker": {
                    "provider": "antfly",
                    "model": CLIPCLAP_MODEL,
                    "store_chunks": True,
                    "text": {
                        "target_tokens": 4,
                        "overlap_tokens": 1,
                        "separator": " ",
                    },
                },
            },
        ),
        "semantic_clipclap_idx",
        "embeddings",
    )

    backup_api.wait_index_ready(
        table_name, "semantic_clipclap_idx", timeout_s=60.0, interval_s=0.5
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha in chunks",
                "body": "alpha alpha alpha alpha beta beta beta beta beta beta",
            },
            "doc:b": {
                "title": "Beta only",
                "body": "beta beta beta beta beta beta beta beta",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    scan = backup_api.scan_keys(
        table_name,
        {
            "from": "doc:a",
            "to": "doc:a;",
            "inclusive_from": True,
            "fields": ["title", "_chunks"],
        },
    )
    assert len(scan) == 1
    assert scan[0]["title"] == "Alpha in chunks"
    chunks = scan[0]["_chunks"]["semantic_clipclap_idx_chunks"]
    assert len(chunks) >= 2
    assert chunks[0]["body"].startswith("alpha")
    assert any(chunk["body"].startswith("beta") for chunk in chunks)
