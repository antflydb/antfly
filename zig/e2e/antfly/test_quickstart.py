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
import os
import subprocess
import time
from pathlib import Path

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
                json_doc(body="relativity"),
            ),
            upsert(
                "ancient-rome",
                json_doc(body="rome"),
            ),
            upsert(
                "machine-learning",
                json_doc(body="learning"),
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

    backup_api.wait_index_ready(
        table_name,
        index_name,
        timeout_s=30.0,
        interval_s=0.5,
        until="complete",
    )

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

    status = backup_api.wait_index_ready(
        table_name,
        index_name,
        timeout_s=30.0,
        interval_s=0.25,
        until="complete",
    )
    index = backup_api.get_index(table_name, index_name)
    assert index["config"]["name"] == index_name
    # Assert the exact readiness observation that satisfied the wait. A second
    # status GET is useful for config validation but is not the completion
    # receipt and must not create a time-of-check/time-of-use race here.
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

    status = backup_api.wait_index_ready(
        table_name,
        index_name,
        timeout_s=30.0,
        interval_s=0.25,
        until="complete",
    )
    index = backup_api.get_index(table_name, index_name)
    assert index["config"]["name"] == index_name
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
        table_name,
        "semantic_chunked_idx",
        timeout_s=30.0,
        interval_s=0.5,
        until="complete",
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
            "template": "{{title}} {{body}}",
            "dimension": 3,
            "execution": {"embedding": {"batch_items": 1}},
            "embedder": {
                "provider": "openai",
                "model": "text-embedding-3-small",
                "url": progressive_openai_embedder.url,
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
        },
    )
    assert_created_index(created, index_name, "embeddings")
    # The public quickstart omits the policy. Verify the API and runtime apply
    # the v0.2 default instead of relying on an explicit test-only override.
    assert created["publication_policy"] == "progressive"
    backup_api.wait_index_ready(
        table_name,
        index_name,
        timeout_s=30.0,
        until="complete",
    )
    progressive_openai_embedder.rate_limit_after_next_requests(
        300, input_substring="progressive publication document"
    )

    # Match the quickstart's index-before-load ordering. Separate durable write
    # revisions make the first checkpoint queryable while later documents are
    # still being embedded.
    documents = {
        f"doc:{i:03d}": {
            "title": f"Document {i}",
            "body": (
                f"retrieval semantic progressive publication document {i} context evidence history details"
                if i < 10
                else f"alpha concept progressive publication document {i} context evidence history details"
                if i == 90
                else f"beta progressive publication document {i} context evidence history details"
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
        # `queryable` is the safety/admission milestone and an empty published
        # generation is valid. The quickstart's time-to-first-result outcome
        # additionally requires a published searchable artifact from the same
        # status observation.
        if (
            readiness.get("state") == "queryable_partial"
            and int(status.get("searchable_vectors", 0)) >= 1
            and int((status.get("source_coverage") or {}).get("covered", 0)) >= 1
        ):
            partial_status = status
            break
        __import__("time").sleep(0.05)
    assert partial_status is not None, __import__("json").dumps(
        observed_states, indent=2, sort_keys=True
    )
    time_to_first_artifact_s = __import__("time").monotonic() - started
    assert time_to_first_artifact_s < 30.0

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

    # Exercise the documented CLI outcome, not only the fixture's equivalent
    # HTTP polling loop. ANTFLY_URL points at the process root because the CLI
    # owns public API prefix selection.
    server = backup_api._server
    if server is not None and Path(server.binary).name == "antfly":
        cli_env = os.environ.copy()
        cli_env["ANTFLY_URL"] = server.url
        cli_wait = subprocess.run(
            [
                server.binary,
                "index",
                "wait",
                "--table",
                table_name,
                "--index",
                index_name,
                "--until",
                "searchable-artifacts=1",
                "--timeout",
                "5s",
                "--poll-interval",
                "25ms",
            ],
            capture_output=True,
            text=True,
            timeout=8.0,
            env=cli_env,
            check=False,
        )
        assert cli_wait.returncode == 0, (
            f"stdout:\n{cli_wait.stdout}\nstderr:\n{cli_wait.stderr}\n"
            f"server logs:\n{backup_api.debug_logs()}"
        )
        assert "reached searchable-artifacts=1" in cli_wait.stdout

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
    assert activity_status is not None, (
        "current incarnation emitted no activity heartbeat"
    )
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
            # The first page embeds to [0.8, 0.2, 0.0], while doc:090 in the
            # unpublished tail is the exact [1.0, 0.0, 0.0] match. This
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

    # Create a second managed index while the first owner is blocked on its
    # provider. Live activation must publish an exact runtime observation
    # without waiting behind generic structural/repair work, and the second
    # index must make progress on its independent title inputs.
    second_index_name = "semantic_title_live"
    first_incarnation = partial_status["readiness"]["incarnation"]
    first_floor = int(partial_status["searchable_vectors"])
    first_coverage_floor = int(partial_status["source_coverage"]["covered"])
    first_last_artifacts = first_floor
    first_last_status = partial_status
    first_continuity_samples = 0

    def assert_first_serving_continuity() -> dict:
        nonlocal first_last_artifacts, first_last_status, first_continuity_samples
        request_started = time.monotonic()
        status = backup_api.get_index(table_name, index_name)["status"]
        request_elapsed = time.monotonic() - request_started
        assert request_elapsed < 1.0, (
            f"status request for {index_name} took {request_elapsed:.3f}s"
        )
        readiness = status.get("readiness") or {}
        assert readiness.get("state") != "runtime_unavailable", status
        assert readiness.get("incarnation") == first_incarnation, status
        assert readiness.get("queryable") is True, status
        searchable = int(status.get("searchable_vectors", 0))
        assert searchable >= first_last_artifacts, json.dumps(
            {"previous": first_last_status, "current": status},
            indent=2,
            sort_keys=True,
        )
        first_last_artifacts = searchable
        first_last_status = status
        covered = int((status.get("source_coverage") or {}).get("covered", 0))
        assert covered >= first_coverage_floor, status
        first_continuity_samples += 1
        return status

    assert_first_serving_continuity()
    activation_started = __import__("time").monotonic()
    second_created = backup_api.create_index(
        table_name,
        second_index_name,
        {
            "name": second_index_name,
            "type": "embeddings",
            "field": "title",
            "dimension": 3,
            "execution": {"embedding": {"batch_items": 1}},
            "embedder": {
                "provider": "openai",
                "model": "text-embedding-3-small",
                "url": progressive_openai_embedder.url,
            },
        },
    )
    assert_created_index(second_created, second_index_name, "embeddings")
    assert_first_serving_continuity()

    activation_samples = []

    def second_index_has_runtime_observation() -> dict | None:
        assert_first_serving_continuity()
        status = backup_api.get_index(table_name, second_index_name)["status"]
        activation_samples.append(status)
        assert status.get("repair") is None, status
        readiness = status.get("readiness") or {}
        pending_reasons = readiness.get("pending_reasons") or []
        if "runtime_unavailable" in pending_reasons:
            return None
        if not str(readiness.get("incarnation", "")).startswith("g-"):
            return None
        return status

    activated = wait_until(
        second_index_has_runtime_observation,
        timeout_s=5.0,
        interval_s=0.05,
    )
    assert activated is not None, __import__("json").dumps(
        activation_samples[-3:], indent=2, sort_keys=True
    )
    assert __import__("time").monotonic() - activation_started < 5.0
    second_incarnation = activated["readiness"]["incarnation"]

    def second_index_has_published_artifact() -> dict | None:
        assert_first_serving_continuity()
        status = backup_api.get_index(table_name, second_index_name)["status"]
        readiness = status.get("readiness") or {}
        pending_reasons = readiness.get("pending_reasons") or []
        assert "runtime_unavailable" not in pending_reasons, json.dumps(
            {"activated": activated, "current": status}, indent=2, sort_keys=True
        )
        assert status.get("repair") is None, status
        assert readiness.get("incarnation") == second_incarnation, status
        if (
            readiness.get("queryable") is True
            and int(status.get("searchable_vectors", 0)) >= 1
            and int((status.get("source_coverage") or {}).get("covered", 0)) >= 1
        ):
            return status
        return None

    second_partial = wait_until(
        second_index_has_published_artifact,
        timeout_s=30.0,
        interval_s=0.05,
    )
    assert second_partial is not None
    assert (
        backup_api.get_index(table_name, index_name)["status"]["readiness"]["complete"]
        is False
    )

    # Status is a bounded immutable-snapshot read. Sample across several owner
    # refresh cycles while both incarnations are active: a missed heartbeat may
    # remove activity, but must never revoke serving authority or zero facts.
    second_floor = int(second_partial["searchable_vectors"])
    second_coverage_floor = int(second_partial["source_coverage"]["covered"])
    second_last_status = second_partial
    sampling_deadline = time.monotonic() + 3.0
    samples = 0
    while time.monotonic() < sampling_deadline:
        for sampled_index, incarnation, artifacts_floor, coverage_floor in (
            (index_name, first_incarnation, first_floor, first_coverage_floor),
            (
                second_index_name,
                second_incarnation,
                second_floor,
                second_coverage_floor,
            ),
        ):
            if sampled_index == index_name:
                assert_first_serving_continuity()
                continue
            request_started = time.monotonic()
            sampled = backup_api.get_index(table_name, sampled_index)["status"]
            request_elapsed = time.monotonic() - request_started
            assert request_elapsed < 1.0, (
                f"status request for {sampled_index} took {request_elapsed:.3f}s"
            )
            sampled_readiness = sampled.get("readiness") or {}
            assert sampled_readiness.get("state") != "runtime_unavailable", sampled
            assert sampled_readiness.get("incarnation") == incarnation, sampled
            assert sampled_readiness.get("queryable") is True, sampled
            sampled_artifacts = int(sampled.get("searchable_vectors", 0))
            sampled_covered = int(
                (sampled.get("source_coverage") or {}).get("covered", 0)
            )
            assert sampled_artifacts >= artifacts_floor, json.dumps(
                {"previous": second_last_status, "current": sampled},
                indent=2,
                sort_keys=True,
            )
            assert sampled_covered >= coverage_floor, sampled
            # No writes, updates, or deletes occur during this window. Within
            # one incarnation and accepted source target, every newer serving
            # observation is therefore monotonic. Mutations and incarnation
            # replacement are the explicit boundaries where counts may fall.
            second_floor = sampled_artifacts
            second_coverage_floor = sampled_covered
            second_last_status = sampled
        samples += 1
        time.sleep(0.05)
    assert samples >= 3
    assert first_continuity_samples >= 5

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
    first_complete_artifacts = complete["searchable_vectors"]
    assert first_complete_artifacts > 100
    assert complete["total_indexed"] == first_complete_artifacts
    second_complete = backup_api.wait_index_ready(
        table_name,
        second_index_name,
        timeout_s=120.0,
        interval_s=0.1,
        until="complete",
        require_query_fresh=True,
    )
    assert second_complete["readiness"]["complete"] is True
    assert second_complete["searchable_vectors"] == 100

    # A newly accepted source revision must fence convergence synchronously.
    # Serving stays available from the published incarnation, but a status
    # read immediately after the write must never replay stale complete=true.
    progressive_openai_embedder.rate_limit_after_next_requests(0)
    assert (
        backup_api.batch_write(
            table_name,
            inserts={
                "doc:100": {
                    "title": "Gamma 100",
                    "body": "gamma progressive publication document 100",
                }
            },
            sync_level="write",
        )["inserted"]
        == 1
    )

    after_write = backup_api.get_index(table_name, index_name)["status"]
    assert after_write["readiness"]["incarnation"] == first_incarnation
    assert after_write["readiness"]["queryable"] is True
    assert after_write["readiness"]["complete"] is False
    assert after_write["readiness"]["state"] == "queryable_partial"
    assert after_write["searchable_vectors"] == first_complete_artifacts
    assert set(after_write["milestones"]["complete"]["blockers"]) & {
        "target_observation",
        "source_coverage",
        "publication",
    }

    progressive_openai_embedder.allow_rate_limited_requests()
    reconverged = backup_api.wait_index_ready(
        table_name,
        index_name,
        timeout_s=30.0,
        interval_s=0.05,
        until="complete",
        require_query_fresh=True,
    )
    assert reconverged["readiness"]["complete"] is True
    assert reconverged["source_coverage"]["covered"] == 101
    assert reconverged["searchable_vectors"] > first_complete_artifacts


def test_live_index_activation_preempts_an_active_enrichment_quantum(
    single_item_enrichment_batches,
    backup_api,
    inference_embedder,
    slow_inference_embedder,
):
    """Quickstart-style live DDL preempts active remote-media enrichment."""
    _ = single_item_enrichment_batches
    table_name = f"quickstart_live_activation_{__import__('time').time_ns()}"
    first_index = "thumbnail_active"
    second_index = "title_live"
    backup_api.create_table(table_name, num_shards=1)

    try:
        created = backup_api.create_index(
            table_name,
            first_index,
            {
                "name": first_index,
                "type": "embeddings",
                "template": "{{#if thumbnail_url}}{{remoteMedia url=thumbnail_url}}{{/if}}",
                "coverage_policy": "partial",
                "dimension": 3,
                "execution": {"embedding": {"batch_items": 1}},
                "embedder": {
                    "provider": "antfly",
                    "model": CLIPCLAP_MODEL,
                    "api_url": slow_inference_embedder.url,
                },
            },
        )
        assert_created_index(created, first_index, "embeddings")
        backup_api.wait_index_ready(
            table_name,
            first_index,
            timeout_s=30.0,
            until="complete",
        )
        slow_inference_embedder.arm_delay()

        tiny_png = (
            "data:image/png;base64,"
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ"
            "AAAADUlEQVR42mNk+M/wHwAF/gL+X3GQAAAAAElFTkSuQmCC"
        )
        documents = {
            f"doc:{i:03d}": {
                "title": f"Live activation {i}",
                "body": f"alpha quickstart activation document {i}",
                "thumbnail_url": tiny_png,
            }
            for i in range(32)
        }
        assert backup_api.batch_write(
            table_name,
            inserts=documents,
            sync_level="write",
        )["inserted"] == len(documents)

        # This is the real quickstart shape: a ClipClap image projection owns
        # an inference request while another embeddings index is created.
        assert slow_inference_embedder.wait_for_embedding_request(20.0), (
            __import__("json").dumps(
                backup_api.get_index(table_name, first_index)["status"],
                indent=2,
                sort_keys=True,
            )
            + "\n"
            + backup_api.debug_logs()
        )
        first_active = backup_api.get_index(table_name, first_index)["status"]
        first_incarnation = first_active["incarnation"]

        # The provider will not answer for 30 seconds. Activation must cancel
        # the borrowed maintenance wait and active transport rather than
        # inheriting that latency or draining the corpus window.
        activation_started = __import__("time").monotonic()
        second_created = backup_api.create_index(
            table_name,
            second_index,
            {
                "name": second_index,
                "type": "embeddings",
                "field": "title",
                "dimension": 3,
                "execution": {"embedding": {"batch_items": 1}},
                "embedder": {
                    "provider": "antfly",
                    "model": "antfly-embed-v1",
                    "api_url": inference_embedder,
                },
            },
        )
        activation_elapsed = __import__("time").monotonic() - activation_started
        assert_created_index(second_created, second_index, "embeddings")
        assert activation_elapsed < 8.0, (
            f"live activation waited {activation_elapsed:.3f}s for corpus work\n"
            f"{backup_api.debug_logs()}"
        )

        def activated_status() -> dict | None:
            status = backup_api.get_index(table_name, second_index)["status"]
            readiness = status.get("readiness") or {}
            if "runtime_unavailable" in (readiness.get("pending_reasons") or []):
                return None
            if not str(readiness.get("incarnation", "")).startswith("g-"):
                return None
            return status

        second_status = wait_until(
            activated_status,
            timeout_s=5.0,
            interval_s=0.05,
        )
        assert second_status is not None, backup_api.debug_logs()
        assert second_status.get("repair") is None

        # Installing a sibling incarnation cannot revoke the already-published
        # generation or identity of the first index.
        first_after = backup_api.get_index(table_name, first_index)["status"]
        assert first_after["incarnation"] == first_incarnation
        assert first_after["readiness"]["queryable"] is True
        assert first_after.get("repair") is None, (
            __import__("json").dumps(first_after, indent=2, sort_keys=True)
            + "\n"
            + backup_api.debug_logs()
        )
    finally:
        # The delay exists only to hold the old owner inside one observable
        # provider call. Remove it before the fixture drops its owned table so
        # the deliberately small corpus does not dominate suite latency.
        slow_inference_embedder.release_delay()


@pytest.mark.fresh_antfly_process
def test_progressive_publication_remains_queryable_across_process_restart(
    single_item_enrichment_batches,
    stateful_api,
    progressive_openai_embedder,
):
    """A durable intra-revision checkpoint remains serviceable on restart."""
    _ = single_item_enrichment_batches
    assert stateful_api.supports_restart
    table_name = f"quickstart_restart_{__import__('time').time_ns()}"
    index_name = "semantic_restart"
    stateful_api.create_table(table_name, num_shards=1)
    created = stateful_api.create_index(
        table_name,
        index_name,
        {
            "name": index_name,
            "type": "embeddings",
            "template": "{{title}} {{body}}",
            "dimension": 3,
            "execution": {"embedding": {"batch_items": 1}},
            "embedder": {
                "provider": "openai",
                "model": "text-embedding-3-small",
                "url": progressive_openai_embedder.url,
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
        },
    )
    assert_created_index(created, index_name, "embeddings")
    initially_complete = wait_until(
        lambda: (
            current
            if (current := stateful_api.get_index(table_name, index_name)["status"])
            .get("milestones", {})
            .get("complete", {})
            .get("reached")
            else None
        ),
        timeout_s=30.0,
        interval_s=0.05,
    )
    assert initially_complete is not None

    # One write revision deliberately spans more than one preparation window.
    # The worker publishes the first window, persists its position within this
    # revision, then remains throttled with later documents still outstanding.
    progressive_openai_embedder.rate_limit_after_next_requests(
        160, input_substring="restart publication document"
    )
    documents = {
        f"doc:{i:03d}": {
            "title": f"Restart {i}",
            "body": (
                f"alpha restart publication document {i} context evidence history details"
                if i < 70
                else f"beta restart publication document {i} context evidence history details"
            ),
        }
        for i in range(100)
    }

    try:
        written = stateful_api.batch_write(
            table_name,
            inserts=documents,
            sync_level="write",
        )
        assert written["inserted"] == len(documents)

        def queryable_partial() -> dict | None:
            status = stateful_api.get_index(table_name, index_name)["status"]
            milestones = status.get("milestones") or {}
            queryable = milestones.get("queryable") or {}
            complete = milestones.get("complete") or {}
            if not queryable.get("reached") or complete.get("reached"):
                return None
            if int(status.get("searchable_vectors", 0)) <= 0:
                return None
            # Restart from a genuinely observed partial checkpoint, not the
            # intentionally conservative handoff snapshot where last-known
            # serving facts remain visible but the new target is still
            # unobserved and pending is therefore unknown.
            pending = (status.get("source_coverage") or {}).get("pending")
            if not isinstance(pending, int) or pending <= 0:
                return None
            return status

        before = wait_until(
            queryable_partial,
            timeout_s=30.0,
            interval_s=0.05,
        )
        assert before is not None
        incarnation = before["incarnation"]
        searchable_vectors = before["searchable_vectors"]
        covered_sources = before["source_coverage"]["covered"]
        assert 0 < covered_sources < len(documents)
        assert searchable_vectors > covered_sources
        assert before["source_coverage"]["pending"] > 0

        stateful_api.restart_server()
        restarted_at = __import__("time").monotonic()
        restart_last_searchable = searchable_vectors

        def restored_queryability() -> dict | None:
            nonlocal restart_last_searchable
            status = stateful_api.get_index(table_name, index_name)["status"]
            if status.get("incarnation") != incarnation:
                return None
            if not (status.get("milestones") or {}).get("queryable", {}).get("reached"):
                return None
            current_searchable = int(status.get("searchable_vectors", 0))
            assert current_searchable >= restart_last_searchable, status
            restart_last_searchable = current_searchable
            return status

        after = wait_until(
            restored_queryability,
            timeout_s=8.0,
            interval_s=0.05,
        )
        assert after is not None, __import__("json").dumps(
            {
                "index": stateful_api.get_index(table_name, index_name),
                "logs": stateful_api.debug_logs(),
            },
            indent=2,
            sort_keys=True,
        )
        assert __import__("time").monotonic() - restarted_at < 8.0
        assert after["milestones"]["queryable"]["blockers"] == []
        assert (
            after["source_coverage"]["covered"] >= before["source_coverage"]["covered"]
        )

        # Startup may first expose the durable serving checkpoint while its
        # owner is still re-establishing convergence authority. That must not
        # delay queries or erase last-known facts, and the current pending
        # count should become authoritative promptly afterward.
        def restored_convergence() -> dict | None:
            nonlocal restart_last_searchable
            status = stateful_api.get_index(table_name, index_name)["status"]
            pending = (status.get("source_coverage") or {}).get("pending")
            if status.get("incarnation") != incarnation:
                return None
            current_searchable = int(status.get("searchable_vectors", 0))
            assert current_searchable >= restart_last_searchable, status
            restart_last_searchable = current_searchable
            if not isinstance(pending, int) or pending <= 0:
                return None
            return status

        converged_after = wait_until(
            restored_convergence,
            timeout_s=8.0,
            interval_s=0.05,
        )
        assert converged_after is not None

        query_started = __import__("time").monotonic()
        result = stateful_api.query_table(
            table_name,
            {
                "embeddings": {index_name: [1.0, 0.0, 0.0]},
                "indexes": [index_name],
                "limit": 5,
            },
        )
        assert __import__("time").monotonic() - query_started < 5.0
        hits = result["responses"][0]["hits"]["hits"]
        assert hits
        assert int(hits[0]["_id"].removeprefix("doc:")) < 70

    finally:
        progressive_openai_embedder.allow_rate_limited_requests()

    complete = wait_until(
        lambda: (
            current
            if (current := stateful_api.get_index(table_name, index_name)["status"])
            .get("milestones", {})
            .get("complete", {})
            .get("reached")
            else None
        ),
        timeout_s=120.0,
        interval_s=0.1,
    )
    assert complete is not None, __import__("json").dumps(
        {
            "index": stateful_api.get_index(table_name, index_name),
            "logs": stateful_api.debug_logs(),
        },
        indent=2,
        sort_keys=True,
    )
    assert complete["milestones"]["complete"]["reached"] is True
    assert complete["source_coverage"]["covered"] == len(documents)
    assert complete["searchable_vectors"] > len(documents)


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
        table_name,
        "semantic_antfly_idx",
        timeout_s=30.0,
        interval_s=0.5,
        until="complete",
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
        table_name,
        "semantic_clipclap_idx",
        timeout_s=60.0,
        interval_s=0.5,
        until="complete",
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
        table_name,
        "semantic_clipclap_idx",
        timeout_s=60.0,
        interval_s=0.5,
        until="complete",
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
