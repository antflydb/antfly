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

from helpers import assert_single_top_hit, json_doc, upsert, wait_until

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
    assert status["swarm_mode"] is True

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

    direct_match_search = wait_until(direct_match_search_results, timeout_s=10.0, interval_s=0.1)
    assert direct_match_search is not None
    assert_single_top_hit(direct_match_search, "theory-relativity")

    direct_prefix_search = wait_until(direct_prefix_search_results, timeout_s=10.0, interval_s=0.1)
    assert direct_prefix_search is not None
    assert_single_top_hit(direct_prefix_search, "theory-relativity")

    filtered_public_search = wait_until(filtered_public_search_results, timeout_s=10.0, interval_s=0.1)
    assert filtered_public_search is not None
    assert [hit["doc_id"] for hit in filtered_public_search["hits"]] == ["theory-relativity"]

    prefix_filtered_search = wait_until(prefix_filtered_search_results, timeout_s=10.0, interval_s=0.1)
    assert prefix_filtered_search is not None
    assert [hit["doc_id"] for hit in prefix_filtered_search["hits"]] == ["theory-relativity"]

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

def test_public_hybrid_quickstart_pipeline(backup_api, termite_reranker):
    table_name = f"quickstart_hybrid_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/dense_idx",
            {
                "name": "dense_idx",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )
    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/sparse_idx",
            {
                "name": "sparse_idx",
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
                        "provider": "termite",
                        "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                        "url": termite_reranker,
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
                        "provider": "termite",
                        "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                        "url": termite_reranker,
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


def test_public_hybrid_quickstart_supports_weighted_merge_and_template_reranking(backup_api, termite_reranker):
    table_name = f"quickstart_hybrid_template_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/dense_idx",
            {
                "name": "dense_idx",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )
    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/sparse_idx",
            {
                "name": "sparse_idx",
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
                        "provider": "termite",
                        "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                        "url": termite_reranker,
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


def test_public_managed_semantic_hybrid_quickstart_pipeline(backup_api, openai_embedder, termite_reranker):
    table_name = f"quickstart_managed_hybrid_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
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
        )
        == {}
    )

    ready = wait_until(
        lambda: (
            status
            if (
                (status := backup_api.get_index(table_name, "semantic_idx").get("status"))
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
                        "provider": "termite",
                        "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                        "url": termite_reranker,
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
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
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
        )
        == {}
    )

    ready = backup_api.wait_index_ready(table_name, "semantic_idx", timeout_s=30.0, interval_s=0.5)
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
                "body": "beta quickstart notes",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = backup_api.query_table(
        table_name,
        {
            "semantic_search": "alpha concept",
            "indexes": ["semantic_idx"],
            "limit": 5,
        },
    )
    responses = result["responses"]
    hits = responses[0]["hits"]["hits"]
    assert hits[0]["_id"] == "doc:a"


def test_public_managed_chunked_semantic_full_index_pipeline(backup_api, openai_embedder):
    table_name = f"quickstart_chunked_semantic_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
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
        )
        == {}
    )

    ready = backup_api.wait_index_ready(table_name, "semantic_chunked_idx", timeout_s=30.0, interval_s=0.5)
    assert ready is not None

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


def test_public_managed_termite_chunked_semantic_full_index_pipeline(backup_api, termite_embedder):
    table_name = f"quickstart_termite_chunked_semantic_{__import__('time').time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.create_index(
            table_name,
            "semantic_termite_idx",
            {
                "name": "semantic_termite_idx",
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "embedder": {
                    "provider": "termite",
                    "model": "termite-embed-v1",
                    "api_url": termite_embedder,
                },
                "chunker": {
                    "provider": "termite",
                    "api_url": termite_embedder,
                    "model": "termite-chunker-v1",
                    "store_chunks": True,
                    "text": {
                        "target_tokens": 4,
                        "overlap_tokens": 1,
                    },
                },
            },
        )
        == {}
    )

    ready = backup_api.wait_index_ready(table_name, "semantic_termite_idx", timeout_s=30.0, interval_s=0.5)
    assert ready is not None

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha in termite chunks",
                "body": "alpha body chunk tail",
            },
            "doc:b": {
                "title": "Beta in termite chunks",
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
            "indexes": ["semantic_termite_idx"],
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
    assert scan[0]["title"] == "Alpha in termite chunks"
    chunks = scan[0]["_chunks"]["semantic_termite_idx_chunks"]
    assert len(chunks) >= 2
    assert chunks[0]["body"] == "alpha body"
    assert chunks[1]["body"] == "chunk tail"


@pytest.mark.real_model
def test_public_managed_antfly_clipclap_gguf_embedder_smoke(real_clipclap_backup_api):
    backup_api = real_clipclap_backup_api
    table_name = f"quickstart_antfly_clipclap_semantic_{__import__('time').time_ns()}"

    try:
        warmup = backup_api.termite_embed(CLIPCLAP_MODEL, "alpha body", timeout_s=120.0)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code in {400, 404}:
            pytest.skip(f"Embedded Termite ClipClap model unavailable: {exc}")
        raise
    warmup_embedding = warmup["data"][0]["embedding"]
    assert len(warmup_embedding) == 512

    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
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
        )
        == {}
    )

    ready = backup_api.wait_index_ready(table_name, "semantic_clipclap_idx", timeout_s=60.0, interval_s=0.5)
    assert ready is not None


@pytest.mark.real_model
def test_public_managed_antfly_clipclap_gguf_chunked_full_index_pipeline(real_clipclap_backup_api):
    backup_api = real_clipclap_backup_api
    table_name = f"quickstart_antfly_clipclap_chunked_{__import__('time').time_ns()}"

    try:
        warmup = backup_api.termite_embed(CLIPCLAP_MODEL, "alpha body", timeout_s=120.0)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code in {400, 404}:
            pytest.skip(f"Embedded Termite ClipClap model unavailable: {exc}")
        raise
    warmup_embedding = warmup["data"][0]["embedding"]
    assert len(warmup_embedding) == 512

    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
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
        )
        == {}
    )

    ready = backup_api.wait_index_ready(table_name, "semantic_clipclap_idx", timeout_s=60.0, interval_s=0.5)
    assert ready is not None

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
