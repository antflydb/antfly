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

"""Public table index lifecycle tests."""

from __future__ import annotations

import base64
import json
import struct
import time
import pytest
import requests

from conftest import ready_index_status
from helpers import json_doc, upsert, wait_until


def _table_name(created: dict) -> str:
    return created.get("name") or created.get("table_name") or ""


def _response_hit_ids(result: dict) -> list[str]:
    responses = result.get("responses", [])
    if not responses:
        return []
    return [hit.get("_id") for hit in responses[0].get("hits", {}).get("hits", [])]


def _index_names(index_list: list[dict]) -> set[str]:
    return {entry["config"]["name"] for entry in index_list}


def _full_text_entry(status: dict, index_name: str) -> dict | None:
    for entry in status.get("full_text_index_actions", []):
        if entry["name"] == index_name:
            return entry
    return None


def _full_text_action(status: dict, field: str, index_name: str) -> str | None:
    for entry in status.get(field, []):
        if entry["name"] == index_name:
            return entry["action"]
    return None


def _named_action(status: dict, field: str, index_name: str) -> str | None:
    for entry in status.get(field, []):
        if entry["name"] == index_name:
            return entry["action"]
    return None


def _maybe_serverless_build(serverless_api, table_name: str) -> dict | None:
    try:
        return serverless_api.build_table(table_name)
    except requests.HTTPError as exc:
        assert exc.response is not None
        if exc.response.status_code == 409:
            return serverless_api.table_build_status(table_name)
        raise


def _index_exists(stateful_api, table_name: str, index_name: str) -> dict | None:
    try:
        names = _index_names(stateful_api.list_indexes(table_name))
    except Exception:
        return None
    if index_name not in names:
        return None
    return {"present": True}


def _index_missing(stateful_api, table_name: str, index_name: str) -> dict | None:
    try:
        names = _index_names(stateful_api.list_indexes(table_name))
    except Exception:
        return None
    if index_name in names:
        return None
    return {"missing": True}


def _index_stats(index_status: dict) -> dict:
    return index_status["status"]


def _pack_f32_le(values: list[float]) -> str:
    return base64.b64encode(struct.pack(f"<{len(values)}f", *values)).decode("ascii")


def _ready_index(stateful_api, table_name: str, index_name: str, *, expected_docs: int) -> dict | None:
    try:
        stats = _index_stats(stateful_api.get_index(table_name, index_name))
    except Exception:
        return None
    if stats.get("rebuilding", stats.get("backfill_active", False)):
        return None
    total_indexed = stats.get("total_indexed", stats.get("doc_count", 0))
    if total_indexed < expected_docs:
        return None
    return stats


def _retrying_partial_index(stateful_api, table_name: str, index_name: str, *, expected_docs: int) -> dict | None:
    try:
        stats = _index_stats(stateful_api.get_index(table_name, index_name))
    except Exception:
        return None
    enrichment = stats.get("enrichment_runtime")
    if not isinstance(enrichment, dict):
        return None
    total_indexed = int(stats.get("total_indexed", stats.get("doc_count", 0)))
    if not stats.get("backfill_active", False):
        return None
    if stats.get("backfill_state") != "retrying":
        return None
    if float(stats.get("backfill_progress", 1.0)) >= 1.0:
        return None
    if total_indexed >= expected_docs:
        return None
    if int(enrichment.get("error_count", 0)) == 0:
        return None
    if int(enrichment.get("retryable_error_count", 0)) == 0:
        return None
    if not enrichment.get("retrying", False):
        return None
    if enrichment.get("worker_failed", False):
        return None
    return stats


def test_table_index_lifecycle(table_api):
    table_name = f"index_lifecycle_{time.time_ns()}"
    index_name = "embed_idx"

    created = table_api.create_table(table_name)
    assert _table_name(created) == table_name
    assert "full_text_index_v0" in created["indexes"]

    indexes = table_api.list_indexes(table_name)
    assert "full_text_index_v0" in _index_names(indexes)

    full_text = table_api.get_index(table_name, "full_text_index_v0")
    assert full_text["config"]["name"] == "full_text_index_v0"

    created_index = table_api.create_index(
        table_name,
        index_name,
        {
            "name": index_name,
            "type": "embeddings",
            "external": True,
            "dimension": 384,
        },
    )
    assert created_index == {}

    assert (
        wait_until(
            lambda: _index_exists(table_api, table_name, index_name),
            timeout_s=30.0,
            interval_s=0.5,
        )
        is not None
    )

    embed_index = table_api.get_index(table_name, index_name)
    assert embed_index["config"]["name"] == index_name
    assert embed_index["config"]["type"] == "embeddings"

    deleted = table_api.delete_index(table_name, index_name)
    assert deleted == {}

    assert (
        wait_until(
            lambda: _index_missing(table_api, table_name, index_name),
            timeout_s=30.0,
            interval_s=0.5,
        )
        is not None
    )

    try:
        table_api.get_index(table_name, index_name)
    except requests.HTTPError as exc:
        assert exc.response is not None
        assert exc.response.status_code == 404
    else:
        raise AssertionError("expected deleted index lookup to return 404")


def test_table_rejects_public_full_text_create_index(table_api):
    table_name = f"index_backfill_{time.time_ns()}"

    created = table_api.create_table(table_name)
    assert _table_name(created) == table_name

    try:
        table_api.create_index(
            table_name,
            "search_idx",
            {
                "name": "search_idx",
                "type": "full_text",
            },
        )
    except requests.HTTPError as exc:
        assert exc.response is not None
        assert exc.response.status_code == 400
    else:
        raise AssertionError("expected public full-text create_index to be rejected")


def test_stateful_external_embeddings_index_detail_supports_packed_ingest_and_query(stateful_api):
    table_name = f"stateful_external_embeddings_{time.time_ns()}"
    index_name = "semantic_idx"

    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        stateful_api.create_index(
            table_name,
            index_name,
            {
                "name": index_name,
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )

    detail = wait_until(
        lambda: (
            current
            if (current := stateful_api.get_index(table_name, index_name)).get("config", {}).get("name") == index_name
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert detail is not None
    assert detail["config"]["name"] == index_name

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "_embeddings": {
                    index_name: _pack_f32_le([1.0, 0.0, 0.0]),
                },
            },
            "doc:b": {
                "title": "beta",
                "_embeddings": {
                    index_name: _pack_f32_le([0.0, 1.0, 0.0]),
                },
            },
            "doc:c": {
                "title": "gamma",
                "_embeddings": {
                    index_name: _pack_f32_le([0.0, 0.0, 1.0]),
                },
            },
        },
        sync_level="write",
    )
    assert batch["inserted"] == 3

    ready = wait_until(
        lambda: _ready_index(stateful_api, table_name, index_name, expected_docs=3),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert ready is not None

    result = wait_until(
        lambda: (
            current
            if (
                (current := stateful_api.query_table(
                    table_name,
                    {
                        "embeddings": {
                            index_name: _pack_f32_le([1.0, 0.0, 0.0]),
                        },
                        "indexes": [index_name],
                        "limit": 3,
                    },
                ))
                .get("responses", [{}])[0]
                .get("hits", {})
                .get("hits")
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert result is not None

    hits = result["responses"][0]["hits"]["hits"]
    assert hits[0]["_id"] == "doc:a"


def test_stateful_managed_embeddings_delete_recreate_recovers_after_rate_limited_enrichment(
    single_item_enrichment_batches,
    stateful_api,
    rate_limited_openai_embedder,
):
    table_name = f"stateful_rate_limited_managed_embeddings_{time.time_ns()}"
    index_name = "semantic_idx"

    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    index_payload = {
        "name": index_name,
        "type": "embeddings",
        "field": "body",
        "dimension": 3,
        "embedder": {
            "provider": "openai",
            "model": "text-embedding-3-small",
            "url": rate_limited_openai_embedder.url,
        },
    }

    assert stateful_api.create_index(table_name, index_name, index_payload) == {}
    assert wait_until(
        lambda: ready_index_status(stateful_api.get_index(table_name, index_name)),
        timeout_s=30.0,
        interval_s=0.5,
    )

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "alpha concept overview",
            },
            "doc:b": {
                "title": "beta",
                "body": "beta architecture notes",
            },
            "doc:c": {
                "title": "gamma",
                "body": "gamma implementation details",
            },
        },
        sync_level="write",
    )
    assert batch["inserted"] == 3

    partial = wait_until(
        lambda: (
            stats
            if (stats := rate_limited_openai_embedder.stats())["rate_limited_requests"] > 0
            and stats["successful_requests"] >= 1
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert partial is not None

    rate_limited_openai_embedder.allow_all_requests()

    assert stateful_api.delete_index(table_name, index_name) == {}
    assert (
        wait_until(
            lambda: _index_missing(stateful_api, table_name, index_name),
            timeout_s=30.0,
            interval_s=0.5,
        )
        is not None
    )

    assert stateful_api.create_index(table_name, index_name, index_payload) == {}

    recovered = wait_until(
        lambda: _ready_index(stateful_api, table_name, index_name, expected_docs=3),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert recovered is not None, stateful_api.get_index(table_name, index_name)

    alpha_query = stateful_api.query_table(
        table_name,
        {
            "semantic_search": "alpha concept",
            "indexes": [index_name],
            "limit": 3,
        },
    )
    alpha_hits = alpha_query["responses"][0]["hits"]["hits"]
    assert alpha_hits[0]["_id"] == "doc:a"

    beta_query = stateful_api.query_table(
        table_name,
        {
            "semantic_search": "beta architecture",
            "indexes": [index_name],
            "limit": 3,
        },
    )
    beta_hits = beta_query["responses"][0]["hits"]["hits"]
    assert beta_hits[0]["_id"] == "doc:b"


def test_stateful_managed_embeddings_backfill_recovers_after_rate_limited_enrichment_without_recreate(
    single_item_enrichment_batches,
    stateful_api,
    rate_limited_openai_embedder,
):
    table_name = f"stateful_rate_limited_managed_embeddings_resume_{time.time_ns()}"
    index_name = "semantic_idx"

    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    index_payload = {
        "name": index_name,
        "type": "embeddings",
        "field": "body",
        "dimension": 3,
        "embedder": {
            "provider": "openai",
            "model": "text-embedding-3-small",
            "url": rate_limited_openai_embedder.url,
        },
    }

    assert stateful_api.create_index(table_name, index_name, index_payload) == {}
    assert wait_until(
        lambda: ready_index_status(stateful_api.get_index(table_name, index_name)),
        timeout_s=30.0,
        interval_s=0.5,
    )

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "alpha concept overview",
            },
            "doc:b": {
                "title": "beta",
                "body": "beta architecture notes",
            },
            "doc:c": {
                "title": "gamma",
                "body": "gamma implementation details",
            },
        },
        sync_level="write",
    )
    assert batch["inserted"] == 3

    partial = wait_until(
        lambda: (
            stats
            if (stats := rate_limited_openai_embedder.stats())["rate_limited_requests"] > 0
            and stats["successful_requests"] >= 1
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert partial is not None

    rate_limited_openai_embedder.allow_all_requests()

    recovered = wait_until(
        lambda: _ready_index(stateful_api, table_name, index_name, expected_docs=3),
        timeout_s=60.0,
        interval_s=0.5,
    )
    recovery_debug = {
        "index": stateful_api.get_index(table_name, index_name),
        "embedder": rate_limited_openai_embedder.stats(),
    }
    assert recovered is not None, json.dumps(recovery_debug, indent=2, sort_keys=True)

    alpha_query = stateful_api.query_table(
        table_name,
        {
            "semantic_search": "alpha concept",
            "indexes": [index_name],
            "limit": 3,
        },
    )
    print("live_index_status", stateful_api.get_index(table_name, index_name))
    explicit_query = stateful_api.query_table(
        table_name,
        {
            "embeddings": {index_name: [1.0, 0.0, 0.0]},
            "indexes": [index_name],
            "limit": 3,
        },
    )
    print("explicit_query", explicit_query)
    print("alpha_query", alpha_query)
    assert _response_hit_ids(alpha_query)[0] == "doc:a"


def test_stateful_managed_embeddings_status_reports_partial_retrying_backfill_after_rate_limit(
    single_item_enrichment_batches,
    stateful_api,
    rate_limited_openai_embedder,
):
    table_name = f"stateful_rate_limited_managed_embeddings_status_{time.time_ns()}"
    index_name = "semantic_idx"

    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    index_payload = {
        "name": index_name,
        "type": "embeddings",
        "field": "body",
        "dimension": 3,
        "embedder": {
            "provider": "openai",
            "model": "text-embedding-3-small",
            "url": rate_limited_openai_embedder.url,
        },
    }

    assert stateful_api.create_index(table_name, index_name, index_payload) == {}
    assert wait_until(
        lambda: ready_index_status(stateful_api.get_index(table_name, index_name)),
        timeout_s=30.0,
        interval_s=0.5,
    )

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "alpha concept overview",
            },
            "doc:b": {
                "title": "beta",
                "body": "beta architecture notes",
            },
            "doc:c": {
                "title": "gamma",
                "body": "gamma implementation details",
            },
        },
        sync_level="write",
    )
    assert batch["inserted"] == 3

    latest_status: dict | None = None

    def current_partial_status() -> dict | None:
        nonlocal latest_status
        stats = rate_limited_openai_embedder.stats()
        if stats["rate_limited_requests"] == 0 or stats["successful_requests"] < 1:
            return None
        latest_status = _index_stats(stateful_api.get_index(table_name, index_name))
        return _retrying_partial_index(stateful_api, table_name, index_name, expected_docs=3)

    partial = wait_until(
        current_partial_status,
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert partial is not None, latest_status
    assert partial["backfill_state"] == "retrying"
    assert partial["backfill_active"] is True
    assert partial["backfill_progress"] < 1.0
    assert partial["replay_applied_sequence"] < partial["replay_target_sequence"]

    enrichment = partial["enrichment_runtime"]
    assert enrichment["error_count"] >= 1
    assert enrichment["retryable_error_count"] >= 1
    assert enrichment["retrying"] is True
    assert enrichment["fatal_error_count"] == 0
    assert enrichment["worker_failed"] is False

    assert ready_index_status({"status": partial}) is None

    rate_limited_openai_embedder.allow_all_requests()

    recovered = wait_until(
        lambda: _ready_index(stateful_api, table_name, index_name, expected_docs=3),
        timeout_s=60.0,
        interval_s=0.5,
    )
    recovery_debug = {
        "index": stateful_api.get_index(table_name, index_name),
        "embedder": rate_limited_openai_embedder.stats(),
    }
    assert recovered is not None, json.dumps(recovery_debug, indent=2, sort_keys=True)
    assert recovered["backfill_state"] == "ready"
    assert recovered["backfill_progress"] == 1.0


def test_stateful_managed_embeddings_provider_pacing_avoids_rate_limit_bursts(
    stateful_api,
    pacing_sensitive_openai_embedder,
):
    table_name = f"stateful_paced_managed_embeddings_{time.time_ns()}"
    index_name = "semantic_idx"

    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    index_payload = {
        "name": index_name,
        "type": "embeddings",
        "field": "body",
        "dimension": 3,
        "embedder": {
            "provider": "openai",
            "model": "text-embedding-3-small",
            "url": pacing_sensitive_openai_embedder.url,
            "requests_per_minute": 6000,
            "burst": 1,
        },
    }

    assert stateful_api.create_index(table_name, index_name, index_payload) == {}
    assert wait_until(
        lambda: ready_index_status(stateful_api.get_index(table_name, index_name)),
        timeout_s=30.0,
        interval_s=0.5,
    )

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "alpha concept overview",
            },
            "doc:b": {
                "title": "beta",
                "body": "beta architecture notes",
            },
            "doc:c": {
                "title": "gamma",
                "body": "gamma implementation details",
            },
        },
        sync_level="write",
    )
    assert batch["inserted"] == 3

    recovered = wait_until(
        lambda: _ready_index(stateful_api, table_name, index_name, expected_docs=3),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert recovered is not None

    stats = pacing_sensitive_openai_embedder.stats()
    assert stats["successful_requests"] >= 3
    assert stats["rate_limited_requests"] == 0


def test_stateful_managed_embeddings_provider_pacing_is_shared_across_tables(
    stateful_api,
    strict_pacing_sensitive_openai_embedder,
):
    first_table = f"stateful_shared_paced_managed_embeddings_a_{time.time_ns()}"
    second_table = f"stateful_shared_paced_managed_embeddings_b_{time.time_ns()}"
    index_name = "semantic_idx"

    for table_name in (first_table, second_table):
        created = stateful_api.create_table(table_name, num_shards=1)
        assert created["name"] == table_name
        index_payload = {
            "name": index_name,
            "type": "embeddings",
            "field": "body",
            "dimension": 3,
            "embedder": {
                "provider": "openai",
                "model": "text-embedding-3-small",
                "url": strict_pacing_sensitive_openai_embedder.url,
                "requests_per_minute": 300,
                "burst": 1,
            },
        }
        assert stateful_api.create_index(table_name, index_name, index_payload) == {}
        assert wait_until(
            lambda table_name=table_name: ready_index_status(stateful_api.get_index(table_name, index_name)),
            timeout_s=30.0,
            interval_s=0.5,
        )

    first_batch = stateful_api.batch_write(
        first_table,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "alpha concept overview",
            }
        },
        sync_level="write",
    )
    assert first_batch["inserted"] == 1

    second_batch = stateful_api.batch_write(
        second_table,
        inserts={
            "doc:b": {
                "title": "beta",
                "body": "beta architecture notes",
            }
        },
        sync_level="write",
    )
    assert second_batch["inserted"] == 1

    first_ready = wait_until(
        lambda: _ready_index(stateful_api, first_table, index_name, expected_docs=1),
        timeout_s=60.0,
        interval_s=0.5,
    )
    def shared_pacing_debug() -> dict:
        def query(table_name: str) -> dict:
            try:
                return stateful_api.query_table(
                    table_name,
                    {
                        "embeddings": {
                            index_name: _pack_f32_le([1.0, 0.0, 0.0]),
                        },
                        "indexes": [index_name],
                        "limit": 3,
                    },
                )
            except Exception as exc:  # pragma: no cover - failure diagnostics only
                return {"error": repr(exc)}

        return {
            "first_index": stateful_api.get_index(first_table, index_name),
            "second_index": stateful_api.get_index(second_table, index_name),
            "first_query": query(first_table),
            "second_query": query(second_table),
            "embedder": strict_pacing_sensitive_openai_embedder.stats(),
        }

    assert first_ready, json.dumps(
        shared_pacing_debug(),
        indent=2,
        sort_keys=True,
    )
    second_ready = wait_until(
        lambda: _ready_index(stateful_api, second_table, index_name, expected_docs=1),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert second_ready, json.dumps(
        shared_pacing_debug(),
        indent=2,
        sort_keys=True,
    )

    stats = strict_pacing_sensitive_openai_embedder.stats()
    assert stats["successful_requests"] >= 2
    assert stats["rate_limited_requests"] == 0


def test_stateful_managed_embeddings_delete_recreate_recovers_after_corrupt_artifact(
    stateful_api,
    openai_embedder,
):
    table_name = f"stateful_corrupt_managed_embeddings_{time.time_ns()}"
    index_name = "semantic_idx"

    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    index_payload = {
        "name": index_name,
        "type": "embeddings",
        "field": "body",
        "dimension": 3,
        "embedder": {
            "provider": "openai",
            "model": "text-embedding-3-small",
            "url": openai_embedder,
        },
    }

    assert stateful_api.create_index(table_name, index_name, index_payload) == {}
    assert wait_until(
        lambda: ready_index_status(stateful_api.get_index(table_name, index_name)),
        timeout_s=30.0,
        interval_s=0.5,
    )

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "alpha concept overview",
            },
            "doc:b": {
                "title": "beta",
                "body": "beta architecture notes",
            },
        },
        sync_level="write",
    )
    assert batch["inserted"] == 2

    ready = wait_until(
        lambda: _ready_index(stateful_api, table_name, index_name, expected_docs=2),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert ready is not None

    alpha_query = stateful_api.query_table(
        table_name,
        {
            "semantic_search": "alpha concept",
            "indexes": [index_name],
            "limit": 2,
        },
    )
    assert _response_hit_ids(alpha_query)[0] == "doc:a"

    stateful_api.corrupt_embedding_artifact(table_name, "doc:a", index_name)

    assert stateful_api.delete_index(table_name, index_name) == {}
    assert (
        wait_until(
            lambda: _index_missing(stateful_api, table_name, index_name),
            timeout_s=30.0,
            interval_s=0.5,
        )
        is not None
    )

    assert stateful_api.create_index(table_name, index_name, index_payload) == {}

    recovered = wait_until(
        lambda: _ready_index(stateful_api, table_name, index_name, expected_docs=2),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert recovered is not None

    recovered_query = stateful_api.query_table(
        table_name,
        {
            "semantic_search": "alpha concept",
            "indexes": [index_name],
            "limit": 2,
        },
    )
    assert _response_hit_ids(recovered_query)[0] == "doc:a"


def test_table_create_table_reserves_full_text_index_names(table_api):
    table_name = f"reserved_full_text_{time.time_ns()}"
    payload = {
        "indexes": {
            "full_text_index_v1": {
                "name": "full_text_index_v1",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            }
        },
    }
    if table_api.backend == "stateful":
        payload["num_shards"] = 1
    else:
        payload["created_at_ns"] = 100

    try:
        table_api.post(
            f"/tables/{table_name}",
            payload,
        )
    except requests.HTTPError as exc:
        assert exc.response is not None
        assert exc.response.status_code == 400
    else:
        raise AssertionError("expected reserved full_text_index name to be rejected")


def test_table_rejects_non_go_full_text_chunk_config(table_api):
    table_name = f"full_text_contract_{time.time_ns()}"

    created = table_api.create_table(table_name)
    assert _table_name(created) == table_name

    try:
        table_api.create_index(
            table_name,
            "full_text_index_v1",
            {
                "name": "full_text_index_v1",
                "type": "full_text",
                "chunk_name": "serverless_chunk_preview",
            },
        )
    except requests.HTTPError as exc:
        assert exc.response is not None
        assert exc.response.status_code == 400
    else:
        raise AssertionError("expected public full-text chunk config to be rejected")


def test_table_create_table_ignores_user_full_text_index_entries(table_api):
    table_name = f"full_text_create_table_{time.time_ns()}"

    payload = {
        "indexes": {
            "search_idx": {
                "name": "search_idx",
                "type": "full_text",
            },
            "embed_idx": {
                "name": "embed_idx",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        },
    }
    if table_api.backend == "stateful":
        payload["num_shards"] = 1
    else:
        payload["created_at_ns"] = 100

    created = table_api.post(
        f"/tables/{table_name}",
        payload,
    )
    if "indexes" not in created:
        created["indexes"] = {
            entry["config"]["name"]: entry["config"]
            for entry in table_api.list_indexes(table_name)
            if "config" in entry and "name" in entry["config"]
        }
    assert _table_name(created) == table_name
    assert "full_text_index_v0" in created["indexes"]
    assert "search_idx" not in created["indexes"]
    assert "embed_idx" in created["indexes"]


def test_table_chunker_full_text_index_routes_template_chunks(table_api, openai_embedder):
    def query_ids(table_name: str) -> list[str]:
        return _response_hit_ids(
            table_api.query_table(
                table_name,
                {
                    "full_text_search": {"field": "body", "match": "routing"},
                    "limit": 5,
                },
            )
        )

    def publish_with_chunker(full_text_enabled: bool) -> tuple[str, dict]:
        table_name = f"template_chunk_full_text_{table_api.backend}_{time.time_ns()}_{'on' if full_text_enabled else 'off'}"
        index_name = "semantic_template_chunked_idx"

        created = table_api.create_table(table_name)
        assert _table_name(created) == table_name

        chunker: dict[str, object] = {
            "provider": "antfly",
            "model": "fixed-bert-tokenizer",
            "store_chunks": False,
            "text": {
                "target_tokens": 4,
                "overlap_tokens": 1,
                "separator": " ",
            },
        }
        if full_text_enabled:
            chunker["full_text_index"] = {}

        assert (
            table_api.create_index(
                table_name,
                index_name,
                {
                    "name": index_name,
                    "type": "embeddings",
                    "template": "{{title}}",
                    "dimension": 3,
                    "embedder": {
                        "provider": "openai",
                        "model": "text-embedding-3-small",
                        "url": openai_embedder,
                    },
                    "chunker": chunker,
                },
            )
            == {}
        )

        batch = table_api.batch_write(
            table_name,
            inserts={
                "doc:a": {
                    "title": "Alpha routing only in template chunks",
                    "body": "body text without the keyword",
                }
            },
            sync_level="full_text",
        )
        assert batch["inserted"] == 1
        if table_api.backend == "serverless":
            assert table_api.publish_table(table_name) is not None

        hits = wait_until(
            lambda: (
                current
                if ((current := query_ids(table_name)) and full_text_enabled)
                else None
            ),
            timeout_s=30.0,
            interval_s=0.5,
        )
        return table_name, {"hits": hits or []}

    without_full_text_name, _ = publish_with_chunker(False)
    assert query_ids(without_full_text_name) == []

    with_full_text_name, with_full_text = publish_with_chunker(True)
    assert with_full_text["hits"]
    assert with_full_text["hits"][0] == "doc:a"
    assert query_ids(with_full_text_name)[0] == "doc:a"


def test_mutable_table_chunker_without_full_text_index_does_not_persist_chunks(stateful_api, openai_embedder):
    table_name = f"chunk_storage_disabled_{time.time_ns()}"
    index_name = "semantic_chunked_idx"

    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        stateful_api.create_index(
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
                "chunker": {
                    "provider": "antfly",
                    "model": "fixed-bert-tokenizer",
                    "store_chunks": False,
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

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha without stored chunks",
                "body": "alpha alpha alpha alpha beta beta beta beta beta beta",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 1

    scan = wait_until(
        lambda: (
            rows
            if (
                (rows := stateful_api.scan_keys(
                    table_name,
                    {
                        "from": "doc:a",
                        "to": "doc:a;",
                        "inclusive_from": True,
                        "fields": ["title", "_chunks"],
                    },
                ))
                and rows[0].get("title") == "Alpha without stored chunks"
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert scan is not None
    chunks = scan[0].get("_chunks", {})
    assert f"{index_name}_chunks" not in chunks


def test_mutable_table_chunker_full_text_index_persists_chunks(stateful_api, openai_embedder):
    table_name = f"chunk_storage_full_text_{time.time_ns()}"
    index_name = "semantic_chunked_idx"

    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        stateful_api.create_index(
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
                "chunker": {
                    "provider": "antfly",
                    "model": "fixed-bert-tokenizer",
                    "store_chunks": False,
                    "full_text_index": {},
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

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha with full text chunks",
                "body": "alpha alpha alpha alpha beta beta beta beta beta beta",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 1

    scan = wait_until(
        lambda: (
            rows
            if (
                (rows := stateful_api.scan_keys(
                    table_name,
                    {
                        "from": "doc:a",
                        "to": "doc:a;",
                        "inclusive_from": True,
                        "fields": ["title", "_chunks"],
                    },
                ))
                and rows[0].get("_chunks", {}).get(f"{index_name}_chunks")
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert scan is not None
    chunks = scan[0]["_chunks"][f"{index_name}_chunks"]
    assert len(chunks) >= 2
    assert chunks[0]["body"].startswith("alpha")
    assert any(chunk["body"].startswith("beta") for chunk in chunks)


def test_serverless_chunker_full_text_index_reports_publication_status(serverless_api, openai_embedder):
    def publish_with_chunker(full_text_enabled: bool) -> tuple[str, dict]:
        table_name = f"serverless_chunk_full_text_{time.time_ns()}_{'on' if full_text_enabled else 'off'}"
        index_name = "semantic_chunked_idx"

        created = serverless_api.ensure_table(table_name, created_at_ns=100)
        assert created["table_name"] == table_name

        chunker: dict[str, object] = {
            "provider": "antfly",
            "model": "fixed-bert-tokenizer",
            "store_chunks": False,
            "text": {
                "target_tokens": 4,
                "overlap_tokens": 1,
                "separator": " ",
            },
        }
        if full_text_enabled:
            chunker["full_text_index"] = {}

        assert (
            serverless_api.create_index(
                table_name,
                index_name,
                {
                    "name": index_name,
                    "type": "embeddings",
                    "field": "body",
                    "template": "{{title}}",
                    "dimension": 3,
                    "embedder": {
                        "provider": "openai",
                        "model": "text-embedding-3-small",
                        "url": openai_embedder,
                    },
                    "chunker": chunker,
                },
            )
            == {}
        )

        serverless_api.ingest_table(
            table_name,
            timestamp_ns=200,
            mutations=[
                upsert(
                    "doc-1",
                    json_doc(
                        title="Alpha chunk routing",
                        body="alpha alpha alpha alpha beta beta beta beta beta beta",
                    ),
                )
            ],
        )

        _maybe_serverless_build(serverless_api, table_name)
        status = wait_until(
            lambda: (
                current
                if (
                    (current := serverless_api.table_build_status(table_name)).get("head_version", 0) >= 1
                    and current.get("published_wal_end_lsn", 0) >= 1
                )
                else None
            ),
            timeout_s=30.0,
            interval_s=0.5,
        )
        assert status is not None
        return table_name, status

    without_full_text_name, without_full_text = publish_with_chunker(False)
    with_full_text_name, with_full_text = publish_with_chunker(True)

    without_entry = _full_text_entry(without_full_text, "full_text_index_v0")
    assert without_entry is not None
    assert without_entry["source_mode"] == "document"
    assert without_entry["chunked_source_count"] == 0

    with_entry = _full_text_entry(with_full_text, "full_text_index_v0")
    assert with_entry is not None
    assert with_entry["source_mode"] == "document_plus_artifact"
    assert with_entry["chunked_source_count"] == 1

    without_detail = serverless_api.get_index(without_full_text_name, "full_text_index_v0")
    assert without_detail["status"]["full_text_source_mode"] == "document"
    assert without_detail["status"]["chunked_source_count"] == 0
    assert without_detail["status"]["chunked_full_text"] is False

    with_detail = serverless_api.get_index(with_full_text_name, "full_text_index_v0")
    assert with_detail["status"]["full_text_source_mode"] == "document_plus_artifact"
    assert with_detail["status"]["chunked_source_count"] == 1
    assert with_detail["status"]["chunked_full_text"] is True


def test_serverless_chunked_dense_index_reports_chunk_embeddings_blocker(serverless_api):
    table_name = f"serverless_chunk_embeddings_blocker_{time.time_ns()}"
    index_name = "semantic_chunked_idx"

    created = serverless_api.ensure_table(table_name, created_at_ns=100)
    assert created["table_name"] == table_name

    assert (
        serverless_api.update_table(
            table_name,
            {
                "policy": {
                    "chunk_embeddings_enabled": True,
                    "chunk_embeddings_publish_min_pending_records": 32,
                }
            },
        )
        == {}
    )
    assert (
        serverless_api.create_index(
            table_name,
            index_name,
            {
                "name": index_name,
                "type": "embeddings",
                "field": "body",
                "dimension": 3,
                "chunker": {
                    "provider": "antfly",
                    "store_chunks": False,
                    "text": {
                        "target_tokens": 4,
                        "separator": " ",
                    },
                },
            },
        )
        == {}
    )

    serverless_api.ingest_table(
        table_name,
        timestamp_ns=200,
        mutations=[
            upsert(
                "doc-1",
                json_doc(
                    body="alpha bravo",
                    chunk_preview=["alpha bravo"],
                    chunk_embeddings=[{"chunk": "alpha bravo", "embedding": [1.0, 0.0, 0.0]}],
                    _enrichment={
                        "chunk_preview": True,
                        "chunk_preview_version": 1,
                        "chunk_embeddings": True,
                        "chunk_embeddings_version": 1,
                    },
                ),
            )
        ],
    )

    built = _maybe_serverless_build(serverless_api, table_name)
    assert built is not None
    assert (
        wait_until(
            lambda: (
                current
                if (current := serverless_api.table_build_status(table_name)).get("head_version", 0) >= 1
                else None
            ),
            timeout_s=30.0,
            interval_s=0.5,
        )
        is not None
    )

    serverless_api.ingest_table(
        table_name,
        timestamp_ns=300,
        mutations=[
            upsert(
                "doc-2",
                json_doc(
                    body="charlie delta echo foxtrot golf",
                    chunk_preview=["charlie delta", "echo foxtrot golf"],
                    _enrichment={
                        "chunk_preview": True,
                        "chunk_preview_version": 1,
                    },
                ),
            )
        ],
    )

    status = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get(
                    "pending_materialization_families", {}
                ).get("chunk_embeddings")
                is True
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert status is not None

    detail = serverless_api.get_index(table_name, index_name)
    assert detail["status"]["materialization_blocked"] is True
    assert detail["status"]["materialization_blocker"] == "chunk_embeddings"


def test_serverless_named_embedding_indexes_report_publication_actions(serverless_api):
    table_name = f"serverless_named_embedding_actions_{time.time_ns()}"

    created = serverless_api.ensure_table(table_name, created_at_ns=100)
    assert created["table_name"] == table_name

    assert (
        serverless_api.create_index(
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
        serverless_api.create_index(
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

    serverless_api.ingest_table(
        table_name,
        timestamp_ns=200,
        mutations=[
            upsert(
                "doc-1",
                json_doc(
                    text="alpha",
                    _embeddings={
                        "semantic_a": [1.0, 0.0, 0.0],
                        "sparse_a": {"alpha": 1.0},
                        "sparse_b": {"beta": 2.0},
                    },
                ),
            )
        ],
    )

    built = _maybe_serverless_build(serverless_api, table_name)
    assert built is not None
    status = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get("head_version", 0) >= 1
                and current.get("published_wal_end_lsn", 0) >= 1
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert status is not None

    assert (
        serverless_api.create_index(
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
        serverless_api.create_index(
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
    assert serverless_api.delete_index(table_name, "semantic_a") == {}

    planned = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get("head_republish_recommended") is True
                and _named_action(current, "vector_index_actions", "semantic_b") == "reuse"
                and _named_action(current, "sparse_index_actions", "sparse_a") == "reuse"
                and _named_action(current, "sparse_index_actions", "sparse_b") == "rebuild"
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    if planned is not None:
        assert _named_action(planned, "vector_index_actions", "semantic_a") == "drop"
        assert planned["artifact_actions"]["dense_vector"] == "reuse"
        assert planned["artifact_actions"]["sparse_vector"] == "rebuild"

    rebuilt = _maybe_serverless_build(serverless_api, table_name)
    assert rebuilt is not None
    target_head_version = rebuilt.get("version") or rebuilt.get("head_version") or 2
    ready = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get("head_version", 0) == target_head_version
                and _named_action(current, "vector_index_actions", "semantic_b") == "reuse"
                and _named_action(current, "sparse_index_actions", "sparse_a") == "reuse"
                and _named_action(current, "sparse_index_actions", "sparse_b") == "reuse"
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert ready is not None

    semantic_b = serverless_api.get_index(table_name, "semantic_b")
    sparse_a = serverless_api.get_index(table_name, "sparse_a")
    sparse_b = serverless_api.get_index(table_name, "sparse_b")
    assert ready_index_status(semantic_b) is not None
    assert ready_index_status(sparse_a) is not None
    assert ready_index_status(sparse_b) is not None


def test_serverless_same_name_dense_index_update_republishes_head(serverless_api):
    table_name = f"serverless_same_name_dense_update_{time.time_ns()}"

    created = serverless_api.ensure_table(table_name, created_at_ns=100)
    assert created["table_name"] == table_name

    assert (
        serverless_api.create_index(
            table_name,
            "semantic_idx",
            {
                "name": "semantic_idx",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )

    serverless_api.ingest_table(
        table_name,
        timestamp_ns=200,
        mutations=[
            upsert(
                "doc-1",
                json_doc(
                    text="alpha",
                    _embeddings={
                        "semantic_idx": [1.0, 0.0, 0.0],
                    },
                ),
            )
        ],
    )

    first_build = _maybe_serverless_build(serverless_api, table_name)
    assert first_build is not None
    first_ready = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get("head_version", 0) >= 1
                and current.get("published_wal_end_lsn", 0) >= 1
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert first_ready is not None
    first_head_version = first_ready["head_version"]
    first_published_wal_end = first_ready["published_wal_end_lsn"]

    assert (
        serverless_api.update_table(
            table_name,
            {
                "indexes": {
                    "full_text_index_v0": {"type": "full_text"},
                    "semantic_idx": {
                        "type": "embeddings",
                        "external": True,
                        "dimension": 3,
                        "distance_metric": "inner_product",
                    },
                }
            },
        )
        == {}
    )

    planned = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get("head_republish_recommended") is True
                and current.get("next_publish_reason") == "head_republish"
                and _named_action(current, "vector_index_actions", "semantic_idx") == "rebuild"
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert planned is not None
    assert planned["artifact_actions"]["dense_vector"] == "rebuild"
    assert planned["published_wal_end_lsn"] == first_published_wal_end

    rebuilt = _maybe_serverless_build(serverless_api, table_name)
    assert rebuilt is not None
    target_head_version = rebuilt.get("version") or rebuilt.get("head_version") or (first_head_version + 1)
    ready = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get("head_version", 0) == target_head_version
                and current.get("published_wal_end_lsn") == first_published_wal_end
                and _named_action(current, "head_vector_index_actions", "semantic_idx") == "rebuild"
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert ready is not None

    detail = serverless_api.get_index(table_name, "semantic_idx")
    assert detail["status"]["head_publication_action"] == "rebuild"
    assert ready_index_status(detail) is not None


def test_serverless_build_status_reports_head_actions_for_text_only_updates(serverless_api):
    table_name = f"serverless_head_actions_{time.time_ns()}"

    created = serverless_api.ensure_table(table_name, created_at_ns=100)
    assert created["table_name"] == table_name

    assert (
        serverless_api.create_index(
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
        serverless_api.create_index(
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

    serverless_api.ingest_table(
        table_name,
        timestamp_ns=200,
        mutations=[
            upsert(
                "doc-1",
                json_doc(
                    text="alpha",
                    _embeddings={
                        "semantic_a": [1.0, 0.0, 0.0],
                        "sparse_a": {"alpha": 1.0},
                    },
                ),
            )
        ],
    )
    first_build = _maybe_serverless_build(serverless_api, table_name)
    assert first_build is not None
    first_ready = wait_until(
        lambda: (
            current
            if (current := serverless_api.table_build_status(table_name)).get("head_version", 0) >= 1
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert first_ready is not None

    serverless_api.ingest_table(
        table_name,
        timestamp_ns=300,
        mutations=[
            upsert(
                "doc-1",
                json_doc(
                    text="bravo",
                    _embeddings={
                        "semantic_a": [1.0, 0.0, 0.0],
                        "sparse_a": {"alpha": 1.0},
                    },
                ),
            )
        ],
    )
    second_build = _maybe_serverless_build(serverless_api, table_name)
    assert second_build is not None
    target_head_version = second_build.get("version") or second_build.get("head_version") or 2

    status = wait_until(
        lambda: (
            current
            if (current := serverless_api.table_build_status(table_name)).get("head_version", 0) == target_head_version
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert status is not None
    assert status["head_artifact_actions"]["document_segment"] == "rebuild"
    assert status["head_artifact_actions"]["full_text"] == "rebuild"
    assert status["head_artifact_actions"]["dense_vector"] == "reuse"
    assert status["head_artifact_actions"]["sparse_vector"] == "reuse"
    assert _full_text_action(status, "head_full_text_index_actions", "full_text_index_v0") == "rebuild"
    assert _named_action(status, "head_vector_index_actions", "semantic_a") == "reuse"
    assert _named_action(status, "head_sparse_index_actions", "sparse_a") == "reuse"


def test_serverless_schema_migration_republishes_versioned_full_text_indexes(serverless_api):
    table_name = f"serverless_full_text_version_transition_{time.time_ns()}"
    initial_schema = {
        "version": 1,
        "document_schemas": {
            "doc": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "body": {"type": "string"},
                    },
                }
            }
        },
    }
    migrated_schema = {
        "version": 2,
        "document_schemas": {
            "doc": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "body": {"type": "string"},
                        "title": {"type": "string"},
                    },
                }
            }
        },
    }

    created = serverless_api.put(
        f"/tables/{table_name}",
        {
            "created_at_ns": 100,
            "schema": initial_schema,
        },
    )
    assert created["table_name"] == table_name

    serverless_api.ingest_table(
        table_name,
        timestamp_ns=200,
        mutations=[
            upsert(
                "doc-1",
                json_doc(
                    body="alpha",
                ),
            )
        ],
    )

    first_build = _maybe_serverless_build(serverless_api, table_name)
    assert first_build is not None
    first_ready = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get("head_version", 0) >= 1
                and current.get("published_wal_end_lsn", 0) >= 1
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert first_ready is not None
    first_head_version = first_ready["head_version"]
    first_published_wal_end = first_ready["published_wal_end_lsn"]

    assert (
        serverless_api.update_table(
            table_name,
            {
                "schema": migrated_schema,
                "read_schema": initial_schema,
                "indexes": {
                    "full_text_index_v0": {"type": "full_text"},
                    "full_text_index_v1": {"type": "full_text"},
                },
            },
        )
        == {}
    )

    planned = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get("head_republish_recommended") is True
                and current.get("next_publish_reason") == "head_republish"
                and _full_text_action(current, "full_text_index_actions", "full_text_index_v0") == "reuse"
                and _full_text_action(current, "full_text_index_actions", "full_text_index_v1") == "rebuild"
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert planned is not None
    assert planned["artifact_actions"]["full_text"] == "rebuild"
    assert planned["published_wal_end_lsn"] == first_published_wal_end

    rebuilt = _maybe_serverless_build(serverless_api, table_name)
    assert rebuilt is not None
    target_head_version = rebuilt.get("version") or rebuilt.get("head_version") or (first_head_version + 1)
    ready = wait_until(
        lambda: (
            current
            if (
                (current := serverless_api.table_build_status(table_name)).get("head_version", 0) == target_head_version
                and current.get("published_wal_end_lsn") == first_published_wal_end
                and _full_text_action(current, "head_full_text_index_actions", "full_text_index_v0") == "reuse"
                and _full_text_action(current, "head_full_text_index_actions", "full_text_index_v1") == "rebuild"
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert ready is not None

    active_index = serverless_api.get_index(table_name, "full_text_index_v0")
    next_index = serverless_api.get_index(table_name, "full_text_index_v1")
    assert active_index["status"]["head_publication_action"] == "reuse"
    assert next_index["status"]["head_publication_action"] == "rebuild"
    assert ready_index_status(active_index) is not None
    assert ready_index_status(next_index) is not None
