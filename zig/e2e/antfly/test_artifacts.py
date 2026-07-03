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

"""E2E coverage for derived document artifact APIs."""

from __future__ import annotations

import json
import time
from urllib.parse import quote

from helpers import wait_until

DOCUMENT_UNITS_ARTIFACT = "document_units_v1"


def _document_artifact_path(table_name: str, doc_key: str, artifact_name: str) -> str:
    return (
        f"/tables/{table_name}/documents/{quote(doc_key, safe='')}"
        f"/artifacts/{quote(artifact_name, safe='')}"
    )


def _artifact_list_path(table_name: str, doc_key: str) -> str:
    return f"/tables/{table_name}/documents/{quote(doc_key, safe='')}/artifacts"


def _table_artifact_path(table_name: str, artifact_name: str) -> str:
    return f"/tables/{table_name}/artifacts/{quote(artifact_name, safe='')}"


def _query_hit_ids(result: dict) -> list[str]:
    responses = result.get("responses", [])
    if not responses:
        return []
    hits = responses[0].get("hits", {}).get("hits", [])
    return [hit.get("_id") for hit in hits]


def _first_query_hit_id(result: dict) -> str | None:
    ids = _query_hit_ids(result)
    return ids[0] if ids else None


def _document_units_index_config() -> dict:
    return {
        "type": "graph",
        "source": {
            "kind": "artifact",
            "artifact": DOCUMENT_UNITS_ARTIFACT,
            "path": "$.edges[*]",
            "format": "extraction_relation",
        },
        "artifact": {
            "name": DOCUMENT_UNITS_ARTIFACT,
            "kind": "asset",
            "field": "url",
            "content_type": "application/json",
            "producer_json": {
                "type": "document_extraction",
                "config": {
                    "source": {
                        "filename_field": "filename",
                        "content_type_field": "mime_type",
                        "version_field": "version",
                    }
                },
            },
        },
        "edge_types": [{"name": "mentions"}],
    }


def _manifest_ready(api, table_name: str, doc_key: str) -> dict | None:
    try:
        manifest = api.get(
            f"{_document_artifact_path(table_name, doc_key, DOCUMENT_UNITS_ARTIFACT)}?detail=raw"
        )
    except Exception:
        return None
    if manifest.get("artifact_name") != DOCUMENT_UNITS_ARTIFACT:
        return None
    if manifest.get("unit_count", 0) < 1:
        return None
    if manifest.get("merge_status") != "converged":
        return None
    return manifest


def _table_has_artifact_enrichment(
    api, table_name: str, artifact_name: str, kind: str
) -> dict | None:
    try:
        table = api.get_table(table_name)
    except Exception:
        return None
    for enrichment in table.get("artifact_enrichments", []):
        if enrichment.get("name") == artifact_name and enrichment.get("kind") == kind:
            return table
    return None


def test_document_artifact_manifest_and_reprocess_job_e2e(stateful_api):
    table_name = f"document_artifacts_{time.time_ns()}"
    created = stateful_api.post(
        f"/tables/{table_name}",
        {
            "num_shards": 1,
            "indexes": {
                "document_units_graph": _document_units_index_config(),
            },
        },
    )
    assert created.get("name") == table_name or created.get("table_name") == table_name

    first_doc = "doc:a/with/slash"
    second_doc = "doc:b"
    batch = stateful_api.batch_write(
        table_name,
        inserts={
            first_doc: {
                "filename": "alpha.txt",
                "mime_type": "text/plain",
                "version": "1",
                "url": "data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==",
            },
            second_doc: {
                "filename": "delta.txt",
                "mime_type": "text/plain",
                "version": "1",
                "url": "data:text/plain;base64,ZGVsdGEgZXBzaWxvbg==",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    first_manifest = wait_until(
        lambda: _manifest_ready(stateful_api, table_name, first_doc),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert first_manifest is not None
    assert first_manifest["document_id"] == first_doc
    assert first_manifest["artifact_name"] == DOCUMENT_UNITS_ARTIFACT
    assert first_manifest["content_type"] == "text/plain"
    assert first_manifest["route_type"] == "text"
    assert first_manifest["unit_count"] == 1
    assert first_manifest["child_range_count"] >= 1
    assert first_manifest["source_url"].startswith("data:text/plain")
    assert len(first_manifest["source_fingerprint"]) == 64
    assert first_manifest["manifest_json"] is not None
    assert first_manifest["state_json"] is not None
    assert "document_extraction_state_v1" in first_manifest["state_json"]

    artifact_list = stateful_api.get(
        f"{_artifact_list_path(table_name, first_doc)}?detail=raw"
    )
    assert artifact_list["document_id"] == first_doc
    artifact_names = {artifact["artifact_name"] for artifact in artifact_list["artifacts"]}
    assert DOCUMENT_UNITS_ARTIFACT in artifact_names

    lookup = stateful_api.lookup_key(table_name, first_doc)
    assert lookup.get("filename") == "alpha.txt"
    assert lookup.get("version") == "1"

    reprocess = stateful_api.post(
        f"{_document_artifact_path(table_name, first_doc, DOCUMENT_UNITS_ARTIFACT)}/reprocess",
        {},
    )
    assert reprocess["reprocess"] == "triggered"

    reprocessed_manifest = wait_until(
        lambda: (
            current
            if (
                (current := _manifest_ready(stateful_api, table_name, first_doc)) is not None
                and current.get("generation", 0) > first_manifest["generation"]
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert reprocessed_manifest is not None
    assert reprocessed_manifest["generation"] > first_manifest["generation"]

    started = stateful_api.post(
        f"{_table_artifact_path(table_name, DOCUMENT_UNITS_ARTIFACT)}/reprocess-jobs",
        {
            "limit": 1,
            "advance": False,
        },
    )
    assert started["phase"] == "queued"
    assert started["artifact_name"] == DOCUMENT_UNITS_ARTIFACT
    assert started["table_name"] == table_name
    assert started["limit"] == 1

    job_id = str(started["job_id"])
    current = started
    for _ in range(6):
        current = stateful_api.post(
            f"{_table_artifact_path(table_name, DOCUMENT_UNITS_ARTIFACT)}/reprocess-jobs/{job_id}/advance",
            {},
        )
        assert current["job_id"] == started["job_id"]
        assert current["scanned"] >= started["scanned"]
        if current["phase"] == "succeeded":
            break
        assert current["phase"] in {"queued", "running"}
        assert current["reprocess_status"] == "in_progress"
    assert current["phase"] == "succeeded"
    assert current["reprocess_status"] == "complete"
    assert current["scanned"] >= 2
    assert current["reprocessed"] >= 2
    assert current["failed"] == 0

    polled = stateful_api.get(
        f"{_table_artifact_path(table_name, DOCUMENT_UNITS_ARTIFACT)}/reprocess-jobs/{job_id}"
    )
    assert polled["phase"] == "succeeded"
    assert polled["reprocess_status"] == "complete"
    assert polled["scanned"] == current["scanned"]

    terminal_advance = stateful_api.post(
        f"{_table_artifact_path(table_name, DOCUMENT_UNITS_ARTIFACT)}/reprocess-jobs/{job_id}/advance",
        {},
    )
    assert terminal_advance["phase"] == "succeeded"
    assert terminal_advance["scanned"] == current["scanned"]


def test_artifact_backed_chunk_embeddings_are_semantic_searchable(stateful_api, openai_embedder):
    table_name = f"artifact_backed_chunk_embeddings_{time.time_ns()}"
    created = stateful_api.create_table(table_name, num_shards=1)
    assert created.get("name") == table_name or created.get("table_name") == table_name

    assert (
        stateful_api.put(
            f"{_table_artifact_path(table_name, 'document_units_v1')}/enrichment",
            {
                "kind": "asset",
                "field": "url",
                "content_type": "application/json",
                "producer_json": json.dumps(
                    {
                        "type": "document_extraction",
                        "config": {
                            "source": {
                                "filename_field": "filename",
                                "content_type_field": "mime_type",
                                "version_field": "version",
                            }
                        },
                    }
                ),
            },
        )
        == {}
    )
    assert (
        wait_until(
            lambda: _table_has_artifact_enrichment(
                stateful_api, table_name, "document_units_v1", "asset"
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        is not None
    )
    assert (
        stateful_api.put(
            f"{_table_artifact_path(table_name, 'document_chunks_v1')}/enrichment",
            {
                "kind": "chunk",
                "source_artifact_name": "document_units_v1",
                "field": "text",
                "chunk_size": 256,
                "chunk_overlap": 0,
                "full_text_index": True,
            },
        )
        == {}
    )
    assert (
        wait_until(
            lambda: _table_has_artifact_enrichment(
                stateful_api, table_name, "document_chunks_v1", "chunk"
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        is not None
    )
    assert (
        stateful_api.put(
            f"{_table_artifact_path(table_name, 'document_chunk_dense_v1')}/enrichment",
            {
                "kind": "embedding",
                "source_artifact_name": "document_chunks_v1",
                "field": "text",
                "expected_dims": 3,
            },
        )
        == {}
    )
    assert (
        wait_until(
            lambda: _table_has_artifact_enrichment(
                stateful_api, table_name, "document_chunk_dense_v1", "embedding"
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        is not None
    )
    assert (
        stateful_api.create_index(
            table_name,
            "document_vectors",
            {
                "name": "document_vectors",
                "type": "embeddings",
                "field": "embedding",
                "dimension": 3,
                "source_artifact_name": "document_chunks_v1",
                "embedding_name": "document_chunk_dense_v1",
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": openai_embedder,
                },
            },
        )
        == {}
    )
    index_detail = stateful_api.get_index(table_name, "document_vectors")
    assert index_detail["config"]["name"] == "document_vectors"
    assert index_detail["config"]["type"] == "embeddings"

    doc_key = "doc-a"
    batch = stateful_api.batch_write(
        table_name,
        inserts={
            doc_key: {
                "filename": "alpha.txt",
                "mime_type": "text/plain",
                "version": "1",
                "url": "data:text/plain;base64,YWxwaGEgYm9keSBnYW1tYSByZXRyaWV2YWw=",
                "text": "source document decoy text that must not feed chunk embeddings",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 1

    manifest = wait_until(
        lambda: _manifest_ready(stateful_api, table_name, doc_key),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert manifest is not None

    full_text = wait_until(
        lambda: (
            response
            if doc_key
            in _query_hit_ids(
                response := stateful_api.query_table(
                    table_name,
                    {
                        "full_text_search": {"field": "text", "match": "gamma"},
                        "limit": 5,
                    },
                )
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert full_text is not None, {
        "manifest": manifest,
        "index": stateful_api.get_index(table_name, "document_vectors"),
    }

    semantic = wait_until(
        lambda: (
            response
            if doc_key
            in _query_hit_ids(
                response := stateful_api.query_table(
                    table_name,
                    {
                        "semantic_search": "alpha concept",
                        "indexes": ["document_vectors"],
                        "limit": 5,
                    },
                )
            )
            else None
        ),
        timeout_s=120.0,
        interval_s=1.0,
    )
    assert semantic is not None, {
        "manifest": manifest,
        "full_text": full_text,
        "index": stateful_api.get_index(table_name, "document_vectors"),
        "semantic_attempt": stateful_api.query_table(
            table_name,
            {
                "semantic_search": "alpha concept",
                "indexes": ["document_vectors"],
                "limit": 5,
            },
        ),
    }

    updated = stateful_api.batch_write(
        table_name,
        inserts={
            doc_key: {
                "filename": "beta.txt",
                "mime_type": "text/plain",
                "version": "2",
                "url": "data:text/plain;base64,YmV0YSBhcmNoaXRlY3R1cmUgZGVsdGE=",
                "text": "alpha concept source decoy for the updated document",
            },
            "doc-b": {
                "filename": "alpha-control.txt",
                "mime_type": "text/plain",
                "version": "1",
                "url": "data:text/plain;base64,YWxwaGEgY29uY2VwdCBjb250cm9s",
                "text": "beta architecture source decoy for the control document",
            },
        },
        sync_level="full_index",
    )
    assert updated["inserted"] >= 1

    reprocess = stateful_api.post(
        f"{_document_artifact_path(table_name, doc_key, 'document_units_v1')}/reprocess",
        {},
    )
    assert reprocess["reprocess"] == "triggered"

    refreshed_manifest = wait_until(
        lambda: (
            current
            if (
                (current := _manifest_ready(stateful_api, table_name, doc_key)) is not None
                and current.get("generation", 0) > manifest.get("generation", 0)
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert refreshed_manifest is not None
    assert (
        wait_until(
            lambda: _manifest_ready(stateful_api, table_name, "doc-b"),
            timeout_s=60.0,
            interval_s=0.5,
        )
        is not None
    )

    refreshed_full_text = wait_until(
        lambda: (
            response
            if (
                doc_key
                in _query_hit_ids(
                    response := stateful_api.query_table(
                        table_name,
                        {
                            "full_text_search": {"field": "text", "match": "delta"},
                            "limit": 5,
                        },
                    )
                )
                and doc_key
                not in _query_hit_ids(
                    stateful_api.query_table(
                        table_name,
                        {
                            "full_text_search": {"field": "text", "match": "gamma"},
                            "limit": 5,
                        },
                    )
                )
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert refreshed_full_text is not None, {
        "manifest": refreshed_manifest,
        "gamma_attempt": stateful_api.query_table(
            table_name,
            {
                "full_text_search": {"field": "text", "match": "gamma"},
                "limit": 5,
            },
        ),
    }

    beta_semantic = wait_until(
        lambda: (
            response
            if _first_query_hit_id(
                response := stateful_api.query_table(
                    table_name,
                    {
                        "semantic_search": "beta architecture",
                        "indexes": ["document_vectors"],
                        "limit": 2,
                    },
                )
            )
            == doc_key
            else None
        ),
        timeout_s=120.0,
        interval_s=1.0,
    )
    assert beta_semantic is not None, {
        "manifest": refreshed_manifest,
        "semantic_attempt": stateful_api.query_table(
            table_name,
            {
                "semantic_search": "beta architecture",
                "indexes": ["document_vectors"],
                "limit": 2,
            },
        ),
    }

    final_index = wait_until(
        lambda: (
            index
            if (
                (index := stateful_api.get_index(table_name, "document_vectors"))
                .get("status", {})
                .get("total_indexed")
                == 2
                and index.get("status", {}).get("query_visible_doc_count") == 2
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert final_index is not None, json.dumps(
        stateful_api.get_index(table_name, "document_vectors"),
        indent=2,
        sort_keys=True,
    )

    alpha_semantic = wait_until(
        lambda: (
            response
            if "doc-b"
            in _query_hit_ids(
                response := stateful_api.query_table(
                    table_name,
                    {
                        "semantic_search": "alpha concept",
                        "indexes": ["document_vectors"],
                        "limit": 2,
                    },
                )
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=1.0,
    )
    assert alpha_semantic is not None, json.dumps(
        {
            "manifest": refreshed_manifest,
            "beta_ids": _query_hit_ids(beta_semantic),
            "index": final_index,
            "alpha_full_text": _query_hit_ids(
                stateful_api.query_table(
                    table_name,
                    {
                        "full_text_search": {"field": "text", "match": "alpha"},
                        "limit": 5,
                    },
                )
            ),
            "alpha_attempt": stateful_api.query_table(
                table_name,
                {
                    "semantic_search": "alpha concept",
                    "indexes": ["document_vectors"],
                    "limit": 2,
                },
            ),
        },
        indent=2,
        sort_keys=True,
    )
