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

import base64
import json
import time
from pathlib import Path
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


def test_pdf_ocr_inline_url_paged_chunks_and_inline_jpeg_e2e(
    stateful_api, pdf_ocr_e2e_server
):
    """Raw inline/URL PDFs render every page, OCR, chunk, and index server-side."""

    table_name = f"document_pdf_ocr_{time.time_ns()}"
    index_config = _document_units_index_config()
    extraction_config = index_config["artifact"]["producer_json"]["config"]
    extraction_config["ocr"] = {
        "enabled": True,
        "mode": "always",
        "render_dpi": 150,
        "max_rendered_pixels": 4_000_000,
        "config": {
            "provider": "antfly",
            "model": "e2e-reader",
            "api_url": pdf_ocr_e2e_server.reader_api_url,
        },
    }
    asset_enrichment = dict(index_config["artifact"])
    asset_enrichment["producer_json"] = json.dumps(asset_enrichment["producer_json"])
    created = stateful_api.create_table(table_name, num_shards=1)
    assert created.get("name") == table_name or created.get("table_name") == table_name
    assert (
        stateful_api.put(
            f"{_table_artifact_path(table_name, DOCUMENT_UNITS_ARTIFACT)}/enrichment",
            asset_enrichment,
        )
        == {}
    )
    assert (
        wait_until(
            lambda: _table_has_artifact_enrichment(
                stateful_api, table_name, DOCUMENT_UNITS_ARTIFACT, "asset"
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
                "source_artifact_name": DOCUMENT_UNITS_ARTIFACT,
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
    zig_root = Path(__file__).resolve().parents[2]
    pdf_bytes = (zig_root / "lib/pdf/testdata/two_page_text_fixture.pdf").read_bytes()
    scanned_table_pdf = (
        zig_root / "lib/pdf/testdata/scanned_table_fixture.pdf"
    ).read_bytes()
    jpeg_bytes = (zig_root / "testdata/image/jpeg/baseline/white-2x1.jpg").read_bytes()
    inline_pdf = "data:application/pdf;base64," + base64.b64encode(pdf_bytes).decode(
        "ascii"
    )
    inline_jpeg = "data:image/jpeg;base64," + base64.b64encode(jpeg_bytes).decode(
        "ascii"
    )
    inline_scanned_table = "data:application/pdf;base64," + base64.b64encode(
        scanned_table_pdf
    ).decode("ascii")
    docs = {
        "pdf-inline": {
            "filename": "inline-two-page.pdf",
            "mime_type": "application/pdf",
            "version": "1",
            "url": inline_pdf,
        },
        "pdf-url": {
            "filename": "url-two-page.pdf",
            "mime_type": "application/pdf",
            "version": "1",
            "url": pdf_ocr_e2e_server.pdf_url,
        },
        "pdf-scanned-table": {
            "filename": "officeqa-scanned-table.pdf",
            "mime_type": "application/pdf",
            "version": "1",
            "url": inline_scanned_table,
        },
        "jpeg-inline": {
            "filename": "inline-caption.jpg",
            "mime_type": "image/jpeg",
            "version": "1",
            "url": inline_jpeg,
        },
    }
    batch = stateful_api.batch_write(
        table_name,
        inserts=docs,
        sync_level="full_index",
    )
    assert batch["inserted"] == len(docs)

    manifests: dict[str, dict] = {}
    for doc_key in docs:
        manifest = wait_until(
            lambda doc_key=doc_key: (
                current
                if (
                    (current := _manifest_ready(stateful_api, table_name, doc_key))
                    is not None
                    and current.get("ocr_failed_count", 0) == 0
                    and current.get("chunk_count", 0)
                    >= (
                        4
                        if doc_key in {"pdf-inline", "pdf-url"}
                        else 2
                        if doc_key == "pdf-scanned-table"
                        else 1
                    )
                )
                else None
            ),
            timeout_s=90.0,
            interval_s=0.5,
        )
        debug_manifest = _manifest_ready(stateful_api, table_name, doc_key)
        reader_stats = pdf_ocr_e2e_server.stats()
        table_enrichments = stateful_api.get_table(table_name).get(
            "artifact_enrichments", []
        )
        configured_asset = next(
            (
                enrichment
                for enrichment in table_enrichments
                if enrichment.get("name") == DOCUMENT_UNITS_ARTIFACT
            ),
            {},
        )
        debug = {
            "document": doc_key,
            "unit_count": (debug_manifest or {}).get("unit_count"),
            "chunk_count": (debug_manifest or {}).get("chunk_count"),
            "ocr_attempted_count": (debug_manifest or {}).get("ocr_attempted_count"),
            "ocr_selected_count": (debug_manifest or {}).get("ocr_selected_count"),
            "ocr_failed_count": (debug_manifest or {}).get("ocr_failed_count"),
            "failed_pages": (debug_manifest or {}).get("ocr_failed_page_numbers"),
            "reader_png_requests": reader_stats["png_requests"],
            "reader_jpeg_requests": reader_stats["jpeg_requests"],
            "producer_json": configured_asset.get("producer_json"),
        }
        assert manifest is not None, json.dumps(debug, sort_keys=True)
        manifests[doc_key] = manifest

    for doc_key in ("pdf-inline", "pdf-url"):
        manifest = manifests[doc_key]
        assert manifest["content_type"] == "application/pdf"
        assert manifest["route_type"] == "pdf"
        assert manifest["unit_count"] == 2
        assert manifest["ocr_attempted_count"] == 2
        assert manifest["ocr_selected_count"] == 2
        assert manifest["ocr_retained_embedded_count"] == 0
        assert manifest["ocr_failed_count"] == 0
        assert manifest["ocr_failed_page_numbers"] == []
        assert manifest["chunk_count"] >= 4
    assert manifests["pdf-inline"]["source_url"].startswith("data:application/pdf")
    assert manifests["pdf-url"]["source_url"] == pdf_ocr_e2e_server.pdf_url

    scanned_manifest = manifests["pdf-scanned-table"]
    assert scanned_manifest["content_type"] == "application/pdf"
    assert scanned_manifest["route_type"] == "pdf"
    assert scanned_manifest["unit_count"] == 1
    assert scanned_manifest["ocr_attempted_count"] == 1
    assert scanned_manifest["ocr_selected_count"] == 1
    assert scanned_manifest["ocr_failed_count"] == 0
    assert scanned_manifest["chunk_count"] >= 2

    jpeg_manifest = manifests["jpeg-inline"]
    assert jpeg_manifest["content_type"] == "image/jpeg"
    assert jpeg_manifest["route_type"] == "image"
    assert jpeg_manifest["unit_count"] == 1
    assert jpeg_manifest["ocr_attempted_count"] == 1
    assert jpeg_manifest["ocr_selected_count"] == 1
    assert jpeg_manifest["ocr_failed_count"] == 0

    for term, expected_ids in (
        ("alpha ledger", {"pdf-inline", "pdf-url"}),
        ("beta invoice", {"pdf-inline", "pdf-url"}),
        ("OfficeQA scanned table", {"pdf-scanned-table"}),
        ("Inline JPEG caption", {"jpeg-inline"}),
    ):
        result = wait_until(
            lambda term=term, expected_ids=expected_ids: (
                response
                if expected_ids.issubset(
                    set(
                        _query_hit_ids(
                            response := stateful_api.query_table(
                                table_name,
                                {
                                    "full_text_search": {
                                        "field": "text",
                                        "match": term,
                                    },
                                    "limit": 10,
                                },
                            )
                        )
                    )
                )
                else None
            ),
            timeout_s=60.0,
            interval_s=0.5,
        )
        assert result is not None, {"term": term, "manifests": manifests}

    reader_stats = pdf_ocr_e2e_server.stats()
    assert reader_stats["unique_pngs"] == 3, reader_stats
    assert reader_stats["png_requests"] >= 5, reader_stats
    assert reader_stats["jpeg_requests"] >= 1, reader_stats
    requests = reader_stats["requests"]
    assert any("Render tables as Markdown" in request["prompt"] for request in requests)
    png_hashes = {
        image["sha256"]
        for request in requests
        for image in request["images"]
        if image["kind"] == "png"
    }
    assert len(png_hashes) == 3


def test_artifact_backed_embedding_table_provisions_atomically(
    stateful_api, openai_embedder
):
    """Cross-index artifact dependencies must be valid during create-table."""

    table_name = f"artifact_backed_embedding_create_{time.time_ns()}"
    created = stateful_api.post(
        f"/tables/{table_name}",
        {
            "num_shards": 1,
            "indexes": {
                "document_units": _document_units_index_config(),
                "document_text": {
                    "type": "full_text",
                    "field": "text",
                    "artifact_name": "document_chunks_v1",
                    "enrichments": [
                        {
                            "name": "document_units_v1",
                            "kind": "asset",
                            "field": "url",
                            "content_type": "application/json",
                            "producer_json": json.dumps(
                                _document_units_index_config()["artifact"][
                                    "producer_json"
                                ],
                                separators=(",", ":"),
                            ),
                        },
                        {
                            "name": "document_chunks_v1",
                            "kind": "chunk",
                            "field": "text",
                            "source_artifact_name": "document_units_v1",
                            "chunk_size": 256,
                            "chunk_overlap": 0,
                            "full_text_index": True,
                        },
                    ],
                },
                "document_vectors": {
                    "name": "document_vectors",
                    "type": "embeddings",
                    "field": "embedding",
                    "dimension": 3,
                    "distance_metric": "cosine",
                    "embedding_name": "document_chunk_dense_v1",
                    "source_artifact_name": "document_chunks_v1",
                    "embedder": {
                        "provider": "openai",
                        "model": "text-embedding-3-small",
                        "url": openai_embedder,
                        "dimensions": 3,
                    },
                    "enrichments": [
                        {
                            "name": "document_chunk_dense_v1",
                            "kind": "embedding",
                            "field": "text",
                            "source_artifact_name": "document_chunks_v1",
                            "expected_dims": 3,
                        }
                    ],
                },
            },
        },
    )
    assert created.get("name") == table_name or created.get("table_name") == table_name

    assert (
        stateful_api.get_index(table_name, "document_text")["config"]["type"]
        == "full_text"
    )
    assert (
        stateful_api.get_index(table_name, "document_vectors")["config"]["type"]
        == "embeddings"
    )
    table_status = stateful_api.get_table(table_name)
    assert all(
        isinstance(enrichment, dict)
        for index_name in ("document_text", "document_vectors")
        for enrichment in table_status["indexes"][index_name]["enrichments"]
    )

    doc_key = "atomic-doc"
    batch = stateful_api.batch_write(
        table_name,
        inserts={
            doc_key: {
                "filename": "atomic.txt",
                "mime_type": "text/plain",
                "version": "1",
                "url": "data:text/plain;base64,YXRvbWljIHF1YWxpZmljYXRpb24gZ2FtbWE=",
            }
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 1
    assert (
        wait_until(
            lambda: _manifest_ready(stateful_api, table_name, doc_key),
            timeout_s=60.0,
            interval_s=0.5,
        )
        is not None
    )
    assert (
        wait_until(
            lambda: (
                response
                if doc_key
                in _query_hit_ids(
                    response := stateful_api.query_table(
                        table_name,
                        {
                            "full_text_search": {
                                "field": "text",
                                "match": "qualification",
                            },
                            "limit": 5,
                        },
                    )
                )
                else None
            ),
            timeout_s=60.0,
            interval_s=0.5,
        )
        is not None
    )
    assert (
        wait_until(
            lambda: (
                response
                if doc_key
                in _query_hit_ids(
                    response := stateful_api.query_table(
                        table_name,
                        {
                            "semantic_search": "atomic qualification",
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
        is not None
    )


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
