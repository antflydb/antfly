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
import os
import time
from pathlib import Path
from urllib.parse import quote

import pytest
import requests

from helpers import assert_created_index, wait_until

DOCUMENT_UNITS_ARTIFACT = "document_units_v1"
DEFAULT_FULL_TEXT_INDEX = "full_text_index_v0"


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


def _document_units_asset_enrichment() -> dict:
    """Return the table artifact-enrichment request shape."""

    return {
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
    }


def _document_units_index_config() -> dict:
    artifact = _document_units_asset_enrichment()
    source_field = artifact.pop("field")
    artifact["name"] = DOCUMENT_UNITS_ARTIFACT
    artifact["source"] = {"type": "field", "value": source_field}
    return {
        "type": "graph",
        "source": {
            "kind": "artifact",
            "artifact": DOCUMENT_UNITS_ARTIFACT,
            "path": "$.edges[*]",
            "format": "extraction_relation",
        },
        "artifact": artifact,
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


def _provision_artifact_full_text(api, table_name: str, *, chunk_size: int) -> dict:
    """Use the public enrichment UX to provision exactly one default text index."""

    asset_enrichment = _document_units_asset_enrichment()
    asset_enrichment["producer_json"] = json.dumps(
        asset_enrichment["producer_json"], separators=(",", ":")
    )
    assert (
        api.put(
            f"{_table_artifact_path(table_name, DOCUMENT_UNITS_ARTIFACT)}/enrichment",
            asset_enrichment,
        )
        == {}
    )
    assert (
        wait_until(
            lambda: _table_has_artifact_enrichment(
                api, table_name, DOCUMENT_UNITS_ARTIFACT, "asset"
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        is not None
    )

    assert (
        api.put(
            f"{_table_artifact_path(table_name, 'document_chunks_v1')}/enrichment",
            {
                "kind": "chunk",
                "source_artifact_name": DOCUMENT_UNITS_ARTIFACT,
                "field": "text",
                "chunk_size": chunk_size,
                "chunk_overlap": 0,
                "full_text_index": True,
            },
        )
        == {}
    )
    assert (
        wait_until(
            lambda: _table_has_artifact_enrichment(
                api, table_name, "document_chunks_v1", "chunk"
            ),
            timeout_s=30.0,
            interval_s=0.25,
        )
        is not None
    )

    def default_index_ready() -> dict | None:
        try:
            current = api.get_index(table_name, DEFAULT_FULL_TEXT_INDEX)
        except Exception:
            return None
        if current.get("config", {}).get("type") != "full_text":
            return None
        return current

    detail = wait_until(
        default_index_ready,
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert detail is not None
    return detail


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
    artifact_names = {
        artifact["artifact_name"] for artifact in artifact_list["artifacts"]
    }
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
                (current := _manifest_ready(stateful_api, table_name, first_doc))
                is not None
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
    asset_enrichment = _document_units_asset_enrichment()
    extraction_config = asset_enrichment["producer_json"]["config"]
    extraction_config["ocr"] = {
        "enabled": True,
        "mode": "always",
        "render_dpi": 150,
        "max_rendered_pixels": 4_000_000,
        "max_rendered_dimension": 2048,
        "config": {
            "provider": "antfly",
            "model": "antflydb/Florence-2-base",
            "api_url": pdf_ocr_e2e_server.reader_api_url,
        },
    }
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
                        else 2 if doc_key == "pdf-scanned-table" else 1
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
    png_requests = [
        request
        for request in requests
        if any(image["kind"] == "png" for image in request["images"])
    ]
    assert png_requests
    assert any(
        sum(image["kind"] == "png" for image in request["images"]) > 1
        for request in png_requests
    ), reader_stats
    assert all(request["prompt"] == "<OCR>" for request in png_requests)
    assert all(
        "Render tables as Markdown" not in request["prompt"] for request in png_requests
    )
    png_hashes = {
        image["sha256"]
        for request in requests
        for image in request["images"]
        if image["kind"] == "png"
    }
    assert len(png_hashes) == 3


def test_pdf_auto_ocr_only_renders_pages_without_usable_embedded_text_e2e(
    stateful_api, pdf_ocr_e2e_server
):
    """Auto OCR keeps Form-XObject text and renders only the scanned page."""

    table_name = f"document_pdf_auto_ocr_{time.time_ns()}"
    asset_enrichment = _document_units_asset_enrichment()
    extraction_config = asset_enrichment["producer_json"]["config"]
    extraction_config["ocr"] = {
        "enabled": True,
        "mode": "auto",
        "render_dpi": 150,
        "max_rendered_pixels": 4_000_000,
        "max_rendered_dimension": 2048,
        "config": {
            "provider": "antfly",
            "model": "antflydb/Florence-2-base",
            "api_url": pdf_ocr_e2e_server.reader_api_url,
        },
    }
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

    zig_root = Path(__file__).resolve().parents[2]
    scanned_table_pdf = (
        zig_root / "lib/pdf/testdata/scanned_table_fixture.pdf"
    ).read_bytes()
    inline_scanned_table = "data:application/pdf;base64," + base64.b64encode(
        scanned_table_pdf
    ).decode("ascii")
    docs = {
        "form-text-url": {
            "filename": "form-xobject-text.pdf",
            "mime_type": "application/pdf",
            "version": "1",
            "url": pdf_ocr_e2e_server.form_pdf_url,
        },
        "scanned-table-inline": {
            "filename": "scanned-table.pdf",
            "mime_type": "application/pdf",
            "version": "1",
            "url": inline_scanned_table,
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
                    and current.get("chunk_count", 0) >= 1
                )
                else None
            ),
            timeout_s=90.0,
            interval_s=0.5,
        )
        assert manifest is not None, {
            "document": doc_key,
            "manifest": _manifest_ready(stateful_api, table_name, doc_key),
            "reader": pdf_ocr_e2e_server.stats(),
        }
        manifests[doc_key] = manifest

    embedded = manifests["form-text-url"]
    assert embedded["source_url"] == pdf_ocr_e2e_server.form_pdf_url
    assert embedded["unit_count"] == 1
    assert embedded["ocr_attempted_count"] == 0
    assert embedded["ocr_selected_count"] == 0
    assert embedded["ocr_failed_count"] == 0

    scanned = manifests["scanned-table-inline"]
    assert scanned["unit_count"] == 1
    assert scanned["ocr_attempted_count"] == 1
    assert scanned["ocr_selected_count"] == 1
    assert scanned["ocr_failed_count"] == 0

    for term, expected_id in (
        ("CONSOLIDATED FINANCIAL HIGHLIGHTS", "form-text-url"),
        ("OfficeQA scanned table", "scanned-table-inline"),
    ):
        result = wait_until(
            lambda term=term, expected_id=expected_id: (
                response
                if expected_id
                in _query_hit_ids(
                    response := stateful_api.query_table(
                        table_name,
                        {
                            "full_text_search": {"field": "text", "match": term},
                            "limit": 10,
                        },
                    )
                )
                else None
            ),
            timeout_s=60.0,
            interval_s=0.5,
        )
        assert result is not None, {"term": term, "manifests": manifests}

    reader_stats = pdf_ocr_e2e_server.stats()
    assert reader_stats["png_requests"] == 1, reader_stats
    assert reader_stats["jpeg_requests"] == 0, reader_stats


def test_artifact_full_text_chunks_remain_with_parents_across_three_shards(
    stateful_api,
):
    """Parent range routing, not the shared ``af1:chunk`` display prefix, owns chunks."""

    table_name = f"artifact_full_text_three_shards_{time.time_ns()}"
    created = stateful_api.create_table(
        table_name,
        num_shards=3,
    )
    assert created.get("name") == table_name or created.get("table_name") == table_name
    _provision_artifact_full_text(stateful_api, table_name, chunk_size=16)

    # Three-shard tables start with lexical ranges. These parent keys
    # deliberately land below, between, and above the two initial boundaries.
    # Their generated artifact keys must stay in the selected parent group.
    parent_markers = {
        "0/parent-left": "leftartifactmarker",
        "8/parent-middle": "middleartifactmarker",
        "z/parent-right": "rightartifactmarker",
    }
    inserts = {}
    for parent_key, marker in parent_markers.items():
        source = ((marker + " common ") * 96).encode()
        inserts[parent_key] = {
            "filename": f"{marker}.txt",
            "mime_type": "text/plain",
            "version": "1",
            "url": "data:text/plain;base64," + base64.b64encode(source).decode(),
        }

    written = stateful_api.batch_write(
        table_name,
        inserts=inserts,
        sync_level="full_index",
    )
    assert written["inserted"] == len(inserts)

    manifests = {}
    for parent_key in parent_markers:
        manifest = wait_until(
            lambda parent_key=parent_key: (
                current
                if (
                    (current := _manifest_ready(stateful_api, table_name, parent_key))
                    is not None
                    and current.get("chunk_count", 0) > 1
                )
                else None
            ),
            timeout_s=90.0,
            interval_s=0.5,
        )
        assert manifest is not None
        assert manifest["child_ranges"]
        assert all(
            child_range.get("placement") == "parent"
            for child_range in manifest["child_ranges"]
        )
        manifests[parent_key] = manifest

    def distributed_index_status() -> dict | None:
        detail = stateful_api.get_index(table_name, DEFAULT_FULL_TEXT_INDEX)
        shards = detail.get("shard_status", {})
        counts = [
            int(status.get("total_indexed", 0))
            for status in shards.values()
        ]
        if len(counts) != 3 or not all(count > 0 for count in counts):
            return None
        return detail

    detail = wait_until(
        distributed_index_status,
        timeout_s=90.0,
        interval_s=0.5,
    )
    assert detail is not None, json.dumps(
        stateful_api.get_index(table_name, DEFAULT_FULL_TEXT_INDEX),
        indent=2,
        sort_keys=True,
    )

    result = stateful_api.query_table(
        table_name,
        {
            "full_text_search": {"field": "text", "match": "common"},
            "limit": 10,
        },
    )
    assert set(parent_markers).issubset(_query_hit_ids(result)), {
        "manifests": manifests,
        "result": result,
    }


@pytest.mark.slow
@pytest.mark.scale
def test_artifact_full_text_scale_exceeds_one_million_chunks_on_three_shards(
    stateful_api,
):
    """Opt-in live qualification for the artifact-specific merge/backpressure path."""

    if os.environ.get("ANTFLY_ARTIFACT_FULL_TEXT_SCALE") != "1":
        pytest.skip("set ANTFLY_ARTIFACT_FULL_TEXT_SCALE=1 to run the >1M chunk gate")

    target_chunks = 1_000_001
    table_name = f"artifact_full_text_scale_{time.time_ns()}"
    created = stateful_api.create_table(
        table_name,
        num_shards=3,
    )
    assert created.get("name") == table_name or created.get("table_name") == table_name
    _provision_artifact_full_text(stateful_api, table_name, chunk_size=8)

    # Keep each source close to the reported PG-19 reproduction's ~829 chunks
    # per book. Giant inline sources are not representative: the source field
    # remains part of each derived indexing input until segment publication.
    source = ("artifactscaletoken " * 340).encode()
    source_url = "data:text/plain;base64," + base64.b64encode(source).decode()
    lane_prefixes = ("0/", "8/", "z/")
    hot_shard_floor = 750_000
    generated_chunks = 0
    parent_number = 0
    sample_manifests: dict[str, dict] = {}

    # Keep source requests bounded and wait for each batch's generated chunks,
    # matching the live PG-19 reproduction instead of measuring accepted rows.
    # Five of every six parents deliberately share one range, so this gate
    # crosses the old ~746k per-shard ceiling while still exercising all three
    # shards. Submit one source at a time so merge backpressure throttles the
    # producer instead of accumulating a multi-thousand-chunk HTTP burst.
    while generated_chunks < target_chunks:
        batch_slot = parent_number % 6
        cycle = parent_number // 6
        prefix = (
            lane_prefixes[0]
            if batch_slot < 5
            else lane_prefixes[1 + cycle % 2]
        )
        parent_key = f"{prefix}scale-{parent_number:08d}"

        def ready_parent() -> dict | None:
            manifest = _manifest_ready(stateful_api, table_name, parent_key)
            if manifest is None or manifest.get("chunk_count", 0) == 0:
                return None
            return manifest

        manifest = None
        for attempt in range(3):
            try:
                inserted = stateful_api.batch_write_with_timeout(
                    table_name,
                    inserts={
                        parent_key: {
                            "filename": f"scale-{parent_number:08d}.txt",
                            "mime_type": "text/plain",
                            "version": "1",
                            "url": source_url,
                        }
                    },
                    sync_level="write",
                    timeout_s=300.0,
                )
                assert inserted["inserted"] == 1
                break
            except requests.RequestException:
                # The write may commit before a busy shard can return its HTTP
                # response. Confirm the deterministic parent before retrying;
                # a dead or permanently stalled server still exhausts this
                # strict attempt bound and fails the qualification.
                manifest = wait_until(
                    ready_parent,
                    timeout_s=30.0,
                    interval_s=1.0,
                )
                if manifest is not None:
                    break
                if attempt == 2:
                    raise

        if manifest is None:
            manifest = wait_until(
                ready_parent,
                timeout_s=300.0,
                interval_s=1.0,
            )
        assert manifest is not None
        generated_chunks += manifest["chunk_count"]
        sample_manifests.setdefault(prefix, manifest)
        parent_number += 1

    assert generated_chunks >= target_chunks
    assert set(sample_manifests) == set(lane_prefixes)
    assert all(
        child_range.get("placement") == "parent"
        for manifest in sample_manifests.values()
        for child_range in manifest["child_ranges"]
    )

    def million_chunks_visible_on_every_shard() -> dict | None:
        detail = stateful_api.get_index(table_name, DEFAULT_FULL_TEXT_INDEX)
        shards = detail.get("shard_status", {})
        counts = [
            int(status.get("total_indexed", 0))
            for status in shards.values()
        ]
        if len(counts) != 3 or not all(count > 0 for count in counts):
            return None
        if sum(counts) < target_chunks:
            return None
        if max(counts) < hot_shard_floor:
            return None
        return detail

    detail = wait_until(
        million_chunks_visible_on_every_shard,
        timeout_s=600.0,
        interval_s=2.0,
    )
    assert detail is not None, json.dumps(
        stateful_api.get_index(table_name, DEFAULT_FULL_TEXT_INDEX),
        indent=2,
        sort_keys=True,
    )


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

    doc_key = "atomic-doc-a"
    second_doc_key = "atomic-doc-b"
    first_merged = stateful_api.linear_merge(
        table_name,
        records={
            doc_key: {
                "filename": "atomic.txt",
                "mime_type": "text/plain",
                "version": "1",
                "url": "data:text/plain;base64,YXRvbWljIHF1YWxpZmljYXRpb24gZ2FtbWE=",
            }
        },
        sync_level="full_index",
    )
    assert first_merged["upserted"] == 1
    merged = stateful_api.linear_merge(
        table_name,
        records={
            second_doc_key: {
                "filename": "secondary.txt",
                "mime_type": "text/plain",
                "version": "1",
                "url": "data:text/plain;base64,c2Vjb25kYXJ5IGNvdmVyYWdlIGRlbHRh",
            },
        },
        last_merged_id=first_merged["next_cursor"],
        sync_level="full_index",
    )
    assert merged["upserted"] == 1
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
            lambda: _manifest_ready(stateful_api, table_name, second_doc_key),
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
    coverage = wait_until(
        lambda: (
            status
            if (
                (status := stateful_api.get_index(table_name, "document_vectors"))
                .get("status", {})
                .get("coverage", {})
                .get("source_total")
                == 2
                and status["status"]["coverage"].get("produced") == 2
                and status["status"]["coverage"].get("covered") == 2
                and status["status"]["coverage"].get("observation_complete") is True
                and status["status"]["coverage"].get("complete") is True
                and status["status"]["coverage"].get("healthy") is True
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert coverage is not None, json.dumps(
        stateful_api.get_index(table_name, "document_vectors"), sort_keys=True
    )

    # Match paged public /merge clients: close the final key range with an
    # empty full-index merge before the process restart. This used to leave a
    # persisted live-writer report whose derived counts survived restart while
    # its primary source denominator collapsed to zero.
    cleanup = stateful_api.linear_merge(
        table_name,
        records={},
        last_merged_id=merged["next_cursor"],
        sync_level="full_index",
    )
    assert cleanup["upserted"] == 0
    assert cleanup["deleted"] == 0
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

    # Runtime-group coverage is propagated through persisted status reports;
    # restarting must not collapse the exact primary-document count to zero.
    stateful_api.restart_server()
    coverage_after_restart = wait_until(
        lambda: (
            status
            if (
                (status := stateful_api.get_index(table_name, "document_vectors"))
                .get("status", {})
                .get("coverage", {})
                .get("source_total")
                == 2
                and status["status"]["coverage"].get("produced") == 2
                and status["status"]["coverage"].get("covered") == 2
                and status["status"]["coverage"].get("observation_complete") is True
                and status["status"]["coverage"].get("complete") is True
                and status["status"]["coverage"].get("healthy") is True
            )
            else None
        ),
        timeout_s=90.0,
        interval_s=1.0,
    )
    assert coverage_after_restart is not None, json.dumps(
        stateful_api.get_index(table_name, "document_vectors"), sort_keys=True
    )


def test_artifact_coverage_terminal_outcomes_by_policy_after_restart(
    stateful_api, openai_embedder
):
    """Produced, skipped, and failed sources settle every coverage policy."""

    def indexes_for_policy(policy: str) -> dict:
        units = _document_units_index_config()
        return {
            "document_units": units,
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
                            units["artifact"]["producer_json"], separators=(",", ":")
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
                "type": "embeddings",
                "coverage_policy": policy,
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
        }

    tables: dict[str, str] = {}

    def expected_status(policy: str) -> dict | None:
        current = stateful_api.get_index(tables[policy], "document_vectors")
        status = current.get("status", {})
        coverage = status.get("coverage", {})
        expected_covered = {"strict": 1, "partial": 2, "best_effort": 3}[policy]
        expected_complete = policy == "best_effort"
        if not (
            coverage.get("source_total") == 3
            and coverage.get("produced") == 1
            and coverage.get("skipped") == 1
            and coverage.get("terminal_failed") == 1
            and coverage.get("covered") == expected_covered
            # Pending is unsettled work, not policy-specific uncovered work.
            # All three source documents have reached terminal outcomes.
            and coverage.get("pending") == 0
            and coverage.get("observation_complete") is True
            and coverage.get("complete") is expected_complete
            and coverage.get("healthy") is False
            and status.get("backfill_active") is False
        ):
            return None
        if expected_complete:
            if coverage.get("degraded") is not True:
                return None
        elif status.get("backfill_state") != "degraded":
            return None
        return current

    bad_pdf = "data:application/pdf;base64," + base64.b64encode(
        b"%PDF-1.7\nnot a complete pdf"
    ).decode("ascii")
    records = {
        "produced": {
            "filename": "produced.txt",
            "mime_type": "text/plain",
            "version": "1",
            "url": "data:text/plain;base64,cHJvZHVjZWQgY292ZXJhZ2UgZG9jdW1lbnQ=",
        },
        "skipped": {
            "filename": "intentional.skip",
            "mime_type": "application/x-antfly-intentional-skip",
            "version": "1",
            "url": "data:application/octet-stream;base64,c2tpcA==",
        },
        "failed": {
            "filename": "failed.pdf",
            "mime_type": "application/pdf",
            "version": "1",
            "url": bad_pdf,
        },
    }

    for policy in ("strict", "partial", "best_effort"):
        table_name = f"artifact_terminal_{policy}_{time.time_ns()}"
        tables[policy] = table_name
        created = stateful_api.post(
            f"/tables/{table_name}",
            {"num_shards": 1, "indexes": indexes_for_policy(policy)},
        )
        assert (
            created.get("name") == table_name or created.get("table_name") == table_name
        )
        merged = stateful_api.linear_merge(
            table_name, records=records, sync_level="full_index"
        )
        assert merged["upserted"] == 3
        cleanup = stateful_api.linear_merge(
            table_name,
            records={},
            last_merged_id=merged["next_cursor"],
            sync_level="full_index",
        )
        assert cleanup["upserted"] == 0

        settled = wait_until(
            lambda policy=policy: expected_status(policy),
            timeout_s=90.0,
            interval_s=0.5,
        )
        assert settled is not None, json.dumps(
            stateful_api.get_index(tables[policy], "document_vectors"), sort_keys=True
        )

    failed_manifest = stateful_api.get(
        f"{_document_artifact_path(tables['strict'], 'failed', DOCUMENT_UNITS_ARTIFACT)}?detail=raw"
    )
    assert failed_manifest["merge_status"] == "failed"
    assert (
        json.loads(failed_manifest["manifest_json"])["last_error"]["stage"]
        == "pdf_structure"
    )
    skipped_manifest = stateful_api.get(
        f"{_document_artifact_path(tables['strict'], 'skipped', DOCUMENT_UNITS_ARTIFACT)}?detail=raw"
    )
    assert skipped_manifest["merge_status"] == "converged"
    assert skipped_manifest["route_type"] == "unsupported"
    assert skipped_manifest["unit_count"] == 0

    stateful_api.restart_server()
    for policy in tables:
        settled = wait_until(
            lambda policy=policy: expected_status(policy),
            timeout_s=90.0,
            interval_s=1.0,
        )
        assert settled is not None, json.dumps(
            stateful_api.get_index(tables[policy], "document_vectors"), sort_keys=True
        )


def test_artifact_backed_chunk_embeddings_are_semantic_searchable(
    stateful_api, openai_embedder
):
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
    assert_created_index(
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
        ),
        "document_vectors",
        "embeddings",
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
                (current := _manifest_ready(stateful_api, table_name, doc_key))
                is not None
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
                and index.get("status", {}).get("coverage", {}).get("source_total") == 2
                and index.get("status", {}).get("coverage", {}).get("produced") == 2
                and index.get("status", {})
                .get("coverage", {})
                .get("observation_complete")
                is True
                and index.get("status", {}).get("coverage", {}).get("complete") is True
                and index.get("status", {}).get("coverage", {}).get("healthy") is True
                and index.get("status", {})
                .get("enrichment_runtime", {})
                .get("embed_batches_completed", 0)
                > 0
                and index.get("status", {})
                .get("enrichment_runtime", {})
                .get("total_embed_ns", 0)
                > 0
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
    assert final_index["status"]["coverage"]["source_total"] == 2
    assert final_index["status"]["coverage"]["produced"] == 2
    assert final_index["status"]["coverage"]["healthy"] is True
    assert final_index["status"]["enrichment_runtime"]["total_embed_ns"] > 0

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
