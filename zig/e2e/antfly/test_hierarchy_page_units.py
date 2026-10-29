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

"""Documented page browsing uses durable source cursors on both storage engines."""

import base64
import json
import os
from pathlib import Path
from urllib.parse import quote

import conftest as harness
import pytest
import requests

from helpers import wait_until


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("storage_engine", ["local", "lite"])
def test_page_unit_cursor_survives_reopen_and_fences_source_changes(storage_engine):
    binary = harness.resolve_binary_path(
        os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
    )
    server = harness.StandaloneAntflyServer(
        binary, "127.0.0.1", 0, storage_engine=storage_engine
    )
    failed = True
    try:
        with requests.Session() as session:
            session.headers["Connection"] = "close"

            def request(method, path, body=None):
                return session.request(
                    method, server.api_url + path, json=body, timeout=30
                )

            def checked(method, path, body=None):
                response = request(method, path, body)
                assert response.ok, response.text
                return response.json()

            checked("POST", "/tables/docs", {"num_shards": 1})
            checked(
                "PUT",
                "/tables/docs/artifacts/document_units_v1/enrichment",
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
                                },
                                "ocr_fallback": False,
                            },
                        }
                    ),
                },
            )
            pdf = (
                Path(__file__).resolve().parents[2]
                / "lib/pdf/testdata/two_page_text_fixture.pdf"
            ).read_bytes()
            source_id = "doc:a/with/slash"
            document = {
                "filename": "two-pages.pdf",
                "mime_type": "application/pdf",
                "version": "1",
                "url": "data:application/pdf;base64,"
                + base64.b64encode(pdf).decode("ascii"),
            }

            def write_source():
                checked(
                    "POST",
                    "/tables/docs/batch",
                    {"inserts": {source_id: document}, "sync_level": "write"},
                )

            manifest_path = (
                f"/tables/docs/documents/{quote(source_id, safe='')}"
                "/artifacts/document_units_v1?detail=raw"
            )

            def ready_manifest(previous=None):
                response = request("GET", manifest_path)
                if response.status_code == 404:
                    return None
                assert response.ok, response.text
                current = response.json()
                if (
                    current.get("unit_count") == 2
                    and current.get("merge_status") == "converged"
                    and (
                        previous is None
                        or current["generation"] > previous["generation"]
                    )
                ):
                    return current
                return None

            write_source()
            manifest = wait_until(ready_manifest, timeout_s=30, interval_s=0.1)
            assert manifest is not None
            assert manifest["route_type"] == "pdf"
            browse = {
                "fields": [
                    "unit_id",
                    "unit_type",
                    "text",
                    "provenance.page_number",
                    "provenance.page_label",
                ],
                "hierarchy": {
                    "children": {
                        "parent": {"level": "source", "id": source_id},
                        "level": "unit",
                    }
                },
                "order_by": [{"field": "_hierarchy.position"}],
                "limit": 1,
            }

            def page(body):
                result = checked("POST", "/tables/docs/query", body)["responses"][0]
                assert result.get("remote_snapshot") is None
                return result["hits"]["hits"]

            first = page(browse)
            assert len(first) == 1
            assert first[0]["_source"]["text"]
            assert first[0]["_source"]["unit_type"] == "page"
            assert first[0]["_source"]["provenance"]["page_number"] == 1
            assert len(first[0]["_sort"]) == 2
            assert all(isinstance(value, str) for value in first[0]["_sort"])
            continuation = {**browse, "search_after": first[0]["_sort"]}
            second = page(continuation)
            assert len(second) == 1
            assert second[0]["_id"] != first[0]["_id"]
            assert second[0]["_source"]["text"]
            assert second[0]["_source"]["provenance"]["page_number"] == 2
            assert page({**browse, "search_after": second[0]["_sort"]}) == []

            # No in-memory or directory-backup token is needed after reopen.
            server.restart()
            reopened = page(continuation)
            assert [hit["_id"] for hit in reopened] == [second[0]["_id"]]
            assert reopened[0]["_sort"] == second[0]["_sort"]

            # A traversal is source-fenced, rather than pinned to a stale copy.
            document["version"] = "2"
            write_source()
            assert (
                wait_until(
                    lambda: ready_manifest(manifest), timeout_s=30, interval_s=0.1
                )
                is not None
            )
            stale = request("POST", "/tables/docs/query", continuation)
            assert stale.status_code == 409, stale.text
            assert stale.json()["error"] == "hierarchy_cursor_stale"
            assert stale.json()["action"] == "restart_hierarchy_traversal"
            assert len(page(browse)) == 1
        failed = False
    finally:
        server.stop(test_failed=failed)
