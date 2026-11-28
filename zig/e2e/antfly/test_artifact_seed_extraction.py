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

"""Partial extraction seeds replace their pages of a real PDF extraction (#1791).

PDF reading is compiled out of the Zig DB tests, which cover the seed record
and the page overlay separately; this drives both through the binary.
"""

import base64
import json
import os
from pathlib import Path
from urllib.parse import quote

import conftest as harness
import pytest
import requests
from helpers import wait_until

SEEDED_PAGE_TWO = "Seeded text of the second page."


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("storage_engine", ["local", "lite"])
def test_partial_extraction_seed_replaces_its_page_of_a_real_pdf(storage_engine):
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
                                },
                            },
                        }
                    ),
                },
            )
            pdf = (
                Path(__file__).resolve().parents[2]
                / "lib/pdf/testdata/two_page_text_fixture.pdf"
            ).read_bytes()
            source_id = "doc:partial"
            document = {
                "filename": "two-pages.pdf",
                "mime_type": "application/pdf",
                "url": "data:application/pdf;base64,"
                + base64.b64encode(pdf).decode("ascii"),
                "_artifacts": {
                    "document_units_v1": {
                        "value": {
                            "partial": True,
                            "units": [{"text": SEEDED_PAGE_TWO, "page_number": 2}],
                        }
                    }
                },
            }
            written = checked(
                "POST",
                "/tables/docs/batch",
                {"inserts": {source_id: document}, "sync_level": "write"},
            )
            # Outcomes are reported where the write's seeds were prepared
            # (see BatchResponse.seeds); when present, the seed is accepted.
            if "seeds" in written:
                assert any(
                    item.get("name") == "document_units_v1"
                    and item.get("outcome") == "accepted"
                    for item in written["seeds"]
                ), written

            manifest_path = (
                f"/tables/docs/documents/{quote(source_id, safe='')}"
                "/artifacts/document_units_v1?detail=raw"
            )

            def ready_manifest():
                response = request("GET", manifest_path)
                if response.status_code == 404:
                    return None
                assert response.ok, response.text
                current = response.json()
                if (
                    current.get("unit_count") == 2
                    and current.get("merge_status") == "converged"
                ):
                    return current
                return None

            manifest = wait_until(ready_manifest, timeout_s=60, interval_s=0.1)
            assert manifest is not None
            assert manifest["route_type"] == "pdf"

            hits = checked(
                "POST",
                "/tables/docs/query",
                {
                    "fields": ["unit_id", "text", "provenance.page_number"],
                    "hierarchy": {
                        "children": {
                            "parent": {"level": "source", "id": source_id},
                            "level": "unit",
                        }
                    },
                    "order_by": [{"field": "_hierarchy.position"}],
                    "limit": 10,
                },
            )["responses"][0]["hits"]["hits"]
            pages = {
                hit["_source"]["provenance"]["page_number"]: hit["_source"]["text"]
                for hit in hits
            }
            assert sorted(pages) == [1, 2]
            # The PDF supplies page one; the seed supplies page two.
            assert pages[1].strip()
            assert SEEDED_PAGE_TWO not in pages[1]
            assert pages[2] == SEEDED_PAGE_TWO
        failed = False
    finally:
        server.stop(test_failed=failed)
