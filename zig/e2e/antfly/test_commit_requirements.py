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

"""Native HTTP, coordinator, Raft and storage prepared-effect contracts."""

from __future__ import annotations

import json
import time

from helpers import wait_until


def test_direct_gate_commits_extraction_transforms_deletes_and_transactions(
    stateful_api,
):
    table = f"commit_requirements_{time.time_ns()}"
    audit = f"{table}_audit"
    stateful_api.create_table(table, num_shards=1)
    stateful_api.create_table(audit, num_shards=1)
    stateful_api.put(
        f"/tables/{table}/artifacts/extraction/enrichment",
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
                        }
                    },
                }
            ),
        },
    )

    def configured():
        metadata = stateful_api.get(f"/tables/{table}")
        return any(
            item.get("name") == "extraction"
            for item in metadata.get("artifact_enrichments", [])
        )

    def source(text):
        return {
            "url": f"data:text/plain,{text}",
            "filename": "doc.txt",
            "mime_type": "text/plain",
            "counter": 1,
        }

    def assert_extracted(key):
        artifacts = stateful_api.get(
            f"/tables/{table}/documents/{key}/artifacts?detail=raw"
        )
        assert any(
            item["artifact_name"] == "extraction" for item in artifacts["artifacts"]
        ), artifacts

    assert wait_until(configured, timeout_s=30, interval_s=0.1)
    first = stateful_api.post(
        f"/tables/{table}/batch",
        {
            "commit_requires": "direct_enrichments",
            "sync_level": "write",
            "inserts": {"doc": source("alpha%20beta"), "victim": source("retire")},
        },
    )
    assert first["status"].startswith("committed"), first
    assert_extracted("doc")

    changed = stateful_api.post(
        f"/tables/{table}/batch",
        {
            "commit_requires": "direct_enrichments",
            "sync_level": "write",
            "transforms": [
                {
                    "key": "doc",
                    "operations": [{"op": "$inc", "path": "$.counter", "value": 1}],
                }
            ],
            "deletes": ["victim"],
        },
    )
    assert changed["status"].startswith("committed"), changed
    assert stateful_api.lookup_key(table, "doc")["counter"] == 2
    response = stateful_api.s.get(f"{stateful_api.url}/tables/{table}/documents/victim")
    assert response.status_code == 404, response.text
    assert_extracted("doc")

    committed = stateful_api.post(
        "/batch",
        {
            "commit_requires": "direct_enrichments",
            "sync_level": "write",
            "tables": {
                table: {"inserts": {"transactional": source("prepared%20vote")}},
                audit: {"inserts": {"accepted": {"counter": 1}}},
            },
        },
    )
    assert committed["tables"][table]["inserted"] == 1, committed
    assert committed["tables"][audit]["inserted"] == 1, committed
    assert_extracted("transactional")
    assert stateful_api.lookup_key(audit, "accepted")["counter"] == 1

    # A direct producer failure must abort every participant, including deletes.
    rejected = stateful_api.s.post(
        f"{stateful_api.url}/batch",
        json={
            "commit_requires": "direct_enrichments",
            "sync_level": "write",
            "tables": {
                table: {
                    "inserts": {
                        "invalid": {
                            "url": "data:application/pdf;base64,bm90IGEgcGRm",
                            "filename": "broken.pdf",
                            "mime_type": "application/pdf",
                        }
                    },
                    "deletes": ["doc"],
                },
                audit: {"deletes": ["accepted"]},
            },
        },
    )
    assert rejected.status_code >= 400, rejected.text
    assert stateful_api.lookup_key(table, "doc")["counter"] == 2
    assert stateful_api.lookup_key(audit, "accepted")["counter"] == 1
    absent = stateful_api.s.get(f"{stateful_api.url}/tables/{table}/documents/invalid")
    assert absent.status_code == 404, absent.text
