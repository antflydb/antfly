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

from __future__ import annotations

import requests


def _table_name(created: dict) -> str:
    return created.get("name") or created.get("table_name") or ""


def _hit_ids(result: dict) -> list[str]:
    if "hits" in result:
        return [hit.get("doc_id") for hit in result.get("hits", [])]
    responses = result.get("responses", [])
    if not responses:
        return []
    return [hit.get("_id") for hit in responses[0].get("hits", {}).get("hits", [])]


def test_table_full_text_sync_level_makes_text_search_visible(table_api):
    table_name = f"docs_sync_full_text_{table_api.backend}"

    created = table_api.create_table(table_name)
    assert _table_name(created) == table_name

    batch = table_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "body": "alpha sync level",
            }
        },
        sync_level="full_text",
    )
    assert batch["inserted"] == 1

    search = table_api.query_table(
        table_name,
        {
            "full_text_search": {
                "query": "body:alpha",
            },
            "limit": 5,
        },
    )
    hits = _hit_ids(search)
    assert hits == ["doc:a"]


def test_serverless_enrichment_sync_level_is_accepted_for_managed_materialization(serverless_api):
    table_name = "docs_sync_enrichments"

    created = serverless_api.put(
        f"/tables/{table_name}",
        {
            "created_at_ns": 100,
            "policy": {
                "chunk_embeddings_enabled": True,
            },
            "indexes": {
                "semantic_idx": {
                    "type": "embeddings",
                    "dimension": 3,
                }
            },
        },
    )
    assert created["created"] is True

    batch = serverless_api._check(
        serverless_api.s.post(
            f"{serverless_api.url}/tables/{table_name}/batch",
            json={
                "inserts": {
                    "doc:a": {
                        "body": "alpha sync level",
                        "_embeddings": {
                            "semantic_idx": [1.0, 0.0, 0.0],
                        },
                    }
                },
                "sync_level": "enrichments",
            },
            timeout=60,
        )
    )
    assert batch["inserted"] == 1
