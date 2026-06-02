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

"""End-to-end tests for relational-mode tables.

Relational tables store a document's declared scalar properties as typed columns
and do NOT keep a JSON blob: a lookup/query reconstructs the document from the
columns. These tests drive the real server to verify, end to end:
  - a document round-trips through column reconstruction on lookup;
  - a keyword-equality predicate is served (routed to the typed column);
  - a numeric range predicate is served from the column;
  - a datetime column accepts an RFC3339 string on ingest;
  - a `json` column preserves its subtree.
"""

from __future__ import annotations

import time

from helpers import wait_until


RELATIONAL_SCHEMA = {
    "version": 0,
    "storage_mode": "relational",
    "default_type": "row",
    "enforce_types": True,
    "document_schemas": {
        "row": {
            "schema": {
                "type": "object",
                "properties": {
                    "title": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "amount": {"type": "numeric"},
                    "created": {"type": "datetime"},
                    "active": {"type": "boolean"},
                    "meta": {"type": "json"},
                },
                "required": ["title", "status", "amount"],
                "additionalProperties": False,
            }
        }
    },
}


def _hit_ids(result: dict) -> list[str]:
    responses = result.get("responses", [])
    if not responses:
        return []
    return [hit.get("_id") for hit in responses[0].get("hits", {}).get("hits", [])]


def _query_hit_ids(stateful_api, table_name: str, payload: dict) -> list[str] | None:
    try:
        result = stateful_api.query_table(table_name, payload)
    except Exception:
        return None
    return _hit_ids(result)


def test_relational_table_columns_reconstruct_and_route(stateful_api):
    table_name = f"relational_{time.time_ns()}"
    created = stateful_api.create_table(table_name, num_shards=1, schema=RELATIONAL_SCHEMA)
    assert created["name"] == table_name
    assert created["schema"]["storage_mode"] == "relational"

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "row:a": {
                "title": "alpha",
                "status": "active",
                "amount": 12.5,
                "created": "2026-01-02T03:04:05Z",
                "active": True,
                "meta": {"k": 1, "tags": ["x", "y"]},
            },
            "row:b": {
                "title": "beta",
                "status": "archived",
                "amount": 50.0,
                "created": "2026-06-01T00:00:00Z",
                "active": False,
                "meta": {"k": 2},
            },
            "row:c": {
                "title": "gamma",
                "status": "active",
                "amount": 100.0,
                "created": "2026-12-31T23:59:59Z",
                "active": True,
                "meta": {"k": 3},
            },
        },
        sync_level="full_text",
    )
    assert batch["inserted"] == 3

    # 1. The document is reconstructed from columns on lookup (no JSON blob is
    #    stored). Scalars round-trip; the json subtree is preserved; the RFC3339
    #    datetime is accepted on ingest and comes back as epoch nanoseconds
    #    (the column's physical encoding).
    doc = stateful_api.lookup_key(table_name, "row:a")
    assert doc["title"] == "alpha"
    assert doc["status"] == "active"
    assert doc["amount"] == 12.5
    assert doc["active"] is True
    assert doc["meta"] == {"k": 1, "tags": ["x", "y"]}
    # 2026-01-02T03:04:05Z == 1767323045 s == 1767323045000000000 ns.
    assert doc["created"] == 1767323045000000000

    # 2. A keyword-equality predicate is served (routed to the typed column).
    active_ids = wait_until(
        lambda: (
            ids
            if (ids := _query_hit_ids(
                stateful_api,
                table_name,
                {"filter_query": {"query": "status:active"}, "limit": 10},
            ))
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert active_ids is not None
    assert set(active_ids) == {"row:a", "row:c"}

    # 3. A numeric range predicate is served from the column. amount in [40 TO
    #    120} matches row:b (50) and row:c (100), not row:a (12.5).
    range_ids = wait_until(
        lambda: (
            ids
            if (ids := _query_hit_ids(
                stateful_api,
                table_name,
                {"filter_query": {"query": "amount:[40 TO 120}"}, "limit": 10},
            ))
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert range_ids is not None
    assert set(range_ids) == {"row:b", "row:c"}


def test_relational_table_enforces_closed_schema(stateful_api):
    table_name = f"relational_closed_{time.time_ns()}"
    stateful_api.create_table(table_name, num_shards=1, schema=RELATIONAL_SCHEMA)

    # A document missing a required column (amount) must be rejected.
    rejected = False
    try:
        stateful_api.batch_write(
            table_name,
            inserts={"row:bad": {"title": "x", "status": "active"}},
            sync_level="write",
        )
    except Exception:
        rejected = True
    assert rejected, "relational table accepted a document missing a required column"
