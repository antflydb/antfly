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

"""End-to-end tests for algebraic (aggregation) indexes on relational tables.

These drive the real server to verify, end to end:
  - the provisioned-table server provisions the `algebraic` index kind on a
    relational table (this previously failed with UnsupportedCreateTableRequest);
  - switching a table to relational mode auto-creates a schema-derived algebraic
    index (algebraic_index_v0) with no user configuration;
  - aggregations (terms / stats) over a relational table return correct results,
    computed over ALL matching documents -- including when the result page is
    bounded below the match count (low or zero `limit`), and respecting a
    predicate.
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
                    "status": {"type": "keyword"},
                    "amount": {"type": "numeric"},
                },
                "required": ["status", "amount"],
                "additionalProperties": False,
            }
        }
    },
}

# 5 rows: 3 active (10, 30, 40), 2 archived (20, 50).
ROWS = {
    "row:1": {"status": "active", "amount": 10.0},
    "row:2": {"status": "archived", "amount": 20.0},
    "row:3": {"status": "active", "amount": 30.0},
    "row:4": {"status": "active", "amount": 40.0},
    "row:5": {"status": "archived", "amount": 50.0},
}


def _index_entries(stateful_api, table_name):
    listed = stateful_api.list_indexes(table_name)
    return listed if isinstance(listed, list) else listed.get("indexes", [])


def _index_names(stateful_api, table_name):
    names = set()
    for entry in _index_entries(stateful_api, table_name):
        if not isinstance(entry, dict):
            continue
        config = entry.get("config", {})
        name = config.get("name") or entry.get("name")
        if name:
            names.add(name)
    return names


def _algebraic_index_name(stateful_api, table_name):
    for entry in _index_entries(stateful_api, table_name):
        if not isinstance(entry, dict):
            continue
        config = entry.get("config", {})
        if config.get("type") == "algebraic":
            return config.get("name") or entry.get("name") or True
    return None


def _terms_counts(stateful_api, table_name, field, *, limit, query=None):
    payload = {"limit": limit, "aggregations": {"by": {"type": "terms", "field": field, "size": 10}}}
    if query is not None:
        payload["filter_query"] = {"query": query}
    result = stateful_api.query_table(table_name, payload)
    responses = result.get("responses") or []
    if not responses:
        return None
    terms = (responses[0].get("aggregations") or {}).get("by")
    if not terms:
        return None
    buckets = terms.get("buckets") or []
    counts = {b.get("key"): b.get("doc_count") for b in buckets if b.get("key") is not None}
    return counts or None


def _stats(stateful_api, table_name, field, *, limit, query=None):
    payload = {"limit": limit, "aggregations": {"s": {"type": "stats", "field": field}}}
    if query is not None:
        payload["filter_query"] = {"query": query}
    result = stateful_api.query_table(table_name, payload)
    responses = result.get("responses") or []
    if not responses:
        return None
    s = (responses[0].get("aggregations") or {}).get("s")
    if not s or s.get("count") is None:
        return None
    return s


def _setup_relational(stateful_api, *, create_index):
    table_name = f"rel_alg_{time.time_ns()}"
    stateful_api.create_table(table_name, num_shards=1)
    stateful_api.update_schema(table_name, RELATIONAL_SCHEMA)
    if create_index:
        stateful_api.create_index(table_name, "agg_idx", {"type": "algebraic", "derive_from_schema": True})
    # Wait for an algebraic index (explicit or auto-created) to be visible.
    alg = wait_until(lambda: _algebraic_index_name(stateful_api, table_name), timeout_s=30.0, interval_s=0.5)
    assert alg is not None, f"no algebraic index; indexes={_index_names(stateful_api, table_name)}"
    stateful_api.batch_write(table_name, inserts=ROWS, sync_level="full_index")
    return table_name


def test_relational_explicit_algebraic_index_serves_aggregations(stateful_api):
    """An explicitly-created algebraic index provisions on a relational table
    (previously failed with UnsupportedCreateTableRequest) and serves correct
    terms/stats aggregations over all rows."""
    table_name = _setup_relational(stateful_api, create_index=True)
    assert "agg_idx" in _index_names(stateful_api, table_name)

    counts = wait_until(lambda: _terms_counts(stateful_api, table_name, "status", limit=10), timeout_s=30.0, interval_s=0.5)
    assert counts == {"active": 3, "archived": 2}, counts

    s = _stats(stateful_api, table_name, "amount", limit=10)
    assert s["count"] == 5 and s["sum"] == 150 and s["min"] == 10 and s["max"] == 50, s


def test_relational_table_autocreates_algebraic_index_and_serves_aggregations(stateful_api):
    """Switching a table to relational mode auto-creates a schema-derived
    algebraic index (no user config) that serves correct aggregations."""
    table_name = _setup_relational(stateful_api, create_index=False)

    counts = wait_until(lambda: _terms_counts(stateful_api, table_name, "status", limit=10), timeout_s=30.0, interval_s=0.5)
    assert counts == {"active": 3, "archived": 2}, counts


def test_relational_aggregations_cover_all_matches_regardless_of_limit(stateful_api):
    """Aggregations are computed over every matching document, not the returned
    page: a low limit, a zero limit (aggregation-only), and a predicate all
    yield results over the full match set."""
    table_name = _setup_relational(stateful_api, create_index=True)
    # Make sure aggregations are available first.
    assert wait_until(lambda: _terms_counts(stateful_api, table_name, "status", limit=10),
                      timeout_s=30.0, interval_s=0.5) == {"active": 3, "archived": 2}

    # Low limit (2 < 5 rows): aggregation still covers all 5.
    assert _terms_counts(stateful_api, table_name, "status", limit=2) == {"active": 3, "archived": 2}
    # Zero limit (size:0, aggregation-only): covers all 5.
    assert _terms_counts(stateful_api, table_name, "status", limit=0) == {"active": 3, "archived": 2}
    s0 = _stats(stateful_api, table_name, "amount", limit=0)
    assert s0["count"] == 5 and s0["sum"] == 150, s0

    # Predicate (status:active) with a low limit: aggregation covers only the
    # 3 matching rows (amounts 10 + 30 + 40 = 80), not all 5.
    s_active = _stats(stateful_api, table_name, "amount", limit=1, query="status:active")
    assert s_active["count"] == 3 and s_active["sum"] == 80, s_active


def _cardinality(stateful_api, table_name, field, *, limit=10):
    payload = {"limit": limit, "aggregations": {"c": {"type": "cardinality", "field": field}}}
    result = stateful_api.query_table(table_name, payload)
    responses = result.get("responses") or []
    if not responses:
        return None
    c = (responses[0].get("aggregations") or {}).get("c")
    return c.get("value") if c else None


def test_relational_aggregations_served_by_algebraic_index(stateful_api):
    """The auto-created algebraic index actually serves aggregations: a
    cardinality aggregation (which has no scan-from-page fallback in this shape)
    returns the correct distinct count, proving the index ingested the relational
    rows and the planner selected it."""
    table_name = _setup_relational(stateful_api, create_index=False)

    # cardinality of `status` over the 5 rows = 2 distinct (active, archived).
    card = wait_until(lambda: _cardinality(stateful_api, table_name, "status"), timeout_s=30.0, interval_s=0.5)
    assert card == 2, card
