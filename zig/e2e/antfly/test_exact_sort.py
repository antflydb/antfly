# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the License at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Public exact-sort schema contract regressions."""

from __future__ import annotations

import time


DOCUMENTS = {
    "a": {
        "title": "alpha note",
        "label": "alpha",
        "size": -9007199254740993,
        "modified_at": "2026-05-05T12:43:00Z",
    },
    "b": {
        "title": "bravo note",
        "label": "bravo",
        "size": 10.5,
        "modified_at": "2026-06-05T12:43:00Z",
    },
    "c": {
        "title": "charlie note",
        "label": "charlie",
        "size": 18446744073709551615,
        "modified_at": "2026-07-05T12:43:00Z",
    },
}


def _hit_ids(result: dict) -> list[str]:
    responses = result.get("responses", [])
    if not responses:
        return []
    return [hit["_id"] for hit in responses[0].get("hits", {}).get("hits", [])]


def _capabilities_by_field(table: dict) -> dict[str, dict]:
    return {
        capability["field"]: capability
        for capability in table.get("field_capabilities", [])
        if capability.get("field")
    }


def _create_table_with_schema(stateful_api, table_name: str, schema: dict) -> dict:
    return stateful_api.post(
        f"/tables/{table_name}",
        {"num_shards": 1, "schema": schema},
    )


def _sorted_query(stateful_api, table_name: str, field: str) -> dict:
    response = stateful_api._request(
        "POST",
        f"/tables/{table_name}/query",
        {
            "query": {"match_all": {}},
            "limit": 3,
            "order_by": [{"field": field}],
            "profile": True,
        },
    )
    assert response.status_code == 200, (
        f"sorted query failed with {response.status_code}: {response.text}"
    )
    return response.json()


def test_public_exact_sort_declares_native_coverage_and_shorthand_fields_are_actionable(
    stateful_api,
):
    shorthand_table = f"exact_sort_shorthand_{time.time_ns()}"
    shorthand_schema = {
        "default_type": "doc",
        "document_schemas": {
            "doc": {
                "schema": {
                    "type": "object",
                    "additionalProperties": True,
                    "properties": {
                        "title": {"type": "string", "x-antfly-types": ["text"]},
                        "label": {"type": "string", "x-antfly-types": ["keyword"]},
                        "size": {"type": "number", "x-antfly-types": ["numeric"]},
                        "modified_at": {
                            "type": "string",
                            "format": "date-time",
                            "x-antfly-types": ["datetime"],
                        },
                    },
                }
            }
        },
    }
    _create_table_with_schema(stateful_api, shorthand_table, shorthand_schema)
    stateful_api.batch_write(
        shorthand_table,
        inserts=DOCUMENTS,
        sync_level="full_text",
    )

    filtered = stateful_api.query_table(
        shorthand_table,
        {
            "query": {"match_all": {}},
            "filter_query": {
                "field": "modified_at",
                "start": "2026-06-01T00:00:00Z",
                "end": "2026-12-01T00:00:00Z",
            },
            "limit": 3,
        },
    )
    assert set(_hit_ids(filtered)) == {"b", "c"}

    shorthand_capabilities = _capabilities_by_field(
        stateful_api.get_table(shorthand_table)
    )
    for field in ("label", "size", "modified_at"):
        capability = shorthand_capabilities[field]
        assert capability["sortable"] is False
        assert capability["sort_lifecycle_state"] == "unsupported"

    rejected = stateful_api._request(
        "POST",
        f"/tables/{shorthand_table}/query",
        {
            "query": {"match_all": {}},
            "limit": 3,
            "order_by": [{"field": "modified_at"}],
        },
    )
    assert rejected.status_code == 422
    rejection = rejected.json()
    assert rejection["sort_rejection_reason"] == "non_sortable_field"
    assert rejection["sort_rejection_detail"] == "non_sortable_field"
    assert rejection["sort_rejection_field"] == "modified_at"

    sortable_table = f"exact_sort_native_{time.time_ns()}"
    sortable_schema = {
        "default_type": "doc",
        "document_schemas": {
            "doc": {
                "schema": {
                    "type": "object",
                    "additionalProperties": True,
                    "properties": {
                        "title": {
                            "type": "string",
                            "x-antfly-field": {"type": "text"},
                        },
                        "label": {
                            "type": "string",
                            "x-antfly-field": {"type": "keyword", "sortable": True},
                        },
                        "size": {
                            "type": "number",
                            "x-antfly-field": {"type": "number", "sortable": True},
                        },
                        "modified_at": {
                            "type": "string",
                            "format": "date-time",
                            "x-antfly-field": {"type": "date", "sortable": True},
                        },
                    },
                }
            }
        },
    }
    _create_table_with_schema(stateful_api, sortable_table, sortable_schema)
    stateful_api.batch_write(
        sortable_table,
        inserts=DOCUMENTS,
        sync_level="full_text",
    )

    # Status is observational and must not scan cold columns. full_text remains
    # the public visibility barrier: the first sorted query validates only its
    # requested column and succeeds without a client-side status polling loop.
    ready = stateful_api.get_table(sortable_table)
    ready_capabilities = _capabilities_by_field(ready)
    for field in ("label", "size", "modified_at"):
        assert ready_capabilities[field]["sort_lifecycle_state"] == "declared"

    for field, expected in (
        ("label", ["a", "b", "c"]),
        ("size", ["a", "b", "c"]),
        ("modified_at", ["a", "b", "c"]),
    ):
        result = stateful_api.query_table(
            sortable_table,
            {
                "query": {"match_all": {}},
                "limit": 3,
                "order_by": [{"field": field}],
                "profile": True,
            },
        )
        assert _hit_ids(result) == expected
        sort_profile = result["responses"][0]["profile"]["sort"]
        assert sort_profile["plan"] == "native_doc_values_top_n"
        assert sort_profile["source"] == "doc_values_collector"
        assert sort_profile["source_load"] == "projected_source_after_page"

        warmed_capabilities = _capabilities_by_field(
            stateful_api.get_table(sortable_table)
        )
        assert warmed_capabilities[field]["sort_lifecycle_state"] in {
            "queryable",
            "accelerated",
        }

    first_numeric_page = stateful_api.query_table(
        sortable_table,
        {
            "query": {"match_all": {}},
            "limit": 1,
            "order_by": [{"field": "size"}],
        },
    )
    first_numeric_hit = first_numeric_page["responses"][0]["hits"]["hits"][0]
    assert first_numeric_hit["_id"] == "a"
    second_numeric_page = stateful_api.query_table(
        sortable_table,
        {
            "query": {"match_all": {}},
            "limit": 2,
            "order_by": [{"field": "size"}],
            "search_after": first_numeric_hit["_sort"],
        },
    )
    assert _hit_ids(second_numeric_page) == ["b", "c"]

    if stateful_api.supports_restart:
        stateful_api.restart_server()
        cold_capabilities = _capabilities_by_field(
            stateful_api.get_table(sortable_table)
        )
        assert cold_capabilities["modified_at"]["sort_lifecycle_state"] == "declared"
        # The public request performs its own cold-handle preflight and retry;
        # clients should not need to hide unexpected 4xx/5xx responses behind
        # a polling loop.
        cold_result = _sorted_query(
            stateful_api,
            sortable_table,
            "modified_at",
        )
        assert _hit_ids(cold_result) == ["a", "b", "c"]
        assert (
            cold_result["responses"][0]["profile"]["sort"]["plan"]
            == "native_doc_values_top_n"
        )
        warmed_capabilities = _capabilities_by_field(
            stateful_api.get_table(sortable_table)
        )
        assert warmed_capabilities["modified_at"]["sort_lifecycle_state"] == "queryable"
