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

"""Portable graph E2E tests adapted from the Go antfly suite."""

from __future__ import annotations

import time

import pytest
import requests

from helpers import assert_created_index, json_doc, query_hits_total_value, upsert, wait_until


pytestmark = pytest.mark.reuse_antfly_process


def _create_index(api, table_name: str, index_name: str, payload: dict) -> dict:
    if hasattr(api, "create_index"):
        return api.create_index(table_name, index_name, payload)
    return api.post(f"/tables/{table_name}/indexes/{index_name}", payload)


def _graph_result(result: dict, name: str) -> dict | None:
    responses = result.get("responses", [])
    if not responses:
        return None
    return responses[0].get("graph_results", {}).get(name)


def _query_graph_result(api, table_name: str, payload: dict, name: str) -> dict | None:
    return _graph_result(api.query_table(table_name, payload), name)


def _wait_for_graph_result(
    api,
    table_name: str,
    payload: dict,
    name: str,
    predicate,
    *,
    timeout_s: float = 120.0,
    interval_s: float = 0.5,
):
    return wait_until(
        lambda: (
            result
            if (result := _query_graph_result(api, table_name, payload, name)) is not None and predicate(result)
            else None
        ),
        timeout_s=timeout_s,
        interval_s=interval_s,
    )


def _two_hop_documents_ready(api, table_name: str, payload: dict) -> dict | None:
    result = _query_graph_result(api, table_name, payload, "two_hop")
    if result is None:
        return None
    rows = result.get("rows", [])
    return result if rows and all(alias in rows[0] for alias in ("a", "b", "c")) else None


def _try_batch_write(api, table_name: str, **kwargs) -> dict | None:
    try:
        return api.batch_write(table_name, **kwargs)
    except requests.RequestException:
        return None


def _try_query_table(api, table_name: str, payload: dict) -> dict | None:
    try:
        return api.query_table(table_name, payload)
    except requests.RequestException:
        return None


def _try_create_table(api, table_name: str, **kwargs) -> dict | None:
    try:
        return api.create_table(table_name, **kwargs)
    except requests.RequestException:
        return None


def _create_stateful_table(api, table_name: str, **kwargs) -> dict:
    created = wait_until(
        lambda: _try_create_table(api, table_name, **kwargs),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert created is not None
    return created


def _batch_write_stateful(api, table_name: str, **kwargs) -> dict:
    batch = wait_until(
        lambda: _try_batch_write(api, table_name, **kwargs),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert batch is not None
    return batch


def test_graph_neighbors_traverse_and_shortest_path(serverless_api):
    public_traverse_payload = {
        "graph_queries": {
            "traverse": {
                "index": "graph_idx",
                "traverse": {
                    "start": {"keys": ["alice"]},
                    "edge_types": ["cites"],
                    "max_depth": 2,
                    "include_paths": True,
                },
            }
        },
        "limit": 10,
    }
    public_shortest_payload = {
        "graph_queries": {
            "shortest": {
                "index": "graph_idx",
                "shortest_path": {
                    "from": {"key": "alice"},
                    "to": {"key": "carol"},
                    "edge_types": ["cites"],
                    "max_depth": 4,
                },
            }
        },
        "limit": 10,
    }
    chained_payload = {
        "graph_queries": {
            "first_hop": {
                "index": "graph_idx",
                "traverse": {"start": {"keys": ["alice"]}, "edge_types": ["cites"], "max_depth": 1},
            },
            "second_hop": {
                "index": "graph_idx",
                "traverse": {
                    "start": {"result_ref": "$graph_results.first_hop"},
                    "edge_types": ["cites"],
                    "max_depth": 1,
                },
            },
        },
        "limit": 10,
    }

    def public_neighbors_query() -> dict | None:
        try:
            result = serverless_api.query_table(
                "graph",
                {
                    "graph_queries": {
                        "neighbors": {
                            "index": "graph_idx",
                            "traverse": {
                                "start": {"keys": ["alice"]},
                                "edge_types": ["cites", "related"],
                                "max_depth": 1,
                            },
                        }
                    },
                    "limit": 10,
                },
            )
        except requests.HTTPError:
            return None
        graph_result = _graph_result(result, "neighbors")
        if graph_result is None or len(graph_result.get("nodes", [])) < 2:
            return None
        return result

    def neighbors_query() -> dict | None:
        try:
            neighbors = serverless_api.graph_neighbors(
                "graph",
                {
                    "doc_id": "alice",
                    "direction": "out",
                    "limit": 10,
                },
            )
        except requests.HTTPError:
            return None
        if neighbors["neighbor_count"] < 2:
            return None
        return neighbors

    serverless_api.ensure_table("graph", created_at_ns=200)
    serverless_api.ingest_table(
        "graph",
        timestamp_ns=300,
        mutations=[
            upsert(
                "alice",
                json_doc(
                    text="Alice",
                    graph_edges=[
                        {"target": "bob", "edge_type": "cites", "weight": 1.0},
                        {"target": "carol", "edge_type": "related", "weight": 0.5},
                    ],
                ),
            ),
            upsert(
                "bob",
                json_doc(
                    text="Bob",
                    graph_edges=[{"target": "carol", "edge_type": "cites", "weight": 1.0}],
                ),
            ),
            upsert("carol", json_doc(text="Carol")),
        ],
    )
    try:
        serverless_api.build_table("graph")
    except requests.HTTPError:
        pass

    neighbors = wait_until(neighbors_query, timeout_s=10.0, interval_s=0.1)
    assert neighbors is not None
    neighbor_ids = {item["doc_id"] for item in neighbors["neighbors"]}
    assert {"bob", "carol"} <= neighbor_ids

    public_neighbors = wait_until(public_neighbors_query, timeout_s=10.0, interval_s=0.1)
    assert public_neighbors is not None
    public_neighbor_result = _graph_result(public_neighbors, "neighbors")
    assert public_neighbor_result is not None
    assert len(public_neighbor_result["nodes"]) == 2
    assert [node["key"] for node in public_neighbor_result["nodes"]] == ["bob", "carol"]

    traverse = serverless_api.graph_traverse(
        "graph",
        {
            "start_doc_id": "alice",
            "direction": "out",
            "max_depth": 2,
            "limit": 10,
            "include_start": True,
        },
    )
    traversed = {item["doc_id"] for item in traverse["nodes"]}
    assert {"alice", "bob", "carol"} <= traversed

    public_traverse_result = _wait_for_graph_result(
        serverless_api,
        "graph",
        public_traverse_payload,
        "traverse",
        lambda result: len(result.get("nodes", [])) >= 2,
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert public_traverse_result is not None
    assert len(public_traverse_result["nodes"]) == 2
    assert [node["key"] for node in public_traverse_result["nodes"]] == ["bob", "carol"]
    assert public_traverse_result["nodes"][1]["path"] == ["alice", "bob", "carol"]

    shortest = serverless_api.graph_shortest_path(
        "graph",
        {
            "start_doc_id": "alice",
            "end_doc_id": "carol",
            "direction": "out",
            "max_depth": 3,
        },
    )
    assert shortest["found"] is True
    assert shortest["node_path"][0] == "alice"
    assert shortest["node_path"][-1] == "carol"

    public_shortest_result = _wait_for_graph_result(
        serverless_api,
        "graph",
        public_shortest_payload,
        "shortest",
        lambda result: len(result.get("paths") or []) >= 1,
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert public_shortest_result is not None
    assert len(public_shortest_result["paths"]) == 1
    assert public_shortest_result["nodes"] == []
    assert len(public_shortest_result["paths"]) == 1
    assert public_shortest_result["paths"][0]["nodes"] == ["alice", "bob", "carol"]

    chained = wait_until(
        lambda: (
            result
            if (result := _try_query_table(serverless_api, "graph", chained_payload)) is not None
            and (first_hop_result := _graph_result(result, "first_hop")) is not None
            and (second_hop_result := _graph_result(result, "second_hop")) is not None
            and [node["key"] for node in first_hop_result["nodes"]] == ["bob"]
            and [node["key"] for node in second_hop_result["nodes"]] == ["carol"]
            else None
        ),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert chained is not None
    first_hop_result = _graph_result(chained, "first_hop")
    second_hop_result = _graph_result(chained, "second_hop")
    assert first_hop_result is not None
    assert second_hop_result is not None
    assert [node["key"] for node in first_hop_result["nodes"]] == ["bob"]
    assert [node["key"] for node in second_hop_result["nodes"]] == ["carol"]

    from_search = serverless_api.query_table(
        "graph",
        {
            "full_text_search": {"query": "Alice"},
            "graph_queries": {
                "neighbors_from_search": {
                    "index": "graph_idx",
                    "traverse": {
                        "start": {"result_ref": "$query_results", "limit": 1},
                        "edge_types": ["cites", "related"],
                        "max_depth": 1,
                    },
                }
            },
            "limit": 10,
        },
    )
    from_search_result = _graph_result(from_search, "neighbors_from_search")
    assert from_search_result is not None
    assert query_hits_total_value(from_search["responses"][0]["hits"]) >= 1
    assert [node["key"] for node in from_search_result["nodes"]] == ["bob", "carol"]

    from_fused = wait_until(
        lambda: serverless_api.query_table(
            "graph",
            {
                "full_text_search": {"query": "Alice"},
                "graph_queries": {
                    "neighbors_from_fused": {
                        "index": "graph_idx",
                        "traverse": {
                            "start": {"result_ref": "$query_results", "limit": 1},
                            "edge_types": ["cites", "related"],
                            "max_depth": 1,
                        },
                    }
                },
                "limit": 10,
            },
        ),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert from_fused is not None
    from_fused_result = _graph_result(from_fused, "neighbors_from_fused")
    assert from_fused_result is not None
    assert [node["key"] for node in from_fused_result["nodes"]] == ["bob", "carol"]


def test_stateful_graph_neighbors_traverse_and_shortest_path(backup_api):
    table_name = f"graph_stateful_{time.time_ns()}"
    neighbors_payload = {
        "graph_queries": {
            "neighbors": {
                "index": "graph_idx",
                "traverse": {"start": {"keys": ["doc-a"]}, "edge_types": ["cites", "related"], "max_depth": 1},
            }
        },
        "limit": 10,
    }
    traverse_payload = {
        "graph_queries": {
            "traverse": {
                "index": "graph_idx",
                "traverse": {
                    "start": {"keys": ["doc-a"]},
                    "edge_types": ["cites"],
                    "max_depth": 2,
                    "include_paths": True,
                },
            }
        },
        "limit": 10,
    }
    shortest_payload = {
        "graph_queries": {
            "shortest": {
                "index": "graph_idx",
                "shortest_path": {
                    "from": {"key": "doc-a"},
                    "to": {"key": "doc-c"},
                    "edge_types": ["cites"],
                    "max_depth": 4,
                },
            }
        },
        "limit": 10,
    }
    chained_payload = {
        "graph_queries": {
            "first_hop": {
                "index": "graph_idx",
                "traverse": {"start": {"keys": ["doc-a"]}, "edge_types": ["cites"], "max_depth": 1},
            },
            "second_hop": {
                "index": "graph_idx",
                "traverse": {
                    "start": {"result_ref": "$graph_results.first_hop"},
                    "edge_types": ["cites"],
                    "max_depth": 1,
                },
            },
        },
        "limit": 10,
    }
    from_search_payload = {
        "full_text_search": {"query": "title:alpha"},
        "graph_queries": {
            "neighbors_from_search": {
                "index": "graph_idx",
                "traverse": {
                    "start": {"result_ref": "$query_results", "limit": 1},
                    "edge_types": ["cites", "related"],
                    "max_depth": 1,
                },
            }
        },
        "limit": 10,
    }
    from_fused_payload = {
        "full_text_search": {"query": "title:alpha"},
        "graph_queries": {
            "neighbors_from_fused": {
                "index": "graph_idx",
                "traverse": {
                    "start": {"result_ref": "$query_results", "limit": 1},
                    "edge_types": ["cites", "related"],
                    "max_depth": 1,
                },
            }
        },
        "limit": 10,
    }

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [
                    {"name": "cites"},
                    {"name": "related"},
                ],
            },
        ),
        "graph_idx",
        "graph",
    )

    batch = _batch_write_stateful(
        backup_api,
        table_name,
        inserts={
            "doc-a": {
                "title": "alpha",
                "_edges": {
                    "graph_idx": {
                        "cites": [{"target": "doc-b", "weight": 1.5}],
                        "related": [{"target": "doc-c", "weight": 0.5}],
                    }
                },
            },
            "doc-b": {
                "title": "beta",
                "_edges": {
                    "graph_idx": {
                        "cites": [{"target": "doc-c", "weight": 2.0}],
                    }
                },
            },
            "doc-c": {
                "title": "gamma",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    neighbor_result = _wait_for_graph_result(
        backup_api,
        table_name,
        neighbors_payload,
        "neighbors",
        lambda result: len(result.get("nodes", [])) >= 2,
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert neighbor_result is not None
    assert len(neighbor_result["nodes"]) == 2
    assert [node["key"] for node in neighbor_result["nodes"]] == ["doc-b", "doc-c"]

    traverse_result = _wait_for_graph_result(
        backup_api,
        table_name,
        traverse_payload,
        "traverse",
        lambda result: len(result.get("nodes", [])) >= 2,
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert traverse_result is not None
    assert len(traverse_result["nodes"]) == 2
    assert [node["key"] for node in traverse_result["nodes"]] == ["doc-b", "doc-c"]
    assert traverse_result["nodes"][1]["depth"] == 2
    assert traverse_result["nodes"][1]["path"] == ["doc-a", "doc-b", "doc-c"]

    shortest_result = _wait_for_graph_result(
        backup_api,
        table_name,
        shortest_payload,
        "shortest",
        lambda result: len(result.get("paths") or []) >= 1,
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert shortest_result is not None
    assert len(shortest_result["paths"]) == 1
    assert shortest_result["nodes"] == []
    assert len(shortest_result["paths"]) == 1
    assert shortest_result["paths"][0]["nodes"] == ["doc-a", "doc-b", "doc-c"]
    assert shortest_result["paths"][0]["length"] == 2

    chained = wait_until(
        lambda: (
            result
            if (result := _try_query_table(backup_api, table_name, chained_payload)) is not None
            and (first_hop_result := _graph_result(result, "first_hop")) is not None
            and (second_hop_result := _graph_result(result, "second_hop")) is not None
            and [node["key"] for node in first_hop_result["nodes"]] == ["doc-b"]
            and [node["key"] for node in second_hop_result["nodes"]] == ["doc-c"]
            else None
        ),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert chained is not None
    first_hop_result = _graph_result(chained, "first_hop")
    second_hop_result = _graph_result(chained, "second_hop")
    assert first_hop_result is not None
    assert second_hop_result is not None
    assert [node["key"] for node in first_hop_result["nodes"]] == ["doc-b"]
    assert [node["key"] for node in second_hop_result["nodes"]] == ["doc-c"]

    from_search_result = _wait_for_graph_result(
        backup_api,
        table_name,
        from_search_payload,
        "neighbors_from_search",
        lambda result: [node["key"] for node in result.get("nodes", [])] == ["doc-b", "doc-c"],
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert from_search_result is not None
    assert [node["key"] for node in from_search_result["nodes"]] == ["doc-b", "doc-c"]

    from_fused_result = _wait_for_graph_result(
        backup_api,
        table_name,
        from_fused_payload,
        "neighbors_from_fused",
        lambda result: [node["key"] for node in result.get("nodes", [])] == ["doc-b", "doc-c"],
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert from_fused_result is not None
    assert [node["key"] for node in from_fused_result["nodes"]] == ["doc-b", "doc-c"]


def test_serverless_graph_pattern_two_hop_and_documents(serverless_api):
    table_name = f"graph_pattern_serverless_{time.time_ns()}"

    serverless_api.ensure_table(table_name, created_at_ns=200)
    serverless_api.ingest_table(
        table_name,
        timestamp_ns=300,
        mutations=[
            upsert(
                "doc-a",
                json_doc(
                    title="alpha",
                    graph_edges=[{"target": "doc-b", "edge_type": "cites", "weight": 1.0}],
                ),
            ),
            upsert(
                "doc-b",
                json_doc(
                    title="beta",
                    graph_edges=[{"target": "doc-c", "edge_type": "cites", "weight": 1.0}],
                ),
            ),
            upsert("doc-c", json_doc(title="gamma")),
        ],
    )
    try:
        serverless_api.build_table(table_name)
    except requests.HTTPError:
        pass

    two_hop_match = {
        "anchor": "a",
        "nodes": {
            "a": {"filter": {"ids": ["doc-a"]}},
            "b": {"filter": {"term": "beta", "field": "title"}},
            "c": {"filter": {"prefix": "ga", "field": "title"}},
        },
        "edges": [
            {"from": "a", "to": "b", "types": ["cites"], "direction": "out"},
            {"from": "b", "to": "c", "types": ["cites"], "direction": "out"},
        ],
    }
    query_payload = {
        "graph_queries": {
            "two_hop": {
                "index": "graph_idx",
                "match": two_hop_match,
                "return": {"bindings": ["a", "b", "c"], "limit": 10},
            },
            "two_hop_count": {
                "index": "graph_idx",
                "match": two_hop_match,
                "return": {"aggregates": {"rows": {"count": "*"}}},
            },
        },
        "limit": 10,
    }

    graph_result = wait_until(
        lambda: _two_hop_documents_ready(serverless_api, table_name, query_payload),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert graph_result is not None
    row = graph_result["rows"][0]
    assert row["a"]["key"] == "doc-a"
    assert row["b"]["key"] == "doc-b"
    assert row["c"]["key"] == "doc-c"
    count_result = _query_graph_result(serverless_api, table_name, query_payload, "two_hop_count")
    assert count_result is not None
    assert count_result["aggregates"]["rows"] == {"value": "1", "exact": True}


def test_multi_batch_graph_push_preserves_boundary_error_and_existing_edges(backup_api):
    table_name = f"graph_transform_boundary_{time.time_ns()}"
    _create_stateful_table(backup_api, table_name, num_shards=1)
    assert_created_index(
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}],
            },
        ),
        "graph_idx",
        "graph",
    )
    seeded = _batch_write_stateful(
        backup_api,
        table_name,
        inserts={
            "doc-a": {
                "title": "alpha",
                "_edges": {"graph_idx": {"knows": [{"target": "doc-b", "weight": 1.0}]}},
            },
            "doc-b": {"title": "beta"},
            "doc-c": {"title": "gamma"},
        },
        sync_level="full_index",
    )
    assert seeded["inserted"] == 3

    # Transaction intents cannot yet carry projected graph deltas. The
    # distributed unit must preserve this exact storage-domain error across the
    # stable runtime ABI so the public handler rejects the request as 400.
    response = backup_api._request(
        "POST",
        "/batch",
        {
            "tables": {
                table_name: {
                    "transforms": [
                        {
                            "key": "doc-a",
                            "operations": [
                                {"op": "$set", "path": "title", "value": "must-not-commit"},
                                {
                                    "op": "$push",
                                    "path": "$._edges.graph_idx.knows",
                                    "value": {"target": "doc-c", "weight": 2.0},
                                },
                            ],
                        }
                    ],
                    "sync_level": "full_index",
                }
            },
            "sync_level": "full_index",
        },
    )
    assert response.status_code == 400, response.text
    assert backup_api.lookup_key(table_name, "doc-a")["title"] == "alpha"

    neighbors_payload = {
        "graph_queries": {
            "neighbors": {
                "index": "graph_idx",
                "traverse": {"start": {"keys": ["doc-a"]}, "edge_types": ["knows"], "direction": "out", "max_depth": 1},
            }
        },
        "limit": 10,
    }
    neighbor_result = _wait_for_graph_result(
        backup_api,
        table_name,
        neighbors_payload,
        "neighbors",
        lambda result: len(result.get("nodes", [])) == 1,
    )
    assert neighbor_result is not None
    assert [node["key"] for node in neighbor_result["nodes"]] == ["doc-b"]


def test_stateful_graph_field_edges_extract_and_update(backup_api):
    table_name = f"graph_field_edges_{time.time_ns()}"

    parent_query_payload = {
        "graph_queries": {
            "parent": {
                "index": "hierarchy",
                "match": {
                    "anchor": "child",
                    "nodes": {
                        "child": {"filter": {"ids": ["child"]}},
                        "parent": {},
                    },
                    "edges": [{"from": "child", "to": "parent", "types": ["child_of"], "direction": "out"}],
                },
                "return": {"bindings": ["parent"], "limit": 10},
            }
        },
        "limit": 10,
    }

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        _create_index(
            backup_api,
            table_name,
            "hierarchy",
            {
                "name": "hierarchy",
                "type": "graph",
                "edge_types": [
                    {
                        "name": "child_of",
                        "field": "parent_id",
                        "topology": "tree",
                    }
                ],
            },
        ),
        "hierarchy",
        "graph",
    )

    batch = _batch_write_stateful(
        backup_api,
        table_name,
        inserts={
            "root-a": {"title": "Root A"},
            "root-b": {"title": "Root B"},
            "child": {"title": "Child", "parent_id": "root-a"},
            "grandchild": {"title": "Grandchild", "parent_id": "child"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 4

    parent_result = wait_until(
        lambda: (
            result
            if (result := _query_graph_result(backup_api, table_name, parent_query_payload, "parent"))
            and result.get("rows")
            else None
        ),
        timeout_s=120.0,
        interval_s=0.25,
    )
    assert parent_result is not None
    assert len(parent_result["rows"]) == 1
    assert parent_result["rows"][0]["parent"]["key"] == "root-a"

    traverse = backup_api.query_table(
        table_name,
        {
            "graph_queries": {
                "traverse": {
                    "index": "hierarchy",
                    "traverse": {
                        "start": {"keys": ["grandchild"]},
                        "edge_types": ["child_of"],
                        "max_depth": 2,
                        "include_paths": True,
                    },
                }
            },
            "limit": 10,
        },
    )
    traverse_result = _graph_result(traverse, "traverse")
    assert traverse_result is not None
    assert len(traverse_result["nodes"]) == 2
    assert [node["key"] for node in traverse_result["nodes"]] == ["child", "root-a"]
    assert traverse_result["nodes"][1]["path"] == ["grandchild", "child", "root-a"]

    update = backup_api.batch_write(
        table_name,
        inserts={
            "child": {"title": "Child", "parent_id": "root-b"},
        },
        sync_level="full_index",
    )
    assert update["inserted"] == 1

    updated_parent_result = wait_until(
        lambda: (
            result
            if (result := _query_graph_result(backup_api, table_name, parent_query_payload, "parent"))
            and result.get("rows")
            and result["rows"][0]["parent"]["key"] == "root-b"
            else None
        ),
        timeout_s=120.0,
        interval_s=0.25,
    )
    assert updated_parent_result is not None
    assert len(updated_parent_result["rows"]) == 1
    assert updated_parent_result["rows"][0]["parent"]["key"] == "root-b"


def test_stateful_graph_pattern_two_hop_and_documents(backup_api):
    table_name = f"graph_pattern_two_hop_{time.time_ns()}"

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}],
            },
        ),
        "graph_idx",
        "graph",
    )

    batch = _batch_write_stateful(
        backup_api,
        table_name,
        inserts={
            "doc-a": {"title": "alpha", "_edges": {"graph_idx": {"knows": [{"target": "doc-b", "weight": 1.0}]}}},
            "doc-b": {"title": "beta", "_edges": {"graph_idx": {"knows": [{"target": "doc-c", "weight": 1.0}]}}},
            "doc-c": {"title": "gamma"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    query_payload = {
        "graph_queries": {
            "two_hop": {
                "index": "graph_idx",
                "match": {
                    "anchor": "a",
                    "nodes": {
                        "a": {"filter": {"ids": ["doc-a"]}},
                        "b": {},
                        "c": {},
                    },
                    "edges": [
                        {"from": "a", "to": "b", "types": ["knows"], "direction": "out"},
                        {"from": "b", "to": "c", "types": ["knows"], "direction": "out"},
                    ],
                },
                "return": {"bindings": ["a", "b", "c"], "limit": 10},
            }
        },
        "limit": 10,
    }
    graph_result = wait_until(
        lambda: _two_hop_documents_ready(backup_api, table_name, query_payload),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert graph_result is not None
    row = graph_result["rows"][0]
    assert row["a"]["key"] == "doc-a"
    assert row["b"]["key"] == "doc-b"
    assert row["c"]["key"] == "doc-c"


def test_stateful_graph_pattern_variable_length_and_cycle(backup_api):
    table_name = f"graph_pattern_cycle_{time.time_ns()}"

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}],
            },
        ),
        "graph_idx",
        "graph",
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc-a": {"title": "alpha", "_edges": {"graph_idx": {"knows": [{"target": "doc-b", "weight": 1.0}]}}},
            "doc-b": {"title": "beta", "_edges": {"graph_idx": {"knows": [{"target": "doc-c", "weight": 1.0}]}}},
            "doc-c": {"title": "gamma", "_edges": {"graph_idx": {"knows": [{"target": "doc-a", "weight": 1.0}]}}},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    query_payload = {
        "graph_queries": {
            "var_length": {
                "index": "graph_idx",
                "match": {
                    "anchor": "start",
                    "nodes": {
                        "start": {"filter": {"ids": ["doc-a"]}},
                        "end": {},
                    },
                    "edges": [
                        {
                            "from": "start",
                            "to": "end",
                            "types": ["knows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 2,
                        }
                    ],
                },
                "return": {"bindings": ["end"], "limit": 10},
            },
            "cycle": {
                "index": "graph_idx",
                "match": {
                    "anchor": "x",
                    "nodes": {"x": {"filter": {"ids": ["doc-a"]}}},
                    "edges": [
                        {"from": "x", "to": "x", "types": ["knows"], "direction": "out", "min_hops": 1, "max_hops": 3}
                    ],
                },
                "return": {"bindings": ["x"], "limit": 10},
            },
        },
        "limit": 10,
    }
    var_length = wait_until(
        lambda: (
            result
            if (result := _query_graph_result(backup_api, table_name, query_payload, "var_length")) is not None
            and len(result.get("rows", [])) >= 2
            else None
        ),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert var_length is not None
    assert {row["end"]["key"] for row in var_length["rows"]} >= {"doc-b", "doc-c"}
    assert all(list(row.keys()) == ["end"] for row in var_length["rows"])

    cycle = _query_graph_result(backup_api, table_name, query_payload, "cycle")
    assert cycle is not None
    assert cycle["rows"][0]["x"]["key"] == "doc-a"


def test_stateful_graph_pattern_diamond_and_edge_type_filter(backup_api):
    table_name = f"graph_pattern_diamond_{time.time_ns()}"

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert_created_index(
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}, {"name": "follows"}],
            },
        ),
        "graph_idx",
        "graph",
    )

    batch = wait_until(
        lambda: (
            _batch
            if (
                _batch := _try_batch_write(
                    backup_api,
                    table_name,
                    inserts={
                        "doc-a": {
                            "title": "alpha",
                            "_edges": {
                                "graph_idx": {
                                    "knows": [
                                        {"target": "doc-b", "weight": 1.0},
                                        {"target": "doc-c", "weight": 1.0},
                                    ]
                                }
                            },
                        },
                        "doc-b": {
                            "title": "beta",
                            "_edges": {"graph_idx": {"knows": [{"target": "doc-d", "weight": 1.0}]}},
                        },
                        "doc-c": {
                            "title": "gamma",
                            "_edges": {"graph_idx": {"knows": [{"target": "doc-d", "weight": 1.0}]}},
                        },
                        "doc-d": {"title": "delta"},
                        "doc-x": {
                            "title": "extra",
                            "_edges": {"graph_idx": {"follows": [{"target": "doc-d", "weight": 1.0}]}},
                        },
                    },
                    sync_level="full_index",
                )
            )
            is not None
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert batch is not None
    assert batch["inserted"] == 5

    query_payload = {
        "graph_queries": {
            "diamond": {
                "index": "graph_idx",
                "match": {
                    "anchor": "a",
                    "nodes": {
                        "a": {"filter": {"ids": ["doc-a"]}},
                        "middle": {},
                        "d": {},
                    },
                    "edges": [
                        {"from": "a", "to": "middle", "types": ["knows"], "direction": "out"},
                        {"from": "middle", "to": "d", "types": ["knows"], "direction": "out"},
                    ],
                },
                "return": {"bindings": ["middle", "d"], "limit": 10},
            },
            "edge_filter": {
                "index": "graph_idx",
                "match": {
                    "anchor": "a",
                    "nodes": {
                        "a": {"filter": {"ids": ["doc-a"]}},
                        "b": {},
                        "c": {},
                    },
                    "edges": [
                        {"from": "a", "to": "b", "types": ["knows"], "direction": "out"},
                        {"from": "b", "to": "c", "types": ["follows"], "direction": "out"},
                    ],
                },
                "return": {"bindings": ["a", "b", "c"], "limit": 10},
            },
        },
        "limit": 10,
    }
    diamond = wait_until(
        lambda: (
            result
            if (result := _query_graph_result(backup_api, table_name, query_payload, "diamond")) is not None
            and len(result.get("rows", [])) >= 2
            else None
        ),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert diamond is not None
    middles = {row["middle"]["key"] for row in diamond["rows"]}
    assert middles >= {"doc-b", "doc-c"}
    assert all(row["d"]["key"] == "doc-d" for row in diamond["rows"])

    edge_filter = _query_graph_result(backup_api, table_name, query_payload, "edge_filter")
    assert edge_filter is not None
    assert edge_filter.get("rows") in (None, [])


def test_stateful_graph_conjunctive_optional_negative_and_aggregates(backup_api):
    table_name = f"graph_pattern_relational_{time.time_ns()}"
    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name
    assert_created_index(
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}, {"name": "likes"}, {"name": "blocks"}],
            },
        ),
        "graph_idx",
        "graph",
    )
    batch = _batch_write_stateful(
        backup_api,
        table_name,
        inserts={
            "doc-a": {
                "title": "anchor",
                "_edges": {
                    "graph_idx": {
                        "knows": [
                            {"target": "doc-b", "weight": 1.0},
                            {"target": "doc-c", "weight": 1.0},
                        ]
                    }
                },
            },
            "doc-b": {
                "title": "with optional",
                "_edges": {
                    "graph_idx": {
                        "likes": [{"target": "doc-d", "weight": 1.0}],
                        "blocks": [{"target": "doc-c", "weight": 1.0}],
                    }
                },
            },
            "doc-c": {"title": "without optional"},
            "doc-d": {"title": "optional target"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 4

    branch = {
        "anchor": "a",
        "nodes": {"a": {"filter": {"ids": ["doc-a"]}}, "b": {}, "c": {}},
        "edges": [
            {"from": "a", "to": "b", "types": ["knows"]},
            {"from": "a", "to": "c", "types": ["knows"]},
        ],
        "where": {"not_equal": {"left": {"alias": "b"}, "right": {"alias": "c"}}},
    }
    query_payload = {
        "graph_queries": {
            "branch": {
                "index": "graph_idx",
                "match": branch,
                "return": {"bindings": ["b", "c"], "limit": 10},
            },
            "negative": {
                "index": "graph_idx",
                "match": {
                    **branch,
                    "where": {
                        "and": [
                            branch["where"],
                            {"not_exists": {"edges": [{"from": "b", "to": "c", "types": ["blocks"]}]}},
                        ]
                    },
                },
                "return": {"bindings": ["b", "c"], "limit": 10},
            },
            "optional": {
                "index": "graph_idx",
                "match": {
                    "anchor": "a",
                    "nodes": {"a": {"filter": {"ids": ["doc-a"]}}, "b": {}},
                    "edges": [{"from": "a", "to": "b", "types": ["knows"]}],
                    "optional": [
                        {
                            "nodes": {"liked": {}},
                            "edges": [{"from": "b", "to": "liked", "types": ["likes"]}],
                        }
                    ],
                },
                "return": {"bindings": ["b", "liked"], "limit": 10},
            },
            "counts": {
                "index": "graph_idx",
                "match": {
                    "anchor": "a",
                    "nodes": {"a": {"filter": {"ids": ["doc-a"]}}, "b": {}},
                    "edges": [{"from": "a", "to": "b", "types": ["knows"]}],
                },
                "return": {
                    "aggregates": {
                        "rows": {"count": "*"},
                        "neighbors": {"count": "b", "distinct": True},
                    }
                },
            },
        },
        "limit": 10,
    }

    results = wait_until(
        lambda: (
            value
            if (value := backup_api.query_table(table_name, query_payload))
            .get("responses", [{}])[0]
            .get("graph_results", {})
            .get("counts", {})
            .get("aggregates", {})
            .get("rows", {})
            .get("value")
            == "2"
            else None
        ),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert results is not None
    graph_results = results["responses"][0]["graph_results"]
    assert len(graph_results["branch"]["rows"]) == 2
    assert len(graph_results["negative"]["rows"]) == 1
    optional_rows = graph_results["optional"]["rows"]
    assert {row["b"]["key"] for row in optional_rows} == {"doc-b", "doc-c"}
    assert any(row["liked"] is None for row in optional_rows)
    assert any(row["liked"] and row["liked"]["key"] == "doc-d" for row in optional_rows)
    assert graph_results["counts"]["aggregates"]["neighbors"] == {"value": "2", "exact": True}


def test_stateful_graph_lsqb_q1_q9_exact_conformance(backup_api):
    """Exercise the nine authoritative LSQB patterns through the public graph DSL."""
    table_name = f"graph_lsqb_{time.time_ns()}"
    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    edge_types = [
        "IS_PART_OF",
        "IS_LOCATED_IN",
        "HAS_MEMBER",
        "CONTAINER_OF",
        "REPLY_OF",
        "HAS_TAG",
        "HAS_TYPE",
        "KNOWS",
        "HAS_CREATOR",
        "LIKES",
        "HAS_INTEREST",
    ]
    assert_created_index(
        _create_index(
            backup_api,
            table_name,
            "social",
            {
                "name": "social",
                "type": "graph",
                "edge_types": [{"name": edge_type} for edge_type in edge_types],
            },
        ),
        "social",
        "graph",
    )

    def doc(label: str, edges: dict | None = None) -> dict:
        value = {"type": label}
        if edges:
            value["_edges"] = {"social": edges}
        return value

    inserts = {
        "country": doc("Country"),
        "city-1": doc("City", {"IS_PART_OF": [{"target": "country"}]}),
        "city-2": doc("City", {"IS_PART_OF": [{"target": "country"}]}),
        "city-3": doc("City", {"IS_PART_OF": [{"target": "country"}]}),
        "person-a": doc(
            "Person",
            {
                "IS_LOCATED_IN": [{"target": "city-1"}],
                "KNOWS": [{"target": "person-b"}],
            },
        ),
        "person-b": doc(
            "Person",
            {
                "IS_LOCATED_IN": [{"target": "city-2"}],
                "KNOWS": [{"target": "person-c"}],
            },
        ),
        "person-c": doc(
            "Person",
            {
                "IS_LOCATED_IN": [{"target": "city-3"}],
                "KNOWS": [{"target": "person-a"}],
            },
        ),
        "forum": doc(
            "Forum",
            {
                "HAS_MEMBER": [{"target": "person-a"}],
                "CONTAINER_OF": [{"target": "post-q1"}],
            },
        ),
        "post-q1": doc("Post"),
        "comment-q1": doc(
            "Comment",
            {
                "REPLY_OF": [{"target": "post-q1"}],
                "HAS_TAG": [{"target": "tag-q1"}],
            },
        ),
        "tag-q1": doc("Tag", {"HAS_TYPE": [{"target": "tag-class"}]}),
        "tag-class": doc("TagClass"),
        "comment-q2": doc(
            "Comment",
            {
                "HAS_CREATOR": [{"target": "person-a"}],
                "REPLY_OF": [{"target": "post-q2"}],
            },
        ),
        "post-q2": doc("Post", {"HAS_CREATOR": [{"target": "person-b"}]}),
        "chain-a": doc("Person", {"KNOWS": [{"target": "chain-b"}]}),
        "chain-b": doc("Person", {"KNOWS": [{"target": "chain-c"}]}),
        "chain-c": doc("Person", {"HAS_INTEREST": [{"target": "interest-tag"}]}),
        "interest-tag": doc("Tag"),
        "creator": doc("Person"),
        "liker": doc("Person", {"LIKES": [{"target": "message-optional"}]}),
        "message-optional": doc(
            "Message",
            {
                "HAS_TAG": [{"target": "tag-q4"}],
                "HAS_CREATOR": [{"target": "creator"}],
            },
        ),
        "message-no-optional": doc(
            "Message",
            {
                "HAS_TAG": [{"target": "tag-q4"}],
                "HAS_CREATOR": [{"target": "creator"}],
            },
        ),
        "comment-optional": doc("Comment", {"REPLY_OF": [{"target": "message-optional"}]}),
        "tag-q4": doc("Tag"),
        "message-q5": doc("Message", {"HAS_TAG": [{"target": "tag-left"}]}),
        "comment-q5": doc(
            "Comment",
            {
                "REPLY_OF": [{"target": "message-q5"}],
                "HAS_TAG": [{"target": "tag-right"}],
            },
        ),
        "comment-q8-blocked": doc(
            "Comment",
            {
                "REPLY_OF": [{"target": "message-q5"}],
                "HAS_TAG": [{"target": "tag-left"}, {"target": "tag-right"}],
            },
        ),
        "tag-left": doc("Tag"),
        "tag-right": doc("Tag"),
    }
    batch = _batch_write_stateful(
        backup_api,
        table_name,
        inserts=inserts,
        sync_level="full_index",
    )
    assert batch["inserted"] == len(inserts)

    def node(label: str) -> dict:
        return {"filter": {"term": label, "field": "type"}}

    def count_query(nodes: dict, edges: list, *, where=None, optional=None) -> dict:
        match = {"anchor": next(iter(nodes)), "nodes": nodes, "edges": edges}
        if where is not None:
            match["where"] = where
        if optional is not None:
            match["optional"] = optional
        return {
            "index": "social",
            "match": match,
            "return": {"aggregates": {"count": {"count": "*"}}},
        }

    neq = lambda left, right: {"not_equal": {"left": {"alias": left}, "right": {"alias": right}}}
    queries = {
        "q1": count_query(
            {
                "country": node("Country"),
                "city": node("City"),
                "person": node("Person"),
                "forum": node("Forum"),
                "post": node("Post"),
                "comment": node("Comment"),
                "tag": node("Tag"),
                "tag_class": node("TagClass"),
            },
            [
                {"from": "city", "to": "country", "types": ["IS_PART_OF"]},
                {"from": "person", "to": "city", "types": ["IS_LOCATED_IN"]},
                {"from": "forum", "to": "person", "types": ["HAS_MEMBER"]},
                {"from": "forum", "to": "post", "types": ["CONTAINER_OF"]},
                {"from": "comment", "to": "post", "types": ["REPLY_OF"]},
                {"from": "comment", "to": "tag", "types": ["HAS_TAG"]},
                {"from": "tag", "to": "tag_class", "types": ["HAS_TYPE"]},
            ],
        ),
        "q2": count_query(
            {
                "person1": node("Person"),
                "person2": node("Person"),
                "comment": node("Comment"),
                "post": node("Post"),
            },
            [
                {"from": "person1", "to": "person2", "types": ["KNOWS"], "direction": "both"},
                {"from": "comment", "to": "person1", "types": ["HAS_CREATOR"]},
                {"from": "comment", "to": "post", "types": ["REPLY_OF"]},
                {"from": "post", "to": "person2", "types": ["HAS_CREATOR"]},
            ],
        ),
        "q3": count_query(
            {
                "country": node("Country"),
                "person1": node("Person"),
                "city1": node("City"),
                "person2": node("Person"),
                "city2": node("City"),
                "person3": node("Person"),
                "city3": node("City"),
            },
            [
                {"from": "person1", "to": "city1", "types": ["IS_LOCATED_IN"]},
                {"from": "city1", "to": "country", "types": ["IS_PART_OF"]},
                {"from": "person2", "to": "city2", "types": ["IS_LOCATED_IN"]},
                {"from": "city2", "to": "country", "types": ["IS_PART_OF"]},
                {"from": "person3", "to": "city3", "types": ["IS_LOCATED_IN"]},
                {"from": "city3", "to": "country", "types": ["IS_PART_OF"]},
                {"from": "person1", "to": "person2", "types": ["KNOWS"], "direction": "both"},
                {"from": "person2", "to": "person3", "types": ["KNOWS"], "direction": "both"},
                {"from": "person3", "to": "person1", "types": ["KNOWS"], "direction": "both"},
            ],
        ),
        "q4": count_query(
            {
                "tag": node("Tag"),
                "message": node("Message"),
                "creator": node("Person"),
                "liker": node("Person"),
                "comment": node("Comment"),
            },
            [
                {"from": "message", "to": "tag", "types": ["HAS_TAG"]},
                {"from": "message", "to": "creator", "types": ["HAS_CREATOR"]},
                {"from": "liker", "to": "message", "types": ["LIKES"]},
                {"from": "comment", "to": "message", "types": ["REPLY_OF"]},
            ],
        ),
        "q5": count_query(
            {
                "tag1": node("Tag"),
                "message": node("Message"),
                "comment": node("Comment"),
                "tag2": node("Tag"),
            },
            [
                {"from": "message", "to": "tag1", "types": ["HAS_TAG"]},
                {"from": "comment", "to": "message", "types": ["REPLY_OF"]},
                {"from": "comment", "to": "tag2", "types": ["HAS_TAG"]},
            ],
            where=neq("tag1", "tag2"),
        ),
        "q6": count_query(
            {
                "person1": node("Person"),
                "person2": node("Person"),
                "person3": node("Person"),
                "tag": node("Tag"),
            },
            [
                {"from": "person1", "to": "person2", "types": ["KNOWS"], "direction": "both"},
                {"from": "person2", "to": "person3", "types": ["KNOWS"], "direction": "both"},
                {"from": "person3", "to": "tag", "types": ["HAS_INTEREST"]},
            ],
            where=neq("person1", "person3"),
        ),
        "q7": count_query(
            {
                "tag": node("Tag"),
                "message": node("Message"),
                "creator": node("Person"),
            },
            [
                {"from": "message", "to": "tag", "types": ["HAS_TAG"]},
                {"from": "message", "to": "creator", "types": ["HAS_CREATOR"]},
            ],
            optional=[
                {
                    "nodes": {"liker": node("Person")},
                    "edges": [{"from": "liker", "to": "message", "types": ["LIKES"]}],
                },
                {
                    "nodes": {"comment": node("Comment")},
                    "edges": [{"from": "comment", "to": "message", "types": ["REPLY_OF"]}],
                },
            ],
        ),
        "q8": count_query(
            {
                "tag1": node("Tag"),
                "message": node("Message"),
                "comment": node("Comment"),
                "tag2": node("Tag"),
            },
            [
                {"from": "message", "to": "tag1", "types": ["HAS_TAG"]},
                {"from": "comment", "to": "message", "types": ["REPLY_OF"]},
                {"from": "comment", "to": "tag2", "types": ["HAS_TAG"]},
            ],
            where={
                "and": [
                    {"not_exists": {"edges": [{"from": "comment", "to": "tag1", "types": ["HAS_TAG"]}]}},
                    neq("tag1", "tag2"),
                ]
            },
        ),
        "q9": count_query(
            {
                "person1": node("Person"),
                "person2": node("Person"),
                "person3": node("Person"),
                "tag": node("Tag"),
            },
            [
                {"from": "person1", "to": "person2", "types": ["KNOWS"], "direction": "both"},
                {"from": "person2", "to": "person3", "types": ["KNOWS"], "direction": "both"},
                {"from": "person3", "to": "tag", "types": ["HAS_INTEREST"]},
            ],
            where={
                "and": [
                    {
                        "not_exists": {
                            "edges": [
                                {
                                    "from": "person1",
                                    "to": "person3",
                                    "types": ["KNOWS"],
                                    "direction": "both",
                                }
                            ]
                        }
                    },
                    neq("person1", "person3"),
                ]
            },
        ),
    }
    expected = {"q1": "1", "q2": "1", "q3": "6", "q4": "1", "q5": "2", "q6": "1", "q7": "2", "q8": "1", "q9": "1"}

    def exact_counts() -> dict | None:
        response = backup_api.query_table(table_name, {"graph_queries": queries, "limit": 10})
        graph_results = response.get("responses", [{}])[0].get("graph_results", {})
        actual = {name: result.get("aggregates", {}).get("count") for name, result in graph_results.items()}
        if all(actual.get(name) == {"value": count, "exact": True} for name, count in expected.items()):
            return actual
        return None

    counts = wait_until(exact_counts, timeout_s=120.0, interval_s=0.5)
    assert counts == {name: {"value": count, "exact": True} for name, count in expected.items()}


def test_stateful_graph_pattern_max_results_limit(backup_api):
    table_name = f"graph_pattern_limit_{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=3)
    assert created["name"] == table_name

    assert_created_index(
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}],
            },
        ),
        "graph_idx",
        "graph",
    )

    batch = _batch_write_stateful(
        backup_api,
        table_name,
        inserts={
            "doc-a": {
                "title": "alpha",
                "_edges": {
                    "graph_idx": {
                        "knows": [
                            {"target": "doc-b", "weight": 1.0},
                            {"target": "doc-c", "weight": 1.0},
                            {"target": "doc-d", "weight": 1.0},
                        ]
                    }
                },
            },
            "doc-b": {"title": "beta"},
            "doc-c": {"title": "gamma"},
            "doc-d": {"title": "delta"},
            "doc-x": {
                "title": "second anchor",
                "_edges": {
                    "graph_idx": {
                        "knows": [
                            {"target": "doc-y", "weight": 1.0},
                            {"target": "doc-z", "weight": 1.0},
                        ]
                    }
                },
            },
            "doc-y": {"title": "epsilon"},
            "doc-z": {"title": "zeta"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 7

    query_payload = {
        "graph_queries": {
            "limited": {
                "index": "graph_idx",
                "match": {
                    "anchor": "a",
                    "nodes": {
                        "a": {},
                        "b": {},
                    },
                    "edges": [{"from": "a", "to": "b", "types": ["knows"], "direction": "out"}],
                },
                "return": {"bindings": ["b"], "limit": 2},
            },
            "counts": {
                "index": "graph_idx",
                "match": {
                    "anchor": "a",
                    "nodes": {"a": {}, "b": {}},
                    "edges": [{"from": "a", "to": "b", "types": ["knows"], "direction": "out"}],
                },
                "return": {
                    "aggregates": {
                        "rows": {"count": "*"},
                        "neighbors": {"count": "b", "distinct": True},
                    }
                },
            },
        },
        "limit": 10,
    }
    limited = wait_until(
        lambda: (
            result
            if (result := _query_graph_result(backup_api, table_name, query_payload, "limited")) is not None
            and len(result.get("rows", [])) == 2
            else None
        ),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert limited is not None
    assert len(limited["rows"]) == 2
    assert limited["stats"] == {"returned_items": 2, "truncated": True}
    counts = wait_until(
        lambda: (
            result
            if (result := _query_graph_result(backup_api, table_name, query_payload, "counts")) is not None
            and result.get("aggregates", {}).get("rows", {}).get("value") == "5"
            else None
        ),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert counts is not None
    assert counts["aggregates"] == {
        "rows": {"value": "5", "exact": True},
        "neighbors": {"value": "5", "exact": True},
    }
