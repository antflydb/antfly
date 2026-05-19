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
import requests

from helpers import json_doc, upsert, wait_until


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
            if (result := _query_graph_result(api, table_name, payload, name)) is not None
            and predicate(result)
            else None
        ),
        timeout_s=timeout_s,
        interval_s=interval_s,
    )


def _two_hop_documents_ready(api, table_name: str, payload: dict) -> dict | None:
    result = _query_graph_result(api, table_name, payload, "two_hop")
    if result is None or result.get("total", 0) < 1:
        return None
    matches = result.get("matches", [])
    if not matches:
        return None
    bindings = matches[0].get("bindings", {})
    for alias in ("a", "b", "c"):
        document = bindings.get(alias, {}).get("document")
        if document is None:
            return None
    return result


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
        "graph_searches": {
            "traverse": {
                "type": "traverse",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["alice"]},
                "params": {
                    "edge_types": ["cites"],
                    "max_depth": 2,
                    "include_paths": True,
                },
            }
        },
        "limit": 10,
    }
    public_shortest_payload = {
        "graph_searches": {
            "shortest": {
                "type": "shortest_path",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["alice"]},
                "target_nodes": {"keys": ["carol"]},
                "params": {
                    "edge_types": ["cites"],
                    "max_depth": 4,
                    "include_paths": True,
                },
            }
        },
        "limit": 10,
    }
    chained_payload = {
        "graph_searches": {
            "first_hop": {
                "type": "neighbors",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["alice"]},
                "params": {"edge_types": ["cites"]},
            },
            "second_hop": {
                "type": "neighbors",
                "index_name": "graph_idx",
                "start_nodes": {"result_ref": "$graph_results.first_hop"},
                "params": {"edge_types": ["cites"]},
            },
        },
        "limit": 10,
    }

    def public_neighbors_query() -> dict | None:
        try:
            result = serverless_api.query_table(
                "graph",
                {
                    "graph_searches": {
                        "neighbors": {
                            "type": "neighbors",
                            "index_name": "graph_idx",
                            "start_nodes": {"keys": ["alice"]},
                            "params": {"edge_types": ["cites", "related"]},
                        }
                    },
                    "limit": 10,
                },
            )
        except requests.HTTPError:
            return None
        graph_result = _graph_result(result, "neighbors")
        if graph_result is None or graph_result["total"] < 2:
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
    assert public_neighbor_result["type"] == "neighbors"
    assert public_neighbor_result["total"] == 2
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
        lambda result: result.get("total", 0) >= 2,
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert public_traverse_result is not None
    assert public_traverse_result["type"] == "traverse"
    assert public_traverse_result["total"] == 2
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
        lambda result: result.get("total", 0) >= 1 and len(result.get("paths") or []) >= 1,
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert public_shortest_result is not None
    assert public_shortest_result["type"] == "shortest_path"
    assert public_shortest_result["total"] == 1
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
            "graph_searches": {
                "neighbors_from_search": {
                    "type": "neighbors",
                    "index_name": "graph_idx",
                    "start_nodes": {"result_ref": "$full_text_results", "limit": 1},
                    "params": {"edge_types": ["cites", "related"]},
                }
            },
            "limit": 10,
        },
    )
    from_search_result = _graph_result(from_search, "neighbors_from_search")
    assert from_search_result is not None
    assert from_search["responses"][0]["hits"]["total"] >= 1
    assert [node["key"] for node in from_search_result["nodes"]] == ["bob", "carol"]

    from_fused = wait_until(
        lambda: serverless_api.query_table(
            "graph",
            {
                "full_text_search": {"query": "Alice"},
                "graph_searches": {
                    "neighbors_from_fused": {
                        "type": "neighbors",
                        "index_name": "graph_idx",
                        "start_nodes": {"result_ref": "$fused_results", "limit": 1},
                        "params": {"edge_types": ["cites", "related"]},
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
        "graph_searches": {
            "neighbors": {
                "type": "neighbors",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "params": {"edge_types": ["cites", "related"]},
            }
        },
        "limit": 10,
    }
    traverse_payload = {
        "graph_searches": {
            "traverse": {
                "type": "traverse",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "params": {
                    "edge_types": ["cites"],
                    "max_depth": 2,
                    "include_paths": True,
                },
            }
        },
        "limit": 10,
    }
    shortest_payload = {
        "graph_searches": {
            "shortest": {
                "type": "shortest_path",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "target_nodes": {"keys": ["doc-c"]},
                "params": {
                    "edge_types": ["cites"],
                    "max_depth": 4,
                    "include_paths": True,
                },
            }
        },
        "limit": 10,
    }
    chained_payload = {
        "graph_searches": {
            "first_hop": {
                "type": "neighbors",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "params": {"edge_types": ["cites"]},
            },
            "second_hop": {
                "type": "neighbors",
                "index_name": "graph_idx",
                "start_nodes": {"result_ref": "$graph_results.first_hop"},
                "params": {"edge_types": ["cites"]},
            },
        },
        "limit": 10,
    }
    from_search_payload = {
        "full_text_search": {"query": "title:alpha"},
        "graph_searches": {
            "neighbors_from_search": {
                "type": "neighbors",
                "index_name": "graph_idx",
                "start_nodes": {"result_ref": "$full_text_results", "limit": 1},
                "params": {"edge_types": ["cites", "related"]},
            }
        },
        "limit": 10,
    }
    from_fused_payload = {
        "full_text_search": {"query": "title:alpha"},
        "graph_searches": {
            "neighbors_from_fused": {
                "type": "neighbors",
                "index_name": "graph_idx",
                "start_nodes": {"result_ref": "$fused_results", "limit": 1},
                "params": {"edge_types": ["cites", "related"]},
            }
        },
        "limit": 10,
    }

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
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
        )
        == {}
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
        lambda result: result.get("total", 0) >= 2,
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert neighbor_result is not None
    assert neighbor_result["type"] == "neighbors"
    assert neighbor_result["total"] == 2
    assert [node["key"] for node in neighbor_result["nodes"]] == ["doc-b", "doc-c"]

    traverse_result = _wait_for_graph_result(
        backup_api,
        table_name,
        traverse_payload,
        "traverse",
        lambda result: result.get("total", 0) >= 2,
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert traverse_result is not None
    assert traverse_result["type"] == "traverse"
    assert traverse_result["total"] == 2
    assert [node["key"] for node in traverse_result["nodes"]] == ["doc-b", "doc-c"]
    assert traverse_result["nodes"][1]["depth"] == 2
    assert traverse_result["nodes"][1]["path"] == ["doc-a", "doc-b", "doc-c"]

    shortest_result = _wait_for_graph_result(
        backup_api,
        table_name,
        shortest_payload,
        "shortest",
        lambda result: result.get("total", 0) >= 1 and len(result.get("paths") or []) >= 1,
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert shortest_result is not None
    assert shortest_result["type"] == "shortest_path"
    assert shortest_result["total"] == 1
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

    query_payload = {
        "graph_searches": {
            "two_hop": {
                "type": "pattern",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "pattern": [
                    {"alias": "a"},
                    {
                        "alias": "b",
                        "node_filter": {"filter_query": {"term": "beta", "field": "title"}},
                        "edge": {
                            "types": ["cites"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                    {
                        "alias": "c",
                        "node_filter": {"filter_query": {"prefix": "ga", "field": "title"}},
                        "edge": {
                            "types": ["cites"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                ],
                "include_documents": True,
                "fields": ["title"],
            }
        },
        "limit": 10,
    }

    graph_result = wait_until(
        lambda: _two_hop_documents_ready(serverless_api, table_name, query_payload),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert graph_result is not None
    assert graph_result["type"] == "pattern"
    assert graph_result["total"] >= 1
    assert graph_result["nodes"] == []
    assert graph_result["paths"] == []

    match = graph_result["matches"][0]
    assert match["bindings"]["a"]["key"] == "doc-a"
    assert match["bindings"]["b"]["key"] == "doc-b"
    assert match["bindings"]["c"]["key"] == "doc-c"
    assert match["bindings"]["a"]["document"]["title"] == "alpha"
    assert match["bindings"]["b"]["document"]["title"] == "beta"
    assert match["bindings"]["c"]["document"]["title"] == "gamma"
    assert len(match["path"]) == 2
    assert match["path"][0]["source"] == "doc-a"
    assert match["path"][1]["target"] == "doc-c"


def test_stateful_graph_field_edges_extract_and_update(backup_api):
    table_name = f"graph_field_edges_{time.time_ns()}"

    parent_query_payload = {
        "graph_searches": {
            "parent": {
                "type": "pattern",
                "index_name": "hierarchy",
                "start_nodes": {"keys": ["child"]},
                "pattern": [
                    {"alias": "child"},
                    {
                        "alias": "parent",
                        "edge": {
                            "types": ["child_of"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                ],
            }
        },
        "limit": 10,
    }

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
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
        )
        == {}
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
            and result.get("matches")
            else None
        ),
        timeout_s=120.0,
        interval_s=0.25,
    )
    assert parent_result is not None
    assert len(parent_result["matches"]) == 1
    assert parent_result["matches"][0]["bindings"]["parent"]["key"] == "root-a"

    traverse = backup_api.query_table(
        table_name,
        {
            "graph_searches": {
                "traverse": {
                    "type": "traverse",
                    "index_name": "hierarchy",
                    "start_nodes": {"keys": ["grandchild"]},
                    "params": {
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
    assert traverse_result["total"] == 2
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
            and result.get("matches")
            and result["matches"][0]["bindings"]["parent"]["key"] == "root-b"
            else None
        ),
        timeout_s=120.0,
        interval_s=0.25,
    )
    assert updated_parent_result is not None
    assert len(updated_parent_result["matches"]) == 1
    assert updated_parent_result["matches"][0]["bindings"]["parent"]["key"] == "root-b"


def test_stateful_graph_pattern_two_hop_and_documents(backup_api):
    table_name = f"graph_pattern_two_hop_{time.time_ns()}"

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}],
            },
        )
        == {}
    )

    batch = _batch_write_stateful(
        backup_api,
        table_name,
        inserts={
            "doc-a": {
                "title": "alpha",
                "_edges": {"graph_idx": {"knows": [{"target": "doc-b", "weight": 1.0}]}}
            },
            "doc-b": {
                "title": "beta",
                "_edges": {"graph_idx": {"knows": [{"target": "doc-c", "weight": 1.0}]}}
            },
            "doc-c": {"title": "gamma"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    query_payload = {
        "graph_searches": {
            "two_hop": {
                "type": "pattern",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "pattern": [
                    {"alias": "a"},
                    {
                        "alias": "b",
                        "edge": {
                            "types": ["knows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                    {
                        "alias": "c",
                        "edge": {
                            "types": ["knows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                ],
                "include_documents": True,
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
    assert graph_result["type"] == "pattern"
    assert graph_result["total"] >= 1
    assert graph_result["nodes"] == []
    assert graph_result["paths"] == []

    match = graph_result["matches"][0]
    assert match["bindings"]["a"]["key"] == "doc-a"
    assert match["bindings"]["b"]["key"] == "doc-b"
    assert match["bindings"]["c"]["key"] == "doc-c"
    assert match["bindings"]["a"]["document"]["title"] == "alpha"
    assert match["bindings"]["b"]["document"]["title"] == "beta"
    assert match["bindings"]["c"]["document"]["title"] == "gamma"
    assert len(match["path"]) == 2
    assert match["path"][0]["source"] == "doc-a"
    assert match["path"][1]["target"] == "doc-c"


def test_stateful_graph_pattern_variable_length_and_cycle(backup_api):
    table_name = f"graph_pattern_cycle_{time.time_ns()}"

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}],
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc-a": {
                "title": "alpha",
                "_edges": {"graph_idx": {"knows": [{"target": "doc-b", "weight": 1.0}]}}
            },
            "doc-b": {
                "title": "beta",
                "_edges": {"graph_idx": {"knows": [{"target": "doc-c", "weight": 1.0}]}}
            },
            "doc-c": {
                "title": "gamma",
                "_edges": {"graph_idx": {"knows": [{"target": "doc-a", "weight": 1.0}]}}
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    query_payload = {
        "graph_searches": {
            "var_length": {
                "type": "pattern",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "pattern": [
                    {"alias": "start"},
                    {
                        "alias": "end",
                        "edge": {
                            "types": ["knows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 2,
                        },
                    },
                ],
                "return_aliases": ["end"],
            },
            "cycle": {
                "type": "pattern",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "pattern": [
                    {"alias": "x"},
                    {
                        "alias": "x",
                        "edge": {
                            "types": ["knows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 3,
                        },
                    },
                ],
            },
        },
        "limit": 10,
    }
    var_length = wait_until(
        lambda: (
            result
            if (result := _query_graph_result(backup_api, table_name, query_payload, "var_length")) is not None
            and result.get("total", 0) >= 2
            else None
        ),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert var_length is not None
    assert var_length["type"] == "pattern"
    assert var_length["total"] >= 2
    assert {match["bindings"]["end"]["key"] for match in var_length["matches"]} >= {"doc-b", "doc-c"}
    assert all(list(match["bindings"].keys()) == ["end"] for match in var_length["matches"])

    cycle = _query_graph_result(backup_api, table_name, query_payload, "cycle")
    assert cycle is not None
    assert cycle["type"] == "pattern"
    assert cycle["total"] >= 1
    assert cycle["matches"][0]["bindings"]["x"]["key"] == "doc-a"
    assert len(cycle["matches"][0]["path"]) == 3


def test_stateful_graph_pattern_diamond_and_edge_type_filter(backup_api):
    table_name = f"graph_pattern_diamond_{time.time_ns()}"

    created = _create_stateful_table(backup_api, table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}, {"name": "follows"}],
            },
        )
        == {}
    )

    batch = wait_until(
        lambda: _batch if (_batch := _try_batch_write(
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
                    "_edges": {"graph_idx": {"knows": [{"target": "doc-d", "weight": 1.0}]}}
                },
                "doc-c": {
                    "title": "gamma",
                    "_edges": {"graph_idx": {"knows": [{"target": "doc-d", "weight": 1.0}]}}
                },
                "doc-d": {"title": "delta"},
                "doc-x": {
                    "title": "extra",
                    "_edges": {"graph_idx": {"follows": [{"target": "doc-d", "weight": 1.0}]}}
                },
            },
            sync_level="full_index",
        )) is not None else None,
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert batch is not None
    assert batch["inserted"] == 5

    query_payload = {
        "graph_searches": {
            "diamond": {
                "type": "pattern",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "pattern": [
                    {"alias": "a"},
                    {
                        "alias": "middle",
                        "edge": {
                            "types": ["knows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                    {
                        "alias": "d",
                        "edge": {
                            "types": ["knows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                ],
            },
            "edge_filter": {
                "type": "pattern",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "pattern": [
                    {"alias": "a"},
                    {
                        "alias": "b",
                        "edge": {
                            "types": ["knows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                    {
                        "alias": "c",
                        "edge": {
                            "types": ["follows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                ],
            },
        },
        "limit": 10,
    }
    diamond = wait_until(
        lambda: (
            result
            if (result := _query_graph_result(backup_api, table_name, query_payload, "diamond")) is not None
            and result.get("total", 0) >= 2
            else None
        ),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert diamond is not None
    assert diamond["type"] == "pattern"
    assert diamond["total"] >= 2
    middles = {match["bindings"]["middle"]["key"] for match in diamond["matches"]}
    assert middles >= {"doc-b", "doc-c"}
    assert all(match["bindings"]["d"]["key"] == "doc-d" for match in diamond["matches"])

    edge_filter = _query_graph_result(backup_api, table_name, query_payload, "edge_filter")
    assert edge_filter is not None
    assert edge_filter["type"] == "pattern"
    assert edge_filter["total"] == 0
    assert edge_filter.get("matches") in (None, [])


def test_stateful_graph_pattern_max_results_limit(backup_api):
    table_name = f"graph_pattern_limit_{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        _create_index(
            backup_api,
            table_name,
            "graph_idx",
            {
                "name": "graph_idx",
                "type": "graph",
                "edge_types": [{"name": "knows"}],
            },
        )
        == {}
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
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 4

    query_payload = {
        "graph_searches": {
            "limited": {
                "type": "pattern",
                "index_name": "graph_idx",
                "start_nodes": {"keys": ["doc-a"]},
                "pattern": [
                    {"alias": "a"},
                    {
                        "alias": "b",
                        "edge": {
                            "types": ["knows"],
                            "direction": "out",
                            "min_hops": 1,
                            "max_hops": 1,
                        },
                    },
                ],
                "return_aliases": ["b"],
                "params": {"max_results": 2},
            }
        },
        "limit": 10,
    }
    limited = wait_until(
        lambda: (
            result
            if (result := _query_graph_result(backup_api, table_name, query_payload, "limited")) is not None
            and result.get("total", 0) == 2
            else None
        ),
        timeout_s=120.0,
        interval_s=0.5,
    )
    assert limited is not None
    assert limited["type"] == "pattern"
    assert limited["total"] == 2
    assert len(limited["matches"]) == 2
