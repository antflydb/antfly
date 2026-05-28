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

"""Stateful public API retrieval-agent tests."""

from __future__ import annotations

import json
import time

import pytest
import requests

from helpers import wait_until


def _parse_sse_events(body: str) -> list[tuple[str, object]]:
    events: list[tuple[str, object]] = []
    for chunk in body.strip().split("\n\n"):
        if not chunk:
            continue
        event_name = None
        data = None
        for line in chunk.splitlines():
            if line.startswith("event: "):
                event_name = line[len("event: ") :]
            elif line.startswith("data: "):
                data = json.loads(line[len("data: ") :])
        if event_name is not None and data is not None:
            events.append((event_name, data))
    return events


def _hit_ids(result: dict) -> list[str]:
    return [hit["_id"] for hit in result.get("hits", [])]


def _query_hit_ids(result: dict) -> list[str]:
    responses = result.get("responses", [])
    if not responses:
        return []
    hits = responses[0].get("hits", {}).get("hits", [])
    return [hit["_id"] for hit in hits]


def _post_until_hits(backup_api, payload: dict, timeout_s: float = 30.0) -> dict:
    result = wait_until(
        lambda: (
            response
            if (response := backup_api.post("/agents/retrieval", payload)).get("hits")
            else None
        ),
        timeout_s=timeout_s,
        interval_s=0.5,
    )
    assert result is not None
    return result


def _post_until_hit_ids(
    backup_api,
    payload: dict,
    expected_ids: list[str],
    timeout_s: float = 30.0,
) -> dict:
    result = wait_until(
        lambda: (
            response
            if _hit_ids(response := backup_api.post("/agents/retrieval", payload)) == expected_ids
            else None
        ),
        timeout_s=timeout_s,
        interval_s=0.5,
    )
    assert result is not None
    return result


def _index_status(api, table_name: str, index_name: str) -> dict | None:
    try:
        return api.get(f"/tables/{table_name}/indexes/{index_name}").get("status")
    except requests.HTTPError:
        return None


def _ready_index(api, table_name: str, index_name: str, *, expected_docs: int) -> dict | None:
    status = _index_status(api, table_name, index_name)
    if not status:
        return None
    if status.get("rebuilding", status.get("backfill_active", False)):
        return None
    total_indexed = status.get("total_indexed", status.get("doc_count", 0))
    if total_indexed < expected_docs:
        return None
    return status


def _settled_index(api, table_name: str, index_name: str) -> dict | None:
    status = _index_status(api, table_name, index_name)
    if not status:
        return None
    if status.get("rebuilding", status.get("backfill_active", False)):
        return None
    return status


def _ready_graph_index(
    api,
    table_name: str,
    index_name: str,
    *,
    expected_nodes: int,
    expected_edges: int,
) -> dict | None:
    status = _settled_index(api, table_name, index_name)
    if not status:
        return None
    if status.get("node_count", 0) < expected_nodes:
        return None
    if status.get("edge_count", 0) < expected_edges:
        return None
    return status


def test_retrieval_agent_pipeline_query(backup_api):
    table_name = f"retrieval_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "alpha", "body": "hello retrieval agent"},
            "doc:b": {"title": "beta", "body": "secondary document"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = wait_until(
        lambda: backup_api.post(
            "/agents/retrieval",
            {
                "query": "find retrieval docs",
                "stream": False,
                "queries": [
                        {
                            "table": table_name,
                            "full_text_search": {"query": "body:retrieval"},
                            "limit": 5,
                        }
                    ],
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert result is not None
    assert result["status"] == "completed"
    assert result["strategy_used"] == "bm25"
    assert _hit_ids(result) == ["doc:a"]
    assert result["steps"][0]["name"] == "pipeline"


def test_retrieval_agent_generation_step(backup_api, termite_generator):
    table_name = f"retrieval_generation_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "alpha", "body": "hello retrieval agent"},
            "doc:b": {"title": "beta", "body": "secondary document"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = wait_until(
        lambda: backup_api.post(
            "/agents/retrieval",
            {
                "query": "find retrieval docs",
                "stream": False,
                "generator": {
                    "provider": "antfly",
                    "model": "local-generator",
                    "api_url": termite_generator,
                    "api_key": "test-key",
                },
                "steps": {
                    "generation": {
                        "enabled": True,
                    }
                },
                "queries": [
                    {
                        "table": table_name,
                        "full_text_search": {"query": "body:retrieval"},
                        "limit": 5,
                    }
                ],
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert result is not None
    assert result["status"] == "completed"
    assert result["generation"] == "Generated answer citing doc:a"
    assert result["model"] == "local-generator"
    assert result["steps"][1]["name"] == "generation"


def test_retrieval_agent_inline_eval_step(backup_api, termite_generator):
    table_name = f"retrieval_eval_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "raft consensus leader follower log replication",
            },
            "doc:b": {
                "title": "beta",
                "body": "secondary document",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = wait_until(
        lambda: backup_api.post(
            "/agents/retrieval",
            {
                "query": "Explain raft consensus in Antfly",
                "stream": False,
                "generator": {
                    "provider": "antfly",
                    "model": "local-generator",
                    "api_url": termite_generator,
                    "api_key": "test-key",
                },
                "steps": {
                    "generation": {"enabled": True},
                    "eval": {
                        "evaluators": [
                            "relevance",
                            "faithfulness",
                            "precision",
                            "recall",
                        ],
                        "judge": {
                            "provider": "antfly",
                            "model": "judge",
                            "api_url": termite_generator,
                            "api_key": "test-key",
                        },
                        "ground_truth": {
                            "relevant_ids": ["doc:a"],
                            "expectations": "raft consensus leader follower log replication",
                        },
                    },
                },
                "queries": [
                    {
                        "table": table_name,
                        "full_text_search": {"query": "body:raft"},
                        "limit": 5,
                    }
                ],
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert result is not None
    assert result["status"] == "completed"
    assert result["eval_result"]["scores"]["retrieval"]["precision"]["score"] >= 0.0
    assert result["eval_result"]["scores"]["retrieval"]["recall"]["score"] >= 0.0
    assert result["eval_result"]["scores"]["generation"]["relevance"]["score"] >= 0.0
    assert result["eval_result"]["scores"]["generation"]["faithfulness"]["score"] >= 0.0
    assert result["eval_result"]["summary"]["total"] == 4
    assert result["steps"][-1]["name"] == "eval"


def test_retrieval_agent_streaming_eval_sse(backup_api, termite_generator):
    table_name = f"retrieval_eval_stream_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "raft consensus leader follower log replication",
            },
            "doc:b": {
                "title": "beta",
                "body": "secondary document",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    response = backup_api._request(
        "POST",
        "/agents/retrieval",
        {
            "query": "Explain raft consensus in Antfly",
            "stream": True,
            "generator": {
                "provider": "antfly",
                "model": "local-generator",
                "api_url": termite_generator,
                "api_key": "test-key",
            },
            "steps": {
                "generation": {"enabled": True},
                "eval": {
                    "evaluators": [
                        "relevance",
                        "faithfulness",
                        "precision",
                        "recall",
                    ],
                    "judge": {
                        "provider": "antfly",
                        "model": "judge",
                        "api_url": termite_generator,
                        "api_key": "test-key",
                    },
                    "ground_truth": {
                        "relevant_ids": ["doc:a"],
                        "expectations": "raft consensus leader follower log replication",
                    },
                },
            },
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:raft"},
                    "limit": 5,
                }
            ],
        },
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"{response.status_code} {response.reason} body={response.text}", response=response)

    body = response.text
    assert response.headers["Content-Type"].startswith("text/event-stream")
    assert "event: generation" in body
    assert "event: eval" in body
    assert '"summary":{"average_score":' in body
    assert '"total":4' in body
    assert "event: done" in body


def test_retrieval_agent_semantic_and_hybrid_queries(backup_api):
    table_name = f"retrieval_strategies_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert backup_api.post(
        f"/tables/{table_name}/indexes/dense_idx",
        {
            "name": "dense_idx",
            "type": "embeddings",
            "external": True,
            "dimension": 3,
        },
    ) == {}
    assert backup_api.post(
        f"/tables/{table_name}/indexes/sparse_idx",
        {
            "name": "sparse_idx",
            "type": "embeddings",
            "external": True,
            "sparse": True,
        },
    ) == {}

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "hello retrieval agent",
                "status": "active",
                "_embeddings": {
                    "dense_idx": [1.0, 0.0, 0.0],
                    "sparse_idx": {"7": 1.5, "42": 0.5},
                },
            },
            "doc:b": {
                "title": "beta",
                "body": "secondary document",
                "status": "draft",
                "_embeddings": {
                    "dense_idx": [0.0, 1.0, 0.0],
                    "sparse_idx": {"99": 2.0},
                },
            },
            "doc:c": {
                "title": "gamma",
                "body": "retrieval systems combine lexical and dense search",
                "status": "active",
                "_embeddings": {
                    "dense_idx": [0.9, 0.1, 0.0],
                    "sparse_idx": {"7": 1.2, "42": 0.4, "50": 1.0},
                },
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    semantic_payload = {
        "query": "find semantically related docs",
        "stream": False,
        "queries": [
            {
                "table": table_name,
                "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                "indexes": ["dense_idx"],
                "limit": 5,
            }
        ],
    }
    semantic = wait_until(
        lambda: (
            response
            if (
                (response := backup_api.post("/agents/retrieval", semantic_payload)).get("hits")
                and response["hits"][0]["_id"] == "doc:a"
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert semantic is not None
    assert semantic["strategy_used"] == "semantic"
    assert semantic["hits"][0]["_id"] == "doc:a"

    hybrid_payload = {
        "query": "find hybrid retrieval docs",
        "stream": False,
        "queries": [
            {
                "table": table_name,
                "full_text_search": {"query": "body:retrieval"},
                "embeddings": {
                    "dense_idx": [1.0, 0.0, 0.0],
                    "sparse_idx": {
                        "indices": [7, 42],
                        "values": [1.5, 0.5],
                    },
                },
                "indexes": ["dense_idx", "sparse_idx"],
                "limit": 5,
            }
        ],
    }
    hybrid = wait_until(
        lambda: (
            response
            if (
                (response := backup_api.post("/agents/retrieval", hybrid_payload)).get("hits")
                and response["hits"][0]["_id"] == "doc:a"
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert hybrid is not None
    assert hybrid["strategy_used"] == "hybrid"
    assert hybrid["hits"][0]["_id"] == "doc:a"

    metadata = _post_until_hits(
        backup_api,
        {
            "query": "find active docs",
            "stream": False,
            "queries": [
                {
                    "table": table_name,
                    "filter_query": {"query": "status:active"},
                    "limit": 5,
                }
            ],
        },
    )
    assert metadata["strategy_used"] == "metadata"
    assert len(metadata["hits"]) == 2


def test_retrieval_agent_tree_search_pipeline(backup_api):
    table_name = f"retrieval_tree_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/doc_hierarchy",
            {
                "name": "doc_hierarchy",
                "type": "graph",
                "edge_types": [{"name": "contains", "topology": "tree"}],
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:root": {
                "title": "root",
                "body": "architecture overview",
                "_edges": {
                    "doc_hierarchy": {
                        "contains": [{"target": "doc:child", "weight": 1.0}],
                    }
                },
            },
            "doc:child": {
                "title": "child",
                "body": "details about the architecture",
            },
            "doc:other": {
                "title": "other",
                "body": "unrelated notes",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    result = wait_until(
        lambda: (
            response
            if _hit_ids(
                response := backup_api.post(
                    "/agents/retrieval",
                    {
                        "query": "how does the architecture work",
                        "stream": False,
                        "queries": [
                            {
                                "table": table_name,
                                "full_text_search": {"query": "body:overview"},
                                "limit": 5,
                            },
                            {
                                "table": table_name,
                                "tree_search": {
                                    "index": "doc_hierarchy",
                                    "start_nodes": "$find_start",
                                    "max_depth": 2,
                                },
                                "limit": 5,
                            },
                        ],
                    },
                )
            )
            == ["doc:root", "doc:child"]
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert result is not None
    assert result["strategy_used"] == "hybrid"
    assert _hit_ids(result) == ["doc:root", "doc:child"]


def test_retrieval_agent_tree_search_from_roots(backup_api):
    table_name = f"retrieval_tree_roots_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/doc_hierarchy",
            {
                "name": "doc_hierarchy",
                "type": "graph",
                "edge_types": [{"name": "contains", "topology": "tree"}],
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:root": {
                "title": "root",
                "body": "architecture overview",
                "_edges": {
                    "doc_hierarchy": {
                        "contains": [{"target": "doc:child", "weight": 1.0}],
                    }
                },
            },
            "doc:child": {
                "title": "child",
                "body": "details about the architecture",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    graph_ready = wait_until(
        lambda: _ready_graph_index(
            backup_api,
            table_name,
            "doc_hierarchy",
            expected_nodes=2,
            expected_edges=1,
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert graph_ready is not None

    result = _post_until_hits(
        backup_api,
        {
            "query": "how does the architecture work",
            "stream": False,
            "queries": [
                {
                    "table": table_name,
                    "tree_search": {
                        "index": "doc_hierarchy",
                        "start_nodes": "$roots",
                        "max_depth": 2,
                    },
                    "limit": 5,
                }
            ],
        },
        timeout_s=60.0,
    )
    assert result["strategy_used"] == "tree"
    assert "doc:child" in _hit_ids(result)


def test_retrieval_agent_tree_search_generation(backup_api, termite_generator):
    table_name = f"retrieval_tree_generation_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/doc_hierarchy",
            {
                "name": "doc_hierarchy",
                "type": "graph",
                "edge_types": [{"name": "contains", "topology": "tree"}],
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:root": {
                "title": "root",
                "body": "architecture overview",
                "_edges": {
                    "doc_hierarchy": {
                        "contains": [{"target": "doc:child", "weight": 1.0}],
                    }
                },
            },
            "doc:child": {
                "title": "child",
                "body": "details about the architecture",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    graph_ready = wait_until(
        lambda: _ready_graph_index(
            backup_api,
            table_name,
            "doc_hierarchy",
            expected_nodes=2,
            expected_edges=1,
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert graph_ready is not None

    result = wait_until(
        lambda: (
            response
            if (
                (response := backup_api.post(
                    "/agents/retrieval",
                    {
                        "query": "summarize the architecture tree",
                        "stream": False,
                        "generator": {
                            "provider": "antfly",
                            "model": "local-generator",
                            "api_url": termite_generator,
                            "api_key": "test-key",
                        },
                        "steps": {
                            "generation": {"enabled": True},
                            "followup": {"enabled": True, "count": 2},
                        },
                        "queries": [
                                {
                                    "table": table_name,
                                    "tree_search": {
                                        "index": "doc_hierarchy",
                                        "start_nodes": "doc:root",
                                        "max_depth": 2,
                                        "beam_width": 2,
                                    },
                                    "limit": 5,
                                }
                        ],
                    },
                )).get("hits")
                and response.get("generation")
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert result is not None
    assert result["strategy_used"] == "tree"
    assert _hit_ids(result) == ["doc:child"]
    assert result["generation"] == "Generated tree answer citing doc:child from root doc:root along path doc:root > doc:child"
    assert result["steps"][-1]["name"] == "generation"
    assert len(result["followup_questions"]) == 2


def test_retrieval_agent_classification_confidence_followup(backup_api, termite_generator):
    table_name = f"retrieval_classify_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "alpha", "body": "hello retrieval agent"},
            "doc:b": {"title": "beta", "body": "secondary document"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = _post_until_hits(
        backup_api,
        {
            "query": "How does retrieval work?",
            "stream": False,
            "generator": {
                "provider": "antfly",
                "model": "local-generator",
                "api_url": termite_generator,
                "api_key": "test-key",
            },
            "steps": {
                "classification": {
                    "enabled": True,
                    "with_reasoning": True,
                },
                "generation": {
                    "enabled": True,
                },
                "confidence": {
                    "enabled": True,
                },
                "followup": {
                    "enabled": True,
                    "count": 3,
                },
            },
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:retrieval"},
                    "limit": 5,
                }
            ],
        },
    )
    assert result["classification"]["route_type"] == "question"
    assert result["classification"]["strategy"] == "step_back"
    assert result["classification"]["step_back_query"]
    assert len(result["classification"]["multi_phrases"]) >= 2
    assert result["classification"]["reasoning"]
    assert result["generation_confidence"] > 0
    assert result["context_relevance"] > 0
    assert len(result["followup_questions"]) == 3


def test_retrieval_agent_classification_can_decompose_queries(backup_api):
    table_name = f"retrieval_classify_decompose_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly"},
            "doc:b": {"title": "termite", "body": "termite embeddings and reranking"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = wait_until(
        lambda: backup_api.post(
            "/agents/retrieval",
            {
                "query": "Compare raft consensus and termite embeddings",
                "stream": False,
                "steps": {
                    "classification": {
                        "enabled": True,
                        "with_reasoning": True,
                    },
                },
                "queries": [
                    {
                        "table": table_name,
                        "full_text_search": {"query": "body:raft"},
                        "limit": 5,
                    }
                ],
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert result is not None
    assert result["classification"]["strategy"] == "decompose"
    assert len(result["classification"]["sub_questions"]) >= 2


def test_retrieval_agent_streaming_sse(backup_api, termite_generator):
    table_name = f"retrieval_streaming_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "alpha", "body": "hello retrieval agent"},
            "doc:b": {"title": "beta", "body": "secondary document"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    response = backup_api._request(
        "POST",
        "/agents/retrieval",
        {
            "query": "find retrieval docs",
            "stream": True,
            "generator": {
                "provider": "antfly",
                "model": "local-generator",
                "api_url": termite_generator,
                "api_key": "test-key",
            },
            "steps": {
                "generation": {
                    "enabled": True,
                },
                "followup": {
                    "enabled": True,
                    "count": 2,
                },
            },
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:retrieval"},
                    "limit": 5,
                }
            ],
        },
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"{response.status_code} {response.reason} body={response.text}", response=response)

    assert response.headers["Content-Type"].startswith("text/event-stream")
    body = response.text
    assert "event: step_started" in body
    assert "event: hit" in body
    assert "event: generation" in body
    assert "event: followup" in body
    assert "event: done" in body
    assert '"_id":"doc:a"' in body
    assert '"generation":"Generated answer citing doc:a"' in body
    events = _parse_sse_events(body)
    generation_chunks = [data for event, data in events if event == "generation"]
    followups = [data for event, data in events if event == "followup"]
    tool_modes = [data for event, data in events if event == "tool_mode"]
    started = [data for event, data in events if event == "step_started"]
    reasoning_chunks = [data for event, data in events if event == "reasoning"]
    assert generation_chunks and all(isinstance(chunk, str) for chunk in generation_chunks)
    assert followups and all(isinstance(question, str) for question in followups)
    assert all(isinstance(chunk, str) for chunk in reasoning_chunks)
    assert all(isinstance(mode, dict) and "mode" in mode and "tools_count" not in mode for mode in tool_modes)
    assert started and all(isinstance(step, dict) and "id" in step and "name" in step and "action" in step and "details" not in step for step in started)


def test_retrieval_agent_streaming_clarification_sse(backup_api):
    table_name = f"retrieval_streaming_clarify_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly", "status": "active"},
            "doc:b": {"title": "other", "body": "unrelated notes", "status": "draft"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    response = backup_api._request(
        "POST",
        "/agents/retrieval",
        {
            "query": "How does Raft consensus work in Antfly?",
            "stream": True,
            "max_internal_iterations": 3,
            "max_user_clarifications": 1,
            "require_decision_after": 0,
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:raft"},
                    "limit": 5,
                },
                {
                    "table": table_name,
                    "filter_query": {"query": "status:active"},
                    "limit": 5,
                },
            ],
        },
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"{response.status_code} {response.reason} body={response.text}", response=response)

    assert response.headers["Content-Type"].startswith("text/event-stream")
    body = response.text
    assert "event: clarification" not in body
    assert "event: reasoning" in body
    assert "event: step_started" in body
    assert '"phase":"clarification"' in body
    assert '"id":"select_query"' in body
    assert "event: step_completed" in body
    assert "event: done" in body


def test_retrieval_agent_streaming_decompose_progress(backup_api):
    table_name = f"retrieval_streaming_decompose_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly", "status": "draft"},
            "doc:b": {"title": "status", "body": "secondary document", "status": "active"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    response = backup_api._request(
        "POST",
        "/agents/retrieval",
        {
            "query": "Compare raft consensus and active document status",
            "stream": True,
            "max_internal_iterations": 3,
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:raft"},
                    "limit": 5,
                },
                {
                    "table": table_name,
                    "filter_query": {"query": "status:active"},
                    "limit": 5,
                },
            ],
        },
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"{response.status_code} {response.reason} body={response.text}", response=response)

    assert response.headers["Content-Type"].startswith("text/event-stream")
    body = response.text
    assert "event: classification" in body
    assert '"phase":"decompose"' in body
    assert '"sub_question"' in body
    assert '"phase":"tool_call"' in body
    assert "event: done" in body


def test_retrieval_agent_streaming_probe_progress(backup_api):
    table_name = f"retrieval_streaming_probe_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/dense_idx",
            {
                "name": "dense_idx",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Architecture",
                "body": "architecture overview",
                "_embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
            },
            "doc:b": {
                "title": "Architecture Guide",
                "body": "architecture guide",
                "_embeddings": {"dense_idx": [0.0, 1.0, 0.0]},
            },
            "doc:c": {
                "title": "Other",
                "body": "unrelated notes",
                "_embeddings": {"dense_idx": [0.0, 0.0, 1.0]},
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    readiness: dict[str, object] = {}

    def _probe_ready() -> bool | None:
        status = _index_status(backup_api, table_name, "dense_idx")
        readiness["status"] = status
        try:
            readiness["lookup_a"] = backup_api.lookup_key(table_name, "doc:a")
        except requests.HTTPError:
            readiness["lookup_a"] = None
        dense = backup_api.query_table(
            table_name,
            {
                "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                "indexes": ["dense_idx"],
                "limit": 1,
            },
        )
        readiness["dense_hits"] = _query_hit_ids(dense)
        hybrid = backup_api.query_table(
            table_name,
            {
                "full_text_search": {"query": "body:architecture"},
                "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                "indexes": ["dense_idx"],
                "limit": 5,
            },
        )
        readiness["hybrid_hits"] = _query_hit_ids(hybrid)
        if readiness["dense_hits"] and readiness["hybrid_hits"]:
            return True
        return None

    ready = wait_until(_probe_ready, timeout_s=30.0, interval_s=0.5)
    assert ready is not None, readiness

    response = backup_api._request(
        "POST",
        "/agents/retrieval",
        {
            "query": "architecture overview",
            "stream": True,
            "max_internal_iterations": 3,
            "steps": {
                "classification": {
                    "enabled": True,
                    "force_strategy": "simple",
                    "with_reasoning": True,
                }
            },
            "queries": [
                {
                    "table": table_name,
                    "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                    "indexes": ["dense_idx"],
                    "limit": 1,
                },
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:architecture"},
                    "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                    "indexes": ["dense_idx"],
                    "limit": 5,
                },
            ],
        },
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"{response.status_code} {response.reason} body={response.text}", response=response)

    body = response.text
    assert response.headers["Content-Type"].startswith("text/event-stream")
    assert "event: step_progress" in body
    assert '"phase":"probe"' in body
    assert '"selection_source":"probe"' in body
    assert '"probe_relevance":' in body
    assert "event: reasoning" in body
    assert "event: tool_mode" in body
    assert '"mode":"structured_output"' in body


def test_retrieval_agent_streaming_fallback_progress(backup_api):
    table_name = f"retrieval_streaming_fallback_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly", "status": "active"},
            "doc:b": {"title": "other", "body": "unrelated notes", "status": "draft"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    response = backup_api._request(
        "POST",
        "/agents/retrieval",
        {
            "query": "How does Raft consensus work in Antfly?",
            "stream": True,
            "max_internal_iterations": 3,
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:missing"},
                    "limit": 5,
                },
                {
                    "table": table_name,
                    "filter_query": {"query": "status:active"},
                    "limit": 5,
                },
            ],
        },
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"{response.status_code} {response.reason} body={response.text}", response=response)

    body = response.text
    assert response.headers["Content-Type"].startswith("text/event-stream")
    assert "event: step_progress" in body
    assert '"phase":"evaluate"' in body
    assert '"selection_source":"evaluation"' in body
    assert '"current_planner_score":' in body
    assert '"best_fallback_score":' in body
    assert '"probe_hits":' in body
    assert "event: hit" in body
    assert "event: done" in body


def test_retrieval_agent_streaming_tree_progress(backup_api):
    table_name = f"retrieval_streaming_tree_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/doc_hierarchy",
            {
                "name": "doc_hierarchy",
                "type": "graph",
                "edge_types": [{"name": "contains", "topology": "tree"}],
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:root": {
                "title": "root",
                "body": "architecture overview",
                "_edges": {
                    "doc_hierarchy": {
                        "contains": [{"target": "doc:child", "weight": 1.0}],
                    }
                },
            },
            "doc:child": {
                "title": "child",
                "body": "details about the architecture",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    response = wait_until(
        lambda: (
            streamed
            if (
                streamed := backup_api._request(
                    "POST",
                    "/agents/retrieval",
                    {
                        "query": "summarize the architecture tree",
                        "stream": True,
                        "queries": [
                            {
                                "table": table_name,
                                "tree_search": {
                                    "index": "doc_hierarchy",
                                    "start_nodes": "$roots",
                                    "max_depth": 2,
                                    "beam_width": 2,
                                },
                                "limit": 5,
                            }
                        ],
                    },
                )
            ).status_code < 400
            and "event: hit" in streamed.text
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert response is not None
    if response.status_code >= 400:
        raise requests.HTTPError(f"{response.status_code} {response.reason} body={response.text}", response=response)

    body = response.text
    assert response.headers["Content-Type"].startswith("text/event-stream")
    assert "event: step_progress" in body
    assert '"phase":"tree_search"' in body
    assert '"depth":' in body
    assert '"num_nodes":' in body
    assert '"collected":' in body
    assert '"complete":true' in body
    assert "event: hit" in body
    assert "event: done" in body


def test_retrieval_agent_streaming_error_sse(backup_api):
    response = backup_api._request(
        "POST",
        "/agents/retrieval",
        {
            "query": "find missing docs",
            "stream": True,
            "queries": [
                {
                    "table": f"missing_{time.time_ns()}",
                    "full_text_search": {"query": "body:alpha"},
                    "limit": 5,
                }
            ],
        },
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"{response.status_code} {response.reason} body={response.text}", response=response)

    assert response.headers["Content-Type"].startswith("text/event-stream")
    body = response.text
    assert "event: error" in body
    assert '"error":"' in body
    assert "event: done" not in body


def test_retrieval_agent_bounded_agentic_mode(backup_api):
    table_name = f"retrieval_agentic_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly"},
            "doc:b": {"title": "other", "body": "unrelated notes"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = _post_until_hits(
        backup_api,
        {
            "query": "How does Raft consensus work in Antfly?",
            "stream": False,
            "max_internal_iterations": 3,
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:raft"},
                    "limit": 5,
                }
            ],
        },
    )
    assert result["status"] == "completed"
    assert result["classification"]["strategy"] == "step_back"
    assert result["tool_calls_made"] == 1
    assert result["iteration"] == 1
    assert result["remaining_internal_iterations"] == 2
    assert _hit_ids(result) == ["doc:a"]


def test_retrieval_agent_bounded_agentic_can_probe_ambiguous_candidates(backup_api):
    table_name = f"retrieval_agentic_probe_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/dense_idx",
            {
                "name": "dense_idx",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Architecture",
                "body": "architecture overview",
                "_embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
            },
            "doc:b": {
                "title": "Architecture Guide",
                "body": "architecture guide",
                "_embeddings": {"dense_idx": [0.0, 1.0, 0.0]},
            },
            "doc:c": {
                "title": "Other",
                "body": "unrelated notes",
                "_embeddings": {"dense_idx": [0.0, 0.0, 1.0]},
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    result = _post_until_hits(
        backup_api,
        {
            "query": "architecture overview",
            "stream": False,
            "max_internal_iterations": 3,
            "steps": {
                "classification": {
                    "enabled": True,
                    "force_strategy": "simple",
                    "with_reasoning": True,
                }
            },
            "queries": [
                {
                    "table": table_name,
                    "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                    "indexes": ["dense_idx"],
                    "limit": 1,
                },
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:architecture"},
                    "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                    "indexes": ["dense_idx"],
                    "limit": 5,
                },
            ],
        },
    )
    assert result["tool_calls_made"] == 1
    assert result["strategy_used"] == "hybrid"
    selection_step = next(step for step in result["steps"] if step["name"] == "select_strategy")
    assert selection_step["details"]["selection_source"] == "probe"
    assert len(selection_step["details"]["candidate_scores"]) == 2
    assert selection_step["details"]["candidate_scores"][0]["probe_hits"] >= 0
    assert selection_step["details"]["candidate_scores"][1]["probe_hits"] >= 0
    assert any("probe_relevance" in candidate for candidate in selection_step["details"]["candidate_scores"])


def test_retrieval_agent_bounded_agentic_selects_best_query(backup_api):
    table_name = f"retrieval_agentic_select_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly", "status": "active"},
            "doc:b": {"title": "other", "body": "unrelated notes", "status": "draft"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = _post_until_hits(
        backup_api,
        {
            "query": "How does Raft consensus work in Antfly?",
            "stream": False,
            "max_internal_iterations": 3,
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:raft"},
                    "limit": 5,
                },
                {
                    "table": table_name,
                    "filter_query": {"query": "status:active"},
                    "limit": 5,
                },
            ],
        },
    )
    assert result["tool_calls_made"] == 1
    assert result["strategy_used"] == "bm25"
    assert _hit_ids(result) == ["doc:a"]


def test_retrieval_agent_bounded_agentic_can_fallback_after_an_empty_first_pass(backup_api):
    table_name = f"retrieval_agentic_fallback_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly", "status": "active"},
            "doc:b": {"title": "other", "body": "unrelated notes", "status": "draft"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    ready = wait_until(
        lambda: (
            response
            if _query_hit_ids(
                response := backup_api.query_table(
                    table_name,
                    {
                        "filter_query": {"query": "status:active"},
                        "limit": 5,
                    },
                )
            )
            == ["doc:a"]
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert ready is not None

    result = _post_until_hits(
        backup_api,
        {
            "query": "How does Raft consensus work in Antfly?",
            "stream": False,
            "max_internal_iterations": 3,
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:missing"},
                    "limit": 5,
                },
                {
                    "table": table_name,
                    "filter_query": {"query": "status:active"},
                    "limit": 5,
                },
            ],
        },
    )
    assert result["status"] == "completed"
    assert result["tool_calls_made"] == 2
    assert result["strategy_used"] == "hybrid"
    assert _hit_ids(result) == ["doc:a"]
    assert any(step["name"] == "evaluate" for step in result["steps"])
    evaluation_select = [
        step
        for step in result["steps"]
        if step["name"] == "select_strategy"
        and step.get("details", {}).get("selection_source") == "evaluation"
    ]
    assert evaluation_select


def test_retrieval_agent_bounded_agentic_can_fallback_after_a_weak_first_pass(backup_api):
    table_name = f"retrieval_agentic_weak_fallback_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/dense_idx",
            {
                "name": "dense_idx",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:thin": {
                "title": "raft",
                "body": "raft",
                "_embeddings": {"dense_idx": [0.0, 0.0, 1.0]},
            },
            "doc:semantic": {
                "title": "Consensus Architecture",
                "body": "distributed consensus architecture overview",
                "_embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    dense_ready = wait_until(
        lambda: _ready_index(backup_api, table_name, "dense_idx", expected_docs=2),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert dense_ready is not None

    semantic_ready = wait_until(
        lambda: (
            response
            if (
                (
                    response := backup_api.post(
                        "/agents/retrieval",
                        {
                            "query": "consensus architecture overview",
                            "stream": False,
                            "queries": [
                                {
                                    "table": table_name,
                                    "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                                    "indexes": ["dense_idx"],
                                    "limit": 5,
                                }
                            ],
                        },
                    )
                ).get("hits")
                and response["hits"][0]["_id"] == "doc:semantic"
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert semantic_ready is not None

    payload = {
        "query": "How does raft consensus work in Antfly?",
        "stream": False,
        "session_id": f"retrieval-weak-fallback-{time.time_ns()}",
        "max_internal_iterations": 3,
        "max_user_clarifications": 1,
        "decisions": [{"question_id": "select_query", "answer": 0}],
        "steps": {
            "classification": {
                "enabled": True,
                "force_strategy": "simple",
                "with_reasoning": True,
            }
        },
        "queries": [
            {
                "table": table_name,
                "full_text_search": {"query": "body:raft"},
                "limit": 5,
            },
            {
                "table": table_name,
                "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                "indexes": ["dense_idx"],
                "limit": 5,
            },
        ],
    }
    result = wait_until(
        lambda: (
            response
            if (
                (response := backup_api.post("/agents/retrieval", payload)).get("status") == "completed"
                and any(step.get("name") == "evaluate" for step in response.get("steps", []))
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert result is not None
    assert result["status"] == "completed"
    assert result["tool_calls_made"] in (2, 3)
    assert result["strategy_used"] in ("bm25", "hybrid")
    assert _hit_ids(result)[0] == "doc:thin"
    evaluate_steps = [step for step in result["steps"] if step["name"] == "evaluate"]
    assert evaluate_steps
    assert evaluate_steps[-1]["details"]["trigger"] == "weak_result"
    evaluation_details = evaluate_steps[-1]["details"]
    assert "current_planner_score" in evaluation_details
    if "candidate_scores" in evaluation_details:
        assert "best_fallback_score" in evaluation_details
        assert any("probe_hits" in candidate for candidate in evaluation_details["candidate_scores"])
    else:
        assert evaluation_details["planner_decision"] == "refine_query"
        assert "best_fallback_score" in evaluation_details
    if result["tool_calls_made"] == 3:
        assert "doc:semantic" in _hit_ids(result)
        refine_steps = [step for step in result["steps"] if step["name"] == "refine_query"]
        assert refine_steps
        assert any(step.get("details", {}).get("phase") == "evaluation_refine" for step in refine_steps)


def test_retrieval_agent_bounded_agentic_can_fallback_after_a_weak_multi_hit_first_pass(backup_api):
    table_name = f"retrieval_agentic_weak_multi_fallback_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    assert (
        backup_api.post(
            f"/tables/{table_name}/indexes/dense_idx",
            {
                "name": "dense_idx",
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
        == {}
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:thin": {
                "title": "raft",
                "body": "raft",
                "_embeddings": {"dense_idx": [0.0, 0.0, 1.0]},
            },
            "doc:other": {
                "title": "other",
                "body": "raft note",
                "_embeddings": {"dense_idx": [0.0, 1.0, 0.0]},
            },
            "doc:semantic": {
                "title": "Consensus Architecture",
                "body": "distributed consensus architecture overview",
                "_embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 3

    semantic_ready = wait_until(
        lambda: (
            response
            if (
                (
                    response := backup_api.post(
                        "/agents/retrieval",
                        {
                            "query": "consensus architecture overview",
                            "stream": False,
                            "queries": [
                                {
                                    "table": table_name,
                                    "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                                    "indexes": ["dense_idx"],
                                    "limit": 5,
                                }
                            ],
                        },
                    )
                ).get("hits")
                and response["hits"][0]["_id"] == "doc:semantic"
            )
            else None
        ),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert semantic_ready is not None

    payload = {
        "query": "How does raft consensus work in Antfly?",
        "stream": False,
        "session_id": f"retrieval-weak-multi-fallback-{time.time_ns()}",
        "max_internal_iterations": 3,
        "max_user_clarifications": 1,
        "decisions": [{"question_id": "select_query", "answer": 0}],
        "steps": {
            "classification": {
                "enabled": True,
                "force_strategy": "simple",
                "with_reasoning": True,
            }
        },
        "queries": [
            {
                "table": table_name,
                "full_text_search": {"query": "body:raft"},
                "limit": 5,
            },
            {
                "table": table_name,
                "embeddings": {"dense_idx": [1.0, 0.0, 0.0]},
                "indexes": ["dense_idx"],
                "limit": 5,
            },
        ],
    }
    result = wait_until(
        lambda: (
            response
            if (
                (response := backup_api.post("/agents/retrieval", payload)).get("status") == "completed"
                and any(step.get("name") == "evaluate" for step in response.get("steps", []))
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert result is not None
    assert result["status"] == "completed"
    assert result["tool_calls_made"] in (2, 3)
    assert result["strategy_used"] in ("bm25", "hybrid")
    assert _hit_ids(result)[:2] == ["doc:thin", "doc:other"]
    evaluate_steps = [step for step in result["steps"] if step["name"] == "evaluate"]
    assert evaluate_steps
    assert evaluate_steps[-1]["details"]["trigger"] == "weak_result"
    evaluation_details = evaluate_steps[-1]["details"]
    assert "current_planner_score" in evaluation_details
    if "candidate_scores" in evaluation_details:
        assert "best_fallback_score" in evaluation_details
        assert any("probe_hits" in candidate for candidate in evaluation_details["candidate_scores"])
    else:
        assert evaluation_details["planner_decision"] == "refine_query"
        assert "best_fallback_score" in evaluation_details
    if result["tool_calls_made"] == 3:
        assert "doc:semantic" in _hit_ids(result)
        refine_steps = [step for step in result["steps"] if step["name"] == "refine_query"]
        assert refine_steps
        assert any(step.get("details", {}).get("phase") == "evaluation_refine" for step in refine_steps)


def test_retrieval_agent_bounded_agentic_can_decompose_queries(backup_api):
    table_name = f"retrieval_agentic_decompose_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly", "status": "draft"},
            "doc:b": {"title": "status", "body": "secondary document", "status": "active"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    result = _post_until_hit_ids(
        backup_api,
        {
            "query": "Compare raft consensus and active document status",
            "stream": False,
            "max_internal_iterations": 3,
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:raft"},
                    "limit": 5,
                },
                {
                    "table": table_name,
                    "filter_query": {"query": "status:active"},
                    "limit": 5,
                },
            ],
        },
        ["doc:a", "doc:b"],
    )
    assert result["tool_calls_made"] == 2
    assert result["classification"]["strategy"] == "decompose"
    assert result["strategy_used"] == "hybrid"
    assert _hit_ids(result) == ["doc:a", "doc:b"]


def test_retrieval_agent_can_require_clarification_and_continue(backup_api):
    table_name = f"retrieval_agentic_decision_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly", "status": "active"},
            "doc:b": {"title": "other", "body": "unrelated notes", "status": "draft"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    session_id = f"retrieval-session-{time.time_ns()}"
    clarify = wait_until(
        lambda: backup_api.post(
            "/agents/retrieval",
            {
                "query": "How does Raft consensus work in Antfly?",
                "stream": False,
                "session_id": session_id,
                "max_internal_iterations": 3,
                "max_user_clarifications": 1,
                "require_decision_after": 0,
                "queries": [
                    {
                        "table": table_name,
                        "full_text_search": {"query": "body:raft"},
                        "limit": 5,
                    },
                    {
                        "table": table_name,
                        "filter_query": {"query": "status:active"},
                        "limit": 5,
                    },
                ],
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert clarify is not None
    assert clarify["status"] == "clarification_required"
    assert clarify["remaining_user_clarifications"] == 1
    assert len(clarify["questions"]) == 1
    assert clarify["questions"][0]["id"] == "select_query"

    continued = _post_until_hits(
        backup_api,
        {
            "query": "How does Raft consensus work in Antfly?",
            "stream": False,
            "session_id": session_id,
            "max_internal_iterations": 3,
            "max_user_clarifications": 1,
            "require_decision_after": 0,
            "decisions": [{"question_id": "select_query", "answer": 0}],
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:raft"},
                    "limit": 5,
                },
                {
                    "table": table_name,
                    "filter_query": {"query": "status:active"},
                    "limit": 5,
                },
            ],
        },
    )
    assert continued["status"] == "completed"
    assert continued["clarification_count"] == 1
    assert continued["remaining_user_clarifications"] == 0
    assert continued["strategy_used"] == "bm25"
    assert _hit_ids(continued) == ["doc:a"]


def test_retrieval_agent_can_ask_to_broaden_after_a_user_selected_query_misses(backup_api):
    table_name = f"retrieval_agentic_broaden_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {"title": "raft", "body": "raft consensus in antfly", "status": "active"},
            "doc:b": {"title": "other", "body": "unrelated notes", "status": "draft"},
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    session_id = f"retrieval-broaden-{time.time_ns()}"
    clarify = wait_until(
        lambda: backup_api.post(
            "/agents/retrieval",
            {
                "query": "How does Raft consensus work in Antfly?",
                "stream": False,
                "session_id": session_id,
                "max_internal_iterations": 3,
                "max_user_clarifications": 2,
                "decisions": [{"question_id": "select_query", "answer": 0}],
                "queries": [
                    {
                        "table": table_name,
                        "full_text_search": {"query": "body:missing"},
                        "limit": 5,
                    },
                    {
                        "table": table_name,
                        "filter_query": {"query": "status:active"},
                        "limit": 5,
                    },
                ],
            },
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert clarify is not None
    assert clarify["status"] == "clarification_required"
    assert clarify["questions"][0]["id"] == "broaden_search"

    continued = _post_until_hits(
        backup_api,
        {
            "query": "How does Raft consensus work in Antfly?",
            "stream": False,
            "session_id": session_id,
            "max_internal_iterations": 3,
            "max_user_clarifications": 2,
            "decisions": [
                {"question_id": "select_query", "answer": 0},
                {"question_id": "broaden_search", "approved": True},
            ],
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:missing"},
                    "limit": 5,
                },
                {
                    "table": table_name,
                    "filter_query": {"query": "status:active"},
                    "limit": 5,
                },
            ],
        },
    )
    assert continued["status"] == "completed"
    assert continued["tool_calls_made"] == 2
    assert continued["strategy_used"] == "hybrid"
    assert _hit_ids(continued) == ["doc:a"]


def test_retrieval_agent_rejects_tree_search_without_start_nodes_or_seed_hits(backup_api):
    table_name = f"retrieval_invalid_{time.time_ns()}"
    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    with pytest.raises(requests.HTTPError, match="invalid retrieval agent request"):
        backup_api.post(
            "/agents/retrieval",
            {
                "query": "find retrieval docs",
                "stream": False,
                "queries": [
                    {
                        "table": table_name,
                        "tree_search": {"index": "doc_hierarchy"},
                    }
                ],
            },
        )
