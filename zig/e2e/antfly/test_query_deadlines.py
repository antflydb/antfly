"""Public query deadlines remain valid across request and routing clocks."""

import pytest

from test_joins import _seed_join_tables


def _check_query_budgets(api, table, query):
    # Establish readiness without a short budget before testing clock identity.
    baseline = api.query_table(table, query)["responses"][0]
    if "graph_results" in baseline:
        assert baseline["graph_results"]["counts"]["aggregates"]["rows"]["value"] == "1"
    else:
        assert baseline["hits"]["hits"]
    for timeout_ms in (1000, 5000, 30000):
        response = api.query_table(table, {**query, "timeout_ms": timeout_ms})[
            "responses"
        ][0]
        assert [hit["_id"] for hit in response["hits"]["hits"]] == [
            hit["_id"] for hit in baseline["hits"]["hits"]
        ]
        if "graph_results" in baseline:
            assert (
                response["graph_results"]["counts"]["aggregates"]
                == baseline["graph_results"]["counts"]["aggregates"]
            )
    expired = api._request("POST", f"/tables/{table}/query", {**query, "timeout_ms": 0})
    assert expired.status_code == 504
    assert expired.json()["code"] == "query_timeout"


@pytest.mark.parametrize("num_shards", [1, 2])
def test_query_timeout_budget_survives_routing(stateful_api, num_shards):
    table = f"query_deadline_{num_shards}"
    stateful_api.create_table(table, num_shards=num_shards)
    # A write acknowledgement does not guarantee full-text visibility. Wait
    # for the searched index before asserting deadline behavior.
    stateful_api.batch_write(
        table, inserts={"doc:a": {"title": "alpha"}}, sync_level="full_text"
    )
    # Warm publication and the query path first; these assertions concern
    # deadline clock identity, not startup latency.
    query = {"full_text_search": {"match_all": {}}, "limit": 10}
    baseline = stateful_api.query_table(table, query)["responses"][0]["hits"]["hits"]
    assert [hit["_id"] for hit in baseline] == ["doc:a"]
    for timeout_ms in (1000, 5000, 30000):
        result = stateful_api.query_table(table, {**query, "timeout_ms": timeout_ms})
        hits = result["responses"][0]["hits"]["hits"]
        assert [hit["_id"] for hit in hits] == ["doc:a"]
    response = stateful_api._request(
        "POST", f"/tables/{table}/query", {**query, "timeout_ms": 0}
    )
    assert response.status_code == 504
    assert response.json()["code"] == "query_timeout"


def test_join_timeout_budget_survives_coordinator_and_workers(stateful_api):
    docs, customers = _seed_join_tables(stateful_api, "deadline_join")
    _check_query_budgets(
        stateful_api,
        docs,
        {
            "full_text_search": {"query": "body:distributed AND body:join"},
            "limit": 128,
            "join": {
                "right_table": customers,
                "join_type": "inner",
                "strategy_hint": "shuffle",
                "on": {
                    "left_field": "customer_id",
                    "right_field": "_id",
                    "operator": "eq",
                },
                "right_fields": ["name"],
            },
        },
    )


def test_graph_timeout_budget_survives_worker_and_catalog(backup_api):
    table = "deadline_graph"
    backup_api.create_table(table, num_shards=3)
    backup_api.create_index(
        table,
        "graph_idx",
        {
            "name": "graph_idx",
            "type": "graph",
            "edge_types": [{"name": "knows"}],
        },
    )
    backup_api.wait_index_ready(table, "graph_idx", timeout_s=30.0, until="complete")
    backup_api.batch_write(
        table,
        inserts={
            "doc:a": {
                "title": "anchor",
                "_edges": {"graph_idx": {"knows": [{"target": "doc:b"}]}},
            },
            "doc:b": {"title": "neighbor"},
        },
        sync_level="full_index",
    )
    _check_query_budgets(
        backup_api,
        table,
        {
            "graph_queries": {
                "counts": {
                    "index": "graph_idx",
                    "match": {
                        "anchor": "a",
                        "nodes": {"a": {}, "b": {}},
                        "edges": [{"from": "a", "to": "b", "types": ["knows"]}],
                    },
                    "return": {"aggregates": {"rows": {"count": "*"}}},
                }
            },
            "limit": 10,
        },
    )


def test_semantic_timeout_budget_survives_embedding_cache(backup_api, openai_embedder):
    table = "deadline_semantic"
    backup_api.create_table(table, num_shards=1)
    backup_api.create_index(
        table,
        "semantic_idx",
        {
            "name": "semantic_idx",
            "type": "embeddings",
            "field": "body",
            "dimension": 3,
            "embedder": {
                "provider": "openai",
                "model": "text-embedding-3-small",
                "url": openai_embedder,
            },
        },
    )
    backup_api.wait_index_ready(table, "semantic_idx", timeout_s=30.0, until="complete")
    backup_api.batch_write(
        table,
        inserts={"doc:a": {"body": "alpha concept overview"}},
        sync_level="full_index",
    )
    _check_query_budgets(
        backup_api,
        table,
        {"semantic_search": "alpha concept", "indexes": ["semantic_idx"], "limit": 5},
    )
