"""Public query deadlines remain valid across request and routing clocks."""

import pytest


@pytest.mark.parametrize("num_shards", [1, 2])
def test_query_timeout_budget_survives_routing(stateful_api, num_shards):
    table = f"query_deadline_{num_shards}"
    stateful_api.create_table(table, num_shards=num_shards)
    stateful_api.batch_write(
        table, inserts={"doc:a": {"title": "alpha"}}, sync_level="write"
    )
    # Warm publication and the query path first; these assertions concern
    # deadline clock identity, not startup latency.
    query = {"full_text_search": {"match_all": {}}, "limit": 10}
    stateful_api.query_table(table, query)
    for timeout_ms in (1000, 5000, 30000):
        result = stateful_api.query_table(table, {**query, "timeout_ms": timeout_ms})
        hits = result["responses"][0]["hits"]["hits"]
        assert [hit["_id"] for hit in hits] == ["doc:a"]
    response = stateful_api._request(
        "POST", f"/tables/{table}/query", {**query, "timeout_ms": 0}
    )
    assert response.status_code == 504
    assert response.json()["code"] == "query_timeout"
