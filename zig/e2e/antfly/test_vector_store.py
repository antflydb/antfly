"""Experimental source-store ownership through the public standalone API."""

import time

import pytest
from helpers import assert_created_index, wait_until


def hit_ids(result):
    return [hit["_id"] for hit in result["responses"][0]["hits"]["hits"]]


@pytest.mark.parametrize("mode", ["primary_lsm", "vector_store"])
def test_vector_source_models_updates_deletes_and_restart(stateful_api, mode):
    api = stateful_api
    table = f"vector_source_{mode}_{time.time_ns()}"
    api.create_table(table, storage={"dense_embeddings": mode})
    assert api.get_table(table)["storage"]["dense_embeddings"] == mode
    for name, dimensions in [("model_a", 3), ("model_b", 2)]:
        assert_created_index(
            api.create_index(
                table,
                name,
                {
                    "name": name,
                    "type": "embeddings",
                    "external": True,
                    "dimension": dimensions,
                },
            ),
            name,
            "embeddings",
        )
    docs = {
        "a": {
            "text": "alpha",
            "_embeddings": {"model_a": [1, 0, 0], "model_b": [0, 1]},
        },
        "b": {"text": "beta", "_embeddings": {"model_a": [0, 1, 0], "model_b": [1, 0]}},
    }
    api.batch_write(table, inserts=docs, sync_level="full_index")

    def wait_ready(index, count):
        def ready():
            status = api.get_index(table, index).get("status", {})
            return (
                status.get("total_indexed") == count
                and status.get("query_visible_doc_count") == count
            )

        assert wait_until(ready, timeout_s=90, interval_s=0.5), api.get_index(
            table, index
        )

    wait_ready("model_a", 2)
    wait_ready("model_b", 2)

    def nearest(index, vector):
        return hit_ids(
            api.query_table(
                table, {"embeddings": {index: vector}, "indexes": [index], "limit": 2}
            )
        )

    assert nearest("model_a", [1, 0, 0])[0] == "a"
    assert nearest("model_b", [1, 0])[0] == "b"
    api.batch_write(
        table,
        inserts={
            "a": {
                "text": "updated",
                "_embeddings": {
                    "model_a": [0, 0, 1],
                    "model_b": [1, 0],
                },
            }
        },
        deletes=["b"],
        sync_level="full_index",
    )
    wait_ready("model_a", 1)
    wait_ready("model_b", 1)
    assert nearest("model_a", [0, 0, 1]) == ["a"]
    assert nearest("model_b", [1, 0]) == ["a"]
    api.restart_server()
    assert api.get_table(table)["storage"]["dense_embeddings"] == mode
    if mode == "vector_store":
        # Accounting must survive startup writer retirement, before any query
        # or write happens to reacquire a live writer for this table.
        assert wait_until(
            lambda: api.get_table(table)
            .get("storage_status", {})
            .get("source_vectors"),
            timeout_s=30,
            interval_s=0.5,
        )
    assert wait_until(
        lambda: nearest("model_a", [0, 0, 1]) == ["a"], timeout_s=90, interval_s=1
    )
    assert nearest("model_b", [1, 0]) == ["a"]
    if mode == "vector_store":

        def source_stats():
            return api.get_table(table).get("storage_status", {}).get("source_vectors")

        def reclaimed_source_stats():
            stats = source_stats()
            if (stats and stats.get("retained_payloads") == 2
                    and stats.get("retained_payload_bytes") == 20
                    and stats.get("collection_pending_bytes") == 0):
                return stats
            return None

        # Incremental marking can remain pending after serving is ready.
        stats = wait_until(reclaimed_source_stats, timeout_s=30, interval_s=0.5)
        assert stats is not None, source_stats()
        assert stats["retained_payloads"] == 2
        assert stats["retained_payload_bytes"] == 20
        assert stats["live_payloads_at_collection"] == 2
        # Removing all ANN consumers must preserve the table's two sources.
        api.delete_index(table, "model_a")
        api.delete_index(table, "model_b")
        api.restart_server()
        assert_created_index(
            api.create_index(
                table,
                "model_a",
                {
                    "name": "model_a",
                    "type": "embeddings",
                    "external": True,
                    "dimension": 3,
                },
            ),
            "model_a",
            "embeddings",
        )
        wait_ready("model_a", 1)
        assert nearest("model_a", [0, 0, 1]) == ["a"]


@pytest.mark.parametrize(
    "payload",
    [
        {"num_shards": 2, "storage": {"dense_embeddings": "vector_store"}},
        {"num_shards": 1, "storage": {"dense_embeddings": "unknown"}},
        {"num_shards": 1, "storage": {"dense_embedding": "vector_store"}},
    ],
)
def test_vector_source_rejects_unsupported_table_configuration(stateful_api, payload):
    import requests

    table = f"vector_source_rejected_{time.time_ns()}"
    with pytest.raises(requests.HTTPError) as rejected:
        stateful_api.post(f"/tables/{table}", payload)
    assert rejected.value.response.status_code == 400
