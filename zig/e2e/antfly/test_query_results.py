# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

"""Public query result semantics across storage and coordinator boundaries."""

import time
import pytest

pytestmark = pytest.mark.reuse_antfly_process


@pytest.mark.parametrize("num_shards", [1, 2])
def test_query_pruning_is_independent_of_shard_count(stateful_api, num_shards):
    name = f"query_pruner_{time.time_ns()}"
    stateful_api.create_table(name, num_shards=num_shards)
    stateful_api.batch_write(
        name, inserts={"a": {"body": "alpha"}}, sync_level="full_index"
    )
    request = {"full_text_search": {"match_all": {}}, "limit": 10}
    initial = stateful_api.query_table(name, request)
    assert len(initial["responses"][0]["hits"]["hits"]) == 1, initial
    pruned = stateful_api.query_table(
        name, {**request, "pruner": {"min_absolute_score": 1000000}}
    )
    assert pruned["responses"][0]["hits"]["hits"] == [], pruned


@pytest.mark.parametrize("num_shards", [1, 2])
def test_query_empty_projection_omits_stored_source(stateful_api, num_shards):
    name = f"query_projection_{time.time_ns()}"
    stateful_api.create_table(name, num_shards=num_shards)
    stateful_api.batch_write(
        name,
        inserts={"a": {"body": "should not be loaded or returned"}},
        sync_level="full_index",
    )
    response = stateful_api.query_table(
        name, {"full_text_search": {"match_all": {}}, "fields": [], "limit": 10}
    )
    hits = response["responses"][0]["hits"]["hits"]
    assert len(hits) == 1, response
    assert not hits[0].get("_source"), response
