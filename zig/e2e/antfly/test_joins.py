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

"""Stateful distributed join E2E coverage."""

from __future__ import annotations

import time
from typing import Any

import pytest

from helpers import wait_until


NUM_SHARDS = 4


def _create_join_table(stateful_api, prefix: str, *, num_shards: int = NUM_SHARDS) -> str:
    table_name = f"{prefix}_{time.time_ns()}"
    created = stateful_api.create_table(table_name, num_shards=num_shards)
    assert (created.get("name") or created.get("table_name")) == table_name
    return table_name


def _result_response(payload: dict[str, Any]) -> dict[str, Any]:
    responses = payload.get("responses")
    assert isinstance(responses, list) and responses
    response = responses[0]
    assert isinstance(response, dict)
    return response


def _result_hits(payload: dict[str, Any]) -> list[dict[str, Any]]:
    response = _result_response(payload)
    hits = response.get("hits")
    assert isinstance(hits, dict)
    items = hits.get("hits")
    assert isinstance(items, list)
    return items


def _join_profile(payload: dict[str, Any]) -> dict[str, Any]:
    response = _result_response(payload)
    profile = response.get("profile")
    assert isinstance(profile, dict)
    join_profile = profile.get("join")
    assert isinstance(join_profile, dict)
    return join_profile


def _joined_field(table_name: str, field_name: str) -> str:
    return f"{table_name}.{field_name}"


def _seed_join_tables(stateful_api, prefix: str) -> tuple[str, str]:
    docs_table = _create_join_table(stateful_api, f"{prefix}_docs")
    customers_table = _create_join_table(stateful_api, f"{prefix}_customers")

    customer_batch = stateful_api.batch_write(
        customers_table,
        inserts={
            "cust:a": {"name": "Alice", "tier": "gold"},
            "cust:b": {"name": "Bob", "tier": "silver"},
            "cust:z": {"name": "Zoe", "tier": "gold"},
        },
        sync_level="write",
    )
    assert customer_batch["inserted"] == 3

    docs = {
        f"{i:02x}_doc_{i:03d}": {
            "customer_id": "cust:a" if i % 2 == 0 else "cust:b",
            "title": f"Doc {i}",
            "body": "distributed join coverage",
        }
        for i in range(80)
    }
    docs["ff_doc_unmatched"] = {
        "customer_id": "cust:missing",
        "title": "Unmatched left",
        "body": "distributed join coverage",
    }

    docs_batch = stateful_api.batch_write(
        docs_table,
        inserts=docs,
        sync_level="full_text",
    )
    assert docs_batch["inserted"] == len(docs)

    return docs_table, customers_table


def _joined_query_result(
    stateful_api,
    docs_table: str,
    customers_table: str,
    *,
    join_type: str,
    strategy_hint: str | None = None,
    right_fields: list[str] | None = None,
) -> dict[str, Any]:
    join_payload: dict[str, Any] = {
        "right_table": customers_table,
        "join_type": join_type,
        "on": {
            "left_field": "customer_id",
            "right_field": "_id",
            "operator": "eq",
        },
        "right_fields": right_fields or ["name", "tier"],
    }
    if strategy_hint is not None:
        join_payload["strategy_hint"] = strategy_hint

    return stateful_api.query_table(
        docs_table,
        {
            "full_text_search": {"query": "body:distributed AND body:join"},
            "fields": ["title"],
            "limit": 128,
            "profile": True,
            "join": join_payload,
        },
    )


def test_distributed_shuffle_join_uses_antfly_to_antfly_execution(stateful_api):
    docs_table, customers_table = _seed_join_tables(stateful_api, "distributed_shuffle_join")
    joined_name = _joined_field(customers_table, "name")

    def query_result() -> dict[str, Any] | None:
        result = _joined_query_result(
            stateful_api,
            docs_table,
            customers_table,
            join_type="inner",
            strategy_hint="shuffle",
        )
        hits = _result_hits(result)
        if len(hits) < 40:
            return None
        profile = _join_profile(result)
        if profile.get("strategy_used") != "shuffle":
            return None
        if profile.get("distributed_execution") is not True:
            return None
        if int(profile.get("rows_matched", 0)) < 40:
            return None
        worker_attempts = profile.get("worker_attempts")
        if not isinstance(worker_attempts, list) or not worker_attempts:
            return None
        return result

    result = wait_until(query_result, timeout_s=30.0, interval_s=0.25)
    assert result is not None

    profile = _join_profile(result)
    assert profile["strategy_used"] == "shuffle"
    assert profile["distributed_execution"] is True
    assert profile["execution_mode"] in {"distributed_transient", "distributed_durable"}
    assert int(profile["shuffle_partitions"]) > 0
    assert int(profile["rows_matched"]) >= 80
    worker_attempts = profile.get("worker_attempts")
    assert isinstance(worker_attempts, list) and worker_attempts
    assert all(isinstance(attempt.get("worker_group_id"), int) for attempt in worker_attempts)

    hits = _result_hits(result)
    assert any(hit["_source"]["title"] == "Doc 0" for hit in hits)
    assert any(hit["_source"]["title"] == "Doc 1" for hit in hits)
    assert all(joined_name in hit["_source"] for hit in hits)


def test_distributed_right_join_returns_unmatched_right_rows(stateful_api):
    docs_table, customers_table = _seed_join_tables(stateful_api, "distributed_right_join")
    joined_name = _joined_field(customers_table, "name")
    joined_tier = _joined_field(customers_table, "tier")

    def query_result() -> dict[str, Any] | None:
        result = _joined_query_result(
            stateful_api,
            docs_table,
            customers_table,
            join_type="right",
        )
        hits = _result_hits(result)
        if len(hits) < 3:
            return None
        if not any(hit["_source"].get(joined_name) == "Zoe" for hit in hits):
            return None
        return result

    result = wait_until(query_result, timeout_s=30.0, interval_s=0.25)
    assert result is not None

    profile = _join_profile(result)
    assert profile["strategy_used"] == "broadcast"
    assert int(profile["rows_unmatched_right"]) == 1

    hits = _result_hits(result)
    zoe_hits = [hit for hit in hits if hit["_source"].get(joined_name) == "Zoe"]
    assert len(zoe_hits) == 1
    zoe_source = zoe_hits[0]["_source"]
    assert zoe_source["title"] is None
    assert zoe_source[joined_tier] == "gold"


def test_distributed_join_still_works_after_swarm_restart(stateful_api):
    docs_table, customers_table = _seed_join_tables(stateful_api, "distributed_join_restart")

    before_restart = wait_until(
        lambda: _joined_query_result(
            stateful_api,
            docs_table,
            customers_table,
            join_type="inner",
            strategy_hint="shuffle",
        ),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert before_restart is not None
    assert _join_profile(before_restart)["strategy_used"] == "shuffle"

    try:
        stateful_api.restart_server()
    except AssertionError as exc:
        pytest.skip(str(exc))

    def query_after_restart() -> dict[str, Any] | None:
        result = _joined_query_result(
            stateful_api,
            docs_table,
            customers_table,
            join_type="inner",
            strategy_hint="shuffle",
        )
        hits = _result_hits(result)
        if len(hits) < 40:
            return None
        profile = _join_profile(result)
        if profile.get("strategy_used") != "shuffle":
            return None
        return result

    after_restart = wait_until(query_after_restart, timeout_s=30.0, interval_s=0.25)
    assert after_restart is not None

    profile = _join_profile(after_restart)
    assert profile["strategy_used"] == "shuffle"
    assert profile["distributed_execution"] is True
    assert int(profile["rows_matched"]) >= 80
    worker_attempts = profile.get("worker_attempts")
    assert isinstance(worker_attempts, list) and worker_attempts
