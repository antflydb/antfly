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

"""Stateful public API Bleve query string tests."""

from __future__ import annotations

import time

from helpers import wait_until


def _hit_ids(result: dict) -> list[str]:
    responses = result.get("responses", [])
    if not responses:
        return []
    return [hit.get("_id") for hit in responses[0].get("hits", {}).get("hits", [])]


def _query_hit_ids(stateful_api, table_name: str, payload: dict) -> list[str] | None:
    try:
        result = stateful_api.query_table(table_name, payload)
    except Exception:
        return None
    return _hit_ids(result)


def test_bleve_query_string_full_text_and_filter(stateful_api):
    table_name = f"query_string_{time.time_ns()}"
    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha guide",
                "body": "hello world",
                "status": "published",
            },
            "doc:b": {
                "title": "beta notes",
                "body": "hello archive",
                "status": "review",
            },
            "doc:c": {
                "title": "gamma guide",
                "body": "secondary text",
                "status": "draft",
            },
        },
        sync_level="full_text",
    )
    assert batch["inserted"] == 3

    full_text_result = wait_until(
        lambda: (
            ids
            if (ids := _query_hit_ids(
                stateful_api,
                table_name,
                {
                    "full_text_search": {
                        "query": "title:guide AND body:hello",
                    },
                    "limit": 5,
                },
            ))
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert full_text_result is not None
    assert full_text_result[0] == "doc:a"

    filtered_result = wait_until(
        lambda: (
            ids
            if (ids := _query_hit_ids(
                stateful_api,
                table_name,
                {
                    "full_text_search": {
                        "query": "body:hello",
                    },
                    "filter_query": {
                        "query": "status:published OR status:review",
                    },
                    "exclusion_query": {
                        "query": "status:draft",
                    },
                    "limit": 5,
                },
            ))
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert filtered_result is not None
    assert filtered_result[:2] == ["doc:a", "doc:b"]
    assert "doc:c" not in filtered_result


def test_direct_full_text_match_and_prefix(stateful_api):
    table_name = f"direct_full_text_{time.time_ns()}"
    created = stateful_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = stateful_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "body": "hello world archive",
            },
            "doc:b": {
                "body": "help wanted memo",
            },
            "doc:c": {
                "body": "secondary text only",
            },
        },
        sync_level="full_text",
    )
    assert batch["inserted"] == 3

    match_result = wait_until(
        lambda: (
            ids
            if (ids := _query_hit_ids(
                stateful_api,
                table_name,
                {
                    "full_text_search": {
                        "match": {
                            "field": "body",
                            "text": "hello world",
                        }
                    },
                    "limit": 5,
                },
            ))
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert match_result is not None
    assert match_result[0] == "doc:a"

    prefix_result = wait_until(
        lambda: (
            ids
            if (ids := _query_hit_ids(
                stateful_api,
                table_name,
                {
                    "full_text_search": {
                        "prefix": {
                            "field": "body",
                            "text": "hel",
                        }
                    },
                    "limit": 5,
                },
            ))
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert prefix_result is not None
    assert prefix_result[:2] == ["doc:a", "doc:b"]
