# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Stateful public API Bleve query string tests."""

from __future__ import annotations

import json
import time

import pytest
import requests

from helpers import wait_until


pytestmark = pytest.mark.reuse_antfly_process


def _hit_ids(result: dict, response_index: int = 0) -> list[str]:
    responses = result.get("responses", [])
    if len(responses) <= response_index:
        return []
    return [
        hit.get("_id")
        for hit in responses[response_index].get("hits", {}).get("hits", [])
    ]


def _query_hit_ids(stateful_api, table_name: str, payload: dict) -> list[str] | None:
    try:
        result = stateful_api.query_table(table_name, payload)
    except Exception:
        return None
    return _hit_ids(result)


def _post_ndjson_multiquery(stateful_api, path: str, payloads: list[dict]) -> dict:
    body = (
        "\n".join(json.dumps(payload, separators=(",", ":")) for payload in payloads)
        + "\n"
    )
    response = requests.post(
        f"{stateful_api.url}{path}",
        data=body,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=30,
    )
    assert response.status_code == 200, response.text
    return response.json()


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
            if (
                ids := _query_hit_ids(
                    stateful_api,
                    table_name,
                    {
                        "full_text_search": {
                            "query": "title:guide AND body:hello",
                        },
                        "limit": 5,
                    },
                )
            )
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
            if (
                ids := _query_hit_ids(
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
                )
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert filtered_result is not None
    assert filtered_result[:2] == ["doc:a", "doc:b"]
    assert "doc:c" not in filtered_result

    table_multiquery = wait_until(
        lambda: (
            result
            if len(
                (
                    result := _post_ndjson_multiquery(
                        stateful_api,
                        f"/tables/{table_name}/query",
                        [
                            {
                                "fields": ["title"],
                                "limit": 5,
                            },
                            {
                                "fields": ["status"],
                                "limit": 5,
                            },
                        ],
                    )
                ).get("responses", [])
            )
            == 2
            and len(result["responses"][0].get("hits", {}).get("hits", [])) > 0
            and len(result["responses"][1].get("hits", {}).get("hits", [])) > 0
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert table_multiquery is not None

    global_multiquery = wait_until(
        lambda: (
            result
            if len(
                (
                    result := _post_ndjson_multiquery(
                        stateful_api,
                        "/query",
                        [
                            {
                                "table": table_name,
                                "fields": ["body"],
                                "limit": 5,
                            },
                            {
                                "table": table_name,
                                "fields": ["status"],
                                "limit": 5,
                            },
                        ],
                    )
                ).get("responses", [])
            )
            == 2
            and len(result["responses"][0].get("hits", {}).get("hits", [])) > 0
            and len(result["responses"][1].get("hits", {}).get("hits", [])) > 0
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert global_multiquery is not None


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
            if (
                ids := _query_hit_ids(
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
                )
            )
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
            if (
                ids := _query_hit_ids(
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
                )
            )
            else None
        ),
        timeout_s=30.0,
        interval_s=0.5,
    )
    assert prefix_result is not None
    assert prefix_result[:2] == ["doc:a", "doc:b"]


def test_query_string_validation_and_indexed_case_folding(stateful_api):
    """Check public lowering, syntax errors, and post-analysis term semantics."""
    table = f"query_contract_{time.time_ns()}"
    stateful_api.create_table(table, num_shards=1)
    stateful_api.batch_write(
        table,
        inserts={
            "one": {"text": "Maxwell Ghislaine"},
            "two": {"text": "Ghislaine unrelated"},
            "three": {"text": "other words"},
        },
        sync_level="full_text",
    )
    path = f"/tables/{table}/query"

    def query_ids(leaf):
        result = stateful_api.post(path, {"full_text_search": leaf, "limit": 10})
        assert not result["responses"][0].get("error"), result
        return set(_hit_ids(result))

    assert wait_until(
        lambda: query_ids({"match": "maxwell", "field": "text"}) == {"one"},
        timeout_s=30,
    )
    # Exact operators match indexed terms. The analyzer stems maxwell to maxwel.
    assert query_ids({"term": "maxwell", "field": "text"}) == set()
    assert query_ids({"term": "maxwel", "field": "text"}) == {"one"}
    for operator, pattern, expected in (
        ("term", "MAXWEL", {"one"}),
        ("regexp", "MAXWEL", {"one"}),
        ("wildcard", "MAXWE?", {"one"}),
        ("prefix", "GHISL", {"one", "two"}),
    ):
        assert (
            query_ids({operator: pattern, "field": "text", "case_insensitive": True})
            == expected
        )
    assert query_ids({"query": "Ghisl*"}) == {"one", "two"}
    assert query_ids({"query": "Ghisl?in*"}) == {"one", "two"}

    for malformed in (
        "+(",
        '"unterminated',
        "AND",
        "alpha OR",
        "(alpha))",
        "alpha^nan",
    ):
        response = requests.post(
            stateful_api.url + path,
            json={"full_text_search": {"query": malformed}, "limit": 5},
            timeout=10,
        )
        assert response.status_code == 400, (
            malformed,
            response.status_code,
            response.text,
        )
    response = requests.post(
        stateful_api.url + path,
        json={"full_text_search": {"prefix": "Ghisl", "case_insensitive": "true"}},
        timeout=10,
    )
    assert response.status_code == 400, response.text
