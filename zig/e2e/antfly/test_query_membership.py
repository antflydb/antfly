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

"""Boolean membership and verification work must survive query composition."""

import time

import pytest

from conftest import finish_create_table

pytestmark = pytest.mark.reuse_antfly_process


@pytest.mark.parametrize("num_shards", [1, 2])
def test_multiword_match_preserves_child_membership(stateful_api, num_shards):
    name = f"match_membership_{time.time_ns()}"
    stateful_api.create_table(name, num_shards=num_shards)
    stateful_api.batch_write(
        name,
        inserts={
            "both": {"text": "red blue", "tag": "keep"},
            "red": {"text": "red", "tag": "keep"},
            "blue": {"text": "blue", "tag": "keep"},
            "green": {"text": "green", "tag": "keep"},
            "excluded": {"text": "red blue", "tag": "keep"},
        },
        sync_level="full_index",
    )
    red = {"term": "red", "field": "text"}
    blue = {"term": "blue", "field": "text"}
    match = {"match": "red blue red", "field": "text"}
    cases = [
        (match, {"both", "red", "blue"}),
        ({"conjuncts": [match, red]}, {"both", "red"}),
        ({"bool": {"must": [match], "must_not": [blue]}}, {"red"}),
        # One match is one should-clause, even when it produces two terms.
        (
            {"bool": {"should": [match, red], "minimum_should_match": 2}},
            {"both", "red"},
        ),
        ({"bool": {"must_not": [match]}}, {"green"}),
    ]
    for scoped in (False, True):
        for predicate, expected in cases:
            if not scoped and "both" in expected:
                expected = expected | {"excluded"}
            for logical in (False, True):
                for reverse in (False, True):
                    parts = [{"contains": "keep", "field": "tag"}, predicate]
                    request = {
                        "fields": [],
                        "limit": 20,
                        "order_by": [{"field": "_id"}],
                        "execution": {
                            "value_verification": {
                                "allow_scan": True,
                                "max_documents": 20,
                            }
                        },
                    }
                    if scoped:
                        request["filter_query"] = {
                            "doc_id": {"ids": ["both", "red", "blue", "green"]}
                        }
                    if logical:
                        request["query"] = {
                            "conjuncts": parts[::-1] if reverse else parts
                        }
                    else:
                        request["full_text_search"] = predicate
                    result = stateful_api.query_table(name, request)["responses"][0]
                    assert result["status"] == 200, result
                    assert {hit["_id"] for hit in result["hits"]["hits"]} == expected, (
                        request,
                        result,
                    )


@pytest.mark.parametrize("num_shards", [1, 2])
def test_nested_indexed_bound_limits_original_value_reads(stateful_api, num_shards):
    name = f"nested_bounds_{time.time_ns()}"
    schema = {
        "default_type": "doc",
        "document_schemas": {
            "doc": {
                "schema": {
                    "type": "object",
                    "additionalProperties": True,
                    "properties": {
                        "certificate": {"type": "string", "x-antfly-types": ["keyword"]}
                    },
                }
            }
        },
    }
    finish_create_table(
        stateful_api,
        name,
        stateful_api.s.post(
            f"{stateful_api.url}/tables/{name}",
            json={"num_shards": num_shards, "schema": schema},
            timeout=30,
        ),
    )
    stateful_api.batch_write(
        name,
        inserts={
            str(i): {
                "text": "needle" if i == 29 else "other",
                "kind": "doc",
                "certificate": "selected" if i == 29 else "other",
            }
            for i in range(30)
        },
        sync_level="full_index",
    )
    metadata = {"equals": "doc", "field": "kind"}
    content = {"contains": "needle", "field": "text"}
    indexed = {"term": "selected", "field": "certificate"}
    for nested in (False, True):
        inner = {"conjuncts": [content, {"conjuncts": [indexed]}]}
        for reverse in (False, True):
            parts = [metadata, inner] if nested else [metadata, content, indexed]
            request = {
                "query": {"conjuncts": parts[::-1] if reverse else parts},
                "fields": [],
                "limit": 10,
                "profile": True,
                "order_by": [{"field": "_id"}],
                "execution": {
                    "value_verification": {"allow_scan": True, "max_documents": 2}
                },
            }
            result = stateful_api.query_table(name, request)["responses"][0]
            assert result["status"] == 200, result
            assert [hit["_id"] for hit in result["hits"]["hits"]] == ["29"], result
            assert result["profile"]["value_verification"]["documents"] == 1, result
