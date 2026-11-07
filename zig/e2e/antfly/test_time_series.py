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

"""Time-series SQL/JSON semantics, exact reads, and algebraic maintenance."""

import time

import pytest
from conftest import finish_create_table


def _schema():
    return {
        "default_type": "reading",
        "index_sort": [{"field": "observed_at", "order": "desc"}],
        "document_schemas": {
            "reading": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "observed_at": {
                            "type": "string",
                            "format": "date-time",
                            "x-antfly-field": {"type": "date", "sortable": True},
                        },
                        "device_id": {
                            "type": "string",
                            "x-antfly-field": {"type": "keyword", "sortable": True},
                        },
                        "temperature": {
                            "type": "number",
                            "x-antfly-field": {"type": "number", "sortable": True},
                        },
                    },
                }
            }
        },
    }


def _query(origin=None, bounds=True):
    histogram = {
        "type": "date_histogram",
        "field": "observed_at",
        "fixed_interval": "5m",
        "min_doc_count": 0,
        "sub_aggregations": {"average": {"type": "avg", "field": "temperature"}},
    }
    if bounds:
        histogram["date_bounds"] = {
            "start": "2026-11-06T12:00:00Z",
            "end": "2026-11-06T12:20:00Z",
        }
    if origin:
        histogram["bucket_origin"] = origin
    return {"limit": 0, "aggregations": {"points": histogram}}


@pytest.mark.parametrize("shards", [1, 2])
def test_time_series_fixed_buckets_and_algebraic_corrections(stateful_api, shards):
    api = stateful_api
    names = []
    for algebraic in [False, True]:
        name = f"series_{shards}_{algebraic}_{time.time_ns()}"
        config = {"num_shards": shards, "schema": _schema()}
        if algebraic:
            config["indexes"] = {
                "series_aggregates": {
                    "type": "algebraic",
                    "derive_from_schema": True,
                    "aggregates": [
                        {
                            "name": op,
                            "op": op,
                            **({"measure": "temperature"} if op != "count" else {}),
                            "time": "observed_at",
                            "bucket": "5m",
                        }
                        for op in ["count", "sum", "avg", "min", "max"]
                    ],
                }
            }
        finish_create_table(api, name, api._request("POST", f"/tables/{name}", config))
        api.batch_write(
            name,
            inserts={
                "a": {
                    "observed_at": "2026-11-06T13:01:00+01:00",
                    "device_id": "sensor",
                    "temperature": 10,
                },
                "b": {
                    "observed_at": "2026-11-06T12:04:59.999999999Z",
                    "device_id": "sensor",
                    "temperature": 20,
                },
                "c": {
                    "observed_at": "2026-11-06T12:10:00Z",
                    "device_id": "sensor",
                    "temperature": 30.123456789,
                },
            },
            sync_level="full_index",
        )
        names.append(name)

    def points(name, query):
        payload = api.query_table(name, query)
        response = payload.get("responses", [payload])[0]
        assert response.get("status", 200) == 200, payload
        return response["aggregations"]["points"]["buckets"]

    expected = points(names[0], _query())
    assert [b["doc_count"] for b in expected] == [2, 0, 1, 0], expected
    assert expected[0]["sub_aggregations"]["average"]["avg"] == 15
    assert points(names[1], _query()) == expected
    grouped_query = {
        "limit": 0,
        "aggregations": {
            "series": {
                "type": "terms",
                "fields": ["device_id"],
                "size": 10,
                "sub_aggregations": _query()["aggregations"],
            }
        },
    }
    grouped_results = []
    for name in names:
        payload = api.query_table(name, grouped_query)
        response = payload.get("responses", [payload])[0]
        grouped_results.append(response["aggregations"]["series"]["buckets"])
    assert grouped_results[0] == grouped_results[1]
    assert grouped_results[0][0]["sub_aggregations"]["points"]["buckets"] == expected
    assert points(names[1], _query("1970-01-01T00:02:00Z", bounds=False)) == points(
        names[0], _query("1970-01-01T00:02:00Z", bounds=False)
    )

    for name in names:
        api.batch_write(
            name,
            inserts={
                "a": {
                    "observed_at": "2026-11-06T12:06:00Z",
                    "device_id": "sensor",
                    "temperature": 40,
                }
            },
            deletes=["b"],
            sync_level="full_index",
        )
    corrected = points(names[0], _query())
    assert [b["doc_count"] for b in corrected] == [0, 1, 1, 0], corrected
    assert points(names[1], _query()) == corrected
    for name in names:
        read = {
            "order_by": [{"field": "observed_at", "desc": True}],
            "limit": 1,
            "profile": True,
            "filter_query": {
                "conjuncts": [
                    {"field": "device_id", "term": "sensor"},
                    {
                        "field": "observed_at",
                        "start": "2026-11-06T12:00:00Z",
                        "end": "2026-11-06T13:00:00Z",
                        "inclusive_start": True,
                        "inclusive_end": False,
                    },
                ]
            },
        }
        result = api.query_table(name, read)
        response = result.get("responses", [result])[0]
        assert response["hits"]["hits"][0]["_id"] == "c", result
        assert response["profile"]["sort"]["plan"] != "stored_json_debug", result
        continuation = dict(
            read,
            remote_snapshot=response["remote_snapshot"],
            search_after=response["hits"]["hits"][0]["_sort"],
        )
        page = api.query_table(name, continuation)
        next_response = page.get("responses", [page])[0]
        assert next_response["hits"]["hits"][0]["_id"] == "a", page

    sql = api.post(
        "/sql",
        {
            "statement": f"SELECT date_bin('5m', observed_at, '1970-01-01T00:00:00Z'::datetime) AS bucket, AVG(temperature) FROM \"{names[0]}\" GROUP BY bucket ORDER BY bucket"
        },
    )
    assert len(sql["rows"]) == 2, sql
    assert (
        float(sql["rows"][1][1])
        == corrected[2]["sub_aggregations"]["average"]["avg"]
        == 30.123456789
    )


def test_time_series_ttl_removes_exact_aggregate_contributions(stateful_api):
    from helpers import wait_until

    api = stateful_api
    name = f"series_ttl_{time.time_ns()}"
    schema = _schema()
    schema["ttl"] = {"field": "expiry_reference", "duration": "1s"}
    schema["document_schemas"]["reading"]["schema"]["properties"][
        "expiry_reference"
    ] = {
        "type": "string",
        "format": "date-time",
        "x-antfly-field": {"type": "date"},
    }
    config = {
        "num_shards": 1,
        "schema": schema,
        "indexes": {
            "series_aggregates": {
                "type": "algebraic",
                "derive_from_schema": True,
                "aggregates": [
                    {
                        "name": "rows",
                        "op": "count",
                        "time": "observed_at",
                        "bucket": "5m",
                    },
                    {
                        "name": "average",
                        "op": "avg",
                        "measure": "temperature",
                        "time": "observed_at",
                        "bucket": "5m",
                    },
                ],
            }
        },
    }
    finish_create_table(api, name, api._request("POST", f"/tables/{name}", config))
    documents = {
        "a": {
            "observed_at": "2026-11-06T12:01:00Z",
            "device_id": "sensor",
            "temperature": 10,
            "expiry_reference": "2400-01-01T00:00:00Z",
        },
        "b": {
            "observed_at": "2026-11-06T12:02:00Z",
            "device_id": "sensor",
            "temperature": 20,
            "expiry_reference": "2400-01-01T00:00:00Z",
        },
    }
    api.batch_write(name, inserts=documents, sync_level="full_index")

    def response(query):
        payload = api.query_table(name, query)
        return payload.get("responses", [payload])[0]

    assert response(_query())["aggregations"]["points"]["buckets"][0]["doc_count"] == 2
    documents["a"]["expiry_reference"] = "2000-01-01T00:00:00Z"
    api.batch_write(name, inserts={"a": documents["a"]}, sync_level="full_index")
    assert wait_until(
        lambda: (
            not response({"full_text_search": {"ids": ["a"]}, "limit": 1})["hits"][
                "hits"
            ]
        ),
        timeout_s=65,
        interval_s=0.5,
    )
    buckets = response(_query())["aggregations"]["points"]["buckets"]
    assert buckets[0]["doc_count"] == 1, buckets
    assert buckets[0]["sub_aggregations"]["average"]["avg"] == 20, buckets
