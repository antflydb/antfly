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

import copy
import json
from pathlib import Path
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
                "exclude_null": True,
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
                    {"field": "device_id", "equals": "sensor"},
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


@pytest.mark.parametrize("shards", [1, 2])
def test_sorted_time_range_preflight_and_bidirectional_pages(stateful_api, shards):
    """Cold sort admission must count filters without inheriting page controls."""
    api = stateful_api
    name = f"series_sorted_probe_{shards}_{time.time_ns()}"
    finish_create_table(
        api,
        name,
        api._request(
            "POST",
            f"/tables/{name}",
            {
                "num_shards": shards,
                "schema": _schema(),
            },
        ),
    )
    readings = {
        str(i): {
            "device_id": "sensor",
            "observed_at": f"2026-11-06T12:0{i}:00Z",
            "temperature": i,
        }
        for i in range(3)
    }
    readings["outside"] = {
        "device_id": "sensor",
        "observed_at": "2026-11-06T13:00:00Z",
        "temperature": 99,
    }
    api.batch_write(name, inserts=readings, sync_level="full_index")
    query = {
        "order_by": [{"field": "observed_at", "desc": True}],
        "limit": 1,
        "profile": True,
        "filter_query": {
            "field": "observed_at",
            "start": "2026-11-06T12:00:00Z",
            "end": "2026-11-06T13:00:00Z",
            "inclusive_start": True,
            "inclusive_end": False,
        },
    }

    def read(request):
        payload = api.query_table(name, request)
        response = payload.get("responses", [payload])[0]
        assert not response.get("error"), payload
        assert response["profile"]["sort"]["plan"] != "stored_json_debug", payload
        return response

    first = read(query)
    assert first["hits"]["hits"][0]["_id"] == "2", first
    page_query = dict(query, remote_snapshot=first["remote_snapshot"])
    second = read(dict(page_query, search_after=first["hits"]["hits"][0]["_sort"]))
    assert second["hits"]["hits"][0]["_id"] == "1", second
    previous = read(dict(page_query, search_before=second["hits"]["hits"][0]["_sort"]))
    assert previous["hits"]["hits"][0]["_id"] == "2", previous


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


@pytest.mark.parametrize("shards", [1, 2])
def test_sdk_preset_grouped_materializations(stateful_api, shards):
    """Exercise the exact SDK wire shape, including presence counts and aliases."""
    fixture = json.loads(
        (Path(__file__).parent / "fixtures/time-series-sdk.json").read_text()
    )
    api = stateful_api
    names = []
    for indexed in [False, True]:
        config = copy.deepcopy(fixture["table"])
        config["num_shards"] = shards
        if not indexed:
            config.pop("indexes")
        name = f"sdk_series_{shards}_{indexed}_{time.time_ns()}"
        finish_create_table(api, name, api._request("POST", f"/tables/{name}", config))
        names.append(name)
    readings = {
        "a": {
            "device_id": "sensor",
            "region": "west",
            "observed_at": "2026-11-06T12:01:00Z",
            "temperature": 10,
        },
        "b": {
            "device_id": "sensor",
            "region": "west",
            "observed_at": "2026-11-06T12:02:00Z",
            "temperature": 30,
        },
        "c": {
            "device_id": "sensor",
            "region": "west",
            "observed_at": "2026-11-06T12:10:00Z",
        },
        "d": {
            "device_id": "sensor",
            "region": "west",
            "observed_at": "2026-11-06T12:15:00Z",
            "temperature": 0,
        },
        "tie_newline": {
            "device_id": "AA\n",
            "region": "west",
            "observed_at": "2026-11-06T12:01:00Z",
            "temperature": 1,
        },
        "tie_space": {
            "device_id": "AA ",
            "region": "west",
            "observed_at": "2026-11-06T12:01:00Z",
            "temperature": 1,
        },
        "excluded": {
            "device_id": "sensor",
            "region": "east",
            "observed_at": "2026-11-06T12:01:00Z",
            "temperature": 900,
        },
        "other": {
            "device_id": "other",
            "region": "west",
            "observed_at": "2026-11-06T12:01:00Z",
            "temperature": 100,
        },
        "boundary": {
            "device_id": "sensor",
            "region": "west",
            "observed_at": "2026-11-06T12:20:00Z",
            "temperature": 900,
        },
    }
    for name in names:
        api.batch_write(name, inserts=readings, sync_level="full_index")
    index_info = api.get_index(names[1], "series_aggregates")
    for recipe in index_info["config"]["aggregates"]:
        assert recipe["time"] == "observed_at", index_info
        assert recipe["bucket"] == "5m", index_info

    def run(name, query=None):
        payload = api.post("/query", dict(query or fixture["query"], table=name))
        result = payload.get("responses", [payload])[0]
        assert result.get("status", 200) == 200, payload
        assert not result.get("error"), payload
        assert result["hits"]["hits"] == [], payload
        return result

    def compare():
        scan, indexed = [run(name) for name in names]
        assert indexed["aggregations"] == scan["aggregations"]
        assert (
            indexed["profile"].get("aggregation_execution") == "materialized_temporal"
        ), indexed
        buckets = indexed["aggregations"]["series"]["buckets"]
        assert buckets[0]["key"] == "sensor", buckets
        assert buckets[0]["doc_count"] == 4, buckets
        points = buckets[0]["sub_aggregations"]["points"]["buckets"]
        assert [p["doc_count"] for p in points] == [2, 0, 1, 1], points
        assert points[0]["sub_aggregations"]["average"]["avg"] == 20
        assert (
            points[2]["sub_aggregations"]["__antfly_series_count_total"]["count"] == 0
        )
        assert (
            points[3]["sub_aggregations"]["__antfly_series_count_total"]["count"] == 1
        )
        assert points[3]["sub_aggregations"]["total"]["value"] == 0

    compare()
    alias_only = {
        "limit": 0,
        "profile": True,
        "aggregations": {"date_histogram": {"type": "avg", "field": "temperature"}},
    }
    plain, indexed_plain = [run(name, alias_only) for name in names]
    assert indexed_plain["aggregations"] == plain["aggregations"]
    assert "temporal" not in indexed_plain["profile"].get(
        "aggregation_execution", ""
    ), indexed_plain
    composite = copy.deepcopy(fixture["query"])
    composite["aggregations"]["series"]["fields"] = ["device_id", "region"]
    scan, indexed = [run(name, composite) for name in names]
    assert indexed["aggregations"] == scan["aggregations"]
    assert indexed["profile"].get("aggregation_execution") == "materialized_temporal", (
        indexed
    )
    # A replay is an upsert, and a correction/deletion changes maintained state.
    for name in names:
        api.batch_write(name, inserts=readings, sync_level="full_index")
    compare()
    for name in names:
        api.batch_write(
            name,
            inserts={"a": dict(readings["a"], temperature=50)},
            deletes=["b"],
            sync_level="full_index",
        )
    scan, indexed = [run(name) for name in names]
    assert indexed["aggregations"] == scan["aggregations"]
    assert indexed["profile"].get("aggregation_execution") == "materialized_temporal", (
        indexed
    )
    # Non-aligned source predicates cannot be answered by whole stored buckets.
    unaligned = copy.deepcopy(fixture["query"])
    unaligned["filter_query"]["conjuncts"][0]["start"] = "2026-11-06T12:00:01Z"
    unaligned["aggregations"]["series"]["sub_aggregations"]["points"]["date_bounds"][
        "start"
    ] = "2026-11-06T12:00:01Z"
    scan, indexed = [run(name, unaligned) for name in names]
    assert indexed["aggregations"] == scan["aggregations"]
    assert indexed["profile"].get("aggregation_execution") != "materialized_temporal", (
        indexed
    )
    # Extra unfiltered dimensions can omit source documents, so exact scanning
    # must retain the source time predicate after declining the temporal plan.
    unfiltered = copy.deepcopy(fixture["query"])
    unfiltered["filter_query"]["conjuncts"] = unfiltered["filter_query"]["conjuncts"][
        :1
    ]
    scan, indexed = [run(name, unfiltered) for name in names]
    assert indexed["aggregations"] == scan["aggregations"]
    assert indexed["profile"].get("aggregation_execution") != "materialized_temporal", (
        indexed
    )


def test_grouped_materializations_exceed_source_collection_limit(stateful_api):
    """A maintained query must not hydrate the fallback's first 100,000 hits."""
    fixture = json.loads(
        (Path(__file__).parent / "fixtures/time-series-sdk.json").read_text()
    )
    api = stateful_api
    name = f"sdk_series_scale_{time.time_ns()}"
    finish_create_table(
        api, name, api._request("POST", f"/tables/{name}", fixture["table"])
    )
    count = 100_001
    for first in range(0, count, 1000):
        # Bulk full-index visibility can include checkpoint warming. Allow a
        # longer setup transport timeout without relaxing the query's budget.
        api.batch_write_with_timeout(
            name,
            timeout_s=120,
            inserts={
                str(i): {
                    "device_id": "sensor",
                    "region": "west",
                    "observed_at": "2026-11-06T12:01:00Z",
                    "temperature": 1,
                }
                for i in range(first, min(first + 1000, count))
            },
            sync_level="full_index",
        )
    payload = api.post("/query", dict(fixture["query"], table=name))
    result = payload.get("responses", [payload])[0]
    assert not result.get("error"), payload
    assert result["hits"]["hits"] == [], payload
    assert result["profile"].get("aggregation_execution") == "materialized_temporal", (
        result
    )
    bucket = result["aggregations"]["series"]["buckets"][0]
    assert bucket["doc_count"] == count
    metrics = bucket["sub_aggregations"]["points"]["buckets"][0]["sub_aggregations"]
    assert metrics["total"]["value"] == count
    assert metrics["average"]["avg"] == 1
    populated = copy.deepcopy(fixture["query"])
    histogram = populated["aggregations"]["series"]["sub_aggregations"]["points"]
    histogram.pop("date_bounds")
    histogram["min_doc_count"] = 1
    response = api.post("/query", dict(populated, table=name))
    result = response.get("responses", [response])[0]
    assert not result.get("error"), response
    assert result["profile"].get("aggregation_execution") == "materialized_temporal", (
        result
    )
    bucket = result["aggregations"]["series"]["buckets"][0]
    assert bucket["doc_count"] == count
    assert len(bucket["sub_aggregations"]["points"]["buckets"]) == 1
    assert (
        bucket["sub_aggregations"]["points"]["buckets"][0]["sub_aggregations"]["total"][
            "value"
        ]
        == count
    )


def test_signed_origin_and_bounded_nesting_rejection(stateful_api):
    api = stateful_api
    name = f"series_epoch_{time.time_ns()}"
    finish_create_table(
        api,
        name,
        api._request("POST", f"/tables/{name}", {"num_shards": 1, "schema": _schema()}),
    )
    api.batch_write(
        name,
        inserts={
            "epoch": {
                "observed_at": "1970-01-01T00:00:01Z",
                "device_id": "sensor",
                "temperature": 2,
            }
        },
        sync_level="full_index",
    )
    histogram = {
        "type": "date_histogram",
        "field": "observed_at",
        "fixed_interval": "5m",
        "bucket_origin": "1970-01-01T00:02:00Z",
        "min_doc_count": 0,
        "date_bounds": {"start": "1970-01-01T00:00:00Z", "end": "1970-01-01T00:05:00Z"},
        "sub_aggregations": {"average": {"type": "avg", "field": "temperature"}},
    }
    payload = api.query_table(name, {"limit": 0, "aggregations": {"points": histogram}})
    result = payload.get("responses", [payload])[0]
    points = result["aggregations"]["points"]["buckets"]
    assert points[0]["key"] == "1969-12-31T23:57:00Z", payload
    assert points[0]["doc_count"] == 1
    sql = api.post(
        "/sql",
        {
            "statement": f"SELECT date_bin('5m', observed_at, '1970-01-01T00:02:00Z'::datetime) FROM \"{name}\""
        },
    )
    assert str(sql["rows"][0][0]).startswith("1969-12-31T23:57:00"), sql
    nested = copy.deepcopy(histogram)
    nested["sub_aggregations"] = {"nested": histogram}
    rejected = api._request(
        "POST",
        f"/tables/{name}/query",
        {"limit": 0, "aggregations": {"points": nested}},
    )
    assert rejected.status_code == 400, rejected.text


def test_histogram_deadline_and_output_budget_remain_safe(stateful_api):
    api = stateful_api
    name = f"series_cancel_budget_{time.time_ns()}"
    finish_create_table(
        api,
        name,
        api._request("POST", f"/tables/{name}", {"num_shards": 1, "schema": _schema()}),
    )
    api.batch_write(
        name,
        inserts={
            "a": {"observed_at": "2026-11-06T12:00:00Z", "temperature": 1},
            "b": {"observed_at": "2026-11-06T12:00:09.999Z", "temperature": 3},
        },
        sync_level="full_index",
    )
    histogram = {
        "type": "date_histogram",
        "field": "observed_at",
        "fixed_interval": "1ms",
        "min_doc_count": 0,
        "date_bounds": {"start": "2026-11-06T12:00:00Z", "end": "2026-11-06T12:00:10Z"},
        "sub_aggregations": {
            f"m{i}": {"type": "avg", "field": "temperature"} for i in range(16)
        },
    }
    for metrics in (8, 16):
        histogram["sub_aggregations"] = {
            f"m{i}": {"type": "avg", "field": "temperature"} for i in range(metrics)
        }
        for timeout in (10, 50, 200):
            response = api._request(
                "POST",
                f"/tables/{name}/query",
                {
                    "limit": 0,
                    "timeout_ms": timeout,
                    "aggregations": {"points": histogram},
                },
            )
            body = response.json()
            result = body.get("responses", [body])[0]
            if metrics == 16:
                assert response.status_code >= 400 or result.get("error"), response.text
            elif response.status_code < 400 and not result.get("error"):
                assert len(result["aggregations"]["points"]["buckets"]) == 10000
            assert api._request("GET", "/tables").status_code == 200


def test_unbounded_histogram_scan_index_gap_equivalence(stateful_api):
    api = stateful_api
    output = []
    for indexed in (False, True):
        name = f"series_implicit_gap_{indexed}_{time.time_ns()}"
        config = {"num_shards": 1, "schema": _schema()}
        if indexed:
            config["indexes"] = {
                "series_aggregates": {
                    "type": "algebraic",
                    "derive_from_schema": True,
                    "aggregates": [
                        {
                            "name": "count",
                            "op": "count",
                            "time": "observed_at",
                            "bucket": "5m",
                        },
                        {
                            "name": "avg",
                            "op": "avg",
                            "measure": "temperature",
                            "time": "observed_at",
                            "bucket": "5m",
                        },
                    ],
                }
            }
        finish_create_table(api, name, api._request("POST", f"/tables/{name}", config))
        api.batch_write(
            name,
            inserts={
                "a": {"observed_at": "2026-11-06T12:00:00Z", "temperature": 1},
                "b": {"observed_at": "2026-11-06T12:10:00Z", "temperature": 3},
            },
            sync_level="full_index",
        )
        response = api.query_table(
            name,
            {
                "limit": 0,
                "profile": True,
                "aggregations": {
                    "points": {
                        "type": "date_histogram",
                        "field": "observed_at",
                        "fixed_interval": "5m",
                        "min_doc_count": 0,
                        "sub_aggregations": {
                            "avg": {"type": "avg", "field": "temperature"}
                        },
                    }
                },
            },
        )
        output.append(response.get("responses", [response])[0])
    assert output[0]["aggregations"] == output[1]["aggregations"], output
    assert len(output[1]["aggregations"]["points"]["buckets"]) == 3
    assert (
        output[1]["profile"].get("aggregation_execution") == "materialized_temporal"
    ), output[1]


def test_sdk_generated_sql_matches_group_selection_and_measurements(stateful_api):
    api = stateful_api
    fixture = json.loads(
        (Path(__file__).parent / "fixtures/time-series-sdk-sql.json").read_text()
    )
    name = f"series_sdk_sql_{time.time_ns()}"
    config = copy.deepcopy(fixture["table"])
    # Ordinary tables may have nullable/optional keyword dimensions. Terms skips
    # null and missing values, so SQL must exclude both before top-series ranking.
    schema = config["schema"]["document_schemas"]["reading"]["schema"]
    schema["required"] = ["observed_at"]
    for field in ("device_id", "region"):
        schema["properties"][field]["type"] = ["string", "null"]
    finish_create_table(api, name, api._request("POST", f"/tables/{name}", config))
    readings = {
        **{
            f"null_device_{i}": {
                "device_id": None,
                "region": "west",
                "observed_at": "2026-11-06T12:01:00Z",
                "temperature": 999,
            }
            for i in range(4)
        },
        **{
            f"null_region_{i}": {
                "device_id": "null_region",
                "region": None,
                "observed_at": "2026-11-06T12:01:00Z",
                "temperature": 999,
            }
            for i in range(4)
        },
        "missing_device": {
            "region": "west",
            "observed_at": "2026-11-06T12:01:00Z",
            "temperature": 999,
        },
        "missing_region": {
            "device_id": "missing_region",
            "observed_at": "2026-11-06T12:01:00Z",
            "temperature": 999,
        },
        "hot1": {
            "device_id": "hot",
            "region": "west",
            "observed_at": "2026-11-06T12:01:00Z",
            "temperature": 1,
        },
        "hot2": {
            "device_id": "hot",
            "region": "west",
            "observed_at": "2026-11-06T12:01:00Z",
        },
        **{
            str(i): {
                "device_id": key,
                "region": "west",
                "observed_at": "2026-11-06T12:01:00Z",
                "temperature": i,
            }
            for i, key in enumerate(("AA\n", "AA ", "zz"))
        },
        "east": {
            "device_id": "excluded",
            "region": "east",
            "observed_at": "2026-11-06T12:01:00Z",
            "temperature": 999,
        },
        "outside": {
            "device_id": "excluded",
            "region": "west",
            "observed_at": "2026-11-06T13:00:00Z",
            "temperature": 999,
        },
    }
    readings.update(
        {
            "epoch_hot1": {
                "device_id": "hot",
                "region": "west",
                "observed_at": "1969-12-31T23:51:00Z",
                "temperature": 2,
            },
            "epoch_hot2": {
                "device_id": "hot",
                "region": "west",
                "observed_at": "1969-12-31T23:51:00Z",
                "temperature": 4,
            },
            "epoch_tie1": {
                "device_id": "AA\n",
                "region": "west",
                "observed_at": "1969-12-31T23:51:00Z",
                "temperature": 0,
            },
            "epoch_tie2": {
                "device_id": "AA ",
                "region": "west",
                "observed_at": "1969-12-31T23:51:00Z",
                "temperature": 5,
            },
        }
    )
    readings.update(
        {
            "missing1": {
                "device_id": "missing",
                "region": "zero",
                "observed_at": "2026-11-06T12:01:00Z",
            },
            "missing2": {
                "device_id": "missing",
                "region": "zero",
                "observed_at": "2026-11-06T12:01:00Z",
            },
            "measured_zero": {
                "device_id": "zero",
                "region": "zero",
                "observed_at": "2026-11-06T12:01:00Z",
                "temperature": 0,
            },
        }
    )
    api.batch_write(name, inserts=readings, sync_level="full_index")
    for case in fixture["cases"]:
        response = api.post("/query", dict(case["query"], table=name))
        result = response.get("responses", [response])[0]
        expected = []
        for group in result["aggregations"]["series"]["buckets"]:
            dimensions = (
                json.loads(group["key"])
                if len(case["options"]["groupBy"]) > 1
                else [group["key"]]
            )
            for point in group["sub_aggregations"]["points"]["buckets"]:
                if point["doc_count"] == 0:
                    continue
                metrics = point["sub_aggregations"]
                average = (
                    metrics["average"].get("avg")
                    if metrics["average"].get("count", 0) > 0
                    else None
                )
                total = (
                    metrics["total"].get("value")
                    if metrics["__antfly_series_count_total"].get("count", 0) > 0
                    else None
                )
                expected.append(
                    [
                        *dimensions,
                        point["key"],
                        average if average is not None else case["options"].get("fill"),
                        total if total is not None else case["options"].get("fill"),
                        metrics["minimum"].get("value")
                        if metrics["minimum"].get("value") is not None
                        else case["options"].get("fill"),
                        metrics["maximum"].get("value")
                        if metrics["maximum"].get("value") is not None
                        else case["options"].get("fill"),
                        metrics["readings"].get("value"),
                    ]
                )
        sql = dict(case["sql"])
        sql["statement"] = sql["statement"].replace('"fixture"', f'"{name}"')
        sql_response = api._request("POST", "/sql", sql)
        assert sql_response.status_code == 200, (case["name"], sql, sql_response.text)
        answer = sql_response.json()
        actual = answer["rows"]
        # SQL datetime output retains nine fraction digits; normalize only keys.
        key_position = len(case["options"]["groupBy"])
        for row in actual:
            row[key_position] = row[key_position].replace(".000000000Z", "Z")
            row[-1] = int(row[-1])  # SQL bigint counts use lossless decimal strings.
        assert actual == expected, (case["name"], actual, expected)
        assert len({tuple(row[:key_position]) for row in actual}) == 2

    # Existing terms queries keep their null grouping policy. A temporal index
    # that omits null dimensions must decline that policy instead of changing it.
    legacy = copy.deepcopy(fixture["cases"][0]["query"])
    legacy["aggregations"]["series"].pop("exclude_null")
    response = api.post("/query", dict(legacy, table=name))
    legacy_result = response.get("responses", [response])[0]
    assert (
        legacy_result["profile"].get("aggregation_execution") != "materialized_temporal"
    ), legacy_result
    assert legacy_result["aggregations"]["series"]["buckets"][0]["key"] == "null", (
        legacy_result
    )
    assert legacy_result["aggregations"]["series"]["buckets"][0]["doc_count"] == 4, (
        legacy_result
    )

    populated = copy.deepcopy(fixture["cases"][0]["query"])
    hist = populated["aggregations"]["series"]["sub_aggregations"]["points"]
    hist.pop("date_bounds")
    hist["min_doc_count"] = 1
    response = api.post("/query", dict(populated, table=name))
    populated_result = response.get("responses", [response])[0]
    assert (
        populated_result["profile"].get("aggregation_execution")
        == "materialized_temporal"
    ), populated_result


@pytest.mark.parametrize("shards", [1, 2])
def test_sdk_retention_preset_lifecycle(stateful_api, shards):
    from datetime import UTC, datetime, timedelta
    from helpers import wait_until

    api = stateful_api
    fixture = json.loads(
        (Path(__file__).parent / "fixtures/time-series-sdk.json").read_text()
    )
    config = copy.deepcopy(fixture["retentionTable"])
    config["num_shards"] = shards
    name = f"sdk_retention_{shards}_{time.time_ns()}"
    finish_create_table(api, name, api._request("POST", f"/tables/{name}", config))
    start_seconds = int(time.time()) // 300 * 300
    start = datetime.fromtimestamp(start_seconds, UTC)
    stamp = lambda value: value.isoformat().replace("+00:00", "Z")
    documents = {
        "a": {
            "device_id": "sensor",
            "region": "west",
            "observed_at": stamp(start + timedelta(seconds=1)),
            "temperature": 10,
        },
        "b": {
            "device_id": "sensor",
            "region": "west",
            "observed_at": stamp(start + timedelta(seconds=2)),
            "temperature": 20,
        },
        "expired": {
            "device_id": "sensor",
            "region": "west",
            "observed_at": stamp(start - timedelta(days=8)),
            "temperature": 999,
        },
    }
    api.batch_write(name, inserts=documents, sync_level="full_index")
    query = copy.deepcopy(fixture["query"])
    query["table"] = name
    query["filter_query"]["conjuncts"][0].update(
        start=stamp(start), end=stamp(start + timedelta(minutes=5))
    )
    histogram = query["aggregations"]["series"]["sub_aggregations"]["points"]
    histogram["date_bounds"] = {
        "start": stamp(start),
        "end": stamp(start + timedelta(minutes=5)),
    }

    def response(request):
        payload = api.post("/query", dict(request, table=name))
        return payload.get("responses", [payload])[0]

    # Expiration maintenance can advance a derived watermark immediately after
    # full_index. Wait for the stable indexed path rather than racing that work.
    last_index_attempt = {}

    def indexed_response():
        result = response(query)
        last_index_attempt.clear()
        last_index_attempt.update(result)
        return (
            result
            if result["profile"].get("aggregation_execution") == "materialized_temporal"
            else None
        )

    result = wait_until(indexed_response, timeout_s=30, interval_s=0.25)
    assert result, last_index_attempt.get("profile", {}).get("aggregation_execution")
    point = result["aggregations"]["series"]["buckets"][0]["sub_aggregations"][
        "points"
    ]["buckets"][0]
    assert point["doc_count"] == 2
    assert point["sub_aggregations"]["total"]["value"] == 30
    assert not response({"full_text_search": {"ids": ["expired"]}, "limit": 1})["hits"][
        "hits"
    ]

    # Correcting a reading to an expired instant removes its recent contribution.
    documents["a"]["observed_at"] = documents["expired"]["observed_at"]
    api.batch_write(name, inserts={"a": documents["a"]}, sync_level="full_index")
    result = wait_until(indexed_response, timeout_s=30, interval_s=0.25)
    assert result, last_index_attempt.get("profile", {}).get("aggregation_execution")
    point = result["aggregations"]["series"]["buckets"][0]["sub_aggregations"][
        "points"
    ]["buckets"][0]
    assert point["doc_count"] == 1
    assert point["sub_aggregations"]["total"]["value"] == 20

    # Shortening retention expires the remaining reading. Reuse must decline
    # when the source range is no longer visible through the query deadline.
    updated = copy.deepcopy(config["schema"])
    updated["ttl"]["duration"] = "1s"
    api.update_schema(name, updated)
    assert wait_until(
        lambda: (
            not response({"full_text_search": {"ids": ["b"]}, "limit": 1})["hits"][
                "hits"
            ]
        ),
        timeout_s=65,
        interval_s=0.5,
    )
    result = response(query)
    assert result["profile"].get("aggregation_execution") != "materialized_temporal", (
        result
    )
    assert result["aggregations"]["series"].get("buckets", []) == [], result
