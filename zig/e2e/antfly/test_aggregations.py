# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

"""Exact aggregation results while unrelated documents are inserted (issue #788)."""

import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests
from conftest import finish_create_table
from helpers import wait_until


def _check_age_aggregation(payload, kind):
    responses = payload["responses"]
    assert len(responses) == 1, payload
    response = responses[0]
    assert response.get("status", 200) == 200, payload
    assert response["hits"]["hits"] == [], payload
    age = response["aggregations"]["age"]
    if kind == "terms":
        buckets = age["buckets"]
        assert len(buckets) == 73, payload
        # 11,000 = 73 * 150 + 50: the extra occurrence ends at age 67.
        assert {int(b["key"]): b["doc_count"] for b in buckets} == {
            age: 151 if age <= 67 else 150 for age in range(18, 91)
        }, payload
    else:
        for field, expected in {
            "count": 11000,
            "min": 18,
            "max": 90,
            "sum": 593425,
        }.items():
            assert age[field] == expected, payload
        assert age["avg"] == pytest.approx(593425 / 11000), payload


def test_aggregations_remain_exact_during_concurrent_inserts(stateful_api):
    name = f"aggregation_concurrency_{time.time_ns()}"
    stateful_api.create_table(name, num_shards=1)
    for start in range(0, 11000, 100):
        result = stateful_api.batch_write(
            name,
            inserts={
                f"User:{i}:": {"id": i, "node_type": "User", "age": 18 + i % 73}
                for i in range(start, start + 100)
            },
            sync_level="full_index" if start == 10900 else "write",
        )
        assert result["inserted"] == 100, result

    def phase(trial, writers):
        barrier = threading.Barrier(10 + writers)

        def worker(index):
            # The fixture serializes its shared session. Each worker needs its
            # own connection to actually overlap reads and writes.
            with requests.Session() as session:
                session.headers.update(stateful_api.s.headers)
                session.auth = stateful_api.s.auth
                session.cookies.update(stateful_api.s.cookies)
                completed = 0
                try:
                    # Exercise both aggregation kinds per reader in every
                    # phase. A five-second loop made correctness depend on
                    # shared-runner throughput and could execute only one kind.
                    # Release readers and writers together in each round.
                    for _ in range(2):
                        barrier.wait(timeout=45)
                        if index < 10:
                            kind = "terms" if (index + completed) % 2 == 0 else "stats"
                            aggregation = {"type": kind, "field": "age"}
                            if kind == "terms":
                                aggregation["size"] = 256
                            route = "query"
                            body = {"aggregations": {"age": aggregation}, "limit": 0}
                        else:
                            key = f"new-{trial}-{index}-{completed}"
                            route = "batch"
                            body = {
                                "inserts": {
                                    key: {
                                        "id": key,
                                        "node_type": "NewDocument",
                                        "counter": completed,
                                    }
                                },
                                "sync_level": "write",
                            }
                        response = session.post(
                            f"{stateful_api.url}/tables/{name}/{route}",
                            json=body,
                            # Match PublicApi's ordinary request timeout;
                            # this is a deadlock bound, not a latency assertion.
                            timeout=30,
                        )
                        expected_statuses = (200,) if index < 10 else (200, 201)
                        assert response.status_code in expected_statuses, (
                            trial,
                            index,
                            route,
                            response.status_code,
                            response.text,
                        )
                        payload = response.json()
                        if index < 10:
                            _check_age_aggregation(payload, kind)
                        else:
                            assert payload["inserted"] == 1, payload
                        completed += 1
                    return completed
                except BaseException:
                    barrier.abort()
                    raise

        with ThreadPoolExecutor(max_workers=10 + writers) as pool:
            futures = [pool.submit(worker, index) for index in range(10 + writers)]
            errors = [
                repr(error)
                for future in futures
                if (error := future.exception()) is not None
            ]
        assert not errors, f"trial={trial}: {errors}\n{stateful_api.debug_logs()}"
        results = [future.result() for future in futures]
        print(
            f"aggregation trial={trial}: readers={results[:10]} writers={results[10:]}"
        )

    phase("read-only", 0)
    for trial in range(3):
        phase(trial, 2)


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("num_shards", [1, 3])
def test_aggregation_pages_cover_all_matches(stateful_api, num_shards):
    name = f"aggregation_pages_{num_shards}_{time.time_ns()}"
    stateful_api.create_table(name, num_shards=num_shards)
    stateful_api.batch_write(
        name,
        inserts={f"doc-{age}": {"age": age} for age in (18, 20, 22, 24)},
        sync_level="full_index",
    )
    for limit in (0, 1):
        for kind in ("stats", "terms"):
            response = stateful_api.query_table(
                name,
                {
                    "limit": limit,
                    "aggregations": {"age": {"type": kind, "field": "age"}},
                },
            )["responses"][0]
            assert response["status"] == 200, response
            assert len(response["hits"]["hits"]) == limit, response
            age = response["aggregations"]["age"]
            if kind == "stats":
                assert age["count"] == 4 and age["sum"] == 84, response
            else:
                assert {int(b["key"]): b["doc_count"] for b in age["buckets"]} == {
                    value: 1 for value in (18, 20, 22, 24)
                }, response


@pytest.mark.e2e_resource("antfly_process")
def test_aggregation_full_result_budget(monkeypatch, request):
    if os.environ.get("ANTFLY_STATEFUL_URL"):
        pytest.skip("Changing the server budget requires a locally started process")
    monkeypatch.setenv("ANTFLY_AGGREGATION_FULL_RESULT_BUDGET", "1")
    api = request.getfixturevalue("stateful_api")
    name = f"aggregation_budget_{time.time_ns()}"
    api.create_table(name, num_shards=1)
    result = api.batch_write(name, inserts={"a": {"age": 18}}, sync_level="full_index")
    assert result["inserted"] == 1, result
    queries = [
        {"aggregations": {"age": {"type": kind, "field": "age"}}, "limit": 0}
        for kind in ("terms", "stats")
    ]
    for query in queries:
        response = api.query_table(name, query)["responses"][0]
        assert response["status"] == 200, response
        age = response["aggregations"]["age"]
        if query["aggregations"]["age"]["type"] == "terms":
            assert [(int(b["key"]), b["doc_count"]) for b in age["buckets"]] == [
                (18, 1)
            ]
        else:
            assert age["count"] == 1 and age["sum"] == 18, response

    result = api.batch_write(name, inserts={"b": {"age": 20}}, sync_level="full_index")
    assert result["inserted"] == 1, result
    for query in queries:
        with pytest.raises(requests.HTTPError) as failure:
            api.query_table(name, query)
        response = failure.value.response
        assert response.status_code == 422, response.text
        assert response.json()["error"] == "query_candidate_budget_exceeded", (
            response.text
        )


# Hybrid aggregation domain.
#
# A full-text query has a matching set; a vector query only has the ranked
# window the caller asked for. Aggregations over a hybrid query therefore count
# every text match plus each vector index's global top window. The corpus below
# makes that domain differ from every wrong answer: the whole index, the page,
# the text matches alone, and a per-shard union of vector windows.

_DOMAIN_DOCS = 3000
_DOMAIN_LIMIT = 10
_DOMAIN_NEAR = 24
_QUERY_A = [1.0, 0.0, 0.0]
_QUERY_B = [0.0, 1.0, 0.0]
_NEAR_A = [i * (_DOMAIN_DOCS // _DOMAIN_NEAR) + 7 for i in range(_DOMAIN_NEAR)]
_NEAR_B = [i * (_DOMAIN_DOCS // _DOMAIN_NEAR) + 131 for i in range(_DOMAIN_NEAR)]
_TEXT_IDS = set(range(1000, 1037)) | {_NEAR_A[0], _NEAR_A[1], _NEAR_B[3]}


def _unit(vector):
    norm = math.sqrt(sum(x * x for x in vector))
    return [x / norm for x in vector]


def _domain_key(i):
    return f"doc:{i:05d}"


def _domain_vectors(i):
    # Far vectors sit in the negative octant: similarity <= 0 to both queries.
    va = _unit([-0.3 - (i % 17) * 0.01, -0.3 - (i % 13) * 0.01, 1.0])
    vb = _unit([-0.3 - ((i + 5) % 17) * 0.01, -0.3 - ((i + 5) % 13) * 0.01, 1.0])
    if i in _NEAR_A:
        angle = 0.02 * (_NEAR_A.index(i) + 1)  # strictly graded ranks
        va = [math.cos(angle), 0.0, math.sin(angle)]
    if i in _NEAR_B:
        angle = 0.02 * (_NEAR_B.index(i) + 1)
        vb = [0.0, math.cos(angle), math.sin(angle)]
    return va, vb


def _domain_top(pick, query, n, predicate=lambda _i: True):
    scored = sorted(
        (-sum(x * y for x, y in zip(_domain_vectors(i)[pick], query)), _domain_key(i))
        for i in range(_DOMAIN_DOCS)
        if predicate(i)
    )
    return [key for _, key in scored[:n]]


def _create_domain_table(api, name, num_shards):
    schema = {
        "default_type": "doc",
        "document_schemas": {
            "doc": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "body": {"type": "string", "x-antfly-types": ["text"]},
                        "key": {"type": "string", "x-antfly-types": ["keyword"]},
                        "status": {"type": "string", "x-antfly-types": ["keyword"]},
                    },
                }
            }
        },
    }
    response = api._request(
        "POST", f"/tables/{name}", {"num_shards": num_shards, "schema": schema}
    )
    finish_create_table(api, name, response)
    for index in ("va", "vb"):
        api.post(
            f"/tables/{name}/indexes/{index}",
            {"name": index, "type": "embeddings", "external": True, "dimension": 3},
        )
    for start in range(0, _DOMAIN_DOCS, 500):
        inserts = {}
        for i in range(start, min(start + 500, _DOMAIN_DOCS)):
            va, vb = _domain_vectors(i)
            inserts[_domain_key(i)] = {
                "body": (
                    ("needle " if i in _TEXT_IDS else "")
                    + ("boundary " if i < 100 else "")
                    + "filler words here"
                ),
                "key": _domain_key(i),
                "status": "active" if i % 2 == 0 else "inactive",
                "_embeddings": {"va": va, "vb": vb},
            }
        result = api.batch_write(name, inserts=inserts, sync_level="full_index")
        assert result["inserted"] == len(inserts), result

    def indexes_ready():
        statuses = {
            index["config"]["name"]: index.get("status", {})
            for index in api.get(f"/tables/{name}/indexes")
        }
        return all(
            statuses.get(index, {}).get("total_indexed", 0) >= _DOMAIN_DOCS
            for index in ("va", "vb")
        )

    assert wait_until(indexes_ready, timeout_s=120.0, interval_s=0.5)


def _domain_query(api, name, shape, aggregations=None):
    payload = dict(shape, limit=_DOMAIN_LIMIT, fields=["key"])
    if aggregations is not None:
        payload["aggregations"] = aggregations
    response = api.query_table(name, payload)["responses"][0]
    assert response.get("status", 200) == 200, response
    return response


_DOMAIN_TERMS = {"keys": {"type": "terms", "field": "key", "size": 5000}}
_DOMAIN_MERGE = {"strategy": "rrf", "rank_constant": 60}


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.parametrize("num_shards", [1, 3])
def test_hybrid_aggregation_domain(stateful_api, num_shards):
    name = f"aggregation_domain_{num_shards}_{time.time_ns()}"
    _create_domain_table(stateful_api, name, num_shards)
    top_a = _domain_top(0, _QUERY_A, _DOMAIN_LIMIT)
    top_b = _domain_top(1, _QUERY_B, _DOMAIN_LIMIT)
    active_a = _domain_top(0, _QUERY_A, _DOMAIN_LIMIT, lambda i: i % 2 == 0)
    active_b = _domain_top(1, _QUERY_B, _DOMAIN_LIMIT, lambda i: i % 2 == 0)
    text_keys = {_domain_key(i) for i in _TEXT_IDS}

    # The fixture itself: single-index retrieval matches brute force.
    for index, query, want in (("va", _QUERY_A, top_a), ("vb", _QUERY_B, top_b)):
        single = _domain_query(
            stateful_api, name, {"embeddings": {index: query}, "indexes": [index]}
        )
        assert [hit["_id"] for hit in single["hits"]["hits"]] == want, single

    shapes = {
        "hybrid": (
            {
                "full_text_search": {"match": "needle", "field": "body"},
                "embeddings": {"va": _QUERY_A, "vb": _QUERY_B},
                "indexes": ["va", "vb"],
                "merge_config": _DOMAIN_MERGE,
            },
            text_keys | set(top_a) | set(top_b),
        ),
        "semantic": (
            {"embeddings": {"va": _QUERY_A}, "indexes": ["va"]},
            set(top_a),
        ),
        "semantic_multi": (
            {
                "embeddings": {"va": _QUERY_A, "vb": _QUERY_B},
                "indexes": ["va", "vb"],
                "merge_config": _DOMAIN_MERGE,
            },
            set(top_a) | set(top_b),
        ),
        "semantic_multi_default": (
            {"embeddings": {"va": _QUERY_A, "vb": _QUERY_B}, "indexes": ["va", "vb"]},
            set(top_a) | set(top_b),
        ),
        "semantic_multi_filtered": (
            {
                "embeddings": {"va": _QUERY_A, "vb": _QUERY_B},
                "indexes": ["va", "vb"],
                "merge_config": _DOMAIN_MERGE,
                "filter_query": {"term": {"path": "/status", "value": "active"}},
            },
            set(active_a) | set(active_b),
        ),
        "keyword": (
            {"full_text_search": {"match": "needle", "field": "body"}},
            text_keys,
        ),
        "explicit_match_all": (
            {
                "full_text_search": {"match_all": {}},
                "embeddings": {"va": _QUERY_A, "vb": _QUERY_B},
                "indexes": ["va", "vb"],
                "merge_config": _DOMAIN_MERGE,
            },
            {_domain_key(i) for i in range(_DOMAIN_DOCS)},
        ),
    }
    for label, (shape, expected) in shapes.items():
        page = _domain_query(stateful_api, name, shape)
        counted = _domain_query(stateful_api, name, shape, _DOMAIN_TERMS)
        assert {hit["_id"] for hit in page["hits"]["hits"]} <= expected, label
        keys = {bucket["key"] for bucket in counted["aggregations"]["keys"]["buckets"]}
        assert keys == expected, (label, sorted(keys ^ expected)[:20])
        assert all(
            bucket["doc_count"] == 1
            for bucket in counted["aggregations"]["keys"]["buckets"]
        ), label
        # Aggregations never change the ranked page.
        assert [hit["_id"] for hit in counted["hits"]["hits"]] == [
            hit["_id"] for hit in page["hits"]["hits"]
        ], label


@pytest.mark.e2e_resource("antfly_process")
def test_vector_aggregation_budget_counts_windows_not_index(monkeypatch, request):
    if os.environ.get("ANTFLY_STATEFUL_URL"):
        pytest.skip("Changing the server budget requires a locally started process")
    monkeypatch.setenv("ANTFLY_AGGREGATION_FULL_RESULT_BUDGET", "100")
    api = request.getfixturevalue("stateful_api")
    name = f"aggregation_vector_budget_{time.time_ns()}"
    _create_domain_table(api, name, 1)

    # Vector windows are the matching set, so a budget far below the index size
    # still admits them.
    semantic = _domain_query(
        api,
        name,
        {
            "embeddings": {"va": _QUERY_A, "vb": _QUERY_B},
            "indexes": ["va", "vb"],
            "merge_config": _DOMAIN_MERGE,
        },
        _DOMAIN_TERMS,
    )
    keys = {bucket["key"] for bucket in semantic["aggregations"]["keys"]["buckets"]}
    assert keys == set(_domain_top(0, _QUERY_A, _DOMAIN_LIMIT)) | set(
        _domain_top(1, _QUERY_B, _DOMAIN_LIMIT)
    )

    # The index is much larger than the budget, but only 40 documents match.
    # Block-Max scoring must prove this underfilled collection window exact.
    text_keys = {_domain_key(i) for i in _TEXT_IDS}
    keyword = _domain_query(
        api,
        name,
        {"full_text_search": {"match": "needle", "field": "body"}},
        _DOMAIN_TERMS,
    )
    assert {
        bucket["key"] for bucket in keyword["aggregations"]["keys"]["buckets"]
    } == text_keys
    # A complete Block-Max window that exactly fills the budget is still exact.
    boundary = _domain_query(
        api,
        name,
        {"full_text_search": {"match": "boundary", "field": "body"}},
        _DOMAIN_TERMS,
    )
    assert {
        bucket["key"] for bucket in boundary["aggregations"]["keys"]["buckets"]
    } == {_domain_key(i) for i in range(100)}
    hybrid = _domain_query(
        api,
        name,
        {
            "full_text_search": {"match": "needle", "field": "body"},
            "embeddings": {"va": _QUERY_A, "vb": _QUERY_B},
            "indexes": ["va", "vb"],
            "merge_config": _DOMAIN_MERGE,
        },
        _DOMAIN_TERMS,
    )
    assert {bucket["key"] for bucket in hybrid["aggregations"]["keys"]["buckets"]} == (
        text_keys | keys
    )

    # Text matches still count against the budget.
    with pytest.raises(requests.HTTPError) as failure:
        api.query_table(
            name,
            {
                "full_text_search": {"match": "filler", "field": "body"},
                "embeddings": {"va": _QUERY_A, "vb": _QUERY_B},
                "indexes": ["va", "vb"],
                "merge_config": _DOMAIN_MERGE,
                "limit": _DOMAIN_LIMIT,
                "aggregations": _DOMAIN_TERMS,
            },
        )
    assert failure.value.response.status_code == 422, failure.value.response.text
    assert (
        failure.value.response.json()["error"] == "query_candidate_budget_exceeded"
    ), failure.value.response.text
