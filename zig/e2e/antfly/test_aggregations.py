# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

"""Exact aggregation results while unrelated documents are inserted (issue #788)."""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests


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
