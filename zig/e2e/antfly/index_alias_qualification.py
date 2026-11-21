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

"""Independent result oracle shared by alias deployment qualifications."""

from __future__ import annotations

import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import requests

INDEXES = {
    name: {"type": "embeddings", "external": True, "dimension": 2}
    for name in ("space_a", "space_b")
}
EXPECTED = {
    "space_a": {"0:a", "8:a", "z:a"},
    "space_b": {"0:b", "8:b", "z:b"},
}
SEED = {
    key: {
        "label": key,
        "_embeddings": {
            "space_a": [1.0, 0.0] if key in EXPECTED["space_a"] else [-1.0, 0.0],
            "space_b": [1.0, 0.0] if key in EXPECTED["space_b"] else [-1.0, 0.0],
        },
    }
    for key in sorted(set.union(*EXPECTED.values()))
}


def checked(response):
    assert response.ok, f"{response.status_code}: {response.text}"
    return response.json()


def alias_url(base, table):
    return f"{base}/tables/{table}/index-aliases/live"


def swap(session, base, table, target, identity, previous=None, admission=None):
    body = {"index": target, "idempotency_key": identity, "require": "queryable"}
    if previous is not None:
        body["expected_index"] = previous
    # Retry only explicit pre-admission failures with the same operation ID.
    # Transport errors and unclassified mutation outcomes are never replayed.
    deadline = time.monotonic() + 60
    while True:
        response = session.put(alias_url(base, table), json=body, timeout=30)
        if response.status_code != 409 or response.json() not in (
            {"error": "IndexAliasTargetNotReady"},
            {"error": "TableGenerationChanged"},
        ):
            break
        if admission is not None:
            admission[response.json()["error"]] += 1
        assert time.monotonic() < deadline, response.text
        time.sleep(0.05)
    result = checked(response)
    assert result["index"] == target, result
    return result


def query(session, base, table, availability=None):
    deadline = time.monotonic() + 60
    while True:
        response = session.post(
            f"{base}/tables/{table}/query",
            json={
                "embeddings": {"live": [1.0, 0.0]},
                "limit": 3,
                "fields": ["label"],
                "search_effort": 1,
            },
            timeout=min(30, max(0.1, deadline - time.monotonic())),
        )
        if response.status_code == 503:
            failure = response.json()
            retry = failure.get("retryable") is True and isinstance(
                failure.get("code"), str
            )
            reason = failure.get("code")
        else:
            retry = response.status_code == 409 and response.json() == {
                "error": "catalog changed during query binding"
            }
            reason = "catalog_changed"
        if not retry:
            break
        if availability is not None:
            availability[reason] += 1
        assert time.monotonic() < deadline, response.text
        time.sleep(0.05)
    value = checked(response)["responses"][0]
    return validate_result(value)


def validate_result(value):
    """Require a positive witness from every shard for the reported target."""
    resolved = value["resolved_indexes"]
    assert "live" not in resolved, value
    concrete = [name for name in resolved if name in EXPECTED]
    assert len(concrete) == 1, value
    target = concrete[0]
    ids = [hit["_id"] for hit in value["hits"]["hits"]]
    assert len(ids) == 3 and set(ids) == EXPECTED[target], value
    return target


def wait_for_target(session, base, table, target, timeout=60):
    deadline = time.monotonic() + timeout
    while True:
        response = session.get(f"{base}/tables/{table}/indexes/{target}", timeout=10)
        if response.ok:
            status = response.json().get("status", {})
            if status.get("readiness", {}).get("queryable") is True:
                return
        else:
            assert response.status_code in (409, 503), response.text
        assert time.monotonic() < deadline, response.text
        time.sleep(0.1)


def exercise_swaps(session, base, table, *, rounds=8, failover=None):
    """Keep independent query and write loops active across acknowledged A/B/A."""
    stop = Event()
    observations = Counter()
    observed = {name: Event() for name in INDEXES}
    availability = Counter()
    admission = Counter()
    write_admission = Counter()
    writes = 0

    def read():
        # Session connection pools are not shared across worker threads.
        try:
            with requests.Session() as reader:
                reader.auth = session.auth
                while not stop.is_set():
                    target = query(reader, base, table, availability)
                    observations[target] += 1
                    observed[target].set()
        finally:
            # Wake the coordinator on failure too; Future.result() below must
            # surface the reader's original error instead of a coverage timeout.
            for witness in observed.values():
                witness.set()

    def write():
        nonlocal writes
        with requests.Session() as writer:
            writer.auth = session.auth
            while not stop.is_set():
                key = f"8:ongoing:{writes}"
                mutation = {
                    "inserts": {
                        key: {
                            "label": key,
                            "_embeddings": {name: [0.0, 1.0] for name in INDEXES},
                        }
                    },
                    "sync_level": "full_index",
                }
                deadline = time.monotonic() + 30
                while True:
                    response = writer.post(
                        f"{base}/tables/{table}/batch",
                        json=mutation,
                        timeout=min(30, max(0.1, deadline - time.monotonic())),
                    )
                    # Only the public batch contract's explicit aborts permit
                    # replay. Unknown outcomes and transport failures fail.
                    try:
                        failure = response.json()
                    except ValueError:
                        failure = {}
                    reason = failure.get("code") if isinstance(failure, dict) else None
                    retry = (
                        response.status_code in (409, 503)
                        and reason
                        in ("integrity_topology_busy", "transaction_precommit_aborted")
                        and failure.get("retryable") is True
                    ) or (response.status_code, response.text.strip()) in (
                        (503, "write unavailable"),
                        (409, "batch transaction conflicted"),
                    )
                    if not retry:
                        checked(response)
                        break
                    write_admission[reason or response.text.strip()] += 1
                    if stop.is_set():
                        return
                    assert time.monotonic() < deadline, response.text
                    time.sleep(0.05)
                writes += 1

    with ThreadPoolExecutor(max_workers=2) as workers:
        readers = workers.submit(read)
        writer = workers.submit(write)
        try:
            previous = "space_a"
            for turn in range(rounds):
                for target in ("space_b", "space_a"):
                    observed[target].clear()
                    identity = f"swap-{turn}-{target}"
                    result = swap(
                        session, base, table, target, identity, previous, admission
                    )
                    assert result["previous_index"] == previous, result
                    # A query started after the acknowledged swap must use it.
                    assert query(session, base, table) == target
                    # Hold this acknowledged target until the independent
                    # reader has witnessed it. Otherwise quick A/B/A swaps can
                    # all fall between its reads, making coverage scheduler-
                    # dependent despite every query being correct. Ingestion
                    # continues during this event-driven rendezvous.
                    assert observed[target].wait(timeout=60), observations
                    if readers.done():
                        readers.result()
                    previous = target
                if failover is not None and turn == rounds // 2:
                    failover()
                if readers.done():
                    readers.result()
                if writer.done():
                    writer.result()
        finally:
            stop.set()
            readers.result(timeout=70)
            writer.result(timeout=45)
    assert writes > 0, "no ingestion overlapped the swaps"
    assert all(observations[name] > 0 for name in INDEXES), observations
    return {
        "queries": dict(observations),
        "retryable_reads": dict(availability),
        "nonadmitted_swaps": dict(admission),
        "writes": writes,
        "aborted_writes": dict(write_admission),
        "swaps": rounds * 2,
    }
