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

"""Public API transform tests."""

from __future__ import annotations

import threading
import time
import json
import requests

from helpers import wait_until
from helpers import json_doc, upsert


def _transform(key: str, *operations: dict[str, object], upsert: bool = False) -> dict[str, object]:
    payload: dict[str, object] = {
        "key": key,
        "operations": list(operations),
    }
    if upsert:
        payload["upsert"] = True
    return payload


def _op(op: str, path: str, value: object | None = None) -> dict[str, object]:
    payload: dict[str, object] = {"op": op, "path": path}
    if value is not None:
        payload["value"] = value
    return payload


def _docs_by_id(query: dict) -> dict[str, dict]:
    docs: dict[str, dict] = {}
    for item in query["documents"]:
        docs[item["doc_id"]] = json.loads(item["body"])
    return docs


def test_transform_max_keeps_latest_value(stateful_api):
    table_name = f"transforms_max_{time.time_ns()}"
    stateful_api.create_table(table_name, num_shards=1)

    initial = stateful_api.batch_write(
        table_name,
        inserts={"item-1": {"name": "test-item", "version": 5, "data": "initial"}},
        sync_level="write",
    )
    assert initial["inserted"] == 1

    lowered = stateful_api.batch_write(
        table_name,
        transforms=[
            _transform(
                "item-1",
                _op("$max", "version", 3),
                _op("$set", "data", "updated with v3"),
            )
        ],
        sync_level="write",
    )
    assert lowered["transformed"] == 1

    doc = stateful_api.lookup_key(table_name, "item-1")
    assert doc["version"] == 5
    assert doc["data"] == "updated with v3"

    raised = stateful_api.batch_write(
        table_name,
        transforms=[
            _transform(
                "item-1",
                _op("$max", "version", 10),
                _op("$set", "data", "updated with v10"),
            )
        ],
        sync_level="write",
    )
    assert raised["transformed"] == 1

    doc = stateful_api.lookup_key(table_name, "item-1")
    assert doc["version"] == 10
    assert doc["data"] == "updated with v10"


def test_transform_upsert_with_max(stateful_api):
    table_name = f"transforms_upsert_{time.time_ns()}"
    stateful_api.create_table(table_name, num_shards=1)

    created = stateful_api.batch_write(
        table_name,
        transforms=[
            _transform(
                "upsert-item",
                _op("$max", "version", 5),
                _op("$set", "name", "upserted-item"),
                upsert=True,
            )
        ],
        sync_level="write",
    )
    assert created["transformed"] == 1

    doc = stateful_api.lookup_key(table_name, "upsert-item")
    assert doc["version"] == 5
    assert doc["name"] == "upserted-item"

    stateful_api.batch_write(
        table_name,
        transforms=[
            _transform(
                "upsert-item",
                _op("$max", "version", 3),
                _op("$set", "status", "updated"),
                upsert=True,
            )
        ],
        sync_level="write",
    )

    doc = stateful_api.lookup_key(table_name, "upsert-item")
    assert doc["version"] == 5
    assert doc["status"] == "updated"

    stateful_api.batch_write(
        table_name,
        transforms=[_transform("upsert-item", _op("$max", "version", 10), upsert=True)],
        sync_level="write",
    )

    doc = stateful_api.lookup_key(table_name, "upsert-item")
    assert doc["version"] == 10


def test_transform_concurrent_max_updates(stateful_api):
    table_name = f"transforms_concurrent_max_{time.time_ns()}"
    stateful_api.create_table(table_name, num_shards=1)
    stateful_api.batch_write(
        table_name,
        inserts={"concurrent-item": {"name": "concurrent-test", "version": 0}},
        sync_level="write",
    )
    seeded = wait_until(
        lambda: _lookup_doc(stateful_api, table_name, "concurrent-item"),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert seeded is not None
    assert seeded["version"] == 0

    errors: list[Exception] = []

    def worker(version: int) -> None:
        try:
            stateful_api.batch_write(
                table_name,
                transforms=[_transform("concurrent-item", _op("$max", "version", version))],
                sync_level="write",
            )
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(version,)) for version in range(1, 21)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    doc = wait_until(
        lambda: _lookup_doc(stateful_api, table_name, "concurrent-item"),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert doc is not None
    assert doc["version"] == 20


def test_transform_inc_atomic_counter(stateful_api):
    table_name = f"transforms_inc_{time.time_ns()}"
    stateful_api.create_table(table_name, num_shards=1)
    stateful_api.batch_write(
        table_name,
        inserts={"counter-item": {"name": "atomic-counter", "counter": 0}},
        sync_level="write",
    )
    seeded = wait_until(
        lambda: _lookup_doc(stateful_api, table_name, "counter-item"),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert seeded is not None
    assert seeded["counter"] == 0

    errors: list[Exception] = []

    def worker() -> None:
        try:
            stateful_api.batch_write(
                table_name,
                transforms=[_transform("counter-item", _op("$inc", "counter", 1))],
                sync_level="write",
            )
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(25)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    doc = wait_until(
        lambda: _lookup_doc(stateful_api, table_name, "counter-item"),
        timeout_s=10.0,
        interval_s=0.1,
    )
    assert doc is not None
    assert doc["counter"] == 25


def test_transform_multiple_operators(stateful_api):
    table_name = f"transforms_multi_{time.time_ns()}"
    stateful_api.create_table(table_name, num_shards=1)
    stateful_api.batch_write(
        table_name,
        inserts={
            "multi-op-item": {
                "name": "multi-operator-test",
                "version": 1,
                "views": 0,
                "tags": ["initial"],
                "metadata": {"created": True},
            }
        },
        sync_level="write",
    )

    result = stateful_api.batch_write(
        table_name,
        transforms=[
            _transform(
                "multi-op-item",
                _op("$max", "version", 5),
                _op("$inc", "views", 1),
                _op("$addToSet", "tags", "updated"),
                _op("$set", "metadata.lastUpdated", "2025-01-26"),
            )
        ],
        sync_level="write",
    )
    assert result["transformed"] == 1

    doc = stateful_api.lookup_key(table_name, "multi-op-item")
    assert doc["version"] == 5
    assert doc["views"] == 1
    assert set(doc["tags"]) == {"initial", "updated"}
    assert doc["metadata"]["created"] is True
    assert doc["metadata"]["lastUpdated"] == "2025-01-26"


def test_serverless_table_transforms_follow_latest_then_published(serverless_api):
    def published_after_transform() -> dict | None:
        try:
            published = serverless_api.query_published(table_name)
        except requests.HTTPError:
            return None
        docs = _docs_by_id(published)
        doc = docs.get("doc-a")
        if doc is None:
            return None
        if doc.get("status") != "updated":
            return None
        if doc.get("version") != 3:
            return None
        return published

    table_name = f"serverless_transforms_{time.time_ns()}"
    serverless_api.ensure_table(table_name, created_at_ns=100)
    serverless_api.ingest_table(
        table_name,
        timestamp_ns=123,
        mutations=[
            upsert(
                "doc-a",
                json_doc(title="alpha", version=1),
            ),
        ],
    )

    initial_build = serverless_api.build_table(table_name)
    assert initial_build["published"] is True

    transformed = serverless_api.batch_table(
        table_name,
        transforms=[
            _transform(
                "doc-a",
                _op("$set", "status", "updated"),
                _op("$max", "version", 3),
            )
        ],
    )
    assert transformed["transformed"] == 1

    latest = serverless_api.query_latest(table_name)
    assert latest["table_name"] == table_name
    assert latest["view"] == "latest"
    assert latest["overlay_mutation_count"] == 1
    latest_docs = _docs_by_id(latest)
    assert latest_docs["doc-a"]["status"] == "updated"
    assert latest_docs["doc-a"]["version"] == 3

    published_before = serverless_api.query_published(table_name)
    assert published_before["table_name"] == table_name
    assert published_before["view"] == "published"
    published_before_docs = _docs_by_id(published_before)
    assert "status" not in published_before_docs["doc-a"]
    assert published_before_docs["doc-a"]["version"] == 1

    try:
        serverless_api.build_table(table_name)
    except requests.HTTPError:
        pass

    published_after = wait_until(published_after_transform, timeout_s=10.0, interval_s=0.1)
    assert published_after is not None
    published_after_docs = _docs_by_id(published_after)
    assert published_after_docs["doc-a"]["status"] == "updated"
    assert published_after_docs["doc-a"]["version"] == 3


def _lookup_doc(stateful_api, table_name: str, key: str) -> dict | None:
    try:
        return stateful_api.lookup_key(table_name, key)
    except Exception:
        return None
