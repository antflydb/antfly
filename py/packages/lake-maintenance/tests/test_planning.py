# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from antfly_lake_maintenance.planning import (
    DurableReachability,
    DurableSet,
    PlanningPending,
)
from antfly_lake_maintenance.store import Store


def test_radix_marks_survive_concurrent_splits_and_restart(tmp_path):
    store = Store()
    prefix = tmp_path.as_uri() + "/marks/"
    marks = DurableSet(store, prefix, leaf_size=4)
    keys = [f"s3://warehouse/data/{index}.parquet" for index in range(200)]
    with ThreadPoolExecutor(max_workers=4) as workers:
        list(workers.map(marks.add, keys))
    restarted = DurableSet(Store(), prefix, leaf_size=4)
    assert all(key in restarted for key in keys)
    assert "s3://warehouse/unknown" not in restarted
    # Pages grow with key count; individual leaves stay bounded.
    for path in tmp_path.rglob("*.json"):
        page = json.loads(path.read_bytes())
        assert len(page.get("keys", [])) <= 8


def test_reachability_budget_resumes_without_losing_partial_marks(tmp_path):
    store = Store()
    provider = SimpleNamespace(store=store)
    args = dict(prefix=tmp_path.as_uri() + "/plan/", max_files=2, max_bytes=1024)
    first = DurableReachability(provider, "s3://warehouse/", **args)
    first.add("s3://warehouse/1")
    first.add("s3://warehouse/2")
    with pytest.raises(PlanningPending):
        first.add("s3://warehouse/3")
    reopened = DurableReachability(provider, "s3://warehouse/", **args)
    for uri in ["s3://warehouse/1", "s3://warehouse/2", "s3://warehouse/3"]:
        reopened.add(uri)
    assert reopened.new_files == 1
    assert all(f"s3://warehouse/{index}" in reopened.files for index in range(1, 4))


def test_filesystem_inventory_pages_preserve_exact_versions(tmp_path):
    store = Store()
    prefix = tmp_path.as_uri() + "/data/"
    for index in range(23):
        store.put(prefix + f"{index:02}.parquet", str(index).encode(), absent=True)
    found, cursor = [], None
    while True:
        page, cursor = store.inventory_page(prefix, cursor, limit=3)
        found.extend(page)
        assert len(page) <= 3
        if cursor is None:
            break
    assert len(found) == len({obj.uri for obj in found}) == 23
    old = found[0]
    assert store.get(old.uri)[1] == old.version


def test_empty_native_snapshot_pin_protects_exact_metadata():
    from antfly_lake_maintenance.provider import Reachability
    from antfly_lake_maintenance.store import digest

    uri, raw = "s3://warehouse/table/metadata/empty.json", b'{"table-uuid":"original"}'
    provider = SimpleNamespace(
        store=SimpleNamespace(get=lambda location: (raw, "etag"))
    )
    graph = Reachability(provider, "s3://warehouse/")
    graph.read_metadata = lambda location: {
        "table-uuid": "original",
        "snapshots": [],
        "current-snapshot-id": -1,
    }
    graph.history(uri)
    identity = "empty:original:" + digest(raw)
    assert identity in graph.snapshots
    graph.mark_snapshot(graph.snapshots[identity][0][1])
    assert uri in graph.files


def test_global_marker_does_not_accumulate_catalog_snapshot_index(tmp_path):
    provider = SimpleNamespace(store=Store())
    graph = DurableReachability(
        provider,
        "s3://warehouse/",
        prefix=tmp_path.as_uri() + "/marks/",
        max_files=100,
        max_bytes=1024,
    )
    graph.mark_snapshot = lambda snapshot: graph.add(snapshot["manifest-list"])
    for index in range(50):
        graph.mark_values(
            {
                "snapshots": [
                    {"snapshot-id": index, "manifest-list": f"s3://warehouse/{index}"}
                ]
            },
            root=f"s3://warehouse/metadata/{index}",
        )
    assert not graph.snapshots
    assert "s3://warehouse/49" in graph.files
