# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
"""Production catalog failover, large control views, and telemetry isolation."""

import json
import os
import time
from pathlib import Path

import pytest
import requests
from catalog_baseline import JsonNumber, plan_baseline, post_baseline
from conftest import DEFAULT_ANTFLY_BIN, internal_service_headers
from test_scaling import MultiNodeScalingCluster


@pytest.fixture
def catalog_cluster():
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.exists():
        pytest.skip(f"antfly binary not built: {binary}")
    cluster = MultiNodeScalingCluster(str(binary), initial_data_node_count=3)
    try:
        yield cluster
    finally:
        cluster.stop()


def post_report(cluster, path, body):
    failures = []
    for index, url in enumerate(cluster.metadata_urls):
        if cluster.metadata_procs[index].poll() is not None:
            continue
        response = requests.post(
            url + path, json=body, headers=internal_service_headers(), timeout=15
        )
        if response.ok:
            return index, response
        failures.append((index, response.status_code, response.text))
    raise AssertionError(failures)


def register_reporter(cluster, groups):
    post_report(cluster, "/internal/v1/nodes", {"node_id": 1000, "role": "data"})
    post_report(
        cluster,
        "/internal/v1/nodes",
        {
            "store_id": 1000,
            "node_id": 1000,
            "reporter_incarnation": 77,
            "live": False,
            "dense_native_storage_protocol_version": 1,
        },
    )
    deadline = time.monotonic() + 15
    while not any(s["store_id"] == 1000 for s in cluster.metadata_snapshot()["stores"]):
        assert time.monotonic() < deadline, "registration did not apply"
        time.sleep(0.02)
    report = {
        "store_id": 1000,
        "reporter_incarnation": 77,
        "status_generation": 1,
        "live": False,
        "dense_native_storage_protocol_version": 1,
        "embedding_activity_protocol_version": 2,
        "embedding_activity_sequence": 1,
        "group_statuses": [
            {"group_id": 100000 + i, "raft_term": 1} for i in range(groups)
        ],
        "runtime_statuses": [
            {
                "group_id": 100000 + i,
                "store_id": 1000,
                "node_id": 1000,
                "indexes": [{"name": "dense", "kind": "embeddings"}],
            }
            for i in range(groups)
        ],
    }
    leader, response = post_report(
        cluster,
        "/internal/v1/nodes/1000/status/update",
        {"sequence": 1, "report": report},
    )
    return report, response.json(), leader


def post_snapshot_admitted(url, body):
    """Retry only the explicit pre-capture capacity rejection, never page errors."""
    deadline = time.monotonic() + 15
    while True:
        response = requests.post(
            url, json=body, headers=internal_service_headers(), timeout=20
        )
        if (
            response.status_code != 503
            or response.text != "snapshot transfer capacity exhausted"
            or time.monotonic() >= deadline
        ):
            return response
        time.sleep(0.05)


def read_pages(cluster, index, *, control, number_lexemes=False, retry_admission=False):
    url = cluster.metadata_urls[index] + "/internal/v1/snapshots/read"
    body = {"control": control}
    chunks = []
    sizes = []
    token = None
    try:
        while True:
            response = (
                post_snapshot_admitted(url, body)
                if retry_admission and token is None
                else requests.post(
                    url, json=body, headers=internal_service_headers(), timeout=20
                )
            )
            if not response.ok:
                raise requests.HTTPError(
                    f"snapshot {response.status_code}: {response.text}",
                    response=response,
                )
            observed = int(response.headers["X-Antfly-Snapshot-Token"])
            assert token is None or observed == token
            token = observed
            sizes.append(len(response.content))
            assert 0 < sizes[-1] <= 512 * 1024
            chunks.append(response.content)
            offset = sum(sizes)
            if offset == int(response.headers["X-Antfly-Snapshot-Bytes"]):
                return json.loads(
                    b"".join(chunks),
                    **({"parse_float": JsonNumber} if number_lexemes else {}),
                ), sizes
            body = {"token": token, "offset": offset}
    finally:
        if token is not None:
            response = requests.post(
                url,
                json={"token": token, "release": True},
                headers=internal_service_headers(),
                timeout=5,
            )
            assert response.status_code == 204


def test_catalog_protocol_activation_survives_leader_failure(catalog_cluster):
    c = catalog_cluster
    report, cursor, leader = register_reporter(c, 100)
    api = c.data_api_urls[0]
    response = requests.post(api + "/databases/failover", json={}, timeout=15)
    response.raise_for_status()
    c.metadata_procs[leader].terminate()
    c.metadata_procs[leader].wait(timeout=10)
    deadline = time.monotonic() + 20
    while True:
        statuses = c.metadata_statuses()
        if any(
            s.get("status", {}).get("metadata_raft_role") == "leader" for s in statuses
        ):
            break
        assert time.monotonic() < deadline, statuses
        time.sleep(0.1)
    patch = {
        **report,
        "group_statuses": [{"group_id": 100000, "raft_term": 2}],
        "runtime_statuses": [],
    }
    _, response = post_report(
        c,
        "/internal/v1/nodes/1000/status/update",
        {"sequence": 2, "base": cursor, "report": patch},
    )
    assert response.json()["sequence"] == 2
    response = requests.post(
        api + "/databases/failover/namespaces/after_election", json={}, timeout=15
    )
    response.raise_for_status()
    assert all(p.poll() is None for p in c.data_procs)


def test_large_inventory_uses_bounded_control_and_diagnostic_transfers(catalog_cluster):
    c = catalog_cluster
    _, _, leader = register_reporter(c, 10000)
    control, control_sizes = read_pages(c, leader, retry_admission=True, control=True)
    assert sum(control_sizes) < 1024 * 1024
    assert all(not s["runtime_statuses"] for s in control["stores"])
    diagnostic, diagnostic_sizes = read_pages(
        c, leader, retry_admission=True, control=False
    )
    assert sum(diagnostic_sizes) > 4 * 1024 * 1024
    synthetic = next(s for s in diagnostic["stores"] if s["store_id"] == 1000)
    assert len(synthetic["runtime_statuses"]) == 10000
    # A retained large diagnostic view exhausts that lane's next worst-case
    # reservation. Control captures keep their separate admission budget.
    snapshot_url = c.metadata_urls[leader] + "/internal/v1/snapshots/read"
    retained = post_snapshot_admitted(snapshot_url, {"control": False})
    retained.raise_for_status()
    try:
        rejected = requests.post(
            snapshot_url,
            json={"control": False},
            headers=internal_service_headers(),
            timeout=5,
        )
        assert rejected.status_code == 503
        _, sizes = read_pages(c, leader, retry_admission=True, control=True)
        assert sum(sizes) < 1024 * 1024
    finally:
        released = requests.post(
            snapshot_url,
            json={
                "token": int(retained.headers["X-Antfly-Snapshot-Token"]),
                "release": True,
            },
            headers=internal_service_headers(),
            timeout=5,
        )
        assert released.status_code == 204
    table_path = c.data_api_urls[0] + "/tables/large_inventory_docs"
    response = requests.post(table_path, json={}, timeout=30)
    response.raise_for_status()
    response = requests.delete(table_path, timeout=30)
    response.raise_for_status()
    # The original regression killed the real data nodes on their next control
    # rounds. Exercise multiple rounds and actual writes, not just process start.
    for i in range(12):
        response = requests.post(
            c.data_api_urls[0] + f"/databases/large_{i}", json={}, timeout=15
        )
        response.raise_for_status()
        assert all(p.poll() is None for p in c.data_procs), c.debug_logs()
        time.sleep(1)


def test_telemetry_batches_do_not_issue_raft_read_barriers(catalog_cluster):
    c = catalog_cluster
    report, cursor, leader = register_reporter(c, 100)
    url = c.metadata_urls[leader]
    before = requests.get(url + "/metadata/v1/status", timeout=5).json()["metrics"][
        "read_index_requests"
    ]
    patch = {**report, "group_statuses": [], "runtime_statuses": []}
    for sequence in range(2, 12):
        patch["embedding_activity_sequence"] = sequence
        response = requests.post(
            url + "/internal/v1/nodes/1000/status/update",
            headers=internal_service_headers(),
            json={
                "telemetry_only": True,
                "sequence": 2,
                "base": cursor,
                "report": patch,
                "activity": [
                    {
                        "group_id": 100000,
                        "index_name": "dense",
                        "index_kind": "embeddings",
                        "activity": {
                            "epoch": 1,
                            "sample_sequence": sequence,
                            "embeddings_computed": sequence,
                        },
                    }
                ],
            },
            timeout=5,
        )
        response.raise_for_status()
        assert response.json() == cursor
    after = requests.get(url + "/metadata/v1/status", timeout=5).json()["metrics"][
        "read_index_requests"
    ]
    # Independent real data nodes may issue a control read during this window;
    # ten telemetry requests must not create ten new read-index requests.
    assert after - before < 10


def test_report_baseline_resumes_after_leader_failure_and_keeps_partial_inventory_invisible(
    catalog_cluster,
):
    c = catalog_cluster
    report, original_cursor, leader = register_reporter(c, 130)
    snapshot, _ = read_pages(
        c, leader, retry_admission=True, control=False, number_lexemes=True
    )
    stored = next(item for item in snapshot["stores"] if item["store_id"] == 1000)
    # Replace the inventory and change a durable fact; do not simply replay
    # identical rows. Removed groups stay visible until the atomic activation.
    stored["group_statuses"] = stored["group_statuses"][:-2]
    stored["runtime_statuses"] = stored["runtime_statuses"][:-2]
    stored["group_statuses"][0]["raft_term"] = 99
    manifest, chunks = plan_baseline(report, stored, 2)
    leader, progress, _ = post_baseline(c, manifest)
    assert progress["next_chunk"] == 0
    first = {**manifest, "action": "chunk", "report": chunks[0]}
    _, progress, _ = post_baseline(c, first)
    assert progress["next_chunk"] == 1
    _, repeated, _ = post_baseline(c, first)
    assert repeated == progress
    before, _ = read_pages(c, leader, retry_admission=True, control=False)
    visible = next(item for item in before["stores"] if item["store_id"] == 1000)
    assert len(visible["group_statuses"]) == 130
    assert visible["group_statuses"][0]["raft_term"] == 1
    c.metadata_procs[leader].terminate()
    c.metadata_procs[leader].wait(timeout=10)
    deadline = time.monotonic() + 20
    while not any(
        item.get("status", {}).get("metadata_raft_role") == "leader"
        for item in c.metadata_statuses()
    ):
        assert time.monotonic() < deadline
        time.sleep(0.1)
    leader, progress, _ = post_baseline(c, manifest)
    assert progress["next_chunk"] == 1
    for index in range(progress["next_chunk"], len(chunks)):
        _, progress, _ = post_baseline(
            c,
            {
                **manifest,
                "action": "chunk",
                "chunk_index": index,
                "report": chunks[index],
            },
        )
        assert progress["next_chunk"] == index + 1
    leader, progress, _ = post_baseline(c, {**manifest, "action": "activate"})
    assert progress["activated"]
    assert progress["cursor"] == manifest["cursor"]
    after, _ = read_pages(c, leader, retry_admission=True, control=False)
    visible = next(item for item in after["stores"] if item["store_id"] == 1000)
    assert len(visible["group_statuses"]) == 128
    assert visible["group_statuses"][0]["raft_term"] == 99
    patch = {
        **report,
        "group_statuses": [{"group_id": 100000, "raft_term": 100}],
        "runtime_statuses": [],
    }
    _, response = post_report(
        c,
        "/internal/v1/nodes/1000/status/update",
        {"sequence": 3, "base": manifest["cursor"], "report": patch},
    )
    assert response.json()["sequence"] == 3
    assert original_cursor["sequence"] == 1
    assert all(proc.poll() is None for proc in c.data_procs)
