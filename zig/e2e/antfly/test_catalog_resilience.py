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

"""Production catalog failover, large control views, and telemetry isolation."""

import copy
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import requests
from catalog_baseline import (
    JsonNumber,
    next_baseline_request,
    plan_baseline,
    post_baseline,
)
from conftest import DEFAULT_ANTFLY_BIN, internal_service_headers
from test_scaling import MultiNodeScalingCluster, _insert_docs


@pytest.fixture
def catalog_cluster(request):
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.exists():
        pytest.skip(f"antfly binary not built: {binary}")
    cluster = MultiNodeScalingCluster(str(binary), initial_data_node_count=3)
    try:
        yield cluster
    finally:
        report = getattr(request.node, "rep_call", None)
        if report and report.failed:
            for path in cluster.log_paths:
                print(f"[{path.name}]\n{path.read_text(errors='replace')[-8192:]}")
        cluster.stop(test_failed=bool(report and report.failed))


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


def test_report_baseline_fragments_large_group_and_batches_neighbors(catalog_cluster):
    c = catalog_cluster
    report, _, leader = register_reporter(c, 260)
    snapshot, _ = read_pages(
        c, leader, retry_admission=True, control=False, number_lexemes=True
    )
    stored = next(item for item in snapshot["stores"] if item["store_id"] == 1000)
    template = stored["runtime_statuses"][0]["indexes"][0]
    stored["runtime_statuses"][0]["indexes"] = [
        {**copy.deepcopy(template), "name": f"dense_{i}"} for i in range(1200)
    ]
    manifest, chunks = plan_baseline(report, stored, 2)
    assert "$fragment" in chunks[0]
    _, progress, _ = post_baseline(c, manifest)
    actions = []
    while not progress["activated"]:
        request = next_baseline_request(manifest, chunks, progress["next_chunk"])
        actions.append(request["action"])
        leader, progress, size = post_baseline(c, request)
        assert size <= 2 * 1024 * 1024
        _, repeated, _ = post_baseline(c, request)
        assert repeated == progress
        if len(actions) == 1:
            # A persisted partial group must neither replace nor append to the
            # active report, even when queried through the diagnostic reader.
            before, _ = read_pages(c, leader, retry_admission=True, control=False)
            visible = next(
                item for item in before["stores"] if item["store_id"] == 1000
            )
            assert len(visible["runtime_statuses"][0]["indexes"]) == 1
    assert "fragment" in actions and "batch" in actions
    after, _ = read_pages(c, leader, retry_admission=True, control=False)
    visible = next(item for item in after["stores"] if item["store_id"] == 1000)
    assert len(visible["group_statuses"]) == 260
    assert len(visible["runtime_statuses"][0]["indexes"]) == 1200
    assert all(proc.poll() is None for proc in c.data_procs)


def test_schema_progress_batches_validate_apply_and_replay(catalog_cluster):
    c = catalog_cluster
    path = "/internal/v1/schema-progress/batch"
    records = [
        {"table_id": 900000 + i, "node_id": 1000, "schema_version": 3}
        for i in range(64)
    ]
    leader, response = post_report(c, path, records)
    assert response.status_code == 200, response.text
    for invalid in (
        [],
        records + [records[0]],
        [records[0], records[0]],
        [records[0], {**records[1], "node_id": 1001}],
    ):
        response = requests.post(
            c.metadata_urls[leader] + path,
            json=invalid,
            headers=internal_service_headers(),
            timeout=15,
        )
        assert response.status_code == 400, response.text
    oversized = requests.post(
        c.metadata_urls[leader] + path,
        data=" " * 17000,
        headers=internal_service_headers(),
        timeout=15,
    )
    assert oversized.status_code == 413, oversized.text
    _, replay = post_report(c, path, records)
    assert replay.status_code == 200, replay.text
    snapshot, _ = read_pages(c, leader, control=True, retry_admission=True)
    observed = [r for r in snapshot["schema_progresses"] if r["node_id"] == 1000]
    assert sorted(observed, key=lambda r: r["table_id"]) == records


def test_concurrent_tenant_schema_migrations_preserve_documents(catalog_cluster):
    c = catalog_cluster
    api = c.data_api_urls[0]
    tables = [f"migration_tenant_{i}" for i in range(3)]
    docs = {
        f"doc-{i:03d}": {"title": f"Migration document {i}", "tenant_value": i}
        for i in range(30)
    }
    for table in tables:
        c.create_table(table, num_shards=2)
        _insert_docs(c, table, docs, min_group_count=2)

    schema = {
        "document_schemas": {
            "default": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "x-antfly-types": ["text"]}
                    },
                }
            }
        }
    }

    def migrate(table):
        response = requests.put(f"{api}/tables/{table}/schema", json=schema, timeout=30)
        response.raise_for_status()
        assert response.json()["schema"]["version"] == 1

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=len(tables)) as pool:
        list(pool.map(migrate, tables))
    pending = set(tables)
    deadline = started + 180
    statuses = {}
    while pending:
        for table in list(pending):
            response = requests.get(f"{api}/tables/{table}", timeout=15)
            response.raise_for_status()
            statuses[table] = response.json()
            if (
                statuses[table].get("migration") is None
                and statuses[table].get("schema", {}).get("version") == 1
            ):
                pending.remove(table)
        assert time.monotonic() < deadline, (
            json.dumps(statuses, indent=2)
            + "\n"
            + c.metadata_snapshot_diagnostic()
            + "\n"
            + c.debug_logs()
        )
        if pending:
            time.sleep(0.2)
    for table in tables:
        for key in ("doc-000", "doc-015", "doc-029"):
            response = requests.get(f"{api}/tables/{table}/documents/{key}", timeout=15)
            response.raise_for_status()
            assert response.json() == docs[key]
    if os.environ.get("ANTFLY_E2E_PHASE_TIMINGS") == "1":
        print(
            json.dumps(
                {
                    "scenario": "concurrent_tenant_schema_migrations",
                    "tables": len(tables),
                    "shards_per_table": 2,
                    "replicas_per_shard": 3,
                    "documents_per_table": len(docs),
                    "migration_and_validation_ms": (time.monotonic() - started) * 1000,
                }
            )
        )


def test_skewed_schema_migrations_keep_foreground_traffic_available(catalog_cluster):
    """A large tenant migrates alongside small tenants and active reads/writes.

    Set ANTFLY_MIGRATION_LARGE_DOCS and ANTFLY_MIGRATION_SMALL_TENANTS for
    repeatable scale runs. JSON records contain raw request latency samples;
    provisioning is excluded and application failures are never retried away.
    """
    import threading

    c = catalog_cluster
    api = c.data_api_urls[0]
    large_docs = int(os.environ.get("ANTFLY_MIGRATION_LARGE_DOCS", "2000"))
    small_count = int(os.environ.get("ANTFLY_MIGRATION_SMALL_TENANTS", "3"))
    traffic_clients = int(os.environ.get("ANTFLY_MIGRATION_TRAFFIC_CLIENTS", "1"))
    assert large_docs > 0 and small_count > 0 and traffic_clients > 0
    tables = ["migration_large"] + [f"migration_small_{i}" for i in range(small_count)]
    for table in tables:
        c.create_table(table, num_shards=1)
        count = large_docs if table == tables[0] else 20
        for offset in range(0, count, 500):
            _insert_docs(
                c,
                table,
                {
                    f"doc-{i:06d}": {
                        "title": f"searchable migration document {i}",
                        "body": "production document payload " * 32,
                    }
                    for i in range(offset, min(offset + 500, count))
                },
            )

    # Start timing only after the retained read indexes have caught up.
    for table in tables:
        deadline = time.monotonic() + 120
        expected = large_docs if table == tables[0] else 20
        while True:
            response = requests.get(
                f"{api}/tables/{table}/indexes/full_text_index_v0", timeout=15
            )
            response.raise_for_status()
            status = response.json()["status"]
            if (
                not status.get("backfill_active", False)
                and status.get("doc_count", 0) >= expected
            ):
                break
            assert time.monotonic() < deadline, response.text
            time.sleep(0.1)

    schema = {
        "document_schemas": {
            "default": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "x-antfly-types": ["text"]},
                        "body": {"type": "string", "x-antfly-types": ["text"]},
                    },
                }
            }
        }
    }
    stop = threading.Event()
    latencies = {"lookup_ms": [], "search_ms": [], "write_ms": []}
    failures = []
    acknowledged = []
    start = time.monotonic()

    def traffic(client_id):
        with requests.Session() as session:
            sequence = 0
            while not stop.is_set():
                for table in (tables[0], tables[-1]):
                    for operation in ("lookup", "search", "write"):
                        if stop.is_set():
                            return
                        before = time.monotonic()
                        response = None
                        try:
                            if operation == "lookup":
                                response = session.get(
                                    f"{api}/tables/{table}/documents/doc-000000",
                                    timeout=10,
                                )
                                response.raise_for_status()
                                assert response.json()["title"].startswith("searchable")
                            elif operation == "search":
                                response = session.post(
                                    f"{api}/tables/{table}/query",
                                    json={
                                        "full_text_search": {"match_all": {}},
                                        "limit": 1,
                                    },
                                    timeout=10,
                                )
                                response.raise_for_status()
                                assert response.json()["responses"][0]["hits"]["hits"]
                            else:
                                key = f"live-{client_id}-{sequence:06d}"
                                response = session.post(
                                    f"{api}/tables/{table}/batch",
                                    json={"inserts": {key: {"title": "live traffic"}}},
                                    timeout=10,
                                )
                                response.raise_for_status()
                                acknowledged.append((table, key))
                        except (
                            requests.RequestException,
                            AssertionError,
                            KeyError,
                            ValueError,
                        ) as error:
                            detail = (
                                response.text
                                if response is not None and not response.ok
                                else ""
                            )
                            failures.append(f"{operation} {table}: {error} {detail}")
                        finally:
                            latencies[f"{operation}_ms"].append(
                                (time.monotonic() - before) * 1000
                            )
                sequence += 1
                stop.wait(0.02)

    def migrate(table):
        response = requests.put(f"{api}/tables/{table}/schema", json=schema, timeout=30)
        response.raise_for_status()

    workers = [
        threading.Thread(target=traffic, args=(i,)) for i in range(traffic_clients)
    ]
    for worker in workers:
        worker.start()
    completed_ms = {}
    try:
        with ThreadPoolExecutor(max_workers=len(tables)) as pool:
            list(pool.map(migrate, tables))
        deadline = start + 240
        while len(completed_ms) != len(tables):
            for table in tables:
                if table in completed_ms:
                    continue
                response = requests.get(f"{api}/tables/{table}", timeout=15)
                response.raise_for_status()
                status = response.json()
                if (
                    status.get("migration") is None
                    and status.get("schema", {}).get("version") == 1
                ):
                    completed_ms[table] = (time.monotonic() - start) * 1000
            assert time.monotonic() < deadline, c.metadata_snapshot_diagnostic()
            time.sleep(0.05)
    finally:
        stop.set()
        join_deadline = time.monotonic() + 45
        for worker in workers:
            worker.join(timeout=max(0, join_deadline - time.monotonic()))
        record = {
            "scenario": "skewed_schema_migrations_with_foreground_traffic",
            "large_documents": large_docs,
            "small_tenants": small_count,
            "traffic_clients": traffic_clients,
            "small_documents_per_tenant": 20,
            "replicas": 3,
            "completed_ms": completed_ms,
            "request_latencies": latencies,
            "failures": failures,
        }
        print(json.dumps(record, sort_keys=True))
    assert not any(worker.is_alive() for worker in workers)
    assert not failures, json.dumps(record)
    assert acknowledged
    # Check every acknowledged write after cutover; migration must not lose
    # documents arriving behind the durable source cursor.
    for table, key in acknowledged:
        response = requests.get(f"{api}/tables/{table}/documents/{key}", timeout=10)
        response.raise_for_status()
        assert response.json()["title"] == "live traffic"
    for table in tables:
        expected = (large_docs if table == tables[0] else 20) + sum(
            name == table for name, _ in acknowledged
        )
        deadline = time.monotonic() + 30
        while True:
            response = requests.post(
                f"{api}/tables/{table}/query",
                json={"full_text_search": {"match_all": {}}, "limit": 1},
                timeout=10,
            )
            response.raise_for_status()
            total = response.json()["responses"][0]["hits"]["total"]["value"]
            if total == expected:
                break
            assert time.monotonic() < deadline, (table, total, expected)
            time.sleep(0.1)
