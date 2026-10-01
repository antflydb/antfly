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

"""Source ownership migration through the production compiled owner and catalog."""

import json
import math
import random
import shutil
import subprocess
import time

import pytest
import requests
from helpers import wait_until
from test_vector_store import hit_ids


def command(api, table, job, action="start"):
    path = f"/tables/{table}/storage/migrations"
    if action == "start":
        return api.post(
            path,
            {
                "job_id": job,
                "target": "vector_store",
                "budget": {
                    "batch_rows": 8,
                    "batch_bytes": 4096,
                    "disk_reserve_bytes": 0,
                },
            },
        )
    if action == "status":
        return api.get(f"{path}/{job}")
    return api.post(f"{path}/{job}", {"action": action})


def seed(api, table):
    api.create_table(table, storage={"dense_embeddings": "primary_lsm"})
    for name in ("model_a", "model_b"):
        api.create_index(
            table,
            name,
            {
                "name": name,
                "type": "embeddings",
                "external": True,
                "dimension": 3,
            },
        )
    api.batch_write(
        table,
        inserts={
            "a": {
                "text": "alpha",
                "_embeddings": {"model_a": [1, 0, 0], "model_b": [0, 1, 0]},
            },
            "b": {
                "text": "beta",
                "_embeddings": {"model_a": [0, 1, 0], "model_b": [1, 0, 0]},
            },
        },
        sync_level="full_index",
    )
    assert wait_until(
        lambda: nearest(api, table, "model_a", [1, 0, 0]) == ["a", "b"], timeout_s=90
    )


def nearest(api, table, index, vector):
    return hit_ids(
        api.query_table(
            table, {"embeddings": {index: vector}, "indexes": [index], "limit": 2}
        )
    )


def finish(api, table, job, status=None, check=None):
    status = status or command(api, table, job)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if check:
            check()
        if status["phase"] in ("complete", "cancelled"):
            return status
        status = command(
            api, table, job, "publish" if status["phase"] == "ready" else "step"
        )
        # Reclamation includes timed GC admission retries and asynchronous work;
        # a fixed number of tight HTTP requests is not a completion deadline.
        if status["phase"] in ("serving", "reclaiming"):
            time.sleep(0.01)
    pytest.fail(f"migration did not finish: {status}")


def wait_for_ann_refresh(api, table, index, count, query):
    """Activate the lazy owner, then wait for a verified clean posting sweep."""
    api.query_table(
        table, {"embeddings": {index: query}, "indexes": [index], "limit": 1}
    )

    def settled():
        status = api.get_index(table, index).get("status", {})
        return (
            status.get("runtime_fresh") is True
            and status.get("total_indexed") == count
            and status.get("readiness", {}).get("complete") is True
            and status.get("hbc_posting", {}).get("refresh_pending") is False
        )

    assert wait_until(settled, timeout_s=90, interval_s=0.1), json.dumps(
        api.get_index(table, index), indent=2
    )


@pytest.mark.parametrize("mode", ["restart", "online", "offline"])
def test_vector_migration_preserves_native_ann_neighbors(stateful_api, mode):
    """Compare the same built ANN before/after ownership conversion and restart."""
    api = stateful_api
    table = f"neighbors_migrate_{mode}_{time.time_ns()}"
    rng = random.Random(728)

    def vector():
        values = [rng.gauss(0, 1) for _ in range(64)]
        length = math.sqrt(sum(x * x for x in values))
        return [x / length for x in values]

    api.create_table(table, storage={"dense_embeddings": "primary_lsm"})
    api.create_index(
        table,
        "model",
        {"name": "model", "type": "embeddings", "external": True, "dimension": 64},
    )
    for first in range(0, 4096, 256):
        api.batch_write(
            table,
            inserts={
                f"doc:{i:06d}": {
                    "text": f"document {i}",
                    "_embeddings": {"model": vector()},
                }
                for i in range(first, first + 256)
            },
            sync_level="full_index",
        )
    assert wait_until(
        lambda: (
            api.get_index(table, "model").get("status", {}).get("total_indexed") == 4096
        ),
        timeout_s=90,
    )
    queries = [vector() for _ in range(32)]

    def neighbors():
        return [
            hit_ids(
                api.query_table(
                    table,
                    {"embeddings": {"model": q}, "indexes": ["model"], "limit": 10},
                )
            )
            for q in queries
        ]

    # Replay completion and restart do not drain optional posting refresh.
    # Dirty leaves use exact member scoring; repaired leaves use RaBitQ, so
    # their approximate candidates can differ even with no storage migration.
    # Certify a clean sweep after reopen before comparing the same ANN state.
    api.restart_server()
    wait_for_ann_refresh(api, table, "model", 4096, queries[0])
    before = neighbors()
    assert all(len(hits) == 10 for hits in before)
    assert neighbors() == before
    if mode == "online":
        status = api.post(
            f"/tables/{table}/storage/migrations",
            {
                "job_id": "neighbors",
                "target": "vector_store",
                "budget": {
                    "batch_rows": 8192,
                    "batch_bytes": 8 * 1024 * 1024,
                    "disk_reserve_bytes": 0,
                },
            },
        )
        assert finish(api, table, "neighbors", status=status)["phase"] == "complete"
    elif mode == "offline":
        server = api._server
        api.pause_server()
        try:
            completed = subprocess.run(
                [
                    str(server.binary),
                    "storage",
                    "migrate",
                    "--to",
                    "vector-store",
                    "--catalog",
                    str(server.root / "metadata/local-metadata.json"),
                    "--replica-root",
                    str(server.replica_root),
                    "--table",
                    table,
                    "--job",
                    "neighbors",
                    "--disk-reserve-bytes",
                    "0",
                ],
                capture_output=True,
                check=False,
                text=True,
                timeout=180,
            )
            assert completed.returncode == 0, completed.stderr
        finally:
            api.resume_server()
    assert neighbors() == before
    api.restart_server()
    wait_for_ann_refresh(api, table, "model", 4096, queries[0])
    assert neighbors() == before
    # A second reopen also checks the no-migration control and catches a
    # baseline captured before deferred maintenance was actually certified.
    api.restart_server()
    wait_for_ann_refresh(api, table, "model", 4096, queries[0])
    assert neighbors() == before


def test_online_vector_migration_cancels_rejected_admission(stateful_api):
    api = stateful_api
    table = f"rejected_migrate_{time.time_ns()}"
    api.create_table(table, storage={"dense_embeddings": "primary_lsm"})
    path = f"/tables/{table}/storage/migrations"
    request = {
        "job_id": "rejected",
        "target": "vector_store",
        "budget": {"disk_reserve_bytes": 2**64 - 1},
    }
    with pytest.raises(requests.HTTPError) as rejected:
        api.post(path, request)
    assert rejected.value.response.status_code == 503
    assert "VectorMigrationDiskReserve" in rejected.value.response.text
    api.restart_server()
    assert command(api, table, "rejected", "status")["phase"] == "admitted"
    cancelled = command(api, table, "rejected", "cancel")
    assert cancelled["phase"] == "cancelled"
    api.restart_server()
    assert command(api, table, "rejected", "cancel") == cancelled
    assert api.post(path, request) == cancelled
    # A second rejected admission encounters the previous cancelled DB receipt.
    request["job_id"] = "second"
    with pytest.raises(requests.HTTPError) as rejected:
        api.post(path, request)
    assert rejected.value.response.status_code == 503
    assert command(api, table, "second", "status")["phase"] == "admitted"
    assert command(api, table, "second", "cancel")["phase"] == "cancelled"
    replacement = command(api, table, "replacement")
    assert replacement["phase"] == "backfill"
    assert finish(api, table, "replacement", status=replacement)["phase"] == "complete"
    api.delete_table(table)


def test_online_vector_migration_restart_concurrent_models_and_rebuild(stateful_api):
    api = stateful_api
    table = f"online_migrate_{time.time_ns()}"
    seed(api, table)
    job = "online"
    status = command(api, table, job)
    assert status["phase"] == "backfill"
    assert command(api, table, job) == status
    assert command(api, table, job, "status") == status
    assert command(api, table, job, "status") == status
    with pytest.raises(requests.HTTPError) as missing:
        command(api, table, "missing", "status")
    assert missing.value.response.status_code == 404
    with pytest.raises(requests.HTTPError) as drop:
        api.delete_table(table)
    assert drop.value.response.status_code in (400, 409)
    with pytest.raises(requests.HTTPError) as duplicate:
        command(api, table, "different")
    assert duplicate.value.response.status_code == 409
    command(api, table, job, "step")
    api.batch_write(
        table,
        inserts={
            "a": {
                "text": "new version",
                "_embeddings": {"model_a": [0, 0, 1], "model_b": [1, 0, 0]},
            },
            "0": {
                "text": "behind cursor",
                "_embeddings": {"model_a": [1, 0, 0], "model_b": [0, 0, 1]},
            },
        },
        deletes=["b"],
        sync_level="full_index",
    )
    api.restart_server()

    def check():
        assert nearest(api, table, "model_a", [0, 0, 1]) == ["a", "0"]
        assert nearest(api, table, "model_b", [0, 0, 1]) == ["0", "a"]

    assert wait_until(
        lambda: nearest(api, table, "model_a", [0, 0, 1]) == ["a", "0"], timeout_s=90
    )
    status = finish(api, table, job, check=check)
    assert status["phase"] == "complete"
    assert status["publication_fence"] >= status["snapshot_fence"]
    assert api.get_table(table)["storage"]["dense_embeddings"] == "vector_store"
    with pytest.raises(requests.HTTPError) as cancel:
        command(api, table, job, "cancel")
    assert cancel.value.response.status_code == 409
    api.restart_server()
    assert command(api, table, job, "status")["phase"] == "complete"
    check()
    api.delete_index(table, "model_a")
    api.delete_index(table, "model_b")
    api.restart_server()
    api.create_index(
        table,
        "model_a",
        {"name": "model_a", "type": "embeddings", "external": True, "dimension": 3},
    )
    assert wait_until(
        lambda: nearest(api, table, "model_a", [0, 0, 1]) == ["a", "0"], timeout_s=90
    )


@pytest.mark.parametrize("crash_phase", ["backfill", "reclaiming"])
def test_online_vector_migration_page_receipt_survives_process_crash(
    stateful_api, crash_phase
):
    api = stateful_api
    table = f"crash_migrate_{time.time_ns()}"
    seed(api, table)
    job = "wal-page"
    state = command(api, table, job)
    for _ in range(256):
        action = "publish" if state["phase"] == "ready" else "step"
        state = command(api, table, job, action)
        if crash_phase == "backfill" and state["prepared_artifacts"]:
            break
        if crash_phase == "reclaiming" and state.get("primary_reclamation_requested"):
            break
    else:
        pytest.fail(f"migration never reached {crash_phase} receipt")
    # Do not allow graceful shutdown to flush the WAL-only page receipt.
    api._server.proc.kill()
    api._server.proc.wait(timeout=10)
    api.restart_server()
    recovered = command(api, table, job, "status")
    for field in (
        "phase",
        "cursor",
        "scanned_rows",
        "prepared_artifacts",
        "primary_reclamation_requested",
    ):
        assert recovered[field] == state[field]
    assert finish(api, table, job, status=recovered)["phase"] == "complete"
    assert nearest(api, table, "model_a", [1, 0, 0]) == ["a", "b"]
    assert nearest(api, table, "model_b", [0, 1, 0]) == ["a", "b"]


def test_online_vector_migration_cancellation_reopens_inline_authority(stateful_api):
    api = stateful_api
    table = f"cancel_migrate_{time.time_ns()}"
    seed(api, table)
    command(api, table, "cancel")
    command(api, table, "cancel", "step")
    command(api, table, "cancel", "cancel")
    api.restart_server()
    assert finish(api, table, "cancel")["phase"] == "cancelled"
    api.restart_server()
    assert api.get_table(table)["storage"]["dense_embeddings"] == "primary_lsm"
    assert nearest(api, table, "model_a", [1, 0, 0]) == ["a", "b"]
    # The packaged command drives the same authenticated HTTP contract.
    server = api._server
    completed = subprocess.run(
        [
            server.binary,
            "storage",
            "migrate",
            "--to",
            "vector-store",
            "--url",
            server.url,
            "--table",
            table,
            "--job",
            "second",
            "--batch-rows",
            "8",
            "--batch-bytes",
            "4096",
            "--disk-reserve-bytes",
            "0",
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr
    assert command(api, table, "second", "status")["phase"] == "complete"


def test_online_vector_migration_skips_large_documents_after_publication(stateful_api):
    api = stateful_api
    table = f"migration_large_document_{time.time_ns()}"
    seed(api, table)
    status = command(api, table, "large")
    for _ in range(128):
        if status["phase"] == "ready":
            break
        status = command(api, table, "large", "step")
    assert status["phase"] == "ready"
    document = {"text": "x" * 6000}
    api.batch_write(table, inserts={"large": document}, sync_level="full_index")
    status = command(api, table, "large", "publish")
    assert status["phase"] == "draining"
    api.restart_server()
    assert finish(api, table, "large")["phase"] == "complete"
    assert api.lookup_key(table, "large") == document
    assert nearest(api, table, "model_a", [1, 0, 0]) == ["a", "b"]


@pytest.mark.parametrize("backup_format", ["native", "portable"])
def test_cancelled_vector_migration_allows_backup_without_restart(
    stateful_api, tmp_path, backup_format
):
    api = stateful_api
    table = f"migration_cancel_backup_{time.time_ns()}"
    seed(api, table)
    command(api, table, "cancel")
    command(api, table, "cancel", "step")
    command(api, table, "cancel", "cancel")
    assert finish(api, table, "cancel")["phase"] == "cancelled"
    assert api.get_table(table)["storage"]["dense_embeddings"] == "primary_lsm"
    location = tmp_path.resolve().as_uri()
    assert (
        api.backup_table(
            table, backup_id="cancelled", location=location, backup_format=backup_format
        )["backup"]
        == "successful"
    )
    api.delete_table(table)
    assert api.restore_table(table, backup_id="cancelled", location=location) == {
        "restore": "triggered"
    }
    assert api.lookup_key(table, "a")["text"] == "alpha"
    api.restart_server()
    assert api.lookup_key(table, "a")["text"] == "alpha"
    assert nearest(api, table, "model_a", [1, 0, 0]) == ["a", "b"]


def test_offline_vector_migration_cancels_before_copy_fence(stateful_api, tmp_path):
    api = stateful_api
    table = f"offline_cancel_admission_{time.time_ns()}"
    api.create_table(table, storage={"dense_embeddings": "primary_lsm"})
    server = api._server
    api.pause_server()
    catalog_path = server.root / "metadata/local-metadata.json"

    def invoke(root, job="cancel-before-fence", *extra):
        return subprocess.run(
            [
                str(server.binary),
                "storage",
                "migrate",
                "--to",
                "vector-store",
                "--catalog",
                str(catalog_path),
                "--replica-root",
                str(root),
                "--table",
                table,
                "--job",
                job,
                "--disk-reserve-bytes",
                "0",
                *extra,
            ],
            text=True,
            capture_output=True,
            check=False,
            timeout=60,
        )

    def record():
        status = invoke(
            server.replica_root, "cancel-before-fence", "--action", "status"
        )
        assert status.returncode == 0, status.stderr
        return json.loads(status.stderr)

    try:
        rejected = invoke(
            server.root / "wrong-replica-root", "cancel-before-fence", "--once"
        )
        assert rejected.returncode != 0 and "FileNotFound" in rejected.stderr
        catalog_store = catalog_path.with_suffix(catalog_path.suffix + ".store")
        admitted_catalog = tmp_path / "admitted-catalog.store"
        shutil.copytree(catalog_store, admitted_catalog)
        assert (
            record()["storage_migration"]["request"]["job_id"] == "cancel-before-fence"
        )
        cancelled = invoke(server.replica_root, "cancel-before-fence", "--cancel")
        assert cancelled.returncode == 0, cancelled.stderr
        assert record().get("storage_migration") is None
        # Model a lost catalog publication after the durable DB cancellation.
        shutil.rmtree(catalog_store)
        shutil.copytree(admitted_catalog, catalog_store)
        cancelled = invoke(server.replica_root, "cancel-before-fence", "--cancel")
        assert cancelled.returncode == 0, cancelled.stderr
        assert record().get("storage_migration") is None
        retried = invoke(server.replica_root, "cancel-before-fence", "--once")
        assert retried.returncode != 0 and "VectorMigrationCancelled" in retried.stderr
        assert record().get("storage_migration") is None
        assert record()["storage"]["dense_embeddings"] == "primary_lsm"
        replacement = invoke(server.replica_root, "replacement", "--once")
        assert replacement.returncode == 0, replacement.stderr
        cancelled = invoke(server.replica_root, "replacement", "--cancel")
        assert cancelled.returncode == 0, cancelled.stderr
        assert record().get("storage_migration") is None
    finally:
        api.resume_server()
    assert api.get_table(table)["storage"]["dense_embeddings"] == "primary_lsm"
    api.delete_table(table)


def test_offline_vector_migration_lock_resume_catalog_and_native_queries(stateful_api):
    api = stateful_api
    table = f"offline_migrate_{time.time_ns()}"
    seed(api, table)
    server = api._server
    assert server is not None and hasattr(server, "root")
    argv = [
        str(server.binary),
        "storage",
        "migrate",
        "--to",
        "vector-store",
        "--catalog",
        str(server.root / "metadata/local-metadata.json"),
        "--replica-root",
        str(server.replica_root),
        "--table",
        table,
        "--job",
        "offline",
        "--batch-bytes",
        "4096",
        "--disk-reserve-bytes",
        "0",
    ]
    locked = subprocess.run(
        argv, capture_output=True, check=False, text=True, timeout=30
    )
    assert locked.returncode != 0 and "VectorMigrationCatalogInUse" in locked.stderr
    api.pause_server()
    try:
        pending = subprocess.run(
            argv + ["--once"], capture_output=True, check=False, text=True, timeout=60
        )
        assert pending.returncode == 0, pending.stderr
        observed = subprocess.run(
            argv + ["--action", "status"],
            capture_output=True,
            check=False,
            text=True,
            timeout=30,
        )
        assert observed.returncode == 0, observed.stderr
        record = json.loads(observed.stderr)
        assert record["storage"]["dense_embeddings"] == "primary_lsm"
        assert record["storage_migration"]["request"]["job_id"] == "offline"
        complete = subprocess.run(
            argv, capture_output=True, check=False, text=True, timeout=180
        )
        assert complete.returncode == 0, complete.stderr
        assert "migration complete" in complete.stderr
        retry = subprocess.run(
            argv, capture_output=True, check=False, text=True, timeout=30
        )
        assert retry.returncode == 0, retry.stderr
        observed = subprocess.run(
            argv + ["--action", "status"],
            capture_output=True,
            check=False,
            text=True,
            timeout=30,
        )
        assert observed.returncode == 0, observed.stderr
        record = json.loads(observed.stderr)
        assert record["storage"]["dense_embeddings"] == "vector_store"
        assert record.get("storage_migration") is None
    finally:
        api.resume_server()
    assert wait_until(
        lambda: nearest(api, table, "model_a", [1, 0, 0]) == ["a", "b"], timeout_s=90
    )
    assert nearest(api, table, "model_b", [1, 0, 0]) == ["b", "a"]
