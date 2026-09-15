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
    for _ in range(256):
        if check:
            check()
        if status["phase"] in ("complete", "cancelled"):
            return status
        status = command(
            api, table, job, "publish" if status["phase"] == "ready" else "step"
        )
    pytest.fail(f"migration did not finish: {status}")


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


def test_online_vector_migration_page_receipt_survives_process_crash(stateful_api):
    api = stateful_api
    table = f"crash_migrate_{time.time_ns()}"
    seed(api, table)
    job = "wal-page"
    state = command(api, table, job)
    for _ in range(256):
        state = command(api, table, job, "step")
        if state["prepared_artifacts"]:
            break
    else:
        pytest.fail("backfill never prepared a payload")
    # Do not allow graceful shutdown to flush the WAL-only page receipt.
    api._server.proc.kill()
    api._server.proc.wait(timeout=10)
    api.restart_server()
    recovered = command(api, table, job, "status")
    for field in ("phase", "cursor", "scanned_rows", "prepared_artifacts"):
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
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr
    assert command(api, table, "second", "status")["phase"] == "complete"


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
    locked = subprocess.run(argv, capture_output=True, text=True, timeout=30)
    assert locked.returncode != 0 and "VectorMigrationCatalogInUse" in locked.stderr
    api.pause_server()
    try:
        pending = subprocess.run(
            argv + ["--once"], capture_output=True, text=True, timeout=60
        )
        assert pending.returncode == 0, pending.stderr
        catalog = json.loads((server.root / "metadata/local-metadata.json").read_text())
        record = next(t for t in catalog["tables"] if t["name"] == table)
        assert record["storage"]["dense_embeddings"] == "primary_lsm"
        assert record["storage_migration"]["request"]["job_id"] == "offline"
        complete = subprocess.run(argv, capture_output=True, text=True, timeout=180)
        assert complete.returncode == 0, complete.stderr
        assert "migration complete" in complete.stderr
        retry = subprocess.run(argv, capture_output=True, text=True, timeout=30)
        assert retry.returncode == 0, retry.stderr
        catalog = json.loads((server.root / "metadata/local-metadata.json").read_text())
        record = next(t for t in catalog["tables"] if t["name"] == table)
        assert record["storage"]["dense_embeddings"] == "vector_store"
        assert record.get("storage_migration") is None
    finally:
        api.resume_server()
    assert wait_until(
        lambda: nearest(api, table, "model_a", [1, 0, 0]) == ["a", "b"], timeout_s=90
    )
    assert nearest(api, table, "model_b", [1, 0, 0]) == ["b", "a"]
