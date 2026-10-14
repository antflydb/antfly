# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in qualification against official Nessie/Polaris and versioned S3."""

import os
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pytest
import requests
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType

from antfly_lake_maintenance.client import GatewayCatalog
from antfly_lake_maintenance.controller import Controller
from antfly_lake_maintenance.server import Gateway
from antfly_lake_maintenance.store import Store, Unavailable, digest, encode


pytestmark = pytest.mark.skipif(
    os.environ.get("ANTFLY_REAL_CATALOGS") != "1",
    reason="set ANTFLY_REAL_CATALOGS=1 with qualification services running",
)
IO_PROPERTIES = {
    "s3.endpoint": "http://127.0.0.1:29700",
    "s3.region": "us-west-2",
    "s3.access-key-id": "antfly_qualification",
    "s3.secret-access-key": "antfly_qualification_secret",
}
TOKENS = {
    "admin": "qualification_admin_123456789",
    "writer": "qualification_writer_12345678",
    "native": "qualification_native_12345678",
}


@pytest.fixture(params=["nessie", "polaris"])
def deployed(request):
    provider = request.param
    identity = uuid.uuid4().hex
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    headers = {}
    if provider == "polaris":
        headers = {"Polaris-Realm": "POLARIS"}
    config = {
        "provider": provider,
        "catalog_uri": base + "/catalog",
        "upstream_uri": "http://127.0.0.1:29720/iceberg"
        if provider == "nessie"
        else "http://127.0.0.1:29710/api/catalog",
        "nessie_uri": "http://127.0.0.1:29720/api/v2",
        "warehouse": "warehouse" if provider == "nessie" else "qualification",
        "warehouse_uri": f"s3://warehouse/{provider}/",
        "authority_uri": f"s3://warehouse/controllers/{identity}/",
        "artifact_uri": f"s3://warehouse/artifacts/{identity}/",
        "artifact_connection": "qualification",
        "gateway_enforced": True,
        "upstream_headers": headers,
        "io_properties": IO_PROPERTIES,
        "delete_batch": 1,
    }
    if provider == "polaris":
        config["oauth"] = {
            "uri": "http://127.0.0.1:29710/api/catalog/v1/oauth/tokens",
            "client_id": "root",
            "client_secret": "qualification_only",
        }
        config["oauth_headers"] = {"Polaris-Realm": "POLARIS"}
    store = Store(IO_PROPERTIES)
    # Inject a controlled clock so retention qualification does not wait ten
    # minutes. The catalogs and object store are real; no provider operation is
    # mocked. Lease acquisition and renewal use that same controller clock.
    controller = Controller(
        config, store, now_ns=lambda: time.time_ns() + 21 * 60 * 1_000_000_000
    )
    gateway = Gateway(("127.0.0.1", port), controller, TOKENS)
    worker = threading.Thread(target=gateway.serve_forever, daemon=True)
    worker.start()
    namespace = ["qualification_" + identity]
    yield controller, gateway, base, namespace
    gateway.shutdown()
    gateway.server_close()
    worker.join()


def catalog(base):
    return GatewayCatalog(
        "qualification", uri=base + "/catalog", token=TOKENS["writer"], **IO_PROPERTIES
    )


def table(catalog, namespace, controller):
    catalog.create_namespace(tuple(namespace))
    return catalog.create_table(
        (*namespace, "items"),
        schema=Schema(NestedField(1, "id", LongType(), required=False)),
        location=controller.config["warehouse_uri"] + namespace[0] + "/items",
    )


def job(controller, namespace, operation=None):
    current = controller.provider.load(namespace, "items")
    source = current["metadata"]["location"]
    identifier = current["metadata"]["table-uuid"]
    artifact = controller.config["artifact_uri"]
    value = {
        "protocol": 1,
        "provider": controller.config["provider"],
        "operation_id": operation or uuid.uuid4().hex,
        "catalog": {
            "uri": controller.config["catalog_uri"],
            "namespace": namespace,
            "name": "items",
            "warehouse": controller.config["warehouse"],
        },
        "source_uri": source,
        "table_uuid": identifier,
        "expected_metadata_location": current["metadata-location"],
        "protected_snapshots": [],
        "reader_registry": {
            "protocol": "antfly-snapshot-pins-v1",
            "connection": "qualification",
            "bucket": "warehouse",
            "prefix": artifact.split("s3://warehouse/", 1)[1].rstrip("/")
            + "/lake-readers/"
            + digest(encode([source, identifier])),
            "lease_grace_ms": 30_000,
        },
        "policy": {
            "operation_id": operation or "vacuum",
            "dry_run": False,
            "retain_ms": 600_000,
            "keep_latest": 1,
            "max_deleted": 128,
        },
    }
    body = encode(value)
    return digest(body), body


def run_to_completion(controller, request_hash, body):
    for _ in range(128):
        result = controller.run_job(request_hash, body)
        if result["state"] == "complete":
            return result
    raise AssertionError("job did not finish bounded turns")


def test_real_rows_vacuum_replay_and_restart(deployed):
    controller, gateway, base, namespace = deployed
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([1, 2], type=pa.int64())}))
        assert sorted(current.scan().to_arrow()["id"].to_pylist()) == [1, 2]
        for number in range(3):
            controller.store.put(
                current.location().rstrip("/") + f"/data/orphan-{number}.parquet",
                b"orphan",
            )
    request_hash, body = job(controller, namespace)
    first = controller.run_job(request_hash, body)
    assert first["state"] == "running"
    assert controller.state()["vacuum"]["id"] == request_hash
    reopened = Controller(
        controller.config, Store(IO_PROPERTIES), now_ns=controller.now_ns
    )
    gateway.controller = reopened
    result = run_to_completion(reopened, request_hash, body)
    assert result["deleted_objects"] == 3
    assert reopened.state()["vacuum"] is None
    with catalog(base) as cat:
        current = cat.load_table((*namespace, "items"))
        current.append(pa.table({"id": pa.array([3], type=pa.int64())}))
        assert sorted(current.scan().to_arrow()["id"].to_pylist()) == [1, 2, 3]
    assert reopened.run_job(request_hash, body) == result


def test_real_incremental_planning_keeps_admission_and_recovers_marks(deployed):
    controller, gateway, base, namespace = deployed
    controller.config["planning_files_per_turn"] = 8
    controller.config["inventory_page_size"] = 2
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([1], type=pa.int64())}))
        for number in range(8):
            controller.store.put(
                current.location().rstrip("/")
                + f"/data/incremental-orphan-{number}.parquet",
                b"orphan",
            )
    request_hash, body = job(controller, namespace)
    first = controller.run_job(request_hash, body)
    assert first["state"] == "running"
    admission = controller.state()["vacuum"]
    assert admission["phase"] == "planning"
    assert (
        controller.store.get(controller.key(f"jobs/{request_hash}/plan.json")) is None
    )
    reopened = Controller(
        controller.config, Store(IO_PROPERTIES), now_ns=controller.now_ns
    )
    gateway.controller = reopened
    assert reopened.state()["vacuum"]["epoch"] == admission["epoch"]
    for _ in range(1024):
        result = reopened.run_job(request_hash, body)
        if result["state"] == "complete":
            break
    else:
        pytest.fail("incremental planner did not converge")
    assert result["deleted_objects"] == 8
    with catalog(base) as cat:
        assert cat.load_table((*namespace, "items")).scan().to_arrow()[
            "id"
        ].to_pylist() == [1]


@pytest.mark.skipif(
    os.environ.get("ANTFLY_REAL_NESSIE_RETENTION") != "1",
    reason="requires a dedicated fresh Nessie catalog: retention cannot coexist with later conservative-history runs",
)
def test_real_nessie_retention_preserves_heads_and_protects_reader(deployed):
    controller, gateway, base, namespace = deployed
    if controller.config["provider"] != "nessie":
        pytest.skip("Nessie-specific retention")
    controller.config["nessie_history_retention_ms"] = 600_000
    controller = Controller(
        controller.config, controller.store, now_ns=controller.now_ns
    )
    gateway.controller = controller
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([1], type=pa.int64())}))
    reader = catalog(base)
    old = reader.load_table((*namespace, "items"))
    old_files = [task.file.file_path for task in old.scan().plan_files()]
    try:
        with catalog(base) as cat:
            changed = cat.load_table((*namespace, "items"))
            changed.overwrite(pa.table({"id": pa.array([2], type=pa.int64())}))
        request_hash, body = job(controller, namespace)
        run_to_completion(controller, request_hash, body)
        assert old.scan().to_arrow()["id"].to_pylist() == [1]
        denied = requests.get(
            base + "/nessie/trees/main/history?fetch=ALL",
            headers={"Authorization": "Bearer " + TOKENS["writer"]},
            timeout=10,
        )
        assert denied.status_code == 403
    finally:
        reader.close()
    request_hash, body = job(controller, namespace)
    result = run_to_completion(controller, request_hash, body)
    assert result["deleted_objects"] >= len(old_files)
    assert all(controller.store.get(uri) is None for uri in old_files)
    with catalog(base) as cat:
        assert cat.load_table((*namespace, "items")).scan().to_arrow()[
            "id"
        ].to_pylist() == [2]
    restarted = Controller(
        controller.config, Store(IO_PROPERTIES), now_ns=controller.now_ns
    )
    assert restarted.state()["nessie_history_floor_ms"] > 0
    with pytest.raises(PermissionError):
        restarted.allow_native_read("/trees/main/history")


def test_real_writer_fence_and_pinned_reader(deployed):
    controller, gateway, base, namespace = deployed
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([1], type=pa.int64())}))
    reader = catalog(base)
    retained = reader.load_table((*namespace, "items"))
    with catalog(base) as writer:
        current = writer.load_table((*namespace, "items"))
        current.overwrite(pa.table({"id": pa.array([2], type=pa.int64())}))
    request_hash, body = job(controller, namespace)
    run_to_completion(controller, request_hash, body)
    assert retained.scan().to_arrow()["id"].to_pylist() == [1]
    reader.close()
    with pytest.raises(PermissionError):
        retained.scan().to_arrow()
    orphan = current.location().rstrip("/") + "/data/racing-orphan.parquet"
    controller.store.put(orphan, b"orphan")
    request_hash, body = job(controller, namespace)
    entered, release = threading.Event(), threading.Event()
    original = controller.store.delete

    def blocked(uri, version):
        entered.set()
        assert release.wait(20)
        return original(uri, version)

    controller.store.delete = blocked
    try:
        with ThreadPoolExecutor() as executor:
            pending = executor.submit(run_to_completion, controller, request_hash, body)
            assert entered.wait(20)
            path = controller.provider.table_path(namespace, "items")
            response = requests.post(
                base + "/catalog" + path,
                headers={"Authorization": "Bearer " + TOKENS["writer"]},
                json={
                    "requirements": [],
                    "updates": [
                        {
                            "action": "set-properties",
                            "updates": {"racing-writer": "blocked"},
                        }
                    ],
                },
                timeout=20,
            )
            assert response.status_code == 409
            release.set()
            assert pending.result(timeout=30)["state"] == "complete"
    finally:
        release.set()
        controller.store.delete = original


def test_real_lost_commit_response_recovers_from_provider_marker(deployed):
    controller, gateway, base, namespace = deployed
    with catalog(base) as cat:
        table(cat, namespace, controller)
    original = controller.provider.request

    def lost(method, path, payload=None, **kwargs):
        status, response = original(method, path, payload, **kwargs)
        if method == "POST" and path.endswith("/items") and status == 200:
            raise Unavailable("simulated lost response after actual vendor commit")
        return status, response

    controller.provider.request = lost
    path = controller.provider.table_path(namespace, "items")
    with pytest.raises(Unavailable):
        controller.proxy_write(
            "POST",
            path,
            encode(
                {
                    "requirements": [],
                    "updates": [
                        {
                            "action": "set-properties",
                            "updates": {"lost-outcome": "committed"},
                        }
                    ],
                }
            ),
        )
    assert controller.state()["writer"]
    reopened = Controller(
        controller.config, Store(IO_PROPERTIES), now_ns=controller.now_ns
    )
    assert reopened.recover_writer()["complete"]
    assert reopened.state()["writer"] is None
    assert (
        reopened.provider.load(namespace, "items")["metadata"]["properties"][
            "lost-outcome"
        ]
        == "committed"
    )


def test_s3_deletes_exact_version_and_preserves_replacement(deployed):
    controller, _gateway, _base, _namespace = deployed
    uri = (
        controller.config["warehouse_uri"]
        + "version-check-"
        + uuid.uuid4().hex
        + "/data"
    )
    controller.store.put(uri, b"old-version")
    old = next(controller.store.inventory(uri.rsplit("/", 1)[0] + "/"))
    controller.store.put(uri, b"replacement")
    controller.store.delete(uri, old.version)
    assert controller.store.get(uri)[0] == b"replacement"


@pytest.mark.skipif(
    not os.environ.get("ANTFLY_NATIVE_BINARY"),
    reason="set ANTFLY_NATIVE_BINARY for native HTTP qualification",
)
def test_native_http_delegates_real_vacuum_and_replays_after_restart(
    deployed, tmp_path
):
    import json
    import subprocess
    from pathlib import Path

    controller, gateway, base, namespace = deployed
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([7], type=pa.int64())}))
        location = current.location()
    orphan = location + "/data/native-orphan.parquet"
    controller.store.put(orphan, b"orphan")
    artifact_prefix = (
        controller.config["artifact_uri"].split("s3://warehouse/", 1)[1].rstrip("/")
    )
    connections = {
        "qualification": {
            "kind": "external_io",
            "capabilities": ["lake_read", "lake_write", "storage.primary"],
            "external_io": {
                "protocol": "s3",
                "endpoint": IO_PROPERTIES["s3.endpoint"],
                "use_ssl": False,
                "addressing_style": "path",
                "buckets": ["warehouse"],
                "credentials": {
                    "source": "static",
                    "access_key_id": IO_PROPERTIES["s3.access-key-id"],
                    "secret_access_key": IO_PROPERTIES["s3.secret-access-key"],
                },
            },
        }
    }
    for name, role, capabilities in [
        ("catalog", "native", ["lake_catalog_read", "lake_catalog_write"]),
        ("maintenance", "admin", ["lake_maintenance"]),
    ]:
        connections[name] = {
            "kind": "external_io",
            "capabilities": capabilities,
            "external_io": {
                "protocol": "http",
                "hosts": [base],
                "headers": {"Authorization": "Bearer " + TOKENS[role]},
            },
        }
    config = {
        "storage": {
            "engine": "local",
            "local": {"base_dir": str(tmp_path / "data")},
            "artifacts": {
                "connection": "qualification",
                "bucket": "warehouse",
                "prefix": artifact_prefix,
            },
        },
        "connections": connections,
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    origin = f"http://127.0.0.1:{port}/db/v1"

    def call(method, path, body=None):
        result = requests.request(method, origin + path, json=body, timeout=90)
        assert result.ok, (result.status_code, result.text)
        return result.json()

    def start(log):
        process = subprocess.Popen(
            [
                str(Path(os.environ["ANTFLY_NATIVE_BINARY"]).resolve()),
                "standalone",
                "--config",
                str(config_path),
                "--data-dir",
                str(tmp_path / "data"),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--health",
                "false",
                "--auth",
                "false",
                "--models-dir",
                str(tmp_path / "models"),
            ],
            stdout=log,
            stderr=log,
        )
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            assert process.poll() is None, (tmp_path / "native.log").read_text()[-4000:]
            try:
                call("GET", "/tables")
                return process
            except requests.ConnectionError:
                time.sleep(0.1)
        process.terminate()
        process.wait(timeout=20)
        raise AssertionError("native daemon did not become ready")

    def stop(process):
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    with (tmp_path / "native.log").open("w") as log:
        process = start(log)
        try:
            call(
                "POST",
                "/tables/items",
                {
                    "schema": {
                        "storage_mode": "relational",
                        "default_type": "row",
                        "document_schemas": {
                            "row": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"id": {"type": "integer"}},
                                }
                            }
                        },
                        "base_source": {
                            "kind": "external",
                            "format": "iceberg",
                            "uri": location,
                            "credentials": {
                                "ref": "qualification",
                                "scope": location.split("s3://warehouse/", 1)[1],
                            },
                            "table_id": "items",
                            "write_policy": "iceberg_writer",
                            "catalog": {
                                "type": "rest",
                                "connection": "catalog",
                                "uri": base + "/catalog",
                                "warehouse": controller.config["warehouse"],
                                "namespace": namespace,
                                "name": "items",
                                "maintenance": {
                                    "provider": controller.config["provider"],
                                    "connection": "maintenance",
                                    "uri": base,
                                },
                            },
                        },
                    }
                },
            )
            request = {
                "action": "vacuum",
                "operation_id": "real-native-vacuum",
                "dry_run": False,
                "retain_ms": 600_000,
                "keep_latest": 1,
                "max_deleted": 128,
            }
            for _ in range(128):
                first = call("POST", "/tables/items/lake/maintenance", request)
                if first["complete"]:
                    break
            assert first["complete"] and first["delegated"], first
            assert first["provider"] == controller.config["provider"]
            assert controller.store.get(orphan) is None
            stop(process)
            process = start(log)
            assert call("POST", "/tables/items/lake/maintenance", request) == first
        finally:
            stop(process)


def test_polaris_expiration_and_retired_snapshot_resurrection_rejected(deployed):
    controller, gateway, base, namespace = deployed
    if controller.config["provider"] != "polaris":
        pytest.skip("Nessie conservatively retains all reachable commit history")
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([1], type=pa.int64())}))
        old = current.current_snapshot().model_dump(
            by_alias=True, exclude_none=True, mode="json"
        )
        old_files = [task.file.file_path for task in current.scan().plan_files()]
        current.overwrite(pa.table({"id": pa.array([2], type=pa.int64())}))
    request_hash, body = job(controller, namespace)
    result = run_to_completion(controller, request_hash, body)
    assert result["state"] == "complete"
    metadata = controller.provider.load(namespace, "items")["metadata"]
    assert old["snapshot-id"] not in [s["snapshot-id"] for s in metadata["snapshots"]]
    assert all(controller.store.get(uri) is None for uri in old_files)
    from antfly_lake_maintenance.store import Conflict

    with pytest.raises(Conflict):
        controller.proxy_write(
            "POST",
            controller.provider.table_path(namespace, "items"),
            encode(
                {
                    "requirements": [
                        {"type": "assert-table-uuid", "uuid": metadata["table-uuid"]}
                    ],
                    "updates": [
                        {"action": "add-snapshot", "snapshot": old},
                        {
                            "action": "set-snapshot-ref",
                            "ref-name": "main",
                            "type": "branch",
                            "snapshot-id": old["snapshot-id"],
                        },
                    ],
                }
            ),
        )
    assert controller.state()["writer"] is None
    with catalog(base) as cat:
        assert cat.load_table((*namespace, "items")).scan().to_arrow()[
            "id"
        ].to_pylist() == [2]


def test_late_planner_cannot_delete_after_admission_was_revoked(deployed):
    from antfly_lake_maintenance.store import Conflict

    controller, gateway, base, namespace = deployed
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([1], type=pa.int64())}))
        location = current.location()
    orphan = location + "/data/revoked-planner.parquet"
    controller.store.put(orphan, b"orphan")
    request_hash, body = job(controller, namespace)
    original = controller._plan
    planned, resume = threading.Event(), threading.Event()
    calls = 0

    def race(*args):
        nonlocal calls
        calls += 1
        if calls != 1:
            raise Unavailable("injected independent planner failure")
        result = original(*args)
        planned.set()
        assert resume.wait(15)
        return result

    controller._plan = race
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(controller.run_job, request_hash, body)
        assert planned.wait(15)
        try:
            with pytest.raises(Unavailable):
                controller.run_job(request_hash, body)
            assert controller.state()["vacuum"] is None
            lease = controller.acquire_reader(namespace, "items")
        finally:
            resume.set()
        with pytest.raises(Conflict):
            pending.result(timeout=15)
    controller._plan = original
    with pytest.raises(Conflict):
        controller.run_job(request_hash, body)
    assert controller.state()["vacuum"] is None
    assert controller.store.get(orphan)[0] == b"orphan"
    controller.release_reader(lease["id"])
    # A new operation can proceed; the stale immutable plan never becomes valid.
    next_hash, next_body = job(controller, namespace)
    assert run_to_completion(controller, next_hash, next_body)["state"] == "complete"


def test_native_pin_arriving_during_planning_blocks_retirement(deployed):
    import json

    controller, gateway, base, namespace = deployed
    if controller.config["provider"] != "polaris":
        pytest.skip("Nessie retains the original historical snapshot")
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([1], type=pa.int64())}))
        old_id = str(current.current_snapshot().snapshot_id)
        files = [task.file.file_path for task in current.scan().plan_files()]
        current.overwrite(pa.table({"id": pa.array([2], type=pa.int64())}))
    request_hash, body = job(controller, namespace)
    registry = controller._job_registry(json.loads(body))
    pin = registry + "pins/" + digest(old_id.encode())
    original = controller._plan

    def publish_pin_after_inventory(*args):
        plan = original(*args)
        controller.store.put(pin, str(controller.now_ns() + 120_000_000_000).encode())
        return plan

    controller._plan = publish_pin_after_inventory
    result = controller.run_job(request_hash, body)
    assert result["state"] == "running" and result["deleted_objects"] == 0
    assert all(controller.store.get(uri) for uri in files)
    assert (
        controller.store.get(registry + "retired/" + digest(old_id.encode()))[0]
        == old_id.encode()
    )
    controller.store.delete(
        pin,
        next(
            obj.version
            for obj in controller.store.inventory(registry + "pins/")
            if obj.uri == pin
        ),
    )
    controller._plan = original
    assert run_to_completion(controller, request_hash, body)["state"] == "complete"
    assert all(controller.store.get(uri) is None for uri in files)


def test_catalog_configuration_never_vends_private_credentials_or_promises(deployed):
    controller, gateway, base, namespace = deployed
    original = controller.provider.request

    def credentials(method, path, body=None, **kwargs):
        if path.startswith("/v1/config"):
            return 200, encode(
                {
                    "defaults": {
                        "token": "private-token",
                        "s3.secret-access-key": "private-key",
                        "prefix": "warehouse",
                    },
                    "overrides": {
                        "oauth2-server-uri": "http://private.vendor/oauth",
                        "idempotency-key-lifetime": "3600",
                        "prefix": "warehouse",
                    },
                }
            )
        return original(method, path, body, **kwargs)

    controller.provider.request = credentials
    response = requests.get(
        base + "/catalog/v1/config",
        headers={"Authorization": "Bearer " + TOKENS["writer"]},
        timeout=20,
    )
    assert response.status_code == 200
    value = response.json()
    assert value["defaults"] == {"prefix": "warehouse"}
    assert value["overrides"] == {
        "prefix": "warehouse",
        "uri": base + "/catalog",
        "rest-metrics-reporting-enabled": "false",
    }
    assert "private" not in response.text and "idempotency-key" not in response.text


def test_sdk_releases_obsolete_readers_but_keeps_live_streams(deployed):
    import gc
    import weakref

    controller, gateway, base, namespace = deployed
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        old = weakref.ref(current.io.lease)
        cap = current.io.lease.value["id"]
        stream = current.io.new_input(current.metadata_location).open()
        current.append(pa.table({"id": pa.array([8], type=pa.int64())}))
        gc.collect()
        assert old() is not None and old() in cat._leases
        assert stream.read(1)
        stream.close()
        del stream
        gc.collect()
        assert old() is None
        # Until release is acknowledged, the server conservatively retains it.
        assert cap in controller.state()["readers"]
    assert not controller.state()["readers"]


def test_nessie_native_branch_roots_survive_main_branch_vacuum(deployed):
    controller, gateway, base, namespace = deployed
    if controller.config["provider"] != "nessie":
        pytest.skip("native branches are Nessie-specific")
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([1], type=pa.int64())}))
        historical_files = [task.file.file_path for task in current.scan().plan_files()]
        head = controller.provider.json("GET", "/trees/main", native=True)["reference"]
        branch = namespace[0] + "_retained"
        status, data = controller.proxy_write(
            "POST",
            "/trees?name=" + branch + "&type=BRANCH",
            encode({"type": "BRANCH", "name": "main", "hash": head["hash"]}),
            native=True,
        )
        assert 200 <= status < 300, (status, data)
        reference = controller.provider.json("GET", "/trees/" + branch, native=True)[
            "reference"
        ]
        assert reference["hash"] == head["hash"]
        current.overwrite(pa.table({"id": pa.array([2], type=pa.int64())}))
    request_hash, body = job(controller, namespace)
    assert run_to_completion(controller, request_hash, body)["state"] == "complete"
    assert all(controller.store.get(uri) for uri in historical_files)
    assert not controller.state()["writer"]


def test_nessie_reference_pagination_uses_v2_page_token(deployed):
    controller, gateway, base, namespace = deployed
    if controller.config["provider"] != "nessie":
        pytest.skip("Nessie v2 pagination")
    full = list(controller.provider._pages("/trees", "references", native=True))
    paged = list(
        controller.provider._pages("/trees?max-records=1", "references", native=True)
    )
    assert len(full) >= 2
    assert sorted(value["name"] for value in full) == sorted(
        value["name"] for value in paged
    )


def test_vacuum_resumes_each_orphan_object_version_independently(deployed):
    controller, gateway, base, namespace = deployed
    with catalog(base) as cat:
        current = table(cat, namespace, controller)
        current.append(pa.table({"id": pa.array([1], type=pa.int64())}))
        prefix = current.location() + "/data/"
    uri = prefix + "versioned-orphan.parquet"
    for number in range(3):
        controller.store.put(uri, f"version-{number}".encode())
    originals = [
        obj
        for obj in controller.store.inventory(prefix, all_versions=True)
        if obj.uri == uri
    ]
    assert len(originals) == 3
    request_hash, body = job(controller, namespace)
    # Each bounded turn reopens the durable owner, rather than remembering a
    # deletion by URI and accidentally skipping its remaining old versions.
    for _ in range(128):
        reopened = Controller(
            controller.config, Store(IO_PROPERTIES), now_ns=controller.now_ns
        )
        result = reopened.run_job(request_hash, body)
        if result["state"] == "complete":
            break
    assert result["state"] == "complete" and result["deleted_objects"] >= 3
    assert not [
        obj
        for obj in controller.store.inventory(prefix, all_versions=True)
        if obj.uri == uri
    ]
    assert controller.store.get(uri) is None
    with catalog(base) as cat:
        assert cat.load_table((*namespace, "items")).scan().to_arrow()[
            "id"
        ].to_pylist() == [1]
