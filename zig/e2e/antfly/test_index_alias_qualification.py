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

"""Alias cutovers through real replicated and object-backed runtime owners."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import requests
from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    resolve_binary_path,
    wait_for_server,
)
from helpers import wait_until
from index_alias_qualification import (
    INDEXES,
    SEED,
    alias_url,
    checked,
    exercise_swaps,
    query,
    swap,
    wait_for_target,
)
from port_reservations import LoopbackPortReservations
from test_backup_restore import (
    _create_cluster_table_when_admitted,
    _seed_cluster_docs_when_writable,
)
from test_backup_restore import (
    three_by_three_backup_cluster as replicated_cluster_fixture,
)

# Reuse the production portable seed/bootstrap helper. It starts an empty
# whole-instance stream, so catalog changes must actually replicate afterwards.
from test_standby import (
    DB_API_ROOT,
    _primary_lsn,
    _promotion_fence_request,
    _wait_for_standby_applied,
    _wait_for_standby_lookup,
)
from test_standby_empty_bootstrap import _bootstrap_empty
from test_standby_empty_bootstrap import ha_cluster as standby_cluster_fixture

three_by_three_backup_cluster = replicated_cluster_fixture
ha_cluster = standby_cluster_fixture


@pytest.mark.slow
def test_index_alias_cutovers_during_ingestion_and_metadata_failover(
    three_by_three_backup_cluster,
):
    cluster = three_by_three_backup_cluster
    table = "alias_cutover"
    with requests.Session() as session:
        session.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
        _create_cluster_table_when_admitted(
            cluster, session, table, {"num_shards": 3, "indexes": INDEXES}
        )
        assert wait_until(
            lambda: cluster.fully_replicated_topology(table), timeout_s=90
        ), cluster.debug_logs()
        _seed_cluster_docs_when_writable(
            cluster, session, table, SEED, sync_level="full_index", timeout_s=60
        )
        leader = cluster.metadata_stable_leader_id(timeout_s=30)
        assert leader is not None
        snapshot = cluster.metadata_snapshot(leader - 1)
        ranges = sorted(
            (
                entry
                for entry in snapshot["ranges"]
                if int(entry["table_id"]) == cluster.table_ids[table]
            ),
            key=lambda entry: entry["start_key"],
        )
        assert len(ranges) == 3, ranges
        for prefix, shard in zip(("0", "8", "z"), ranges, strict=True):
            for suffix in ("a", "b"):
                key = f"{prefix}:{suffix}"
                assert shard["start_key"] <= key
                assert shard["end_key"] is None or key < shard["end_key"]
        base = cluster.data_api_urls[0]
        for name in INDEXES:
            wait_for_target(session, base, table, name)
        swap(session, base, table, "space_a", "initial")

        def failover():
            old = cluster.metadata_stable_leader_id(timeout_s=30)
            assert old is not None
            process = cluster.metadata_procs[old - 1]
            process.kill()
            process.wait(timeout=10)
            try:
                new = cluster.metadata_stable_leader_id(timeout_s=45)
                assert new is not None and new != old, cluster.debug_logs()
                # Read through every data gateway while the old metadata leader
                # is absent. The caller's target remains bound per statement.
                for gateway in cluster.data_api_urls:
                    assert query(session, gateway, table) == "space_a"
            finally:
                cluster.restart_crashed_node(metadata=True, index=old - 1)

        print(exercise_swaps(session, base, table, failover=failover))
        # A replay after subsequent swaps reconciles the original response and
        # never reactivates that historical target.
        replay = swap(session, base, table, "space_b", "swap-0-space_b", "space_a")
        assert replay["index"] == "space_b"
        assert (
            checked(session.get(alias_url(base, table), timeout=10))["index"]
            == "space_a"
        )
        assert query(session, base, table) == "space_a"


@pytest.mark.objectstore_integration
@pytest.mark.fresh_antfly_process
@pytest.mark.e2e_resource("antfly_process")
def test_index_alias_object_runtime_cutover_and_cold_restart(tmp_path, monkeypatch):
    """Use actual MinIO conditional writes/ranges, not the in-memory S3 double."""
    minio = shutil.which("minio")
    aws = shutil.which("aws")
    if not minio or not aws:
        pytest.skip("Local S3 qualification requires minio and aws executables")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    assert Path(binary).is_file(), binary
    ports = LoopbackPortReservations("127.0.0.1")
    port, console = ports.reserve_many(2)
    endpoint = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "MINIO_ROOT_USER": "aliasfixture",
        "MINIO_ROOT_PASSWORD": "alias-fixture-secret",
        "AWS_ACCESS_KEY_ID": "aliasfixture",
        "AWS_SECRET_ACCESS_KEY": "alias-fixture-secret",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    log = (tmp_path / "minio.log").open("w")
    process = None
    server = None
    try:
        process = ports.handoff_to(
            (port, console),
            lambda: subprocess.Popen(
                [
                    minio,
                    "server",
                    str(tmp_path / "objects"),
                    "--address",
                    f"127.0.0.1:{port}",
                    "--console-address",
                    f"127.0.0.1:{console}",
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            ),
        )
        assert wait_for_server(
            endpoint, path="/minio/health/ready", processes=[("minio", process)]
        ), (tmp_path / "minio.log").read_text()
        subprocess.run(
            [
                aws,
                "--endpoint-url",
                endpoint,
                "s3api",
                "create-bucket",
                "--bucket",
                "aliases",
            ],
            env=env,
            check=True,
            capture_output=True,
            timeout=30,
        )
        import conftest

        original = conftest._write_remote_content_e2e_config

        def configured(root, **kwargs):
            path = original(root, **kwargs)
            cfg = json.loads(path.read_text())
            cfg["storage"] = {
                "engine": "local",
                "local": {"base_dir": str(root)},
                "artifacts": {
                    "connection": "alias_objects",
                    "bucket": "aliases",
                    "prefix": "tables",
                },
            }
            cfg.setdefault("connections", {})["alias_objects"] = {
                "kind": "external_io",
                "capabilities": ["storage.primary"],
                "external_io": {
                    "protocol": "s3",
                    "endpoint": endpoint,
                    "addressing_style": "path",
                    "use_ssl": False,
                    "region": "us-east-1",
                    "buckets": ["aliases"],
                    "credentials": {
                        "source": "static",
                        "access_key_id": "aliasfixture",
                        "secret_access_key": "alias-fixture-secret",
                    },
                },
            }
            path.write_text(json.dumps(cfg))
            return path

        monkeypatch.setattr(conftest, "_write_remote_content_e2e_config", configured)
        server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
        with requests.Session() as session:
            session.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            base = server.api_url
            table = "object_alias_cutover"
            checked(
                session.post(
                    f"{base}/tables/{table}",
                    json={
                        "storage": {"engine": "object"},
                        "indexes": {**INDEXES, "text/model/v1": {"type": "full_text"}},
                    },
                    timeout=30,
                )
            )
            checked(
                session.post(
                    f"{base}/tables/{table}/batch",
                    json={"inserts": SEED, "sync_level": "full_index"},
                    timeout=120,
                )
            )
            for name in INDEXES:
                wait_for_target(session, base, table, name)
            listed = checked(session.get(f"{base}/tables/{table}/indexes", timeout=30))
            statuses = {entry["config"]["name"]: entry["status"] for entry in listed}
            assert all(
                statuses[name]["readiness"]["queryable"] is True for name in INDEXES
            ), listed
            physical = checked(
                session.get(
                    f"{base}/tables/{table}/indexes/text%2Fmodel%2Fv1", timeout=30
                )
            )
            assert physical["config"]["name"] == "text/model/v1", physical
            swap(session, base, table, "space_a", "initial")
            print(exercise_swaps(session, base, table, rounds=3))
            server.pause(crash=True)
            server.resume()
            assert (
                checked(session.get(alias_url(base, table), timeout=30))["index"]
                == "space_a"
            )
            assert query(session, base, table) == "space_a"
            swap(session, base, table, "space_b", "after-restart", "space_a")
            assert query(session, base, table) == "space_b"
            # Durable objects must really exist; disposable process caches alone
            # cannot satisfy this test's storage oracle.
            listing = subprocess.run(
                [
                    aws,
                    "--endpoint-url",
                    endpoint,
                    "s3api",
                    "list-objects-v2",
                    "--bucket",
                    "aliases",
                ],
                env=env,
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert json.loads(listing.stdout).get("Contents"), listing.stdout
    finally:
        if server is not None:
            failed = sys.exc_info()[0] is not None
            if failed:
                print(server.debug_logs())
            server.stop(test_failed=failed)
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        ports.close()
        log.close()


@pytest.mark.ha_standby
def test_index_alias_replicates_restarts_and_survives_standby_promotion(ha_cluster):
    cluster = ha_cluster
    _bootstrap_empty(cluster)
    table = "standby_alias_cutover"
    primary = cluster.primary.url + DB_API_ROOT
    standby = cluster.standby.url + DB_API_ROOT
    with requests.Session() as session:
        checked(
            session.post(
                f"{primary}/tables/{table}",
                json={"num_shards": 1, "indexes": INDEXES},
                timeout=60,
            )
        )
        checked(
            session.post(
                f"{primary}/tables/{table}/batch",
                json={"inserts": SEED, "sync_level": "full_index"},
                timeout=60,
            )
        )
        for name in INDEXES:
            wait_for_target(session, primary, table, name)
        swap(
            session,
            primary + "/databases/default/namespaces/public",
            table,
            "space_a",
            "initial",
        )
        observed = session.get(alias_url(primary, table), timeout=30)
        checked(observed)
        checked(
            session.delete(
                alias_url(primary, table),
                headers={"If-Match": observed.headers["ETag"]},
                timeout=30,
            )
        )
        deleted_lsn = _primary_lsn(cluster)
        _wait_for_standby_applied(cluster, deleted_lsn, require_live_replication=True)
        assert session.get(alias_url(standby, table), timeout=30).status_code == 404
        swap(session, primary, table, "space_a", "recreated")
        # A locally committed receipt must not bypass RemoteApply on retry.
        cluster.standby.stop()
        pending = {
            "index": "space_b",
            "expected_index": "space_a",
            "idempotency_key": "outage-receipt",
            "require": "queryable",
        }
        try:
            for restart in (False, False, True):
                if restart:
                    cluster.primary.restart()
                response = session.put(
                    alias_url(primary, table), json=pending, timeout=30
                )
                assert (
                    response.headers.get("X-Antfly-Raft-Mutation-Outcome")
                    == "unknown-v1"
                ), response.text
                assert response.status_code == 503, response.text
                assert response.json()["error"] == "MetadataMutationOutcomeUnknown", (
                    response.text
                )
        finally:
            cluster.standby.start()
        _wait_for_standby_applied(
            cluster, _primary_lsn(cluster), require_live_replication=True
        )
        recovered = checked(
            session.put(alias_url(primary, table), json=pending, timeout=30)
        )
        assert recovered["index"] == "space_b", recovered
        swap(session, primary, table, "space_a", "after-outage", "space_b")
        committed = swap(
            session, primary, table, "space_b", "before-promotion", "space_a"
        )
        lsn = _primary_lsn(cluster)
        _wait_for_standby_applied(cluster, lsn, require_live_replication=True)
        _wait_for_standby_lookup(cluster, table, "0:a")
        cluster.standby.restart()
        _wait_for_standby_applied(cluster, lsn, require_live_replication=True)
        assert (
            checked(session.get(alias_url(standby, table), timeout=30))["index"]
            == "space_b"
        )
        fence = _promotion_fence_request(cluster, lsn)
        cluster.primary.admin_post("/fence", fence)
        cluster.standby.admin_post("/fence", fence)
        promoted = cluster.standby.admin_post("/promotion/current-fence", {})
        assert promoted["promotion"]["data_loss_possible"] is False
        assert (
            checked(session.get(alias_url(standby, table), timeout=30))["index"]
            == "space_b"
        )
        assert query(session, standby, table) == "space_b"
        assert (
            swap(session, standby, table, "space_b", "before-promotion", "space_a")
            == committed
        )
        # The old primary cannot acknowledge another alias mutation after its
        # authority has been fenced.
        rejected = session.put(
            alias_url(primary, table),
            json={
                "index": "space_a",
                "expected_index": "space_b",
                "idempotency_key": "stale-primary",
                "require": "queryable",
            },
            timeout=30,
        )
        assert rejected.status_code == 409, rejected.text
        assert rejected.json()["error"] in (
            "HAFencedPrimary",
            "HAPromotedStandbyRequiresPrimaryOpen",
        ), rejected.text
