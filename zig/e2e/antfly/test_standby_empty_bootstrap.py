# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
# the Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Bootstrap an empty instance through the production portable seed pipeline."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
from test_standby import (
    DB_API_ROOT,
    HACluster,
    _primary_lsn,
    _promotion_fence_request,
    _wait_for_standby_applied,
    _wait_for_standby_lookup,
)
from test_standby import (
    ha_cluster as ha_cluster,  # noqa: PLC0414 - export pytest fixture
)

pytestmark = pytest.mark.ha_standby


def _bootstrap_empty(cluster: HACluster) -> None:
    # Explicit zero identities select the whole-instance stream and engage
    # the continuous mutation guard. The fixture also enables RemoteApply
    # with one required standby and a blocking failure policy.
    cluster.configure_table_identity(shard_id=0, table_id=0)
    # Match the operator, which preloads the sync policy on the standby for
    # promotion. Standby create rejection must not depend on an async policy.
    cluster.standby.extra_runtime_args = [
        "--hot-standby-sync-mode",
        "remote_apply",
        "--hot-standby-sync-selection",
        "first",
        "--hot-standby-sync-required",
        "1",
        "--hot-standby-sync-standby",
        "standby-a",
        "--hot-standby-sync-failure",
        "block",
    ]
    capture_root = cluster.primary.ha_root / "seed-captures"
    cluster.primary.extra_runtime_args = [
        "--hot-standby-seed-capture-root",
        str(capture_root),
    ]
    cluster.primary.start()
    binding = {
        "topology_id": "empty-cloud-instance",
        "topology_generation": 1,
        "node_id": cluster.standby.node_id,
        "target_pvc_name": "standby-data",
        "target_pvc_uid": "standby-data-incarnation-1",
    }
    generation = "empty-seed-1"
    capture = cluster.primary.admin_post(
        "/base-backups/capture",
        {"slot_name": "standby-a", "generation": generation, **binding},
    )
    topology = json.loads((Path(capture["content_root"]) / "TOPOLOGY.json").read_text())
    assert topology["catalog"]["epoch"] > 0
    assert topology["catalog"]["tables"] == []
    assert topology["catalog"]["ranges"] == []
    assert topology["replicas"] == []

    def flags(values):
        return [
            part
            for key, value in values.items()
            for part in ("--" + key.replace("_", "-"), str(value))
        ]

    common = {
        "generation": generation,
        "slot": "standby-a",
        "capture_receipt_sha256": capture["capture_receipt_sha256"],
        **binding,
    }
    identity = {
        "ha_cluster_id": cluster.primary.cluster_id,
        "ha_shard_id": 0,
        "ha_table_id": 0,
        "ha_timeline_id": cluster.primary.timeline_id,
        "ha_epoch": cluster.primary.epoch,
    }

    def artifact(action, **values):
        result = subprocess.run(
            [
                cluster.primary.binary,
                "standby",
                "artifact",
                action,
                *flags({**common, **values}),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    # Use the same file-backed object-store transport supported by operator
    # fixtures. Never copy the primary's live catalog into the standby.
    location = (cluster.root / "object-store").as_uri()
    artifact(
        "publish",
        location=location,
        manifest=capture["manifest_path"],
        content_root=capture["content_root"],
        capture_receipt=Path(capture["generation_root"]) / "COMPLETE.json",
    )
    staging = cluster.root / "standby-staging"
    target = cluster.standby.node_root / "standby-generations"
    artifact("restore", location=location, staging_root=staging, **identity)
    activated = artifact(
        "activate",
        staging_root=staging,
        target_root=target,
        target_local_node_id=1,
        target_replica_id=1,
        **identity,
    )
    # Repeating activation must reuse the exact published generation.
    repeated = artifact(
        "activate",
        staging_root=staging,
        target_root=target,
        target_local_node_id=1,
        target_replica_id=1,
        **identity,
    )
    assert repeated["generation_path"] == activated["generation_path"]

    startup = {
        "target_root": target,
        "generation": generation,
        "slot_name": "standby-a",
        "timeline_id": 1,
        "epoch": 1,
        **{key: value for key, value in binding.items() if key != "node_id"},
        **{
            key: activated[key]
            for key in (
                "capture_receipt_sha256",
                "materialized_receipt_sha256",
                "materialized_aggregate_sha256",
                "target_local_node_id",
                "target_replica_id",
            )
        },
    }
    cluster.standby.extra_runtime_args += flags(
        {"hot_standby_startup_" + key: value for key, value in startup.items()}
    )
    cluster.primary.admin_post(
        "/base-backups/activate",
        {
            key: activated[key]
            for key in (
                "slot_name",
                "generation",
                "manifest_id",
                "timeline_id",
                "checkpoint_lsn",
                "seed_receipt_sha256",
                "capture_receipt_sha256",
                "manifest_sha256",
                "aggregate_sha256",
            )
        },
    )
    cluster.standby.start()
    _wait_for_standby_applied(
        cluster, _primary_lsn(cluster), require_live_replication=True
    )


def test_empty_seed_artifact_bootstrap_and_restart(ha_cluster: HACluster):
    cluster = ha_cluster
    _bootstrap_empty(cluster)
    cluster.standby.restart()
    snapshot = _wait_for_standby_applied(
        cluster, _primary_lsn(cluster), require_live_replication=True
    )
    assert snapshot["last_error"] in (None, "")


def test_empty_seed_then_first_table_replication_and_fenced_promotion(
    ha_cluster: HACluster,
):
    # Table catalog and document effects must survive the same stream.
    cluster = ha_cluster
    _bootstrap_empty(cluster)
    cluster.primary.create_table("first_table")
    cluster.primary.batch_write(
        "first_table", {"first": {"title": "created after empty bootstrap"}}
    )
    cluster.primary.create_table("second_table")
    cluster.primary.batch_write(
        "second_table", {"second": {"title": "independent table"}}
    )
    lsn = _primary_lsn(cluster)
    _wait_for_standby_applied(cluster, lsn)
    _wait_for_standby_lookup(cluster, "second_table", "second")
    cluster.primary.restart()
    assert (
        cluster.primary.lookup_key("second_table", "second")["title"]
        == "independent table"
    )
    _wait_for_standby_lookup(cluster, "first_table", "first")
    cluster.standby.restart()
    _wait_for_standby_applied(cluster, lsn, require_live_replication=True)
    _wait_for_standby_lookup(cluster, "first_table", "first")

    _wait_for_standby_lookup(cluster, "second_table", "second")
    fence = _promotion_fence_request(cluster, lsn)
    # Fence the old primary before authorizing the standby to write.
    cluster.primary.admin_post("/fence", fence)
    cluster.standby.admin_post("/fence", fence)
    promotion = cluster.standby.admin_post("/promotion/current-fence", {})
    assert promotion["promotion"]["data_loss_possible"] is False
    stale = cluster.primary.batch_write_response(
        "first_table", {"stale": {"title": "must fail"}}
    )
    assert stale.status_code >= 400
    assert (
        cluster.standby.lookup_key("first_table", "first")["title"]
        == "created after empty bootstrap"
    )
    # Authority transfer alone must not acknowledge writes without a new
    # synchronous replica, even with the future-primary policy preconfigured.
    promoted_write = cluster.standby.batch_write_response(
        "first_table", {"unprotected": {"title": "must not acknowledge"}}
    )
    assert promoted_write.status_code == 503
    assert promoted_write.text == (
        "write committed locally; standby durability acknowledgment pending"
    )


def test_catalog_remote_apply_outage_recovers_without_primary_restart(
    ha_cluster: HACluster,
):
    cluster = ha_cluster
    _bootstrap_empty(cluster)
    primary_pid = cluster.primary.proc.pid
    cluster.standby.stop()
    response = cluster.primary._request(
        "POST",
        f"{cluster.primary.url}{DB_API_ROOT}/tables/pending_table",
        json={"num_shards": 1},
        timeout=30,
    )
    assert response.status_code >= 400, response.text
    assert "outcome is unknown" in response.text, response.text
    assert response.headers.get("X-Antfly-Raft-Mutation-Outcome") == "unknown-v1"
    # The table is visible locally, but neither a retry nor a primary restart
    # may turn that visibility into an acknowledged AlreadyExists response.
    for restart in (False, True):
        if restart:
            cluster.primary.restart()
            primary_pid = cluster.primary.proc.pid
        retry = cluster.primary._request(
            "POST",
            f"{cluster.primary.url}{DB_API_ROOT}/tables/pending_table",
            json={"num_shards": 1},
            timeout=30,
        )
        assert retry.status_code == 409, retry.text
        assert retry.headers.get("X-Antfly-Raft-Mutation-Outcome") == "unknown-v1", (
            retry.text
        )
        assert "outcome is unknown" in retry.text, retry.text
    # Creation is durable but unacknowledged. Replication catches up without
    # restarting the primary when its required standby becomes available.
    cluster.standby.start()
    _wait_for_standby_applied(
        cluster, _primary_lsn(cluster), require_live_replication=True
    )
    retry = cluster.primary._request(
        "POST",
        f"{cluster.primary.url}{DB_API_ROOT}/tables/pending_table",
        json={"num_shards": 1},
        timeout=30,
    )
    assert retry.status_code == 409, retry.text
    assert retry.text == "table already exists"
    assert retry.headers.get("X-Antfly-Raft-Mutation-Outcome") is None
    cluster.primary.batch_write(
        "pending_table", {"recovered": {"title": "replayed catalog"}}
    )
    _wait_for_standby_lookup(cluster, "pending_table", "recovered")
    cluster.primary.create_table("after_outage")
    assert cluster.primary.proc.pid == primary_pid
    cluster.primary.restart()
    assert (
        cluster.primary.lookup_key("pending_table", "recovered")["title"]
        == "replayed catalog"
    )


def test_catalog_replays_when_local_snapshot_lags_wal(ha_cluster: HACluster, tmp_path):
    cluster = ha_cluster
    _bootstrap_empty(cluster)
    cluster.primary.stop()
    catalog_store = cluster.primary.catalog_path.with_suffix(
        cluster.primary.catalog_path.suffix + ".store"
    )
    before = tmp_path / "before-catalog.store"
    shutil.copytree(catalog_store, before)
    cluster.primary.start()
    cluster.primary.create_table("replay_table")
    cluster.primary.batch_write("replay_table", {"saved": {"title": "durable data"}})
    # Model the startup state after WAL durability but before local catalog
    # publication. Only the disposable fixture catalog is rolled back.
    cluster.primary.stop()
    shutil.rmtree(catalog_store)
    shutil.copytree(before, catalog_store)
    cluster.primary.start()
    assert (
        cluster.primary.lookup_key("replay_table", "saved")["title"] == "durable data"
    )
    _wait_for_standby_lookup(cluster, "replay_table", "saved")
    for method, suffix in (("DELETE", ""), ("PUT", "/schema"), ("POST", "/unknown")):
        response = cluster.primary._request(
            method,
            f"{cluster.primary.url}{DB_API_ROOT}/tables/replay_table{suffix}",
            json={},
            timeout=10,
        )
        assert response.status_code >= 400, response.text
    response = cluster.standby._request(
        "POST",
        f"{cluster.standby.url}{DB_API_ROOT}/tables/standby_local",
        json={"num_shards": 1},
        timeout=10,
    )
    assert response.status_code >= 400, response.text
