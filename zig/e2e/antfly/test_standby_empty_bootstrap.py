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
import subprocess
from pathlib import Path

import pytest

from test_standby import (
    HACluster,
    _primary_lsn,
    _promotion_fence_request,
    _wait_for_standby_applied,
    _wait_for_standby_lookup,
    ha_cluster,
)

pytestmark = pytest.mark.ha_standby


def _bootstrap_empty(cluster: HACluster) -> None:
    # Explicit zero identities select the whole-instance stream and engage
    # the continuous mutation guard. The fixture also enables RemoteApply
    # with one required standby and a blocking failure policy.
    cluster.configure_table_identity(shard_id=0, table_id=0)
    capture_root = cluster.primary.ha_root / "seed-captures"
    cluster.primary.extra_runtime_args = ["--hot-standby-seed-capture-root", str(capture_root)]
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
        "ha_cluster_id": 100,
        "ha_shard_id": 0,
        "ha_table_id": 0,
        "ha_timeline_id": 1,
        "ha_epoch": 1,
    }

    def artifact(action, **values):
        result = subprocess.run(
            [cluster.primary.binary, "standby", "artifact", action, *flags({**common, **values})],
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
        "publish", location=location, manifest=capture["manifest_path"],
        content_root=capture["content_root"],
        capture_receipt=Path(capture["generation_root"]) / "COMPLETE.json",
    )
    staging = cluster.root / "standby-staging"
    target = cluster.standby.node_root / "standby-generations"
    artifact("restore", location=location, staging_root=staging, **identity)
    activated = artifact(
        "activate", staging_root=staging, target_root=target,
        target_local_node_id=1, target_replica_id=1, **identity,
    )
    # Repeating activation must reuse the exact published generation.
    repeated = artifact(
        "activate", staging_root=staging, target_root=target,
        target_local_node_id=1, target_replica_id=1, **identity,
    )
    assert repeated["generation_path"] == activated["generation_path"]

    startup = {
        "target_root": target, "generation": generation, "slot_name": "standby-a",
        "timeline_id": 1, "epoch": 1,
        **{key: value for key, value in binding.items() if key != "node_id"},
        **{key: activated[key] for key in (
            "capture_receipt_sha256", "materialized_receipt_sha256",
            "materialized_aggregate_sha256", "target_local_node_id", "target_replica_id",
        )},
    }
    cluster.standby.extra_runtime_args = flags(
        {"hot_standby_startup_" + key: value for key, value in startup.items()}
    )
    cluster.primary.admin_post("/base-backups/activate", {
        key: activated[key] for key in (
            "slot_name", "generation", "manifest_id", "timeline_id", "checkpoint_lsn",
            "seed_receipt_sha256", "capture_receipt_sha256", "manifest_sha256", "aggregate_sha256",
        )
    })
    cluster.standby.start()
    _wait_for_standby_applied(cluster, _primary_lsn(cluster), require_live_replication=True)


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
    # Acceptance test for selectable Cloud hot standby. Currently fails at
    # create_table: standalone catalog mutations are not yet replicated.
    cluster = ha_cluster
    _bootstrap_empty(cluster)
    cluster.primary.create_table("first_table")
    cluster.primary.batch_write(
        "first_table", {"first": {"title": "created after empty bootstrap"}}
    )
    lsn = _primary_lsn(cluster)
    _wait_for_standby_applied(cluster, lsn)
    _wait_for_standby_lookup(cluster, "first_table", "first")
    cluster.standby.restart()
    _wait_for_standby_applied(cluster, lsn, require_live_replication=True)
    _wait_for_standby_lookup(cluster, "first_table", "first")

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
    assert cluster.standby.lookup_key("first_table", "first")["title"] == "created after empty bootstrap"
