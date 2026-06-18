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

"""Hot-standby HA E2E tests for the supported Zig swarm runtime."""

from __future__ import annotations

import json
import os
import signal
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import pytest
import requests

from conftest import (
    DEFAULT_ANTFLY_BIN,
    _read_log_tail,
    _swarm_stateful_command,
    find_free_port,
    maybe_preserve_tempdir,
    resolve_binary_path,
    wait_for_server,
)


HA_ADMIN_ROOT = "/admin/v1/ha"
DB_API_ROOT = "/db/v1"

pytestmark = pytest.mark.ha_standby


class HASwarmNode:
    def __init__(
        self,
        *,
        binary: str,
        root: Path,
        role: str,
        node_id: str,
        cluster_id: int,
        timeline_id: int,
        epoch: int,
        shard_id: int | None = None,
        table_id: int | None = None,
        upstream_url: str | None = None,
        slot_name: str | None = None,
    ):
        self.binary = binary
        self.root = root
        self.role = role
        self.node_id = node_id
        self.host = "127.0.0.1"
        self.port = find_free_port()
        self.health_port = find_free_port()
        self.url = f"http://{self.host}:{self.port}"
        self.log_path = self.root / f"{role}-{node_id}.log"
        self.log_file = self.log_path.open("a")
        self.cluster_id = cluster_id
        self.shard_id = shard_id
        self.table_id = table_id
        self.timeline_id = timeline_id
        self.epoch = epoch
        self.upstream_url = upstream_url
        self.slot_name = slot_name
        self.proc: subprocess.Popen[str] | None = None

    @property
    def node_root(self) -> Path:
        return self.root / self.node_id

    @property
    def ha_root(self) -> Path:
        return self.node_root / "ha"

    @property
    def catalog_path(self) -> Path:
        return self.node_root / "metadata" / "local-metadata.json"

    def start(self) -> None:
        self.node_root.mkdir(parents=True, exist_ok=True)
        command = _swarm_stateful_command(self.binary, host=self.host, port=self.port, root=self.node_root)
        command.extend(["--health-port", str(self.health_port)])
        if self.role == "primary":
            command.extend(
                [
                    "--ha-primary-log",
                    str(self.ha_root / "primary.log"),
                    "--ha-primary-slots",
                    str(self.ha_root / "primary-slots.wal"),
                    "--ha-primary-node-id",
                    self.node_id,
                ]
            )
        elif self.role == "standby":
            command.extend(
                [
                    "--ha-standby-log",
                    str(self.ha_root / "standby.log"),
                    "--ha-standby-progress",
                    str(self.ha_root / "standby-progress.wal"),
                    "--ha-standby-node-id",
                    self.node_id,
                ]
            )
            if self.upstream_url is not None:
                command.extend(["--ha-standby-upstream-url", self.upstream_url])
            if self.slot_name is not None:
                command.extend(["--ha-standby-slot", self.slot_name])
        else:
            raise ValueError(f"unsupported HA role {self.role!r}")

        command.extend(
            [
                "--ha-fence-wal",
                str(self.ha_root / "fence.wal"),
                "--ha-cluster-id",
                str(self.cluster_id),
            ]
        )
        if self.shard_id is not None:
            command.extend(["--ha-shard-id", str(self.shard_id)])
        if self.table_id is not None:
            command.extend(["--ha-table-id", str(self.table_id)])
        command.extend(
            [
                "--ha-timeline-id",
                str(self.timeline_id),
                "--ha-epoch",
                str(self.epoch),
            ]
        )

        self.proc = subprocess.Popen(
            command,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            cwd=self.root,
        )
        if not wait_for_server(self.url, path="/status", timeout=30.0):
            logs = self.debug_logs()
            self.stop()
            raise RuntimeError(f"HA {self.role} node failed to start at {self.url}\n{logs}")

    def reset_ha_state(self) -> None:
        self.stop()
        if self.ha_root.exists():
            shutil.rmtree(self.ha_root)

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None

    def restart(self) -> None:
        self.stop()
        self.log_file.close()
        self.log_file = self.log_path.open("a")
        self.start()

    def close(self) -> None:
        self.stop()
        self.log_file.close()

    def debug_logs(self) -> str:
        self.log_file.flush()
        return _read_log_tail(self.log_path)

    def admin_get(self, path: str, **params: Any) -> dict[str, Any]:
        response = requests.get(f"{self.url}{HA_ADMIN_ROOT}{path}", params=params, timeout=10)
        return self._check(response)

    def admin_post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = requests.post(f"{self.url}{HA_ADMIN_ROOT}{path}", json=payload, timeout=10)
        return self._check(response)

    def create_table(self, table_name: str) -> dict[str, Any]:
        response = requests.post(
            f"{self.url}{DB_API_ROOT}/tables/{table_name}",
            json={"num_shards": 1},
            timeout=30,
        )
        return self._check(response)

    def batch_write(self, table_name: str, inserts: dict[str, dict[str, Any]]) -> dict[str, Any]:
        response = self.batch_write_response(table_name, inserts)
        return self._check(response)

    def batch_write_response(self, table_name: str, inserts: dict[str, dict[str, Any]]) -> requests.Response:
        return requests.post(
            f"{self.url}{DB_API_ROOT}/tables/{table_name}/batch",
            json={"inserts": inserts},
            timeout=30,
        )

    def _check(self, response: requests.Response) -> dict[str, Any]:
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"{response.status_code} {response.reason} for {response.request.method} {response.url}\n"
                f"[body]\n{response.text}\n[logs]\n{self.debug_logs()}",
                response=response,
            )
        return response.json()


class HACluster:
    def __init__(self, binary: str):
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-ha-standby-e2e-")
        self.root = Path(self.tempdir.name)
        self.primary = HASwarmNode(
            binary=binary,
            root=self.root,
            role="primary",
            node_id="primary-a",
            cluster_id=100,
            timeline_id=1,
            epoch=1,
        )
        self.standby = HASwarmNode(
            binary=binary,
            root=self.root,
            role="standby",
            node_id="standby-a",
            cluster_id=100,
            timeline_id=1,
            epoch=1,
            upstream_url=self.primary.url,
            slot_name="standby-a",
        )

    def configure_table_identity(self, *, shard_id: int, table_id: int) -> None:
        for node in (self.primary, self.standby):
            node.shard_id = shard_id
            node.table_id = table_id

    def seed_standby_catalog_from_primary(self) -> None:
        self.standby.node_root.mkdir(parents=True, exist_ok=True)
        self.standby.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.primary.catalog_path, self.standby.catalog_path)

    def close(self) -> None:
        self.standby.close()
        self.primary.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()

    def debug_logs(self) -> str:
        return f"[primary]\n{self.primary.debug_logs()}\n[standby]\n{self.standby.debug_logs()}"


@pytest.fixture
def ha_cluster() -> HACluster:
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Antfly binary not found: {binary}")
    if Path(binary).name != "antfly":
        pytest.skip("HA standby e2e requires the supported Zig antfly binary")
    if not _binary_supports_ha_swarm(binary):
        pytest.skip(f"Antfly binary does not expose HA swarm flags; rebuild current Zig binary: {binary}")

    cluster = HACluster(binary)
    try:
        yield cluster
    finally:
        cluster.close()


def _wait_for_standby_applied(cluster: HACluster, lsn: int, *, timeout_s: float = 20.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last_snapshot: dict[str, Any] | None = None
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            status = cluster.standby.admin_get("/standby/status", upstream_lsn=lsn)
        except requests.RequestException as err:
            last_error = err
            time.sleep(0.25)
            continue
        snapshot = status["snapshot"]
        last_snapshot = snapshot
        if snapshot["received_lsn"] >= lsn and snapshot["applied_lsn"] >= lsn:
            return snapshot
        time.sleep(0.25)
    raise AssertionError(
        f"standby did not apply through LSN {lsn}; last={last_snapshot}; last_error={last_error}\n"
        f"{cluster.debug_logs()}"
    )


def _primary_lsn(cluster: HACluster) -> int:
    status = cluster.primary.admin_get("/primary/status")
    return int(status["snapshot"]["current_lsn"])


def _table_identity_from_catalog(node: HASwarmNode, table_name: str) -> tuple[int, int]:
    catalog = json.loads(node.catalog_path.read_text())
    table = next(table for table in catalog["tables"] if table["name"] == table_name)
    table_id = int(table["table_id"])
    table_range = next(record for record in catalog["ranges"] if int(record["table_id"]) == table_id)
    return int(table_range["group_id"]), table_id


def _identity(cluster: HACluster) -> dict[str, int]:
    assert cluster.primary.shard_id is not None
    assert cluster.primary.table_id is not None
    return {
        "cluster_id": cluster.primary.cluster_id,
        "shard_id": cluster.primary.shard_id,
        "table_id": cluster.primary.table_id,
        "timeline_id": cluster.primary.timeline_id,
        "epoch": cluster.primary.epoch,
    }


def _promotion_fence_request(cluster: HACluster, required_lsn: int) -> dict[str, Any]:
    return {
        "identity": _identity(cluster),
        "old_primary_id": cluster.primary.node_id,
        "promoted_node_id": cluster.standby.node_id,
        "new_timeline_id": cluster.primary.timeline_id + 1,
        "new_epoch": cluster.primary.epoch + 1,
        "required_lsn": required_lsn,
        "observed_lsn": required_lsn,
        "force": False,
        "reason": "ha-standby-e2e",
    }


def _binary_supports_ha_swarm(binary: str) -> bool:
    result = subprocess.run(
        [binary, "swarm", "--help"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10,
        check=False,
    )
    return "--ha-primary-log" in result.stdout and "--ha-standby-log" in result.stdout


def test_standby_streams_public_writes_restarts_and_rejects_writes(ha_cluster: HACluster):
    table_name = "ha_standby_docs"
    ha_cluster.primary.start()
    created = ha_cluster.primary.create_table(table_name)
    assert created["name"] == table_name
    shard_id, table_id = _table_identity_from_catalog(ha_cluster.primary, table_name)
    ha_cluster.primary.reset_ha_state()
    ha_cluster.configure_table_identity(shard_id=shard_id, table_id=table_id)
    ha_cluster.seed_standby_catalog_from_primary()
    ha_cluster.primary.start()

    created = ha_cluster.primary.admin_post(
        "/replication-slots",
        {"slot_name": "standby-a", "initial_lsn": 0},
    )
    assert created["slot"]["slot_name"] == "standby-a"
    assert created["slot"]["restart_lsn"] == 0

    ha_cluster.standby.start()

    ha_cluster.primary.batch_write(table_name, {"doc:first": {"title": "first"}})
    first_lsn = _primary_lsn(ha_cluster)
    assert first_lsn >= 1
    first_snapshot = _wait_for_standby_applied(ha_cluster, first_lsn)
    assert first_snapshot["role"] == "standby"
    assert first_snapshot["received_lsn"] >= first_lsn
    assert first_snapshot["applied_lsn"] >= first_lsn
    assert first_snapshot["caught_up_to_received"] is True
    read_check = ha_cluster.standby.admin_post(
        "/read/check",
        {"consistency": "stale_ok", "required_lsn": first_lsn},
    )
    assert read_check["decision"]["action"] == "serve_standby"
    assert read_check["decision"]["serve_lsn"] >= first_lsn

    write_check = ha_cluster.standby.admin_post("/write/check", {"role": "standby"})
    assert write_check["decision"]["action"] == "reject_read_only_standby"

    ha_cluster.standby.restart()
    restarted_snapshot = _wait_for_standby_applied(ha_cluster, first_lsn)
    assert restarted_snapshot["received_lsn"] >= first_lsn
    assert restarted_snapshot["applied_lsn"] >= first_lsn

    ha_cluster.primary.batch_write(table_name, {"doc:second": {"title": "second"}})
    second_lsn = _primary_lsn(ha_cluster)
    assert second_lsn > first_lsn
    second_snapshot = _wait_for_standby_applied(ha_cluster, second_lsn)
    assert second_snapshot["received_lsn"] >= second_lsn
    assert second_snapshot["applied_lsn"] >= second_lsn
    second_read_check = ha_cluster.standby.admin_post(
        "/read/check",
        {"consistency": "stale_ok", "required_lsn": second_lsn},
    )
    assert second_read_check["decision"]["action"] == "serve_standby"
    assert second_read_check["decision"]["serve_lsn"] >= second_lsn

    primary_status = ha_cluster.primary.admin_get("/primary/status")
    slot = next(
        slot
        for slot in primary_status["snapshot"]["slots"]
        if slot.get("slot_name", slot.get("name")) == "standby-a"
    )
    assert slot["received_lsn"] >= second_lsn
    assert slot["applied_lsn"] >= second_lsn

    fence_request = _promotion_fence_request(ha_cluster, second_lsn)
    fence = ha_cluster.standby.admin_post("/fence", fence_request)
    assert fence["receipt"]["promoted_node_id"] == "standby-a"
    assert fence["receipt"]["old_primary_id"] == "primary-a"
    assert fence["receipt"]["new_timeline_id"] == 2

    assessment = ha_cluster.standby.admin_post(
        "/promotion/assess",
        {"required_lsn": second_lsn, "fencing_confirmed": False, "force": False, "use_current_fence": True},
    )
    assert assessment["assessment"]["can_promote"] is True
    assert assessment["assessment"]["fencing_confirmed"] is True

    promoted = ha_cluster.standby.admin_post("/promotion/current-fence", {})
    assert promoted["promotion"]["node_id"] == "standby-a"
    assert promoted["promotion"]["new_identity"]["timeline_id"] == 2
    assert promoted["promotion"]["new_identity"]["epoch"] == 2
    assert promoted["promotion"]["switch_lsn"] == second_lsn + 1

    primary_fence = ha_cluster.primary.admin_post("/fence", fence_request)
    assert primary_fence["receipt"]["promoted_node_id"] == "standby-a"
    fenced_write_check = ha_cluster.primary.admin_post(
        "/write/check",
        {"role": "primary", "expected_identity": _identity(ha_cluster)},
    )
    assert fenced_write_check["decision"]["role"] == "fenced_primary"
    assert fenced_write_check["decision"]["action"] == "reject_fenced_primary"

    rejected = ha_cluster.primary.batch_write_response(
        table_name,
        {"doc:old-primary": {"title": "must not commit"}},
    )
    assert rejected.status_code >= 400, rejected.text
