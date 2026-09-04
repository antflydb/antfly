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

"""Hot-standby HA E2E tests for the supported Zig standalone runtime."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import struct
import subprocess
import tempfile
import time
import zlib
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import pytest
import requests
from conftest import (
    DEFAULT_ANTFLY_BIN,
    _read_log_tail,
    _standalone_stateful_command,
    lookup_key_path,
    maybe_preserve_tempdir,
    resolve_binary_path,
    wait_for_server,
)
from port_reservations import LoopbackPortReservations

HA_ADMIN_ROOT = "/admin/v1/ha"
DB_API_ROOT = "/db/v1"
HA_BACKUP_MAGIC = b"AFHABKP\n"
HA_BACKUP_HEADER_SIZE = 96
HA_BACKUP_ENTRY_HEADER_SIZE = 28
HA_BACKUP_FORMAT_VERSION = 1
HA_BACKUP_FILE_KIND_METADATA = 3
HA_TRANSITION_BUSY_BODY = b"HAStateTransitionBusy"
HA_TRANSITION_RETRY_TIMEOUT_S = 20.0
HA_TRANSITION_RETRY_INTERVAL_S = 0.1

pytestmark = pytest.mark.ha_standby


def _is_ha_transition_busy(response: requests.Response) -> bool:
    return response.status_code == 503 and response.content == HA_TRANSITION_BUSY_BODY


class HAStandaloneNode:
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
        sync_standby_name: str | None = None,
        admin_token_env: str | None = None,
        admin_token: str | None = None,
    ):
        self.binary = binary
        self.root = root
        self.role = role
        self.node_id = node_id
        self.host = "127.0.0.1"
        with ExitStack() as setup:
            self.port_reservations = LoopbackPortReservations(self.host)
            setup.callback(self.port_reservations.close)
            self.port, self.health_port = self.port_reservations.reserve_many(2)
            self.url = f"http://{self.host}:{self.port}"
            self.log_path = self.root / f"{role}-{node_id}.log"
            self.log_file = setup.enter_context(self.log_path.open("a"))
            setup.pop_all()
        self.cluster_id = cluster_id
        self.shard_id = shard_id
        self.table_id = table_id
        self.timeline_id = timeline_id
        self.epoch = epoch
        self.upstream_url = upstream_url
        self.slot_name = slot_name
        self.sync_standby_name = sync_standby_name
        self.admin_token_env = admin_token_env
        self.admin_token = admin_token
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

    def start(self, *, enable_replication: bool = True) -> None:
        self.node_root.mkdir(parents=True, exist_ok=True)
        command = _standalone_stateful_command(
            self.binary, host=self.host, port=self.port, root=self.node_root
        )
        command.extend(["--health", "true", "--health-port", str(self.health_port)])
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
            if enable_replication and self.upstream_url is not None:
                command.extend(["--ha-standby-upstream-url", self.upstream_url])
            if enable_replication and self.slot_name is not None:
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
        if (
            self.role == "primary"
            and self.sync_standby_name is not None
            and self.table_id is not None
        ):
            command.extend(
                [
                    "--ha-sync-mode",
                    "remote_apply",
                    "--ha-sync-selection",
                    "first",
                    "--ha-sync-required",
                    "1",
                    "--ha-sync-standby",
                    self.sync_standby_name,
                    "--ha-sync-failure",
                    "block",
                ]
            )
        command.extend(
            [
                "--ha-timeline-id",
                str(self.timeline_id),
                "--ha-epoch",
                str(self.epoch),
            ]
        )
        env = os.environ.copy()
        if self.admin_token_env is not None:
            command.extend(["--admin-token-env", self.admin_token_env])
            assert self.admin_token is not None
            env[self.admin_token_env] = self.admin_token

        self.proc = self.port_reservations.handoff_to(
            (self.port, self.health_port),
            lambda: subprocess.Popen(
                command,
                stdout=self.log_file,
                stderr=subprocess.STDOUT,
                cwd=self.root,
                env=env,
            ),
        )
        if not wait_for_server(self.url, path="/readyz", timeout=30.0):
            logs = self.debug_logs()
            self.stop()
            raise RuntimeError(
                f"HA {self.role} node failed to start at {self.url}\n{logs}"
            )

    def reset_ha_state(self) -> None:
        self.stop()
        if self.ha_root.exists():
            shutil.rmtree(self.ha_root)

    def _stop_process(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None

    def stop(self) -> None:
        self._stop_process()
        self.port_reservations.ensure_reserved(self.port, self.health_port)

    def restart(self, *, enable_replication: bool = True) -> None:
        self.stop()
        self.log_file.close()
        self.log_file = self.log_path.open("a")
        self.start(enable_replication=enable_replication)

    def close(self) -> None:
        self._stop_process()
        self.port_reservations.close()
        self.log_file.close()

    def debug_logs(self) -> str:
        self.log_file.flush()
        return _read_log_tail(self.log_path)

    def admin_headers(self) -> dict[str, str] | None:
        if self.admin_token is None:
            return None
        return {"Authorization": f"Bearer {self.admin_token}"}

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        try:
            return requests.request(method, url, **kwargs)
        except requests.RequestException as err:
            exit_code = self.proc.poll() if self.proc is not None else None
            err.add_note(
                f"HA {self.role} request failed; process_exit_code={exit_code}\n"
                f"[logs]\n{self.debug_logs()}"
            )
            raise

    def admin_get_response(self, path: str, **params: Any) -> requests.Response:
        return self._request(
            "GET",
            f"{self.url}{HA_ADMIN_ROOT}{path}",
            params=params,
            headers=self.admin_headers(),
            timeout=10,
        )

    def admin_post_response(
        self, path: str, payload: dict[str, Any]
    ) -> requests.Response:
        return self._request(
            "POST",
            f"{self.url}{HA_ADMIN_ROOT}{path}",
            json=payload,
            headers=self.admin_headers(),
            timeout=10,
        )

    def admin_get(self, path: str, **params: Any) -> dict[str, Any]:
        deadline = time.monotonic() + HA_TRANSITION_RETRY_TIMEOUT_S
        while True:
            response = self.admin_get_response(path, **params)
            if not _is_ha_transition_busy(response):
                return self._check(response)
            if time.monotonic() >= deadline:
                return self._check(response)
            # HA GETs are observations. The runtime deliberately sheds them
            # while a role transition owns the state mutex, so retry the exact
            # transient response without weakening any other failure signal.
            time.sleep(HA_TRANSITION_RETRY_INTERVAL_S)

    def admin_post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        deadline = time.monotonic() + HA_TRANSITION_RETRY_TIMEOUT_S
        while True:
            response = self.admin_post_response(path, payload)
            if not _is_ha_transition_busy(response):
                return self._check(response)
            if time.monotonic() >= deadline:
                return self._check(response)
            # This exact response is emitted before route dispatch when the HA
            # state mutex is owned, so no mutation has occurred and retrying is
            # safe. Do not retry arbitrary 503 responses from route handlers.
            time.sleep(HA_TRANSITION_RETRY_INTERVAL_S)

    def create_table(self, table_name: str) -> dict[str, Any]:
        response = self._request(
            "POST",
            f"{self.url}{DB_API_ROOT}/tables/{table_name}",
            json={"num_shards": 1},
            timeout=30,
        )
        return self._check(response)

    def batch_write(
        self, table_name: str, inserts: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        response = self.batch_write_response(table_name, inserts)
        return self._check(response)

    def batch_write_response(
        self, table_name: str, inserts: dict[str, dict[str, Any]]
    ) -> requests.Response:
        return self._request(
            "POST",
            f"{self.url}{DB_API_ROOT}/tables/{table_name}/batch",
            json={"inserts": inserts},
            timeout=30,
        )

    def lookup_key(
        self, table_name: str, key: str, *, consistency: str | None = None
    ) -> dict[str, Any]:
        params = {"consistency": consistency} if consistency is not None else None
        response = self._request(
            "GET",
            f"{self.url}{DB_API_ROOT}{lookup_key_path(table_name, key)}",
            params=params,
            timeout=30,
        )
        return self._check(response)

    def _check(self, response: requests.Response) -> dict[str, Any]:
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"{response.status_code} {response.reason} for {response.request.method} {response.url}\n"
                f"[body]\n{response.text}\n[logs]\n{self.debug_logs()}",
                response=response,
            )
        return response.json()


def _test_response(status_code: int, body: bytes) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = body
    response.request = requests.Request(
        "POST", f"http://127.0.0.1{HA_ADMIN_ROOT}/test"
    ).prepare()
    response.url = response.request.url
    return response


def test_admin_post_retries_exact_pre_dispatch_transition_busy(
    monkeypatch: pytest.MonkeyPatch,
):
    node = object.__new__(HAStandaloneNode)
    responses = iter(
        [
            _test_response(503, HA_TRANSITION_BUSY_BODY),
            _test_response(200, b'{"result":"ok"}'),
        ]
    )
    attempts = 0

    def next_response(_path: str, _payload: dict[str, Any]) -> requests.Response:
        nonlocal attempts
        attempts += 1
        return next(responses)

    node.admin_post_response = next_response
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    assert node.admin_post("/test", {}) == {"result": "ok"}
    assert attempts == 2


def test_admin_post_does_not_retry_ambiguous_service_unavailable():
    node = object.__new__(HAStandaloneNode)
    attempts = 0

    def unavailable(_path: str, _payload: dict[str, Any]) -> requests.Response:
        nonlocal attempts
        attempts += 1
        return _test_response(503, b"upstream unavailable")

    node.admin_post_response = unavailable
    node.debug_logs = lambda: "test logs"

    with pytest.raises(requests.HTTPError, match="upstream unavailable"):
        node.admin_post("/test", {})
    assert attempts == 1


class HACluster:
    def __init__(self, binary: str):
        with ExitStack() as setup:
            self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-ha-standby-e2e-")
            setup.callback(self.tempdir.cleanup)
            self.root = Path(self.tempdir.name).resolve()
            self.admin_token_env = "ANTFLY_HA_E2E_ADMIN_TOKEN"
            self.admin_token = "ha-e2e-secret-token"
            self.primary = HAStandaloneNode(
                binary=binary,
                root=self.root,
                role="primary",
                node_id="primary-a",
                cluster_id=100,
                timeline_id=1,
                epoch=1,
                sync_standby_name="standby-a",
                admin_token_env=self.admin_token_env,
                admin_token=self.admin_token,
            )
            setup.callback(self.primary.close)
            self.standby = HAStandaloneNode(
                binary=binary,
                root=self.root,
                role="standby",
                node_id="standby-a",
                cluster_id=100,
                timeline_id=1,
                epoch=1,
                upstream_url=self.primary.url,
                slot_name="standby-a",
                admin_token_env=self.admin_token_env,
                admin_token=self.admin_token,
            )
            setup.callback(self.standby.close)
            setup.pop_all()

    def configure_table_identity(self, *, shard_id: int, table_id: int) -> None:
        for node in (self.primary, self.standby):
            node.shard_id = shard_id
            node.table_id = table_id

    def seed_standby_catalog_from_primary(self) -> dict[str, Any]:
        backup_root = self.root / "base-backup"
        if backup_root.exists():
            shutil.rmtree(backup_root)
        catalog_rel = Path("metadata") / "local-metadata.json"
        catalog_backup_path = backup_root / catalog_rel
        catalog_backup_path.parent.mkdir(parents=True, exist_ok=True)
        catalog_bytes = self.primary.catalog_path.read_bytes()
        catalog_backup_path.write_bytes(catalog_bytes)

        self.standby.node_root.mkdir(parents=True, exist_ok=True)
        self.standby.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.primary.catalog_path, self.standby.catalog_path)

        manifest_id = "base-standby-a"
        begun = self.primary.admin_post(
            "/base-backups",
            {"slot_name": "standby-a", "manifest_id": manifest_id},
        )
        assert begun["slot_name"] == "standby-a"
        assert begun["manifest_id"] == manifest_id
        _assert_action_receipt(
            begun,
            action_id=f"base_backup_begin:{manifest_id}",
            action_kind="base_backup_begin",
            target=manifest_id,
            state="applied",
            node_id=self.primary.node_id,
        )
        backup_lsn = int(begun["backup_lsn"])
        manifest_path = backup_root / "backup.afha"
        manifest_path.write_bytes(
            _encode_ha_backup_manifest(
                identity=_identity(self),
                manifest_id=manifest_id,
                backup_lsn=backup_lsn,
                checkpoint_lsn=backup_lsn,
                files=[
                    {
                        "path": catalog_rel.as_posix(),
                        "kind": HA_BACKUP_FILE_KIND_METADATA,
                        "data": catalog_bytes,
                    }
                ],
            )
        )
        finished = self.primary.admin_post(
            "/base-backups/finish",
            {"manifest_path": str(manifest_path)},
        )
        assert finished["manifest_id"] == manifest_id
        assert int(finished["backup_lsn"]) == backup_lsn
        assert int(finished["end_record_lsn"]) >= backup_lsn
        _assert_action_receipt(
            finished,
            action_id=f"base_backup_finish:{manifest_id}",
            action_kind="base_backup_finish",
            target=manifest_id,
            state="applied",
            node_id=self.primary.node_id,
        )
        return {
            "backup_lsn": backup_lsn,
            "content_root": backup_root,
            "finished": finished,
            "manifest_id": manifest_id,
            "manifest_path": manifest_path,
        }

    def activate_seeded_slot(
        self,
        seed: dict[str, Any],
        bootstrapped: dict[str, Any],
    ) -> dict[str, Any]:
        generation = f"seed-{self.standby.node_id}-{seed['backup_lsn']}"
        seed_receipt_sha256 = _canonical_json_sha256(bootstrapped)
        capture_receipt_sha256 = _canonical_json_sha256(seed["finished"])
        manifest_sha256 = hashlib.sha256(seed["manifest_path"].read_bytes()).hexdigest()
        aggregate_sha256 = hashlib.sha256(
            b"antfly-ha-seed-activation-v1\0"
            + bytes.fromhex(seed_receipt_sha256)
            + bytes.fromhex(capture_receipt_sha256)
            + bytes.fromhex(manifest_sha256)
        ).hexdigest()
        return self.primary.admin_post(
            "/base-backups/activate",
            {
                "slot_name": self.standby.node_id,
                "generation": generation,
                "manifest_id": seed["manifest_id"],
                "timeline_id": self.primary.timeline_id,
                "checkpoint_lsn": seed["backup_lsn"],
                "seed_receipt_sha256": seed_receipt_sha256,
                "capture_receipt_sha256": capture_receipt_sha256,
                "manifest_sha256": manifest_sha256,
                "aggregate_sha256": aggregate_sha256,
            },
        )

    def close(self, *, test_failed: bool = False) -> None:
        self.standby.close()
        self.primary.close()
        if not maybe_preserve_tempdir(self.tempdir, failed=test_failed):
            self.tempdir.cleanup()

    def debug_logs(self) -> str:
        return f"[primary]\n{self.primary.debug_logs()}\n[standby]\n{self.standby.debug_logs()}"


@pytest.fixture
def ha_cluster(request: pytest.FixtureRequest) -> HACluster:
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Antfly binary not found: {binary}")
    if Path(binary).name != "antfly":
        pytest.skip("HA standby e2e requires the supported Zig antfly binary")
    if not _binary_supports_ha_standalone(binary):
        pytest.skip(
            f"Antfly binary does not expose HA standalone flags; rebuild current Zig binary: {binary}"
        )

    cluster = HACluster(binary)
    try:
        yield cluster
    finally:
        report = getattr(request.node, "rep_call", None)
        cluster.close(test_failed=bool(report and report.failed))


def _wait_for_standby_applied(
    cluster: HACluster, lsn: int, *, timeout_s: float = 20.0
) -> dict[str, Any]:
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


def _wait_for_promoted_write_check(
    cluster: HACluster,
    payload: dict[str, Any],
    *,
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    last_response: str | None = None
    while time.monotonic() < deadline:
        if cluster.standby.proc is not None and cluster.standby.proc.poll() is not None:
            break
        try:
            response = cluster.standby.admin_post_response("/write/check", payload)
        except requests.RequestException as err:
            last_error = err
            time.sleep(0.1)
            continue
        if response.ok:
            return cluster.standby._check(response)
        last_response = f"{response.status_code}: {response.text}"
        if response.status_code not in {409, 503}:
            return cluster.standby._check(response)
        time.sleep(0.1)
    exit_code = (
        cluster.standby.proc.poll() if cluster.standby.proc is not None else None
    )
    raise AssertionError(
        "promoted standby did not expose its write decision before the deadline; "
        f"exit_code={exit_code}; last_response={last_response}; last_error={last_error}\n"
        f"{cluster.debug_logs()}"
    )


def _wait_for_promoted_primary_missing_lookup(
    cluster: HACluster,
    table_name: str,
    key: str,
    *,
    timeout_s: float = 20.0,
) -> requests.Response:
    """Wait for the asynchronous promotion handoff to reach the public gate."""
    deadline = time.monotonic() + timeout_s
    last_response: str | None = None
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if cluster.standby.proc is not None and cluster.standby.proc.poll() is not None:
            break
        try:
            response = requests.get(
                f"{cluster.standby.url}{DB_API_ROOT}{lookup_key_path(table_name, key)}",
                timeout=30,
            )
        except requests.RequestException as err:
            last_error = err
            time.sleep(0.1)
            continue
        if response.status_code == 404:
            return response
        last_response = f"{response.status_code}: {response.text}"
        if response.status_code != 503 or response.text != "read requires primary":
            cluster.standby._check(response)
            raise AssertionError(f"promoted primary unexpectedly contained {key!r}")
        time.sleep(0.1)
    exit_code = (
        cluster.standby.proc.poll() if cluster.standby.proc is not None else None
    )
    raise AssertionError(
        "promoted standby did not publish primary read authority before the deadline; "
        f"exit_code={exit_code}; last_response={last_response}; last_error={last_error}\n"
        f"{cluster.debug_logs()}"
    )


def _wait_for_standby_lookup(
    cluster: HACluster,
    table_name: str,
    key: str,
    *,
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return cluster.standby.lookup_key(table_name, key, consistency="stale")
        except requests.HTTPError as err:
            if err.response is not None and err.response.status_code == 404:
                last_error = err
                time.sleep(0.25)
                continue
            raise
        except requests.RequestException as err:
            last_error = err
            time.sleep(0.25)
    raise AssertionError(
        f"standby lookup for {key!r} did not become visible; last_error={last_error}\n{cluster.debug_logs()}"
    )


def _primary_lsn(cluster: HACluster, *, timeout_s: float = 20.0) -> int:
    deadline = time.monotonic() + timeout_s
    last_response: requests.Response | None = None
    while time.monotonic() < deadline:
        response = cluster.primary.admin_get_response("/primary/status")
        last_response = response
        if response.status_code == 200:
            status = cluster.primary._check(response)
            return int(status["snapshot"]["current_lsn"])
        if response.status_code == 503 and response.text == "HAStateTransitionBusy":
            time.sleep(0.1)
            continue
        cluster.primary._check(response)
    raise AssertionError(
        "primary status remained transition-busy before the deadline; "
        f"last_response={last_response}\n{cluster.debug_logs()}"
    )


def _wait_for_primary_slot_applied(
    cluster: HACluster,
    slot_name: str,
    lsn: int,
    *,
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last_slot: dict[str, Any] | None = None
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            status = cluster.primary.admin_get("/primary/status")
            slot = _slot_by_name(status, slot_name)
        except (StopIteration, requests.RequestException) as err:
            last_error = err
            time.sleep(0.25)
            continue
        last_slot = slot
        if slot["received_lsn"] >= lsn and slot["applied_lsn"] >= lsn:
            return slot
        time.sleep(0.25)
    raise AssertionError(
        f"primary slot {slot_name!r} did not observe standby progress through LSN {lsn}; "
        f"last={last_slot}; last_error={last_error}\n{cluster.debug_logs()}"
    )


def _table_identity_from_catalog(
    node: HAStandaloneNode, table_name: str
) -> tuple[int, int]:
    catalog = json.loads(node.catalog_path.read_text())
    table = next(table for table in catalog["tables"] if table["name"] == table_name)
    table_id = int(table["table_id"])
    table_range = next(
        record for record in catalog["ranges"] if int(record["table_id"]) == table_id
    )
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


def _canonical_json_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _encode_ha_backup_manifest(
    *,
    identity: dict[str, int],
    manifest_id: str,
    backup_lsn: int,
    checkpoint_lsn: int,
    files: list[dict[str, Any]],
) -> bytes:
    manifest_id_bytes = manifest_id.encode()
    body = bytearray(manifest_id_bytes)
    for file in files:
        path_bytes = str(file["path"]).encode()
        data = bytes(file["data"])
        body.extend(
            struct.pack(
                "<HHIQIII",
                int(file["kind"]),
                0,
                0,
                len(data),
                zlib.crc32(data) & 0xFFFFFFFF,
                len(path_bytes),
                0,
            )
        )
        assert HA_BACKUP_ENTRY_HEADER_SIZE == struct.calcsize("<HHIQIII")
        body.extend(path_bytes)

    header = bytearray(HA_BACKUP_HEADER_SIZE)
    header[0:8] = HA_BACKUP_MAGIC
    struct.pack_into("<H", header, 8, HA_BACKUP_FORMAT_VERSION)
    struct.pack_into("<H", header, 10, HA_BACKUP_HEADER_SIZE)
    struct.pack_into("<I", header, 12, 0)
    struct.pack_into("<Q", header, 16, identity["cluster_id"])
    struct.pack_into("<Q", header, 24, identity["shard_id"])
    struct.pack_into("<Q", header, 32, identity["table_id"])
    struct.pack_into("<Q", header, 40, identity["timeline_id"])
    struct.pack_into("<Q", header, 48, identity["epoch"])
    struct.pack_into("<Q", header, 56, backup_lsn)
    struct.pack_into("<Q", header, 64, checkpoint_lsn)
    struct.pack_into("<I", header, 72, len(files))
    struct.pack_into("<I", header, 76, len(manifest_id_bytes))
    struct.pack_into("<Q", header, 80, len(body))
    struct.pack_into("<I", header, 88, zlib.crc32(body) & 0xFFFFFFFF)
    struct.pack_into("<I", header, 92, zlib.crc32(header[:92]) & 0xFFFFFFFF)
    return bytes(header + body)


def _promotion_fence_request(cluster: HACluster, required_lsn: int) -> dict[str, Any]:
    return {
        # Model the exact Kubernetes Lease transition generation supplied by
        # the operator. Runtime receipts must preserve this external value.
        "generation": 1,
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


def _whole_instance_identity(cluster: HACluster) -> dict[str, int]:
    return {
        "cluster_id": cluster.standby.cluster_id,
        "shard_id": 0,
        "table_id": 0,
        "timeline_id": cluster.standby.timeline_id,
        "epoch": cluster.standby.epoch,
    }


def _assert_action_receipt(
    response: dict[str, Any],
    *,
    action_id: str,
    action_kind: str,
    target: str,
    state: str,
    node_id: str,
) -> dict[str, Any]:
    action = response["action"]
    assert action["action_id"] == action_id
    assert action["action_kind"] == action_kind
    assert action["target"] == target
    assert action["state"] == state
    assert action["node_id"] == node_id
    return action


def _binary_supports_ha_standalone(binary: str) -> bool:
    result = subprocess.run(
        [binary, "standalone", "--help"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10,
        check=False,
    )
    return "--ha-primary-log" in result.stdout and "--ha-standby-log" in result.stdout


def _assert_admin_requires_bearer(node: HAStandaloneNode, path: str) -> None:
    missing = requests.get(f"{node.url}{HA_ADMIN_ROOT}{path}", timeout=10)
    assert missing.status_code == 401
    wrong = requests.get(
        f"{node.url}{HA_ADMIN_ROOT}{path}",
        headers={"Authorization": "Bearer wrong-token"},
        timeout=10,
    )
    assert wrong.status_code == 401


def _assert_internal_replication_requires_bearer(node: HAStandaloneNode) -> None:
    url = f"{node.url}/internal/v1/ha/replication/identify"
    missing = requests.get(url, timeout=10)
    assert missing.status_code == 401
    wrong = requests.get(
        url,
        headers={"Authorization": "Bearer wrong-token"},
        timeout=10,
    )
    assert wrong.status_code == 401
    authorized = requests.get(url, headers=node.admin_headers(), timeout=10)
    assert authorized.status_code == 200
    assert authorized.json()["identity"]["cluster_id"] == node.cluster_id


def _sync_policy(
    mode: str, *, failure_policy: str = "block", standby_name: str = "standby-a"
) -> dict[str, Any]:
    return {
        "mode": mode,
        "selection": "first",
        "required": 1,
        "standby_names": [standby_name],
        "failure_policy": failure_policy,
    }


def _slot_by_name(status: dict[str, Any], slot_name: str) -> dict[str, Any]:
    return next(
        slot
        for slot in status["snapshot"]["slots"]
        if slot.get("slot_name", slot.get("name")) == slot_name
    )


def test_standby_streams_public_writes_restarts_and_rejects_writes(
    ha_cluster: HACluster,
):
    table_name = "ha_standby_docs"
    ha_cluster.primary.start()
    created = ha_cluster.primary.create_table(table_name)
    assert created["name"] == table_name
    shard_id, table_id = _table_identity_from_catalog(ha_cluster.primary, table_name)
    ha_cluster.primary.reset_ha_state()
    ha_cluster.configure_table_identity(shard_id=shard_id, table_id=table_id)
    ha_cluster.primary.start()
    _assert_admin_requires_bearer(ha_cluster.primary, "/primary/status")
    _assert_internal_replication_requires_bearer(ha_cluster.primary)

    seed = ha_cluster.seed_standby_catalog_from_primary()
    assert seed["backup_lsn"] >= 1

    ha_cluster.standby.start(enable_replication=False)
    _assert_admin_requires_bearer(ha_cluster.standby, "/standby/status")
    bootstrapped = ha_cluster.standby.admin_post(
        "/standby/bootstrap",
        {
            "manifest_path": str(seed["manifest_path"]),
            "content_root": str(seed["content_root"]),
        },
    )
    assert bootstrapped["manifest_id"] == seed["manifest_id"]
    assert int(bootstrapped["backup_lsn"]) == seed["backup_lsn"]
    assert int(bootstrapped["checkpoint_lsn"]) == seed["backup_lsn"]
    _assert_action_receipt(
        bootstrapped,
        action_id=f"standby_bootstrap:{seed['manifest_id']}",
        action_kind="standby_bootstrap",
        target=seed["manifest_id"],
        state="applied",
        node_id="standby-a",
    )

    seeding_slot = _slot_by_name(
        ha_cluster.primary.admin_get("/primary/status"), "standby-a"
    )
    assert seeding_slot["active"] is False
    blocked_stream = requests.post(
        f"{ha_cluster.primary.url}/internal/v1/ha/replication/start",
        headers=ha_cluster.primary.admin_headers(),
        json={
            "slot_name": "standby-a",
            "from_lsn": seed["backup_lsn"] + 1,
            "max_records": 1,
            "max_encoded_bytes": 1024 * 1024,
        },
        timeout=10,
    )
    assert blocked_stream.status_code == 409
    assert blocked_stream.text == "SlotSeeding"

    activated = ha_cluster.activate_seeded_slot(seed, bootstrapped)
    generation = f"seed-standby-a-{seed['backup_lsn']}"
    _assert_action_receipt(
        activated,
        action_id=f"seeded_slot_activate:{generation}",
        action_kind="seeded_slot_activate",
        target=generation,
        state="applied",
        node_id="primary-a",
    )
    assert activated["slot_name"] == "standby-a"
    assert int(activated["checkpoint_lsn"]) == seed["backup_lsn"]
    activated_slot = _slot_by_name(
        ha_cluster.primary.admin_get("/primary/status"), "standby-a"
    )
    assert activated_slot["active"] is True

    activated_retry = ha_cluster.activate_seeded_slot(seed, bootstrapped)
    _assert_action_receipt(
        activated_retry,
        action_id=f"seeded_slot_activate:{generation}",
        action_kind="seeded_slot_activate",
        target=generation,
        state="already_applied",
        node_id="primary-a",
    )

    ha_cluster.standby.restart()

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
    at_least_read_check = ha_cluster.standby.admin_post(
        "/read/check",
        {"consistency": "at_least_lsn", "required_lsn": first_lsn},
    )
    assert at_least_read_check["decision"]["action"] == "serve_standby"
    assert at_least_read_check["decision"]["consistency"] == "at_least_lsn"
    assert at_least_read_check["decision"]["serve_lsn"] >= first_lsn
    future_read_check = ha_cluster.standby.admin_post(
        "/read/check",
        {"consistency": "at_least_lsn", "required_lsn": first_lsn + 1},
    )
    assert future_read_check["decision"]["action"] == "wait_for_apply"
    assert future_read_check["decision"]["missing_lsn_count"] == 1
    primary_read_check = ha_cluster.standby.admin_post(
        "/read/check",
        {"consistency": "primary", "required_lsn": first_lsn},
    )
    assert primary_read_check["decision"]["action"] == "route_to_primary"
    assert primary_read_check["decision"]["serve_lsn"] is None
    first_doc = _wait_for_standby_lookup(ha_cluster, table_name, "doc:first")
    assert first_doc["title"] == "first"

    write_check = ha_cluster.standby.admin_post("/write/check", {"role": "standby"})
    assert write_check["decision"]["action"] == "reject_read_only_standby"

    ha_cluster.standby.restart()
    restarted_snapshot = _wait_for_standby_applied(ha_cluster, first_lsn)
    assert restarted_snapshot["received_lsn"] >= first_lsn
    assert restarted_snapshot["applied_lsn"] >= first_lsn
    restarted_doc = _wait_for_standby_lookup(ha_cluster, table_name, "doc:first")
    assert restarted_doc["title"] == "first"

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
    second_doc = _wait_for_standby_lookup(ha_cluster, table_name, "doc:second")
    assert second_doc["title"] == "second"

    slot = _wait_for_primary_slot_applied(ha_cluster, "standby-a", second_lsn)
    assert slot["received_lsn"] >= second_lsn
    assert slot["applied_lsn"] >= second_lsn
    remote_write_status = ha_cluster.primary.admin_get(
        "/primary/status",
        sync_mode="remote_write",
        sync_selection="first",
        sync_required=1,
        sync_standby=["standby-a"],
    )
    remote_write = remote_write_status["snapshot"]["durability"]
    assert remote_write["status"] == "satisfied"
    assert remote_write["mode"] == "remote_write"
    assert remote_write["progress_lsn"] >= second_lsn
    assert remote_write["satisfied_count"] == 1

    remote_apply_status = ha_cluster.primary.admin_get(
        "/primary/status",
        sync_mode="remote_apply",
        sync_selection="first",
        sync_required=1,
        sync_standby=["standby-a"],
    )
    remote_apply = remote_apply_status["snapshot"]["durability"]
    assert remote_apply["status"] == "satisfied"
    assert remote_apply["mode"] == "remote_apply"
    assert remote_apply["progress_lsn"] >= second_lsn
    assert remote_apply["satisfied_count"] == 1

    blocked_commit = ha_cluster.primary.admin_post(
        "/commit/check",
        {
            "target_lsn": second_lsn,
            "sync_policy": _sync_policy("remote_apply", standby_name="missing-standby"),
        },
    )
    assert blocked_commit["gate"]["action"] == "wait_for_standby"
    assert blocked_commit["gate"]["durability"]["status"] == "would_block"
    assert blocked_commit["gate"]["durability"]["candidate_count"] == 0
    assert blocked_commit["gate"]["durability"]["required_count"] == 1

    failed_commit = ha_cluster.primary.admin_post(
        "/commit/check",
        {
            "target_lsn": second_lsn,
            "sync_policy": _sync_policy(
                "remote_apply",
                failure_policy="fail_closed",
                standby_name="missing-standby",
            ),
        },
    )
    assert failed_commit["gate"]["action"] == "reject"
    assert failed_commit["gate"]["durability"]["status"] == "fail_closed"

    stale_slot = ha_cluster.primary.admin_post(
        "/replication-slots",
        {"slot_name": "stale-standby", "initial_lsn": 0},
    )
    assert stale_slot["slot"]["slot_name"] == "stale-standby"
    retention_status = ha_cluster.primary.admin_get("/primary/status", max_lag_lsn=1)
    assert retention_status["snapshot"]["retention"]["reseed_recommended"] == 1
    retained_live_slot = _slot_by_name(retention_status, "standby-a")
    assert retained_live_slot["reseed_required"] is False
    retained_stale_slot = _slot_by_name(retention_status, "stale-standby")
    assert retained_stale_slot["reseed_required"] is True
    assert retained_stale_slot["status"] == "reseed_required"

    old_primary_slot = ha_cluster.primary.admin_post(
        "/replication-slots",
        {"slot_name": "primary-a", "initial_lsn": second_lsn},
    )
    assert old_primary_slot["slot"]["slot_name"] == "primary-a"

    fence_request = _promotion_fence_request(ha_cluster, second_lsn)
    fence = ha_cluster.standby.admin_post("/fence", fence_request)
    _assert_action_receipt(
        fence,
        action_id="fence_acquire:standby-a",
        action_kind="fence_acquire",
        target="standby-a",
        state="applied",
        node_id="standby-a",
    )
    assert fence["receipt"]["promoted_node_id"] == "standby-a"
    assert fence["receipt"]["old_primary_id"] == "primary-a"
    assert fence["receipt"]["parent_timeline_id"] == 1
    assert fence["receipt"]["parent_epoch"] == 1
    assert fence["receipt"]["new_timeline_id"] == 2
    assert fence["receipt"]["new_epoch"] == 2
    assert fence["receipt"]["required_lsn"] == second_lsn
    assert fence["receipt"]["observed_lsn"] == second_lsn
    assert fence["receipt"]["generation"] == fence_request["generation"]
    assert fence["receipt"]["forced"] is False
    assert fence["receipt"]["token"]

    assessment = ha_cluster.standby.admin_post(
        "/promotion/assess",
        {
            "required_lsn": second_lsn,
            "fencing_confirmed": False,
            "force": False,
            "use_current_fence": True,
        },
    )
    _assert_action_receipt(
        assessment,
        action_id="promotion_assess:standby-a",
        action_kind="promotion_assess",
        target="standby-a",
        state="assessed",
        node_id="standby-a",
    )
    assert assessment["assessment"]["can_promote"] is True
    assert assessment["assessment"]["fencing_confirmed"] is True
    assert assessment["assessment"]["mode"] == "safe"
    assert assessment["assessment"]["force"] is False
    assert assessment["assessment"]["requires_force"] is False

    promoted = ha_cluster.standby.admin_post("/promotion/current-fence", {})
    _assert_action_receipt(
        promoted,
        action_id="promotion:standby-a",
        action_kind="promotion",
        target="standby-a",
        state="applied",
        node_id="standby-a",
    )
    assert promoted["promotion"]["node_id"] == "standby-a"
    assert promoted["promotion"]["new_identity"]["timeline_id"] == 2
    assert promoted["promotion"]["new_identity"]["epoch"] == 2
    assert promoted["promotion"]["switch_lsn"] == second_lsn + 1
    assert promoted["promotion"]["forced"] is False
    assert promoted["promotion"]["data_loss_possible"] is False
    assert promoted["forced"] is False
    assert promoted["fence_generation"] == fence["receipt"]["generation"]
    assert promoted["fence_token"] == fence["receipt"]["token"]

    promoted_write_check = _wait_for_promoted_write_check(
        ha_cluster,
        {
            "role": "standby",
            "expected_identity": {
                **_identity(ha_cluster),
                "timeline_id": 2,
                "epoch": 2,
            },
        },
    )
    assert promoted_write_check["decision"]["role"] == "promoted_standby"
    assert promoted_write_check["decision"]["action"] == "open_promoted_primary"
    assert (
        promoted_write_check["decision"]["durable_lsn"]
        == promoted["promotion"]["switch_lsn"]
    )
    assert (
        promoted_write_check["decision"]["next_lsn"]
        == promoted["promotion"]["switch_lsn"] + 1
    )

    primary_fence = ha_cluster.primary.admin_post("/fence", fence_request)
    _assert_action_receipt(
        primary_fence,
        action_id="fence_acquire:standby-a",
        action_kind="fence_acquire",
        target="standby-a",
        state="applied",
        # Receipts identify the node-local endpoint that performed the action;
        # the promoted standby remains the separately asserted action target.
        node_id="primary-a",
    )
    assert primary_fence["receipt"]["promoted_node_id"] == "standby-a"
    fenced_write_check = ha_cluster.primary.admin_post(
        "/write/check",
        {"role": "primary", "expected_identity": _identity(ha_cluster)},
    )
    assert fenced_write_check["decision"]["role"] == "fenced_primary"
    assert fenced_write_check["decision"]["action"] == "reject_fenced_primary"

    rejoin_request = {
        "node_id": "primary-a",
        "identity": _identity(ha_cluster),
        "last_lsn": second_lsn,
        "retained_from_lsn": 0,
        "allow_rewind_after_forced_promotion": False,
    }
    unfenced_rejoin = ha_cluster.primary.admin_post("/rejoin/assess", rejoin_request)
    _assert_action_receipt(
        unfenced_rejoin,
        action_id="rejoin_assess:primary-a",
        action_kind="rejoin_assess",
        target="primary-a",
        state="assessed",
        node_id="primary-a",
    )
    assert unfenced_rejoin["assessment"]["action"] == "reject_unfenced"
    assert unfenced_rejoin["assessment"]["reason"] == "no_fence"

    reseed_rejoin_request = {
        **rejoin_request,
        "retained_from_lsn": second_lsn + 1,
        "receipt": fence["receipt"],
    }
    rejoin_assessment = ha_cluster.primary.admin_post(
        "/rejoin/assess", reseed_rejoin_request
    )
    _assert_action_receipt(
        rejoin_assessment,
        action_id="rejoin_assess:primary-a",
        action_kind="rejoin_assess",
        target="primary-a",
        state="assessed",
        node_id="primary-a",
    )
    assert rejoin_assessment["assessment"]["action"] == "reseed"
    assert rejoin_assessment["assessment"]["reason"] == "parent_timeline_wal_expired"
    assert rejoin_assessment["assessment"]["former_node_id"] == "primary-a"
    assert rejoin_assessment["assessment"]["target_timeline_id"] == 2
    assert rejoin_assessment["assessment"]["target_epoch"] == 2
    assert rejoin_assessment["assessment"]["fork_lsn"] == second_lsn

    rejoin_reseed = ha_cluster.primary.admin_post(
        "/rejoin/reseed", reseed_rejoin_request
    )
    _assert_action_receipt(
        rejoin_reseed,
        action_id="rejoin_reseed:primary-a",
        action_kind="rejoin_reseed",
        target="primary-a",
        state="applied",
        node_id="primary-a",
    )
    assert rejoin_reseed["reseed"]["node_id"] == "primary-a"
    assert rejoin_reseed["reseed"]["slot_name"] == "primary-a"
    assert rejoin_reseed["reseed"]["reseed_required"] is True
    assert rejoin_reseed["reseed"]["base_backup_required"] is True
    post_reseed_status = ha_cluster.primary.admin_get("/primary/status")
    reseeded_old_primary_slot = _slot_by_name(post_reseed_status, "primary-a")
    assert reseeded_old_primary_slot["reseed_required"] is True
    assert reseeded_old_primary_slot["status"] == "reseed_required"

    rejected = ha_cluster.primary.batch_write_response(
        table_name,
        {"doc:old-primary": {"title": "must not commit"}},
    )
    assert rejected.status_code >= 400, rejected.text
    with pytest.raises(requests.HTTPError) as missing_old_primary_doc:
        # The fenced node is no longer authoritative. Ask explicitly for its
        # retained local generation to verify that the rejected write did not
        # reach storage; the default read-index mode must remain unavailable.
        ha_cluster.primary.lookup_key(
            table_name, "doc:old-primary", consistency="stale"
        )
    assert missing_old_primary_doc.value.response is not None
    assert missing_old_primary_doc.value.response.status_code == 404
    missing_promoted_primary_doc = _wait_for_promoted_primary_missing_lookup(
        ha_cluster,
        table_name,
        "doc:old-primary",
    )
    assert missing_promoted_primary_doc.status_code == 404

    # Promotion changes authority, but this in-process standby has not been
    # restarted as a primary with a replacement synchronous replica. Keep the
    # public mutation surface fail-closed until continuous replication exists.
    rejected_promoted_write = ha_cluster.standby.batch_write_response(
        table_name,
        {"doc:promoted": {"title": "must not commit without replication"}},
    )
    assert rejected_promoted_write.status_code == 503
    assert rejected_promoted_write.json() == {
        "error": "mutation is not continuously replicated while HA is active",
        "code": "ha_mutation_not_replicated",
        "surface": "document_batch",
    }
    missing_promoted_copy = _wait_for_promoted_primary_missing_lookup(
        ha_cluster,
        table_name,
        "doc:promoted",
    )
    assert missing_promoted_copy.status_code == 404
    with pytest.raises(requests.HTTPError) as missing_former_primary_copy:
        ha_cluster.primary.lookup_key(table_name, "doc:promoted", consistency="stale")
    assert missing_former_primary_copy.value.response is not None
    assert missing_former_primary_copy.value.response.status_code == 404


def test_forced_promotion_receipt_records_lossy_runtime_evidence(ha_cluster: HACluster):
    ha_cluster.standby.start(enable_replication=False)
    _assert_admin_requires_bearer(ha_cluster.standby, "/standby/status")

    required_lsn = 1
    forced_request = {
        "generation": 1,
        "identity": _whole_instance_identity(ha_cluster),
        "old_primary_id": ha_cluster.primary.node_id,
        "promoted_node_id": ha_cluster.standby.node_id,
        "new_timeline_id": ha_cluster.standby.timeline_id + 1,
        "new_epoch": ha_cluster.standby.epoch + 1,
        "required_lsn": required_lsn,
        "observed_lsn": 0,
        "force": True,
        "reason": "ha-standby-forced-e2e",
    }

    fence = ha_cluster.standby.admin_post("/fence", forced_request)
    _assert_action_receipt(
        fence,
        action_id="fence_acquire:standby-a",
        action_kind="fence_acquire",
        target="standby-a",
        state="applied",
        node_id="standby-a",
    )
    assert fence["receipt"]["forced"] is True
    assert fence["receipt"]["required_lsn"] == required_lsn
    assert fence["receipt"]["observed_lsn"] == 0
    assert fence["receipt"]["generation"] == forced_request["generation"]
    assert fence["receipt"]["token"]

    assessment = ha_cluster.standby.admin_post(
        "/promotion/assess",
        {
            "required_lsn": required_lsn,
            "fencing_confirmed": False,
            "force": False,
            "use_current_fence": True,
        },
    )
    _assert_action_receipt(
        assessment,
        action_id="promotion_assess:standby-a",
        action_kind="promotion_assess",
        target="standby-a",
        state="assessed",
        node_id="standby-a",
    )
    assert assessment["assessment"]["can_promote"] is True
    assert assessment["assessment"]["force"] is True
    assert assessment["assessment"]["mode"] == "lossy"
    assert assessment["assessment"]["data_loss_possible"] is True
    assert assessment["assessment"]["safe"] is False
    assert assessment["assessment"]["requires_force"] is False

    promoted = ha_cluster.standby.admin_post("/promotion/current-fence", {})
    _assert_action_receipt(
        promoted,
        action_id="promotion:standby-a",
        action_kind="promotion",
        target="standby-a",
        state="applied",
        node_id="standby-a",
    )
    assert promoted["forced"] is True
    assert promoted["fence_generation"] == fence["receipt"]["generation"]
    assert promoted["fence_token"] == fence["receipt"]["token"]
    assert promoted["assessment"]["mode"] == "lossy"
    assert promoted["assessment"]["data_loss_possible"] is True
    assert promoted["assessment"]["safe"] is False
    assert promoted["promotion"]["node_id"] == "standby-a"
    assert promoted["promotion"]["new_identity"]["timeline_id"] == 2
    assert promoted["promotion"]["new_identity"]["epoch"] == 2
    assert promoted["promotion"]["switch_lsn"] == required_lsn
    assert promoted["promotion"]["forced"] is True
    assert promoted["promotion"]["data_loss_possible"] is True
