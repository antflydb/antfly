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

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import pytest
import requests

from conftest import (
    DEFAULT_ANTFLY_BIN,
    REPO_ROOT,
    _metadata_command,
    _read_log_tail,
    antfly_public_api_url,
    find_free_port,
    maybe_preserve_tempdir,
    wait_for_server,
)
from helpers import wait_until


def _data_command(
    binary: str,
    *,
    host: str,
    port: int,
    raft_port: int,
    metadata_admin_base_uri: str,
    root: Path,
    node_id: int,
    store_id: int,
    store_role: str,
) -> list[str]:
    return [
        binary,
        "data",
        "--api-host",
        host,
        "--api-port",
        str(port),
        "--raft-host",
        host,
        "--raft-port",
        str(raft_port),
        "--metadata-api",
        metadata_admin_base_uri,
        "--node-id",
        str(node_id),
        "--store-id",
        str(store_id),
        "--store-role",
        store_role,
        "--tick-ms",
        "5",
        "--replica-root-dir",
        str(root / f"{store_role}-{store_id}-replicas"),
        "--replica-catalog-path",
        str(root / f"{store_role}-{store_id}-catalog.txt"),
    ]


class SplitStatusCluster:
    def __init__(self, binary: str):
        self.binary = binary
        self.host = "127.0.0.1"
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-status-e2e-")
        self.root = Path(self.tempdir.name)

        self.metadata_port = find_free_port()
        self.metadata_admin_port = find_free_port()
        self.data_port = find_free_port()
        self.data_raft_port = find_free_port()
        self.api_port = find_free_port()
        self.api_raft_port = find_free_port()

        self.metadata_admin_url = f"http://{self.host}:{self.metadata_admin_port}"
        self.data_url = f"http://{self.host}:{self.data_port}"
        self.data_api_url = antfly_public_api_url(self.data_url, binary=binary)
        self.api_url = antfly_public_api_url(f"http://{self.host}:{self.api_port}", binary=binary)

        self.metadata_log_path = self.root / "metadata.log"
        self.data_log_path = self.root / "data-owner.log"
        self.api_log_path = self.root / "api-node.log"
        self.metadata_log_file = self.metadata_log_path.open("w")
        self.data_log_file = self.data_log_path.open("w")
        self.api_log_file = self.api_log_path.open("w")

        self.metadata_proc: subprocess.Popen[str] | None = None
        self.data_proc: subprocess.Popen[str] | None = None
        self.api_proc: subprocess.Popen[str] | None = None

        try:
            self._start()
        except BaseException:
            self.stop()
            raise

    def _start(self) -> None:
        metadata_command = _metadata_command(
            self.binary,
            host=self.host,
            raft_port=self.metadata_port,
            admin_port=self.metadata_admin_port,
            root=self.root,
        )
        self.metadata_proc = subprocess.Popen(
            metadata_command,
            stdout=self.metadata_log_file,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
        if not wait_for_server(self.metadata_admin_url, path="/metadata/v1/status"):
            raise RuntimeError(f"Metadata server failed to start\n{self.debug_logs()}")

        data_command = _data_command(
            self.binary,
            host=self.host,
            port=self.data_port,
            raft_port=self.data_raft_port,
            metadata_admin_base_uri=self.metadata_admin_url,
            root=self.root,
            node_id=2,
            store_id=2,
            store_role="data",
        )
        self.data_proc = subprocess.Popen(
            data_command,
            stdout=self.data_log_file,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
        if not wait_for_server(self.data_api_url):
            raise RuntimeError(f"Data owner failed to start\n{self.debug_logs()}")

        api_command = _data_command(
            self.binary,
            host=self.host,
            port=self.api_port,
            raft_port=self.api_raft_port,
            metadata_admin_base_uri=self.metadata_admin_url,
            root=self.root,
            node_id=3,
            store_id=3,
            store_role="api",
        )
        self.api_proc = subprocess.Popen(
            api_command,
            stdout=self.api_log_file,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
        if not wait_for_server(self.api_url):
            raise RuntimeError(f"API-only node failed to start\n{self.debug_logs()}")

    def debug_logs(self) -> str:
        for handle in (self.metadata_log_file, self.data_log_file, self.api_log_file):
            handle.flush()
        return (
            f"[metadata]\n{_read_log_tail(self.metadata_log_path)}\n"
            f"[data-owner]\n{_read_log_tail(self.data_log_path)}\n"
            f"[api-node]\n{_read_log_tail(self.api_log_path)}"
        )

    def metadata_snapshot(self) -> dict[str, Any]:
        response = requests.get(f"{self.metadata_admin_url}/metadata/v1/admin/snapshot", timeout=10)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def stop(self) -> None:
        for proc in (self.api_proc, self.data_proc, self.metadata_proc):
            if proc is not None and proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        self.api_proc = None
        self.data_proc = None
        self.metadata_proc = None

        for handle in (self.api_log_file, self.data_log_file, self.metadata_log_file):
            if not handle.closed:
                handle.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


@pytest.fixture
def split_status_cluster() -> SplitStatusCluster:
    binary = os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
    resolved = Path(binary).expanduser().resolve()
    if resolved.name != "antfly":
        pytest.skip("distributed status e2e requires the antfly binary")
    if not resolved.exists():
        pytest.skip(f"antfly binary not built: {resolved}")

    cluster = SplitStatusCluster(str(resolved))
    try:
        yield cluster
    finally:
        cluster.stop()


def _check_response(response: requests.Response) -> dict[str, Any]:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise AssertionError(f"{response.request.method} {response.url} failed: {response.text}") from exc
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def _runtime_status_reports(snapshot: dict[str, Any], table_name: str) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for store in snapshot.get("stores", []):
        if not isinstance(store, dict):
            continue
        for report in store.get("runtime_statuses", []):
            if isinstance(report, dict) and report.get("table_name") == table_name:
                reports.append(report)
    return reports


def _runtime_report_has_index_doc_count(report: dict[str, Any], index_name: str, expected_docs: int) -> bool:
    indexes = report.get("indexes", [])
    if not isinstance(indexes, list):
        return False
    for index in indexes:
        if not isinstance(index, dict):
            continue
        if index.get("name") == index_name and int(index.get("doc_count", 0)) >= expected_docs:
            return True
    return False


def test_non_host_api_reports_remote_index_status_from_metadata_heartbeat(
    split_status_cluster: SplitStatusCluster,
) -> None:
    table_name = f"distributed_status_{time.time_ns()}"
    index_name = "full_text_index_v0"
    expected_docs = 2

    session = requests.Session()
    _check_response(
        session.post(
            f"{split_status_cluster.data_api_url}/tables/{table_name}",
            json={"num_shards": 1},
            timeout=30,
        )
    )
    _check_response(
        session.post(
            f"{split_status_cluster.data_api_url}/tables/{table_name}/batch",
            json={
                "inserts": {
                    "doc:a": {"body": "alpha remote status"},
                    "doc:b": {"body": "beta remote status"},
                },
            },
            timeout=30,
        )
    )

    def propagated_runtime_snapshot() -> dict[str, Any] | None:
        try:
            snapshot = split_status_cluster.metadata_snapshot()
        except requests.RequestException:
            return None
        reports = _runtime_status_reports(snapshot, table_name)
        if any(_runtime_report_has_index_doc_count(report, index_name, expected_docs) for report in reports):
            return snapshot
        return None

    propagated = wait_until(propagated_runtime_snapshot, timeout_s=45.0, interval_s=0.5)
    assert propagated is not None, (
        "data owner did not publish runtime status into metadata heartbeat\n"
        f"{split_status_cluster.debug_logs()}"
    )

    def remote_index_detail() -> dict[str, Any] | None:
        try:
            detail = _check_response(
                session.get(
                    f"{split_status_cluster.api_url}/tables/{table_name}/indexes/{index_name}",
                    timeout=10,
                )
            )
        except (AssertionError, requests.RequestException):
            return None
        status = detail.get("status")
        if not isinstance(status, dict):
            return None
        if int(status.get("expected_groups", 0)) < 1:
            return None
        if int(status.get("reported_groups", 0)) < 1:
            return None
        if int(status.get("missing_groups", 0)) != 0:
            return None
        if int(status.get("doc_count", status.get("total_indexed", 0))) < expected_docs:
            return None
        return detail

    detail = wait_until(remote_index_detail, timeout_s=45.0, interval_s=0.5)
    assert detail is not None, (
        "API-only node did not report propagated remote runtime status\n"
        f"metadata snapshot: {split_status_cluster.metadata_snapshot()}\n"
        f"{split_status_cluster.debug_logs()}"
    )

    status = detail["status"]
    assert status["expected_groups"] == 1
    assert status["reported_groups"] == 1
    assert status["missing_groups"] == 0

    shard_status = detail.get("shard_status")
    assert isinstance(shard_status, dict)
    assert any(
        isinstance(shard, dict) and int(shard.get("doc_count", 0)) >= expected_docs
        for shard in shard_status.values()
    )
