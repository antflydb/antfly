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

"""Stateful public API backup and restore tests."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path

import pytest
import requests

from conftest import (
    ANTFLY_PUBLIC_API_ROOT,
    DEFAULT_ANTFLY_BIN,
    REPO_ROOT,
    _read_log_tail,
    antfly_public_api_url,
    maybe_preserve_tempdir,
    resolve_binary_path,
    wait_for_server,
)
from helpers import assert_created_index, wait_until
from port_reservations import LoopbackPortReservations

BACKUP_CONNECTION = "e2e-backups"
EXPECTED_CORPUS_SHA256 = (
    "3478905dd4d3259bed910b86edd3882b6adacb1a36e320e4ad9701d0dc88d197"
)


def _file_location(path: str | Path) -> str:
    """Return a canonical file URI accepted by no-symlink backup traversal."""
    return Path(path).resolve().as_uri()


def _wait_for_terminal_restore_job(
    backup_api, response: requests.Response, *, timeout_s: float = 30.0
) -> dict:
    assert response.status_code == 202
    accepted = response.json()
    job_id = accepted["job_id"]

    def terminal() -> dict | None:
        job = backup_api.get(f"/restore/jobs/{job_id}")
        return job if job.get("phase") in {"succeeded", "failed", "cancelled"} else None

    job = wait_until(terminal, timeout_s=timeout_s, interval_s=0.1)
    assert job is not None
    return job


def _lookup_doc(stateful_api, table_name: str, key: str) -> dict | None:
    try:
        return stateful_api.lookup_key(table_name, key)
    except requests.HTTPError:
        return None


def _lookup_doc_from_url(
    session: requests.Session, api_url: str, table_name: str, key: str
) -> dict | None:
    try:
        response = session.get(
            f"{api_url}/tables/{table_name}/documents/{key}", timeout=10
        )
        if response.status_code >= 400:
            return None
        payload = response.json()
        return payload if isinstance(payload, dict) else None
    except (requests.RequestException, ValueError):
        return None


def _lookup_docs(
    stateful_api, table_names: tuple[str, ...], key: str
) -> dict[str, dict] | None:
    docs: dict[str, dict] = {}
    for table_name in table_names:
        doc = _lookup_doc(stateful_api, table_name, key)
        if doc is None:
            return None
        docs[table_name] = doc
    return docs


def _wait_until_absent(
    stateful_api, table_name: str, key: str, *, timeout_s: float, interval_s: float
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _lookup_doc(stateful_api, table_name, key) is None:
            return
        time.sleep(interval_s)
    raise AssertionError(f"{table_name}:{key} remained visible after delete")


def _lookup_table(stateful_api, table_name: str) -> dict | None:
    try:
        return stateful_api.get_table(table_name)
    except requests.HTTPError:
        return None


def _wait_until_table_absent(
    stateful_api, table_name: str, *, timeout_s: float, interval_s: float
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _lookup_table(stateful_api, table_name) is None:
            return
        time.sleep(interval_s)
    raise AssertionError(f"{table_name} remained visible after delete")


def _top_hit(
    stateful_api, table_name: str, query: str, expected_id: str
) -> dict | None:
    try:
        result = stateful_api.query_table(
            table_name,
            {
                "full_text_search": {
                    "match": {
                        "field": "content",
                        "text": query,
                    },
                },
                "limit": 5,
            },
        )
    except requests.HTTPError:
        return None

    responses = result.get("responses", [])
    if not responses:
        return None
    hits = responses[0].get("hits", {}).get("hits", [])
    if not hits:
        return None
    for hit in hits:
        if hit.get("_id") == expected_id:
            return result
    return None


def _semantic_top_hit(
    stateful_api, table_name: str, query: str, index_name: str, expected_id: str
) -> dict | None:
    try:
        result = stateful_api.query_table(
            table_name,
            {
                "semantic_search": query,
                "indexes": [index_name],
                "limit": 5,
            },
        )
    except requests.HTTPError:
        return None

    responses = result.get("responses", [])
    if not responses:
        return None
    hits = responses[0].get("hits", {}).get("hits", [])
    if not hits:
        return None
    for hit in hits:
        if hit.get("_id") == expected_id:
            return result
    return None


def _chunked_doc(
    stateful_api, table_name: str, key: str, chunk_field: str
) -> list[dict] | None:
    scan = stateful_api.scan_keys(
        table_name,
        {
            "from": key,
            "to": f"{key};",
            "inclusive_from": True,
            "fields": ["title", "_chunks"],
        },
    )
    if len(scan) != 1:
        return None
    chunks = scan[0].get("_chunks", {}).get(chunk_field)
    return scan if chunks else None


def _write_single_doc(
    stateful_api, table_name: str, key: str, *, title: str, content: str
) -> None:
    batch = stateful_api.batch_write(
        table_name,
        inserts={
            key: {
                "title": title,
                "content": content,
            }
        },
        sync_level="full_text",
    )
    assert batch["inserted"] == 1


def _integration_enabled(env_name: str) -> bool:
    value = os.environ.get(env_name, "")
    return value != "" and value not in {"0", "false", "False"}


def _remote_backup_location(backend: str) -> str:
    if backend == "s3":
        enable_env = "OBJECTSTORE_S3_INTEGRATION"
        bucket_env = "OBJECTSTORE_S3_TEST_BUCKET"
        scheme = "s3"
    elif backend == "gs":
        enable_env = "OBJECTSTORE_GCS_INTEGRATION"
        bucket_env = "OBJECTSTORE_GCS_TEST_BUCKET"
        scheme = "gs"
    else:
        raise AssertionError(f"unsupported backend: {backend}")

    if not _integration_enabled(enable_env):
        pytest.skip(f"set {enable_env}=1 to enable {scheme} backup integration tests")

    bucket = os.environ.get(bucket_env)
    if not bucket:
        pytest.skip(f"missing env {bucket_env}")

    prefix = f"antfly-backup-e2e/{scheme}/{time.time_ns()}"
    return f"{scheme}://{bucket}/{prefix}"


def _check_success(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise AssertionError(
            f"{response.request.method} {response.url} failed: {response.text}"
        ) from exc


def _check_response(response: requests.Response) -> dict:
    _check_success(response)
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def _is_metadata_not_leader_response(response: requests.Response) -> bool:
    return response.headers.get("X-Antfly-Metadata-Not-Leader", "").lower() == "true"


class MultiMetadataBackupCluster:
    def __init__(
        self,
        binary: str,
        *,
        data_node_count: int = 1,
        replication_factor: int = 1,
    ):
        if data_node_count < 1:
            raise ValueError("data_node_count must be positive")
        if replication_factor < 1 or replication_factor > data_node_count:
            raise ValueError("replication_factor must fit within data_node_count")

        self.binary = binary
        self.host = "127.0.0.1"
        self.replication_factor = replication_factor
        with ExitStack() as setup:
            self.tempdir = tempfile.TemporaryDirectory(
                prefix="antfly-zig-metadata-backup-e2e-"
            )
            setup.callback(self.tempdir.cleanup)
            self.root = Path(self.tempdir.name)
            self.port_reservations = LoopbackPortReservations(self.host)
            setup.callback(self.port_reservations.close)

            self.metadata_raft_ports = list(self.port_reservations.reserve_many(3))
            self.metadata_admin_ports = list(self.port_reservations.reserve_many(3))
            self.metadata_admin_urls = [
                f"http://{self.host}:{port}" for port in self.metadata_admin_ports
            ]
            self.metadata_public_urls = [
                antfly_public_api_url(url, root=ANTFLY_PUBLIC_API_ROOT)
                for url in self.metadata_admin_urls
            ]
            self.data_nodes = [
                {
                    "node_id": node_id,
                    "store_id": node_id,
                    "api_port": self.port_reservations.reserve(),
                    "raft_port": self.port_reservations.reserve(),
                }
                for node_id in range(4, 4 + data_node_count)
            ]
            self.data_urls = [
                f"http://{self.host}:{node['api_port']}" for node in self.data_nodes
            ]
            self.data_api_urls = [
                antfly_public_api_url(url, binary=binary) for url in self.data_urls
            ]

            self.config_path = self.root / "antfly-metadata-cluster.json"
            self._write_config()

            self.metadata_log_paths = [
                self.root / f"metadata-{node_id}.log" for node_id in range(1, 4)
            ]
            self.metadata_log_files = [
                setup.enter_context(path.open("w")) for path in self.metadata_log_paths
            ]
            self.data_log_paths = [
                self.root / f"data-{node['node_id']}.log" for node in self.data_nodes
            ]
            self.data_log_files = [
                setup.enter_context(path.open("w")) for path in self.data_log_paths
            ]

            self.metadata_procs: list[subprocess.Popen[str]] = []
            self.data_procs: list[subprocess.Popen[str]] = []
            setup.pop_all()

        try:
            self._start()
        except BaseException:
            self.stop()
            raise

    def _write_config(self) -> None:
        metadata = {
            "orchestration_urls": {
                str(node_id): self.metadata_admin_urls[node_id - 1]
                for node_id in range(1, 4)
            },
            "raft_urls": {
                str(
                    node_id
                ): f"http://{self.host}:{self.metadata_raft_ports[node_id - 1]}"
                for node_id in range(1, 4)
            },
        }
        self.config_path.write_text(
            json.dumps(
                {
                    "metadata": metadata,
                    "remote_content": {"security": {"block_private_ips": False}},
                    "connections": {
                        BACKUP_CONNECTION: {
                            "kind": "external_io",
                            "capabilities": ["backup.write", "restore.read"],
                            "external_io": {"protocol": "filesystem", "root": "/"},
                        }
                    },
                    "replication_factor": self.replication_factor,
                    "default_shards_per_table": 1,
                }
            ),
            encoding="utf-8",
        )

    def _metadata_command(self, node_id: int) -> list[str]:
        return [
            self.binary,
            "metadata",
            "--config",
            str(self.config_path),
            "--id",
            str(node_id),
            "--raft-host",
            self.host,
            "--raft-port",
            str(self.metadata_raft_ports[node_id - 1]),
            "--api-host",
            self.host,
            "--api-port",
            str(self.metadata_admin_ports[node_id - 1]),
            "--health",
            "false",
            "--data-dir",
            str(self.root / f"metadata-{node_id}"),
            "--replica-root-dir",
            str(self.root / f"metadata-{node_id}-replicas"),
            "--replica-catalog-path",
            str(self.root / f"metadata-{node_id}-catalog.txt"),
            "--snapshot-root-dir",
            str(self.root / f"metadata-{node_id}-snapshots"),
        ]

    @property
    def data_url(self) -> str:
        return self.data_urls[0]

    @property
    def data_api_url(self) -> str:
        return self.data_api_urls[0]

    def _data_command(self, index: int) -> list[str]:
        node = self.data_nodes[index]
        command = [
            self.binary,
            "data",
            "--config",
            str(self.config_path),
            "--api-host",
            self.host,
            "--api-port",
            str(node["api_port"]),
            "--raft-host",
            self.host,
            "--raft-port",
            str(node["raft_port"]),
            "--node-id",
            str(node["node_id"]),
            "--store-id",
            str(node["store_id"]),
            "--store-role",
            "data",
            "--health",
            "false",
            "--data-dir",
            str(self.root / f"data-{node['node_id']}"),
            "--replica-root-dir",
            str(self.root / f"data-{node['node_id']}-replicas"),
            "--replica-catalog-path",
            str(self.root / f"data-{node['node_id']}-catalog.txt"),
            "--snapshot-root-dir",
            str(self.root / f"data-{node['node_id']}-snapshots"),
        ]
        for url in self.metadata_admin_urls:
            command.extend(["--metadata-api", url])
        return command

    def _spawn_metadata(
        self, index: int, *, reserved_ports: bool
    ) -> subprocess.Popen[str]:
        spawn = lambda: subprocess.Popen(
            self._metadata_command(index + 1),
            stdout=self.metadata_log_files[index],
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
        if reserved_ports:
            return self.port_reservations.handoff_to(
                (self.metadata_raft_ports[index], self.metadata_admin_ports[index]),
                spawn,
            )
        return spawn()

    def _spawn_data(self, index: int, *, reserved_ports: bool) -> subprocess.Popen[str]:
        node = self.data_nodes[index]
        spawn = lambda: subprocess.Popen(
            self._data_command(index),
            stdout=self.data_log_files[index],
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
        if reserved_ports:
            return self.port_reservations.handoff_to(
                (node["api_port"], node["raft_port"]),
                spawn,
            )
        return spawn()

    def _start(self) -> None:
        for i in range(3):
            self.metadata_procs.append(self._spawn_metadata(i, reserved_ports=True))

        for url in self.metadata_admin_urls:
            if not wait_for_server(url, path="/metadata/v1/status", timeout=30.0):
                raise RuntimeError(
                    f"metadata server failed to start at {url}\n{self.debug_logs()}"
                )

        if self.metadata_stable_leader_index(timeout_s=30.0) is None:
            raise RuntimeError(
                f"metadata cluster did not elect a leader\n{self.debug_logs()}"
            )

        for i, data_api_url in enumerate(self.data_api_urls):
            self.data_procs.append(self._spawn_data(i, reserved_ports=True))
            if not wait_for_server(data_api_url, timeout=30.0):
                raise RuntimeError(
                    f"data server failed to start at {data_api_url}\n{self.debug_logs()}"
                )

        if self.wait_for_data_stores(timeout_s=30.0) is None:
            raise RuntimeError(f"data stores did not register\n{self.debug_logs()}")

    def debug_logs(self) -> str:
        for handle in self.metadata_log_files:
            handle.flush()
        for handle in self.data_log_files:
            handle.flush()
        parts = [
            f"[metadata-{i + 1}]\n{_read_log_tail(path)}"
            for i, path in enumerate(self.metadata_log_paths)
        ]
        parts.extend(
            f"[data-{node['node_id']}]\n{_read_log_tail(path)}"
            for node, path in zip(self.data_nodes, self.data_log_paths, strict=True)
        )
        return "\n".join(parts)

    def metadata_statuses(self, *, request_timeout_s: float = 1.0) -> list[dict | None]:
        def fetch(url: str) -> dict | None:
            try:
                response = requests.get(
                    f"{url}/metadata/v1/status", timeout=request_timeout_s
                )
                return _check_response(response)
            except (AssertionError, requests.RequestException, ValueError):
                return None

        # A serial probe makes one unavailable node consume the complete
        # election-observation window before healthy peers are considered.
        # Keep one bounded request per node and preserve configured ordering.
        with ThreadPoolExecutor(max_workers=len(self.metadata_admin_urls)) as executor:
            return list(executor.map(fetch, self.metadata_admin_urls))

    def metadata_leader_index_once(self, *, request_timeout_s: float) -> int | None:
        statuses = self.metadata_statuses(request_timeout_s=request_timeout_s)
        leader_ids = {
            int(status["metadata_raft_leader_id"])
            for status in statuses
            if status and status.get("metadata_raft_leader_id") is not None
        }
        if len(leader_ids) != 1:
            return None
        leader_id = leader_ids.pop()
        if leader_id < 1 or leader_id > len(self.metadata_admin_urls):
            return None
        leader_status = statuses[leader_id - 1]
        if not leader_status:
            return None
        if leader_status.get("metadata_raft_role") != "leader":
            return None
        if int(leader_status.get("metadata_raft_local_node_id", 0)) != leader_id:
            return None
        return leader_id - 1

    def metadata_leader_index(self, *, timeout_s: float) -> int | None:
        def current_leader() -> dict | None:
            leader_index = self.metadata_leader_index_once(
                request_timeout_s=min(1.0, max(0.05, timeout_s))
            )
            return {"index": leader_index} if leader_index is not None else None

        result = wait_until(current_leader, timeout_s=timeout_s, interval_s=0.25)
        return int(result["index"]) if result is not None else None

    def metadata_stable_leader_index(
        self,
        *,
        timeout_s: float,
        stable_observations: int = 3,
        interval_s: float = 0.25,
    ) -> int | None:
        last_leader: int | None = None
        observed = 0

        def current_stable_leader() -> dict | None:
            nonlocal last_leader, observed
            leader_index = self.metadata_leader_index_once(
                request_timeout_s=min(1.0, max(0.05, timeout_s))
            )
            if leader_index is None:
                last_leader = None
                observed = 0
                return None
            if leader_index == last_leader:
                observed += 1
            else:
                last_leader = leader_index
                observed = 1
            return {"index": leader_index} if observed >= stable_observations else None

        result = wait_until(
            current_stable_leader, timeout_s=timeout_s, interval_s=interval_s
        )
        return int(result["index"]) if result is not None else None

    def metadata_leader_public_url(self, *, timeout_s: float = 30.0) -> str:
        leader_index = self.metadata_stable_leader_index(timeout_s=timeout_s)
        if leader_index is None:
            raise AssertionError(f"metadata leader unavailable\n{self.debug_logs()}")
        return self.metadata_public_urls[leader_index]

    def metadata_follower_public_url(self, *, timeout_s: float = 30.0) -> str:
        leader_index = self.metadata_stable_leader_index(timeout_s=timeout_s)
        if leader_index is None:
            raise AssertionError(f"metadata leader unavailable\n{self.debug_logs()}")
        return next(
            url
            for index, url in enumerate(self.metadata_public_urls)
            if index != leader_index
        )

    def metadata_snapshot(self) -> dict:
        last_error: Exception | None = None
        for candidate in range(len(self.metadata_admin_urls)):
            try:
                response = requests.get(
                    f"{self.metadata_admin_urls[candidate]}/metadata/v1/admin/snapshot",
                    timeout=10,
                )
                return _check_response(response)
            except (AssertionError, requests.RequestException, ValueError) as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise AssertionError("metadata snapshot has no candidate node")

    def wait_for_data_stores(self, *, timeout_s: float) -> dict | None:
        expected = {
            int(node["store_id"]): (
                int(node["node_id"]),
                self.data_urls[index],
                f"http://{self.host}:{node['raft_port']}",
            )
            for index, node in enumerate(self.data_nodes)
        }

        def registered() -> dict | None:
            try:
                snapshot = self.metadata_snapshot()
            except (AssertionError, requests.RequestException, ValueError):
                return None
            stores = {
                int(store.get("store_id", 0)): store
                for store in snapshot.get("stores", [])
                if isinstance(store, dict) and store.get("role") == "data"
            }
            for store_id, (node_id, api_url, raft_url) in expected.items():
                store = stores.get(store_id)
                if store is None:
                    return None
                if (
                    int(store.get("node_id", 0)) != node_id
                    or store.get("api_url") != api_url
                    or store.get("raft_url") != raft_url
                    or store.get("live") is not True
                    or store.get("health_class") != "healthy"
                ):
                    return None
            return snapshot

        return wait_until(registered, timeout_s=timeout_s, interval_s=0.25)

    @staticmethod
    def _terminate_process(
        proc: subprocess.Popen[str], *, timeout_s: float = 10.0
    ) -> None:
        if proc.poll() is not None:
            return
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    def restart_metadata_node(self, index: int) -> None:
        self._terminate_process(self.metadata_procs[index])
        if self.metadata_stable_leader_index(timeout_s=30.0) is None:
            raise AssertionError(
                f"metadata quorum did not elect while node {index + 1} was stopped\n"
                f"{self.debug_logs()}"
            )
        self.metadata_procs[index] = self._spawn_metadata(index, reserved_ports=False)
        url = self.metadata_admin_urls[index]
        if not wait_for_server(url, path="/metadata/v1/status", timeout=30.0):
            raise AssertionError(
                f"metadata node {index + 1} did not restart\n{self.debug_logs()}"
            )
        if self.metadata_stable_leader_index(timeout_s=30.0) is None:
            raise AssertionError(
                f"metadata cluster did not converge after node {index + 1} restart\n"
                f"{self.debug_logs()}"
            )

    def restart_data_node(self, index: int) -> None:
        self._terminate_process(self.data_procs[index])
        self.data_procs[index] = self._spawn_data(index, reserved_ports=False)
        if not wait_for_server(self.data_api_urls[index], timeout=30.0):
            raise AssertionError(
                f"data node {self.data_nodes[index]['node_id']} did not restart\n"
                f"{self.debug_logs()}"
            )
        if self.wait_for_data_stores(timeout_s=30.0) is None:
            raise AssertionError(
                f"data stores did not converge after node "
                f"{self.data_nodes[index]['node_id']} restart\n{self.debug_logs()}"
            )

    def stop(self, *, test_failed: bool = False) -> None:
        self.port_reservations.close()
        for proc in reversed([*self.data_procs, *self.metadata_procs]):
            self._terminate_process(proc)
        self.data_procs = []
        self.metadata_procs = []

        for handle in [*self.data_log_files, *self.metadata_log_files]:
            if not handle.closed:
                handle.close()
        if not maybe_preserve_tempdir(self.tempdir, failed=test_failed):
            self.tempdir.cleanup()


def _distributed_antfly_binary() -> str:
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    resolved = Path(binary)
    if resolved.name != "antfly":
        pytest.skip("distributed backup e2e requires the antfly binary")
    if not resolved.exists():
        pytest.skip(f"antfly binary not built: {resolved}")
    return str(resolved)


@pytest.fixture
def multi_metadata_backup_cluster(
    request: pytest.FixtureRequest,
) -> MultiMetadataBackupCluster:
    cluster = MultiMetadataBackupCluster(_distributed_antfly_binary())
    try:
        yield cluster
    finally:
        report = getattr(request.node, "rep_call", None)
        cluster.stop(test_failed=bool(report and report.failed))


@pytest.fixture
def replicated_backup_cluster(
    request: pytest.FixtureRequest,
) -> MultiMetadataBackupCluster:
    cluster = MultiMetadataBackupCluster(
        _distributed_antfly_binary(),
        data_node_count=3,
        replication_factor=3,
    )
    try:
        yield cluster
    finally:
        report = getattr(request.node, "rep_call", None)
        cluster.stop(test_failed=bool(report and report.failed))


def _table_identity(snapshot: dict, table_name: str) -> dict | None:
    tables = [
        table
        for table in snapshot.get("tables", [])
        if isinstance(table, dict) and table.get("name") == table_name
    ]
    if len(tables) != 1:
        return None
    table_id = int(tables[0].get("table_id", 0))
    ranges = [
        record
        for record in snapshot.get("ranges", [])
        if isinstance(record, dict) and int(record.get("table_id", 0)) == table_id
    ]
    if len(ranges) != 1:
        return None
    record = ranges[0]
    range_id = int(record.get("range_id", 0))
    group_id = int(record.get("group_id", 0))
    if table_id <= 0 or range_id <= 0 or group_id <= 0:
        return None
    return {
        "table_id": table_id,
        "range_id": range_id,
        "group_id": group_id,
        "doc_shard_id": int(record.get("doc_identity_shard_id") or group_id),
        "doc_range_id": int(record.get("doc_identity_range_id") or range_id),
    }


def _metadata_raft_state(cluster: MultiMetadataBackupCluster) -> list[dict] | None:
    statuses = cluster.metadata_statuses(request_timeout_s=2.0)
    if any(status is None for status in statuses):
        return None
    resolved = [status for status in statuses if status is not None]
    leaders = [
        status for status in resolved if status.get("metadata_raft_role") == "leader"
    ]
    if len(leaders) != 1:
        return None
    leader_id = int(leaders[0].get("metadata_raft_local_node_id", 0))
    fingerprints = {
        status.get("metadata_raft_voter_set_fingerprint") for status in resolved
    }
    group_ids = {int(status.get("metadata_group_id", 0)) for status in resolved}
    terms = {int(status.get("metadata_raft_term", 0)) for status in resolved}
    if (
        len(fingerprints) != 1
        or None in fingerprints
        or len(group_ids) != 1
        or min(group_ids) <= 0
        or len(terms) != 1
        or min(terms) <= 0
    ):
        return None
    for status in resolved:
        if (
            int(status.get("metadata_raft_leader_id", 0)) != leader_id
            or int(status.get("metadata_raft_commit_index", 0)) <= 0
            or status.get("metadata_raft_local_voter") is not True
            or int(status.get("metadata_raft_voter_count", 0)) != 3
            or status.get("metadata_raft_joint_consensus") is not False
            or int(status.get("metadata_raft_learner_count", -1)) != 0
            or int(status.get("metadata_raft_transport_served_groups", 0)) != 1
        ):
            return None
    if int(leaders[0].get("reconcile_lease_owner_node_id", 0)) != leader_id:
        return None
    return resolved


def _wait_for_metadata_raft(
    cluster: MultiMetadataBackupCluster, *, timeout_s: float
) -> list[dict] | None:
    return wait_until(
        lambda: _metadata_raft_state(cluster),
        timeout_s=timeout_s,
        interval_s=0.25,
    )


def _replicated_group_state(
    cluster: MultiMetadataBackupCluster,
    table_name: str,
    *,
    expected_doc_count: int,
    minimum_applied_index: int = 0,
    require_new_entry: bool = False,
) -> dict | None:
    try:
        snapshot = cluster.metadata_snapshot()
    except (AssertionError, requests.RequestException, ValueError):
        return None
    identity = _table_identity(snapshot, table_name)
    if identity is None:
        return None
    group_id = identity["group_id"]
    reports: list[dict] = []
    for store in snapshot.get("stores", []):
        if not isinstance(store, dict):
            continue
        if (
            store.get("role") != "data"
            or store.get("live") is not True
            or store.get("health_class") != "healthy"
        ):
            continue
        for status in store.get("group_statuses", []):
            if isinstance(status, dict) and int(status.get("group_id", 0)) == group_id:
                reports.append(
                    {
                        "store_id": int(store.get("store_id", 0)),
                        "node_id": int(store.get("node_id", 0)),
                        "api_url": store.get("api_url"),
                        "raft_url": store.get("raft_url"),
                        **status,
                    }
                )
    if len(reports) != cluster.replication_factor:
        return None
    fingerprints = {
        tuple(value) if isinstance(value, list) else value
        for value in (report.get("voter_set_fingerprint") for report in reports)
    }
    applied_indexes = [int(report.get("raft_applied_index", -1)) for report in reports]
    if len(fingerprints) != 1 or None in fingerprints or len(set(applied_indexes)) != 1:
        return None
    if require_new_entry:
        if min(applied_indexes) <= minimum_applied_index:
            return None
    elif min(applied_indexes) < minimum_applied_index:
        return None
    for report in reports:
        if (
            report.get("local_voter") is not True
            or int(report.get("voter_count", 0)) != cluster.replication_factor
            or report.get("voter_set_known") is not True
            or report.get("joint_consensus") is not False
            or int(report.get("doc_count", -1)) != expected_doc_count
        ):
            return None
    merged = [
        status
        for status in snapshot.get("merged_group_statuses", [])
        if isinstance(status, dict) and int(status.get("group_id", 0)) == group_id
    ]
    if len(merged) != 1:
        return None
    if (
        merged[0].get("leader_known") is not True
        or int(merged[0].get("leader_store_id", 0)) <= 0
        or int(merged[0].get("voter_count", 0)) != cluster.replication_factor
        or int(merged[0].get("healthy_voter_reports", 0)) != cluster.replication_factor
        or merged[0].get("joint_consensus") is not False
        or merged[0].get("transition_pending") is not False
    ):
        return None
    return {
        "identity": identity,
        "reports": reports,
        "merged": merged[0],
        "applied_index": applied_indexes[0],
    }


def _wait_for_replicated_group(
    cluster: MultiMetadataBackupCluster,
    table_name: str,
    *,
    expected_doc_count: int,
    timeout_s: float,
    minimum_applied_index: int = 0,
    require_new_entry: bool = False,
) -> dict | None:
    return wait_until(
        lambda: _replicated_group_state(
            cluster,
            table_name,
            expected_doc_count=expected_doc_count,
            minimum_applied_index=minimum_applied_index,
            require_new_entry=require_new_entry,
        ),
        timeout_s=timeout_s,
        interval_s=0.25,
    )


def _write_replicated_document(
    cluster: MultiMetadataBackupCluster,
    table_name: str,
    key: str,
    document: dict,
) -> None:
    response = requests.post(
        f"{cluster.data_api_url}/tables/{table_name}/batch",
        json={"inserts": {key: document}},
        timeout=30,
    )
    payload = _check_response(response)
    assert payload["inserted"] == 1


def _metadata_public_request(
    cluster: MultiMetadataBackupCluster,
    method: str,
    path: str,
    *,
    json_body: dict | None = None,
    timeout_s: float,
) -> dict:
    last_response: requests.Response | None = None
    for _ in range(3):
        url = cluster.metadata_leader_public_url(timeout_s=30.0)
        response = requests.request(
            method,
            f"{url}{path}",
            json=json_body,
            timeout=timeout_s,
        )
        if _is_metadata_not_leader_response(response):
            last_response = response
            continue
        return _check_response(response)
    raise AssertionError(
        "metadata leader stayed unavailable"
        + (f": {last_response.text}" if last_response is not None else "")
    )


def _wait_for_restore_job_on_cluster(
    cluster: MultiMetadataBackupCluster,
    job_id: int,
    *,
    timeout_s: float,
) -> dict | None:
    def terminal() -> dict | None:
        try:
            job = _metadata_public_request(
                cluster,
                "GET",
                f"/restore/jobs/{job_id}",
                timeout_s=10.0,
            )
        except (AssertionError, requests.RequestException, ValueError):
            return None
        return job if job.get("phase") in {"succeeded", "failed", "cancelled"} else None

    return wait_until(terminal, timeout_s=timeout_s, interval_s=0.25)


def _wait_for_group_retired(
    cluster: MultiMetadataBackupCluster,
    group_id: int,
    *,
    timeout_s: float,
) -> bool:
    deadline = time.monotonic() + timeout_s
    confirmations = 0
    while time.monotonic() < deadline:
        try:
            snapshot = cluster.metadata_snapshot()
        except (AssertionError, requests.RequestException, ValueError):
            confirmations = 0
            time.sleep(0.25)
            continue
        group_present = (
            any(
                isinstance(record, dict) and int(record.get("group_id", 0)) == group_id
                for record in snapshot.get("ranges", [])
            )
            or any(
                isinstance(intent, dict)
                and int(intent.get("record", {}).get("group_id", 0)) == group_id
                for intent in snapshot.get("placement_intents", [])
            )
            or any(
                isinstance(status, dict) and int(status.get("group_id", 0)) == group_id
                for status in snapshot.get("merged_group_statuses", [])
            )
            or any(
                isinstance(status, dict) and int(status.get("group_id", 0)) == group_id
                for store in snapshot.get("stores", [])
                if isinstance(store, dict)
                for status in [
                    *store.get("group_statuses", []),
                    *store.get("runtime_statuses", []),
                ]
            )
        )
        confirmations = 0 if group_present else confirmations + 1
        if confirmations >= 3:
            return True
        time.sleep(0.25)
    return False


def _corpus_hash_from_data_node(
    api_url: str,
    table_name: str,
    documents: dict[str, dict],
) -> str:
    rows: list[str] = []
    for key, expected in sorted(documents.items()):
        response = requests.get(
            f"{api_url}/tables/{table_name}/documents/{key}", timeout=10
        )
        actual = _check_response(response)
        assert actual == expected
        rows.append(
            f"{key}\t{json.dumps(actual, separators=(',', ':'), sort_keys=True)}\n"
        )
    return hashlib.sha256("".join(rows).encode()).hexdigest()


@pytest.mark.slow
def test_replicated_backup_restore_survives_sequential_member_restarts(
    replicated_backup_cluster: MultiMetadataBackupCluster,
) -> None:
    cluster = replicated_backup_cluster
    table_name = f"replicated_backup_restore_{time.time_ns()}"
    backup_id = f"replicated-backup-{time.time_ns()}"
    documents: dict[str, dict] = {}

    assert _wait_for_metadata_raft(cluster, timeout_s=30.0) is not None, (
        cluster.debug_logs()
    )
    created = _check_response(
        requests.post(
            f"{cluster.metadata_follower_public_url()}/tables/{table_name}",
            json={"num_shards": 1, "description": "replicated backup restore"},
            timeout=30,
        )
    )
    assert created["name"] == table_name
    group = _wait_for_replicated_group(
        cluster, table_name, expected_doc_count=0, timeout_s=60.0
    )
    assert group is not None, cluster.debug_logs()
    source_identity = group["identity"]

    documents["before-roll"] = {"phase": "before-roll", "sequence": 1}
    _write_replicated_document(
        cluster, table_name, "before-roll", documents["before-roll"]
    )
    group = _wait_for_replicated_group(
        cluster, table_name, expected_doc_count=1, timeout_s=60.0
    )
    assert group is not None, cluster.debug_logs()

    metadata_leader = cluster.metadata_stable_leader_index(timeout_s=30.0)
    assert metadata_leader is not None
    for index in [metadata_leader, *[i for i in range(3) if i != metadata_leader]]:
        cluster.restart_metadata_node(index)
        assert _wait_for_metadata_raft(cluster, timeout_s=30.0) is not None, (
            cluster.debug_logs()
        )

    documents["after-metadata"] = {"phase": "after-metadata", "sequence": 2}
    _write_replicated_document(
        cluster, table_name, "after-metadata", documents["after-metadata"]
    )
    group = _wait_for_replicated_group(
        cluster, table_name, expected_doc_count=2, timeout_s=60.0
    )
    assert group is not None, cluster.debug_logs()

    leader_store_id = int(group["merged"]["leader_store_id"])
    data_leader = next(
        index
        for index, node in enumerate(cluster.data_nodes)
        if int(node["store_id"]) == leader_store_id
    )
    for index in [data_leader, *[i for i in range(3) if i != data_leader]]:
        before = _replicated_group_state(
            cluster, table_name, expected_doc_count=len(documents)
        )
        assert before is not None, cluster.debug_logs()
        baseline_index = int(before["applied_index"])
        node = cluster.data_nodes[index]

        cluster.restart_data_node(index)
        rejoined = _wait_for_replicated_group(
            cluster,
            table_name,
            expected_doc_count=len(documents),
            timeout_s=60.0,
            minimum_applied_index=baseline_index,
        )
        assert rejoined is not None, cluster.debug_logs()
        target = [
            report
            for report in rejoined["reports"]
            if int(report["node_id"]) == int(node["node_id"])
        ]
        assert len(target) == 1
        assert int(target[0]["store_id"]) == int(node["store_id"])
        assert int(target[0]["raft_applied_index"]) == int(rejoined["applied_index"])

        key = f"after-data-{index}"
        documents[key] = {"phase": "data-rejoin", "ordinal": index}
        _write_replicated_document(cluster, table_name, key, documents[key])
        advanced = _wait_for_replicated_group(
            cluster,
            table_name,
            expected_doc_count=len(documents),
            timeout_s=60.0,
            minimum_applied_index=baseline_index,
            require_new_entry=True,
        )
        assert advanced is not None, cluster.debug_logs()

    expected_hash = _corpus_hash_from_data_node(
        cluster.data_api_url, table_name, documents
    )
    assert expected_hash == EXPECTED_CORPUS_SHA256
    for api_url in cluster.data_api_urls[1:]:
        assert (
            _corpus_hash_from_data_node(api_url, table_name, documents) == expected_hash
        )

    backup_dir = cluster.root / "backups"
    backup_dir.mkdir()
    location = _file_location(backup_dir)
    backup = _metadata_public_request(
        cluster,
        "POST",
        "/backup",
        json_body={
            "backup_id": backup_id,
            "location": location,
            "connection": BACKUP_CONNECTION,
            "table_names": [table_name],
        },
        timeout_s=120.0,
    )
    assert backup["status"] == "completed", backup
    assert [table["name"] for table in backup["tables"]] == [table_name]

    _check_success(
        requests.delete(
            f"{cluster.metadata_follower_public_url()}/tables/{table_name}", timeout=30
        )
    )
    assert _wait_for_group_retired(
        cluster, source_identity["group_id"], timeout_s=120.0
    ), cluster.debug_logs()

    accepted = _metadata_public_request(
        cluster,
        "POST",
        "/restore",
        json_body={
            "backup_id": backup_id,
            "location": location,
            "connection": BACKUP_CONNECTION,
            "table_names": [table_name],
            "restore_mode": "fail_if_exists",
        },
        timeout_s=120.0,
    )
    restored_job = _wait_for_restore_job_on_cluster(
        cluster, int(accepted["job_id"]), timeout_s=180.0
    )
    assert restored_job is not None, cluster.debug_logs()
    assert restored_job["phase"] == "succeeded", restored_job

    restored = _wait_for_replicated_group(
        cluster,
        table_name,
        expected_doc_count=len(documents),
        timeout_s=180.0,
    )
    assert restored is not None, cluster.debug_logs()
    restored_identity = restored["identity"]
    assert restored_identity["table_id"] == source_identity["table_id"]
    assert restored_identity["range_id"] != source_identity["range_id"]
    assert restored_identity["group_id"] != source_identity["group_id"]
    assert restored_identity["doc_shard_id"] != source_identity["doc_shard_id"]
    assert restored_identity["doc_range_id"] != source_identity["doc_range_id"]
    assert restored_identity["range_id"] == restored_identity["group_id"]
    assert restored_identity["doc_shard_id"] == restored_identity["group_id"]
    assert restored_identity["doc_range_id"] == restored_identity["range_id"]

    for api_url in cluster.data_api_urls:
        assert (
            _corpus_hash_from_data_node(api_url, table_name, documents) == expected_hash
        )

    documents["after-restore"] = {"phase": "post-restore-write", "sequence": 6}
    _write_replicated_document(
        cluster, table_name, "after-restore", documents["after-restore"]
    )
    final_group = _wait_for_replicated_group(
        cluster,
        table_name,
        expected_doc_count=len(documents),
        timeout_s=60.0,
        minimum_applied_index=int(restored["applied_index"]),
        require_new_entry=True,
    )
    assert final_group is not None, cluster.debug_logs()
    assert _wait_for_metadata_raft(cluster, timeout_s=30.0) is not None

    _check_success(
        requests.delete(
            f"{cluster.metadata_follower_public_url()}/tables/{table_name}", timeout=30
        )
    )
    assert _wait_for_group_retired(
        cluster, restored_identity["group_id"], timeout_s=120.0
    ), cluster.debug_logs()


def test_table_backup_restore_round_trip(backup_api):
    table_name = f"backup_restore_{time.time_ns()}"
    backup_id = f"backup-{time.time_ns()}"

    created = backup_api.create_table(
        table_name, num_shards=1, description="backup restore docs"
    )
    assert created["name"] == table_name
    assert "full_text_index_v0" in created["indexes"]

    docs = {
        "doc:db": {
            "title": "Distributed Databases",
            "content": "Distributed databases replicate state across nodes and coordinate writes with consensus.",
        },
        "doc:vector": {
            "title": "Vector Search",
            "content": "Vector search uses embeddings to retrieve semantically similar documents.",
        },
        "doc:raft": {
            "title": "Raft Consensus",
            "content": "Raft coordinates leaders and followers to keep replicated logs consistent.",
        },
    }
    batch = backup_api.batch_write(table_name, inserts=docs, sync_level="full_text")
    assert batch["inserted"] == len(docs)
    assert wait_until(
        lambda: _top_hit(backup_api, table_name, "distributed consensus", "doc:db"),
        timeout_s=60.0,
        interval_s=1.0,
    )

    with tempfile.TemporaryDirectory(prefix="antfly-backup-") as backup_dir:
        location = _file_location(backup_dir)

        backup = backup_api.backup_table(
            table_name, backup_id=backup_id, location=location
        )
        assert backup["backup"] == "successful"

        deleted = backup_api.delete_table(table_name)
        assert deleted == {}

        _wait_until_table_absent(backup_api, table_name, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(
            backup_api, table_name, "doc:db", timeout_s=10.0, interval_s=0.5
        )

        restore = backup_api.restore_table(
            table_name, backup_id=backup_id, location=location
        )
        assert restore == {"restore": "triggered"}

        restored_doc = wait_until(
            lambda: _lookup_doc(backup_api, table_name, "doc:db"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        assert restored_doc is not None, "restored document did not reappear"
        assert restored_doc["title"] == "Distributed Databases"
        assert "consensus" in restored_doc["content"]
        assert wait_until(
            lambda: _top_hit(backup_api, table_name, "distributed consensus", "doc:db"),
            timeout_s=60.0,
            interval_s=1.0,
        )


def test_table_backup_restore_round_trip_managed_chunked_semantic(
    backup_api, slow_openai_embedder
):
    table_name = f"backup_chunked_semantic_{time.time_ns()}"
    backup_id = f"backup-chunked-semantic-{time.time_ns()}"

    created = backup_api.create_table(
        table_name, num_shards=1, description="chunked semantic backup docs"
    )
    assert created["name"] == table_name

    assert_created_index(
        backup_api.create_index(
            table_name,
            "semantic_chunked_idx",
            {
                "name": "semantic_chunked_idx",
                "type": "embeddings",
                "field": "content",
                "dimension": 3,
                "embedder": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "url": slow_openai_embedder,
                },
                "chunker": {
                    "provider": "antfly",
                    "model": "fixed-bert-tokenizer",
                    "store_chunks": True,
                    "text": {
                        "target_tokens": 4,
                        "overlap_tokens": 1,
                        "separator": " ",
                    },
                },
            },
        ),
        "semantic_chunked_idx",
        "embeddings",
    )

    backup_api.wait_index_ready(
        table_name, "semantic_chunked_idx", timeout_s=30.0, interval_s=0.5
    )

    batch = backup_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Alpha backup",
                "content": "alpha body alpha body alpha body alpha body alpha tail",
            },
            "doc:b": {
                "title": "Beta backup",
                "content": "beta body beta body beta body beta tail",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    before_scan = wait_until(
        lambda: _chunked_doc(
            backup_api, table_name, "doc:a", "semantic_chunked_idx_chunks"
        ),
        timeout_s=60.0,
        interval_s=1.0,
    )
    assert before_scan is not None

    assert wait_until(
        lambda: _semantic_top_hit(
            backup_api, table_name, "alpha concept", "semantic_chunked_idx", "doc:a"
        ),
        timeout_s=120.0,
        interval_s=1.0,
    )

    before_chunks = before_scan[0]["_chunks"]["semantic_chunked_idx_chunks"]
    assert len(before_chunks) >= 2

    with tempfile.TemporaryDirectory(
        prefix="antfly-backup-chunked-semantic-"
    ) as backup_dir:
        location = _file_location(backup_dir)

        backup = backup_api.backup_table(
            table_name, backup_id=backup_id, location=location
        )
        assert backup["backup"] == "successful"

        deleted = backup_api.delete_table(table_name)
        assert deleted == {}

        _wait_until_table_absent(backup_api, table_name, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(
            backup_api, table_name, "doc:a", timeout_s=10.0, interval_s=0.5
        )

        restore = backup_api.restore_table(
            table_name, backup_id=backup_id, location=location
        )
        assert restore == {"restore": "triggered"}

        restored_doc = wait_until(
            lambda: _lookup_doc(backup_api, table_name, "doc:a"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        assert restored_doc is not None
        assert restored_doc["title"] == "Alpha backup"

        backup_api.wait_index_ready(
            table_name,
            "semantic_chunked_idx",
            timeout_s=180.0,
            interval_s=1.0,
            require_query_fresh=True,
        )

        semantic_after = wait_until(
            lambda: _semantic_top_hit(
                backup_api, table_name, "alpha concept", "semantic_chunked_idx", "doc:a"
            ),
            timeout_s=120.0,
            interval_s=1.0,
        )
        if semantic_after is None:
            after_status = backup_api.get_index(table_name, "semantic_chunked_idx")
            after_scan = backup_api.scan_keys(
                table_name,
                {
                    "from": "doc:a",
                    "to": "doc:a;",
                    "inclusive_from": True,
                    "fields": ["title", "_chunks", "_embeddings"],
                },
            )
            after_query = backup_api.query_table(
                table_name,
                {
                    "semantic_search": "alpha concept",
                    "indexes": ["semantic_chunked_idx"],
                    "limit": 5,
                    "fields": ["title", "_chunks", "_embeddings"],
                },
            )
            server_logs = backup_api.debug_logs()
            raise AssertionError(
                "semantic restore query did not recover; "
                f"status={after_status}, scan={after_scan}, query={after_query}, logs={server_logs}"
            )
        assert semantic_after

        after_scan = wait_until(
            lambda: _chunked_doc(
                backup_api, table_name, "doc:a", "semantic_chunked_idx_chunks"
            ),
            timeout_s=60.0,
            interval_s=1.0,
        )
        assert after_scan is not None
        assert after_scan[0]["title"] == "Alpha backup"
        after_chunks = after_scan[0]["_chunks"]["semantic_chunked_idx_chunks"]
        assert len(after_chunks) >= 2


def test_cluster_backup_restore_round_trip(backup_api):
    table_a = f"cluster_backup_a_{time.time_ns()}"
    table_b = f"cluster_backup_b_{time.time_ns()}"
    backup_id = f"cluster-backup-{time.time_ns()}"

    for table_name, title in (
        (table_a, "Cluster Backup Alpha"),
        (table_b, "Cluster Backup Beta"),
    ):
        created = backup_api.create_table(
            table_name, num_shards=1, description=f"{table_name} docs"
        )
        assert created["name"] == table_name
        batch = backup_api.batch_write(
            table_name,
            inserts={
                "doc:1": {
                    "title": title,
                    "content": f"{title} survives backup and restore.",
                }
            },
            sync_level="full_text",
        )
        assert batch["inserted"] == 1
        assert wait_until(
            lambda tn=table_name, q=title.lower(), doc_id="doc:1": _top_hit(
                backup_api, tn, q, doc_id
            ),
            timeout_s=60.0,
            interval_s=1.0,
        )

    with tempfile.TemporaryDirectory(prefix="antfly-cluster-backup-") as backup_dir:
        location = _file_location(backup_dir)

        backup = backup_api.cluster_backup(backup_id=backup_id, location=location)
        assert backup["backup_id"] == backup_id
        assert backup["status"] == "completed"
        assert {table["name"] for table in backup["tables"]} == {table_a, table_b}

        listed = backup_api.list_backups(location=location)
        backups = listed["backups"]
        matched = [item for item in backups if item["backup_id"] == backup_id]
        assert len(matched) == 1
        assert set(matched[0]["tables"]) == {table_a, table_b}

        backup_api.delete_table(table_a)
        backup_api.delete_table(table_b)
        _wait_until_table_absent(backup_api, table_a, timeout_s=10.0, interval_s=0.5)
        _wait_until_table_absent(backup_api, table_b, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_a, "doc:1", timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_b, "doc:1", timeout_s=10.0, interval_s=0.5)

        restore = backup_api.cluster_restore(
            backup_id=backup_id,
            location=location,
            restore_mode="fail_if_exists",
        )
        assert restore["status"] == "completed"
        assert restore["committed_table_count"] == 2
        assert restore["triggered_table_count"] == 0
        assert restore["skipped_table_count"] == 0
        assert restore["failed_table_count"] == 0

        for table_name, expected_title in (
            (table_a, "Cluster Backup Alpha"),
            (table_b, "Cluster Backup Beta"),
        ):
            restored_doc = wait_until(
                lambda tn=table_name: _lookup_doc(backup_api, tn, "doc:1"),
                timeout_s=60.0,
                interval_s=1.0,
            )
            assert restored_doc is not None
            assert restored_doc["title"] == expected_title


def test_cluster_backup_through_metadata_leader_public_api(
    multi_metadata_backup_cluster: MultiMetadataBackupCluster,
) -> None:
    cluster = multi_metadata_backup_cluster
    table_name = f"metadata_leader_backup_{time.time_ns()}"
    backup_id = f"metadata-leader-backup-{time.time_ns()}"
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Connection"] = "close"

    created = _check_response(
        session.post(
            f"{cluster.data_api_url}/tables/{table_name}",
            json={"num_shards": 1, "description": "metadata leader backup docs"},
            timeout=30,
        )
    )
    assert created["name"] == table_name

    batch = _check_response(
        session.post(
            f"{cluster.data_api_url}/tables/{table_name}/batch",
            json={
                "inserts": {
                    "doc:1": {
                        "title": "Leader Backup",
                        "content": "cluster backup requests are routed to metadata leaders",
                    }
                }
            },
            timeout=30,
        )
    )
    assert batch["inserted"] == 1
    assert wait_until(
        lambda: _lookup_doc_from_url(
            session, cluster.data_api_url, table_name, "doc:1"
        ),
        timeout_s=30.0,
        interval_s=0.5,
    ), cluster.debug_logs()

    with tempfile.TemporaryDirectory(
        prefix="antfly-metadata-leader-cluster-backup-"
    ) as backup_dir:
        backup = None
        last_response: requests.Response | None = None
        for _ in range(3):
            leader_public_url = cluster.metadata_leader_public_url(timeout_s=30.0)
            response = session.post(
                f"{leader_public_url}/backup",
                json={
                    "backup_id": backup_id,
                    "location": _file_location(backup_dir),
                    "connection": BACKUP_CONNECTION,
                    "table_names": [table_name],
                },
                timeout=120,
            )
            if _is_metadata_not_leader_response(response):
                last_response = response
                continue
            backup = _check_response(response)
            break
        assert backup is not None, (
            f"metadata leader stayed unavailable for backup after retries; "
            f"last_response={last_response.text if last_response is not None else None}\n{cluster.debug_logs()}"
        )

        assert backup["backup_id"] == backup_id
        assert backup["status"] == "completed", (
            f"backup={backup}\n{cluster.debug_logs()}"
        )
        assert [table["name"] for table in backup["tables"]] == [table_name]


@pytest.mark.objectstore_integration
@pytest.mark.parametrize("backend", ["s3", "gs"])
def test_cluster_backup_restore_round_trip_remote_backend(backup_api, backend: str):
    location = _remote_backup_location(backend)
    table_a = f"cluster_{backend}_a_{time.time_ns()}"
    table_b = f"cluster_{backend}_b_{time.time_ns()}"
    backup_id = f"cluster-{backend}-backup-{time.time_ns()}"

    for table_name, title in (
        (table_a, f"{backend.upper()} Backup Alpha"),
        (table_b, f"{backend.upper()} Backup Beta"),
    ):
        created = backup_api.create_table(
            table_name, num_shards=1, description=f"{table_name} docs"
        )
        assert created["name"] == table_name
        batch = backup_api.batch_write(
            table_name,
            inserts={
                "doc:1": {
                    "title": title,
                    "content": f"{title} survives remote backup and restore.",
                }
            },
            sync_level="full_text",
        )
        assert batch["inserted"] == 1
        assert wait_until(
            lambda tn=table_name, q=title.lower(), doc_id="doc:1": _top_hit(
                backup_api, tn, q, doc_id
            ),
            timeout_s=60.0,
            interval_s=1.0,
        )

    backup = backup_api.cluster_backup(backup_id=backup_id, location=location)
    assert backup["backup_id"] == backup_id
    assert backup["status"] == "completed"
    assert {table["name"] for table in backup["tables"]} == {table_a, table_b}

    listed = backup_api.list_backups(location=location)
    backups = listed["backups"]
    matched = [item for item in backups if item["backup_id"] == backup_id]
    assert len(matched) == 1
    assert set(matched[0]["tables"]) == {table_a, table_b}
    assert matched[0]["location"] == location

    backup_api.delete_table(table_a)
    backup_api.delete_table(table_b)
    _wait_until_table_absent(backup_api, table_a, timeout_s=10.0, interval_s=0.5)
    _wait_until_table_absent(backup_api, table_b, timeout_s=10.0, interval_s=0.5)

    restore = backup_api.cluster_restore(
        backup_id=backup_id,
        location=location,
        restore_mode="fail_if_exists",
    )
    assert restore["status"] == "completed"
    assert restore["committed_table_count"] == 2
    assert restore["triggered_table_count"] == 0
    assert restore["skipped_table_count"] == 0
    assert restore["failed_table_count"] == 0

    for table_name, expected_title in (
        (table_a, f"{backend.upper()} Backup Alpha"),
        (table_b, f"{backend.upper()} Backup Beta"),
    ):
        restored_doc = wait_until(
            lambda tn=table_name: _lookup_doc(backup_api, tn, "doc:1"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        assert restored_doc is not None
        assert restored_doc["title"] == expected_title


def test_cluster_restore_modes(backup_api):
    table_a = f"cluster_modes_a_{time.time_ns()}"
    table_b = f"cluster_modes_b_{time.time_ns()}"
    backup_id = f"cluster-modes-{time.time_ns()}"

    for table_name, title in (
        (table_a, "Original Alpha"),
        (table_b, "Original Beta"),
    ):
        created = backup_api.create_table(
            table_name, num_shards=1, description=f"{table_name} docs"
        )
        assert created["name"] == table_name
        _write_single_doc(
            backup_api,
            table_name,
            "doc:1",
            title=title,
            content=f"{title} backup source",
        )
        assert wait_until(
            lambda tn=table_name, q=title.lower(), doc_id="doc:1": _top_hit(
                backup_api, tn, q, doc_id
            ),
            timeout_s=60.0,
            interval_s=1.0,
        )

    with tempfile.TemporaryDirectory(prefix="antfly-cluster-modes-") as backup_dir:
        location = _file_location(backup_dir)

        backup = backup_api.cluster_backup(backup_id=backup_id, location=location)
        assert backup["status"] == "completed"

        fail_resp = backup_api._request(
            "POST",
            "/restore",
            {
                "backup_id": backup_id,
                "location": location,
                "connection": BACKUP_CONNECTION,
                "restore_mode": "fail_if_exists",
            },
        )
        failed_job = _wait_for_terminal_restore_job(backup_api, fail_resp)
        assert failed_job["phase"] == "failed"
        assert failed_job["error"] == "TableAlreadyExists"

        _write_single_doc(
            backup_api, table_a, "doc:1", title="Mutated Alpha", content="mutated alpha"
        )
        _write_single_doc(
            backup_api, table_b, "doc:1", title="Mutated Beta", content="mutated beta"
        )
        mutated_a = wait_until(
            lambda: _lookup_doc(backup_api, table_a, "doc:1"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        mutated_b = wait_until(
            lambda: _lookup_doc(backup_api, table_b, "doc:1"),
            timeout_s=30.0,
            interval_s=1.0,
        )
        assert mutated_a is not None and mutated_a["title"] == "Mutated Alpha"
        assert mutated_b is not None and mutated_b["title"] == "Mutated Beta"

        skip_restore = backup_api.cluster_restore(
            backup_id=backup_id,
            location=location,
            restore_mode="skip_if_exists",
        )
        assert skip_restore["status"] == "completed"
        assert skip_restore["committed_table_count"] == 0
        assert skip_restore["triggered_table_count"] == 0
        assert skip_restore["skipped_table_count"] == 2
        assert skip_restore["failed_table_count"] == 0

        skipped_a = _lookup_doc(backup_api, table_a, "doc:1")
        skipped_b = _lookup_doc(backup_api, table_b, "doc:1")
        assert skipped_a is not None and skipped_a["title"] == "Mutated Alpha"
        assert skipped_b is not None and skipped_b["title"] == "Mutated Beta"

        overwrite_restore = backup_api.cluster_restore(
            backup_id=backup_id,
            location=location,
            restore_mode="overwrite",
        )
        assert overwrite_restore["status"] == "completed", overwrite_restore
        assert overwrite_restore["committed_table_count"] == 2, overwrite_restore
        assert overwrite_restore["triggered_table_count"] == 0, overwrite_restore
        assert overwrite_restore["skipped_table_count"] == 0, overwrite_restore
        assert overwrite_restore["failed_table_count"] == 0, overwrite_restore

        restored_docs = wait_until(
            lambda: _lookup_docs(backup_api, (table_a, table_b), "doc:1"),
            timeout_s=60.0,
            interval_s=1.0,
        )
        assert restored_docs is not None
        assert restored_docs[table_a]["title"] == "Original Alpha"
        assert restored_docs[table_b]["title"] == "Original Beta"


def test_partial_cluster_backup_is_not_published_and_can_retry(backup_api):
    table_name = f"cluster_partial_{time.time_ns()}"
    missing_table = f"cluster_partial_missing_{time.time_ns()}"
    backup_id = f"cluster-partial-{time.time_ns()}"

    created = backup_api.create_table(
        table_name, num_shards=1, description="partial backup docs"
    )
    assert created["name"] == table_name
    _write_single_doc(
        backup_api,
        table_name,
        "doc:1",
        title="Partial Table",
        content="table survives partial backup",
    )
    assert wait_until(
        lambda: _top_hit(backup_api, table_name, "partial table", "doc:1"),
        timeout_s=60.0,
        interval_s=1.0,
    )

    with tempfile.TemporaryDirectory(prefix="antfly-cluster-partial-") as backup_dir:
        location = _file_location(backup_dir)

        backup = backup_api.cluster_backup(
            backup_id=backup_id,
            location=location,
            table_names=[table_name, missing_table],
        )
        assert backup["status"] == "partial"
        by_name = {table["name"]: table for table in backup["tables"]}
        assert by_name[table_name]["status"] == "completed"
        assert by_name[missing_table]["status"] == "failed"
        assert "not found" in by_name[missing_table]["error"]

        # A partial attempt is diagnostic output, not a restorable aggregate.
        # It must remain absent from discovery, and cleanup must release the
        # reservation and all per-table artifacts so the same id is reusable.
        listed = backup_api.list_backups(location=location)
        matched = [item for item in listed["backups"] if item["backup_id"] == backup_id]
        assert matched == []

        created = backup_api.create_table(
            missing_table, num_shards=1, description="retry backup docs"
        )
        assert created["name"] == missing_table
        _write_single_doc(
            backup_api,
            missing_table,
            "doc:2",
            title="Recovered Missing Table",
            content="Recovered Missing Table retry publishes a complete aggregate",
        )
        assert wait_until(
            lambda: _top_hit(
                backup_api, missing_table, "recovered missing table", "doc:2"
            ),
            timeout_s=60.0,
            interval_s=1.0,
        )

        retried = backup_api.cluster_backup(
            backup_id=backup_id,
            location=location,
            table_names=[table_name, missing_table],
        )
        assert retried["status"] == "completed"
        assert {table["name"] for table in retried["tables"]} == {
            table_name,
            missing_table,
        }

        listed = backup_api.list_backups(location=location)
        matched = [item for item in listed["backups"] if item["backup_id"] == backup_id]
        assert len(matched) == 1
        assert set(matched[0]["tables"]) == {table_name, missing_table}

        backup_api.delete_table(table_name)
        backup_api.delete_table(missing_table)
        for deleted_table, doc_id in ((table_name, "doc:1"), (missing_table, "doc:2")):
            _wait_until_table_absent(
                backup_api, deleted_table, timeout_s=10.0, interval_s=0.5
            )
            _wait_until_absent(
                backup_api, deleted_table, doc_id, timeout_s=10.0, interval_s=0.5
            )

        restore = backup_api.cluster_restore(
            backup_id=backup_id,
            location=location,
            restore_mode="fail_if_exists",
        )
        assert restore["status"] == "completed"
        assert restore["committed_table_count"] == 2
        assert restore["triggered_table_count"] == 0
        assert restore["skipped_table_count"] == 0
        assert restore["failed_table_count"] == 0

        restored_doc = wait_until(
            lambda: _lookup_doc(backup_api, table_name, "doc:1"),
            timeout_s=60.0,
            interval_s=1.0,
        )
        assert restored_doc is not None
        assert restored_doc["title"] == "Partial Table"
        restored_missing_doc = wait_until(
            lambda: _lookup_doc(backup_api, missing_table, "doc:2"),
            timeout_s=60.0,
            interval_s=1.0,
        )
        assert restored_missing_doc is not None
        assert restored_missing_doc["title"] == "Recovered Missing Table"


def test_backup_restore_request_validation(backup_api):
    with tempfile.TemporaryDirectory(prefix="antfly-backup-validate-") as backup_dir:
        location = _file_location(backup_dir)
        table_name = f"validate_backup_case_{time.time_ns()}"

        created = backup_api.create_table(table_name, num_shards=1)
        assert created["name"] == table_name

        invalid_cases = (
            ("POST", f"/tables/{table_name}/backup", {}, "invalid backup request"),
            ("POST", f"/tables/{table_name}/restore", {}, "invalid restore request"),
            ("POST", "/backup", {}, "invalid backup request"),
            ("POST", "/restore", {}, "invalid restore request"),
            (
                "POST",
                f"/tables/{table_name}/backup",
                {
                    "backup_id": "snap",
                    "location": "ftp://bucket/path",
                    "connection": BACKUP_CONNECTION,
                },
                "unsupported backup location",
            ),
            (
                "POST",
                f"/tables/{table_name}/restore",
                {
                    "backup_id": "snap",
                    "location": "ftp://bucket/path",
                    "connection": BACKUP_CONNECTION,
                },
                "unsupported backup location",
            ),
            (
                "POST",
                "/backup",
                {
                    "backup_id": "snap",
                    "location": "ftp://bucket/path",
                    "connection": BACKUP_CONNECTION,
                },
                "unsupported backup location",
            ),
            (
                "POST",
                "/restore",
                {
                    "backup_id": "snap",
                    "location": "ftp://bucket/path",
                    "connection": BACKUP_CONNECTION,
                },
                "unsupported backup location",
            ),
            (
                "POST",
                "/restore",
                {
                    "backup_id": "snap",
                    "location": location,
                    "connection": BACKUP_CONNECTION,
                    "restore_mode": "bogus",
                },
                "invalid restore mode",
            ),
        )

        for method, path, payload, expected in invalid_cases:
            response = backup_api._request(method, path, payload)
            assert response.status_code == 400
            assert expected in response.text

        missing_location = backup_api.s.get(f"{backup_api.url}/backups", timeout=30)
        assert missing_location.status_code == 400
        assert "Missing required query parameter: location" in missing_location.text

        unsupported_location = backup_api.s.get(
            f"{backup_api.url}/backups?location=ftp://bucket/path&connection={BACKUP_CONNECTION}",
            timeout=30,
        )
        assert unsupported_location.status_code == 400
        assert "unsupported backup location" in unsupported_location.text

        encoded_location = backup_api.s.get(
            f"{backup_api.url}/backups",
            params={
                "location": "ftp://bucket/path",
                "connection": BACKUP_CONNECTION,
            },
            timeout=30,
        )
        assert encoded_location.status_code == 400
        assert "unsupported backup location" in encoded_location.text


def test_list_backups_empty_location(backup_api):
    with tempfile.TemporaryDirectory(prefix="antfly-empty-backups-") as backup_dir:
        location = _file_location(backup_dir)
        listed = backup_api.list_backups(location=location)
        assert listed == {"backups": []}


def test_restore_missing_backup_returns_bad_request(backup_api):
    table_name = f"restore_missing_{time.time_ns()}"
    missing_backup_id = f"missing-{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    with tempfile.TemporaryDirectory(prefix="antfly-missing-restore-") as backup_dir:
        location = _file_location(backup_dir)

        table_restore = backup_api._request(
            "POST",
            f"/tables/{table_name}/restore",
            {
                "backup_id": missing_backup_id,
                "location": location,
                "connection": BACKUP_CONNECTION,
            },
        )
        # Table restore must inspect the manifest before it can authorize every
        # stored destination. A missing manifest is therefore rejected at
        # admission and never consumes a durable job slot.
        assert table_restore.status_code == 400
        assert table_restore.json() == {"error": "invalid backup manifest"}

        cluster_restore = backup_api._request(
            "POST",
            "/restore",
            {
                "backup_id": missing_backup_id,
                "location": location,
                "connection": BACKUP_CONNECTION,
                "restore_mode": "fail_if_exists",
            },
        )
        cluster_job = _wait_for_terminal_restore_job(backup_api, cluster_restore)
        assert cluster_job["phase"] == "failed"
        assert cluster_job["error"] == "InvalidRequest"
