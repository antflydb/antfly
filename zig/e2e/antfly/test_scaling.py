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

"""Metadata scaling and node-shutdown API tests."""

from __future__ import annotations

import json
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
    _read_log_tail,
    antfly_public_api_url,
    find_free_port,
    lookup_key_path,
    maybe_preserve_tempdir,
    wait_for_server,
)
from helpers import wait_until


def _metadata_admin_url(stateful_api) -> str:
    server = getattr(stateful_api, "_server", None)
    admin_url = getattr(server, "metadata_admin_url", None)
    if not admin_url:
        pytest.skip("node shutdown e2e requires the local stateful metadata admin server")
    return str(admin_url).rstrip("/")


def _admin_snapshot(admin_url: str) -> dict:
    resp = requests.get(f"{admin_url}/metadata/v1/admin/snapshot", timeout=10)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _find_store(snapshot: dict, store_id: int) -> dict:
    for store in snapshot.get("stores", []):
        if isinstance(store, dict) and store.get("store_id") == store_id:
            return store
    raise AssertionError(f"store {store_id} not found in metadata snapshot: {snapshot!r}")


def _maybe_find_store(snapshot: dict, store_id: int) -> dict | None:
    for store in snapshot.get("stores", []):
        if isinstance(store, dict) and store.get("store_id") == store_id:
            return store
    return None


def _maybe_find_node(snapshot: dict, node_id: int) -> dict | None:
    for node in snapshot.get("nodes", []):
        if isinstance(node, dict) and node.get("node_id") == node_id:
            return node
    return None


def test_node_shutdown_preserves_drain_intent_across_healthy_status(stateful_api):
    admin_url = _metadata_admin_url(stateful_api)
    node_id = 99
    store_id = 99

    node_resp = requests.post(
        f"{admin_url}/internal/v1/nodes",
        json={"node_id": node_id, "role": "data"},
        timeout=10,
    )
    assert node_resp.status_code == 202, node_resp.text

    store_resp = requests.post(
        f"{admin_url}/internal/v1/nodes",
        json={
            "store_id": store_id,
            "node_id": node_id,
            "role": "data",
            "health_class": "healthy",
            "live": True,
            "capacity_bytes": 1024,
            "available_bytes": 900,
        },
        timeout=10,
    )
    assert store_resp.status_code == 202, store_resp.text
    assert (
        wait_until(
            lambda: _maybe_find_store(
                _admin_snapshot(admin_url),
                store_id,
            ),
            timeout_s=5,
            interval_s=0.1,
        )
        is not None
    )

    shutdown_resp = requests.put(
        f"{admin_url}/internal/v1/nodes/{node_id}/shutdown",
        json={"type": "remove", "reason": "e2e"},
        timeout=10,
    )
    assert shutdown_resp.status_code == 202, shutdown_resp.text
    assert (
        wait_until(
            lambda: (
                store
                if (store := _maybe_find_store(_admin_snapshot(admin_url), store_id))
                and store.get("drain_requested") is True
                else None
            ),
            timeout_s=5,
            interval_s=0.1,
        )
        is not None
    )

    status_resp = requests.get(f"{admin_url}/internal/v1/nodes/{node_id}/shutdown", timeout=10)
    assert status_resp.status_code == 200, status_resp.text
    status = status_resp.json()
    assert status["phase"] == "complete"
    assert status["safe_to_terminate"] is True
    assert status["stores"][0]["store_id"] == store_id

    reregister_resp = requests.post(
        f"{admin_url}/internal/v1/nodes",
        json={
            "store_id": store_id,
            "node_id": node_id,
            "role": "data",
            "health_class": "healthy",
            "live": True,
            "capacity_bytes": 1024,
            "available_bytes": 900,
        },
        timeout=10,
    )
    assert reregister_resp.status_code == 202, reregister_resp.text
    assert (
        wait_until(
            lambda: (
                store
                if (store := _maybe_find_store(_admin_snapshot(admin_url), store_id))
                and store.get("drain_requested") is True
                else None
            ),
            timeout_s=5,
            interval_s=0.1,
        )
        is not None
    )

    healthy_resp = requests.post(
        f"{admin_url}/internal/v1/nodes/{node_id}/status",
        json={
            "store_id": store_id,
            "live": True,
            "health_class": "healthy",
            "capacity_bytes": 1024,
            "available_bytes": 900,
        },
        timeout=10,
    )
    assert healthy_resp.status_code == 202, healthy_resp.text

    store = _find_store(_admin_snapshot(admin_url), store_id)
    assert store["drain_requested"] is True


def test_node_shutdown_before_store_registration_is_durable(stateful_api):
    admin_url = _metadata_admin_url(stateful_api)
    node_id = 100
    store_id = 100

    shutdown_resp = requests.put(
        f"{admin_url}/internal/v1/nodes/{node_id}/shutdown",
        json={"type": "remove", "reason": "e2e-no-store"},
        timeout=10,
    )
    assert shutdown_resp.status_code == 202, shutdown_resp.text
    assert (
        wait_until(
            lambda: (
                node
                if (node := _maybe_find_node(_admin_snapshot(admin_url), node_id))
                and node.get("lifecycle") == "draining"
                else None
            ),
            timeout_s=5,
            interval_s=0.1,
        )
        is not None
    )

    store_resp = requests.post(
        f"{admin_url}/internal/v1/nodes",
        json={
            "store_id": store_id,
            "node_id": node_id,
            "role": "data",
            "health_class": "healthy",
            "live": True,
            "capacity_bytes": 1024,
            "available_bytes": 900,
        },
        timeout=10,
    )
    assert store_resp.status_code == 202, store_resp.text
    assert (
        wait_until(
            lambda: (
                store
                if (store := _maybe_find_store(_admin_snapshot(admin_url), store_id))
                and store.get("drain_requested") is True
                else None
            ),
            timeout_s=5,
            interval_s=0.1,
        )
        is not None
    )

    status_resp = requests.get(f"{admin_url}/internal/v1/nodes/{node_id}/shutdown", timeout=10)
    assert status_resp.status_code == 200, status_resp.text
    status = status_resp.json()
    assert status["phase"] == "complete"
    assert status["safe_to_terminate"] is True


def test_node_shutdown_cancellation_clears_node_and_store_drain_intent(stateful_api):
    admin_url = _metadata_admin_url(stateful_api)
    node_id = 99
    store_id = 99

    node_resp = requests.post(
        f"{admin_url}/internal/v1/nodes",
        json={"node_id": node_id, "role": "data"},
        timeout=10,
    )
    assert node_resp.status_code == 202, node_resp.text

    store_body = {
        "store_id": store_id,
        "node_id": node_id,
        "role": "data",
        "health_class": "healthy",
        "live": True,
        "capacity_bytes": 1024,
        "available_bytes": 900,
    }
    store_resp = requests.post(
        f"{admin_url}/internal/v1/nodes",
        json=store_body,
        timeout=10,
    )
    assert store_resp.status_code == 202, store_resp.text
    assert (
        wait_until(
            lambda: _maybe_find_store(
                _admin_snapshot(admin_url),
                store_id,
            ),
            timeout_s=5,
            interval_s=0.1,
        )
        is not None
    )

    shutdown_resp = requests.put(
        f"{admin_url}/internal/v1/nodes/{node_id}/shutdown",
        json={"type": "remove", "reason": "e2e-cancel"},
        timeout=10,
    )
    assert shutdown_resp.status_code == 202, shutdown_resp.text
    assert (
        wait_until(
            lambda: (
                store
                if (store := _maybe_find_store(_admin_snapshot(admin_url), store_id))
                and store.get("drain_requested") is True
                else None
            ),
            timeout_s=5,
            interval_s=0.1,
        )
        is not None
    )

    cancel_resp = requests.delete(f"{admin_url}/internal/v1/nodes/{node_id}/shutdown", timeout=10)
    assert cancel_resp.status_code == 202, cancel_resp.text
    assert (
        wait_until(
            lambda: (
                snapshot
                if (
                    (snapshot := _admin_snapshot(admin_url))
                    and (node := _maybe_find_node(snapshot, node_id))
                    and node.get("lifecycle") == "active"
                    and (store := _maybe_find_store(snapshot, store_id))
                    and store.get("drain_requested") is False
                )
                else None
            ),
            timeout_s=5,
            interval_s=0.1,
        )
        is not None
    )

    status_resp = requests.get(f"{admin_url}/internal/v1/nodes/{node_id}/shutdown", timeout=10)
    assert status_resp.status_code == 200, status_resp.text
    status = status_resp.json()
    assert status["phase"] == "active"
    assert status["safe_to_terminate"] is False

    reregister_resp = requests.post(
        f"{admin_url}/internal/v1/nodes",
        json=store_body,
        timeout=10,
    )
    assert reregister_resp.status_code == 202, reregister_resp.text
    store = _find_store(_admin_snapshot(admin_url), store_id)
    assert store["drain_requested"] is False

    retry_cancel_resp = requests.delete(f"{admin_url}/internal/v1/nodes/{node_id}/shutdown", timeout=10)
    assert retry_cancel_resp.status_code == 202, retry_cancel_resp.text


class MultiNodeScalingCluster:
    def __init__(
        self,
        binary: str,
        *,
        initial_data_node_count: int = 5,
        max_shard_size_bytes: int = 0,
    ):
        self.binary = binary
        self.host = "127.0.0.1"
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-scaling-e2e-")
        self.root = Path(self.tempdir.name)
        self.max_shard_size_bytes = max_shard_size_bytes

        self.metadata_nodes = [
            {
                "id": node_id,
                "raft_port": find_free_port(),
                "api_port": find_free_port(),
            }
            for node_id in range(1, 4)
        ]
        self.data_nodes = [self._new_data_node(node_id) for node_id in range(101, 101 + initial_data_node_count)]
        self.config_path = self.root / "antfly.json"
        self.metadata_procs: list[subprocess.Popen[str]] = []
        self.data_procs: list[subprocess.Popen[str]] = []
        self.data_proc_by_node_id: dict[int, subprocess.Popen[str]] = {}
        self.log_files: list[Any] = []
        self.log_paths: list[Path] = []

        self._write_config()
        try:
            self._start()
        except BaseException:
            self.stop()
            raise

    @property
    def metadata_urls(self) -> list[str]:
        return [f"http://{self.host}:{node['api_port']}" for node in self.metadata_nodes]

    @property
    def data_api_urls(self) -> list[str]:
        return [
            antfly_public_api_url(f"http://{self.host}:{node['api_port']}", binary=self.binary)
            for node in self.data_nodes
        ]

    @property
    def live_data_api_urls(self) -> list[str]:
        urls: list[str] = []
        for node in self.data_nodes:
            proc = self.data_proc_by_node_id.get(int(node["id"]))
            if proc is not None and proc.poll() is None:
                urls.append(self.data_api_url_for_node(node))
        return urls

    def data_api_url_for_node(self, node: dict[str, int]) -> str:
        return antfly_public_api_url(f"http://{self.host}:{node['api_port']}", binary=self.binary)

    def _new_data_node(self, node_id: int) -> dict[str, int]:
        return {
            "id": node_id,
            "store_id": node_id,
            "api_port": find_free_port(),
            "raft_port": find_free_port(),
        }

    def _write_config(self) -> None:
        config = {
            "metadata": {
                "orchestration_urls": {
                    str(node["id"]): f"http://{self.host}:{node['api_port']}"
                    for node in self.metadata_nodes
                },
                "raft_urls": {
                    str(node["id"]): f"http://{self.host}:{node['raft_port']}"
                    for node in self.metadata_nodes
                },
            },
            "replication_factor": 1,
            "max_shard_size_bytes": self.max_shard_size_bytes,
            "max_shards_per_table": 64,
            "default_shards_per_table": 1,
            "storage": {"local": {"base_dir": str(self.root / "config-storage")}},
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    def _open_log(self, name: str) -> Any:
        path = self.root / name
        handle = path.open("w")
        self.log_paths.append(path)
        self.log_files.append(handle)
        return handle

    def _start(self) -> None:
        for node in self.metadata_nodes:
            log = self._open_log(f"metadata-{node['id']}.log")
            command = [
                self.binary,
                "metadata",
                "--config",
                str(self.config_path),
                "--id",
                str(node["id"]),
                "--tick-ms",
                "25",
                "--replica-root-dir",
                str(self.root / f"metadata-{node['id']}-replicas"),
                "--replica-catalog-path",
                str(self.root / f"metadata-{node['id']}-catalog.txt"),
                "--snapshot-root-dir",
                str(self.root / f"metadata-{node['id']}-snapshots"),
            ]
            self.metadata_procs.append(
                subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, cwd=REPO_ROOT)
            )

        for url in self.metadata_urls:
            if not wait_for_server(url, path="/metadata/v1/status"):
                raise RuntimeError(f"Metadata node failed to start at {url}\n{self.debug_logs()}")
        time.sleep(1.0)

        for node in self.data_nodes:
            self._start_data_node(node)

        for url in self.data_api_urls:
            if not wait_for_server(url):
                raise RuntimeError(f"Data node failed to start at {url}\n{self.debug_logs()}")
        if not self.wait_for_all_data_nodes_registered(timeout_s=60.0):
            raise RuntimeError(
                "Data nodes did not register on all metadata nodes\n"
                f"metadata statuses: {json.dumps(self.metadata_statuses(), indent=2, sort_keys=True)}\n"
                f"{self.debug_logs()}"
            )

    def _start_data_node(self, node: dict[str, int]) -> None:
        log = self._open_log(f"data-{node['id']}.log")
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
            str(node["id"]),
            "--store-id",
            str(node["store_id"]),
            "--tick-ms",
            "25",
            "--replica-root-dir",
            str(self.root / f"data-{node['id']}-replicas"),
            "--replica-catalog-path",
            str(self.root / f"data-{node['id']}-catalog.txt"),
        ]
        proc = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, cwd=REPO_ROOT)
        self.data_procs.append(proc)
        self.data_proc_by_node_id[int(node["id"])] = proc

    def add_data_node(self) -> dict[str, int]:
        node = self._new_data_node(max(int(existing["id"]) for existing in self.data_nodes) + 1)
        self.data_nodes.append(node)
        self._start_data_node(node)
        url = self.data_api_url_for_node(node)
        if not wait_for_server(url):
            raise RuntimeError(f"Added data node failed to start at {url}\n{self.debug_logs()}")
        if not self.wait_for_data_nodes_registered({int(node["id"])}, timeout_s=60.0):
            raise RuntimeError(
                f"Added data node {node['id']} did not register on all metadata nodes\n"
                f"metadata statuses: {json.dumps(self.metadata_statuses(), indent=2, sort_keys=True)}\n"
                f"{self.debug_logs()}"
            )
        return node

    def stop_data_node(self, node_id: int) -> None:
        proc = self.data_proc_by_node_id.get(node_id)
        if proc is None:
            raise AssertionError(f"data node {node_id} has no process")
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    def metadata_snapshot(self, index: int | None = None) -> dict[str, Any]:
        if index is not None:
            response = requests.get(f"{self.metadata_urls[index]}/metadata/v1/admin/snapshot", timeout=10)
            response.raise_for_status()
            payload = response.json()
            assert isinstance(payload, dict)
            return payload

        last_error: Exception | None = None
        for url in self.metadata_urls:
            try:
                response = requests.get(f"{url}/metadata/v1/admin/snapshot", timeout=5)
                response.raise_for_status()
                payload = response.json()
                assert isinstance(payload, dict)
                return payload
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise AssertionError("cluster has no metadata URLs")

    def post_metadata(self, path: str, *, json_body: dict[str, Any] | None = None) -> requests.Response:
        last_error: Exception | None = None
        for url in self.metadata_urls:
            try:
                response = requests.post(f"{url}{path}", json=json_body, timeout=5)
                if response.ok:
                    return response
                last_error = AssertionError(f"{response.status_code}: {response.text}")
            except requests.RequestException as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise AssertionError("cluster has no metadata URLs")

    def put_metadata(self, path: str, *, json_body: dict[str, Any] | None = None) -> requests.Response:
        last_error: Exception | None = None
        for url in self.metadata_urls:
            try:
                response = requests.put(f"{url}{path}", json=json_body, timeout=5)
                if response.ok:
                    return response
                last_error = AssertionError(f"{response.status_code}: {response.text}")
            except requests.RequestException as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise AssertionError("cluster has no metadata URLs")

    def delete_metadata(self, path: str) -> requests.Response:
        last_error: Exception | None = None
        for url in self.metadata_urls:
            try:
                response = requests.delete(f"{url}{path}", timeout=5)
                if response.ok:
                    return response
                last_error = AssertionError(f"{response.status_code}: {response.text}")
            except requests.RequestException as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise AssertionError("cluster has no metadata URLs")

    def metadata_snapshot_from(self, index: int) -> dict[str, Any]:
        response = requests.get(f"{self.metadata_urls[index]}/metadata/v1/admin/snapshot", timeout=10)
        response.raise_for_status()
        payload = response.json()
        assert isinstance(payload, dict)
        return payload

    def wait_for_all_data_nodes_registered(self, *, timeout_s: float) -> dict[str, Any] | None:
        return self.wait_for_data_nodes_registered({int(node["id"]) for node in self.data_nodes}, timeout_s=timeout_s)

    def wait_for_data_nodes_registered(self, expected_node_ids: set[int], *, timeout_s: float) -> dict[str, Any] | None:
        def registered_on_all_metadata_nodes() -> dict[str, Any] | None:
            try:
                snapshots = [self.metadata_snapshot(index) for index in range(len(self.metadata_urls))]
            except (AssertionError, requests.RequestException):
                return None
            nodes_by_id = {int(node["id"]): node for node in self.data_nodes}
            for snapshot in snapshots:
                stores = [store for store in snapshot.get("stores", []) if isinstance(store, dict)]
                by_node = {int(store.get("node_id", 0)): store for store in stores}
                if not expected_node_ids.issubset(by_node):
                    return None
                for node_id in expected_node_ids:
                    node = nodes_by_id[node_id]
                    store = by_node[node["id"]]
                    if store.get("api_url") != f"http://{self.host}:{node['api_port']}":
                        return None
                    if store.get("raft_url") != f"http://{self.host}:{node['raft_port']}":
                        return None
            return snapshots[0]

        return wait_until(registered_on_all_metadata_nodes, timeout_s=timeout_s, interval_s=0.5)

    def create_table(self, table_name: str, *, num_shards: int) -> None:
        last_error: str | None = None

        def table_created_in_metadata() -> bool:
            try:
                group_ids = _table_group_ids(self, table_name)
            except (AssertionError, requests.RequestException, ValueError):
                return False
            return group_ids is not None and len(group_ids) >= num_shards

        def create_once() -> bool | None:
            nonlocal last_error
            for api_url in self.live_data_api_urls or self.data_api_urls:
                try:
                    response = requests.post(
                        f"{api_url}/tables/{table_name}",
                        json={"num_shards": num_shards},
                        timeout=10,
                    )
                    if response.ok:
                        return True
                    last_error = f"{response.status_code}: {response.text}"
                except requests.RequestException as exc:
                    last_error = repr(exc)
                if table_created_in_metadata():
                    return True
            return None

        created = wait_until(create_once, timeout_s=60.0, interval_s=0.5)
        assert created is True, (
            f"failed to create table {table_name}: {last_error}\n"
            f"metadata statuses: {json.dumps(self.metadata_statuses(), indent=2, sort_keys=True)}\n"
            f"{self.debug_logs()}"
        )

    def request_node_shutdown(self, node_id: int, *, timeout_s: float = 30.0) -> None:
        last_error: str | None = None

        def intent_visible_on_all_metadata_nodes() -> dict[str, Any] | None:
            nonlocal last_error
            try:
                snapshots = [self.metadata_snapshot(index) for index in range(len(self.metadata_urls))]
            except (AssertionError, requests.RequestException) as exc:
                last_error = repr(exc)
                return None
            for snapshot in snapshots:
                nodes = [node for node in snapshot.get("nodes", []) if isinstance(node, dict)]
                stores = [store for store in snapshot.get("stores", []) if isinstance(store, dict)]
                node_draining = any(
                    int(node.get("node_id", 0)) == node_id and node.get("lifecycle") == "draining"
                    for node in nodes
                )
                store_draining = any(
                    int(store.get("node_id", 0)) == node_id and store.get("drain_requested") is True
                    for store in stores
                )
                if not node_draining and not store_draining:
                    return None
            return snapshots[0]

        def request_until_visible() -> dict[str, Any] | None:
            nonlocal last_error
            try:
                response = self.put_metadata(
                    f"/internal/v1/nodes/{node_id}/shutdown",
                    json_body={"type": "remove", "reason": "e2e"},
                )
                response.raise_for_status()
            except (AssertionError, requests.RequestException) as exc:
                last_error = repr(exc)
                return None
            return intent_visible_on_all_metadata_nodes()

        visible = wait_until(request_until_visible, timeout_s=timeout_s, interval_s=0.5)
        assert visible is not None, (
            f"node shutdown intent did not become visible on all metadata nodes for {node_id}: {last_error}\n"
            f"metadata statuses: {json.dumps(self.metadata_statuses(), indent=2, sort_keys=True)}\n"
            f"snapshot: {self.metadata_snapshot()}\n"
            f"{self.debug_logs()}"
        )

    def finalize_node_shutdown(self, node_id: int) -> None:
        response = self.delete_metadata(f"/internal/v1/nodes/{node_id}")
        response.raise_for_status()

    def trigger_reallocate(self) -> None:
        response = self.post_metadata("/internal/v1/reallocate")
        response.raise_for_status()

    def request_split(self, table_name: str, split_key: str) -> None:
        response = self.post_metadata(f"/internal/v1/tables/{table_name}/split", json_body={"split_key": split_key})
        response.raise_for_status()

    def metadata_statuses(self) -> list[dict[str, Any]]:
        statuses: list[dict[str, Any]] = []
        for index, url in enumerate(self.metadata_urls):
            try:
                response = requests.get(f"{url}/metadata/v1/status", timeout=5)
                response.raise_for_status()
                payload = response.json()
                assert isinstance(payload, dict)
                statuses.append({"index": index, "url": url, "status": payload})
            except Exception as exc:
                statuses.append({"index": index, "url": url, "error": repr(exc)})
        return statuses

    def debug_logs(self) -> str:
        for handle in self.log_files:
            handle.flush()
        return "\n".join(f"[{path.name}]\n{_read_log_tail(path)}" for path in self.log_paths)

    def stop(self) -> None:
        for proc in [*self.data_procs, *self.metadata_procs]:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        for handle in self.log_files:
            if not handle.closed:
                handle.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


@pytest.fixture
def multi_node_scaling_cluster() -> MultiNodeScalingCluster:
    cluster = MultiNodeScalingCluster(_scaling_antfly_binary())
    try:
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture
def compact_scaling_cluster() -> MultiNodeScalingCluster:
    cluster = MultiNodeScalingCluster(_scaling_antfly_binary(), initial_data_node_count=3)
    try:
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture
def split_scaling_cluster() -> MultiNodeScalingCluster:
    cluster = MultiNodeScalingCluster(
        _scaling_antfly_binary(),
        initial_data_node_count=5,
        max_shard_size_bytes=2048,
    )
    try:
        yield cluster
    finally:
        cluster.stop()


def _scaling_antfly_binary() -> str:
    binary = os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
    resolved = Path(binary).expanduser().resolve()
    if resolved.name != "antfly":
        pytest.skip("multi-node scaling e2e requires the antfly binary")
    if not resolved.exists():
        pytest.skip(f"antfly binary not built: {resolved}")
    return str(resolved)


def _table_group_ids(cluster: MultiNodeScalingCluster, table_name: str) -> set[int] | None:
    snapshot = cluster.metadata_snapshot()
    table_id = None
    for table in snapshot.get("tables", []):
        if isinstance(table, dict) and table.get("name") == table_name:
            table_id = int(table["table_id"])
            break
    if table_id is None:
        return None
    group_ids = {
        int(record["group_id"])
        for record in snapshot.get("ranges", [])
        if isinstance(record, dict) and int(record.get("table_id", 0)) == table_id
    }
    return group_ids if group_ids else None


def _placed_nodes_for_groups(cluster: MultiNodeScalingCluster, group_ids: set[int]) -> set[int]:
    snapshot = cluster.metadata_snapshot()
    return {
        int(intent["record"]["local_node_id"])
        for intent in snapshot.get("placement_intents", [])
        if isinstance(intent, dict)
        and isinstance(intent.get("record"), dict)
        and int(intent["record"].get("group_id", 0)) in group_ids
    }


def _all_metadata_snapshots(cluster: MultiNodeScalingCluster) -> list[dict[str, Any]] | None:
    try:
        return [cluster.metadata_snapshot(index) for index in range(len(cluster.metadata_urls))]
    except (AssertionError, requests.RequestException, ValueError):
        return None


def _lookup_from_any_data_node(
    cluster: MultiNodeScalingCluster,
    table_name: str,
    key: str,
    expected: dict[str, Any],
) -> dict[str, Any] | None:
    for api_url in cluster.live_data_api_urls:
        try:
            response = requests.get(
                f"{api_url}{lookup_key_path(table_name, key)}",
                timeout=10,
            )
        except requests.RequestException:
            continue
        if not response.ok:
            continue
        payload = response.json()
        if payload == expected:
            return payload
    return None


def _insert_docs(
    cluster: MultiNodeScalingCluster,
    table_name: str,
    docs: dict[str, dict[str, Any]],
    *,
    min_group_count: int = 1,
) -> None:
    last_error: str | None = None

    def route_ready() -> str | None:
        try:
            return _data_api_url_for_table(
                cluster,
                table_name,
                require_all_group_leaders=True,
                min_group_count=min_group_count,
            )
        except (AssertionError, requests.RequestException, ValueError):
            return None

    api_url = wait_until(route_ready, timeout_s=60.0, interval_s=0.5)
    assert api_url is not None, (
        f"table {table_name} never became write-routable before seed batch\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )

    def post_once() -> bool | None:
        nonlocal api_url, last_error
        api_url = route_ready() or api_url
        try:
            response = requests.post(
                f"{api_url}/tables/{table_name}/batch",
                json={"inserts": docs, "sync_level": "write"},
                timeout=30,
            )
        except requests.RequestException as exc:
            last_error = repr(exc)
            return None
        if response.ok:
            return True
        last_error = f"{response.status_code}: {response.text}"
        if response.status_code in {429, 500, 503}:
            return None
        response.raise_for_status()
        return None

    inserted = wait_until(post_once, timeout_s=90.0, interval_s=0.5)
    assert inserted is True, (
        f"failed to insert seed docs for {table_name}: {last_error}\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )


def _data_api_url_for_table(
    cluster: MultiNodeScalingCluster,
    table_name: str,
    *,
    require_all_group_leaders: bool = False,
    min_group_count: int = 1,
) -> str | None:
    snapshot = cluster.metadata_snapshot()
    table_id: int | None = None
    for table in snapshot.get("tables", []):
        if isinstance(table, dict) and table.get("name") == table_name:
            table_id = int(table.get("table_id", 0))
            break
    if table_id is None:
        return None
    group_ids: list[int] = []
    for table_range in snapshot.get("ranges", []):
        if isinstance(table_range, dict) and int(table_range.get("table_id", 0)) == table_id:
            group_ids.append(int(table_range.get("group_id", 0)))
    if len(group_ids) < min_group_count:
        return None
    group_ids.sort()

    leader_store_by_group: dict[int, int] = {}
    for status in snapshot.get("merged_group_statuses", []):
        if not isinstance(status, dict):
            continue
        group_id = int(status.get("group_id", 0))
        if group_id not in group_ids:
            continue
        raw_leader = int(status.get("leader_store_id", 0))
        if raw_leader != 0:
            leader_store_by_group[group_id] = raw_leader
    if require_all_group_leaders and any(group_id not in leader_store_by_group for group_id in group_ids):
        return None

    group_id = group_ids[0]
    leader_store_id: int | None = None
    if group_id in leader_store_by_group:
        leader_store_id = leader_store_by_group[group_id]
    if leader_store_id is not None:
        for store in snapshot.get("stores", []):
            if not isinstance(store, dict) or int(store.get("store_id", 0)) != leader_store_id:
                continue
            leader_node_id = int(store.get("node_id", 0))
            for node in cluster.data_nodes:
                if int(node["id"]) == leader_node_id:
                    return cluster.data_api_url_for_node(node)
    placed_node_ids = {
        int(intent.get("record", {}).get("local_node_id", 0))
        for intent in snapshot.get("placement_intents", [])
        if isinstance(intent, dict) and int(intent.get("record", {}).get("group_id", 0)) == group_id
    }
    for node in cluster.data_nodes:
        if int(node["id"]) in placed_node_ids:
            return cluster.data_api_url_for_node(node)
    return None


def _assert_docs_readable(
    cluster: MultiNodeScalingCluster,
    table_name: str,
    docs: dict[str, dict[str, Any]],
    *,
    timeout_s: float = 60.0,
) -> None:
    for key, expected in docs.items():
        lookup = wait_until(
            lambda: _lookup_from_any_data_node(cluster, table_name, key, expected),
            timeout_s=timeout_s,
            interval_s=0.5,
        )
        assert lookup == expected, (
            f"lookup did not converge for {key}\n"
            f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
            f"snapshot: {cluster.metadata_snapshot()}\n"
            f"{cluster.debug_logs()}"
        )


def _wait_for_group_count(
    cluster: MultiNodeScalingCluster,
    table_name: str,
    *,
    min_count: int,
    timeout_s: float = 60.0,
) -> set[int]:
    group_ids = wait_until(
        lambda: (
            groups
            if (groups := _table_group_ids(cluster, table_name)) is not None and len(groups) >= min_count
            else None
        ),
        timeout_s=timeout_s,
        interval_s=0.5,
    )
    assert group_ids is not None, (
        f"table {table_name} did not reach {min_count} groups\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )
    return group_ids


def _wait_node_owns_group(
    cluster: MultiNodeScalingCluster,
    table_name: str,
    node_id: int,
    *,
    timeout_s: float = 90.0,
) -> dict[str, Any] | None:
    def owns_group() -> dict[str, Any] | None:
        try:
            cluster.trigger_reallocate()
            group_ids = _table_group_ids(cluster, table_name)
        except (AssertionError, requests.RequestException):
            return None
        if not group_ids:
            return None
        if node_id not in _placed_nodes_for_groups(cluster, group_ids):
            return None
        return cluster.metadata_snapshot()

    return wait_until(owns_group, timeout_s=timeout_s, interval_s=0.5)


def _wait_node_drained_for_groups(
    cluster: MultiNodeScalingCluster,
    node_id: int,
    group_ids: set[int],
    *,
    timeout_s: float = 90.0,
) -> dict[str, Any] | None:
    def drained_and_replaced() -> dict[str, Any] | None:
        snapshots = _all_metadata_snapshots(cluster)
        if snapshots is None:
            return None
        for snapshot in snapshots:
            stores = [store for store in snapshot.get("stores", []) if isinstance(store, dict)]
            drained_store = next(
                (store for store in stores if int(store.get("node_id", 0)) == node_id),
                None,
            )
            if not drained_store or drained_store.get("drain_requested") is not True:
                return None
            for intent in snapshot.get("placement_intents", []):
                if not isinstance(intent, dict) or not isinstance(intent.get("record"), dict):
                    continue
                record = intent["record"]
                if int(record.get("group_id", 0)) in group_ids and int(record.get("local_node_id", 0)) == node_id:
                    return None
        return snapshots[0]

    return wait_until(drained_and_replaced, timeout_s=timeout_s, interval_s=0.5)


def _wait_node_shutdown_phase(
    cluster: MultiNodeScalingCluster,
    node_id: int,
    phase: str,
    *,
    timeout_s: float = 60.0,
) -> dict[str, Any] | None:
    def status_matches() -> dict[str, Any] | None:
        try:
            response = requests.get(f"{cluster.metadata_urls[0]}/internal/v1/nodes/{node_id}/shutdown", timeout=10)
            response.raise_for_status()
            payload = response.json()
        except (AssertionError, requests.RequestException, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        if payload.get("phase") != phase:
            return None
        return payload

    return wait_until(status_matches, timeout_s=timeout_s, interval_s=0.5)


def test_multinode_cluster_uses_configured_multi_metadata_discovery_and_data_raft_urls(
    multi_node_scaling_cluster: MultiNodeScalingCluster,
) -> None:
    cluster = multi_node_scaling_cluster
    snapshot = cluster.wait_for_all_data_nodes_registered(timeout_s=1.0)
    assert snapshot is not None, (
        "data nodes did not register on all metadata APIs\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"{cluster.debug_logs()}"
    )

    table_name = f"split_multi_{time.time_ns()}"
    cluster.create_table(table_name, num_shards=5)

    def placed_across_data_nodes() -> dict[str, Any] | None:
        try:
            group_ids = _table_group_ids(cluster, table_name)
        except (AssertionError, requests.RequestException):
            return None
        if group_ids is None or len(group_ids) < 5:
            return None
        placed_nodes = _placed_nodes_for_groups(cluster, group_ids)
        if len(placed_nodes) < 5:
            return None
        return cluster.metadata_snapshot()

    placed = wait_until(placed_across_data_nodes, timeout_s=60.0, interval_s=0.5)
    assert placed is not None, (
        "table shards were not placed across all five data nodes\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )


def test_autoscaling_drains_data_node_and_replaces_placements(
    multi_node_scaling_cluster: MultiNodeScalingCluster,
) -> None:
    cluster = multi_node_scaling_cluster
    table_name = f"split_drain_{time.time_ns()}"
    cluster.create_table(table_name, num_shards=5)

    docs = {f"doc-{i:02d}": {"title": f"doc {i}", "rank": i} for i in range(10)}
    batch = requests.post(
        f"{cluster.data_api_urls[0]}/tables/{table_name}/batch",
        json={"inserts": docs, "sync_level": "write"},
        timeout=30,
    )
    batch.raise_for_status()

    group_ids = wait_until(lambda: _table_group_ids(cluster, table_name), timeout_s=60.0, interval_s=0.5)
    assert group_ids is not None and len(group_ids) >= 5, (
        "table groups were not created before node drain\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )

    initial_nodes = wait_until(
        lambda: (nodes if len(nodes := _placed_nodes_for_groups(cluster, group_ids)) >= 5 else None),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert initial_nodes is not None, (
        "table groups were not placed before node drain\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )
    node_to_drain = sorted(initial_nodes)[0]
    cluster.request_node_shutdown(node_to_drain)

    def drained_and_replaced() -> dict[str, Any] | None:
        snapshots = _all_metadata_snapshots(cluster)
        if snapshots is None:
            return None
        for snapshot in snapshots:
            stores = [store for store in snapshot.get("stores", []) if isinstance(store, dict)]
            drained_store = next(
                (store for store in stores if int(store.get("node_id", 0)) == node_to_drain),
                None,
            )
            if not drained_store or drained_store.get("drain_requested") is not True:
                return None
            for intent in snapshot.get("placement_intents", []):
                if not isinstance(intent, dict) or not isinstance(intent.get("record"), dict):
                    continue
                record = intent["record"]
                if int(record.get("group_id", 0)) in group_ids and int(record.get("local_node_id", 0)) == node_to_drain:
                    return None
        return snapshots[0]

    drained = wait_until(drained_and_replaced, timeout_s=90.0, interval_s=0.5)
    assert drained is not None, (
        "drained data node still owned table placements\n"
        f"node_to_drain: {node_to_drain}\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )

    for key, expected in docs.items():
        lookup = wait_until(
            lambda: _lookup_from_any_data_node(cluster, table_name, key, expected),
            timeout_s=60.0,
            interval_s=0.5,
        )
        assert lookup == expected, (
            f"post-drain lookup did not converge for {key}\n"
            f"node_to_drain: {node_to_drain}\n"
            f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
            f"snapshot: {cluster.metadata_snapshot()}\n"
            f"{cluster.debug_logs()}"
        )


def test_autoscaling_adds_data_node_and_assigns_placements(
    compact_scaling_cluster: MultiNodeScalingCluster,
) -> None:
    cluster = compact_scaling_cluster
    table_name = f"scale_add_{time.time_ns()}"
    cluster.create_table(table_name, num_shards=8)

    initial_groups = _wait_for_group_count(cluster, table_name, min_count=8)
    initial_nodes = wait_until(
        lambda: (nodes if len(nodes := _placed_nodes_for_groups(cluster, initial_groups)) >= 3 else None),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert initial_nodes is not None, (
        "initial table groups were not placed before adding a data node\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )

    new_node = cluster.add_data_node()
    assigned = _wait_node_owns_group(cluster, table_name, int(new_node["id"]))
    assert assigned is not None, (
        "added data node did not receive any table placement\n"
        f"new_node: {new_node['id']}\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )


def test_autoscaling_drains_stops_and_finalizes_data_node_without_losing_reads(
    multi_node_scaling_cluster: MultiNodeScalingCluster,
) -> None:
    cluster = multi_node_scaling_cluster
    table_name = f"scale_stop_{time.time_ns()}"
    cluster.create_table(table_name, num_shards=5)

    docs = {f"doc-{i:02d}": {"title": f"doc {i}", "rank": i} for i in range(12)}
    _insert_docs(cluster, table_name, docs, min_group_count=5)

    group_ids = _wait_for_group_count(cluster, table_name, min_count=5)
    initial_nodes = wait_until(
        lambda: (nodes if len(nodes := _placed_nodes_for_groups(cluster, group_ids)) >= 5 else None),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert initial_nodes is not None, (
        "table groups were not placed before node stop\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )

    node_to_stop = sorted(initial_nodes)[-1]
    cluster.request_node_shutdown(node_to_stop)
    drained = _wait_node_drained_for_groups(cluster, node_to_stop, group_ids)
    assert drained is not None, (
        "drained node still owned table placements before stop\n"
        f"node_to_stop: {node_to_stop}\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )
    complete = _wait_node_shutdown_phase(cluster, node_to_stop, "complete")
    assert complete is not None and complete.get("safe_to_terminate") is True, (
        "node shutdown never became safe to terminate\n"
        f"node_to_stop: {node_to_stop}\n"
        f"status: {complete}\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"{cluster.debug_logs()}"
    )

    cluster.stop_data_node(node_to_stop)
    cluster.finalize_node_shutdown(node_to_stop)
    finalized = _wait_node_shutdown_phase(cluster, node_to_stop, "not_found")
    assert finalized is not None and finalized.get("safe_to_terminate") is True, (
        "finalized node still appeared as shutdown debt\n"
        f"node_to_stop: {node_to_stop}\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )
    _assert_docs_readable(cluster, table_name, docs)


def test_autoscaling_finalizes_shard_split_from_size_threshold(
    split_scaling_cluster: MultiNodeScalingCluster,
) -> None:
    cluster = split_scaling_cluster
    table_name = f"scale_split_{time.time_ns()}"
    cluster.create_table(table_name, num_shards=1)

    docs = {
        f"doc:{i:03d}": {
            "title": f"split doc {i}",
            "body": "x" * 768,
            "rank": i,
        }
        for i in range(48)
    }
    _insert_docs(cluster, table_name, docs, min_group_count=1)

    def split_completed() -> set[int] | None:
        try:
            cluster.trigger_reallocate()
            group_ids = _table_group_ids(cluster, table_name)
        except (AssertionError, requests.RequestException):
            return None
        if group_ids is None:
            return None
        return group_ids if len(group_ids) >= 2 else None

    split_groups = wait_until(split_completed, timeout_s=180.0, interval_s=0.5)
    assert split_groups is not None, (
        "table did not finalize an automatic split after exceeding the configured shard size threshold\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )
    _assert_docs_readable(cluster, table_name, docs, timeout_s=60.0)


def test_autoscaling_node_churn_keeps_reads_available(
    compact_scaling_cluster: MultiNodeScalingCluster,
) -> None:
    cluster = compact_scaling_cluster
    table_name = f"scale_churn_{time.time_ns()}"
    cluster.create_table(table_name, num_shards=6)

    docs = {f"doc-{i:02d}": {"title": f"churn doc {i}", "rank": i} for i in range(18)}
    _insert_docs(cluster, table_name, docs, min_group_count=6)
    _assert_docs_readable(cluster, table_name, docs)

    group_ids = _wait_for_group_count(cluster, table_name, min_count=6)
    initial_nodes = wait_until(
        lambda: (nodes if len(nodes := _placed_nodes_for_groups(cluster, group_ids)) >= 3 else None),
        timeout_s=60.0,
        interval_s=0.5,
    )
    assert initial_nodes is not None, (
        "table groups were not placed before churn\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )

    node_to_replace = sorted(initial_nodes)[0]
    cluster.request_node_shutdown(node_to_replace)
    drained = _wait_node_drained_for_groups(cluster, node_to_replace, group_ids)
    assert drained is not None, (
        "drained node still owned placements during churn\n"
        f"node_to_replace: {node_to_replace}\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )
    cluster.stop_data_node(node_to_replace)
    _assert_docs_readable(cluster, table_name, docs)

    replacement = cluster.add_data_node()
    assigned = _wait_node_owns_group(cluster, table_name, int(replacement["id"]))
    assert assigned is not None, (
        "replacement data node did not receive placement during churn\n"
        f"replacement: {replacement['id']}\n"
        f"metadata statuses: {json.dumps(cluster.metadata_statuses(), indent=2, sort_keys=True)}\n"
        f"snapshot: {cluster.metadata_snapshot()}\n"
        f"{cluster.debug_logs()}"
    )
    _assert_docs_readable(cluster, table_name, docs)
