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


def _dense_top_hit(
    stateful_api,
    table_name: str,
    vector: list[float],
    index_name: str,
    expected_id: str,
) -> dict | None:
    try:
        result = stateful_api.query_table(
            table_name,
            {
                "embeddings": {index_name: vector},
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
    return result if any(hit.get("_id") == expected_id for hit in hits) else None


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


def _check_response(response: requests.Response) -> dict:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise AssertionError(
            f"{response.request.method} {response.url} failed: {response.text}"
        ) from exc
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def _is_metadata_not_leader_response(response: requests.Response) -> bool:
    return response.headers.get("X-Antfly-Metadata-Not-Leader", "").lower() == "true"


def _metadata_quorum_leader_id(
    statuses: list[dict | None], *, cluster_size: int
) -> int | None:
    """Return a self-confirmed leader backed by a same-term voter quorum.

    Followers can legitimately lag an election by a heartbeat, so requiring
    every reachable node to report one leader turns normal Raft convergence
    into a false outage. A quorum in one term is the safety boundary; requiring
    the candidate to report itself as leader avoids routing to a stale hint.
    """
    if cluster_size < 1:
        return None

    quorum = cluster_size // 2 + 1
    observations: dict[tuple[int, int], int] = {}
    by_node: dict[int, dict] = {}
    for status in statuses:
        if not status:
            continue
        try:
            node_id = int(status["metadata_raft_local_node_id"])
            term = int(status["metadata_raft_term"])
            leader_id = int(status["metadata_raft_leader_id"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (1 <= node_id <= cluster_size and 1 <= leader_id <= cluster_size):
            continue
        if node_id in by_node:
            continue
        by_node[node_id] = status
        if status.get("metadata_raft_local_voter", True):
            key = (term, leader_id)
            observations[key] = observations.get(key, 0) + 1

    # A majority cannot support two different observations in one term. Sort
    # by term so a quorum-confirmed newer election wins over stale hints.
    for (term, leader_id), count in sorted(observations.items(), reverse=True):
        if count < quorum:
            continue
        leader_status = by_node.get(leader_id)
        if not leader_status:
            continue
        try:
            leader_term = int(leader_status["metadata_raft_term"])
            self_leader_id = int(leader_status["metadata_raft_leader_id"])
        except (KeyError, TypeError, ValueError):
            continue
        if (
            leader_term == term
            and self_leader_id == leader_id
            and leader_status.get("metadata_raft_role") == "leader"
        ):
            return leader_id
    return None


def _metadata_status_observations(statuses: list[dict | None]) -> list[dict | None]:
    """Keep leader-discovery failures compact and operationally useful."""
    fields = (
        "metadata_raft_local_node_id",
        "metadata_raft_role",
        "metadata_raft_leader_id",
        "metadata_raft_term",
        "metadata_raft_commit_index",
        "metadata_raft_local_voter",
    )
    return [
        {field: status.get(field) for field in fields} if status else None
        for status in statuses
    ]


def _metadata_status(
    node_id: int,
    *,
    term: int,
    leader_id: int | None,
    role: str = "follower",
    voter: bool = True,
) -> dict:
    """Build a focused status fixture for leader-discovery contract tests."""
    return {
        "metadata_raft_local_node_id": node_id,
        "metadata_raft_term": term,
        "metadata_raft_leader_id": leader_id,
        "metadata_raft_role": role,
        "metadata_raft_local_voter": voter,
    }


def test_metadata_quorum_leader_discovery_tolerates_one_stale_follower() -> None:
    statuses = [
        _metadata_status(1, term=8, leader_id=2),
        _metadata_status(2, term=8, leader_id=2, role="leader"),
        _metadata_status(3, term=7, leader_id=1),
    ]
    assert _metadata_quorum_leader_id(statuses, cluster_size=3) == 2


def test_metadata_quorum_leader_discovery_keeps_node_ids_truthy() -> None:
    statuses = [
        _metadata_status(1, term=8, leader_id=1, role="leader"),
        _metadata_status(2, term=8, leader_id=1),
        _metadata_status(3, term=8, leader_id=1),
    ]
    # Readiness polling treats falsey values as pending, so carry Raft's
    # one-based node ID and convert to a zero-based URL index only at the edge.
    assert _metadata_quorum_leader_id(statuses, cluster_size=3) == 1


def test_wait_until_explicit_readiness_accepts_zero() -> None:
    assert (
        wait_until(
            lambda: 0,
            timeout_s=0.1,
            interval_s=0.01,
            ready_when=lambda value: value is not None,
        )
        == 0
    )


def test_metadata_quorum_leader_discovery_requires_a_quorum() -> None:
    statuses = [
        _metadata_status(1, term=8, leader_id=2),
        _metadata_status(2, term=8, leader_id=2, role="leader", voter=False),
        None,
    ]
    assert _metadata_quorum_leader_id(statuses, cluster_size=3) is None


def test_metadata_quorum_leader_discovery_requires_self_confirmation() -> None:
    statuses = [
        _metadata_status(1, term=8, leader_id=2),
        _metadata_status(2, term=8, leader_id=2),
        _metadata_status(3, term=7, leader_id=1),
    ]
    assert _metadata_quorum_leader_id(statuses, cluster_size=3) is None


class ThreeByThreeBackupCluster:
    def __init__(self, binary: str):
        self.binary = binary
        self.host = "127.0.0.1"
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
            self.data_ports = list(self.port_reservations.reserve_many(3))
            self.data_raft_ports = list(self.port_reservations.reserve_many(3))
            self.data_urls = [
                f"http://{self.host}:{port}" for port in self.data_ports
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
                self.root / f"data-{node_id}.log" for node_id in range(4, 7)
            ]
            self.data_log_files = [
                setup.enter_context(path.open("w")) for path in self.data_log_paths
            ]

            self.metadata_procs: list[subprocess.Popen[str]] = []
            self.data_procs: list[subprocess.Popen[str]] = []
            self.last_metadata_statuses: list[dict | None] = []
            setup.pop_all()

        try:
            self._start()
        except BaseException:
            self.stop(test_failed=True)
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
                    "replication_factor": 3,
                    "default_shards_per_table": 3,
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
            "--raft-tick-ms",
            "5",
            "--control-tick-ms",
            "5",
            "--data-dir",
            str(self.root / f"metadata-{node_id}"),
            "--replica-root-dir",
            str(self.root / f"metadata-{node_id}-replicas"),
            "--replica-catalog-path",
            str(self.root / f"metadata-{node_id}-catalog.txt"),
            "--snapshot-root-dir",
            str(self.root / f"metadata-{node_id}-snapshots"),
        ]

    def _data_command(self, index: int) -> list[str]:
        node_id = index + 4
        command = [
            self.binary,
            "data",
            "--config",
            str(self.config_path),
            "--api-host",
            self.host,
            "--api-port",
            str(self.data_ports[index]),
            "--raft-host",
            self.host,
            "--raft-port",
            str(self.data_raft_ports[index]),
            "--node-id",
            str(node_id),
            "--store-id",
            str(node_id),
            "--store-role",
            "data",
            "--health",
            "false",
            "--raft-tick-ms",
            "5",
            "--control-tick-ms",
            "5",
            "--data-dir",
            str(self.root / f"data-{node_id}"),
            "--replica-root-dir",
            str(self.root / f"data-{node_id}-replicas"),
            "--replica-catalog-path",
            str(self.root / f"data-{node_id}-catalog.txt"),
            "--snapshot-root-dir",
            str(self.root / f"data-{node_id}-snapshots"),
        ]
        for url in self.metadata_admin_urls:
            command.extend(["--metadata-api", url])
        return command

    def _start(self) -> None:
        for i in range(3):
            command = self._metadata_command(i + 1)
            proc = self.port_reservations.handoff_to(
                (self.metadata_raft_ports[i], self.metadata_admin_ports[i]),
                lambda: subprocess.Popen(
                    command,
                    stdout=self.metadata_log_files[i],
                    stderr=subprocess.STDOUT,
                    cwd=REPO_ROOT,
                ),
            )
            self.metadata_procs.append(proc)

        for url in self.metadata_admin_urls:
            if not wait_for_server(url, path="/metadata/v1/status", timeout=30.0):
                raise RuntimeError(
                    f"metadata server failed to start at {url}\n{self.debug_logs()}"
                )

        if self.metadata_stable_leader_id(timeout_s=30.0) is None:
            raise RuntimeError(
                "metadata cluster did not elect a leader; "
                "last_statuses="
                f"{_metadata_status_observations(self.last_metadata_statuses)!r}\n"
                f"{self.debug_logs()}"
            )

        for i, data_api_url in enumerate(self.data_api_urls):
            data_command = self._data_command(i)
            proc = self.port_reservations.handoff_to(
                (self.data_ports[i], self.data_raft_ports[i]),
                lambda command=data_command, log_file=self.data_log_files[i]: subprocess.Popen(
                    command,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    cwd=REPO_ROOT,
                ),
            )
            self.data_procs.append(proc)
            if not wait_for_server(data_api_url, timeout=30.0):
                raise RuntimeError(
                    f"data server failed to start at {data_api_url}\n{self.debug_logs()}"
                )

        if not wait_until(
            self.all_data_nodes_registered,
            timeout_s=60.0,
            interval_s=0.5,
        ):
            raise RuntimeError(
                "data nodes did not register on every metadata node\n"
                f"{self.debug_logs()}"
            )

    def metadata_snapshot(self, index: int) -> dict:
        response = requests.get(
            f"{self.metadata_admin_urls[index]}/metadata/v1/admin/snapshot",
            timeout=10,
        )
        return _check_response(response)

    def all_data_nodes_registered(self) -> bool:
        self.assert_processes_alive()
        expected_node_ids = set(range(4, 7))
        try:
            snapshots = [
                self.metadata_snapshot(index)
                for index in range(len(self.metadata_admin_urls))
            ]
        except (AssertionError, requests.RequestException, ValueError):
            return False
        for snapshot in snapshots:
            registered = {
                int(store.get("node_id", 0))
                for store in snapshot.get("stores", [])
                if isinstance(store, dict)
            }
            if not expected_node_ids.issubset(registered):
                return False
        return True

    def table_is_fully_replicated(self, table_name: str) -> bool:
        self.assert_processes_alive()
        expected_node_ids = set(range(4, 7))
        try:
            snapshots = [
                self.metadata_snapshot(index)
                for index in range(len(self.metadata_admin_urls))
            ]
        except (AssertionError, requests.RequestException, ValueError):
            return False

        for snapshot in snapshots:
            table_id = next(
                (
                    int(table.get("table_id", 0))
                    for table in snapshot.get("tables", [])
                    if isinstance(table, dict) and table.get("name") == table_name
                ),
                None,
            )
            if table_id is None:
                return False
            group_ids = {
                int(record.get("group_id", 0))
                for record in snapshot.get("ranges", [])
                if isinstance(record, dict)
                and int(record.get("table_id", 0)) == table_id
            }
            if len(group_ids) != 3:
                return False

            placed_nodes_by_group = {group_id: set() for group_id in group_ids}
            for intent in snapshot.get("placement_intents", []):
                if not isinstance(intent, dict) or not isinstance(
                    intent.get("record"), dict
                ):
                    continue
                record = intent["record"]
                group_id = int(record.get("group_id", 0))
                if group_id in placed_nodes_by_group:
                    placed_nodes_by_group[group_id].add(
                        int(record.get("local_node_id", 0))
                    )
            if any(
                placed_nodes != expected_node_ids
                for placed_nodes in placed_nodes_by_group.values()
            ):
                return False

            statuses = {
                int(status.get("group_id", 0)): status
                for status in snapshot.get("merged_group_statuses", [])
                if isinstance(status, dict)
                and int(status.get("group_id", 0)) in group_ids
            }
            if set(statuses) != group_ids:
                return False
            if any(
                status.get("leader_known") is not True
                or status.get("voter_count_known") is not True
                or int(status.get("voter_count", 0)) != 3
                or int(status.get("healthy_voter_reports", 0)) < 3
                for status in statuses.values()
            ):
                return False
        return True

    def table_topology(self, table_name: str) -> tuple[int, set[int]] | None:
        try:
            snapshot = self.metadata_snapshot(0)
        except (AssertionError, requests.RequestException, ValueError):
            return None
        table_id = next(
            (
                int(table.get("table_id", 0))
                for table in snapshot.get("tables", [])
                if isinstance(table, dict) and table.get("name") == table_name
            ),
            None,
        )
        if table_id is None:
            return None
        group_ids = {
            int(record.get("group_id", 0))
            for record in snapshot.get("ranges", [])
            if isinstance(record, dict)
            and int(record.get("table_id", 0)) == table_id
        }
        return table_id, group_ids

    def restore_progress_cleared(self, table_name: str) -> bool:
        self.assert_processes_alive()
        try:
            snapshots = [
                self.metadata_snapshot(index)
                for index in range(len(self.metadata_admin_urls))
            ]
        except (AssertionError, requests.RequestException, ValueError):
            return False
        for snapshot in snapshots:
            table_id = next(
                (
                    int(table.get("table_id", 0))
                    for table in snapshot.get("tables", [])
                    if isinstance(table, dict) and table.get("name") == table_name
                ),
                None,
            )
            if table_id is None:
                return False
            if any(
                isinstance(record, dict)
                and int(record.get("table_id", 0)) == table_id
                for record in snapshot.get("restore_progresses", [])
            ):
                return False
        return True

    def table_absent_on_all_metadata_nodes(
        self, table_name: str, table_id: int, group_ids: set[int]
    ) -> bool:
        self.assert_processes_alive()
        try:
            snapshots = [
                self.metadata_snapshot(index)
                for index in range(len(self.metadata_admin_urls))
            ]
        except (AssertionError, requests.RequestException, ValueError):
            return False
        return all(
            not any(
                isinstance(table, dict) and table.get("name") == table_name
                for table in snapshot.get("tables", [])
            )
            and not any(
                isinstance(record, dict)
                and (
                    int(record.get("table_id", 0)) == table_id
                    or int(record.get("group_id", 0)) in group_ids
                )
                for record in snapshot.get("ranges", [])
            )
            for snapshot in snapshots
        )

    def assert_processes_alive(self) -> None:
        exited = [
            f"metadata-{index}: exit={proc.poll()}"
            for index, proc in enumerate(self.metadata_procs, start=1)
            if proc.poll() is not None
        ]
        exited.extend(
            f"data-{index}: exit={proc.poll()}"
            for index, proc in enumerate(self.data_procs, start=4)
            if proc.poll() is not None
        )
        if exited:
            raise AssertionError(
                f"cluster process exited: {', '.join(exited)}\n{self.debug_logs()}"
            )

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
            f"[data-{i + 4}]\n{_read_log_tail(path)}"
            for i, path in enumerate(self.data_log_paths)
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
            statuses = list(executor.map(fetch, self.metadata_admin_urls))
        self.last_metadata_statuses = statuses
        return statuses

    def metadata_leader_id_once(self, *, request_timeout_s: float) -> int | None:
        statuses = self.metadata_statuses(request_timeout_s=request_timeout_s)
        return _metadata_quorum_leader_id(
            statuses, cluster_size=len(self.metadata_admin_urls)
        )

    def metadata_leader_id(self, *, timeout_s: float) -> int | None:
        def current_leader() -> int | None:
            return self.metadata_leader_id_once(
                request_timeout_s=min(1.0, max(0.05, timeout_s))
            )

        return wait_until(
            current_leader,
            timeout_s=timeout_s,
            interval_s=0.25,
            ready_when=lambda value: value is not None,
        )

    def metadata_stable_leader_id(
        self,
        *,
        timeout_s: float,
        stable_observations: int = 3,
        interval_s: float = 0.25,
    ) -> int | None:
        last_leader: int | None = None
        observed = 0

        def current_stable_leader() -> int | None:
            nonlocal last_leader, observed
            leader_id = self.metadata_leader_id_once(
                # Poll cadence and per-request latency are independent. A
                # 250ms status deadline was too aggressive under loaded CI and
                # amplified transient scheduler delay into a false outage.
                request_timeout_s=1.0
            )
            if leader_id is None:
                last_leader = None
                observed = 0
                return None
            if leader_id == last_leader:
                observed += 1
            else:
                last_leader = leader_id
                observed = 1
            return leader_id if observed >= stable_observations else None

        return wait_until(
            current_stable_leader,
            timeout_s=timeout_s,
            interval_s=interval_s,
            ready_when=lambda value: value is not None,
        )

    def metadata_leader_public_url(self, *, timeout_s: float = 30.0) -> str:
        leader_id = self.metadata_stable_leader_id(timeout_s=timeout_s)
        if leader_id is None:
            raise AssertionError(
                "metadata leader unavailable; "
                "last_statuses="
                f"{_metadata_status_observations(self.last_metadata_statuses)!r}\n"
                f"{self.debug_logs()}"
            )
        return self.metadata_public_urls[leader_id - 1]

    def stop(self, *, test_failed: bool = False) -> None:
        self.port_reservations.close()
        for proc in reversed(self.data_procs):
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        self.data_procs = []

        for proc in reversed(self.metadata_procs):
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        self.metadata_procs = []

        for handle in [*self.data_log_files, *self.metadata_log_files]:
            if not handle.closed:
                handle.close()
        if not maybe_preserve_tempdir(self.tempdir, failed=test_failed):
            self.tempdir.cleanup()


@pytest.fixture
def three_by_three_backup_cluster(
    request: pytest.FixtureRequest,
) -> ThreeByThreeBackupCluster:
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    resolved = Path(binary)
    if resolved.name != "antfly":
        pytest.skip("3x3 backup e2e requires the antfly binary")
    if not resolved.exists():
        pytest.skip(f"antfly binary not built: {resolved}")

    cluster = ThreeByThreeBackupCluster(str(resolved))
    try:
        yield cluster
    finally:
        report = getattr(request.node, "rep_call", None)
        cluster.stop(test_failed=bool(report and report.failed))


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


@pytest.mark.parametrize("backup_format", ["portable", "native"])
def test_table_backup_restore_round_trip_managed_chunked_semantic(
    backup_api, rate_limited_openai_embedder, backup_format: str
):
    table_name = f"backup_{backup_format}_chunked_semantic_{time.time_ns()}"
    backup_id = f"backup-{backup_format}-chunked-semantic-{time.time_ns()}"

    created = backup_api.create_table(
        table_name, num_shards=1, description="chunked semantic backup docs"
    )
    assert created["name"] == table_name
    rate_limited_openai_embedder.allow_all_requests()

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
                    "url": rate_limited_openai_embedder.url,
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
    before_status = backup_api.wait_index_ready(
        table_name,
        "semantic_chunked_idx",
        timeout_s=60.0,
        interval_s=0.5,
        require_query_fresh=True,
    )
    before_readiness_incarnation = before_status["readiness"]["incarnation"]
    before_coverage = {
        key: before_status["coverage"][key]
        for key in (
            "config_fingerprint",
            "source_total",
            "covered",
            "produced",
            "complete",
            "healthy",
        )
    }
    before_counts = {
        key: before_status[key]
        for key in ("total_indexed", "doc_count", "query_visible_doc_count")
    }

    with tempfile.TemporaryDirectory(
        prefix="antfly-backup-chunked-semantic-"
    ) as backup_dir:
        location = _file_location(backup_dir)

        backup = backup_api.backup_table(
            table_name,
            backup_id=backup_id,
            location=location,
            backup_format=backup_format,
        )
        assert backup["backup"] == "successful"
        if backup_format == "native":
            rate_limited_openai_embedder.deny_requests()
        embedder_before_restore = rate_limited_openai_embedder.stats()

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

        after_status = backup_api.wait_index_ready(
            table_name,
            "semantic_chunked_idx",
            timeout_s=180.0,
            interval_s=1.0,
            require_query_fresh=True,
        )
        if backup_format == "native":
            assert (
                after_status["readiness"]["incarnation"] == before_readiness_incarnation
            )
            assert {
                key: after_status["coverage"].get(key) for key in before_coverage
            } == before_coverage
            assert {
                key: after_status.get(key) for key in before_counts
            } == before_counts

            semantic_after = _dense_top_hit(
                backup_api,
                table_name,
                [1.0, 0.0, 0.0],
                "semantic_chunked_idx",
                "doc:a",
            )
            assert semantic_after is not None, {
                "status": after_status,
                "logs": backup_api.debug_logs(),
            }
            assert rate_limited_openai_embedder.stats() == embedder_before_restore
        else:
            semantic_after = wait_until(
                lambda: _semantic_top_hit(
                    backup_api,
                    table_name,
                    "alpha concept",
                    "semantic_chunked_idx",
                    "doc:a",
                ),
                timeout_s=120.0,
                interval_s=1.0,
            )
            if semantic_after is None:
                after_query = backup_api.query_table(
                    table_name,
                    {
                        "semantic_search": "alpha concept",
                        "indexes": ["semantic_chunked_idx"],
                        "limit": 5,
                        "fields": ["title", "_chunks", "_embeddings"],
                    },
                )
                raise AssertionError(
                    "portable semantic restore query did not recover; "
                    f"status={after_status}, query={after_query}, "
                    f"logs={backup_api.debug_logs()}"
                )

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


def test_three_by_three_cluster_backup_restore_through_metadata_public_api(
    three_by_three_backup_cluster: ThreeByThreeBackupCluster,
) -> None:
    cluster = three_by_three_backup_cluster
    table_name = f"metadata_leader_backup_{time.time_ns()}"
    backup_id = f"metadata-leader-backup-{time.time_ns()}"
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Connection"] = "close"

    data_api_url = cluster.data_api_urls[0]
    _check_response(
        session.post(
            f"{data_api_url}/tables/{table_name}",
            json={"num_shards": 3, "description": "3x3 backup and restore docs"},
            timeout=30,
        )
    )

    assert wait_until(
        lambda: cluster.table_is_fully_replicated(table_name) or None,
        timeout_s=90.0,
        interval_s=0.5,
    ), f"table did not reach 3x3 replication before backup\n{cluster.debug_logs()}"

    source_docs = {
        "0:backup": {
            "title": "Three by Three Alpha",
            "content": "low range backup and restore coverage",
        },
        "8:backup": {
            "title": "Three by Three Middle",
            "content": "middle range backup and restore coverage",
        },
        "z:backup": {
            "title": "Three by Three Omega",
            "content": "high range backup and restore coverage",
        },
    }
    batch = _check_response(
        session.post(
            f"{data_api_url}/tables/{table_name}/batch",
            json={"inserts": source_docs, "sync_level": "write"},
            timeout=30,
        )
    )
    assert batch["inserted"] == len(source_docs)
    assert wait_until(
        lambda: (
            True
            if all(
                (
                    doc := _lookup_doc_from_url(
                        session, data_api_url, table_name, key
                    )
                )
                is not None
                and doc.get("title") == expected["title"]
                for key, expected in source_docs.items()
            )
            else None
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
                timeout=30,
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
        table_manifests = [
            path
            for path in Path(backup_dir).glob("*-metadata.json")
            if not path.name.endswith("-cluster-metadata.json")
        ]
        assert len(table_manifests) == 1
        table_manifest = json.loads(table_manifests[0].read_text(encoding="utf-8"))
        assert len(table_manifest["shards"]) == 3
        assert len(
            {int(shard["group_id"]) for shard in table_manifest["shards"]}
        ) == 3
        routed_source_groups = set()
        for key in source_docs:
            matching_shards = [
                shard
                for shard in table_manifest["shards"]
                if key >= shard["start_key"]
                and (shard.get("end_key") is None or key < shard["end_key"])
            ]
            assert len(matching_shards) == 1, (
                f"source key {key!r} did not resolve to exactly one backup shard: "
                f"{matching_shards!r}"
            )
            routed_source_groups.add(int(matching_shards[0]["group_id"]))
        assert len(routed_source_groups) == len(source_docs), (
            "3x3 acceptance documents must exercise a non-empty payload in every shard"
        )
        assert all(
            (Path(backup_dir) / shard["snapshot_path"]).is_file()
            for shard in table_manifest["shards"]
        )

        original_topology = cluster.table_topology(table_name)
        assert original_topology is not None
        original_table_id, original_group_ids = original_topology
        assert len(original_group_ids) == 3

        deleted = session.delete(f"{data_api_url}/tables/{table_name}", timeout=30)
        deleted.raise_for_status()
        assert deleted.status_code == 204
        assert wait_until(
            lambda: cluster.table_absent_on_all_metadata_nodes(
                table_name, original_table_id, original_group_ids
            )
            or None,
            timeout_s=30.0,
            interval_s=0.5,
        ), f"table remained in metadata after delete\n{cluster.debug_logs()}"

        restore_response = None
        restore_coordinator_url = None
        last_response = None
        restore_payload = {
            "backup_id": backup_id,
            "location": _file_location(backup_dir),
            "connection": BACKUP_CONNECTION,
            "restore_mode": "fail_if_exists",
        }
        for _ in range(3):
            leader_public_url = cluster.metadata_leader_public_url(timeout_s=30.0)
            response = session.post(
                f"{leader_public_url}/restore",
                json=restore_payload,
                timeout=30,
            )
            if _is_metadata_not_leader_response(response):
                last_response = response
                continue
            restore_response = response
            restore_coordinator_url = leader_public_url
            break
        assert restore_response is not None, (
            "metadata leader stayed unavailable for restore after retries; "
            f"last_response={last_response.text if last_response is not None else None}\n"
            f"{cluster.debug_logs()}"
        )
        assert restore_coordinator_url is not None
        assert restore_response.status_code == 202, restore_response.text
        accepted = _check_response(restore_response)
        job_id = accepted.get("job_id")
        assert isinstance(job_id, str) and job_id

        def terminal_restore() -> dict | None:
            cluster.assert_processes_alive()
            candidate_urls = [
                restore_coordinator_url,
                *(
                    url
                    for url in cluster.metadata_public_urls
                    if url != restore_coordinator_url
                ),
            ]
            for api_url in candidate_urls:
                try:
                    response = session.get(
                        f"{api_url}/restore/jobs/{job_id}", timeout=1
                    )
                except requests.RequestException:
                    continue
                if response.status_code == 503:
                    continue
                job = _check_response(response)
                return (
                    job
                    if job.get("phase") in {"succeeded", "failed", "cancelled"}
                    else None
                )
            return None

        restore_job = wait_until(
            terminal_restore, timeout_s=120.0, interval_s=0.1
        )
        assert restore_job is not None, (
            f"restore job {job_id} did not finish\n{cluster.debug_logs()}"
        )
        assert restore_job["phase"] == "succeeded", (
            f"restore={restore_job}\n{cluster.debug_logs()}"
        )
        restore = restore_job["result"]
        assert restore["status"] == "completed"
        assert restore["committed_table_count"] == 1
        assert restore["failed_table_count"] == 0

        assert wait_until(
            lambda: cluster.table_is_fully_replicated(table_name) or None,
            timeout_s=90.0,
            interval_s=0.5,
        ), f"restored table did not converge to 3x3 replication\n{cluster.debug_logs()}"
        restored_topology = cluster.table_topology(table_name)
        assert restored_topology is not None
        restored_table_id, restored_group_ids = restored_topology
        assert restored_table_id == original_table_id
        assert len(restored_group_ids) == 3
        assert restored_group_ids.isdisjoint(original_group_ids), (
            "restore reused source physical Raft groups instead of allocating "
            f"a fresh incarnation: source={original_group_ids}, restored={restored_group_ids}"
        )

        def restored_docs_visible_from_every_data_node() -> bool | None:
            for api_url in cluster.data_api_urls:
                for key, expected in source_docs.items():
                    doc = _lookup_doc_from_url(session, api_url, table_name, key)
                    if doc is None or doc.get("title") != expected["title"]:
                        return None
            return True

        assert wait_until(
            restored_docs_visible_from_every_data_node,
            timeout_s=60.0,
            interval_s=0.5,
        ), f"restored documents were not readable through every data node\n{cluster.debug_logs()}"

        assert wait_until(
            lambda: cluster.restore_progress_cleared(table_name) or None,
            timeout_s=30.0,
            interval_s=0.25,
        ), f"completed restore progress was not retired\n{cluster.debug_logs()}"


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
