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
from pathlib import Path

import pytest
import requests

from conftest import (
    ANTFLY_PUBLIC_API_ROOT,
    DEFAULT_ANTFLY_BIN,
    REPO_ROOT,
    _read_log_tail,
    antfly_public_api_url,
    find_free_port,
    maybe_preserve_tempdir,
    resolve_binary_path,
    wait_for_server,
)
from helpers import wait_until

BACKUP_CONNECTION = "e2e-backups"


def _wait_for_terminal_restore_job(backup_api, response: requests.Response, *, timeout_s: float = 30.0) -> dict:
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


def _lookup_doc_from_url(session: requests.Session, api_url: str, table_name: str, key: str) -> dict | None:
    try:
        response = session.get(f"{api_url}/tables/{table_name}/documents/{key}", timeout=10)
        if response.status_code >= 400:
            return None
        payload = response.json()
        return payload if isinstance(payload, dict) else None
    except (requests.RequestException, ValueError):
        return None


def _lookup_docs(stateful_api, table_names: tuple[str, ...], key: str) -> dict[str, dict] | None:
    docs: dict[str, dict] = {}
    for table_name in table_names:
        doc = _lookup_doc(stateful_api, table_name, key)
        if doc is None:
            return None
        docs[table_name] = doc
    return docs


def _wait_until_absent(stateful_api, table_name: str, key: str, *, timeout_s: float, interval_s: float) -> None:
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


def _wait_until_table_absent(stateful_api, table_name: str, *, timeout_s: float, interval_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _lookup_table(stateful_api, table_name) is None:
            return
        time.sleep(interval_s)
    raise AssertionError(f"{table_name} remained visible after delete")


def _top_hit(stateful_api, table_name: str, query: str, expected_id: str) -> dict | None:
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


def _semantic_top_hit(stateful_api, table_name: str, query: str, index_name: str, expected_id: str) -> dict | None:
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


def _chunked_doc(stateful_api, table_name: str, key: str, chunk_field: str) -> list[dict] | None:
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


def _write_single_doc(stateful_api, table_name: str, key: str, *, title: str, content: str) -> None:
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
        raise AssertionError(f"{response.request.method} {response.url} failed: {response.text}") from exc
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def _is_metadata_not_leader_response(response: requests.Response) -> bool:
    return response.headers.get("X-Antfly-Metadata-Not-Leader", "").lower() == "true"


class MultiMetadataBackupCluster:
    def __init__(self, binary: str):
        self.binary = binary
        self.host = "127.0.0.1"
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-metadata-backup-e2e-")
        self.root = Path(self.tempdir.name)

        self.metadata_raft_ports = [find_free_port() for _ in range(3)]
        self.metadata_admin_ports = [find_free_port() for _ in range(3)]
        self.metadata_admin_urls = [f"http://{self.host}:{port}" for port in self.metadata_admin_ports]
        self.metadata_public_urls = [
            antfly_public_api_url(url, root=ANTFLY_PUBLIC_API_ROOT) for url in self.metadata_admin_urls
        ]
        self.data_port = find_free_port()
        self.data_raft_port = find_free_port()
        self.data_url = f"http://{self.host}:{self.data_port}"
        self.data_api_url = antfly_public_api_url(self.data_url, binary=binary)

        self.config_path = self.root / "antfly-metadata-cluster.json"
        self._write_config()

        self.metadata_log_paths = [self.root / f"metadata-{node_id}.log" for node_id in range(1, 4)]
        self.metadata_log_files = [path.open("w") for path in self.metadata_log_paths]
        self.data_log_path = self.root / "data.log"
        self.data_log_file = self.data_log_path.open("w")

        self.metadata_procs: list[subprocess.Popen[str]] = []
        self.data_proc: subprocess.Popen[str] | None = None

        try:
            self._start()
        except BaseException:
            self.stop()
            raise

    def _write_config(self) -> None:
        metadata = {
            "orchestration_urls": {
                str(node_id): self.metadata_admin_urls[node_id - 1] for node_id in range(1, 4)
            },
            "raft_urls": {
                str(node_id): f"http://{self.host}:{self.metadata_raft_ports[node_id - 1]}"
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
                    "replication_factor": 1,
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
            "--tick-ms",
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

    def _data_command(self) -> list[str]:
        command = [
            self.binary,
            "data",
            "--config",
            str(self.config_path),
            "--api-host",
            self.host,
            "--api-port",
            str(self.data_port),
            "--raft-host",
            self.host,
            "--raft-port",
            str(self.data_raft_port),
            "--node-id",
            "4",
            "--store-id",
            "4",
            "--store-role",
            "data",
            "--health",
            "false",
            "--tick-ms",
            "5",
            "--data-dir",
            str(self.root / "data"),
            "--replica-root-dir",
            str(self.root / "data-replicas"),
            "--replica-catalog-path",
            str(self.root / "data-catalog.txt"),
            "--snapshot-root-dir",
            str(self.root / "data-snapshots"),
        ]
        for url in self.metadata_admin_urls:
            command.extend(["--metadata-api", url])
        return command

    def _start(self) -> None:
        for i in range(3):
            proc = subprocess.Popen(
                self._metadata_command(i + 1),
                stdout=self.metadata_log_files[i],
                stderr=subprocess.STDOUT,
                cwd=REPO_ROOT,
            )
            self.metadata_procs.append(proc)

        for url in self.metadata_admin_urls:
            if not wait_for_server(url, path="/metadata/v1/status", timeout=30.0):
                raise RuntimeError(f"metadata server failed to start at {url}\n{self.debug_logs()}")

        if self.metadata_stable_leader_index(timeout_s=30.0) is None:
            raise RuntimeError(f"metadata cluster did not elect a leader\n{self.debug_logs()}")

        self.data_proc = subprocess.Popen(
            self._data_command(),
            stdout=self.data_log_file,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
        if not wait_for_server(self.data_api_url, timeout=30.0):
            raise RuntimeError(f"data server failed to start at {self.data_api_url}\n{self.debug_logs()}")

    def debug_logs(self) -> str:
        for handle in self.metadata_log_files:
            handle.flush()
        self.data_log_file.flush()
        parts = [
            f"[metadata-{i + 1}]\n{_read_log_tail(path)}"
            for i, path in enumerate(self.metadata_log_paths)
        ]
        parts.append(f"[data]\n{_read_log_tail(self.data_log_path)}")
        return "\n".join(parts)

    def metadata_statuses(self) -> list[dict | None]:
        statuses: list[dict | None] = []
        for url in self.metadata_admin_urls:
            try:
                response = requests.get(f"{url}/metadata/v1/status", timeout=5)
                statuses.append(_check_response(response))
            except (AssertionError, requests.RequestException, ValueError):
                statuses.append(None)
        return statuses

    def metadata_leader_index(self, *, timeout_s: float) -> int | None:
        def current_leader() -> int | None:
            statuses = self.metadata_statuses()
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
            return leader_id - 1

        return wait_until(current_leader, timeout_s=timeout_s, interval_s=0.25)

    def metadata_stable_leader_index(
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
            leader_index = self.metadata_leader_index(timeout_s=interval_s)
            if leader_index is None:
                last_leader = None
                observed = 0
                return None
            if leader_index == last_leader:
                observed += 1
            else:
                last_leader = leader_index
                observed = 1
            return leader_index if observed >= stable_observations else None

        return wait_until(current_stable_leader, timeout_s=timeout_s, interval_s=interval_s)

    def metadata_leader_public_url(self, *, timeout_s: float = 30.0) -> str:
        leader_index = self.metadata_stable_leader_index(timeout_s=timeout_s)
        if leader_index is None:
            raise AssertionError(f"metadata leader unavailable\n{self.debug_logs()}")
        return self.metadata_public_urls[leader_index]

    def stop(self) -> None:
        if self.data_proc is not None and self.data_proc.poll() is None:
            self.data_proc.send_signal(signal.SIGTERM)
            try:
                self.data_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.data_proc.kill()
                self.data_proc.wait()
        self.data_proc = None

        for proc in reversed(self.metadata_procs):
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        self.metadata_procs = []

        for handle in [self.data_log_file, *self.metadata_log_files]:
            if not handle.closed:
                handle.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


@pytest.fixture
def multi_metadata_backup_cluster() -> MultiMetadataBackupCluster:
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    resolved = Path(binary)
    if resolved.name != "antfly":
        pytest.skip("multi-metadata backup e2e requires the antfly binary")
    if not resolved.exists():
        pytest.skip(f"antfly binary not built: {resolved}")

    cluster = MultiMetadataBackupCluster(str(resolved))
    try:
        yield cluster
    finally:
        cluster.stop()


def test_table_backup_restore_round_trip(backup_api):
    table_name = f"backup_restore_{time.time_ns()}"
    backup_id = f"backup-{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1, description="backup restore docs")
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
        location = f"file://{backup_dir}"

        backup = backup_api.backup_table(table_name, backup_id=backup_id, location=location)
        assert backup["backup"] == "successful"

        deleted = backup_api.delete_table(table_name)
        assert deleted == {}

        _wait_until_table_absent(backup_api, table_name, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_name, "doc:db", timeout_s=10.0, interval_s=0.5)

        restore = backup_api.restore_table(table_name, backup_id=backup_id, location=location)
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


def test_table_backup_restore_round_trip_managed_chunked_semantic(backup_api, openai_embedder):
    table_name = f"backup_chunked_semantic_{time.time_ns()}"
    backup_id = f"backup-chunked-semantic-{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1, description="chunked semantic backup docs")
    assert created["name"] == table_name

    assert (
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
                    "url": openai_embedder,
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
        )
        == {}
    )

    backup_api.wait_index_ready(table_name, "semantic_chunked_idx", timeout_s=30.0, interval_s=0.5)

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
        lambda: _chunked_doc(backup_api, table_name, "doc:a", "semantic_chunked_idx_chunks"),
        timeout_s=60.0,
        interval_s=1.0,
    )
    assert before_scan is not None

    assert wait_until(
        lambda: _semantic_top_hit(backup_api, table_name, "alpha concept", "semantic_chunked_idx", "doc:a"),
        timeout_s=120.0,
        interval_s=1.0,
    )

    before_chunks = before_scan[0]["_chunks"]["semantic_chunked_idx_chunks"]
    assert len(before_chunks) >= 2

    with tempfile.TemporaryDirectory(prefix="antfly-backup-chunked-semantic-") as backup_dir:
        location = f"file://{backup_dir}"

        backup = backup_api.backup_table(table_name, backup_id=backup_id, location=location)
        assert backup["backup"] == "successful"

        deleted = backup_api.delete_table(table_name)
        assert deleted == {}

        _wait_until_table_absent(backup_api, table_name, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_name, "doc:a", timeout_s=10.0, interval_s=0.5)

        restore = backup_api.restore_table(table_name, backup_id=backup_id, location=location)
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
            lambda: _semantic_top_hit(backup_api, table_name, "alpha concept", "semantic_chunked_idx", "doc:a"),
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
            raise AssertionError(
                f"semantic restore query did not recover; status={after_status}, scan={after_scan}, query={after_query}"
            )
        assert semantic_after

        after_scan = wait_until(
            lambda: _chunked_doc(backup_api, table_name, "doc:a", "semantic_chunked_idx_chunks"),
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
        created = backup_api.create_table(table_name, num_shards=1, description=f"{table_name} docs")
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
            lambda tn=table_name, q=title.lower(), doc_id="doc:1": _top_hit(backup_api, tn, q, doc_id),
            timeout_s=60.0,
            interval_s=1.0,
        )

    with tempfile.TemporaryDirectory(prefix="antfly-cluster-backup-") as backup_dir:
        location = f"file://{backup_dir}"

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
        lambda: _lookup_doc_from_url(session, cluster.data_api_url, table_name, "doc:1"),
        timeout_s=30.0,
        interval_s=0.5,
    ), cluster.debug_logs()

    with tempfile.TemporaryDirectory(prefix="antfly-metadata-leader-cluster-backup-") as backup_dir:
        backup = None
        last_response: requests.Response | None = None
        for _ in range(3):
            leader_public_url = cluster.metadata_leader_public_url(timeout_s=30.0)
            response = session.post(
                f"{leader_public_url}/backup",
                json={
                    "backup_id": backup_id,
                    "location": f"file://{backup_dir}",
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
        assert backup["status"] == "completed", f"backup={backup}\n{cluster.debug_logs()}"
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
        created = backup_api.create_table(table_name, num_shards=1, description=f"{table_name} docs")
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
            lambda tn=table_name, q=title.lower(), doc_id="doc:1": _top_hit(backup_api, tn, q, doc_id),
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
        created = backup_api.create_table(table_name, num_shards=1, description=f"{table_name} docs")
        assert created["name"] == table_name
        _write_single_doc(backup_api, table_name, "doc:1", title=title, content=f"{title} backup source")
        assert wait_until(
            lambda tn=table_name, q=title.lower(), doc_id="doc:1": _top_hit(backup_api, tn, q, doc_id),
            timeout_s=60.0,
            interval_s=1.0,
        )

    with tempfile.TemporaryDirectory(prefix="antfly-cluster-modes-") as backup_dir:
        location = f"file://{backup_dir}"

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

        _write_single_doc(backup_api, table_a, "doc:1", title="Mutated Alpha", content="mutated alpha")
        _write_single_doc(backup_api, table_b, "doc:1", title="Mutated Beta", content="mutated beta")
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


def test_cluster_backup_restore_partial_statuses(backup_api):
    table_name = f"cluster_partial_{time.time_ns()}"
    backup_id = f"cluster-partial-{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1, description="partial backup docs")
    assert created["name"] == table_name
    _write_single_doc(backup_api, table_name, "doc:1", title="Partial Table", content="table survives partial backup")
    assert wait_until(
        lambda: _top_hit(backup_api, table_name, "partial table", "doc:1"),
        timeout_s=60.0,
        interval_s=1.0,
    )

    with tempfile.TemporaryDirectory(prefix="antfly-cluster-partial-") as backup_dir:
        location = f"file://{backup_dir}"

        backup = backup_api.cluster_backup(
            backup_id=backup_id,
            location=location,
            table_names=[table_name, "missing"],
        )
        assert backup["status"] == "partial"
        by_name = {table["name"]: table for table in backup["tables"]}
        assert by_name[table_name]["status"] == "completed"
        assert by_name["missing"]["status"] == "failed"
        assert "not found" in by_name["missing"]["error"]

        listed = backup_api.list_backups(location=location)
        matched = [item for item in listed["backups"] if item["backup_id"] == backup_id]
        assert len(matched) == 1
        assert matched[0]["tables"] == [table_name]

        backup_api.delete_table(table_name)
        _wait_until_table_absent(backup_api, table_name, timeout_s=10.0, interval_s=0.5)
        _wait_until_absent(backup_api, table_name, "doc:1", timeout_s=10.0, interval_s=0.5)

        restore_response = backup_api._request(
            "POST",
            "/restore",
            {
                "backup_id": backup_id,
                "location": location,
                "connection": BACKUP_CONNECTION,
                "table_names": [table_name, "missing"],
            },
        )
        restore_job = _wait_for_terminal_restore_job(backup_api, restore_response)
        assert restore_job["phase"] == "failed"
        restore = restore_job["result"]
        assert restore["status"] == "partial"
        assert restore["committed_table_count"] == 1
        assert restore["triggered_table_count"] == 0
        assert restore["skipped_table_count"] == 0
        assert restore["failed_table_count"] == 1
        assert restore["failure_details_truncated"] is False
        assert len(restore["failure_details"]) == 1
        assert restore["failure_details"][0]["table_name"] == "missing"
        assert "backup does not include table" in restore["failure_details"][0]["error"]

        restored_doc = wait_until(
            lambda: _lookup_doc(backup_api, table_name, "doc:1"),
            timeout_s=60.0,
            interval_s=1.0,
        )
        assert restored_doc is not None
        assert restored_doc["title"] == "Partial Table"


def test_backup_restore_request_validation(backup_api):
    with tempfile.TemporaryDirectory(prefix="antfly-backup-validate-") as backup_dir:
        location = f"file://{backup_dir}"
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
                {"backup_id": "snap", "location": "ftp://bucket/path", "connection": BACKUP_CONNECTION},
                "unsupported backup location",
            ),
            (
                "POST",
                f"/tables/{table_name}/restore",
                {"backup_id": "snap", "location": "ftp://bucket/path", "connection": BACKUP_CONNECTION},
                "unsupported backup location",
            ),
            (
                "POST",
                "/backup",
                {"backup_id": "snap", "location": "ftp://bucket/path", "connection": BACKUP_CONNECTION},
                "unsupported backup location",
            ),
            (
                "POST",
                "/restore",
                {"backup_id": "snap", "location": "ftp://bucket/path", "connection": BACKUP_CONNECTION},
                "unsupported backup location",
            ),
            (
                "POST",
                "/restore",
                {"backup_id": "snap", "location": location, "connection": BACKUP_CONNECTION, "restore_mode": "bogus"},
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


def test_list_backups_empty_location(backup_api):
    with tempfile.TemporaryDirectory(prefix="antfly-empty-backups-") as backup_dir:
        location = f"file://{backup_dir}"
        listed = backup_api.list_backups(location=location)
        assert listed == {"backups": []}


def test_restore_missing_backup_returns_bad_request(backup_api):
    table_name = f"restore_missing_{time.time_ns()}"
    missing_backup_id = f"missing-{time.time_ns()}"

    created = backup_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    with tempfile.TemporaryDirectory(prefix="antfly-missing-restore-") as backup_dir:
        location = f"file://{backup_dir}"

        table_restore = backup_api._request(
            "POST",
            f"/tables/{table_name}/restore",
            {
                "backup_id": missing_backup_id,
                "location": location,
                "connection": BACKUP_CONNECTION,
            },
        )
        table_job = _wait_for_terminal_restore_job(backup_api, table_restore)
        assert table_job["phase"] == "failed"
        assert table_job["error"] == "TableAlreadyExists"

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
