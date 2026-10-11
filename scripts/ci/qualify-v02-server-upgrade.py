#!/usr/bin/env python3
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

"""Qualify actual released server outputs; no fabricated legacy encodings.

Pass --old-bin v0.2.1=/path/to/released/antfly (repeat for each release),
--new-bin /path/to/main/antfly, and --work-dir /path/to/retained/evidence.
--seed-only creates reusable release-produced directories and backups.
--verify-only reuses them. Every fixture and server log is retained.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path


def wait(check, description, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError, AssertionError, IndexError) as exc:
            last = exc
        time.sleep(0.2)
    raise AssertionError(f"timed out: {description}; last error: {last}")


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, binary, root, label):
        self.binary, self.root, self.label = binary, root, label
        self.process = None

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "extensions").mkdir(exist_ok=True)
        config = self.root / "antfly.json"
        config.write_text(
            json.dumps(
                {
                    "connections": {
                        "upgrade-backups": {
                            "kind": "external_io",
                            "capabilities": ["backup.write", "restore.read"],
                            "external_io": {"protocol": "filesystem", "root": "/"},
                        }
                    }
                }
            )
        )
        http_port = port()
        self.url = f"http://127.0.0.1:{http_port}/db/v1"
        self.log = (self.root.parent / f"{self.root.name}-{self.label}.log").open("ab")
        command = [
            str(self.binary),
            "standalone",
            "--host",
            "127.0.0.1",
            "--port",
            str(http_port),
            "--health-port",
            str(port()),
            "--config",
            str(config),
            "--data-dir",
            str(self.root),
            "--replica-root-dir",
            str(self.root / "replicas"),
            "--replica-catalog-path",
            str(self.root / "catalog.txt"),
            "--snapshot-root-dir",
            str(self.root / "snapshots"),
            "--extension-package-store",
            str(self.root / "extensions"),
        ]
        if self.root.name == "ha-primary":
            command.extend(
                [
                    "--ha-primary-log",
                    str(self.root / "ha-primary.wal"),
                    "--ha-primary-slots",
                    str(self.root / "ha-slots"),
                    "--ha-primary-node-id",
                    "upgrade-primary",
                    "--admin-token-env",
                    "ANTFLY_UPGRADE_QUALIFICATION_TOKEN",
                    "--ha-fence-wal",
                    str(self.root / "ha-fence.wal"),
                    "--ha-seed-capture-root",
                    str(self.root / "captures"),
                    "--ha-cluster-id",
                    "847",
                    "--ha-timeline-id",
                    "1",
                    "--ha-epoch",
                    "1",
                ]
            )
        self.process = subprocess.Popen(
            command,
            stdout=self.log,
            stderr=self.log,
            env={
                **os.environ,
                "ANTFLY_UPGRADE_QUALIFICATION_TOKEN": "upgrade-test-token",
            },
        )
        try:
            wait(
                lambda: self.request("GET", "/tables") is not None,
                f"{self.label} startup",
                timeout=60,
            )
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *_):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.log.close()

    def crash(self):
        self.process.send_signal(signal.SIGKILL)
        self.process.wait(timeout=10)

    def request(self, method, path, payload=None):
        if self.process.poll() is not None:
            raise RuntimeError(f"{self.label} exited: {self.process.returncode}")
        data = json.dumps(payload).encode() if payload is not None else None
        base = (
            self.url.removesuffix("/db/v1") if path.startswith("/admin/") else self.url
        )
        headers = {"Content-Type": "application/json"}
        if path.startswith("/admin/"):
            headers["Authorization"] = "Bearer upgrade-test-token"
        request = urllib.request.Request(
            base + path, data=data, method=method, headers=headers
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raise AssertionError(
                f"{method} {path}: {exc.code} {exc.read().decode()}"
            ) from exc

    def query(self, payload):
        return self.request("POST", "/query", {"table": "upgrade", **payload})[
            "responses"
        ][0]

    def ids(self, payload):
        return [hit["_id"] for hit in self.query(payload)["hits"]["hits"]]

    def batch(self, inserts=None, deletes=None):
        return wait(
            lambda: self.request(
                "POST",
                "/tables/upgrade/batch",
                {
                    "inserts": inserts or {},
                    "deletes": deletes or [],
                    "sync_level": "full_index",
                },
            ),
            "idempotent batch admission",
        )


class Swarm:
    """Stopped-upgrade corpus with real metadata/data Raft journals."""

    def __init__(self, binary, root, label):
        self.binary, self.root, self.label = binary, root, label
        self.nodes = []
        self.logs = []

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True)
        ports_file = self.root / "ports.json"
        if not ports_file.exists():
            ports_file.write_text(json.dumps([port() for _ in range(4)]))
        meta_api, meta_raft, data_api, data_raft = json.loads(ports_file.read_text())
        config = self.root / "config.json"
        config.write_text(
            json.dumps(
                {
                    "metadata": {
                        "orchestration_urls": {"1": f"http://127.0.0.1:{meta_api}"},
                        "raft_urls": {"1": f"http://127.0.0.1:{meta_raft}"},
                    },
                    "replication_factor": 1,
                    "default_shards_per_table": 1,
                }
            )
        )
        self.metadata = Server(self.binary, self.root, self.label)
        self.data = Server(self.binary, self.root, self.label)
        try:
            for role, api_port, raft_port, node_id in (
                ("metadata", meta_api, meta_raft, 1),
                ("data", data_api, data_raft, 2),
            ):
                root = self.root / role
                root.mkdir(exist_ok=True)
                command = [
                    str(self.binary),
                    role,
                    "--config",
                    str(config),
                    "--api-host",
                    "127.0.0.1",
                    "--api-port",
                    str(api_port),
                    "--raft-host",
                    "127.0.0.1",
                    "--raft-port",
                    str(raft_port),
                    "--health",
                    "false",
                    "--data-dir",
                    str(root),
                    "--replica-root-dir",
                    str(root / "replicas"),
                    "--replica-catalog-path",
                    str(root / "catalog.txt"),
                    "--snapshot-root-dir",
                    str(root / "snapshots"),
                ]
                if role == "metadata":
                    command.extend(["--id", str(node_id), "--auth", "false"])
                else:
                    command.extend(
                        [
                            "--node-id",
                            str(node_id),
                            "--store-id",
                            str(node_id),
                            "--store-role",
                            "data",
                            "--metadata-api",
                            f"http://127.0.0.1:{meta_api}",
                        ]
                    )
                log = (self.root / f"{role}-{self.label}.log").open("ab")
                self.logs.append(log)
                process = subprocess.Popen(
                    command,
                    stdout=log,
                    stderr=log,
                    env={
                        **os.environ,
                        "ANTFLY_INTERNAL_SERVICE_SECRET": "upgrade-test-internal-service-secret",
                        "ANTFLY_INTERNAL_SERVICE_ISSUER": "upgrade-test",
                    },
                )
                self.nodes.append(process)
                endpoint = self.metadata if role == "metadata" else self.data
                endpoint.process = process
                endpoint.url = f"http://127.0.0.1:{api_port}/db/v1"
                wait(
                    lambda: endpoint.request("GET", "/tables") is not None,
                    f"{role} startup",
                )
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        for process in reversed(self.nodes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        for log in self.logs:
            log.close()


SCHEMA = {
    "default_type": "doc",
    "document_schemas": {
        "doc": {
            "schema": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "body": {"type": "string", "x-antfly-types": ["text"]},
                    "category": {"type": "string", "x-antfly-types": ["keyword"]},
                    "count": {"type": "integer", "x-antfly-types": ["numeric"]},
                },
            }
        }
    },
}


def document(body, vector, target=None):
    result = {
        "_type": "doc",
        "body": body,
        "category": "old",
        "count": 7,
        "_embeddings": {
            "dense": vector,
            "sparse": {"10": vector[0] * 2, "20": vector[1] * 2, "30": vector[2] * 2},
        },
    }
    if target:
        result["_edges"] = {"links": {"knows": [{"target": target}]}}
    return result


TEXT_QUERY = {"full_text_search": {"match": "zebrafish", "field": "body"}, "limit": 10}
VECTOR_QUERY = {"embeddings": {"dense": [1, 0, 0]}, "indexes": ["dense"], "limit": 2}
SPARSE_QUERY = {
    "embeddings": {"sparse": {"indices": [10], "values": [2]}},
    "indexes": ["sparse"],
    "limit": 2,
}


def verify(server, expected=("a",), vectors=True):
    assert server.request("GET", "/tables/upgrade")["schema"]["default_type"] == "doc"
    wait(lambda: sorted(server.ids(TEXT_QUERY)) == sorted(expected), "full-text rows")
    row = server.request("GET", "/tables/upgrade/documents/a")
    assert row["body"].startswith("zebrafish") and row["count"] == 7, row
    if vectors:
        wait(
            lambda: (
                sorted(
                    server.ids(
                        {
                            "full_text_search": {"term": "old", "field": "category"},
                            "limit": 10,
                        }
                    )
                )
                == sorted((*expected, "b"))
            ),
            "exact keyword rows",
        )
        wait(lambda: server.ids(VECTOR_QUERY)[0] == "a", "dense nearest neighbor")
        wait(lambda: server.ids(SPARSE_QUERY)[0] == "a", "sparse nearest neighbor")

        def graph_ready():
            result = server.query(
                {
                    "graph_queries": {
                        "neighbors": {
                            "index": "links",
                            "traverse": {
                                "start": {"keys": ["a"]},
                                "edge_types": ["knows"],
                                "max_depth": 1,
                            },
                        }
                    },
                    "limit": 10,
                }
            )
            return [
                node["key"] for node in result["graph_results"]["neighbors"]["nodes"]
            ] == ["b"]

        wait(graph_ready, "persisted graph edge")


def backup(server, location, kind):
    response = server.request(
        "POST",
        "/tables/upgrade/backup",
        {
            "backup_id": f"released-{kind}",
            "location": location.as_uri(),
            "connection": "upgrade-backups",
            "format": kind,
        },
    )
    assert response.get("backup") == "successful", response


def seed(binary, release, evidence):
    version = subprocess.check_output(
        [str(binary), "--version"], text=True, stderr=subprocess.STDOUT
    ).strip()
    assert release.removeprefix("v") in version, version
    clean = evidence / "clean"
    assert not clean.exists(), f"fixture already exists: {clean}; use --verify-only"
    with Server(binary, clean, "released-create") as server:
        server.request("POST", "/tables/upgrade", {"schema": SCHEMA})
        for name, config in {
            "dense": {"type": "embeddings", "external": True, "dimension": 3},
            "sparse": {"type": "embeddings", "external": True, "sparse": True},
            "links": {"type": "graph", "edge_types": [{"name": "knows"}]},
        }.items():
            server.request("POST", f"/tables/upgrade/indexes/{name}", config)
        server.batch(
            {
                "a": document("zebrafish alpha", [1, 0, 0], "b"),
                "b": document("harbor beta", [0, 1, 0]),
            }
        )
        verify(server)
        for kind in ("portable", "native"):
            location = evidence / f"backup-{kind}"
            location.mkdir()
            backup(server, location, kind)
    with Server(binary, clean, "released-control") as server:
        verify(server)
        server.batch({"control": document("released control", [0, 0, 1])})
        server.batch(deletes=["control"])
    # Make an independent directory through the old writer, then kill it with
    # a durably admitted mutation. Do not synthesize or patch recovery bytes.
    with Server(binary, evidence / "crashed", "released-crash") as server:
        server.request("POST", "/tables/upgrade", {"schema": SCHEMA})
        server.batch({"a": document("zebrafish crash", [1, 0, 0])})
        verify(server, vectors=False)
        server.crash()
    files = {}
    for path in evidence.rglob("*"):
        if path.is_file() and path.suffix != ".log":
            files[str(path.relative_to(evidence))] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    (evidence / "producer.json").write_text(
        json.dumps(
            {
                "release": release,
                "version": version,
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "files": files,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"{release}: released writer and same-binary control passed", flush=True)


def seed_ha(binary, release, evidence):
    root = evidence / "ha-primary"
    assert not root.exists(), f"HA fixture already exists: {root}"
    with Server(binary, root, "released-ha-create") as server:
        server.request("POST", "/tables/upgrade", {"schema": SCHEMA})
        server.batch({"a": document("zebrafish replicated", [1, 0, 0])})
        verify(server, vectors=False)
        receipt = server.request(
            "POST",
            "/admin/v1/ha/base-backups/capture",
            {
                "slot_name": "upgrade-fixture",
                "generation": "released-seed",
                "topology_id": "upgrade",
                "topology_generation": 1,
                "node_id": "upgrade-standby",
                "target_pvc_name": "upgrade-data",
                "target_pvc_uid": "upgrade-data-uid",
            },
        )
        (evidence / "ha-capture.json").write_text(json.dumps(receipt, indent=2) + "\n")
    with Server(binary, root, "released-ha-control") as server:
        verify(server, vectors=False)
    print(
        f"{release}: released HA WAL/capture and same-binary control passed", flush=True
    )


def qualify_swarm(
    binary, old_binary, release, evidence, seed_only=False, verify_only=False
):
    root = evidence / "swarm"
    if not verify_only:
        assert not root.exists(), f"swarm fixture already exists: {root}"
        with Swarm(old_binary, root, "released-create") as swarm:
            wait(
                lambda: (
                    swarm.metadata.request(
                        "POST", "/tables/upgrade", {"schema": SCHEMA}
                    )
                    is not None
                ),
                "cluster table create",
            )
            swarm.data.batch({"a": document("zebrafish raft", [1, 0, 0])})
            verify(swarm.data, vectors=False)
        with Swarm(old_binary, root, "released-control") as swarm:
            verify(swarm.data, vectors=False)
        print(f"{release}: released swarm and same-binary control passed", flush=True)
    if not seed_only:
        for label in ("main-first", "main-second"):
            with Swarm(binary, root, label) as swarm:
                verify(swarm.data, vectors=False)
                swarm.data.batch({"c": document("raft control", [0, 0, 1])})
                swarm.data.batch(deletes=["c"])
        print(
            f"{release}: stopped swarm upgrade, reads/writes/restart passed", flush=True
        )


def qualify_ha(binary, release, evidence):
    for label in ("main-ha-first", "main-ha-second"):
        with Server(binary, evidence / "ha-primary", label) as server:
            verify(server, vectors=False)
            server.batch({"c": document("HA control", [0, 0, 1])})
            server.batch(deletes=["c"])
    print(f"{release}: HA primary WAL reopen and writes/restart passed", flush=True)


def restore(server, location, kind):
    response = server.request(
        "POST",
        "/tables/upgrade/restore",
        {
            "backup_id": f"released-{kind}",
            "location": location.as_uri(),
            "connection": "upgrade-backups",
        },
    )
    if response.get("job_id"):

        def finished():
            job = server.request("GET", f"/restore/jobs/{response['job_id']}")
            return (
                job
                if job.get("phase") in ("failed", "cancelled", "succeeded")
                else None
            )

        job = wait(finished, f"{kind} restore job")
        assert job["phase"] == "succeeded", job
    verify(server)


def qualify(binary, old_binary, release, evidence, formats):
    assert json.loads((evidence / "producer.json").read_text())["release"] == release
    for name in ("clean", "crashed"):
        root = evidence / name
        with Server(binary, root, "main-first") as server:
            verify(server, vectors=name == "clean")
            server.batch({"c": document("zebrafish added", [0, 0, 1])})
            verify(server, ("a", "c"), vectors=name == "clean")
            server.batch(deletes=["c"])
            verify(server, vectors=name == "clean")
            server.batch(
                {
                    "a": document(
                        "zebrafish updated", [1, 0, 0], "b" if name == "clean" else None
                    )
                }
            )
            verify(server, vectors=name == "clean")
        marker = json.loads((root / "ANTFLY_FORMAT").read_text())
        assert marker["min_reader_storage_format"] > 1, marker
        with Server(binary, root, "main-second") as server:
            verify(server, vectors=name == "clean")
        # The real old reader must reject the marker before touching old data.
        rejected = subprocess.run(
            [
                str(old_binary),
                "standalone",
                "--data-dir",
                str(root),
                "--port",
                str(port()),
                "--health-port",
                str(port()),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert rejected.returncode != 0, rejected.stdout + rejected.stderr
        assert "unsupported Antfly data directory format" in rejected.stderr, (
            rejected.stderr
        )
    for kind in formats:
        root = evidence / f"restored-{kind}-{time.time_ns()}"
        with Server(binary, root, "restore") as server:
            restore(server, evidence / f"backup-{kind}", kind)
            server.batch({"c": document("zebrafish restored", [0, 0, 1])})
            verify(server, ("a", "c"))
        with Server(binary, root, "restore-reopen") as server:
            verify(server, ("a", "c"))
    print(
        f"{release}: reopen, updates/deletes, crash recovery, rollback gate, {'/'.join(formats)} restore passed",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--old-bin", action="append", required=True, metavar="RELEASE=PATH"
    )
    parser.add_argument("--new-bin", type=Path)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument(
        "--backup-format",
        action="append",
        choices=("portable", "native"),
        help="Limit restore qualification (default: both)",
    )
    parser.add_argument(
        "--ha-only",
        action="store_true",
        help="Generate/qualify HA primary WAL and runtime seed captures",
    )
    parser.add_argument(
        "--swarm-only",
        action="store_true",
        help="Qualify stopped metadata/data Raft upgrade",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--seed-only", action="store_true")
    mode.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    if not args.seed_only and not args.new_bin:
        parser.error("--new-bin is required unless --seed-only")
    for item in args.old_bin:
        release, path = item.split("=", 1)
        assert release in {f"v0.2.{i}" for i in range(1, 6)}, release
        binary = Path(path).resolve()
        evidence = args.work_dir.resolve() / release
        evidence.mkdir(parents=True, exist_ok=True)
        if args.swarm_only:
            qualify_swarm(
                args.new_bin.resolve() if args.new_bin else None,
                binary,
                release,
                evidence,
                seed_only=args.seed_only,
                verify_only=args.verify_only,
            )
            continue
        if not args.verify_only:
            if args.ha_only:
                seed_ha(binary, release, evidence)
            else:
                seed(binary, release, evidence)
        if not args.seed_only:
            if args.ha_only:
                qualify_ha(args.new_bin.resolve(), release, evidence)
            else:
                qualify(
                    args.new_bin.resolve(),
                    binary,
                    release,
                    evidence,
                    args.backup_format or ("portable", "native"),
                )


if __name__ == "__main__":
    main()
