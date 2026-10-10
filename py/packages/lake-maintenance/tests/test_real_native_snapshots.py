# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Public cursor qualification across live native split, merge and restart."""

import base64
import hashlib
import hmac
import json
import os
import socket
import subprocess
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import pytest
import requests

pytestmark = pytest.mark.skipif(
    not os.environ.get("ANTFLY_NATIVE_BINARY")
    or os.environ.get("ANTFLY_REAL_CATALOGS") != "1",
    reason="requires native binary and disposable qualification S3",
)


class Cluster:
    def __init__(self, directory):
        self.directory, self.processes, self.logs = directory, [], []
        listeners = [socket.socket() for _ in range(8)]
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        self.ports = [listener.getsockname()[1] for listener in listeners]
        for listener in listeners:
            listener.close()
        self.metadata = f"http://127.0.0.1:{self.ports[0]}"
        self.endpoint = f"http://127.0.0.1:{self.ports[2]}/db/v1"
        self.secret = "qualification_internal_service_secret_123456789"
        self.environment = {
            **os.environ,
            "ANTFLY_INTERNAL_SERVICE_SECRET": self.secret,
            "ANTFLY_INTERNAL_SERVICE_ISSUER": "snapshot-qualification",
        }
        prefix = "snapshots/" + uuid.uuid4().hex
        self.configuration = {
            "deployment_mode": "distributed",
            "lake_indexes": {"query_cursors": {"retention_ms": 3600000}},
            "storage": {
                "engine": "local",
                "artifacts": {
                    "connection": "qualification",
                    "bucket": "warehouse",
                    "prefix": prefix,
                },
            },
            "connections": {
                "qualification": {
                    "kind": "external_io",
                    "capabilities": ["storage.primary"],
                    "external_io": {
                        "protocol": "s3",
                        "endpoint": "http://127.0.0.1:29700",
                        "use_ssl": False,
                        "addressing_style": "path",
                        "buckets": ["warehouse"],
                        "prefix": prefix,
                        "credentials": {
                            "source": "static",
                            "access_key_id": "antfly_qualification",
                            "secret_access_key": "antfly_qualification_secret",
                        },
                    },
                }
            },
        }

    def start(self, name, arguments):
        config = {
            **self.configuration,
            "storage": {
                **self.configuration["storage"],
                "local": {"base_dir": str(self.directory / name)},
            },
        }
        path = self.directory / (name + ".json")
        path.write_text(json.dumps(config))
        log = (self.directory / (name + ".log")).open("a")
        self.logs.append(log)
        process = subprocess.Popen(
            [
                str(Path(os.environ["ANTFLY_NATIVE_BINARY"]).resolve()),
                *arguments,
                "--config",
                str(path),
                "--data-dir",
                str(self.directory / name),
                "--health",
                "false",
                "--auth",
                "false",
            ],
            stdout=log,
            stderr=log,
            env=self.environment,
        )
        self.processes.append(process)
        return process

    def data_arguments(self, ordinal):
        api, raft = self.ports[2 + ordinal * 2 : 4 + ordinal * 2]
        return [
            "data",
            "--node-id",
            str(ordinal + 2),
            "--store-id",
            str(ordinal + 2),
            "--metadata-api",
            self.metadata,
            "--api-port",
            str(api),
            "--raft-port",
            str(raft),
            "--api-advertise-url",
            f"http://127.0.0.1:{api}",
            "--raft-advertise-url",
            f"http://127.0.0.1:{raft}",
            "--failure-domain",
            "qualification-" + str(ordinal),
        ]

    def launch(self):
        cluster = {
            "1": {
                "raft_url": f"http://127.0.0.1:{self.ports[1]}",
                "orchestration_url": self.metadata,
            }
        }
        self.start(
            "metadata",
            [
                "metadata",
                "--id",
                "1",
                "--raft-port",
                str(self.ports[1]),
                "--api-port",
                str(self.ports[0]),
                "--cluster",
                json.dumps(cluster),
            ],
        )
        self.nodes = [
            self.start("data" + str(i), self.data_arguments(i)) for i in range(3)
        ]

    def diagnostics(self):
        return "\n".join(
            path.read_text()[-4000:] for path in self.directory.glob("*.log")
        )

    @staticmethod
    def stop(process):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

    def close(self):
        for process in reversed(self.processes):
            self.stop(process)
        for log in self.logs:
            log.close()

    def call(self, method, path, body=None, *, internal=False):
        headers = {}
        if internal:

            def b64(value):
                return base64.urlsafe_b64encode(value).rstrip(b"=")

            now = int(time.time())
            claims = {
                "iss": "snapshot-qualification",
                "sub": "antfly-node",
                "aud": "antfly-internal-v1",
                "principal_kind": "service",
                "admin": True,
                "iat": now,
                "exp": now + 60,
            }
            signed = (
                b64(b'{"alg":"HS256","typ":"JWT"}')
                + b"."
                + b64(json.dumps(claims).encode())
            )
            token = (
                signed
                + b"."
                + b64(hmac.new(self.secret.encode(), signed, hashlib.sha256).digest())
            )
            headers["X-Antfly-Trusted-Principal"] = token.decode()
        response = requests.request(
            method,
            (self.metadata if internal else self.endpoint) + path,
            json=body,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        return (
            response.json()
            if response.headers.get("Content-Type", "").startswith("application/json")
            else response.text
        )

    def eventually(self, operation, predicate=lambda result: bool(result), timeout=90):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = operation()
                if predicate(last):
                    return last
            except requests.RequestException as error:
                last = str(error)
            time.sleep(0.2)
        pytest.fail(f"native cluster did not converge: {last}; {self.diagnostics()}")


def test_public_cursor_recovers_original_cover_after_live_split_merge_restart(tmp_path):
    cluster = Cluster(tmp_path)
    try:
        cluster.launch()
        cluster.eventually(
            lambda: cluster.call(
                "POST",
                "/tables/current",
                {
                    "num_shards": 1,
                    "schema": {
                        "default_type": "row",
                        "document_schemas": {
                            "row": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"body": {"type": "string"}},
                                }
                            }
                        },
                    },
                    "indexes": {"body_text": {"type": "full_text"}},
                },
            )
        )
        cluster.eventually(
            lambda: cluster.call(
                "POST",
                "/tables/current/batch",
                {
                    "inserts": {"a": {"body": "original"}, "z": {"body": "original"}},
                    "sync_level": "full_index",
                },
            )
        )
        query = {
            "full_text_search": {"match": "original", "field": "body"},
            "order_by": [{"field": "_id"}],
            "limit": 1,
        }
        first = cluster.eventually(
            lambda: cluster.call("POST", "/tables/current/query", query)["responses"][
                0
            ],
            lambda value: len(value["hits"]["hits"]) == 1,
        )
        assert first["hits"]["hits"][0]["_id"] == "a"
        page = {
            **query,
            "remote_snapshot": first["remote_snapshot"],
            "search_after": first["hits"]["hits"][0]["_sort"],
        }

        def routing():
            return cluster.call(
                "POST", "/internal/v1/catalog/linearizable-snapshot", {}, internal=True
            )

        before = routing()
        (tmp_path / "before.json").write_text(json.dumps(before))
        identity = cluster.call("GET", "/tables/current/sources/managed")["table_id"]
        table = next(
            value for value in before["tables"] if value["table_id"] == identity
        )
        physical = quote(table["name"], safe="")
        cluster.call(
            "POST",
            f"/internal/v1/tables/{physical}/split",
            {"split_key": "m"},
            internal=True,
        )

        def ranges():
            return [
                value
                for value in routing()["ranges"]
                if value["table_id"] == table["table_id"]
            ]

        def finalized(kind):
            records = [
                record
                for record in routing()[kind + "_transitions"]
                if record["table_contract"]["table_id"] == identity
            ]
            expected = 2 if kind == "split" else 1
            current = [
                value for value in routing()["ranges"] if value["table_id"] == identity
            ]
            # Finalized records may already have been garbage collected.
            return len(current) == expected and all(
                record["phase"] == "finalized" for record in records
            )

        cluster.eventually(lambda: finalized("split"), timeout=180)
        (tmp_path / "after-split.json").write_text(json.dumps(routing()))
        divided = sorted(
            cluster.eventually(ranges, lambda value: len(value) == 2),
            key=lambda value: value["start_key"],
        )
        resumed = cluster.eventually(
            lambda: cluster.call("POST", "/tables/current/query", page)["responses"][0]
        )
        assert [hit["_id"] for hit in resumed["hits"]["hits"]] == ["z"]
        cluster.call(
            "POST",
            f"/internal/v1/tables/{physical}/merge",
            {
                "donor_group_id": divided[1]["group_id"],
                "receiver_group_id": divided[0]["group_id"],
                "allow_doc_identity_reassignment": True,
            },
            internal=True,
        )
        cluster.eventually(lambda: finalized("merge"), timeout=180)
        cluster.eventually(ranges, lambda value: len(value) == 1)
        for process in cluster.nodes:
            cluster.stop(process)
        cluster.nodes = [
            cluster.start("data" + str(i), cluster.data_arguments(i)) for i in range(3)
        ]
        restored = cluster.eventually(
            lambda: cluster.call("POST", "/tables/current/query", page)["responses"][0]
        )
        assert [hit["_id"] for hit in restored["hits"]["hits"]] == ["z"]
        assert restored["remote_snapshot"] == first["remote_snapshot"]
    finally:
        cluster.close()
