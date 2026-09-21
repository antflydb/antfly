# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
"""Secret administration and credential use through real deployment processes."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
import requests
from conftest import (
    DEFAULT_ANTFLY_BIN,
    REPO_ROOT,
    _data_command,
    _metadata_command,
    _serverless_combined_command,
    _standalone_stateful_command,
    internal_service_headers,
    resolve_binary_path,
)
from port_reservations import LoopbackPortReservations

pytestmark = pytest.mark.e2e_resource("antfly_process")

ADMIN_TOKEN = "secret-e2e-admin-token-with-at-least-32-bytes"
KEY = "provider.api_key"


def eventually(predicate, timeout=45):
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if result:
            return result
        assert time.monotonic() < deadline, "secret deployment did not converge"
        time.sleep(0.1)


def replace_json(path: Path, value):
    pending = path.with_suffix(".pending")
    pending.write_text(json.dumps(value))
    pending.replace(path)


def file_secret(path: Path, value: str | None):
    replace_json(
        path,
        {
            "secrets": []
            if value is None
            else [
                {
                    "key": KEY,
                    "value": value,
                    "created_at_ns": 1,
                    "updated_at_ns": time.time_ns(),
                }
            ]
        },
    )


class CredentialProvider:
    """An OpenAI-compatible endpoint that records credential use, not just CRUD."""

    def __init__(self):
        self.calls = []
        self.lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                payload = json.loads(
                    self.rfile.read(int(self.headers["Content-Length"]))
                )
                inputs = payload["input"]
                if isinstance(inputs, str):
                    inputs = [inputs]
                with outer.lock:
                    outer.calls.extend(
                        (text, self.headers.get("Authorization")) for text in inputs
                    )
                body = json.dumps(
                    {
                        "object": "list",
                        "data": [
                            {
                                "object": "embedding",
                                "index": i,
                                "embedding": [0.1, 0.2, 0.3],
                            }
                            for i in range(len(inputs))
                        ],
                        "model": "test",
                        "usage": {"prompt_tokens": 1, "total_tokens": 1},
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def saw(self, text, value):
        with self.lock:
            matching = [header for content, header in self.calls if text in content]
        if matching:
            assert all(header == f"Bearer {value}" for header in matching), matching
        return bool(matching)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class TableObjectStorage:
    """Small S3 HTTP fixture for serverless table lanes, separate from secrets.

    Serverless --config requires object connections. Keep those lanes local while
    native secrets use the production filesystem, S3, or GCS adapter under test.
    This fixture makes no claim about real S3 credential/CAS qualification.
    """

    def __init__(self):
        self.objects = {}
        self.content_types = {}
        self.lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def handle_request(self):
                parsed = urlsplit(self.path)
                path = unquote(parsed.path).lstrip("/")
                bucket, _, key = path.partition("/")
                query = parse_qs(parsed.query)
                size = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(size) if size else b""
                status, response, headers = 200, b"", {}
                with outer.lock:
                    current = outer.objects.get(path)
                    etag = (
                        ('"' + hashlib.sha256(current).hexdigest() + '"')
                        if current is not None
                        else None
                    )
                    if self.command == "PUT":
                        if (
                            self.headers.get("If-None-Match") == "*"
                            and current is not None
                        ) or (
                            self.headers.get("If-Match")
                            and self.headers["If-Match"] != etag
                        ):
                            status = 412
                        else:
                            outer.objects[path] = body
                            outer.content_types[path] = self.headers.get(
                                "Content-Type", "application/octet-stream"
                            )
                            headers["ETag"] = (
                                '"' + hashlib.sha256(body).hexdigest() + '"'
                            )
                    elif self.command == "DELETE":
                        outer.objects.pop(path, None)
                        outer.content_types.pop(path, None)
                        status = 204
                    elif self.command in ("GET", "HEAD") and not key:
                        root = ET.Element("ListBucketResult")
                        ET.SubElement(root, "Name").text = bucket
                        ET.SubElement(root, "IsTruncated").text = "false"
                        prefix = query.get("prefix", [""])[0]
                        for name, value in sorted(outer.objects.items()):
                            if not name.startswith(bucket + "/" + prefix):
                                continue
                            entry = ET.SubElement(root, "Contents")
                            ET.SubElement(entry, "Key").text = name[len(bucket) + 1 :]
                            ET.SubElement(entry, "Size").text = str(len(value))
                            ET.SubElement(
                                entry, "LastModified"
                            ).text = "2026-01-01T00:00:00.000Z"
                            ET.SubElement(entry, "ETag").text = (
                                '"' + hashlib.sha256(value).hexdigest() + '"'
                            )
                        response = ET.tostring(root)
                        headers["Content-Type"] = "application/xml"
                    elif self.command in ("GET", "HEAD") and current is not None:
                        response = current
                        headers["ETag"] = etag
                        headers["Content-Type"] = outer.content_types[path]
                        if requested := self.headers.get("Range"):
                            start, _, end = requested.removeprefix("bytes=").partition(
                                "-"
                            )
                            lo, hi = int(start), int(end) if end else len(current) - 1
                            response = current[lo : hi + 1]
                            status = 206
                            headers["Content-Range"] = f"bytes {lo}-{hi}/{len(current)}"
                    else:
                        status = 404
                        response = b"<Error><Code>NoSuchKey</Code></Error>"
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(response)

            do_GET = do_HEAD = do_PUT = do_DELETE = handle_request

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.endpoint = f"127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class SecretDeployment:
    def __init__(self, root: Path, mode: str, native_uri=None):
        self.root, self.mode = root, mode
        root.mkdir(parents=True)
        self.binary = resolve_binary_path(
            os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
        )
        assert Path(self.binary).is_file(), f"Build antfly first: {self.binary}"
        self.ports = LoopbackPortReservations("127.0.0.1")
        self.nodes = []
        self.table_objects = None
        self.env = os.environ.copy()
        self.env.update(
            PROVIDER_API_KEY="environment-fallback",
            ANTFLY_SECRET_ADMIN_TOKEN=ADMIN_TOKEN,
        )
        self.keyring = root / "keyring.json"
        replace_json(
            self.keyring, {"active": "one", "keys": [{"id": "one", "key": "11" * 32}]}
        )
        self.fallback = root / "fallback.json"
        file_secret(self.fallback, "file-fallback")
        self.native_file = root / "native.json"
        self.reader_key = root / "reader.key"
        self.reader_key.write_text("22" * 32)
        self.data_key = root / "data-reader.key"
        self.data_key.write_text("22" * 32)
        self.native_uri = native_uri or f"file://{root / 'secrets'}"
        if mode == "serverless" and native_uri is None:
            (root / "secrets/buckets/native-secrets").mkdir(parents=True)

    def config(self, native):
        cfg = {
            "secrets": {
                "native": native,
                "sources": [
                    {"name": "mounted", "type": "file", "path": str(self.fallback)}
                ],
            },
            "replication_factor": 1,
            "default_shards_per_table": 1,
            "remote_content": {"security": {"block_private_ips": False}},
        }

        if self.mode == "serverless":
            cfg.update(
                {
                    "deployment_mode": "serverless",
                    "storage": {
                        "engine": "object",
                        "object": {
                            "connection": "tables",
                            "bucket": "tables",
                            "prefix": f"worker-{len(self.nodes)}",
                        },
                    },
                    "connections": {
                        "tables": {
                            "kind": "external_io",
                            "capabilities": ["storage.primary"],
                            "external_io": {
                                "protocol": "s3",
                                "addressing_style": "path",
                                "endpoint": self.table_objects.endpoint,
                                "use_ssl": False,
                                "region": "us-east-1",
                                "buckets": ["tables"],
                                "credentials": {
                                    "source": "static",
                                    "access_key_id": "test",
                                    "secret_access_key": "test",
                                },
                            },
                        }
                    },
                }
            )
        return cfg

    def add(self, name, command, ports, config, env=None):
        path = self.root / f"{name}.json"
        replace_json(path, config)
        if "--config" in command:
            command[command.index("--config") + 1] = str(path)
        else:
            command += ["--config", str(path)]
        node = {
            "name": name,
            "command": command,
            "ports": ports,
            "env": env or self.env,
            "url": f"http://127.0.0.1:{ports[-1]}",
            "proc": None,
        }
        self.nodes.append(node)
        self.start(node)
        return node

    def start(self, node):
        log = (self.root / f"{node['name']}.log").open("ab")
        try:
            node["proc"] = self.ports.handoff_to(
                node["ports"],
                lambda: subprocess.Popen(
                    node["command"],
                    env=node["env"],
                    cwd=REPO_ROOT,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                ),
            )
        finally:
            log.close()

    def stop(self, node):
        proc = node["proc"]
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)

    def restart(self, node):
        self.stop(node)
        # The original reservation has been handed off; reacquire before restart.
        self.ports.ensure_reserved(*node["ports"])
        self.start(node)
        self.ready(node)

    def ready(self, node):
        def check():
            assert node["proc"].poll() is None, (
                self.root / f"{node['name']}.log"
            ).read_text()[-12000:]
            try:
                return self.request(node, "GET", "/secrets").status_code == 200
            except requests.RequestException:
                return False

        eventually(check)

    def request(self, node, method, path, **kwargs):
        headers = (
            {"Authorization": f"Bearer {ADMIN_TOKEN}"}
            if self.mode == "serverless"
            else {}
        )
        headers.update(kwargs.pop("headers", {}))
        prefix = (
            "" if self.mode == "serverless" and path.startswith("/tables") else "/db/v1"
        )
        return requests.request(
            method,
            f"{node['url']}{prefix}{path}",
            headers=headers,
            timeout=15,
            **kwargs,
        )

    def launch(self):
        host = "127.0.0.1"
        if self.mode == "standalone":
            port = self.ports.reserve()
            node = self.add(
                "standalone",
                _standalone_stateful_command(
                    self.binary, host=host, port=port, root=self.root
                ),
                [port],
                self.config({"path": str(self.native_file)}),
            )
            self.admin = self.consumer = node
        elif self.mode == "serverless":
            self.table_objects = TableObjectStorage()
            for i in range(2):
                root = self.root / f"worker-{i}"
                root.mkdir()
                env = self.env.copy()
                for kind in ("artifacts", "manifests", "wal", "progress", "catalog"):
                    env.pop(f"ANTFLY_SERVERLESS_{kind.upper()}_URI", None)
                port = self.ports.reserve()
                self.add(
                    f"serverless-{i}",
                    _serverless_combined_command(
                        self.binary, host=host, port=port, root=root
                    ),
                    [port],
                    self.config(
                        {
                            "backend": "serverless",
                            "path": self.native_uri,
                            "keyring_path": str(self.keyring),
                        }
                    ),
                    env,
                )
            self.admin, self.consumer = self.nodes
        else:
            ports = [self.ports.reserve_many(2) for _ in range(3)]
            metadata = {
                "raft_urls": {
                    str(i + 1): f"http://{host}:{p[0]}" for i, p in enumerate(ports)
                },
                "orchestration_urls": {
                    str(i + 1): f"http://{host}:{p[1]}" for i, p in enumerate(ports)
                },
            }
            for i, p in enumerate(ports):
                root = self.root / f"metadata-{i}"
                root.mkdir()
                cfg = self.config(
                    {
                        "backend": "distributed",
                        "keyring_path": str(self.keyring),
                        "grants": [
                            {
                                "name": "data",
                                "credential_path": str(self.reader_key),
                                "keys": [KEY],
                            }
                        ],
                    }
                )
                cfg["metadata"] = metadata
                command = _metadata_command(
                    self.binary, host=host, raft_port=p[0], admin_port=p[1], root=root
                )
                self.add(f"metadata-{i}", command + ["--id", str(i + 1)], p, cfg)
            for node in self.nodes:
                self.ready(node)
            api_port, raft_port = self.ports.reserve_many(2)
            cfg = self.config(
                {
                    "backend": "distributed",
                    "reader": {
                        "name": "data",
                        "credential_path": str(self.data_key),
                        "urls": [n["url"] for n in self.nodes],
                    },
                }
            )
            cfg["metadata"] = metadata
            command = _data_command(
                self.binary,
                host=host,
                port=api_port,
                raft_port=raft_port,
                metadata_admin_base_uri=self.nodes[0]["url"],
                root=self.root / "data",
            )
            for node in self.nodes[1:]:
                command += ["--metadata-api", node["url"]]
            command[command.index("--node-id") + 1] = "4"
            command[command.index("--store-id") + 1] = "4"
            self.consumer = self.add("data", command, [raft_port, api_port], cfg)
            self.admin = self.nodes[1]
        for node in self.nodes:
            self.ready(node)
        return self

    def put(self, value, key=KEY, node=None):
        response = self.request(
            node or self.admin, "PUT", f"/secrets/{key}", json={"value": value}
        )
        assert response.status_code == 200, response.text
        assert "application/json" in response.headers["Content-Type"]
        assert value not in response.text
        return response.json()

    def metadata(self, node=None):
        response = self.request(node or self.consumer, "GET", "/secrets")
        assert response.status_code == 200, response.text
        assert "application/json" in response.headers["Content-Type"]
        entries = response.json()["secrets"]
        assert all("value" not in entry for entry in entries)
        if self.mode == "serverless":
            assert response.headers["Cache-Control"] == "no-store"
        return {entry["key"]: entry for entry in entries}

    def close(self):
        for node in reversed(self.nodes):
            self.stop(node)
        self.ports.close()
        if self.table_objects:
            self.table_objects.close()


@pytest.fixture
def provider():
    server = CredentialProvider()
    try:
        yield server
    finally:
        server.close()


def create_consumer(deployment, provider):
    def create():
        response = deployment.request(
            deployment.consumer,
            "PUT" if deployment.mode == "serverless" else "POST",
            "/tables/secret_consumer",
            json={
                "created_at_ns": 100,
                "policy": {
                    "chunk_embeddings_enabled": True,
                    "chunk_embeddings_publish_min_pending_records": 1,
                },
            }
            if deployment.mode == "serverless"
            else {"num_shards": 1},
        )
        if response.status_code == 503:
            return False  # Data-node routing/reporting may lag the first election.
        assert response.status_code in (200, 201), response.text
        return True

    eventually(create)
    response = deployment.request(
        deployment.consumer,
        "POST",
        "/tables/secret_consumer/indexes/embedding",
        json={
            "name": "embedding",
            "type": "embeddings",
            "field": "text",
            "dimension": 3,
            "chunker": {
                "provider": "antfly",
                "model": "fixed",
                "text": {"target_tokens": 256},
            },
            "embedder": {
                "provider": "openai",
                "model": "test",
                "url": provider.url,
                "api_key": "${secret:provider.api_key}",
            },
        },
    )
    assert response.status_code in (200, 201, 202), response.text


def use_secret(deployment, provider, expected):
    marker = uuid.uuid4().hex
    response = deployment.request(
        deployment.consumer,
        "POST",
        "/tables/secret_consumer/batch",
        json={
            "inserts": {marker: {"text": marker}},
            "sync_level": "write",
        },
    )
    assert response.status_code in (200, 201), response.text

    def observed():
        assert deployment.consumer["proc"].poll() is None, (
            deployment.root / f"{deployment.consumer['name']}.log"
        ).read_text()[-12000:]
        return provider.saw(marker, expected)

    eventually(observed)


@pytest.mark.parametrize("mode", ["standalone", "distributed", "serverless"])
def test_secret_runtime_rotation_restart_and_fallback(tmp_path, mode, provider):
    deployment = SecretDeployment(tmp_path / mode, mode)
    try:
        deployment.launch()
        first = deployment.put("native-first-credential")
        assert deployment.metadata()[KEY]["source"] == "native"
        create_consumer(deployment, provider)
        use_secret(deployment, provider, "native-first-credential")
        second = deployment.put("native-rotated-credential")
        if mode != "standalone":
            assert second["revision"] > first["revision"]
            assert deployment.metadata()[KEY]["revision"] == second["revision"]
        use_secret(deployment, provider, "native-rotated-credential")
        deployment.restart(deployment.consumer)
        use_secret(deployment, provider, "native-rotated-credential")
        response = deployment.request(deployment.admin, "DELETE", f"/secrets/{KEY}")
        assert response.status_code == 204, response.text
        use_secret(deployment, provider, "file-fallback")
        file_secret(deployment.fallback, None)
        use_secret(deployment, provider, "environment-fallback")
        if mode == "standalone":
            file_secret(deployment.native_file, "external-rotation")
            use_secret(deployment, provider, "external-rotation")
        else:
            # A missing wrapping key must fail closed instead of using fallback.
            deployment.put("native-encrypted-credential")
            saved = deployment.keyring.read_bytes()
            deployment.keyring.write_text("{}")
            # Listing need not decrypt, but an actual provider invocation must.
            marker = uuid.uuid4().hex
            response = deployment.request(
                deployment.consumer,
                "POST",
                "/tables/secret_consumer/batch",
                json={
                    "inserts": {marker: {"text": marker}},
                    "sync_level": "write",
                },
            )
            assert response.status_code in (200, 201, 500, 503), response.text
            time.sleep(2)
            with provider.lock:
                assert not any(marker in text for text, _ in provider.calls)
            deployment.keyring.write_bytes(saved)
            use_secret(deployment, provider, "native-encrypted-credential")
        for log in deployment.root.glob("*.log"):
            assert "native-first-credential" not in log.read_text(errors="replace")
            assert "native-rotated-credential" not in log.read_text(errors="replace")
    finally:
        deployment.close()


def test_serverless_secret_admin_auth_and_concurrent_writers(tmp_path):
    deployment = SecretDeployment(tmp_path / "serverless", "serverless")
    try:
        deployment.launch()
        for method, path in [
            ("GET", "/secrets"),
            ("PUT", f"/secrets/{KEY}"),
            ("DELETE", f"/secrets/{KEY}"),
        ]:
            response = requests.request(
                method,
                f"{deployment.admin['url']}/db/v1{path}",
                json={"value": "denied"},
                timeout=5,
            )
            assert response.status_code == 401
            assert response.headers["Cache-Control"] == "no-store"
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda i: deployment.put(
                        f"value-{i}", key=f"key-{i}", node=deployment.nodes[i]
                    ),
                    range(2),
                )
            )
        assert len({entry["revision"] for entry in results}) == 2
        for node in deployment.nodes:
            assert {"key-0", "key-1"} <= deployment.metadata(node).keys()
            deployment.restart(node)
            assert {"key-0", "key-1"} <= deployment.metadata(node).keys()
    finally:
        deployment.close()


def test_distributed_secret_follower_failover_revocation_and_outage(tmp_path, provider):
    deployment = SecretDeployment(tmp_path / "distributed", "distributed")
    try:
        deployment.launch()
        metadata = deployment.nodes[:3]

        def leader():
            for node in metadata:
                if node["proc"].poll() is not None:
                    continue
                response = requests.get(
                    f"{node['url']}/metadata/v1/status",
                    headers=internal_service_headers(),
                    timeout=5,
                )
                response.raise_for_status()
                if response.json().get("metadata_raft_role") == "leader":
                    return node
            return None

        original = eventually(leader)
        deployment.admin = next(node for node in metadata if node is not original)
        committed = deployment.put("follower-committed-secret")
        create_consumer(deployment, provider)
        use_secret(deployment, provider, "follower-committed-secret")
        original["proc"].kill()
        original["proc"].wait(timeout=10)
        replacement = eventually(leader)
        assert replacement is not original
        deployment.admin = replacement
        assert deployment.metadata()[KEY]["revision"] == committed["revision"]
        deployment.put("post-election-secret")
        use_secret(deployment, provider, "post-election-secret")
        deployment.restart(original)
        assert (
            deployment.metadata(original)[KEY]["revision"]
            == deployment.metadata()[KEY]["revision"]
        )
        # Internal service identity alone cannot read ungranted names.
        deployment.put("not-granted", key="private.api_key")
        assert "private.api_key" not in deployment.metadata()
        assert (
            deployment.request(
                deployment.consumer,
                "PUT",
                f"/secrets/{KEY}",
                json={"value": "read-only"},
            ).status_code
            == 503
        )
        deployment.data_key.write_text("33" * 32)
        assert (
            deployment.request(deployment.consumer, "GET", "/secrets").status_code
            == 503
        )
        deployment.data_key.write_text("22" * 32)
        use_secret(deployment, provider, "post-election-secret")
        for node in metadata:
            deployment.stop(node)
        # Local mounted/environment fallback must not mask native unavailability.
        assert (
            deployment.request(deployment.consumer, "GET", "/secrets").status_code
            == 503
        )
    finally:
        deployment.close()


@pytest.mark.objectstore_integration
@pytest.mark.parametrize("backend,scheme", [("S3", "s3"), ("GCS", "gs")])
def test_cloud_secret_conditional_publication(tmp_path, backend, scheme):
    if os.getenv(f"OBJECTSTORE_{backend}_INTEGRATION") != "1":
        pytest.skip(f"OBJECTSTORE_{backend}_INTEGRATION=1 required")
    bucket = os.environ[f"OBJECTSTORE_{backend}_TEST_BUCKET"]
    # Isolated namespace in an explicitly configured qualification bucket.
    uri = f"{scheme}://{bucket}/antfly-secret-e2e/{uuid.uuid4().hex}"
    deployment = SecretDeployment(tmp_path / backend, "serverless", uri)
    try:
        deployment.launch()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda i: deployment.put(
                        f"cloud-{i}", key=f"key-{i}", node=deployment.nodes[i]
                    ),
                    range(2),
                )
            )
        assert len({item["revision"] for item in results}) == 2
        for node in deployment.nodes:
            deployment.restart(node)
            assert {"key-0", "key-1"} <= deployment.metadata(node).keys()
        for i in range(2):
            assert (
                deployment.request(
                    deployment.admin, "DELETE", f"/secrets/key-{i}"
                ).status_code
                == 204
            )
        assert not ({"key-0", "key-1"} & deployment.metadata().keys())
    finally:
        deployment.close()
