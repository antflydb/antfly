# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run with ANTFLY_NATIVE_BINARY=/absolute/path/to/antfly for wire qualification."""

import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urlsplit, unquote
from pyiceberg.io.pyarrow import PyArrowFileIO, PyArrowFile

import pyarrow as pa
import pytest
from pydantic import TypeAdapter
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder
from pyiceberg.table.metadata import new_table_metadata, TableMetadataUtil
from pyiceberg.table.update import TableRequirement, TableUpdate, update_table_metadata

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from native_catalog import NativeCatalog
from lite_state import State


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RestAuthority(BaseHTTPRequestHandler):
    """Independent PyIceberg requirement/update oracle, no embedded SQL catalog."""

    def log_message(self, *args):
        pass

    def respond(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def result(self):
        return {
            "metadata-location": self.server.location,
            "metadata": self.server.metadata.model_dump(
                by_alias=True, mode="json", exclude_none=True
            ),
        }

    def object_path(self):
        path = unquote(urlsplit(self.path).path)
        assert path.startswith("/archive/")
        relative = path[len("/archive/") :]
        assert ".." not in Path(relative).parts
        return self.server.root / relative

    def object_read(self, head=False):
        if self.path.rstrip("/") == "/archive":
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        path = self.object_path()
        if not path.is_file():
            return self.respond(404, {})
        data = path.read_bytes()
        etag = '"' + hashlib.md5(data).hexdigest() + '"'
        if self.headers.get("If-Match") and self.headers["If-Match"] != etag:
            return self.respond(412, {})
        start, end = 0, len(data) - 1
        if range_header := self.headers.get("Range"):
            first, last = range_header.removeprefix("bytes=").split("-")
            start, end = int(first), min(int(last), end)
        self.send_response(206 if self.headers.get("Range") else 200)
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("ETag", etag)
        self.send_header("Last-Modified", "Thu, 08 Oct 2026 00:00:00 GMT")
        if self.headers.get("Range"):
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.end_headers()
        if not head:
            self.wfile.write(data[start : end + 1])

    def do_HEAD(self):
        self.object_read(head=True)

    def do_PUT(self):
        path = self.object_path()
        if self.headers.get("If-None-Match") == "*" and path.exists():
            return self.respond(412, {})
        if expected := self.headers.get("If-Match"):
            actual = (
                '"' + hashlib.md5(path.read_bytes()).hexdigest() + '"'
                if path.exists()
                else None
            )
            if actual != expected:
                return self.respond(412, {})
        path.parent.mkdir(parents=True, exist_ok=True)
        data = self.rfile.read(int(self.headers["Content-Length"]))
        path.write_bytes(data)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.send_header("ETag", '"' + hashlib.md5(data).hexdigest() + '"')
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/archive"):
            return self.object_read()
        if self.path == "/v1/config":
            self.respond(200, {"defaults": {}, "overrides": {}})
        elif self.server.metadata is None:
            self.respond(
                404,
                {
                    "error": {
                        "message": "missing",
                        "type": "NoSuchTableException",
                        "code": 404,
                    }
                },
            )
        else:
            self.respond(200, self.result())

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        try:
            if "schema" in body:
                if self.server.metadata is not None:
                    return self.respond(
                        409,
                        {
                            "error": {
                                "message": "exists",
                                "type": "AlreadyExistsException",
                                "code": 409,
                            }
                        },
                    )
                metadata = new_table_metadata(
                    Schema.model_validate(body["schema"]),
                    PartitionSpec.model_validate(body["partition-spec"]),
                    SortOrder.model_validate(body["write-order"]),
                    body["location"],
                    body.get("properties", {}),
                )
            else:
                for requirement in body["requirements"]:
                    TypeAdapter(TableRequirement).validate_python(requirement).validate(
                        self.server.metadata
                    )
                updates = tuple(
                    TypeAdapter(TableUpdate).validate_python(value)
                    for value in body["updates"]
                )
                metadata = update_table_metadata(
                    self.server.metadata,
                    updates,
                    enforce_validation=True,
                    metadata_location=self.server.location,
                )
            self.server.metadata = metadata
            self.server.revision += 1
            location = (
                self.server.root
                / "metadata"
                / f"rest-{self.server.revision}.metadata.json"
            )
            location.parent.mkdir(parents=True, exist_ok=True)
            location.write_text(
                metadata.model_dump_json(by_alias=True, exclude_none=True)
            )
            self.server.location = "s3://archive/" + str(
                location.relative_to(self.server.root)
            )
            self.respond(200, self.result())
        except Exception as error:
            self.respond(
                409,
                {
                    "error": {
                        "message": str(error),
                        "type": "CommitFailedException",
                        "code": 409,
                    }
                },
            )


@pytest.mark.parametrize("mode", ["managed", "rest"])
def test_native_catalog_file_commit_read_and_restart(tmp_path, mode):
    binary = os.environ.get("ANTFLY_NATIVE_BINARY")
    if not binary:
        pytest.skip("set ANTFLY_NATIVE_BINARY for native HTTP qualification")
    port = free_port()
    root = tmp_path / "warehouse"
    root.mkdir()
    authority = ThreadingHTTPServer(("127.0.0.1", 0), RestAuthority)
    authority.metadata, authority.revision, authority.root = None, 0, root
    authority.location = ""
    threading.Thread(target=authority.serve_forever, daemon=True).start()
    origin = f"http://127.0.0.1:{authority.server_port}"
    storage_connection = {
        "kind": "external_io",
        "capabilities": ["lake_read", "lake_write", "storage.primary"],
        "external_io": {
            "protocol": "s3",
            "endpoint": origin,
            "use_ssl": False,
            "addressing_style": "path",
            "buckets": ["archive"],
            "credentials": {
                "source": "static",
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
            },
        },
    }
    config = {
        "storage": {
            "engine": "local",
            "local": {"base_dir": str(tmp_path / "data")},
            "artifacts": {
                "connection": "objects",
                "bucket": "archive",
                "prefix": "journal",
            },
        },
        "connections": {"objects": storage_connection},
    }
    catalog_config = {"type": "managed"}
    if mode == "rest":
        config["connections"]["catalog"] = {
            "kind": "external_io",
            "capabilities": ["lake_catalog_read", "lake_catalog_write"],
            "external_io": {"protocol": "http", "hosts": [origin]},
        }
        catalog_config = {
            "type": "rest",
            "connection": "catalog",
            "uri": origin,
            "namespace": ["hackernews"],
            "name": "items",
        }
    warehouse = "s3://archive/hn"

    class LocalS3IO(PyArrowFileIO):
        def local(self, location):
            assert location.startswith("s3://archive/")
            return (root / location[len("s3://archive/") :]).as_uri()

        def new_input(self, location):
            return PyArrowFile(
                location,
                unquote(urlsplit(self.local(location)).path),
                pa.fs.LocalFileSystem(),
            )

        def new_output(self, location):
            path = Path(unquote(urlsplit(self.local(location)).path))
            path.parent.mkdir(parents=True, exist_ok=True)
            return PyArrowFile(location, str(path), pa.fs.LocalFileSystem())

        def delete(self, location):
            return super().delete(self.local(location))

    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    endpoint = f"http://127.0.0.1:{port}/db/v1"
    log = (tmp_path / "server.log").open("w")
    process = None
    state = State(tmp_path / "ingestion.aflite")

    def call(method, path, body=None):
        request = Request(
            endpoint + path,
            None if body is None else json.dumps(body).encode(),
            {"Content-Type": "application/json"},
            method=method,
        )
        with urlopen(request, timeout=30) as response:
            return json.load(response)

    def start():
        nonlocal process
        process = subprocess.Popen(
            [
                str(Path(binary).resolve()),
                "standalone",
                "--config",
                str(config_path),
                "--data-dir",
                str(tmp_path / "data"),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--health",
                "false",
                "--auth",
                "false",
                "--models-dir",
                str(tmp_path / "models"),
            ],
            stdout=log,
            stderr=log,
        )
        for _ in range(150):
            assert process.poll() is None, (tmp_path / "server.log").read_text()[-4000:]
            try:
                call("GET", "/tables")
                return
            except (URLError, HTTPError):
                time.sleep(0.2)
        pytest.fail("daemon did not start")

    def stop():
        process.terminate()
        try:
            process.wait(20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    try:
        start()
        call(
            "POST",
            "/tables/hn",
            {
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {"amount": {"type": "integer"}},
                                "additionalProperties": False,
                            }
                        }
                    },
                    "base_source": {
                        "kind": "external",
                        "format": "iceberg",
                        "uri": warehouse,
                        "credentials": {"ref": "objects", "scope": "hn"},
                        "table_id": "hn",
                        "write_policy": "iceberg_writer",
                        "catalog": catalog_config,
                    },
                }
            },
        )
        catalog = NativeCatalog(state, warehouse, endpoint, "hn")
        catalog._load_file_io = lambda *args, **kwargs: LocalS3IO()
        table = catalog.create_table(
            "hackernews.items",
            pa.schema([pa.field("amount", pa.int64())]),
            location=warehouse,
            properties={"format-version": "2"},
        )
        with table.update_spec() as spec_update:
            spec_update.add_identity("amount")
        table.append(
            pa.table(
                {"amount": [1, 2, 3]},
                schema=pa.schema([pa.field("amount", pa.int64())]),
            )
        )
        loaded = call("GET", "/tables/hn/lake/catalog")
        TableMetadataUtil.parse_obj(loaded["metadata"])
        assert (
            loaded["metadata"]["current-snapshot-id"]
            == table.current_snapshot().snapshot_id
        )
        rows = call("POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn"})
        assert rows["rows"] == [["3"]]
        assert not (root / "hn" / "metadata" / "version-hint.text").exists()
        stop()
        start()
        assert (
            call("GET", "/tables/hn/lake/catalog")["metadata_location"]
            == loaded["metadata_location"]
        )
        assert call("POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn"})[
            "rows"
        ] == [["3"]]
    finally:
        if process and process.poll() is None:
            stop()
        log.close()
        state.db.close()
        if authority:
            authority.shutdown()
            authority.server_close()
