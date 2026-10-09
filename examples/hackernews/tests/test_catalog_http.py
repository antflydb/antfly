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

"""Run with ANTFLY_NATIVE_BINARY=/absolute/path/to/antfly for wire qualification."""

import hashlib
import io
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


def atomic_authority_operation(method):
    def execute(self, *args, **kwargs):
        with self._state_lock:
            return method(self, *args, **kwargs)

    return execute


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RestAuthority(BaseHTTPRequestHandler):
    """Independent PyIceberg requirement/update oracle, no embedded SQL catalog."""

    # Match object-store atomic reads and conditional writes. Filesystem
    # truncate/write and a separate ETag check cannot model those guarantees
    # when native queries, enrichment, and publication run concurrently.
    _state_lock = threading.RLock()

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

    @atomic_authority_operation
    def object_read(self, head=False):
        if urlsplit(self.path).path.rstrip("/") == "/archive":
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

    @atomic_authority_operation
    def do_PUT(self):
        if urlsplit(self.path).path.rstrip("/") == "/archive":
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        path = self.object_path()
        if (
            getattr(self.server, "block_native_data", False)
            and path.name.startswith("antfly-")
            and path.suffix == ".parquet"
        ):
            self.rfile.read(int(self.headers["Content-Length"]))
            return self.respond(503, {})
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

    @atomic_authority_operation
    def do_DELETE(self):
        path = self.object_path()
        if path.exists():
            etag = '"' + hashlib.md5(path.read_bytes()).hexdigest() + '"'
            if self.headers.get("If-Match") and self.headers["If-Match"] != etag:
                return self.respond(412, {})
            path.unlink()
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    @atomic_authority_operation
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

    @atomic_authority_operation
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/embedding/v1/embeddings":
            inputs = body["input"]
            if isinstance(inputs, str):
                inputs = [inputs]
            return self.respond(
                200,
                {
                    "object": "list",
                    "data": [
                        {"object": "embedding", "index": index, "embedding": [1.0, 0.0]}
                        for index, _ in enumerate(inputs)
                    ],
                    "model": "fixture",
                    "usage": {"prompt_tokens": 0, "total_tokens": 0},
                },
            )
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
@pytest.mark.parametrize("native_rows", [False, True, "delete_first", "overlay"])
def test_native_catalog_file_commit_read_and_restart(tmp_path, mode, native_rows):
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
        if (
            native_rows == "overlay"
            and body is not None
            and "full_text_search" in body
            and "indexes" not in body
        ):
            body = dict(body, indexes=["body_text"])
        request = Request(
            endpoint + path,
            None if body is None else json.dumps(body).encode(),
            {"Content-Type": "application/json"},
            method=method,
        )
        try:
            with urlopen(request, timeout=30) as response:
                return None if response.status == 204 else json.load(response)
        except HTTPError as error:
            payload = error.read()
            error.read = io.BytesIO(payload).read
            error.add_note(payload.decode("utf-8", errors="replace"))
            error.add_note(f"{method} {path} {body}")
            raise
        except Exception as error:
            error.add_note(f"{method} {path} {body}")
            raise

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
                    "relational_indexes": [
                        {"name": "amount_idx", "keys": [{"column": "amount"}]}
                    ]
                    if native_rows
                    else [],
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "amount": {
                                        "type": "integer",
                                        "x-antfly-field": {
                                            "type": "numeric",
                                            "sortable": True,
                                        },
                                    },
                                    "body": {"type": "string"},
                                    **(
                                        {
                                            "dense_native": {"type": "string"},
                                            "sparse_native": {"type": "string"},
                                        }
                                        if native_rows == "overlay"
                                        else {}
                                    ),
                                },
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
                },
                "indexes": (
                    {
                        "body_text": {"type": "full_text"},
                        **(
                            {
                                **{
                                    name: {
                                        "type": "embeddings",
                                        "field": "body",
                                        "dimension": 2,
                                        "embedder": {
                                            "provider": "openai",
                                            "model": "fixture",
                                            "url": origin + "/embedding/v1",
                                            "api_key": "fixture",
                                        },
                                        "chunker": {
                                            "provider": "mock",
                                            "text": {
                                                "target_tokens": 1,
                                                "overlap_tokens": 0,
                                            },
                                        },
                                    }
                                    for name in (
                                        "managed_chunks",
                                        "managed_chunks_second",
                                    )
                                },
                                "dense_native": {
                                    "type": "embeddings",
                                    "external": True,
                                    "dimension": 2,
                                },
                                "sparse_native": {
                                    "type": "embeddings",
                                    "external": True,
                                    "sparse": True,
                                },
                            }
                            if native_rows == "overlay"
                            else {}
                        ),
                    }
                    if native_rows
                    else {}
                ),
            },
        )
        catalog = NativeCatalog(state, warehouse, endpoint, "hn")
        catalog._load_file_io = lambda *args, **kwargs: LocalS3IO()
        arrow_schema = pa.schema(
            [pa.field("amount", pa.int64()), pa.field("body", pa.string())]
            + (
                [
                    pa.field("dense_native", pa.string()),
                    pa.field("sparse_native", pa.string()),
                ]
                if native_rows == "overlay"
                else []
            )
        )
        table = catalog.create_table(
            "hackernews.items",
            arrow_schema,
            location=warehouse,
            properties={"format-version": "2"},
        )
        with table.update_spec() as spec_update:
            spec_update.add_identity("amount")
        wal_offset = int(native_rows == "delete_first")
        if wal_offset:
            empty = {
                "batch_id": "delete-before-backfill",
                "source": "wire-cdc",
                "epoch": "exported-snapshot-1",
                "checkpoint": "opaque-provider-offset-0",
                "key_fields": ["amount"],
                "changes": [{"op": "delete", "row": {"amount": 999}}],
            }
            assert call("POST", "/tables/hn/lake/changes", empty)["wal_lsn"] == 1
            deadline = time.monotonic() + 180
            while True:
                loaded = call("GET", "/tables/hn/lake/catalog")
                if loaded["metadata"]["properties"].get("antfly.wal.coverage") == "1":
                    assert call(
                        "POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn"}
                    )["rows"] == [["0"]]
                    break
                assert time.monotonic() < deadline, (
                    tmp_path / "server.log"
                ).read_text()[-5000:]
                time.sleep(0.1)
            cleared = call(
                "POST",
                "/tables/hn/lake/maintenance",
                {
                    "action": "compact",
                    "operation_id": "delete-only-compaction",
                    "dry_run": False,
                },
            )
            assert cleared["committed"] and cleared["output_rows"] == 0, cleared
            assert call("POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn"})[
                "rows"
            ] == [["0"]]
            table = catalog.load_table("hackernews.items")
        table.append(
            pa.table(
                {
                    "amount": [1, 2, 3],
                    "body": ["original one", "original two", "original three"],
                    **(
                        {
                            "dense_native": ["[0,1]", "[0,1]", "[1,0]"],
                            "sparse_native": ['{"1":1}', '{"1":10}', '{"1":3}'],
                        }
                        if native_rows == "overlay"
                        else {}
                    ),
                },
                schema=arrow_schema,
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
        if native_rows == "overlay":
            deadline = time.monotonic() + 90
            found = None
            while True:
                try:
                    found = call(
                        "POST",
                        "/tables/hn/query",
                        {
                            "full_text_search": {"match": "original", "field": "body"},
                            "fields": ["amount"],
                            "limit": 10,
                        },
                    )
                    hits = (
                        found.get("responses", [found])[0]
                        .get("hits", {})
                        .get("hits", [])
                    )
                    if len(hits) == 3:
                        break
                except HTTPError as error:
                    assert error.code in (409, 422, 503), error.read().decode()
                if time.monotonic() >= deadline:
                    pytest.fail(json.dumps(found))
                time.sleep(0.1)
            call(
                "POST",
                "/tables/hn_live_native",
                {
                    "schema": {
                        "default_type": "row",
                        "document_schemas": {
                            "row": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "amount": {
                                            "type": "integer",
                                            "x-antfly-field": {
                                                "type": "numeric",
                                                "sortable": True,
                                            },
                                        },
                                        "body": {"type": "string"},
                                    },
                                }
                            }
                        },
                    },
                    "indexes": {"body_text": {"type": "full_text"}},
                },
            )
            call(
                "POST",
                "/tables/hn_live_native/batch",
                {
                    "inserts": {
                        "recent": {"amount": 100, "body": "original native current"}
                    },
                    "sync_level": "full_index",
                },
            )
            mixed_query = {
                "source": {"union": [{"table": "hn"}, {"table": "hn_live_native"}]},
                "full_text_search": {"match": "original", "field": "body"},
                "order_by": [{"field": "amount"}],
                "fields": ["amount"],
                "limit": 1,
            }
            mixed_first = call("POST", "/query", mixed_query)["responses"][0]
            assert mixed_first["hits"]["hits"][0]["_source"]["amount"] == 1, mixed_first
            mixed_next = dict(
                mixed_query, limit=10, source_cursor=mixed_first["next_source_cursor"]
            )
            current_uri = warehouse + "/current"
            call(
                "POST",
                "/tables/hn_current",
                {
                    "schema": {
                        "storage_mode": "relational",
                        "default_type": "row",
                        "relational_indexes": [
                            {"name": "amount_idx", "keys": [{"column": "amount"}]}
                        ],
                        "document_schemas": {
                            "row": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "amount": {
                                            "type": "integer",
                                            "x-antfly-field": {
                                                "type": "numeric",
                                                "sortable": True,
                                            },
                                        },
                                        "body": {"type": "string"},
                                        "deleted": {"type": "boolean"},
                                    },
                                    "additionalProperties": False,
                                }
                            }
                        },
                        "base_source": {
                            "kind": "external",
                            "format": "iceberg",
                            "uri": current_uri,
                            "credentials": {"ref": "objects", "scope": "hn"},
                            "table_id": "hn_current",
                            "write_policy": "iceberg_writer",
                            "catalog": {"type": "managed"},
                        },
                    },
                    "indexes": {"body_text": {"type": "full_text"}},
                },
            )
            current_state = State(tmp_path / "current.aflite")
            try:
                current_catalog = NativeCatalog(
                    current_state, current_uri, endpoint, "hn_current"
                )
                current_catalog._load_file_io = lambda *args, **kwargs: LocalS3IO()
                current_schema = pa.schema(
                    [
                        pa.field("amount", pa.int64()),
                        pa.field("body", pa.string()),
                        pa.field("deleted", pa.bool_()),
                    ]
                )
                current_table = current_catalog.create_table(
                    "hackernews.items",
                    current_schema,
                    location=current_uri,
                    properties={"format-version": "2"},
                )
                current_table.append(
                    pa.table(
                        {
                            "amount": [1, 2, 4],
                            "body": ["nonmatching edit", "", "original recent"],
                            "deleted": [False, True, False],
                        },
                        schema=current_schema,
                    )
                )
            finally:
                current_state.db.close()
            deadline = time.monotonic() + 90
            while True:
                try:
                    ready = call(
                        "POST",
                        "/tables/hn_current/query",
                        {"full_text_search": {"match": "recent", "field": "body"}},
                    )
                    if len(ready["responses"][0]["hits"]["hits"]) == 1:
                        break
                except HTTPError as error:
                    assert error.code in (409, 422, 503), error.read().decode()
                assert time.monotonic() < deadline
                time.sleep(0.1)
            composed = {
                "source": {
                    "overlay": {
                        "base": {"table": "hn"},
                        "changes": {"table": "hn_current"},
                        "key": ["amount"],
                    }
                },
                "source_ranking": "rrf",
                "full_text_search": {"match": "original", "field": "body"},
                "fields": ["amount"],
                "limit": 1,
            }
            combined = call("POST", "/query", composed)["responses"][0]
            assert combined["hits"]["total"]["value"] == 2, combined
            assert combined["hits"]["hits"][0]["_source"]["amount"] == 3, combined
            second = call(
                "POST",
                "/query",
                dict(composed, source_cursor=combined["next_source_cursor"]),
            )["responses"][0]
            assert second["hits"]["hits"][0]["_source"]["amount"] == 4, second
            created_source = call(
                "POST", "/sources/hackernews", {"source": composed["source"]}
            )
            source_id = created_source["source_id"]
            saved_body = dict(composed, source={"saved": "hackernews"})
            saved_first = call("POST", "/query", saved_body)["responses"][0]
            assert saved_first["hits"]["hits"][0]["_source"]["amount"] == 3
            saved_next_body = dict(
                saved_body, source_cursor=saved_first["next_source_cursor"]
            )

            ordered = call(
                "POST",
                "/query",
                {
                    "source": {"union": [{"table": "hn"}, {"table": "hn_current"}]},
                    "full_text_search": {"match": "original", "field": "body"},
                    "order_by": [{"field": "amount"}],
                    "fields": ["amount"],
                    "limit": 10,
                },
            )["responses"][0]
            assert [hit["_source"]["amount"] for hit in ordered["hits"]["hits"]] == [
                1,
                2,
                3,
                4,
            ], ordered
            authority.block_native_data = True
        if native_rows:
            batch = {
                "batch_id": "cdc-transaction-1",
                "source": "wire-cdc",
                "epoch": "exported-snapshot-1",
                "checkpoint": "opaque-provider-offset-1",
                "key_fields": ["amount"],
                "expected_checkpoint": "opaque-provider-offset-0"
                if wal_offset
                else None,
                "changes": [
                    {
                        "op": "upsert",
                        "row": {
                            "amount": 1,
                            "body": "native searchable comet",
                            **(
                                {"dense_native": "[1,0]", "sparse_native": '{"1":7}'}
                                if native_rows == "overlay"
                                else {}
                            ),
                        },
                    },
                    {"op": "delete", "row": {"amount": 2}},
                    {
                        "op": "upsert",
                        "row": {
                            "amount": 4,
                            "body": "",
                            **(
                                {"dense_native": "[0,1]", "sparse_native": '{"1":4}'}
                                if native_rows == "overlay"
                                else {}
                            ),
                        },
                    },
                ],
            }
            accepted = call("POST", "/tables/hn/lake/changes", batch)
            assert {
                key: accepted[key] for key in ("state", "wal_lsn", "searchable")
            } == {
                "state": "accepted",
                "wal_lsn": 1 + wal_offset,
                "searchable": False,
            }
            assert accepted["table_id"] > 0
            assert accepted["object_generation"] >= 0
            assert call("POST", "/tables/hn/lake/changes", batch) == accepted
            if native_rows == "overlay":
                call(
                    "POST",
                    "/tables/hn_live_native/batch",
                    {
                        "inserts": {
                            "recent": {"amount": 200, "body": "changed native current"}
                        },
                        "sync_level": "full_index",
                    },
                )
            # A caller restart immediately after acceptance must not strand WAL.
            stop()
            start()
            if native_rows == "overlay":
                before = call("GET", "/tables/hn/lake/catalog")
                assert (
                    before["metadata"]["properties"].get("antfly.wal.coverage", "0")
                    == "0"
                )
                immediate = call(
                    "POST",
                    "/tables/hn/query",
                    {
                        "full_text_search": {"match": "comet", "field": "body"},
                        "fields": ["amount", "body"],
                        "highlight": {"fields": ["body"]},
                        "limit": 10,
                    },
                )
                hits = immediate.get("responses", [immediate])[0]["hits"]["hits"]
                assert [hit["_source"]["amount"] for hit in hits] == [1], immediate
                assert hits[0].get("_highlights", {}).get("body"), immediate
                dense_request = {
                    "embeddings": {"dense_native": [1, 0]},
                    "indexes": ["dense_native"],
                    "fields": ["amount"],
                    "limit": 10,
                }
                sparse_request = {
                    "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
                    "indexes": ["sparse_native"],
                    "fields": ["amount"],
                    "limit": 10,
                }
                for vector_request in (dense_request, sparse_request):
                    vector_result = call("POST", "/tables/hn/query", vector_request)
                    vector_hits = vector_result.get("responses", [vector_result])[0][
                        "hits"
                    ]["hits"]
                    assert sorted(hit["_source"]["amount"] for hit in vector_hits) == [
                        1,
                        3,
                        4,
                    ], vector_result
                    deleted = call(
                        "POST",
                        "/tables/hn/query",
                        dict(
                            vector_request,
                            filter_query={"term": {"path": "/amount", "value": 2}},
                        ),
                    )
                    assert (
                        deleted.get("responses", [deleted])[0]["hits"]["hits"] == []
                    ), deleted
                chunk_requests = []
                for name in ("managed_chunks", "managed_chunks_second"):
                    chunk_query = {
                        "embeddings": {name: [1, 0]},
                        "indexes": [name],
                        "limit": 10,
                    }
                    chunk_requests.append(chunk_query)
                    parents = call(
                        "POST", "/tables/hn/query", dict(chunk_query, fields=["amount"])
                    )["responses"][0]
                    assert sorted(
                        hit["_source"]["amount"] for hit in parents["hits"]["hits"]
                    ) == [1, 3], parents
                    filtered_chunks = call(
                        "POST",
                        "/tables/hn/query",
                        dict(
                            chunk_query,
                            fields=["amount"],
                            filter_query={"term": {"path": "/amount", "value": 3}},
                        ),
                    )["responses"][0]
                    assert [
                        hit["_source"]["amount"]
                        for hit in filtered_chunks["hits"]["hits"]
                    ] == [3], filtered_chunks
                    members = call(
                        "POST",
                        "/tables/hn/query",
                        dict(chunk_query, hierarchy={}, fields=["text"]),
                    )["responses"][0]
                    assert len(members["hits"]["hits"]) >= 3, members
                    assert all(
                        hit["_source"]["text"] for hit in members["hits"]["hits"]
                    ), members
                    units = call(
                        "POST",
                        "/tables/hn/query",
                        dict(
                            chunk_query,
                            hierarchy={"group_by": {"level": "unit"}},
                            fields=["text"],
                        ),
                    )["responses"][0]
                    assert len(units["hits"]["hits"]) == 2, units
                    assert all(
                        hit["_source"]["text"] for hit in units["hits"]["hits"]
                    ), units
                first_members = call(
                    "POST",
                    "/tables/hn/query",
                    dict(chunk_requests[0], hierarchy={}, fields=[]),
                )["responses"][0]
                second_members = call(
                    "POST",
                    "/tables/hn/query",
                    dict(chunk_requests[1], hierarchy={}, fields=[]),
                )["responses"][0]
                assert {hit["_id"] for hit in first_members["hits"]["hits"]}.isdisjoint(
                    {hit["_id"] for hit in second_members["hits"]["hits"]}
                )
                # Reload durable recent artifacts independently of process caches.
                stop()
                start()
                dense_reopened = call("POST", "/tables/hn/query", dense_request)
                assert sorted(
                    hit["_source"]["amount"]
                    for hit in dense_reopened.get("responses", [dense_reopened])[0][
                        "hits"
                    ]["hits"]
                ) == [1, 3, 4], dense_reopened
                chunk_reopened = call(
                    "POST",
                    "/tables/hn/query",
                    dict(chunk_requests[0], hierarchy={}, fields=["text"]),
                )["responses"][0]
                assert len(chunk_reopened["hits"]["hits"]) >= 3, chunk_reopened
                assert all(
                    hit["_source"]["text"] for hit in chunk_reopened["hits"]["hits"]
                ), chunk_reopened
                mixed_restarted = call("POST", "/query", mixed_next)["responses"][0]
                assert [
                    hit["_source"]["amount"] for hit in mixed_restarted["hits"]["hits"]
                ] == [2, 3, 100], mixed_restarted
                status = call(
                    "POST",
                    "/tables/hn/lake/maintenance",
                    {"action": "enrichment_status"},
                )
                assert (
                    status["state"] == "ready"
                    and status["wal_lsn"] == accepted["wal_lsn"]
                ), status
                assert call("GET", "/sources/hackernews")["source_id"] == source_id
                resumed = call("POST", "/query", saved_next_body)["responses"][0]
                assert resumed["hits"]["hits"][0]["_source"]["amount"] == 4, resumed

                remaining = call(
                    "POST",
                    "/tables/hn/query",
                    {
                        "full_text_search": {"match": "original", "field": "body"},
                        "fields": ["amount"],
                        "limit": 10,
                    },
                )
                hits = remaining.get("responses", [remaining])[0]["hits"]["hits"]
                assert [hit["_source"]["amount"] for hit in hits] == [3], remaining
                filtered = call(
                    "POST",
                    "/tables/hn/query",
                    {
                        "full_text_search": {"match": "comet", "field": "body"},
                        "filter_query": {"term": {"path": "/amount", "value": 1}},
                        "order_by": [{"field": "_score", "desc": True}],
                        "fields": ["amount"],
                        "limit": 10,
                    },
                )
                assert (
                    len(filtered.get("responses", [filtered])[0]["hits"]["hits"]) == 1
                ), filtered
                assert call("POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn"})[
                    "rows"
                ] == [["3"]]
                assert call(
                    "POST",
                    "/sql",
                    {
                        "statement": "SELECT amount FROM hn ORDER BY amount",
                        "lake_visibility": "accepted",
                    },
                )["rows"] == [["1"], ["3"], ["4"]]
                assert call(
                    "POST",
                    "/sql",
                    {
                        "statement": "SELECT COUNT(*) FROM hn WHERE amount = 2",
                        "lake_visibility": "accepted",
                    },
                )["rows"] == [["0"]]
                assert call(
                    "POST",
                    "/sql",
                    {
                        "statement": "SELECT a.amount FROM hn a JOIN hn b ON a.amount=b.amount ORDER BY a.amount",
                        "lake_visibility": "accepted",
                    },
                )["rows"] == [["1"], ["3"], ["4"]]
                published = call(
                    "POST",
                    "/tables/hn/query",
                    {
                        "full_text_search": {"match": "original", "field": "body"},
                        "fields": ["amount"],
                        "lake_read": {"visibility": "published"},
                    },
                )
                assert len(published["responses"][0]["hits"]["hits"]) == 3
                receipt = {
                    key: accepted[key]
                    for key in ("table_id", "object_generation", "wal_lsn")
                }
                with pytest.raises(HTTPError) as lagging:
                    call(
                        "POST",
                        "/tables/hn/query",
                        {
                            "full_text_search": {"match": "original", "field": "body"},
                            "lake_read": {
                                "visibility": "published",
                                "through": receipt,
                                "wait_ms": 0,
                            },
                        },
                    )
                assert lagging.value.code == 503, lagging.value.read().decode()
                with pytest.raises(HTTPError) as wrong_incarnation:
                    call(
                        "POST",
                        "/tables/hn/query",
                        {
                            "full_text_search": {"match": "comet", "field": "body"},
                            "lake_read": {
                                "through": dict(
                                    receipt, table_id=receipt["table_id"] + 1
                                ),
                                "wait_ms": 0,
                            },
                        },
                    )
                assert wrong_incarnation.value.code == 409, (
                    wrong_incarnation.value.read().decode()
                )
                authority.block_native_data = False
            deadline = time.monotonic() + 180
            while True:
                loaded = call("GET", "/tables/hn/lake/catalog")
                if loaded["metadata"]["properties"].get("antfly.wal.coverage") == str(
                    1 + wal_offset
                ):
                    break
                assert time.monotonic() < deadline, (
                    tmp_path / "server.log"
                ).read_text()[-5000:]
                time.sleep(0.1)
            TableMetadataUtil.parse_obj(loaded["metadata"])
            # Arrow verifies actual native pages, null levels and Iceberg IDs.
            import pyarrow.parquet as pq

            native_data = sorted((root / "hn" / "data").glob("antfly-*-data.parquet"))
            assert len(native_data) == 1
            assert pq.read_table(native_data[0]).select(
                ["amount", "body"]
            ).to_pylist() == [
                {"amount": 1, "body": "native searchable comet"},
                {"amount": 4, "body": ""},
            ]
            refreshed = catalog.load_table("hackernews.items")
            assert len(list(refreshed.current_snapshot().manifests(refreshed.io))) >= 3
            # Publication is automatic: no create-index/refresh action here.
            while True:
                try:
                    found = call(
                        "POST",
                        "/tables/hn/query",
                        {
                            "full_text_search": {"match": "comet", "field": "body"},
                            "fields": ["amount", "body"],
                            "limit": 10,
                        },
                    )
                    result = found.get("responses", [found])[0]
                    hits = result.get("hits", {}).get("hits", [])
                    if hits:
                        assert len(hits) == 1, found
                        break
                except HTTPError as error:
                    assert error.code in (409, 422, 503), error.read().decode()
                assert time.monotonic() < deadline, (
                    tmp_path / "server.log"
                ).read_text()[-5000:]
                time.sleep(0.2)
            assert call(
                "POST", "/sql", {"statement": "SELECT amount FROM hn ORDER BY amount"}
            )["rows"] == [["1"], ["3"], ["4"]]
            if native_rows == "overlay":
                resumed = call("POST", "/query", saved_next_body)["responses"][0]
                assert resumed["hits"]["hits"][0]["_source"]["amount"] == 4, resumed
                fresh_source_page = call("POST", "/query", saved_body)["responses"][0]
                recreated_cursor = dict(
                    saved_body, source_cursor=fresh_source_page["next_source_cursor"]
                )
                call("DELETE", "/sources/hackernews")
                recreated = call(
                    "POST", "/sources/hackernews", {"source": composed["source"]}
                )
                assert recreated["source_id"] != source_id
                with pytest.raises(HTTPError) as source_conflict:
                    call("POST", "/query", recreated_cursor)
                assert source_conflict.value.code == 409
            changed = dict(batch, changes=[{"op": "delete", "row": {"amount": 1}}])
            with pytest.raises(HTTPError) as conflict:
                call("POST", "/tables/hn/lake/changes", changed)
            assert conflict.value.code == 409
            second = dict(
                batch,
                batch_id="cdc-transaction-2",
                expected_checkpoint=batch["checkpoint"],
                checkpoint="opaque-provider-offset-2",
                changes=[
                    {"op": "delete", "row": {"amount": 1}},
                    {"op": "upsert", "row": {"amount": 4, "body": None}},
                    {"op": "upsert", "row": {"amount": 5, "body": "next comet"}},
                ],
            )
            assert (
                call("POST", "/tables/hn/lake/changes", second)["wal_lsn"]
                == 2 + wal_offset
            )
            while True:
                loaded = call("GET", "/tables/hn/lake/catalog")
                if loaded["metadata"]["properties"].get("antfly.wal.coverage") == str(
                    2 + wal_offset
                ):
                    break
                assert time.monotonic() < deadline, (
                    tmp_path / "server.log"
                ).read_text()[-5000:]
                time.sleep(0.1)
        stop()
        start()
        assert (
            call("GET", "/tables/hn/lake/catalog")["metadata_location"]
            == loaded["metadata_location"]
        )
        assert call("POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn"})[
            "rows"
        ] == [["3"]]
        if native_rows:
            deadline = time.monotonic() + 180
            while True:
                try:
                    result = call(
                        "POST",
                        "/tables/hn/query",
                        {
                            "full_text_search": {"match": "comet", "field": "body"},
                            "fields": ["amount"],
                            "limit": 10,
                        },
                    )
                    result = result.get("responses", [result])[0]
                    hits = result.get("hits", {}).get("hits", [])
                    if hits:
                        assert [hit["_source"]["amount"] for hit in hits] == [5], result
                        break
                except HTTPError as error:
                    assert error.code in (409, 422, 503), error.read().decode()
                assert time.monotonic() < deadline, (
                    tmp_path / "server.log"
                ).read_text()[-5000:]
                time.sleep(0.1)
            assert call(
                "POST", "/sql", {"statement": "SELECT amount FROM hn ORDER BY amount"}
            )["rows"] == [["3"], ["4"], ["5"]]
        if native_rows:
            maintenance = "/tables/hn/lake/maintenance"
            planned = call(
                "POST",
                maintenance,
                {
                    "action": "compact",
                    "operation_id": "wire-compaction",
                    "dry_run": True,
                },
            )
            assert planned["input_files"] >= 2, planned
            compact_request = {
                "action": "compact",
                "operation_id": "wire-compaction",
                "dry_run": False,
                "max_rows": 1,
            }
            compacted = call("POST", maintenance, compact_request)
            assert not compacted["complete"] and not compacted["committed"], compacted
            assert compacted["scanned_rows"] == 1, compacted
            parent = call("GET", "/tables/hn/lake/catalog")["metadata_location"]
            stop()
            start()
            for _ in range(32):
                previous = compacted["scanned_rows"]
                compacted = call("POST", maintenance, compact_request)
                assert 0 <= compacted["scanned_rows"] - previous <= 1, compacted
                if compacted["complete"]:
                    break
                assert (
                    call("GET", "/tables/hn/lake/catalog")["metadata_location"]
                    == parent
                )
            else:
                pytest.fail(f"compaction failed to finish bounded turns: {compacted}")
            assert compacted["committed"], compacted
            assert compacted["complete"] and not compacted["conflicted"], compacted
            assert (
                call(
                    "POST",
                    maintenance,
                    {
                        "action": "compact",
                        "operation_id": "wire-compaction",
                        "dry_run": False,
                    },
                )
                == compacted
            )
            catalog.load_table("hackernews.items").metadata.model_dump()
            assert call(
                "POST", "/sql", {"statement": "SELECT amount FROM hn ORDER BY amount"}
            )["rows"] == [["3"], ["4"], ["5"]]
            conflict_request = dict(
                compact_request, operation_id="wire-conflicted-compaction"
            )
            pending = call("POST", maintenance, conflict_request)
            assert not pending["complete"], pending
            catalog.load_table("hackernews.items").transaction().set_properties(
                {"qualification.concurrent-writer": "preserved"}
            ).commit_transaction()
            for _ in range(32):
                pending = call("POST", maintenance, conflict_request)
                if pending["complete"]:
                    break
            assert pending["complete"] and pending["conflicted"], pending
            assert not pending["committed"], pending
            assert call("POST", maintenance, conflict_request) == pending
            assert (
                call("GET", "/tables/hn/lake/catalog")["metadata"]["properties"][
                    "qualification.concurrent-writer"
                ]
                == "preserved"
            )
            assert call(
                "POST", "/sql", {"statement": "SELECT amount FROM hn ORDER BY amount"}
            )["rows"] == [["3"], ["4"], ["5"]]
            dry_gc = call(
                "POST", maintenance, {"action": "vacuum", "operation_id": "wire-vacuum"}
            )
            assert dry_gc["expired_snapshots"] == 0, dry_gc
            with pytest.raises(HTTPError) as denied:
                call(
                    "POST",
                    maintenance,
                    {
                        "action": "vacuum",
                        "operation_id": "wire-vacuum",
                        "dry_run": False,
                    },
                )
            assert denied.value.code == 403
            gc = call(
                "POST",
                maintenance,
                {
                    "action": "wal_gc",
                    "operation_id": "wire-wal-gc",
                    "dry_run": False,
                    "max_deleted": 1,
                },
            )
            for _ in range(8):
                if gc["complete"]:
                    break
                gc = call(
                    "POST",
                    maintenance,
                    {
                        "action": "wal_gc",
                        "operation_id": "wire-wal-gc",
                        "dry_run": False,
                        "max_deleted": 1,
                    },
                )
            assert gc["complete"], gc
            assert call("POST", "/tables/hn/lake/changes", batch) == accepted
            stop()
            start()
            assert call("POST", "/sql", {"statement": "SELECT COUNT(*) FROM hn"})[
                "rows"
            ] == [["3"]]
            if native_rows == "overlay" and mode == "managed":
                # Opt-in scheduling uses durable object-store progress and
                # resumes after a process restart without a manual job call.
                table = catalog.load_table("hackernews.items")
                table.transaction().set_properties(
                    {
                        "antfly.maintenance.policy": json.dumps(
                            {
                                "enabled": True,
                                "compact": False,
                                "wal_gc": True,
                                "interval_ms": 60000,
                                "max_deleted": 1,
                            }
                        )
                    }
                ).commit_transaction()
                stop()
                start()
                deadline = time.monotonic() + 180
                while True:
                    scheduled = call("POST", maintenance, {"action": "status"})
                    progress = scheduled.get("state")
                    if progress and progress.get("last_result"):
                        assert not progress.get("last_error"), scheduled
                        assert json.loads(progress["last_result"])["complete"], (
                            scheduled
                        )
                        break
                    assert time.monotonic() < deadline, (
                        tmp_path / "server.log"
                    ).read_text()[-5000:]
                    time.sleep(0.2)
                stop()
                start()
                assert (
                    call("POST", maintenance, {"action": "status"})["state"][
                        "last_result"
                    ]
                    == progress["last_result"]
                )
    finally:
        if process and process.poll() is None:
            stop()
        log.close()
        state.db.close()
        if authority:
            authority.shutdown()
            authority.server_close()
