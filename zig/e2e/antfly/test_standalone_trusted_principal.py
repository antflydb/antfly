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

"""Hosted principal verification must be wired by the standalone CLI itself."""

import base64
import hmac
import json
import os
from pathlib import Path
import socket
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest

from conftest import resolve_binary_path


def _token(
    secret,
    *,
    tables=None,
    admin=False,
    issuer="cloudaf",
    expired=False,
    operations=None,
    row_filter=None,
):
    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=")

    now = int(time.time())
    payload = {
        "iss": issuer,
        "sub": "key:hosted-fixture",
        "iat": now - 120 if expired else now,
        "exp": now - 60 if expired else now + 300,
        "admin": admin,
        "tables": tables or [],
        "operations": operations
        if operations is not None
        else ([] if admin else ["read"]),
    }
    if row_filter is not None:
        payload["row_filter"] = row_filter
    message = encode({"alg": "HS256", "typ": "JWT"}) + b"." + encode(payload)
    signature = base64.urlsafe_b64encode(
        hmac.digest(secret.encode(), message, "sha256")
    ).rstrip(b"=")
    return (message + b"." + signature).decode()


def _exercise_lake_writer(call, secret, tmp_path, admin):
    table = "lake_events"
    status, result = call(
        "POST",
        "/tables/" + table,
        {
            "storage": {"engine": "object"},
            "schema": {
                "storage_mode": "relational",
                "default_type": "row",
                "document_schemas": {
                    "row": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer"},
                                "body": {"type": "string"},
                            },
                        }
                    }
                },
                "base_source": {
                    "kind": "external",
                    "format": "iceberg",
                    "uri": (tmp_path / "lake").as_uri(),
                    "table_id": "events",
                    "schema_fingerprint": "auto",
                    "write_policy": "iceberg_writer",
                    "catalog": {"type": "managed"},
                },
            },
        },
        admin,
    )
    assert status == 200, result
    status, result = call(
        "POST",
        f"/tables/{table}/lake/catalog",
        {
            "commit_id": "initialize",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [1],
                "fields": [
                    {"id": 1, "name": "id", "required": True, "type": "long"},
                    {"id": 2, "name": "body", "required": False, "type": "string"},
                ],
            },
        },
        admin,
    )
    assert status == 200, result
    changes = {
        "batch_id": "writer-upsert",
        "source": "fixture",
        "epoch": "1",
        "checkpoint": "1",
        "expected_checkpoint": None,
        "key_fields": ["id"],
        "changes": [{"op": "upsert", "row": {"id": 7, "body": "writer fixture"}}],
    }
    reader = _token(secret, tables=[table])
    writer = _token(secret, tables=[table], operations=["read", "write"])
    assert call("POST", f"/tables/{table}/lake/changes", changes, reader)[0] == 403
    assert call("POST", "/tables/hidden/lake/changes", changes, writer)[0] == 403
    for route in ("catalog", "commits", "maintenance"):
        assert call("POST", f"/tables/{table}/lake/{route}", {}, writer)[0] == 403
    # Even a table-admin principal must not bypass a row filter on CDC writes.
    for operations in (["read", "write"], ["admin"]):
        filtered = _token(
            secret,
            tables=[table],
            operations=operations,
            row_filter={table: {"term": {"path": "/id", "value": 7}}},
        )
        assert (
            call("POST", f"/tables/{table}/lake/changes", changes, filtered)[0] == 403
        )
    status, accepted = call("POST", f"/tables/{table}/lake/changes", changes, writer)
    assert status == 202, accepted
    assert accepted["state"] == "accepted" and accepted["wal_lsn"] > 0
    deleted = dict(
        changes,
        batch_id="writer-delete",
        checkpoint="2",
        expected_checkpoint="1",
        changes=[{"op": "delete", "row": {"id": 7}}],
    )
    status, result = call("POST", f"/tables/{table}/lake/changes", deleted, writer)
    assert status == 202 and result["wal_lsn"] > accepted["wal_lsn"], result
    return deleted, result["wal_lsn"]


def test_standalone_cli_loads_hosted_principal_without_local_password(tmp_path):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", "zig-out/bin/antfly"))
    if not Path(binary).is_file():
        pytest.skip("Set ANTFLY_BIN to run the standalone CLI integration")
    secret = "standalone-cloudaf-verifier-fixture-32"
    projected = tmp_path / "secrets.json"
    projected.write_text(
        json.dumps(
            {
                "secrets": [
                    {"key": "antfly.trusted_principal.secret", "value": secret},
                    {"key": "antfly.trusted_principal.issuer", "value": "cloudaf"},
                ]
            }
        )
    )
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "enable_auth": False,
                "storage": {
                    "engine": "local",
                    "local": {"base_dir": str(tmp_path / "data")},
                },
                "inference": {"api_url": "https://inference.invalid/v1"},
            }
        )
    )
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}/db/v1"
    env = dict(os.environ)
    env.pop("ANTFLY_BOOTSTRAP_ADMIN_PASSWORD", None)
    process = None
    log = (tmp_path / "standalone.log").open("w")

    def call(method, path, body=None, token=None):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Antfly-Trusted-Principal"] = token
        request = Request(
            endpoint + path,
            None if body is None else json.dumps(body).encode(),
            headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=20) as response:
                raw = response.read()
                return response.status, json.loads(raw) if raw else None
        except HTTPError as error:
            with error:
                raw = error.read()
                return error.code, raw.decode(errors="replace")

    def start():
        nonlocal process
        process = subprocess.Popen(
            [
                str(Path(binary).resolve()),
                "standalone",
                "--config",
                str(config),
                "--secret-store-path",
                str(projected),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--health",
                "false",
                "--models-dir",
                str(tmp_path / "models"),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            assert process.poll() is None, (tmp_path / "standalone.log").read_text()[
                -4000:
            ]
            try:
                call("GET", "/tables")
                return
            except (URLError, TimeoutError):
                time.sleep(0.1)
        pytest.fail("Standalone did not start")

    def stop():
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    try:
        start()
        admin = _token(secret, admin=True)
        assert call("GET", "/tables", token=admin)[0] == 200
        assert call("GET", "/tables")[0] == 401
        assert call("GET", "/tables", token=_token("incorrect", admin=True))[0] == 401
        assert (
            call("GET", "/tables", token=_token(secret, admin=True, issuer="other"))[0]
            == 401
        )
        assert (
            call("GET", "/tables", token=_token(secret, admin=True, expired=True))[0]
            == 401
        )
        for table in ("visible", "visible_two", "hidden"):
            status, result = call(
                "POST",
                "/tables/" + table,
                {"indexes": {"body_text": {"type": "full_text", "field": "body"}}},
                admin,
            )
            assert status == 200, result
        status, result = call(
            "POST",
            "/tables/visible/batch",
            {
                "inserts": {"fixture": {"body": "searchable fixture"}},
                "sync_level": "full_index",
            },
            admin,
        )
        assert status == 201, result
        status, result = call(
            "POST",
            "/sources/hosted_sample",
            {"source": {"union": [{"table": "visible"}, {"table": "visible_two"}]}},
            admin,
        )
        assert status == 201, result
        reader = _token(secret, tables=["visible", "visible_two", "hosted_sample"])
        assert call("GET", "/tables/visible", token=reader)[0] == 200
        assert call("GET", "/tables/hidden", token=reader)[0] == 403
        assert call("DELETE", "/tables/visible", token=reader)[0] == 403
        status, result = call(
            "POST",
            "/query",
            {
                "source": {"saved": "hosted_sample"},
                "source_ranking": "rrf",
                "full_text_search": {"match": "fixture", "field": "body"},
                "full_text_index": "body_text",
                "limit": 1,
            },
            reader,
        )
        assert status == 200, result
        assert len(result["responses"][0]["hits"]["hits"]) == 1, result
        deleted, delete_lsn = _exercise_lake_writer(call, secret, tmp_path, admin)
        stop()
        start()
        writer = _token(secret, tables=["lake_events"], operations=["read", "write"])
        status, replay = call(
            "POST", "/tables/lake_events/lake/changes", deleted, writer
        )
        assert status == 202 and replay["wal_lsn"] == delete_lsn, replay
        reader = _token(secret, tables=["visible", "visible_two", "hosted_sample"])
        assert call("GET", "/tables/visible", token=reader)[0] == 200
        assert call("GET", "/tables")[0] == 401
        status, result = call(
            "POST",
            "/query",
            {
                "source": {"saved": "hosted_sample"},
                "source_ranking": "rrf",
                "full_text_search": {"match": "fixture", "field": "body"},
                "full_text_index": "body_text",
                "limit": 1,
            },
            reader,
        )
        assert status == 200, result
        assert len(result["responses"][0]["hits"]["hits"]) == 1, result
    finally:
        stop()
        log.close()
