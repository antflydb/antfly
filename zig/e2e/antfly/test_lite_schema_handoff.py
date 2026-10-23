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

"""Ordinary schema migration must preserve the single-file Lite handoff."""

from __future__ import annotations

import os
import json
import shutil
import struct
import subprocess
import zlib
from pathlib import Path

import requests
from conftest import DEFAULT_ANTFLY_BIN, StandaloneAntflyServer, resolve_binary_path
from helpers import wait_until


class SeededLiteServer(StandaloneAntflyServer):
    def __init__(self, binary: str, seed: Path):
        self.seed = seed
        super().__init__(binary, "127.0.0.1", 0, storage_engine="lite")

    def _start_process(self, *, truncate_logs: bool) -> None:
        destination = self.root / "db.aflite"
        if not destination.exists():
            shutil.copy2(self.seed, destination)
        super()._start_process(truncate_logs=truncate_logs)


def write_binary_schema_archive(path: Path):
    """A valid released ASCH v11 schema, with no public JSON contract."""

    def u32(value):
        return struct.pack("<I", value)

    def string(value):
        return u32(len(value)) + value

    schema = (
        b"ASCH"
        + u32(11)
        + u32(1)
        + string(b"doc")
        + struct.pack("<Q", 1_000_000_000)
        + string(b"_timestamp")
        + b"\0"
        + u32(0) * 3
    )
    entries = [
        (b"\0\0__metadata__:schema", schema),
        (b"\0\0__metadata__:schema_v1", schema),
    ]
    metadata = u32(len(entries)) + b"".join(
        string(key) + string(value) for key, value in entries
    )
    header = bytearray(64)
    header[:8] = b"ANTFLYB\n"
    struct.pack_into("<IIq", header, 8, 1, 0, 0)
    struct.pack_into("<II", header, 40, 1, 1)
    struct.pack_into("<I", header, 48, zlib.crc32(header[:48]))
    blocks = [
        (2, b"{}"),
        (3, string(b"default") + u32(0) + string(b"") + string(b"")),
        (24, metadata),
        (240, struct.pack("<IQQQQ", 0, 0, 0, 0, 0)),
        (255, struct.pack("<IIQQ", 1, 1, 0, 0)),
    ]

    def block(kind, payload):
        framed = struct.pack("<BBI", kind, 0, len(payload)) + payload
        return framed + u32(zlib.crc32(framed))

    path.write_bytes(
        header + b"".join(block(kind, payload) for kind, payload in blocks)
    )


def test_binary_schema_adoption_refuses_transfer_and_allows_embedded_repair(tmp_path):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    archive = tmp_path / "typed.afb"
    seed = tmp_path / "typed.aflite"
    write_binary_schema_archive(archive)
    subprocess.run(
        [binary, "lite", "restore", str(archive), "--out", str(seed)],
        capture_output=True,
        check=True,
        timeout=60,
    )
    refused = subprocess.run(
        [
            binary,
            "standalone",
            "--storage-engine",
            "lite",
            "--storage-path",
            str(seed),
            "--data-dir",
            str(tmp_path / "refused"),
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--health",
            "false",
        ],
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert refused.returncode != 0, refused.stdout
    assert "binary-only schema" in refused.stderr, refused.stderr
    schema = {
        "version": 2,
        "default_type": "doc",
        "ttl": {"duration": "1h"},
        "document_schemas": {
            "doc": {"schema": {"type": "object", "additionalProperties": True}}
        },
    }
    public = tmp_path / "schema.json"
    public.write_text(json.dumps(schema))
    # This must still be permitted after refusal: no adoption alias or owner
    # marker may have escaped from the failed transfer.
    subprocess.run(
        [binary, "lite", "schema", "set", str(seed), "--file", str(public)],
        capture_output=True,
        check=True,
        timeout=30,
    )
    server = SeededLiteServer(binary, seed)
    failed = True
    try:
        response = requests.get(server.api_url + "/tables/default", timeout=30)
        assert response.ok, response.text
        assert response.json()["schema"] == schema
        response = requests.post(
            server.api_url + "/tables/default/batch",
            json={"inserts": {"repaired": {"body": "portable"}}, "sync_level": "write"},
            timeout=30,
        )
        assert response.ok, response.text
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_lite_schema_migration_and_document_survive_file_only_handoff(tmp_path):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    seed = tmp_path / "embedded.aflite"
    subprocess.run([binary, "lite", "init", str(seed)], capture_output=True, check=True)
    servers = []
    failed = True
    try:
        first = SeededLiteServer(binary, seed)
        servers.append(first)
        table_url = first.api_url + "/tables/default"
        response = requests.put(
            table_url + "/schema",
            json={
                "default_type": "doc",
                "document_schemas": {
                    "doc": {
                        "schema": {
                            "type": "object",
                            "properties": {"body": {"type": "string"}},
                        }
                    }
                },
            },
            timeout=30,
        )
        assert response.ok, response.text
        assert response.json()["schema"]["version"] == 1

        def current_table():
            response = requests.get(table_url, timeout=30)
            assert response.ok, response.text
            return response.json()

        completed = wait_until(
            current_table,
            timeout_s=60,
            interval_s=0.2,
            ready_when=lambda table: table is not None and not table.get("migration"),
        )
        assert completed is not None and not completed.get("migration"), completed
        response = requests.post(
            table_url + "/batch",
            json={
                "inserts": {"after-migration": {"body": "portable"}},
                "sync_level": "write",
            },
            timeout=30,
        )
        assert response.ok, response.text
        first.pause()

        # Copy only the file into a fresh server directory. No predecessor
        # identities, journals, or directory-owned root proofs are transferred.
        second = SeededLiteServer(binary, first.root / "db.aflite")
        servers.append(second)
        response = requests.get(second.api_url + "/tables/default", timeout=30)
        assert response.ok, response.text
        assert response.json()["schema"]["version"] == 1
        assert not response.json().get("migration")
        response = requests.get(
            second.api_url + "/tables/default/documents/after-migration", timeout=30
        )
        assert response.ok, response.text
        assert response.json() == {"body": "portable"}
        second.pause()

        # Serving the returned file again must retain the same property.
        third = SeededLiteServer(binary, second.root / "db.aflite")
        servers.append(third)
        response = requests.get(
            third.api_url + "/tables/default/documents/after-migration", timeout=30
        )
        assert response.ok, response.text
        assert response.json() == {"body": "portable"}
        failed = False
    finally:
        for server in reversed(servers):
            server.stop(test_failed=failed)
