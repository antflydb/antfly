# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
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

"""Ordinary schema migration must preserve the single-file Lite handoff."""

from __future__ import annotations

import os
import shutil
import subprocess
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
