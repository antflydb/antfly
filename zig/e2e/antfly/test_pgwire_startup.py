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

"""Publication readiness through a real standalone process and SQL listener."""

import json
import os
from pathlib import Path

import conftest as harness
import pytest
import requests


@pytest.mark.slow
@pytest.mark.e2e_resource("antfly_process")
def test_large_catalog_startup_waits_for_publication(monkeypatch):
    psycopg = pytest.importorskip("psycopg")
    binary = harness.resolve_binary_path(
        os.environ.get("ANTFLY_BIN", str(harness.DEFAULT_ANTFLY_BIN))
    )
    assert Path(binary).is_file(), f"Antfly binary not found: {binary}"

    class SeededServer(harness.StandaloneAntflyServer):
        def _start_process(self, *, truncate_logs):
            catalog = self.root / "metadata" / "local-metadata.json"
            catalog.parent.mkdir(parents=True, exist_ok=True)
            # Seed enough unpublished relations to require multiple bounded pages.
            catalog.write_text(
                json.dumps(
                    {
                        "tables": [
                            {"table_id": i + 7, "name": f"startup_{i:05d}"}
                            for i in range(4000)
                        ]
                    }
                )
            )
            super()._start_process(truncate_logs=truncate_logs)

    wait_for_server = harness.wait_for_server

    def wait_for_large_catalog(url, **kwargs):
        return wait_for_server(url, timeout=300, **kwargs)

    monkeypatch.setattr(harness, "wait_for_server", wait_for_large_catalog)

    def first_ddl(server):
        with psycopg.connect(
            f"postgres://admin:{harness.AUTH_BOOTSTRAP_PASSWORD}"
            f"@127.0.0.1:{server.pgwire_port}/default",
            autocommit=True,
        ) as conn:
            conn.execute("CREATE TABLE ready_after_large_catalog (id TEXT PRIMARY KEY)")
            # Unlike listing, DROP INDEX uses the published relation-owner index.
            # A missing terminal table would make IF EXISTS silently succeed.
            with pytest.raises(psycopg.errors.WrongObjectType):
                conn.execute("DROP INDEX IF EXISTS public.startup_03999")

        # Walk the public catalog at the same readiness boundary. A partial
        # import can accept the unrelated CREATE while omitting later pages.
        params = {"prefix": "startup_", "limit": 1000}
        seen = {}
        with requests.Session() as session:
            session.auth = ("admin", harness.AUTH_BOOTSTRAP_PASSWORD)
            while True:
                response = session.get(
                    f"{server.api_url}/databases/default/namespaces/public/tables",
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                for table in response.json():
                    name = table["name"]
                    assert name not in seen
                    seen[name] = int(table["table_id"])
                cursor = response.headers.get("X-Antfly-Next-Cursor")
                if cursor is None:
                    break
                params["cursor"] = cursor
        assert seen == {f"startup_{i:05d}": i + 7 for i in range(4000)}

    server = SeededServer(binary, "127.0.0.1", 0, pgwire=True, on_first_ready=first_ddl)
    try:
        assert server.proc.poll() is None
    finally:
        server.stop()
