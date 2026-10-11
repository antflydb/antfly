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

"""Publication readiness through a real standalone process and SQL listener."""

import json
import os
import time
from pathlib import Path

import conftest as harness
import pytest


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
            # Seed an unpublished persisted source cut. Publication needs at
            # least three passes of 64-row pages at a 250 ms control cadence:
            # 4,000 tables cannot finish within the former 30-second deadline.
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
    started = time.monotonic()
    server = SeededServer(binary, "127.0.0.1", 0, pgwire=True)
    try:
        assert time.monotonic() - started > 30
        # The first DDL after HTTP readiness must have allocation authority.
        # No retry can hide premature listener startup.
        with psycopg.connect(
            f"postgres://admin:{harness.AUTH_BOOTSTRAP_PASSWORD}"
            f"@127.0.0.1:{server.pgwire_port}/default",
            autocommit=True,
        ) as conn:
            conn.execute("CREATE TABLE ready_after_large_catalog (id TEXT PRIMARY KEY)")
        assert server.proc.poll() is None
    finally:
        server.stop()
