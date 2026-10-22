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

"""Offline inspection must cover the whole standalone Lite catalog (#1212)."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess

import pytest
import requests
from conftest import DEFAULT_ANTFLY_BIN, StandaloneAntflyServer, resolve_binary_path


# Server ownership is dynamic/direct, outside static fixture inference.
pytestmark = pytest.mark.e2e_resource("antfly_process")


def test_inspect_standalone_lite_tables_and_dependencies(tmp_path):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0, storage_engine="lite")
    failed = True
    try:
        for name, indexes in (
            ("first", {}),
            (
                "later",
                {"vec": {"type": "embeddings", "external": True, "dimension": 2}},
            ),
        ):
            response = requests.post(
                server.api_url + f"/tables/{name}",
                json={"indexes": indexes},
                timeout=30,
            )
            assert response.ok, response.text
            response = requests.post(
                server.api_url + f"/tables/{name}/batch",
                json={"inserts": {"row1": {"body": name}}, "sync_level": "full_index"},
                timeout=30,
            )
            assert response.ok, response.text
        server.pause()
        source = server.root / "db.aflite"
        before = hashlib.sha256(source.read_bytes()).hexdigest()
        capabilities = tmp_path / "capabilities.json"
        capabilities.write_text(
            json.dumps({"version": 1, "configuration_id": "test", "requirements": []})
        )
        result = subprocess.run(
            [
                binary,
                "backup",
                "inspect",
                "--input",
                str(source),
                "--destination-capabilities",
                str(capabilities),
            ],
            text=True,
            capture_output=True,
            timeout=60,
            check=True,
        )
        report = json.loads(result.stdout)
        assert report["completeness"]["scan_complete"], report
        assert report["inventory"]["documents"] == 2, report
        assert report["inventory"]["shards"] == 2, report
        assert any(r["kind"] == "model" for r in report["inventory"]["requirements"]), (
            report
        )
        assert report["compatibility"]["verdict"] != "compatible", report
        assert report["source"]["sha256"] == before
        assert hashlib.sha256(source.read_bytes()).hexdigest() == before
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_inspect_postgres_source_requires_exact_future_ingestion_evidence(tmp_path):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0, storage_engine="lite")
    failed = True
    try:
        # No source connection is made: the secret is deliberately unavailable.
        # Its declarative dependency must still be inventoried before reconcile.
        response = requests.post(
            server.api_url + "/tables/cdc",
            json={
                "replication_sources": [
                    {
                        "type": "postgres",
                        "dsn": "${secret:review-private-pg}",
                        "postgres_table": "public.users",
                    }
                ]
            },
            timeout=30,
        )
        assert response.ok, response.text
        server.pause()
        source = server.root / "db.aflite"
        before = hashlib.sha256(source.read_bytes()).hexdigest()
        capabilities = tmp_path / "capabilities.json"

        def inspect(requirements):
            capabilities.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "configuration_id": "test",
                        "requirements": requirements,
                    }
                )
            )
            result = subprocess.run(
                [
                    binary,
                    "backup",
                    "inspect",
                    "--input",
                    str(source),
                    "--destination-capabilities",
                    str(capabilities),
                ],
                text=True,
                capture_output=True,
                timeout=60,
                check=True,
            )
            assert "review-private-pg" not in result.stdout + result.stderr
            report = json.loads(result.stdout)
            assert report["completeness"]["scan_complete"], report
            assert report["completeness"]["dependency_analysis_complete"], report
            return report

        report = inspect([])
        assert report["compatibility"]["verdict"] == "unknown", report
        requirements = report["inventory"]["requirements"]
        assert (
            len(requirements) == 1 and requirements[0]["kind"] == "replication_source"
        ), report
        capability = {
            "requirement_sha256": requirements[0]["sha256"],
            "supported": True,
        }
        report = inspect([capability])
        assert report["compatibility"]["verdict"] == "blocked", report
        capability["future_ingestion"] = True
        report = inspect([capability])
        assert report["compatibility"]["verdict"] == "compatible", report
        assert hashlib.sha256(source.read_bytes()).hexdigest() == before
        failed = False
    finally:
        server.stop(test_failed=failed)
