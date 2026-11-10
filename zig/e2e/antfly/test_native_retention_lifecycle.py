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

"""Idle expiry, durable admission, and local-only warming across real restarts."""

from __future__ import annotations

import json
import os
import shutil
import time
import uuid

import pytest
import requests
from conftest import DEFAULT_ANTFLY_BIN, StandaloneAntflyServer, resolve_binary_path

pytestmark = pytest.mark.e2e_resource("antfly_process")


def _wait(predicate, server, timeout=75):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        assert server.proc is not None and server.proc.poll() is None, (
            server.debug_logs()
        )
        result = predicate()
        if result:
            return result
        time.sleep(0.2)
    pytest.fail(f"retention maintenance did not finish: {server.debug_logs()}")


def test_native_retention_lifecycle(tmp_path):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    config = tmp_path / "retention.json"

    def configure(warming=False, capacity=1):
        config.write_text(
            json.dumps(
                {
                    "lake_indexes": {
                        "query_cursors": {
                            "background_warming": warming,
                            "max_durable_native_cuts": capacity,
                            "max_native_cuts": 1 if warming else 64,
                        }
                    }
                }
            )
        )

    configure()
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0, config_path=config)
    failed = True

    def post(path, body, status=None):
        response = requests.post(server.api_url + path, json=body, timeout=60)
        assert response.ok if status is None else response.status_code == status, (
            response.text
        )
        return response.json()

    ordered = {"query": {"match_all": {}}, "limit": 1, "order_by": [{"field": "rank"}]}

    def query(body):
        result = post("/tables/retention/query", body)["responses"][0]
        assert "error" not in result, result
        return result

    def durable_manifests():
        return [
            p
            for p in server.root.rglob("query-cut.json")
            if "native-query-links" in p.parts
        ]

    try:
        response = requests.post(
            server.api_url + "/tables/retention",
            json={
                "num_shards": 1,
                "schema": {
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "rank": {
                                        "type": "number",
                                        "x-antfly-field": {
                                            "type": "number",
                                            "sortable": True,
                                        },
                                    },
                                },
                            }
                        }
                    },
                },
            },
            timeout=60,
        )
        assert response.ok, response.text
        post(
            "/tables/retention/batch",
            {
                "sync_level": "full_index",
                "inserts": {
                    key: {"rank": rank} for rank, key in enumerate(["a", "b", "c"], 1)
                },
            },
        )
        first = query(ordered)
        assert first["hits"]["hits"][0]["_id"] == "a"
        cursor = first["remote_snapshot"]
        continuation = {
            **ordered,
            "limit": 10,
            "remote_snapshot": cursor,
            "search_after": first["hits"]["hits"][0]["_sort"],
        }
        post(
            "/tables/retention/batch",
            {
                "sync_level": "full_index",
                "inserts": {
                    "b": {"rank": 20},
                    "d": {"rank": 0},
                },
            },
        )
        rejection = post("/tables/retention/query", ordered, status=422)
        assert rejection["error"] == "query_candidate_budget_exceeded"
        for restart in (False, True):
            if restart:
                server.restart()
            assert [h["_id"] for h in query(continuation)["hits"]["hits"]] == ["b", "c"]

        # Seed a complete, inode-consistent expired private generation while
        # stopped. No retained query is admitted while waiting for its removal.
        server.pause()
        source = next(
            p
            for p in server.root.rglob("query-cut.json")
            if ".query-pins" in p.parent.parent.name
        )
        expired_id = uuid.uuid4().hex + uuid.uuid4().hex
        expired = source.parent.parent / expired_id
        shutil.copytree(source.parent, expired)
        manifest = json.loads((expired / "query-cut.json").read_text())
        manifest["request"] = {
            "id": expired_id,
            "table_id": manifest["namespace"]["table_id"],
            "expires_ms": int(time.time() * 1000) - 60_000,
            "create": True,
        }
        for item in manifest["files"]:
            stat = (expired / item["path"]).stat()
            item.update(inode=stat.st_ino, size=stat.st_size, mtime_ns=stat.st_mtime_ns)
        (expired / "query-cut.json").write_text(json.dumps(manifest))
        server.resume()
        _wait(lambda: not expired.exists(), server)
        assert not list(server.root.rglob(".warming.json"))

        server.pause()
        configure(warming=True, capacity=3)
        server.resume()

        def completed_warm():
            for path in server.root.rglob(".warming.json"):
                state = json.loads(path.read_text())
                if state.get("local_only") and state.get("complete"):
                    return path, state
            return None

        state_path, state = _wait(completed_warm, server)
        assert state["bytes"] > 0
        before = durable_manifests()
        assert len(before) == 1  # warming must not publish another generation
        warm = json.loads(
            (state_path.parent / state["request"]["id"] / "query-cut.json").read_text()
        )
        promoted = query(ordered)
        assert promoted["hits"]["hits"][0]["_id"] == "d"
        created = [p for p in durable_manifests() if p not in before]
        assert len(created) == 1
        binding = json.loads(created[0].read_text())
        assert binding["files"] == warm["files"]
        assert binding["request"]["expires_ms"] > warm["request"]["expires_ms"]
        # Promotion can evict the old local cache at a one-slot limit, but
        # the already-issued cursor must still reopen its durable generation.
        assert [h["_id"] for h in query(continuation)["hits"]["hits"]] == ["b", "c"]
        failed = False
    finally:
        server.stop(test_failed=failed)
