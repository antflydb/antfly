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

"""Real composed readers: shared setup, deferred capabilities and restart."""

import hashlib
import io
import json
import os
import struct
import time

import pytest
import requests

from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    resolve_binary_path,
)


pytestmark = [
    pytest.mark.iceberg_integration,
    pytest.mark.fresh_antfly_process,
    pytest.mark.e2e_resource("antfly_process"),
]


def test_composed_lake_preparation_reuses_pins_and_keeps_restart_cursors(
    monkeypatch, record_property
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    import conftest

    original = conftest._write_remote_content_e2e_config

    def configured(root, **kwargs):
        path = original(root, **kwargs)
        config = json.loads(path.read_text())
        config["deployment_mode"] = "standalone"
        config["storage"] = {"engine": "local", "local": {"base_dir": str(root)}}
        path.write_text(json.dumps(config))
        return path

    monkeypatch.setattr(conftest, "_write_remote_content_e2e_config", configured)
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
        metrics=True,
    )
    failed = True
    try:
        archive = server.root / "archive"
        objects = archive / "buckets/antfly/objects"
        objects.mkdir(parents=True)
        payload = io.BytesIO()
        pq.write_table(
            pa.table(
                {
                    "id": list(range(100)),
                    "body": ["quasar"] * 100,
                    "deleted": [False] * 100,
                }
            ),
            payload,
            compression="NONE",
        )
        data = payload.getvalue()
        (objects / "sample.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(data), 0)
            + hashlib.sha256(data).hexdigest().encode()
            + data
        )

        def call(method, path, body=None, allow=()):
            r = requests.request(
                method,
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            assert r.ok or r.status_code in allow, r.text + server.debug_logs()
            value = r.json() if r.content else {}
            return r.status_code, value.get("responses", [value])[0]

        def wait_query(name, query):
            deadline = time.monotonic() + 120
            while True:
                status, value = call(
                    "POST", f"/tables/{name}/query", query, allow=(409, 503)
                )
                if status == 200:
                    return value
                assert time.monotonic() < deadline, value
                time.sleep(0.1)

        for name, source in (
            ("history", {"format": "parquet", "uri": archive.as_uri()}),
            (
                "current",
                {
                    "format": "iceberg",
                    "uri": (server.root / "current").as_uri(),
                    "write_policy": "iceberg_writer",
                    "catalog": {"type": "managed"},
                },
            ),
        ):
            call(
                "POST",
                "/tables/" + name,
                {
                    "storage": {"engine": "object"},
                    "schema": {
                        "storage_mode": "relational",
                        "default_type": "row",
                        "enforce_types": True,
                        "relational_indexes": [
                            {"name": name + "_id", "keys": [{"column": "id"}]}
                        ],
                        "base_source": {
                            "kind": "external",
                            "table_id": name,
                            "object_mutability": "immutable",
                            "schema_fingerprint": "auto",
                            **source,
                        },
                        "document_schemas": {
                            "row": {
                                "schema": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "properties": {
                                        "id": {
                                            "type": "integer",
                                            "x-antfly-field": {
                                                "type": "number",
                                                "sortable": True,
                                            },
                                        },
                                        "body": {
                                            "type": "string",
                                            "x-antfly-field": {"type": "text"},
                                        },
                                        "deleted": {"type": "boolean"},
                                    },
                                }
                            }
                        },
                    },
                    "indexes": {
                        "body_text": {"type": "full_text", "store_source": True}
                    },
                },
            )
        call(
            "POST",
            "/tables/current/lake/catalog",
            {
                "commit_id": "initialize",
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": False, "type": "long"},
                        {"id": 2, "name": "body", "required": False, "type": "string"},
                        {
                            "id": 3,
                            "name": "deleted",
                            "required": False,
                            "type": "boolean",
                        },
                    ],
                },
            },
        )
        rows = [
            {"id": i, "body": "different" if i == 1 else "quasar", "deleted": i == 2}
            for i in range(30)
        ] + [{"id": 200, "body": "quasar", "deleted": False}]
        _, receipt = call(
            "POST",
            "/tables/current/lake/changes",
            {
                "batch_id": "seed",
                "source": "e2e",
                "epoch": "one",
                "checkpoint": "1",
                "key_fields": ["id"],
                "changes": [{"op": "upsert", "row": row} for row in rows],
            },
        )
        text = {
            "full_text_search": {"match": "quasar", "field": "body"},
            "full_text_index": "body_text",
        }
        wait_query("history", {**text, "limit": 1})
        wait_query(
            "current",
            {
                **text,
                "limit": 1,
                "read_requirements": {
                    "visibility": "published",
                    "after_write": receipt["read_receipt"],
                    "wait_ms": 1000,
                },
            },
        )
        query = {
            **text,
            "source": {
                "overlay": {
                    "base": {"table": "history"},
                    "changes": {"table": "current"},
                    "key": ["id"],
                }
            },
            "source_ranking": "rrf",
            "limit": 7,
        }

        def metrics():
            r = requests.get(server.health_url + "/metrics", timeout=10)
            assert r.ok
            return {
                k: float(v)
                for line in r.text.splitlines()
                if line.startswith("antfly_lake_query_")
                for k, v in [line.split()]
            }

        # The health endpoint exposes an asynchronous five-second cache.
        time.sleep(6)
        before = metrics()
        started = time.monotonic()
        _, first = call("POST", "/query", query)
        record_property("first_page_ms", (time.monotonic() - started) * 1000)
        assert len(first["hits"]["hits"]) == 7
        assert "request-lake:" not in json.dumps(first)
        assert first["next_source_cursor"].startswith("source2:")
        time.sleep(6)
        after = metrics()
        assert (
            after["antfly_lake_query_source_calls_total"]
            - before["antfly_lake_query_source_calls_total"]
            == 2
        )
        assert (
            after["antfly_lake_query_publication_calls_total"]
            - before["antfly_lake_query_publication_calls_total"]
            == 2
        )
        assert (
            after["antfly_lake_query_composed_leaf_calls_total"]
            - before["antfly_lake_query_composed_leaf_calls_total"]
            > 2
        )
        record_property(
            "preparation_calls",
            after["antfly_lake_query_source_calls_total"]
            - before["antfly_lake_query_source_calls_total"],
        )
        record_property(
            "leaf_calls",
            after["antfly_lake_query_composed_leaf_calls_total"]
            - before["antfly_lake_query_composed_leaf_calls_total"],
        )
        # A later ACK must affect fresh requests without changing the first
        # page's retained cut, even if publication advances before restart.
        call(
            "POST",
            "/tables/current/lake/changes",
            {
                "batch_id": "later",
                "source": "e2e",
                "epoch": "one",
                "expected_checkpoint": "1",
                "checkpoint": "2",
                "key_fields": ["id"],
                "changes": [
                    {"op": "upsert", "row": row}
                    for row in (
                        {"id": 3, "body": "quasar", "deleted": True},
                        {"id": 50, "body": "different", "deleted": False},
                        {"id": 201, "body": "quasar", "deleted": False},
                    )
                ],
            },
        )
        _, counted = call("POST", "/query", {**query, "count": True})
        assert counted["hits"]["total"]["value"] == 98
        assert not counted.get("next_source_cursor")
        _, complete = call("POST", "/query", {**query, "limit": 128})
        assert len(complete["hits"]["hits"]) == 98
        assert {h["_source"]["id"] for h in complete["hits"]["hits"]} == (
            set(range(100)) | {200, 201}
        ) - {1, 2, 3, 50}
        assert not complete.get("next_source_cursor")
        assert "request-lake:" not in json.dumps(complete)
        server.restart()
        seen = {h["_source"]["id"] for h in first["hits"]["hits"]}
        cursor = first["next_source_cursor"]
        while cursor:
            _, page = call("POST", "/query", {**query, "source_cursor": cursor})
            ids = [h["_source"]["id"] for h in page["hits"]["hits"]]
            assert not (seen & set(ids))
            seen.update(ids)
            cursor = page.get("next_source_cursor")
        assert seen == (set(range(100)) | {200}) - {1, 2}
        failed = False
    finally:
        server.stop(test_failed=failed)
