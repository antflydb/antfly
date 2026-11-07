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

import base64
import hashlib
import io
import json
import os
import platform
from pathlib import Path
import re
import struct
import time
from urllib.parse import quote
import uuid

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


@pytest.mark.parametrize(
    "provider",
    ["local", pytest.param("gcs", marks=pytest.mark.objectstore_integration)],
)
def test_composed_lake_preparation_reuses_pins_and_keeps_restart_cursors(
    monkeypatch, record_property, provider
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    import conftest

    gcs_bucket = os.environ.get("ANTFLY_E2E_GCS_BUCKET")
    gcs_prefix = os.environ.get("ANTFLY_E2E_GCS_PREFIX")
    token_file = os.environ.get("ANTFLY_E2E_GCS_TOKEN_FILE")
    token = None
    if provider == "gcs":
        if not all((gcs_bucket, gcs_prefix, token_file)):
            pytest.skip(
                "GCS qualification requires an explicit bucket, qualification prefix and token file"
            )
        assert re.fullmatch(r"[a-z0-9][a-z0-9._-]+", gcs_bucket)
        assert gcs_prefix.startswith("qualification/") and ".." not in gcs_prefix
        gcs_prefix = gcs_prefix.rstrip("/") + "/" + uuid.uuid4().hex
        token = Path(token_file).read_text().strip()
        assert token and "\n" not in token
        monkeypatch.setenv("COMPOSED_GCS_TOKEN", token)
        record_property("artifact_prefix", f"gs://{gcs_bucket}/{gcs_prefix}")
    publication_blocked = False
    original = conftest._write_remote_content_e2e_config

    def configured(root, **kwargs):
        path = original(root, **kwargs)
        config = json.loads(path.read_text())
        config["deployment_mode"] = "standalone"
        config["storage"] = {"engine": "local", "local": {"base_dir": str(root)}}
        config["connections"]["warehouse-fs"] = {
            "kind": "external_io",
            "capabilities": ["lake.read"]
            + ([] if publication_blocked else ["lake.write"]),
            "external_io": {"protocol": "filesystem", "root": str(root)},
        }
        if provider == "gcs":
            config["connections"]["prepared-gcs"] = {
                "kind": "external_io",
                "capabilities": ["storage.primary"],
                "external_io": {
                    "protocol": "gcs",
                    "buckets": [gcs_bucket],
                    "prefix": gcs_prefix,
                    "credentials": {
                        "source": "bearer_token",
                        "bearer_token": "${secret:composed.gcs.token}",
                    },
                },
            }
            config["storage"]["artifacts"] = {
                "connection": "prepared-gcs",
                "bucket": gcs_bucket,
                "prefix": gcs_prefix,
            }
        path.write_text(json.dumps(config))
        path.chmod(0o600)
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
            # GCS limits repeated updates to the same mutable coordination
            # object. This is a paced correctness fixture, with no retries;
            # keep unpaced throughput qualification separate.
            if provider == "gcs":
                time.sleep(1.2)
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
                    "credentials": {"ref": "warehouse-fs"},
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
        if provider == "gcs":

            def artifact(reference, generation=None):
                assert re.fullmatch(
                    r"sha256:[a-f0-9]{64}:graph:[a-f0-9]{64}:[a-f0-9]{32}", reference
                )
                checksum = reference[7:71]
                key = f"{gcs_prefix}/graph/{reference[78:142]}/{reference[143:175]}/{checksum}"
                r = requests.get(
                    f"https://storage.googleapis.com/storage/v1/b/{gcs_bucket}/o/{quote(key, safe='')}",
                    params={
                        "alt": "media",
                        **({"generation": generation} if generation else {}),
                    },
                    headers={"Authorization": "Bearer " + token},
                    timeout=60,
                )
                assert r.ok, f"GCS reference read failed: {r.status_code}"
                assert hashlib.sha256(r.content).hexdigest() == checksum
                return r.json()

            def retained(value, prefix):
                assert value.startswith(prefix)
                rest = value[len(prefix) :]
                reference = rest[:175]
                suffix = rest[175:].rsplit(":", 1)[0]
                assert suffix.startswith(":g:"), (
                    "GCS capability must bind an exact generation"
                )
                encoded = suffix[3:]
                generation = base64.urlsafe_b64decode(
                    encoded + "=" * (-len(encoded) % 4)
                ).decode()
                return artifact(reference, generation)

            cursor = retained(first["next_source_cursor"], "source2:")
            positions = json.loads(cursor["payload"])["sources"]
            for position in positions:
                lease = retained(position["remote_snapshot"], "lake2:")
                ref = lease["state"]
                state = artifact(ref["id"], ref["generation"])
                assert state["publication"]["cloud_generations"] is not None
            record_property("cloud_generation_publications", len(positions))
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
        # Make pending overlays deterministic on every OS/UID: source reads
        # stay authorized, but the publisher cannot write the warehouse. WAL
        # admission uses the independent artifact lane and remains writable.
        publication_blocked = True
        server.restart()
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
        _, committed = call("GET", "/tables/current/lake/catalog")
        assert committed["metadata"]["properties"]["antfly.wal.coverage"] == "1"
        _, accepted_first = call("POST", "/query", query)
        assert accepted_first["next_source_cursor"].startswith("source2:")
        if provider == "gcs":
            cursor = retained(accepted_first["next_source_cursor"], "source2:")
            positions = json.loads(cursor["payload"])["sources"]
            current = next(p for p in positions if p["table"] == "current")
            ref = retained(current["remote_snapshot"], "lake2:")["state"]
            state = artifact(ref["id"], ref["generation"])
            assert state["pending"]["lsn"] == 2 and state["change_parts"]
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
        for page_one, expected in (
            (first, (set(range(100)) | {200}) - {1, 2}),
            (accepted_first, (set(range(100)) | {200, 201}) - {1, 2, 3, 50}),
        ):
            seen = {h["_source"]["id"] for h in page_one["hits"]["hits"]}
            cursor = page_one["next_source_cursor"]
            resumed_pending = page_one is accepted_first
            if resumed_pending:
                time.sleep(6)
                before = metrics()
            while cursor:
                _, page = call("POST", "/query", {**query, "source_cursor": cursor})
                if resumed_pending:
                    time.sleep(6)
                    after = metrics()
                    preparations = (
                        after["antfly_lake_query_source_calls_total"]
                        - before["antfly_lake_query_source_calls_total"]
                    )
                    leaves = (
                        after["antfly_lake_query_composed_leaf_calls_total"]
                        - before["antfly_lake_query_composed_leaf_calls_total"]
                    )
                    assert preparations == 2
                    assert leaves > preparations
                    record_property("resumed_pending_preparations", preparations)
                    record_property("resumed_pending_leaf_calls", leaves)
                    resumed_pending = False
                ids = [h["_source"]["id"] for h in page["hits"]["hits"]]
                assert not (seen & set(ids))
                seen.update(ids)
                cursor = page.get("next_source_cursor")
            assert seen == expected
        # Opt-in qualification runs after correctness assertions. "Cold" is
        # the first query after process restart, with the durable cache kept;
        # this does not flush OS/provider caches or qualify the full archive.
        rounds = int(os.environ.get("ANTFLY_E2E_COMPOSED_PROFILE_ROUNDS", "0"))
        assert 0 <= rounds <= 20
        if rounds:
            from bench_source_projection import measure
            from test_grep_indexed import logical_process_reader

            record_property(
                "binary_sha256",
                hashlib.sha256(Path(server.binary).read_bytes()).hexdigest(),
            )
            record_property("host", platform.platform())
            samples = []
            server.restart()
            read = logical_process_reader(server.proc.pid)
            for index in range(rounds + 1):
                if provider == "gcs":
                    time.sleep(1.2)
                response, usage = measure(
                    read,
                    lambda: requests.post(
                        server.api_url + "/query",
                        json=query,
                        auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                        timeout=60,
                    ),
                )
                value = response.json()
                value = value.get("responses", [value])[0]
                assert len(value["hits"]["hits"]) == 7
                samples.append({"mode": "restart" if index == 0 else "warm", **usage})
            record_property("query_samples", json.dumps(samples))
        failed = False
    finally:
        server.stop(test_failed=failed)
