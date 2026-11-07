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
    [
        "local",
        pytest.param("gcs", marks=pytest.mark.objectstore_integration),
        pytest.param("gcs_remote", marks=pytest.mark.objectstore_integration),
    ],
)
@pytest.mark.parametrize("index_selector", ["explicit", "inferred"])
@pytest.mark.parametrize("owned", [False, True])
def test_composed_lake_preparation_reuses_pins_and_keeps_restart_cursors(
    monkeypatch, record_property, provider, index_selector, owned
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    import conftest

    archive_rows = int(os.environ.get("ANTFLY_E2E_COMPOSED_ARCHIVE_ROWS", "100"))
    assert 100 <= archive_rows <= 1_000_000
    extra_id = archive_rows + 100
    later_id = archive_rows + 101
    body_bytes = int(os.environ.get("ANTFLY_E2E_COMPOSED_BODY_BYTES", "6"))
    assert 6 <= body_bytes <= 4096
    record_property("archive_rows", archive_rows)
    record_property("body_bytes", body_bytes)
    switches = {
        name: os.environ.get("ANTFLY_E2E_COMPOSED_" + name, default)
        for name, default in (("HIGHLIGHTS", "0"), ("STORE_SOURCE", "1"))
    }
    assert all(value in {"0", "1"} for value in switches.values())
    highlights = switches["HIGHLIGHTS"] == "1"
    store_source = switches["STORE_SOURCE"] == "1"
    record_property("highlights", highlights)
    record_property("store_source", store_source)

    gcs_bucket = os.environ.get("ANTFLY_E2E_GCS_BUCKET")
    gcs_prefix = os.environ.get("ANTFLY_E2E_GCS_PREFIX")
    token_file = os.environ.get("ANTFLY_E2E_GCS_TOKEN_FILE")
    token = None
    is_gcs = provider.startswith("gcs")
    remote_source = provider == "gcs_remote"
    if is_gcs:
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
    cursor_retention_ms = 300_000
    original = conftest._write_remote_content_e2e_config

    def configured(root, **kwargs):
        path = original(root, **kwargs)
        config = json.loads(path.read_text())
        config["deployment_mode"] = "standalone"
        config.setdefault("lake_indexes", {})["query_cursors"] = {
            "retention_ms": cursor_retention_ms
        }
        if owned:
            config.setdefault("lake_indexes", {})["serving_cuts"] = {"enabled": True}
        config["lake_cache"] = {
            "root": str(root / "serving-cache"),
            "max_memory_bytes": 64 * 1024**2,
            "max_disk_bytes": 1024**3,
        }
        config["storage"] = {"engine": "local", "local": {"base_dir": str(root)}}
        config["connections"]["warehouse-fs"] = {
            "kind": "external_io",
            "capabilities": ["lake.read"]
            + ([] if publication_blocked else ["lake.write"]),
            "external_io": {"protocol": "filesystem", "root": str(root)},
        }
        if is_gcs:
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
        if remote_source:
            config["connections"]["warehouse-gcs"] = {
                "kind": "external_io",
                "capabilities": ["lake.read"]
                + ([] if publication_blocked else ["lake.write"]),
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
    last_call_ms = 0.0
    try:
        archive = server.root / "archive"
        objects = archive / "buckets/antfly/objects"
        objects.mkdir(parents=True)
        bodies = [
            ("quasar " + "archive " * ((body_bytes - 6) // 8)).strip()
        ] * archive_rows
        corpus_dir = os.environ.get("ANTFLY_E2E_COMPOSED_CORPUS_DIR")
        if corpus_dir:
            files = sorted(Path(corpus_dir).glob("*.parquet"))
            assert files, (
                "corpus directory must contain Parquet files with a body column"
            )
            bodies = [
                body or ""
                for path in files
                for body in pq.read_table(path, columns=["body"])
                .column("body")
                .to_pylist()
            ]
            assert len(bodies) == archive_rows
            # Keep deterministic correctness assertions on the first 100 rows;
            # profiling can search an independent term in the real comments.
            bodies[:100] = ["quasar " + body for body in bodies[:100]]
            record_property(
                "corpus_files_sha256",
                json.dumps(
                    {
                        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                        for path in files
                    }
                ),
            )
        profile_term = os.environ.get("ANTFLY_E2E_COMPOSED_PROFILE_TERM", "quasar")
        assert 1 <= len(profile_term) <= 64
        record_property("profile_term", profile_term)
        payload = io.BytesIO()
        pq.write_table(
            pa.table(
                {
                    "id": list(range(archive_rows)),
                    "body": bodies,
                    "deleted": [False] * archive_rows,
                }
            ),
            payload,
            compression="NONE",
            row_group_size=1000,
        )
        data = payload.getvalue()
        (objects / "sample.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(data), 0)
            + hashlib.sha256(data).hexdigest().encode()
            + data
        )

        archive_uri = archive.as_uri()
        current_uri = (server.root / "current").as_uri()
        warehouse_connection = "warehouse-fs"
        if remote_source:

            def put_remote(name, body):
                r = requests.post(
                    f"https://storage.googleapis.com/upload/storage/v1/b/{gcs_bucket}/o",
                    params={
                        "uploadType": "media",
                        "name": f"{gcs_prefix}/source/archive/{name}",
                        "ifGenerationMatch": "0",
                    },
                    data=body,
                    headers={
                        "Authorization": "Bearer " + token,
                        "Content-Type": "application/octet-stream",
                    },
                    timeout=60,
                )
                assert r.ok, f"GCS fixture upload failed: {r.status_code}"
                return r.json()

            version = put_remote("sample.parquet", data)["generation"]
            archive_uri = f"gs://{gcs_bucket}/{gcs_prefix}/source/archive"
            current_uri = f"gs://{gcs_bucket}/{gcs_prefix}/source/current"
            warehouse_connection = "warehouse-gcs"

        if owned:

            def write_object(name, body):
                (objects / name).write_bytes(
                    b"AFOBJ001"
                    + struct.pack("<QI", len(body), 0)
                    + hashlib.sha256(body).hexdigest().encode()
                    + body
                )

            if remote_source:
                write_object = put_remote
            chunk = json.dumps(
                [
                    {
                        "file_id": "sample.parquet",
                        "object_uri": f"gs://{gcs_bucket}/{gcs_prefix}/source/archive/sample.parquet"
                        if remote_source
                        else "object://antfly/sample.parquet",
                        "version_id": str(version) if remote_source else "",
                        "etag": ""
                        if remote_source
                        else hashlib.sha256(data).hexdigest(),
                        "byte_len": len(data),
                        "row_count": archive_rows,
                    }
                ]
            ).encode()
            write_object("files.json", chunk)
            head = json.dumps(
                {
                    "version": 1,
                    "source_id": "history",
                    "chunks": [
                        {
                            "uri": f"gs://{gcs_bucket}/{gcs_prefix}/source/archive/files.json"
                            if remote_source
                            else "object://antfly/files.json",
                            "sha256": hashlib.sha256(chunk).hexdigest(),
                            "byte_len": len(chunk),
                            "file_count": 1,
                        }
                    ],
                    "file_count": 1,
                }
            ).encode()
            write_object("sealed.parquet-manifest.json", head)
            archive_uri = (
                f"gs://{gcs_bucket}/{gcs_prefix}/source/archive/sealed.parquet-manifest.json"
                if remote_source
                else (archive / "sealed.parquet-manifest.json").as_uri()
            )

        def assert_highlights(value, term="quasar"):
            if not highlights:
                return
            for hit in value.get("hits", {}).get("hits", []):
                assert any(
                    fragment["text"]
                    .encode("utf-8")[span["start"] : span["end"]]
                    .decode("utf-8")
                    .lower()
                    == term.lower()
                    for fragment in hit["_highlights"]["body"]
                    for span in fragment["spans"]
                )

        def call(method, path, body=None, allow=()):
            nonlocal last_call_ms
            # GCS limits repeated updates to the same mutable coordination
            # object. This is a paced correctness fixture, with no retries;
            # keep unpaced throughput qualification separate.
            if is_gcs and not (owned and method == "POST" and path == "/query"):
                time.sleep(1.2)
            request_started = time.monotonic()
            r = requests.request(
                method,
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            last_call_ms = (time.monotonic() - request_started) * 1000
            assert r.ok or r.status_code in allow, r.text + server.debug_logs()
            try:
                value = r.json() if r.content else {}
            except requests.exceptions.JSONDecodeError:
                assert not r.ok and r.status_code in allow, r.text
                value = {"error": r.text}
            value = value.get("responses", [value])[0]
            if r.status_code == 200 and body and body.get("highlight"):
                assert_highlights(value)
            return r.status_code, value

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
            (
                "history",
                {
                    "format": "parquet",
                    "uri": archive_uri,
                    **(
                        {"credentials": {"ref": warehouse_connection}}
                        if remote_source
                        else {}
                    ),
                },
            ),
            (
                "current",
                {
                    "format": "iceberg",
                    "uri": current_uri,
                    "write_policy": "iceberg_writer",
                    "credentials": {"ref": warehouse_connection},
                    "catalog": {"type": "managed"},
                },
            ),
        ):
            table_request = {
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
                    "body_text": {"type": "full_text", "store_source": store_source}
                },
            }
            if owned and provider == "local" and name == "history":
                # An inventory without any immutable object identity must be
                # rejected before publishing the table, with a client error.
                invalid_files = json.loads(chunk)
                invalid_files[0]["etag"] = ""
                invalid_chunk = json.dumps(invalid_files).encode()
                invalid_head = json.loads(head)
                invalid_head["chunks"][0].update(
                    sha256=hashlib.sha256(invalid_chunk).hexdigest(),
                    byte_len=len(invalid_chunk),
                )
                try:
                    write_object("files.json", invalid_chunk)
                    write_object(
                        "sealed.parquet-manifest.json",
                        json.dumps(invalid_head).encode(),
                    )
                    status, _ = call(
                        "POST", "/tables/history", table_request, allow=(400,)
                    )
                    assert status == 400
                    status, _ = call("GET", "/tables/history", allow=(404,))
                    assert status == 404
                finally:
                    write_object("files.json", chunk)
                    write_object("sealed.parquet-manifest.json", head)
            call("POST", "/tables/" + name, table_request)
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
        ] + [{"id": extra_id, "body": "quasar", "deleted": False}]
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
        text = {"full_text_search": {"match": "quasar", "field": "body"}}
        if highlights:
            text["highlight"] = {"fields": ["body"]}
        if index_selector == "explicit":
            text["full_text_index"] = "body_text"
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

        if archive_rows > 100:
            query["filter_query"] = {
                "disjuncts": [
                    {"numeric_range": {"field": "id", "min": 0, "max": 99}},
                    {"term": {"path": "/id", "value": extra_id}},
                    {"term": {"path": "/id", "value": later_id}},
                ]
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
        _, first = call("POST", "/query", query)
        record_property("first_page_ms", last_call_ms)
        assert len(first["hits"]["hits"]) == 7
        assert "request-lake:" not in json.dumps(first)
        assert first["next_source_cursor"].startswith(
            "source3:" if owned else "source2:"
        )
        if is_gcs and not owned:

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
        cursor_publications = (
            after["antfly_lake_query_cursor_publications_total"]
            - before["antfly_lake_query_cursor_publications_total"]
        )
        assert cursor_publications == (0 if owned else 3)
        record_property("query_cursor_publications", cursor_publications)
        if owned:
            # The state is already durable: a forged inline continuation must
            # fail authentication before any retained source can be selected.
            record_property("fresh_cursor_bytes", len(first["next_source_cursor"]))
            original_cursor = first["next_source_cursor"]
            forged = original_cursor[:-1] + ("0" if original_cursor[-1] != "0" else "1")
            status, _ = call(
                "POST", "/query", {**query, "source_cursor": forged}, allow=(400, 409)
            )
            assert status in (400, 409)
            # Cancellation after admission must release the whole request scope;
            # the next ordinary request still resolves the same live owner cut.
            call("POST", "/query", {**query, "timeout_ms": 1}, allow=(408, 504))
            _, healthy = call("POST", "/query", query)
            assert len(healthy["hits"]["hits"]) == 7
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
                        {"id": later_id, "body": "quasar", "deleted": False},
                    )
                ],
            },
        )
        _, committed = call("GET", "/tables/current/lake/catalog")
        assert committed["metadata"]["properties"]["antfly.wal.coverage"] == "1"
        _, accepted_first = call("POST", "/query", query)
        assert accepted_first["next_source_cursor"].startswith(
            "source3:" if owned else "source2:"
        )
        if is_gcs and not owned:
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
            set(range(100)) | {extra_id, later_id}
        ) - {1, 2, 3, 50}
        assert not complete.get("next_source_cursor")
        assert "request-lake:" not in json.dumps(complete)
        server.restart()
        for page_one, expected in (
            (first, (set(range(100)) | {extra_id}) - {1, 2}),
            (accepted_first, (set(range(100)) | {extra_id, later_id}) - {1, 2, 3, 50}),
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
        # A longer configured TTL must refresh an owned cut immediately,
        # rather than leave fresh queries unavailable until its old refresh time.
        # Both modes use the same one-hour TTL for the measurements below.
        cursor_retention_ms = 3_600_000
        server.restart()
        wait_query("history", {**text, "limit": 1})
        wait_query("current", {**text, "limit": 1})
        _, longer = call("POST", "/query", query)
        assert len(longer["hits"]["hits"]) == 7
        if owned:
            encoded = longer["next_source_cursor"].split(":")[4]
            envelope = json.loads(
                base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
            )
            assert (
                3_500_000
                < envelope["expires_ms"] - envelope["issued_ms"]
                <= cursor_retention_ms
            )
        # Increasing the new-cursor horizon must preserve already-issued cuts.
        _, old_page = call(
            "POST", "/query", {**query, "source_cursor": first["next_source_cursor"]}
        )
        old_ids = {h["_source"]["id"] for h in old_page["hits"]["hits"]}
        assert old_ids and old_ids <= (set(range(100)) | {extra_id}) - {1, 2}
        assert not old_ids & {h["_source"]["id"] for h in first["hits"]["hits"]}
        record_property("cursor_ttl_refresh", True)
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
            record_property("host_load_average", json.dumps(os.getloadavg()))
            import subprocess

            commands = subprocess.check_output(["ps", "-axo", "comm,args"], text=True)
            record_property(
                "concurrent_zig_compilers",
                sum(
                    bool(
                        re.search(
                            r"\bzig(?:-\S+)?\s+(?:build-lib|build-exe|test)\b", line
                        )
                    )
                    for line in commands.splitlines()
                ),
            )
            samples = []
            profile_query = {k: v for k, v in query.items() if k != "filter_query"}
            profile_query["full_text_search"] = {"match": profile_term, "field": "body"}
            server.restart()
            read = logical_process_reader(server.proc.pid)
            for index in range(rounds + 1):
                if is_gcs and not owned:
                    time.sleep(1.2)
                response, usage = measure(
                    read,
                    lambda: requests.post(
                        server.api_url + "/query",
                        json=profile_query,
                        auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                        timeout=60,
                    ),
                )
                value = response.json()
                value = value.get("responses", [value])[0]
                assert_highlights(value, profile_term)
                assert len(value["hits"]["hits"]) == 7
                samples.append({"mode": "restart" if index == 0 else "warm", **usage})
            record_property("query_samples", json.dumps(samples))
            if owned:
                from concurrent.futures import ThreadPoolExecutor

                def simultaneous(_):
                    started = time.monotonic()
                    response = requests.post(
                        server.api_url + "/query",
                        json=profile_query,
                        auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                        timeout=60,
                    )
                    assert response.ok, (
                        f"Concurrent query failed: {response.status_code}"
                    )
                    result = response.json()["responses"][0]
                    assert_highlights(result, profile_term)
                    assert len(result["hits"]["hits"]) == 7
                    assert result["next_source_cursor"].startswith("source3:")
                    return (time.monotonic() - started) * 1000

                with ThreadPoolExecutor(max_workers=4) as pool:
                    record_property(
                        "concurrent_query_ms",
                        json.dumps(list(pool.map(simultaneous, range(12)))),
                    )
            # Reset only this fixture's verified serving cache, with the
            # process stopped. Preserve native metadata/WAL and source objects.
            import shutil

            server.pause()
            cache_root = (server.root / "serving-cache").resolve()
            assert cache_root.parent == server.root.resolve()
            shutil.rmtree(cache_root, ignore_errors=True)
            cold_started = time.monotonic()
            server.resume()
            response, usage = measure(
                logical_process_reader(server.proc.pid),
                lambda: requests.post(
                    server.api_url + "/query",
                    json=profile_query,
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    timeout=60,
                ),
            )
            assert (
                response.ok
                and len(response.json()["responses"][0]["hits"]["hits"]) == 7
            )
            assert_highlights(response.json()["responses"][0], profile_term)
            record_property("cold_cache_query", json.dumps(usage))
            record_property(
                "cold_cache_startup_and_query_ms",
                (time.monotonic() - cold_started) * 1000,
            )
        failed = False
    finally:
        server.stop(test_failed=failed)
