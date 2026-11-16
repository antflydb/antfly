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

"""Privacy text routing, native batching, enrollment and durable failure repair."""

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Lock
import time
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import pytest
from helpers import start_http_server, wait_until
from requests.exceptions import ReadTimeout


@pytest.fixture
def privacy_provider(request):
    native = request.param if hasattr(request, "param") else False
    backfill = native == "backfill"
    calls = {"embed": [], "extract": [], "paths": [], "batches": [], "embed_models": []}
    records = []
    records_lock = Lock()

    def snapshot():
        with records_lock:
            return [dict(row) for row in records]

    calls["snapshot"] = snapshot
    behavior = {"deny": False, "embed_gate": Event(), "embed_held": Event()}
    behavior["embed_gate"].set()
    behavior["model_gate"] = Event()
    behavior["model_gate"].set()
    behavior["model_held"] = Event()
    behavior["hold_model"] = None

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, value, headers=()):
            raw = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            route = urlparse(self.path)
            if (
                backfill
                and route.path == "/ai/v1/models"
                and parse_qs(route.query).get("task") == ["embed"]
            ):
                return self.reply(
                    200,
                    {
                        "embedders": {
                            model: {
                                "inputs": ["text"],
                                "model_revision": revision,
                                "model_revision_pinning": True,
                                "inference_capabilities": {
                                    "task": "embed",
                                    "batch": {
                                        "mode": "native",
                                        "preferred_items": 32,
                                        "max_items": 32,
                                        "max_encoded_bytes": 0,
                                        "max_decoded_pixels": 0,
                                        "max_media_parts_per_item": 0,
                                        "per_item_failures": False,
                                    },
                                },
                            }
                            for model, revision in (
                                ("fixture", "11" * 32),
                                ("fixture-v2", "22" * 32),
                            )
                        }
                    },
                )
            if (
                native
                and route.path == "/ai/v1/models"
                and parse_qs(route.query).get("task") == ["extract"]
            ):
                return self.reply(
                    200,
                    {
                        "extractors": {
                            "fixture": {
                                "inputs": ["text"],
                                "inference_capabilities": {
                                    "task": "extract",
                                    "batch": {
                                        "mode": "native",
                                        "preferred_items": 8,
                                        "max_items": 8,
                                        "max_encoded_bytes": 0,
                                        "max_decoded_pixels": 0,
                                        "max_media_parts_per_item": 0,
                                        "per_item_failures": False,
                                    },
                                },
                            }
                        }
                    },
                )
            self.reply(404, {"error": "legacy native provider"})

        def do_POST(self):
            calls["paths"].append(self.path)
            path = self.path.removeprefix("/ai/v1")
            if path not in ("/embed", "/extract"):
                return self.reply(404, {})
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if path == "/embed":
                inputs = body["input"]
                if isinstance(inputs, str):
                    inputs = [inputs]
                row = {
                    "task": "embed",
                    "model": body["model"],
                    "inputs": tuple(inputs),
                    "started": time.monotonic(),
                    "completed": None,
                    "status": None,
                }
                with records_lock:
                    records.append(row)
                calls["embed"].extend(inputs)
                calls["embed_models"].append(body["model"])
                if not behavior["embed_gate"].is_set():
                    behavior["embed_held"].set()
                assert behavior["embed_gate"].wait(timeout=60)
                # Hold only authored document work, after model admission probes.
                document_work = any("Orchard schedule." in text for text in inputs)
                with records_lock:
                    completed_work = sum(
                        r["model"] == body["model"]
                        and r["status"] == 200
                        and any("Orchard schedule." in text for text in r["inputs"])
                        for r in records
                    )
                if (
                    body["model"] == behavior["hold_model"]
                    and document_work
                    and completed_work >= behavior.get("hold_after_document_calls", 0)
                ):
                    behavior["model_held"].set()
                    assert behavior["model_gate"].wait(timeout=60)
                revision = ("22" if body["model"] == "fixture-v2" else "11") * 32
                if backfill and body.get("model_revision") != revision:
                    with records_lock:
                        row.update(completed=time.monotonic(), status=412)
                    return self.reply(412, {"error": "model revision mismatch"})
                try:
                    self.reply(
                        200,
                        {
                            "object": "list",
                            "data": [
                                {
                                    "object": "embedding",
                                    "index": i,
                                    "embedding": (
                                        [
                                            -1.0
                                            if body["model"] == "fixture-v2"
                                            else 1.0,
                                            (int(match.group(1)) + 1) / 2048
                                            if (
                                                match := re.search(
                                                    r"Document (\d+)\.", text
                                                )
                                            )
                                            else 0.0,
                                        ]
                                        if backfill
                                        else [0.0, 1.0]
                                        if body["model"] == "fixture-v2"
                                        else [1.0, 0.0]
                                    ),
                                }
                                for i, text in enumerate(inputs)
                            ],
                        },
                        headers=(("X-Antfly-Model-Revision", revision),)
                        if backfill
                        else (),
                    )
                except OSError:
                    with records_lock:
                        row.update(completed=time.monotonic(), status=502)
                    raise
                else:
                    with records_lock:
                        row.update(completed=time.monotonic(), status=200)
                return
            calls["batches"].append(len(body["inputs"]))
            data = []
            for item in body["inputs"]:
                text = item["content"]
                if isinstance(text, list):
                    text = "\n".join(part.get("text", "") for part in text)
                calls["extract"].append(text)
                result = {
                    "offset_unit": "utf8_bytes",
                    "entities": [
                        {
                            "label": "email",
                            "text": match.group(),
                            "start": len(text[: match.start()].encode()),
                            "end": len(text[: match.end()].encode()),
                            "score": 1.0,
                        }
                        for match in re.finditer(r"[\w.+-]+@[\w.-]+\.[A-Za-z]+", text)
                    ],
                }
                if "id" in item:
                    result["id"] = item["id"]
                data.append(result)
            if behavior["deny"]:
                return self.reply(403, {"error": "fixture access denied"})
            self.reply(
                200,
                {
                    "object": "extraction",
                    "model": body["model"],
                    "schema_version": 2,
                    "data": data,
                },
            )

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = start_http_server(server)
    endpoint = f"http://127.0.0.1:{server.server_port}"
    try:
        yield endpoint, calls, behavior, native
    finally:
        behavior["model_gate"].set()
        behavior["embed_gate"].set()
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def create_indexed_table(api, endpoint):
    table = "privacy_indexed_" + uuid4().hex[:8]
    api.create_table(table)
    api.create_index(
        table,
        "a0",
        {
            "type": "embeddings",
            "dimension": 2,
            "field": "body",
            "distance_metric": "cosine",
            "publication_policy": "atomic",
            "embedder": {"provider": "antfly", "model": "fixture", "api_url": endpoint},
        },
    )
    return table


def policy(endpoint):
    return {
        "mode": "redact_for_search_and_ai",
        "fields": ["body"],
        "category_groups": [],
        "categories": [{"name": "email"}],
        "replacement": "category",
        "producer": {
            "type": "extractor",
            "revision": "fixture-v1",
            "config": {
                "provider": "antfly",
                "model": "fixture",
                "schema_version": 2,
                "api_url": endpoint,
            },
        },
    }


def query(api, table, index="a0", vector=None):
    try:
        response = api.s.post(
            api.url + "/query",
            json={
                "table": table,
                "indexes": [index],
                "embeddings": {index: vector or [1.0, 0.0]},
                "fields": ["body"],
                "limit": 100,
            },
            timeout=15,
        )
    except ReadTimeout:
        # The protected-read barrier can outlive one polling request while
        # inspection/publication catches up; the outer wait bounds convergence.
        return None
    if response.status_code in (409, 503):
        return None
    assert response.status_code == 200, response.text
    row = response.json()["responses"][0]
    if row["status"] in (409, 503):
        return None
    assert row["status"] == 200, row
    return row["hits"]["hits"]


def wait_complete(api, table, index):
    observation = {}

    def complete():
        nonlocal observation
        observation = api.get_index(table, index)
        return (
            observation.get("status", {}).get("readiness", {}).get("complete") is True
        )

    assert wait_until(complete, timeout_s=60), (observation, api.debug_logs())


def redacted(api, table, docs, index="a0", vector=None):
    hits = query(api, table, index, vector)
    if hits is None:
        return None
    for hit in hits:
        body = hit["_source"]["body"]
        assert "@" not in body and body.startswith("Orchard schedule. Contact "), hit
        # Hierarchy source projections travel in the same response envelope.
        assert "@" not in json.dumps(hit), hit
    return hits if {hit["_id"] for hit in hits} == set(docs) else None


@pytest.mark.parametrize(
    "privacy_provider", [False, True], indirect=True, ids=["legacy", "native-batch"]
)
@pytest.mark.parametrize(
    "before_write", [False, True], ids=["backfill", "before-first-write"]
)
@pytest.mark.parametrize("documents", [1, 8])
def test_privacy_activation_on_indexed_documents(
    stateful_api, privacy_provider, before_write, documents
):
    api = stateful_api
    endpoint, calls, _, native = privacy_provider
    table = create_indexed_table(api, endpoint)
    docs = {
        f"d{i}": {"body": f"Orchard schedule. Contact Éva person{i}@example.invalid."}
        for i in range(documents)
    }
    if before_write:
        api.put(f"/tables/{table}/privacy", policy(endpoint))
    api.batch_write(table, inserts=docs, sync_level="full_index")
    if not before_write:
        baseline = query(api, table)
        assert baseline is not None and {hit["_id"] for hit in baseline} == set(docs)
        api.put(f"/tables/{table}/privacy", policy(endpoint))
    assert wait_until(
        lambda: redacted(api, table, docs), timeout_s=60, interval_s=0.2
    ), api.debug_logs()
    assert len(calls["extract"]) >= documents, calls
    assert any("[EMAIL]" in text for text in calls["embed"]), calls
    assert not any(path.endswith("/execute") for path in calls["paths"]), calls
    if documents == 8 and native:
        assert max(calls["batches"]) > 1 and max(calls["batches"]) <= 8, calls
    if documents == 1 and not before_write and api.supports_restart:
        api.restart_server()
        assert wait_until(
            lambda: redacted(api, table, docs), timeout_s=60, interval_s=0.2
        ), api.debug_logs()


def test_privacy_parked_native_failure_requires_explicit_repair_after_restart(
    stateful_api, privacy_provider
):
    api = stateful_api
    endpoint, calls, behavior, _ = privacy_provider
    table = create_indexed_table(api, endpoint)
    path = f"/tables/{table}/privacy"
    docs = {"d0": {"body": "Orchard schedule. Contact person@example.invalid."}}
    api.batch_write(table, inserts=docs, sync_level="full_index")
    behavior["deny"] = True
    installed = api.put(path, policy(endpoint))

    def failed():
        value = api.get(path)["coverage"]
        return (
            value if value.get("available") and value.get("failed_known") == 1 else None
        )

    assert wait_until(failed, timeout_s=60, interval_s=0.2), api.debug_logs()
    issues = api.post(
        f"/tables/{table}/repair/issues", {"target": "artifact", "limit": 50}
    )
    assert any(
        issue["generation_error"] == "ExtractionForbidden" for issue in issues["issues"]
    ), issues
    previous = len(calls["extract"])
    # Changing the deployment/provider does not grant permission to erase
    # durable terminal debt. The same source must remain closed after reopen.
    behavior["deny"] = False
    if api.supports_restart:
        api.restart_server()
    assert wait_until(failed, timeout_s=60, interval_s=0.2), api.debug_logs()
    denied = api.s.get(api.url + f"/tables/{table}/documents/d0", timeout=15)
    assert denied.status_code in (404, 409, 503), denied.text
    assert "@" not in denied.text, denied.text
    assert len(calls["extract"]) == previous, calls
    retried = api.post(path + "/scans", {"limit": 100})

    def repaired():
        scan = api.get(path + "/scans/" + retried["scan_id"])
        assert scan["phase"] != "failed", scan
        return scan if scan["phase"] == "succeeded" else None

    assert wait_until(repaired, timeout_s=60, interval_s=0.2), api.debug_logs()
    assert wait_until(
        lambda: redacted(api, table, docs), timeout_s=60, interval_s=0.2
    ), api.debug_logs()
    current = api.get(path)
    assert current["policy_version"] == installed["policy_version"]
    assert (
        current["coverage"]["pending"] == 0
        and current["coverage"]["protected_blocked"] == 0
    ), current
    assert len(calls["extract"]) > previous, calls


def test_privacy_policy_replacement_and_new_model_migration(
    stateful_api, privacy_provider
):
    api = stateful_api
    endpoint, calls, _, _ = privacy_provider
    table = create_indexed_table(api, endpoint)
    docs = {"d0": {"body": "Orchard schedule. Contact person@example.invalid."}}
    api.batch_write(table, inserts=docs, sync_level="full_index")
    path = f"/tables/{table}/privacy"
    api.put(path, policy(endpoint))
    assert wait_until(lambda: redacted(api, table, docs), timeout_s=60), (
        api.debug_logs()
    )

    # Each policy update must complete backfill, not merely return 202.
    for field, value in (("revision", "fixture-v2"), ("replacement", "mask")):
        current = api.get(path)
        updated = current["policy"]
        if field == "revision":
            updated["producer"][field] = value
        else:
            updated[field] = value
        before = len(calls["extract"])
        response = api.s.put(
            api.url + path,
            json=updated,
            headers={"If-Match": current["etag"]},
            timeout=20,
        )
        assert response.status_code == 202, response.text
        assert wait_until(
            lambda before=before: (
                len(calls["extract"]) > before and redacted(api, table, docs)
            ),
            timeout_s=60,
        ), api.debug_logs()
        if field == "replacement":
            assert "[EMAIL]" not in json.dumps(redacted(api, table, docs))

    alias = f"/tables/{table}/index-aliases/search"
    wait_complete(api, table, "a0")
    api.put(alias, {"index": "a0", "idempotency_key": "initial", "require": "complete"})
    before = len(calls["extract"])
    api.create_index(
        table,
        "b",
        {
            "type": "embeddings",
            "dimension": 2,
            "field": "body",
            "distance_metric": "cosine",
            "publication_policy": "atomic",
            "embedder": {
                "provider": "antfly",
                "model": "fixture-v2",
                "api_url": endpoint,
            },
        },
    )
    assert wait_until(
        lambda: redacted(api, table, docs, "b", [0.0, 1.0]), timeout_s=60
    ), api.debug_logs()
    assert redacted(api, table, docs, "search")
    # The existing producer's inspection receipts are shared with B.
    assert len(calls["extract"]) == before
    wait_complete(api, table, "b")
    # Persisted chunk vectors are inspected through the admin artifact boundary.
    before_inspection = len(calls["embed"])
    identities = {}
    for name, model, expected in (
        ("a0", "fixture", [1.0, 0.0]),
        ("b", "fixture-v2", [0.0, 1.0]),
    ):
        manifest = api.get(f"/tables/{table}/documents/d0/artifacts/{name}?detail=raw")
        assert manifest["route_type"] == "dense_index_inspection"
        assert manifest["manifest_version"] == 1
        snapshot = json.loads(manifest["manifest_json"])
        assert snapshot["format"] == "antfly.dense-index-inspection.v1"
        assert snapshot["current"] is True
        assert snapshot["policy_version"] == api.get(path)["policy_version"]
        assert snapshot["index"] == name and snapshot["document_id"] == "d0"
        assert len(snapshot["vectors"]) == 1
        vector = snapshot["vectors"][0]
        assert vector["values"] == expected
        assert vector["model"] == model
        assert "@" not in vector["source_text"]
        assert vector["source_text"] == json.loads(snapshot["document_json"])["body"]
        identities[name] = snapshot["index_fingerprint"]
    assert identities["a0"] != identities["b"]
    assert len(calls["embed"]) == before_inspection
    api.put(
        alias,
        {
            "index": "b",
            "expected_index": "a0",
            "idempotency_key": "cutover",
            "require": "complete",
        },
    )
    hits = redacted(api, table, docs, "search", [0.0, 1.0])
    assert hits and all(hit["_score"] > 0.99 for hit in hits), hits
    # The old model can be decommissioned while protection remains active.
    response = api.s.delete(api.url + f"/tables/{table}/indexes/a0", timeout=20)
    assert response.status_code == 201, (response.text, api.debug_logs())
    previous_models = len(calls["embed_models"])
    docs["d0"]["body"] += " Updated after retirement."
    api.batch_write(table, inserts=docs, sync_level="full_index")
    assert redacted(api, table, docs, "search", [0.0, 1.0])
    assert calls["embed_models"][previous_models:]
    assert set(calls["embed_models"][previous_models:]) == {"fixture-v2"}
    current = api.get(path)
    blocked = current["policy"]
    blocked["replacement"] = "category"
    response = api.s.put(
        api.url + path, json=blocked, headers={"If-Match": current["etag"]}, timeout=20
    )
    assert response.status_code == 409, response.text
    assert response.json()["code"] == "IndexReferencedByAlias"
    assert api.get(path)["etag"] == current["etag"]
    alias_resource = api.s.get(api.url + alias, timeout=20)
    assert alias_resource.status_code == 200, alias_resource.text
    alias_tag = alias_resource.headers["ETag"]
    response = api.s.delete(api.url + alias, timeout=20)
    assert response.status_code == 428, response.text
    response = api.s.delete(api.url + alias, headers={"If-Match": "*"}, timeout=20)
    assert response.status_code == 412, response.text
    response = api.s.delete(
        api.url + alias, headers={"If-Match": alias_tag}, timeout=20
    )
    assert response.status_code == 200, response.text
    assert api.s.get(api.url + alias, timeout=20).status_code == 404
    response = api.s.put(
        api.url + path, json=blocked, headers={"If-Match": current["etag"]}, timeout=20
    )
    assert response.status_code == 202, response.text
    assert wait_until(
        lambda: redacted(api, table, docs, "b", [0.0, 1.0]), timeout_s=60
    ), api.debug_logs()
    wait_complete(api, table, "b")
    api.put(
        alias, {"index": "b", "idempotency_key": "recreated", "require": "complete"}
    )
    response = api.s.delete(
        api.url + alias, headers={"If-Match": alias_tag}, timeout=20
    )
    assert response.status_code == 412, response.text
    if api.supports_restart:
        api.restart_server()
        assert wait_until(
            lambda: redacted(api, table, docs, "search", [0.0, 1.0]), timeout_s=60
        ), api.debug_logs()


def test_privacy_held_update_delete_crash_recovers_current_sources(
    stateful_api, privacy_provider
):
    api = stateful_api
    if not api.supports_restart or not hasattr(api._server, "pause"):
        pytest.skip("requires locally managed crash recovery")
    endpoint, _, behavior, _ = privacy_provider
    table = create_indexed_table(api, endpoint)
    docs = {"d0": {"body": "Orchard schedule. Contact person@example.invalid."}}
    api.batch_write(table, inserts=docs, sync_level="full_index")
    api.put(f"/tables/{table}/privacy", policy(endpoint))
    assert wait_until(lambda: redacted(api, table, docs), timeout_s=60), (
        api.debug_logs()
    )
    behavior["embed_gate"].clear()
    try:
        api.batch_write(
            table,
            inserts={
                "d0": {
                    "body": "Orchard schedule. Contact person@example.invalid. First revision."
                },
                "temporary": {
                    "body": "Orchard schedule. Contact temporary@example.invalid."
                },
            },
        )
        assert behavior["embed_held"].wait(timeout=20), api.debug_logs()
        docs["d0"]["body"] += " Final correction."
        api.batch_write(table, inserts=docs, deletes=["temporary"])
        api.s.close()
        api._server.pause(crash=True)
        api.resume_server()
    finally:
        behavior["embed_gate"].set()

    def recovered():
        hits = redacted(api, table, docs)
        return (
            hits if hits and "Final correction." in hits[0]["_source"]["body"] else None
        )

    assert wait_until(recovered, timeout_s=90), api.debug_logs()
    assert "StorageKernelFailure" not in api.debug_logs()
