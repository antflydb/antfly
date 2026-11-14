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
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import pytest
from helpers import start_http_server, wait_until


@pytest.fixture
def privacy_provider(request):
    native = request.param if hasattr(request, "param") else False
    calls = {"embed": [], "extract": [], "paths": [], "batches": []}
    behavior = {"deny": False}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, value):
            raw = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            route = urlparse(self.path)
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
                calls["embed"].extend(inputs)
                return self.reply(
                    200,
                    {
                        "object": "list",
                        "data": [
                            {"object": "embedding", "index": i, "embedding": [1.0, 0.0]}
                            for i in range(len(inputs))
                        ],
                    },
                )
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


def query(api, table):
    response = api.s.post(
        api.url + "/query",
        json={
            "table": table,
            "indexes": ["a0"],
            "embeddings": {"a0": [1.0, 0.0]},
            "fields": ["body"],
            "limit": 100,
        },
        timeout=5,
    )
    if response.status_code in (409, 503):
        return None
    assert response.status_code == 200, response.text
    row = response.json()["responses"][0]
    if row["status"] in (409, 503):
        return None
    assert row["status"] == 200, row
    return row["hits"]["hits"]


def redacted(api, table, docs):
    hits = query(api, table)
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
