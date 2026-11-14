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

"""Model-free regression for privacy enrollment on existing dense indexes (#1682)."""

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest
from helpers import start_http_server, wait_until


@pytest.mark.parametrize("documents", [1, 8])
def test_privacy_activation_on_indexed_documents(stateful_api, documents):
    api = stateful_api
    calls = {"embed": [], "extract": [], "paths": []}

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
            self.reply(404, {"error": "legacy native provider"})

        def do_POST(self):
            calls["paths"].append(self.path)
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            path = self.path.removeprefix("/ai/v1")
            if path == "/embed":
                inputs = body["input"]
                if isinstance(inputs, str):
                    inputs = [inputs]
                calls["embed"].extend(inputs)
                self.reply(
                    200,
                    {
                        "object": "list",
                        "data": [
                            {"object": "embedding", "index": i, "embedding": [1.0, 0.0]}
                            for i in range(len(inputs))
                        ],
                    },
                )
            elif path == "/extract":
                data = []
                for item in body["inputs"]:
                    text = item["content"]
                    if isinstance(text, list):
                        text = "\n".join(part.get("text", "") for part in text)
                    calls["extract"].append(text)
                    data.append(
                        {
                            "offset_unit": "utf8_bytes",
                            "entities": [
                                {
                                    "label": "email",
                                    "text": match.group(),
                                    "start": len(text[: match.start()].encode()),
                                    "end": len(text[: match.end()].encode()),
                                    "score": 1.0,
                                }
                                for match in re.finditer(
                                    r"[\w.+-]+@[\w.-]+\.[A-Za-z]+", text
                                )
                            ],
                        }
                    )
                self.reply(
                    200,
                    {
                        "object": "extraction",
                        "model": body["model"],
                        "schema_version": 2,
                        "data": data,
                    },
                )
            else:
                self.reply(404, {})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = start_http_server(server)
    endpoint = f"http://127.0.0.1:{server.server_port}"
    table = "privacy_indexed_" + uuid4().hex[:8]
    docs = {
        f"d{i}": {"body": f"Orchard schedule. Contact person{i}@example.invalid."}
        for i in range(documents)
    }
    query = {
        "table": table,
        "indexes": ["a0"],
        "embeddings": {"a0": [1.0, 0.0]},
        "fields": ["body"],
        "limit": 100,
    }
    try:
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
                "embedder": {
                    "provider": "antfly",
                    "model": "fixture",
                    "api_url": endpoint,
                },
            },
        )
        api.batch_write(table, inserts=docs, sync_level="full_index")
        baseline = api.post("/query", query)["responses"][0]
        assert baseline["status"] == 200, baseline
        assert {hit["_id"] for hit in baseline["hits"]["hits"]} == set(docs)
        api.put(
            f"/tables/{table}/privacy",
            {
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
            },
        )

        def redacted():
            response = api.s.post(api.url + "/query", json=query, timeout=5)
            if response.status_code in (409, 503):
                return None
            assert response.status_code == 200, response.text
            row = response.json()["responses"][0]
            if row["status"] in (409, 503):
                return None
            assert row["status"] == 200, row
            hits = row["hits"]["hits"]
            # Every successful protected response must fail closed, including
            # partial publication while the replacement graph is catching up.
            for hit in hits:
                body = hit["_source"]["body"]
                assert "@" not in body and body.startswith("Orchard schedule. Contact ")
            return hits if {hit["_id"] for hit in hits} == set(docs) else None

        assert wait_until(redacted, timeout_s=60, interval_s=0.2), api.debug_logs()
        assert len(calls["extract"]) >= documents, calls
        assert any("[EMAIL]" in text for text in calls["embed"]), calls
        assert not any(path.endswith("/execute") for path in calls["paths"]), calls
        if documents == 1 and api.supports_restart:
            api.restart_server()
            assert wait_until(redacted, timeout_s=60, interval_s=0.2), api.debug_logs()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
