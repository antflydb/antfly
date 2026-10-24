#!/usr/bin/env python3
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

"""Native ordinary-table catalog contracts; no Cloud or ingestion service needed."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BINARY = str(Path(sys.argv.pop(1)).resolve())


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.mode = "complete"
        self.pages = []
        self.content_requests = []
        self.file_catalog = None
        self.records = []
        for i in range(70):
            identity = str(uuid.UUID(int=i + 1))
            self.records.append(
                {
                    "_id": identity,
                    "_source": {
                        "version": 1,
                        "doc_type": "source_catalog",
                        "document_version": 1,
                        "published_revision": "inventory-1",
                        "source": {
                            "id": identity,
                            "alias": "manuals" if i == 0 else f"other-{i}",
                            "table": "documents",
                            "document_field": "grep",
                            "filter": {"equals": identity, "field": "source_id"},
                            "status": "searchable",
                            "valid_until": int(time.time()) + 900,
                        },
                    },
                }
            )
        test = self

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

            def do_POST(self):
                if self.headers.get("Authorization") != "Bearer table-token":
                    self.reply(403, {})
                    return
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/db/v1/tables/antfly_sources/query":
                    test.pages.append(body)
                    if test.mode == "unauthorized":
                        self.reply(403, {})
                        return
                    if body.get("remote_snapshot") and test.mode == "failed":
                        self.reply(503, {})
                        return
                    after = body.get("search_after", [""])[0]
                    rows = test.records
                    selected = body.get("query", {}).get("conjuncts")
                    if selected:
                        selected_id = selected[1]["equals"]
                        rows = [row for row in rows if row["_id"] == selected_id]
                    hits = [row for row in rows if row["_id"] > after][: body["limit"]]
                    snapshot = (
                        "changed" if after and test.mode == "changed" else "catalog-1"
                    )
                    self.reply(
                        200,
                        {
                            "responses": [
                                {
                                    "status": 200,
                                    "remote_snapshot": snapshot,
                                    "hits": {"hits": hits},
                                }
                            ]
                        },
                    )
                    return
                if self.path != "/db/v1/tables/documents/query":
                    self.reply(404, {})
                    return
                test.content_requests.append(body)
                if not body.get("remote_snapshot"):
                    if test.mode == "syncing_before_snapshot":
                        test.records[0]["_source"]["source"]["status"] = "syncing"
                    elif test.mode == "file_syncing_before_snapshot":
                        value = json.loads(test.file_catalog.read_text())
                        value["sources"][0]["status"] = "syncing"
                        test.file_catalog.write_text(json.dumps(value))
                    elif test.mode == "republished_before_snapshot":
                        test.records[0]["_source"]["published_revision"] = "inventory-2"
                identity = test.records[0]["_id"]
                hits = (
                    []
                    if body.get("search_after")
                    else [
                        {
                            "_id": "item-1",
                            "_sort": ["item-1"],
                            "_source": {
                                "grep": {
                                    "version": 1,
                                    "uri": "s3://docs/manual.txt",
                                    "path": "manual.txt",
                                    "content": "retry budget\n",
                                    "revision": "content-1",
                                    "indexed_at": 1,
                                    "permissions": {"public": True, "readers": []},
                                    "metadata": {"source_id": identity},
                                }
                            },
                        }
                    ]
                )
                self.reply(
                    200,
                    {
                        "responses": [
                            {
                                "status": 200,
                                "remote_snapshot": "content-1",
                                "hits": {"hits": hits},
                            }
                        ]
                    },
                )

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}/db/v1"

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def search(self):
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("ANTFLY_GREP_") and key != "RIPGREP_CONFIG_PATH"
        }
        env.update(
            ANTFLY_GREP_CONFIG_PATH="",
            RIPGREP_CONFIG_PATH="",
            ANTFLY_GREP_TABLE_TOKEN="table-token",
            PATH=self.temp.name,
        )
        return subprocess.run(
            [
                BINARY,
                *(
                    ["--source-catalog", str(self.file_catalog)]
                    if self.file_catalog
                    else ["--source-endpoint", self.endpoint]
                ),
                "--source",
                "manuals",
                "retry budget",
            ],
            env=env,
            capture_output=True,
            timeout=15,
        )

    def test_discovers_complete_snapshot_without_cloud_or_file(self):
        result = self.search()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b"retry budget", result.stdout)
        self.assertEqual(len(self.pages), 3)
        self.assertEqual(self.pages[1]["remote_snapshot"], "catalog-1")
        self.assertEqual(self.pages[1]["search_after"], [self.records[63]["_id"]])
        self.assertIn(
            self.records[0]["_id"], json.dumps(self.content_requests[0]["filter_query"])
        )

    def test_publication_changes_before_snapshot_fail_without_output(self):
        for mode, error in (
            ("syncing_before_snapshot", b"SourceNotSearchable"),
            ("republished_before_snapshot", b"SourcePublicationChanged"),
        ):
            with self.subTest(mode=mode):
                self.mode = mode
                self.records[0]["_source"]["source"]["status"] = "searchable"
                self.records[0]["_source"]["published_revision"] = "inventory-1"
                result = self.search()
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(error, result.stderr)
                self.assertEqual(result.stdout, b"")

    def test_file_catalog_is_checked_after_content_snapshot(self):
        self.file_catalog = Path(self.temp.name) / "catalog.json"
        source = dict(self.records[0]["_source"]["source"])
        source["published_revision"] = "publication-1"
        value = {
            "version": 2,
            "endpoint": self.endpoint,
            "sources": [source],
            "principals": [],
        }
        self.file_catalog.write_text(json.dumps(value))
        result = self.search()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.mode = "file_syncing_before_snapshot"
        result = self.search()
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn(b"SourceNotSearchable", result.stderr)
        self.assertEqual(result.stdout, b"")

    def test_partial_changed_or_unauthorized_discovery_never_searches(self):
        for mode in ("changed", "failed", "unauthorized"):
            with self.subTest(mode=mode):
                self.mode = mode
                result = self.search()
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse(self.content_requests)

    def test_invalid_publication_and_expired_lease_never_search(self):
        record = self.records[0]["_source"]
        for mutation, error in (
            ("version", b"InvalidSourceCatalog"),
            ("revision", b"InvalidSourceCatalog"),
            ("lease", b"SourceNotSearchable"),
        ):
            with self.subTest(mutation=mutation):
                record["version"] = 2 if mutation == "version" else 1
                record["published_revision"] = (
                    "" if mutation == "revision" else "inventory-1"
                )
                record["source"]["valid_until"] = (
                    1 if mutation == "lease" else int(time.time()) + 900
                )
                result = self.search()
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(error, result.stderr)
                self.assertFalse(self.content_requests)


if __name__ == "__main__":
    unittest.main()
