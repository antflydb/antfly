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

"""Black-box stream contracts; real native extraction lives in sources' E2E test."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BINARY = str(Path(sys.argv.pop(1)).resolve())


class NativeStreams(unittest.TestCase):
    def setUp(self):
        self.complete = True
        self.revision = 1
        self.wrong_field = False
        self.requests = []
        self.readers = ["user:self"]
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def reply(self, body):
                raw = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                owner.requests.append(("GET", self.path))
                artifact = "summary" if self.path.endswith("summary_text") else "speech"
                config = {
                    "type": "full_text",
                    "sources": [
                        {
                            "artifact": artifact,
                            **({"field": "text"} if artifact == "speech" else {}),
                        }
                    ],
                }
                if owner.wrong_field:
                    config["sources"][0]["field"] = "different_text"
                self.reply(
                    {
                        "config": config,
                        "status": {
                            "readiness": {
                                "queryable": True,
                                "incarnation": "index-1",
                                "target_revision": owner.revision,
                                "published_revision": owner.revision,
                                "sources": [
                                    {"artifact": artifact, "complete": owner.complete}
                                ],
                            }
                        },
                    }
                )

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                owner.requests.append(("POST", body))
                parent = {
                    "source": {
                        "uri": "gs://originals/interview.wav",
                        "path": "interview.wav",
                        "revision": "recording-7",
                        "indexed_at": 1793740000,
                    },
                    "permissions": {"public": False, "readers": owner.readers},
                    "metadata": {},
                }
                children = body.get("hierarchy", {}).get("children")
                if children is None:
                    records = (
                        []
                        if body.get("search_after")
                        else [
                            {
                                "_id": "recording",
                                "_sort": ["recording"],
                                "_source": parent,
                            }
                        ]
                    )
                    self.reply(
                        {
                            "responses": [
                                {
                                    "status": 200,
                                    "remote_snapshot": "retained-1",
                                    "hits": {"hits": records},
                                }
                            ]
                        }
                    )
                    return
                artifact = children["artifact"]
                records = [
                    {
                        "_id": f"segment-{i}",
                        "_sort": [f"position-{i}", f"segment-{i}"],
                        "_source": {
                            "text": text,
                            "provenance": {
                                "start_time_ms": i * 1000,
                                "end_time_ms": (i + 1) * 1000,
                            },
                        },
                        "hierarchy": {
                            "parent_doc_key": "recording",
                            "revision": "artifact-4",
                            "artifact": {
                                "name": artifact,
                                "kind": "asset",
                                "revision": "member-revision-4",
                                "derivation_revision": "producer-input-3",
                                "unit_id": f"segment-{i}",
                            },
                            "ancestors": {
                                "source": {"id": "recording", "document": parent}
                            },
                        },
                    }
                    for i, text in enumerate(
                        ["Tuesday arrived", "meeting started", "Tuesday ended"]
                    )
                ]
                if artifact == "summary":
                    records = records[:1]
                    records[0]["_source"] = {"$": "Tuesday summary"}
                cursor = body.get("search_after")
                start = int(cursor[1].split("-")[-1]) + 1 if cursor else 0
                self.reply(
                    {
                        "responses": [
                            {
                                "status": 200,
                                "remote_snapshot": "retained-1",
                                "hits": {
                                    "hits": records[start : start + body["limit"]]
                                },
                            }
                        ]
                    }
                )

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.config = Path(self.tmp.name) / "tables.json"
        self.config.write_text(
            json.dumps(
                {
                    "version": 1,
                    "endpoint": f"http://127.0.0.1:{self.server.server_port}/db/v1",
                    "principals": ["user:self"],
                    "page_size": 1,
                    "tables": [
                        {
                            "name": "recordings",
                            "fields": {
                                "uri": "source.uri",
                                "path": "source.path",
                                "revision": "source.revision",
                                "indexed_at": "source.indexed_at",
                                "permissions": "permissions",
                                "metadata": "metadata",
                            },
                            "streams": [
                                {
                                    "name": "transcript",
                                    "artifact": "speech",
                                    "index": "speech_text",
                                    "field": "text",
                                    "default": True,
                                    "boundary": "unit",
                                    "purpose": "transcription",
                                },
                                {
                                    "name": "summary",
                                    "artifact": "summary",
                                    "index": "summary_text",
                                    "field": "$",
                                    "default": False,
                                    "generated": True,
                                    "boundary": "record",
                                    "purpose": "summary",
                                },
                            ],
                        }
                    ],
                }
            )
        )

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.tmp.cleanup()

    def run_grep(self, pattern, *flags):
        return subprocess.run(
            [BINARY, "--table-config", str(self.config), "--json", *flags, pattern],
            capture_output=True,
            text=True,
            timeout=20,
        )

    def test_pagination_identity_and_boundaries(self):
        result = self.run_grep("Tuesday")
        self.assertEqual(result.returncode, 0, result.stderr)
        matches = [
            json.loads(row)
            for row in result.stdout.splitlines()
            if json.loads(row)["type"] == "match"
        ]
        self.assertEqual(len(matches), 2, result.stdout)
        self.assertIn('"artifact_key"', result.stdout)
        self.assertIn('"start_time_ms"', result.stdout)
        self.assertIn("recording-7", result.stdout)
        self.assertEqual(self.run_grep("arrived.*meeting", "-U").returncode, 1)
        posts = [body for method, body in self.requests if method == "POST"]
        self.assertTrue(any(body.get("search_after") for body in posts))
        self.assertTrue(
            all(
                body["hierarchy"]["children"]["parent"]["id"] == "recording"
                for body in posts
                if body.get("hierarchy")
            )
        )

    def test_generated_selection_and_authorization_narrowing(self):
        result = self.run_grep("Tuesday", "--artifact", "summary")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("[generated]", result.stdout)
        self.assertIn('"generated":true', result.stdout)
        for flags in [
            ("--all-artifacts",),
            ("--artifact", "transcript", "--artifact", "summary"),
        ]:
            both = self.run_grep("Tuesday", *flags)
            self.assertEqual(both.returncode, 0, both.stderr)
            self.assertEqual(
                sum(
                    json.loads(row)["type"] == "match"
                    for row in both.stdout.splitlines()
                ),
                3,
                both.stdout,
            )
        self.readers = ["group:other"]
        self.assertEqual(self.run_grep("Tuesday").returncode, 1)

    def test_streams_require_identity_and_permission_fields(self):
        config = json.loads(self.config.read_text())
        del config["tables"][0]["fields"]
        self.config.write_text(json.dumps(config))
        result = self.run_grep("Tuesday")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse(self.requests)

    def test_data_revision_change_invalidates_scan(self):
        handler = self.server.RequestHandlerClass
        original = handler.do_POST
        owner = self

        def changing_post(request):
            original(request)
            owner.revision += 1

        handler.do_POST = changing_post
        result = self.run_grep("absent")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('"complete":false', result.stdout)

    def test_pending_unknown_and_budget_are_errors(self):
        self.complete = False
        result = self.run_grep("absent")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse(any(method == "POST" for method, _ in self.requests))
        self.complete = True
        self.wrong_field = True
        self.assertEqual(self.run_grep("Tuesday").returncode, 2)
        self.assertFalse(any(method == "POST" for method, _ in self.requests))
        self.wrong_field = False
        self.assertEqual(
            self.run_grep("Tuesday", "--artifact", "missing").returncode, 2
        )
        self.assertEqual(
            self.run_grep("Tuesday", "--max-index-documents", "1").returncode, 2
        )
        self.assertEqual(self.run_grep("Tuesday", "--max-requests", "1").returncode, 2)


if __name__ == "__main__":
    unittest.main()
