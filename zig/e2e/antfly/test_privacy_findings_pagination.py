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

"""Findings cursors preserve empty IDs across byte-bounded pages."""

import hashlib
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest
from helpers import start_http_server, wait_until
from privacy_findings_read import read_findings_page


@pytest.mark.parametrize("escaped", [False, True], ids=["large", "escaped"])
def test_privacy_findings_empty_id_byte_cutoff(stateful_api, escaped):
    class Detector(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            prompt = json.loads(request["messages"][1]["content"])
            items = [
                {
                    "item": item["id"],
                    "value": {
                        "findings": [
                            {"category": "email", "start": 0, "end": 6},
                            {"category": "email", "start": 7, "end": 13},
                        ]
                    },
                }
                for item in prompt["items"]
            ]
            payload = json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "tool_calls": [
                                    {
                                        "id": "pagination-fixture",
                                        "type": "function",
                                        "function": {
                                            "name": "antfly_output",
                                            "arguments": json.dumps({"items": items}),
                                        },
                                    }
                                ]
                            }
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    detector = ThreadingHTTPServer(("127.0.0.1", 0), Detector)
    thread = start_http_server(detector)
    try:
        api = stateful_api
        table = "privacy_empty_cursor_" + uuid4().hex[:8]
        # Either the raw ID or its JSON escaping exceeds the 8 MiB page
        # budget. The preceding empty ID must be the first page's last ID.
        large = (
            "\\" * (4 * 1024 * 1024 + 256) if escaped else "z" * (8 * 1024 * 1024 + 256)
        )
        api.create_table(table)
        api.batch_write(
            table,
            inserts={"": {"body": "a@b.co a@b.co"}, large: {"body": "a@b.co a@b.co"}},
        )
        path = f"/tables/{table}/privacy"
        saved = api.put(
            path,
            {
                "mode": "monitor",
                "fields": ["body"],
                "category_groups": [],
                "categories": [{"name": "email"}],
                "producer": {
                    "type": "generator",
                    "revision": "pagination-fixture-v1",
                    "config": {
                        "generator": {
                            "provider": "openai",
                            "model": "gpt-4.1",
                            "api_key": "",
                            "url": f"http://127.0.0.1:{detector.server_port}/v1",
                        },
                        "output": "auto",
                        "output_schema": {
                            "type": "object",
                            "required": ["findings"],
                            "additionalProperties": False,
                            "properties": {
                                "findings": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "required": ["category", "start", "end"],
                                        "additionalProperties": False,
                                        "properties": {
                                            "category": {"type": "string"},
                                            "start": {"type": "integer"},
                                            "end": {"type": "integer"},
                                        },
                                    },
                                }
                            },
                        },
                    },
                },
            },
        )

        def inspected():
            scan = api.get(path + "/scans/" + saved["scan_id"])
            assert scan["phase"] != "failed", scan
            return scan["phase"] == "succeeded"

        assert wait_until(inspected, timeout_s=180, interval_s=0.2), api.get(path)

        # Availability retries share this budget across every page and the
        # final GET replay; failed reads never advance cursor or observations.
        deadline = time.monotonic() + 180
        cursor = None
        previous = set()
        observed = []
        examined = []
        continuations = []
        # A one-finding limit forces inclusive continuations within each
        # document, as well as exclusive continuations across the ID cutoff.
        for _ in range(5):
            page = read_findings_page(
                api.s,
                api.url + path,
                {"limit": 1, **({"cursor": cursor} if cursor else {})},
                deadline=deadline,
            )
            assert page["pending_documents"] == 0
            examined.append(page["examined_documents"])
            for finding in page["findings"]:
                observed.append(
                    (
                        hashlib.sha256(finding["document"].encode()).hexdigest(),
                        finding["start"],
                        finding["end"],
                    )
                )
            cursor = page["next_cursor"]
            if cursor is None:
                break
            digest = hashlib.sha256(cursor.encode()).hexdigest()
            assert digest not in previous, (
                "Findings continuation repeated instead of advancing"
            )
            previous.add(digest)
            continuations.append(cursor)
        assert cursor is None, "Findings did not reach EOF within the expected pages"
        empty_digest = hashlib.sha256(b"").hexdigest()
        large_digest = hashlib.sha256(large.encode()).hexdigest()
        assert observed == [
            (empty_digest, 0, 6),
            (empty_digest, 7, 13),
            (large_digest, 0, 6),
            (large_digest, 7, 13),
        ]
        assert examined == [1, 1, 1, 1, 0]
        assert len(continuations) == 4
        # The exclusive cursor after the empty ID must also resume through
        # GET, while larger opaque cursors continue through the POST body.
        page = read_findings_page(
            api.s,
            api.url + path,
            {"cursor": continuations[1], "limit": 1},
            deadline=deadline,
            method="GET",
        )
        assert page["examined_documents"] == 1
        assert len(page["findings"]) == 1
        assert (
            hashlib.sha256(page["findings"][0]["document"].encode()).hexdigest()
            == large_digest
        )
    finally:
        detector.shutdown()
        detector.server_close()
        thread.join(timeout=5)
