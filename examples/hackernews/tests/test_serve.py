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

"""Public input cannot select private APIs, arbitrary sources or DSL options."""

import json
from pathlib import Path
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).parents[1]))
from serve import Gateway, next_cursor, search_request


class SearchGatewayTest(unittest.TestCase):
    def test_views_and_cursor_keep_fixed_search_contract(self):
        for view, table in (
            ("all", None),
            ("history", "hn_archive"),
            ("recent", "hn_current"),
        ):
            request = {"q": " database ", "view": view}
            if view == "recent":
                request["recent_since"] = 1700000000
            query = search_request(request)
            if table is None:
                self.assertEqual(query["source"], {"saved": "hackernews"})
                self.assertEqual(
                    search_request(dict(request, cursor="opaque"))["source_cursor"],
                    "opaque",
                )
            else:
                self.assertEqual(query["table"], table)
                self.assertNotIn("source", query)
                self.assertNotIn("source_ranking", query)
                cursor = next_cursor(query, self.page())
                resumed = search_request(dict(request, cursor=cursor))
                self.assertEqual(resumed["remote_snapshot"], "native2:retained")
                self.assertEqual(resumed["search_after"], [1.0, "row-19"])
                self.assertNotIn("source_cursor", resumed)
            self.assertEqual(query["full_text_search"]["match"], "database")
            self.assertEqual(query["limit"], 20)

    def test_recent_pagination_keeps_its_original_cutoff(self):
        first = search_request({"q": "database", "view": "recent"})
        cutoff = first["filter_query"]["range"]["gte"]
        second = search_request(
            {
                "q": "database",
                "view": "recent",
                "cursor": next_cursor(first, self.page()),
                "recent_since": cutoff,
            }
        )
        self.assertEqual(first["filter_query"], second["filter_query"])
        for value in (
            {"q": "database", "view": "recent", "cursor": "opaque"},
            {"q": "database", "recent_since": cutoff},
            {"q": "database", "view": "recent", "recent_since": True},
        ):
            with self.assertRaises(ValueError):
                search_request(value)

    @staticmethod
    def page():
        return {
            "remote_snapshot": "native2:retained",
            "hits": {
                "hits": [
                    {"_id": f"row-{i}", "_sort": [1.0, f"row-{i}"]} for i in range(20)
                ]
            },
        }

    def test_table_cursor_rejects_changed_search_and_stops_on_short_page(self):
        first = search_request({"q": "database", "view": "history"})
        cursor = next_cursor(first, self.page())
        for value in (
            {"q": "different", "view": "history", "cursor": cursor},
            {
                "q": "database",
                "view": "recent",
                "recent_since": 1700000000,
                "cursor": cursor,
            },
            {"q": "database", "view": "history", "cursor": "opaque"},
        ):
            with self.assertRaises(ValueError):
                search_request(value)
        self.assertIsNone(next_cursor(first, {"hits": {"hits": []}}))

    def test_wire_views_route_to_their_query_owner_and_resume(self):
        pages = []
        owner = self

        class Backend(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                query = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                pages.append((self.path, query))
                # A table source at /query is deliberately unsupported.
                valid = (
                    self.path
                    in (
                        "/db/v1/tables/hn_archive/query",
                        "/db/v1/tables/hn_current/query",
                    )
                    and "source" not in query
                )
                valid |= self.path == "/db/v1/query" and query.get("source") == {
                    "saved": "hackernews"
                }
                self.send_response(200 if valid else 400)
                self.end_headers()
                result = owner.page()
                if self.path == "/db/v1/query":
                    result["next_source_cursor"] = "composed-page"
                elif "search_after" in query:
                    result["hits"]["hits"] = []
                self.wfile.write(json.dumps({"responses": [result]}).encode())

        backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
        gateway = ThreadingHTTPServer(("127.0.0.1", 0), Gateway)
        gateway.backend = f"http://127.0.0.1:{backend.server_port}/db/v1"
        gateway.searches = threading.BoundedSemaphore(8)
        threads = [
            threading.Thread(target=server.serve_forever, daemon=True)
            for server in (backend, gateway)
        ]
        for thread in threads:
            thread.start()
        try:

            def call(body):
                with urlopen(
                    Request(
                        f"http://127.0.0.1:{gateway.server_port}/api/search",
                        json.dumps(body).encode(),
                    ),
                    timeout=2,
                ) as response:
                    return json.load(response)

            for view, table in (("history", "hn_archive"), ("recent", "hn_current")):
                body = {"q": "database", "view": view}
                first = call(body)
                self.assertEqual(len(first["hits"]["hits"]), 20)
                if view == "recent":
                    body["recent_since"] = first["recent_since"]
                second = call(dict(body, cursor=first["cursor"]))
                self.assertIsNone(second["cursor"])
                self.assertEqual(pages[-1][0], f"/db/v1/tables/{table}/query")
                self.assertEqual(pages[-1][1]["remote_snapshot"], "native2:retained")
                self.assertEqual(pages[-1][1]["search_after"], [1.0, "row-19"])
            first = call({"q": "database"})
            call({"q": "database", "cursor": first["cursor"]})
            self.assertEqual(pages[-1][0], "/db/v1/query")
            self.assertEqual(pages[-1][1]["source_cursor"], "composed-page")
        finally:
            for server in (gateway, backend):
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join()

    def test_arbitrary_queries_sources_and_malformed_input_are_rejected(self):
        for value in (
            [],
            {},
            {"q": " "},
            {"q": "x" * 257},
            {"q": "database", "source": {"table": "private"}},
            {"q": "database", "statement": "DELETE FROM private"},
            {"q": "database", "view": []},
            {"q": "database", "cursor": {}},
            {"q": "database", "cursor": "x" * 65537},
        ):
            with self.subTest(value=str(value)[:80]), self.assertRaises(ValueError):
                search_request(value)

    def test_wire_routes_do_not_proxy_administration_or_invalid_input(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Gateway)
        server.backend = "http://127.0.0.1:1/db/v1"
        server.searches = threading.BoundedSemaphore(8)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            root = f"http://127.0.0.1:{server.server_port}"
            for path, value, expected in (
                ("/db/v1/tables", {}, 404),
                ("/api/search", {"q": "database", "statement": "SELECT 1"}, 400),
            ):
                with self.assertRaises(HTTPError) as caught:
                    urlopen(Request(root + path, json.dumps(value).encode()), timeout=2)
                self.assertEqual(caught.exception.code, expected)
                self.assertNotIn(b"127.0.0.1:1", caught.exception.read())
            with urlopen(root, timeout=2) as response:
                self.assertIn(
                    "frame-ancestors 'none'",
                    response.headers["Content-Security-Policy"],
                )
                self.assertIn(b"Hacker News search", response.read())
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()


def test_public_views_against_native_query_contract(tmp_path):
    import os
    import socket
    import subprocess
    import time

    import pytest
    from poc import create_current_table

    binary = os.environ.get("ANTFLY_NATIVE_BINARY")
    if not binary:
        pytest.skip("set ANTFLY_NATIVE_BINARY for native gateway qualification")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "storage": {
                    "engine": "local",
                    "local": {"base_dir": str(tmp_path / "data")},
                }
            }
        )
    )
    backend = f"http://127.0.0.1:{port}/db/v1"

    def call(path, body=None):
        with urlopen(
            Request(
                backend + path,
                None if body is None else json.dumps(body).encode(),
                {"Content-Type": "application/json"},
            ),
            timeout=30,
        ) as response:
            return None if response.status == 204 else json.load(response)

    log_path = tmp_path / "server.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                str(Path(binary).resolve()),
                "standalone",
                "--config",
                str(config),
                "--data-dir",
                str(tmp_path / "data"),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--health",
                "false",
                "--auth",
                "false",
                "--models-dir",
                str(tmp_path / "models"),
            ],
            stdout=log,
            stderr=log,
        )
        gateway = None
        try:
            deadline = time.monotonic() + 30
            while True:
                assert process.poll() is None, log_path.read_text()[-6000:]
                try:
                    call("/tables")
                    break
                except OSError:
                    assert time.monotonic() < deadline, "daemon did not start"
                    time.sleep(0.1)
            for table in ("hn_archive", "hn_current"):
                create_current_table(
                    lambda method, path, body=None: call(path, body), table
                )
                call(
                    f"/tables/{table}/batch",
                    {
                        "inserts": {
                            f"{table}-{i}": {
                                "hn_id": i,
                                "body": "database search",
                                "title": table,
                                "created_at": int(time.time()),
                                "points": i,
                                "author": "tester",
                                "item_type": "story",
                            }
                            for i in range(25)
                        },
                        "sync_level": "full_index",
                    },
                )
            call(
                "/sources/hackernews",
                {
                    "source": {
                        "union": [{"table": "hn_archive"}, {"table": "hn_current"}]
                    }
                },
            )
            gateway = ThreadingHTTPServer(("127.0.0.1", 0), Gateway)
            gateway.backend = backend
            gateway.searches = threading.BoundedSemaphore(8)
            thread = threading.Thread(target=gateway.serve_forever, daemon=True)
            thread.start()

            def search(body):
                with urlopen(
                    Request(
                        f"http://127.0.0.1:{gateway.server_port}/api/search",
                        json.dumps(body).encode(),
                    ),
                    timeout=30,
                ) as response:
                    return json.load(response)

            for view in ("history", "recent", "all"):
                body = {"q": "database", "view": view}
                first = search(body)
                assert len(first["hits"]["hits"]) == 20
                assert first["cursor"]
                if view == "recent":
                    body["recent_since"] = first["recent_since"]
                second = search(dict(body, cursor=first["cursor"]))
                first_ids = {hit["_id"] for hit in first["hits"]["hits"]}
                assert not first_ids.intersection(
                    hit["_id"] for hit in second["hits"]["hits"]
                )
                if view != "all":
                    assert len(second["hits"]["hits"]) == 5
                    assert second["cursor"] is None
                    assert {
                        hit["_source"]["title"]
                        for hit in first["hits"]["hits"] + second["hits"]["hits"]
                    } == {"hn_archive" if view == "history" else "hn_current"}
                else:
                    assert len(second["hits"]["hits"]) == 20
        finally:
            if gateway is not None:
                gateway.shutdown()
                gateway.server_close()
                thread.join()
            process.terminate()
            try:
                process.wait(20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
