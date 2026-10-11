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
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).parents[1]))
from serve import Gateway, search_request


class SearchGatewayTest(unittest.TestCase):
    def test_views_and_cursor_keep_fixed_search_contract(self):
        for view, source in (
            ("all", {"saved": "hackernews"}),
            ("history", {"table": "hn_archive"}),
            ("recent", {"table": "hn_current"}),
        ):
            request = {"q": " database ", "view": view, "cursor": "opaque"}
            if view == "recent":
                request["recent_since"] = 1700000000
            query = search_request(request)
            self.assertEqual(query["source"], source)
            self.assertEqual(query["source_cursor"], "opaque")
            self.assertEqual(query["full_text_search"]["match"], "database")
            self.assertEqual(query["limit"], 20)

    def test_recent_pagination_keeps_its_original_cutoff(self):
        first = search_request({"q": "database", "view": "recent"})
        cutoff = first["filter_query"]["range"]["gte"]
        second = search_request(
            {
                "q": "database",
                "view": "recent",
                "cursor": "opaque",
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
