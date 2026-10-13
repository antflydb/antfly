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

"""Read-only HN search gateway. Run beside a private Antfly API."""

import argparse
import base64
import hashlib
import json
import math
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def search_request(value):
    if not isinstance(value, dict) or set(value) - {
        "q",
        "view",
        "cursor",
        "recent_since",
    }:
        raise ValueError("Expected q, view and optional cursor")
    term = value.get("q")
    if not isinstance(term, str) or not 1 <= len(term.strip()) <= 256:
        raise ValueError("Search must contain 1–256 characters")
    tables = {"history": "hn_archive", "recent": "hn_current"}
    view = value.get("view", "all")
    if not isinstance(view, str) or view not in ("all", *tables):
        raise ValueError("Unknown search view")
    query = {
        "full_text_search": {"match": term.strip(), "field": "body"},
        "full_text_index": "body_text",
        "fields": [
            "hn_id",
            "title",
            "author",
            "item_type",
            "points",
            "created_at",
            "body",
        ],
        "highlight": {"fields": ["body"]},
        "order_by": [{"field": "_score", "desc": True}],
        "limit": 20,
    }
    if view == "all":
        query.update(source={"saved": "hackernews"}, source_ranking="rrf")
    else:
        query["table"] = tables[view]
    cursor = value.get("cursor")
    if view == "recent":
        since = value.get("recent_since")
        if since is None:
            if cursor is not None:
                raise ValueError("Recent pagination requires its original cutoff")
            since = max(0, int(time.time()) - 365 * 86400)
        if type(since) is not int or not 0 <= since <= int(time.time()):
            raise ValueError("Invalid recent cutoff")
        query["filter_query"] = {"range": {"path": "/created_at", "gte": since}}
    elif "recent_since" in value:
        raise ValueError("Recent cutoff is only valid for the recent view")
    if cursor is not None:
        if not isinstance(cursor, str) or not 1 <= len(cursor) <= 65536:
            raise ValueError("Invalid cursor")
        if view == "all":
            query["source_cursor"] = cursor
        else:
            try:
                page = json.loads(
                    base64.b64decode(cursor, altchars=b"-_", validate=True)
                )
                if not isinstance(page, dict) or set(page) != {
                    "query",
                    "snapshot",
                    "after",
                }:
                    raise ValueError("Invalid cursor")
                after = page["after"]
                if (
                    page["query"] != cursor_query_key(query)
                    or not isinstance(page["snapshot"], str)
                    or not 1 <= len(page["snapshot"]) <= 65536
                    or not isinstance(after, list)
                    or not 1 <= len(after) <= 4
                    or any(
                        not (
                            (isinstance(item, str) and len(item) <= 8192)
                            or (type(item) in (int, float) and math.isfinite(item))
                        )
                        for item in after
                    )
                ):
                    raise ValueError("Invalid cursor")
            except (ValueError, TypeError, OverflowError) as error:
                raise ValueError("Invalid cursor") from error
            query.update(remote_snapshot=page["snapshot"], search_after=after)
    return query


def cursor_query_key(query):
    # Bind continuation to the fixed table, term and original recent cutoff.
    body = {
        k: v for k, v in query.items() if k not in ("remote_snapshot", "search_after")
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def next_cursor(query, result):
    if "source" in query:
        return result.get("next_source_cursor")
    hits = result["hits"]["hits"]
    if len(hits) < query["limit"]:
        return None
    page = {
        "query": cursor_query_key(query),
        "snapshot": result["remote_snapshot"],
        "after": hits[-1]["_sort"],
    }
    cursor = base64.urlsafe_b64encode(json.dumps(page).encode()).decode()
    if len(cursor) > 65536:
        raise ValueError("Cursor too large")
    return cursor


class Gateway(BaseHTTPRequestHandler):
    # No upstream headers, paths or arbitrary query DSL come from public input.
    def log_message(self, *_):
        pass

    def respond(self, status, body, content_type="application/json"):
        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'",
        )
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        assets = {
            "/": ("index.html", "text/html; charset=utf-8"),
            "/app.js": ("app.js", "text/javascript; charset=utf-8"),
            "/app.css": ("app.css", "text/css; charset=utf-8"),
        }
        if self.path in assets:
            filename, content_type = assets[self.path]
            return self.respond(
                200,
                (Path(__file__).parent / "web" / filename).read_bytes(),
                content_type,
            )
        if self.path == "/healthz":
            return self.respond(200, {"ok": True})
        if self.path == "/readyz":
            try:
                for table in ("hn_archive", "hn_current"):
                    with urlopen(
                        self.server.backend + f"/tables/{table}/indexes/body_text",
                        timeout=3,
                    ) as response:
                        index = json.load(response)
                    if (
                        not index.get("status", {})
                        .get("readiness", {})
                        .get("queryable")
                    ):
                        return self.respond(503, {"ready": False})
                with urlopen(
                    self.server.backend + "/sources/hackernews", timeout=3
                ) as response:
                    json.load(response)
                return self.respond(200, {"ready": True})
            except (URLError, TimeoutError, ValueError):
                return self.respond(503, {"ready": False})
        return self.respond(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/api/search":
            return self.respond(404, {"error": "Not found"})
        self.connection.settimeout(10)
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 131072 or self.headers.get("Transfer-Encoding"):
                raise ValueError("Invalid request length")
            query = search_request(json.loads(self.rfile.read(length)))
        except (ValueError, TimeoutError):
            return self.respond(400, {"error": "Invalid search request"})
        if not self.server.searches.acquire(blocking=False):
            return self.respond(
                429, {"error": "Search capacity reached; retry shortly"}
            )
        try:
            request = Request(
                self.server.backend
                + (f"/tables/{query['table']}/query" if "table" in query else "/query"),
                json.dumps({k: v for k, v in query.items() if k != "table"}).encode(),
                {"Content-Type": "application/json"},
            )
            with urlopen(request, timeout=15) as response:
                raw = response.read(4194305)
            if len(raw) > 4194304:
                return self.respond(502, {"error": "Search response too large"})
            result = json.loads(raw)["responses"][0]
            output = {
                "hits": result["hits"],
                "cursor": next_cursor(query, result),
            }
            if "filter_query" in query:
                output["recent_since"] = query["filter_query"]["range"]["gte"]
            return self.respond(200, output)
        except HTTPError as error:
            if error.code in (409, 410):
                return self.respond(
                    409, {"error": "Search snapshot expired; start a new search"}
                )
            return self.respond(503, {"error": "Search is temporarily unavailable"})
        except (URLError, TimeoutError, ValueError, KeyError, IndexError):
            return self.respond(503, {"error": "Search is temporarily unavailable"})
        finally:
            self.server.searches.release()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", default="http://127.0.0.1:8877/db/v1")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Gateway)
    server.backend = args.backend.rstrip("/")
    server.searches = threading.BoundedSemaphore(8)
    server.serve_forever()


if __name__ == "__main__":
    main()
