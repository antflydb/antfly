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
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request

from managed_api import authorization_headers, open_request, validate_backend


class RateLimit:
    """One bounded token bucket per gateway process, independent of client input."""

    def __init__(self, rate, burst):
        self.rate, self.burst = rate, burst
        self.tokens, self.updated = float(burst), time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            self.tokens = min(
                self.burst, self.tokens + (now - self.updated) * self.rate
            )
            self.updated = now
            if self.tokens < 1:
                return False
            self.tokens -= 1
            return True


def configure(server, backend, headers=None, concurrency=8, rate=2, burst=8):
    server.backend = validate_backend(backend, bool(headers))
    server.backend_headers = dict(headers or {})
    server.searches = threading.BoundedSemaphore(concurrency)
    server.rate_limit = RateLimit(rate, burst)
    server.ready_lock = threading.Lock()
    server.ready_until, server.ready_value = 0.0, False


def search_request(value):
    if not isinstance(value, dict) or set(value) - {
        "q",
        "view",
        "cursor",
        "recent_since",
        "type",
        "sort",
        "date_from",
        "date_to",
    }:
        raise ValueError("Expected q, view and optional cursor")
    term = value.get("q")
    if not isinstance(term, str) or not 1 <= len(term.strip()) <= 256:
        raise ValueError("Search must contain 1–256 characters")
    tables = {"history": "hn_archive", "recent": "hn_current"}
    view = value.get("view", "all")
    if not isinstance(view, str) or view not in ("all", *tables):
        raise ValueError("Unknown search view")
    item_type = value.get("type", "all")
    sort = value.get("sort", "relevance")
    if not isinstance(item_type, str) or item_type not in ("all", "story", "comment"):
        raise ValueError("Unknown item type")
    if not isinstance(sort, str) or sort not in ("relevance", "date"):
        raise ValueError("Unknown sort order")
    bounds = {}
    for name, operator in (("date_from", "gte"), ("date_to", "lt")):
        if name in value:
            bound = value[name]
            if type(bound) is not int or not 0 <= bound <= 253402300800:
                raise ValueError("Invalid date boundary")
            bounds[operator] = bound
    if "gte" in bounds and "lt" in bounds and bounds["gte"] >= bounds["lt"]:
        raise ValueError("Date range must start before it ends")
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
    if sort == "date":
        query["order_by"] = [
            {"field": "created_at", "desc": True},
            {"field": "hn_id", "desc": True},
        ]
    if view == "all":
        query["source"] = {"saved": "hackernews"}
        if sort == "relevance":
            query["source_ranking"] = "rrf"
    else:
        query["table"] = tables[view]
    predicates = []
    cursor = value.get("cursor")
    if view == "recent":
        since = value.get("recent_since")
        if since is None:
            if cursor is not None:
                raise ValueError("Recent pagination requires its original cutoff")
            since = max(0, int(time.time()) - 365 * 86400)
        if type(since) is not int or not 0 <= since <= int(time.time()):
            raise ValueError("Invalid recent cutoff")
        predicates.append({"range": {"path": "/created_at", "gte": since}})
    elif "recent_since" in value:
        raise ValueError("Recent cutoff is only valid for the recent view")
    if bounds:
        predicates.append({"range": {"path": "/created_at", **bounds}})
    if item_type != "all":
        predicates.append({"term": {"path": "/item_type", "value": item_type}})
    if predicates:
        query["filter_query"] = (
            predicates[0] if len(predicates) == 1 else {"conjuncts": predicates}
        )
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

    def upstream(self, path, body=None, timeout=15):
        headers = {"Content-Type": "application/json"}
        headers.update(getattr(self.server, "backend_headers", {}))
        request = Request(
            self.server.backend + path,
            None if body is None else json.dumps(body).encode(),
            headers,
        )
        with open_request(request, timeout=timeout) as response:
            raw = response.read(4194305)
        if len(raw) > 4194304:
            raise ValueError("Search response too large")
        return json.loads(raw)

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
        # Search state lives in the browser's query string, not the asset route.
        path = self.path.partition("?")[0]
        assets = {
            "/": ("index.html", "text/html; charset=utf-8"),
            "/app.js": ("app.js", "text/javascript; charset=utf-8"),
            "/search-state.mjs": ("search-state.mjs", "text/javascript; charset=utf-8"),
            "/app.css": ("app.css", "text/css; charset=utf-8"),
        }
        if path in assets:
            filename, content_type = assets[path]
            return self.respond(
                200,
                (Path(__file__).parent / "web" / filename).read_bytes(),
                content_type,
            )
        if path in ("/health", "/healthz"):
            return self.respond(200, {"ok": True})
        if path in ("/ready", "/readyz"):
            lock = getattr(self.server, "ready_lock", None)
            if lock is None:
                ready = self.check_readiness()
            elif time.monotonic() < self.server.ready_until:
                ready = self.server.ready_value
            elif lock.acquire(blocking=False):
                try:
                    ready = self.check_readiness()
                    self.server.ready_value = ready
                    self.server.ready_until = time.monotonic() + 10
                finally:
                    lock.release()
            else:
                ready = False
            return self.respond(200 if ready else 503, {"ready": ready})
        return self.respond(404, {"error": "Not found"})

    def check_readiness(self):
        try:
            for table in ("hn_archive", "hn_current"):
                index = self.upstream(f"/tables/{table}/indexes/body_text", timeout=3)
                status = index.get("status") if isinstance(index, dict) else None
                readiness = (
                    status.get("readiness") if isinstance(status, dict) else None
                )
                if not isinstance(readiness, dict) or not readiness.get("queryable"):
                    return False
            self.upstream("/sources/hackernews", timeout=3)
            return True
        except HTTPError as error:
            error.close()
            return False
        except (URLError, TimeoutError, ValueError):
            return False

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
        limiter = getattr(self.server, "rate_limit", None)
        if limiter is not None and not limiter.acquire():
            return self.respond(429, {"error": "Search rate reached; retry shortly"})
        if not self.server.searches.acquire(blocking=False):
            return self.respond(
                429, {"error": "Search capacity reached; retry shortly"}
            )
        try:
            result = self.upstream(
                f"/tables/{query['table']}/query" if "table" in query else "/query",
                {k: v for k, v in query.items() if k != "table"},
            )["responses"][0]
            output = {
                "hits": result["hits"],
                "cursor": next_cursor(query, result),
            }
            if query.get("table") == "hn_current":
                predicate = query["filter_query"]
                dates = predicate.get("range") or predicate["conjuncts"][0]["range"]
                output["recent_since"] = dates["gte"]
            return self.respond(200, output)
        except HTTPError as error:
            error.close()
            if error.code in (409, 410):
                return self.respond(
                    409, {"error": "Search snapshot expired; start a new search"}
                )
            return self.respond(503, {"error": "Search is temporarily unavailable"})
        except (URLError, TimeoutError, ValueError, KeyError, IndexError, TypeError):
            return self.respond(503, {"error": "Search is temporarily unavailable"})
        finally:
            self.server.searches.release()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend",
        default=os.environ.get("ANTFLY_API_BASE", "http://127.0.0.1:8877/db/v1"),
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")))
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--requests-per-second", type=float, default=2)
    parser.add_argument("--burst", type=int, default=8)
    parser.add_argument("--require-auth", action="store_true")
    args = parser.parse_args()
    if not (
        1 <= args.concurrency <= 32
        and 0 < args.requests_per_second <= 100
        and 1 <= args.burst <= 256
    ):
        parser.error("Invalid gateway capacity bounds")
    try:
        headers = authorization_headers()
        if args.require_auth and not headers:
            raise ValueError("Managed hosting requires Antfly authorization")
        backend = validate_backend(args.backend, bool(headers))
    except ValueError as error:
        parser.error(str(error))
    server = ThreadingHTTPServer((args.host, args.port), Gateway)
    configure(
        server, backend, headers, args.concurrency, args.requests_per_second, args.burst
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
