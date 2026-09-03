# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Shared helpers for antfly-zig E2E tests."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from socketserver import BaseServer

import requests

HTTP_SERVER_POLL_INTERVAL_S = 0.02


def start_http_server(server: BaseServer) -> threading.Thread:
    """Start a test HTTP server without Python's 500ms shutdown latency."""
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": HTTP_SERVER_POLL_INTERVAL_S},
        daemon=True,
    )
    thread.start()
    return thread


def json_doc(**fields: object) -> dict[str, object]:
    """Construct a public JSON document without an intermediate encoding."""
    return fields


def upsert(doc_id: str, document: dict[str, object]) -> dict[str, object]:
    """Construct the canonical structural table-upsert mutation."""
    return {"kind": "upsert", "doc_id": doc_id, "document": document}


def assert_single_top_hit(payload: dict, doc_id: str) -> None:
    hits = payload["hits"]
    assert len(hits) >= 1
    assert hits[0]["doc_id"] == doc_id


def assert_created_index(created: dict, name: str, index_type: str) -> None:
    assert created["name"] == name
    assert created["type"] == index_type


def create_index_payload(payload: dict, index_name: str) -> dict:
    """Return the path-owned create payload without mutating the caller's value."""
    body = payload.copy()
    repeated_name = body.pop("name", index_name)
    assert repeated_name == index_name, "index payload name must match the path name"
    return body


def query_hits_total_value(hits: dict) -> int:
    total = hits["total"]
    assert isinstance(total, dict), (
        f"expected structured hits.total, got {type(total).__name__}"
    )
    assert total.get("relation") in {"exact", "gte"}
    return int(total["value"])


def wait_until(
    fn: Callable[[], dict | None],
    *,
    timeout_s: float,
    interval_s: float = 1.0,
) -> dict | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            result = fn()
        except requests.HTTPError as err:
            response = err.response
            if (
                response is not None
                and response.status_code == 503
                and "doc identity unavailable" in response.text
            ):
                result = None
            else:
                raise
        if result:
            return result
        time.sleep(interval_s)
    return None
