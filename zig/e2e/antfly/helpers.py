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

import json
import time
from typing import Callable


def json_doc(**fields) -> str:
    return json.dumps(fields, separators=(",", ":"), sort_keys=True)


def upsert(doc_id: str, body: str) -> dict:
    return {"kind": "upsert", "doc_id": doc_id, "body": body}


def assert_single_top_hit(payload: dict, doc_id: str) -> None:
    hits = payload["hits"]
    assert len(hits) >= 1
    assert hits[0]["doc_id"] == doc_id


def wait_until(
    fn: Callable[[], dict | None],
    *,
    timeout_s: float,
    interval_s: float = 1.0,
) -> dict | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(interval_s)
    return None
