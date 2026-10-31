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

"""Check full-text drain and durable visibility after a retained load fixture."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from lite_contention import sdk


def verify(library: str, database: Path) -> None:
    af = sdk(library)
    nonce = time.time_ns()
    key = f"qualification-canary-{nonce}"
    term = f"qualificationcanary{nonce}"
    request = {
        "full_text_search": {"match": {"field": "text", "text": term}},
        "limit": 10,
    }
    with af.open(database, busy_timeout=5.0) as db:
        before = db.check()
        assert isinstance(before, dict) and before.get("valid") is True, before
        began = time.monotonic()
        db.run_until_idle()
        drain_seconds = time.monotonic() - began
        db.batch_json({"inserts": {key: {"text": term, "nonce": nonce}}})
        began = time.monotonic()
        db.run_until_idle()
        canary_seconds = time.monotonic() - began
        assert key in json.dumps(db.search(request))
        assert db.lookup(key)["nonce"] == nonce
        pending = db.pending_work_stats()
        debts = pending["index_replay_debt"]
        assert debts, pending
        for debt in debts:
            assert debt["catch_up_required"] is False, debt
            assert debt["applied_sequence"] >= debt["target_sequence"], debt
    with af.open(database, busy_timeout=5.0) as db:
        assert key in json.dumps(db.search(request))
        assert db.lookup(key)["nonce"] == nonce
        integrity = db.check()
        assert isinstance(integrity, dict) and integrity.get("valid") is True, integrity
    print(
        json.dumps(
            {
                "kind": "indexed_after_load",
                "status": "ok",
                "database": str(database),
                "drain_seconds": drain_seconds,
                "canary_seconds": canary_seconds,
                "canary_survives_reopen": True,
                "pending": pending,
                "integrity": integrity,
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument("--database", type=Path, required=True)
    args = parser.parse_args()
    verify(args.library, args.database)
