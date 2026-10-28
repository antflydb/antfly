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

"""Resumable initial recent-table seed without permanently filtering old edits."""

import hashlib
from itertools import islice
import json
from pathlib import Path


def seed_current(state, filenames, since, publish, batch_size=1000):
    if since <= 0 or not 1 <= batch_size <= 16384:
        raise ValueError("Invalid seed boundary or batch size")
    files = []
    for filename in filenames:
        with Path(filename).open("rb") as source:
            files.append(hashlib.file_digest(source, "sha256").hexdigest())
    binding = json.dumps({"since": since, "files": files}, sort_keys=True)
    previous = state.get("initial_current_seed", "")
    if previous and previous != binding:
        raise RuntimeError(
            "Seed inventory or boundary changed; restore the original inputs"
        )
    if not previous and (
        state.get("native_changes_checkpoint", "")
        or state.get("native_changes_request", "")
    ):
        raise RuntimeError(
            "Initial seed requires a writer that has not published changes"
        )
    state.set("initial_current_seed", binding)
    if state.get("initial_current_seed_complete", ""):
        return {"seeded": True, "resumed": True}
    state.backfill(filenames, batch_size=batch_size)
    after = ""
    while batch := list(islice(state.db.entries("change:", after), batch_size)):
        with state.transaction():
            for key, _ in batch:
                payload = state.item(int(key.split(":", 1)[1]))["payload"]
                created = int(payload.get("time", payload.get("created_at", 0)) or 0)
                if created <= 0:
                    raise RuntimeError("Seed row has no original creation time")
                if created < since:
                    state.db.delete(key)
        after = batch[-1][0]
    while next(state.db.entries("change:"), None) is not None:
        publish()
    state.set("initial_current_seed_complete", "1")
    return {"seeded": True, "resumed": bool(previous)}
