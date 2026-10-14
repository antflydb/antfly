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

"""Capture stopped Codex rollout files and rewrite only the importer's DB copy.

Run immediately after prepare.py while Codex remains stopped. This operation
copies the history named by rollouts.json; it does not import or switch stores.
"""

import argparse
import json
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

from prepare import digest, write_json


def capture(bundle, codex_home, source_quiesced):
    if not source_quiesced:
        raise ValueError(
            "stop Codex and explicitly assert --source-quiesced; file copies cannot create a common live source cut"
        )
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    files = json.loads((bundle / "rollouts.json").read_text())
    state_entry = manifest["sources"]["state"]
    state = bundle / state_entry.get("snapshot_path", state_entry["filename"])
    if digest(state) != manifest["sources"]["state"]["sha256"]:
        raise ValueError("captured state database changed")
    destination = bundle / "captured-codex-home"
    if destination.exists():
        raise ValueError("history capture destination must be new")
    destination.mkdir(mode=0o700)
    home = codex_home.resolve()
    for entry in files:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("invalid history manifest path")
        source = home / relative
        source.resolve().relative_to(home)
        before = source.stat()
        if (before.st_size, before.st_mtime_ns) != (
            entry["bytes"],
            entry["mtime_ns"],
        ):
            raise ValueError(
                "a rollout changed since SQLite capture; generate a fresh bundle while Codex is stopped"
            )
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        shutil.copy2(source, target)
        target.chmod(0o600)
        after = source.stat()
        if (before.st_size, before.st_mtime_ns) != (
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ValueError("a rollout changed during capture")
    captured_state = destination / state.name
    shutil.copy2(state, captured_state)
    with closing(sqlite3.connect(captured_state)) as db:
        for thread_id, original in db.execute(
            "SELECT id,rollout_path FROM threads"
        ).fetchall():
            relative = Path(original).resolve().relative_to(home)
            target = destination / relative
            if not target.is_file():
                raise ValueError(
                    "a thread's selected rollout is absent from the history capture"
                )
            # Preserve the immutable relational source. Only this separate
            # importer input changes, keeping SQLite's selected fork/revert
            # tip addressable at the corresponding captured path.
            db.execute(
                "UPDATE threads SET rollout_path=? WHERE id=?",
                (str(target), thread_id),
            )
        db.commit()
    manifest["history"]["capture_required"] = False
    manifest["history"]["captured_home"] = "captured-codex-home"
    manifest["capture_consistency"] = (
        "operator asserted Codex stopped throughout database and rollout capture"
    )
    manifest["blockers"] = [
        "full history import and application acceptance have not run"
    ]
    write_json(manifest_path, manifest)
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--codex-home", type=Path, required=True)
    parser.add_argument("--source-quiesced", action="store_true")
    args = parser.parse_args()
    print(
        capture(args.bundle.resolve(), args.codex_home.resolve(), args.source_quiesced)
    )


if __name__ == "__main__":
    main()
