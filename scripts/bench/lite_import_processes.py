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

"""Measure one short-lived Codex importer process per thread on one Lite file.

Private source data stays local. Detailed importer output goes to adjacent logs;
stdout contains JSON timings, file growth and unlocked lease-marker counts.
Use the same frozen CODEX_HOME, importer and ordered thread list for both builds.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


MARKER = re.compile(r"\.aflite-lease-[0-9a-f]{32}\Z")


def storage_state(database: Path) -> dict[str, int]:
    markers = 0
    unlocked = 0
    for path in database.parent.glob(".aflite-lease-*"):
        if not MARKER.fullmatch(path.name):
            continue
        try:
            with path.open("rb") as marker:
                markers += 1
                try:
                    fcntl.flock(marker, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                unlocked += 1
        except FileNotFoundError:
            continue
    return {
        "file_bytes": database.stat().st_size if database.exists() else 0,
        "lease_markers": markers,
        "unlocked_lease_markers": unlocked,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--importer", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--codex-home", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--thread", action="append", required=True)
    parser.add_argument("--append", action="store_true")
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    for path in (args.importer, args.library):
        if not path.is_file():
            parser.error(f"required executable or library is missing: {path}")
    fingerprints = {
        "importer_sha256": hashlib.sha256(args.importer.read_bytes()).hexdigest(),
        "library_sha256": hashlib.sha256(args.library.read_bytes()).hexdigest(),
    }
    database = args.database.resolve()
    if database.exists() and not args.append:
        parser.error(
            "database already exists; use --append to measure follow-on processes"
        )
    database.parent.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    library_directory = str(args.library.resolve().parent)
    for name in ("DYLD_LIBRARY_PATH", "LD_LIBRARY_PATH"):
        environment[name] = os.pathsep.join(
            filter(None, (library_directory, environment.get(name)))
        )
    for ordinal, thread in enumerate(args.thread):
        log = database.parent / f"{database.stem}-import-{ordinal:03d}-{thread}.log"
        before = storage_state(database)
        started = time.monotonic()
        with log.open("w") as output:
            try:
                result = subprocess.run(
                    [
                        str(args.importer.resolve()),
                        "--codex-home",
                        str(args.codex_home.resolve()),
                        "--thread",
                        thread,
                        "--no-semantic",
                        "--to",
                        str(database),
                        "--verbose",
                    ],
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=args.timeout,
                    check=False,
                )
                returncode = result.returncode
            except subprocess.TimeoutExpired:
                returncode = "timeout"
        elapsed = time.monotonic() - started
        after = storage_state(database)
        print(
            json.dumps(
                {
                    **fingerprints,
                    "thread": thread,
                    "elapsed_seconds": elapsed,
                    "returncode": returncode,
                    "before": before,
                    "after": after,
                    "growth_bytes": after["file_bytes"] - before["file_bytes"],
                    "log": str(log),
                }
            ),
            flush=True,
        )
        if returncode != 0:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
