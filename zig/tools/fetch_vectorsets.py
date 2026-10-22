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

"""Fetch and verify the recall-conformance vector datasets.

The datasets are pinned by SHA-256 in testdata/vectorsets.manifest.json and
cached outside the checkout so every worktree shares one copy. Files already
present with the expected hash are reused; anything missing or corrupt is
downloaded again unless --no-fetch is given, in which case setup fails.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

CHUNK_SIZE = 1 << 20


class FixtureError(Exception):
    pass


@dataclass(frozen=True)
class Fixture:
    name: str
    size: int
    sha256: str


@dataclass(frozen=True)
class Manifest:
    base_url: str
    files: tuple[Fixture, ...]


def load_manifest(path: Path) -> Manifest:
    data = json.loads(path.read_text())
    files = tuple(
        Fixture(name=entry["name"], size=int(entry["size"]), sha256=entry["sha256"])
        for entry in data["files"]
    )
    for fixture in files:
        if "/" in fixture.name or fixture.name in ("", ".", ".."):
            raise FixtureError(f"invalid fixture name in manifest: {fixture.name!r}")
    return Manifest(base_url=data["base_url"], files=files)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_valid(path: Path, fixture: Fixture) -> bool:
    try:
        if path.stat().st_size != fixture.size:
            return False
    except FileNotFoundError:
        return False
    return sha256_file(path) == fixture.sha256


def download(url: str, dest: Path, fixture: Fixture) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{fixture.name}.", dir=dest.parent)
    tmp = Path(tmp_name)
    try:
        digest = hashlib.sha256()
        with (
            os.fdopen(fd, "wb") as out,
            urllib.request.urlopen(url, timeout=120) as response,
        ):
            for chunk in iter(lambda: response.read(CHUNK_SIZE), b""):
                digest.update(chunk)
                out.write(chunk)
        actual = digest.hexdigest()
        if actual != fixture.sha256:
            raise FixtureError(
                f"{fixture.name}: sha256 mismatch from {url}: expected {fixture.sha256}, got {actual}"
            )
        tmp.replace(dest)
    finally:
        tmp.unlink(missing_ok=True)


def ensure(
    manifest: Manifest, dest: Path, allow_fetch: bool, base_url: str | None = None
) -> list[str]:
    """Ensure every manifest file is present and verified; return fetched names."""
    base = base_url or manifest.base_url
    if not base.endswith("/"):
        base += "/"
    fetched: list[str] = []
    for fixture in manifest.files:
        path = dest / fixture.name
        if is_valid(path, fixture):
            continue
        if not allow_fetch:
            state = "corrupt" if path.exists() else "missing"
            raise FixtureError(f"{state} recall fixture {path} (fetching disabled)")
        download(base + fixture.name, path, fixture)
        fetched.append(fixture.name)
    return fetched


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--dest", type=Path, required=True)
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="fail instead of downloading missing files",
    )
    parser.add_argument(
        "--base-url", help="override the manifest base URL (mirrors and tests)"
    )
    args = parser.parse_args(argv)
    try:
        manifest = load_manifest(args.manifest)
        fetched = ensure(
            manifest, args.dest, allow_fetch=not args.no_fetch, base_url=args.base_url
        )
    except (FixtureError, OSError) as err:
        print(f"recall fixtures: {err}", file=sys.stderr)
        return 1
    if fetched:
        print(
            f"recall fixtures: fetched {len(fetched)} file(s) into {args.dest}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
