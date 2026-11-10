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

"""Stage an immutable released Antfly runtime; never compile server sources."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from urllib.request import Request, urlopen

HERE = Path(__file__).resolve().parent
PIN = json.loads((HERE / "policy.json").read_text())["runtime"]
BASE = "https://releases.antfly.io/antfly/" + PIN["tag"]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url, destination, expected_hash, max_bytes):
    if destination.exists() and sha256(destination) == expected_hash:
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".partial")
    try:
        with (
            urlopen(
                Request(url, headers={"User-Agent": "Antfly-Dictation-Release/1.0"}),
                timeout=60,
            ) as response,
            partial.open("wb") as output,
        ):
            total = 0
            while chunk := response.read(1024 * 1024):
                total += len(chunk)
                if total > max_bytes:
                    raise ValueError("Released Antfly artifact exceeds its pinned size")
                output.write(chunk)
        if sha256(partial) != expected_hash:
            raise ValueError("Released Antfly artifact checksum mismatch")
        os.replace(partial, destination)
    finally:
        partial.unlink(missing_ok=True)


def selected(name):
    return name in {
        "antfly",
        "LICENSE",
        "LICENSING.md",
        "THIRD_PARTY_NOTICES.md",
        "lib/libantfly-apple.dylib",
    } or name.startswith(("LICENSES/", "share/licenses/"))


def extract(archive, destination):
    seen = set()
    total = 0
    with tarfile.open(archive, "r:gz") as source:
        for member in source:
            name = member.name.removeprefix("./")
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts or "\\" in name:
                raise ValueError("Unsafe runtime archive path")
            if not selected(name) or member.isdir():
                continue
            if not member.isfile() or name in seen:
                raise ValueError("Runtime archive contains links or duplicate members")
            seen.add(name)
            total += member.size
            if total > 512 * 1024 * 1024:
                raise ValueError("Extracted runtime exceeds its size limit")
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with source.extractfile(member) as contents, target.open("wb") as output:
                shutil.copyfileobj(contents, output, 1024 * 1024)
            target.chmod(0o755 if name == "antfly" else 0o644)


def payload_digest(directory):
    files = {}
    for path in directory.rglob("*"):
        if path.is_symlink():
            raise ValueError("Runtime payload contains a symlink")
        if path.is_file() and path.relative_to(directory).as_posix() != "release.json":
            files[path.relative_to(directory).as_posix()] = sha256(path)
    return hashlib.sha256(
        json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def verify_staged(directory):
    if json.loads((directory / "release.json").read_bytes()) != PIN:
        raise ValueError("Bundled runtime release differs from the trusted pin")
    if payload_digest(directory) != PIN["payload_sha256"]:
        raise ValueError("Bundled runtime bytes differ from the released payload")
    if not (directory / "antfly").is_file() or not (directory / "LICENSE").is_file():
        raise ValueError("Released runtime or its license is missing")


def stage(root):
    root.mkdir(parents=True, exist_ok=True)
    target = root / "released"
    if target.exists():
        try:
            verify_staged(target)
            return target
        except (ValueError, OSError):
            pass
    cache = root / "downloads"
    ledger = cache / "artifacts.json"
    download(BASE + "/artifacts.json", ledger, PIN["ledger_sha256"], 4 * 1024 * 1024)
    document = json.loads(ledger.read_bytes())
    expected = {"name": PIN["archive"], "sha256": PIN["sha256"], "size": PIN["size"]}
    entries = [
        entry for entry in document["artifacts"] if entry["name"] == PIN["archive"]
    ]
    if (
        document["tag"] != PIN["tag"]
        or document["commit"] != PIN["source_commit"]
        or len(entries) != 1
        or any(entries[0][key] != value for key, value in expected.items())
    ):
        raise ValueError("Released runtime ledger does not match the trusted pin")
    archive = cache / PIN["archive"]
    download(BASE + "/" + PIN["archive"], archive, PIN["sha256"], PIN["size"])
    with tempfile.TemporaryDirectory(dir=root, prefix="stage-") as temporary:
        candidate = Path(temporary) / "runtime"
        candidate.mkdir()
        extract(archive, candidate)
        (candidate / "release.json").write_text(json.dumps(PIN, sort_keys=True) + "\n")
        verify_staged(candidate)
        if target.exists():
            shutil.rmtree(target)
        candidate.rename(target)
    return target


if __name__ == "__main__":
    print("Staged released Antfly:", stage(HERE.parent / ".build/runtime"))
