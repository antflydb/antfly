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

"""Exchange CI files through private, create-only GCS objects and small receipts.

This is temporary transport, never a release store. R2 owns durable releases.
Fourteen-day namespace windows plus 30-day (ordinary) or 120-day (replay)
lifecycles cover the full fourteen/ninety-day receipt lifetime, including reuse.
"""

from __future__ import annotations

import argparse
import fnmatch
import glob
import hashlib
from http.client import HTTPException
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import tempfile
import time
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

BUCKET = "antfly-image-artifacts-ci-artifacts"
WINDOW_SECONDS = 14 * 86400
RECEIPT_PREFIX = "_ci-artifact-"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("CI storage redirects are forbidden")


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def safe_name(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", name) or name in {".", ".."}:
        raise ValueError("invalid artifact name")
    return name


def safe_path(name: str) -> str:
    path = PurePosixPath(name)
    if (
        not name
        or path.is_absolute()
        or ".." in path.parts
        or "\\" in name
        or str(path) != name
        or name.startswith(RECEIPT_PREFIX)
    ):
        raise ValueError("invalid artifact member path")
    return name


class GCS:
    def __init__(self, token: str):
        if not token:
            raise ValueError("GCS access token is required")
        self.token = token

    def request(self, key, *, data=None, method="GET"):
        headers = {"Authorization": "Bearer " + self.token}
        if method == "POST":
            url = f"https://storage.googleapis.com/upload/storage/v1/b/{BUCKET}/o?"
            url += urlencode(
                {"uploadType": "media", "name": key, "ifGenerationMatch": 0}
            )
            headers.update(
                {
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(os.fstat(data.fileno()).st_size),
                }
            )
        else:
            url = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/{quote(key, safe='')}?alt=media"
        # Never follow redirects with an access token to another endpoint.
        return build_opener(NoRedirect()).open(
            Request(url, data=data, headers=headers, method=method), timeout=300
        )

    def put(self, key: str, path: Path):
        for attempt in range(3):
            try:
                with (
                    path.open("rb") as stream,
                    self.request(key, data=stream, method="POST") as response,
                ):
                    response.read()
                return
            except HTTPError as exc:
                if exc.code in {403, 412}:
                    # A create-only identity may receive either a precondition
                    # failure or an overwrite-permission denial for an existing
                    # object. Accept neither without verifying the actual bytes.
                    try:
                        with self.request(key) as stream:
                            actual = hashlib.file_digest(stream, "sha256").hexdigest()
                    except HTTPError:
                        raise exc
                    if actual != digest(path):
                        raise ValueError(
                            "existing CI object differs from its digest"
                        ) from exc
                    return
                if exc.code not in {408, 429, 500, 502, 503, 504} or attempt == 2:
                    raise
                time.sleep(2**attempt)
            except (OSError, HTTPException):
                if attempt == 2:
                    raise
                time.sleep(2**attempt)

    def get(self, key: str, path: Path):
        for attempt in range(3):
            try:
                with self.request(key) as stream, path.open("wb") as out:
                    shutil.copyfileobj(stream, out, length=1024 * 1024)
                return
            except HTTPError as exc:
                if exc.code not in {408, 429, 500, 502, 503, 504} or attempt == 2:
                    raise
                time.sleep(2**attempt)
            except (OSError, HTTPException):
                if attempt == 2:
                    raise
                time.sleep(2**attempt)


def inventory(patterns: str) -> list[tuple[str, Path]]:
    roots = []
    for pattern in patterns.splitlines():
        if not pattern.strip():
            continue
        if pattern.lstrip().startswith("!"):
            raise ValueError("artifact exclusions are not supported")
        roots.extend(Path(p) for p in glob.glob(pattern.strip(), recursive=True))
    if not roots:
        raise ValueError("artifact paths matched no files")
    files = set()
    bases = []
    for root in roots:
        if root.is_symlink():
            raise ValueError("artifact symlinks are forbidden")
        bases.append(root if root.is_dir() else root.parent)
        for path in root.rglob("*") if root.is_dir() else [root]:
            if path.is_symlink():
                raise ValueError("artifact symlinks are forbidden")
            if path.is_file():
                files.add(path.absolute())
    base = Path(os.path.commonpath([p.absolute() for p in bases]))
    result = [
        (safe_path(path.relative_to(base).as_posix()), path) for path in sorted(files)
    ]
    if not result:
        raise ValueError("artifact contains no files")
    return result


def upload(
    store,
    *,
    patterns,
    name,
    repository,
    run_id,
    destination,
    namespace="release",
    retention_days=14,
    now=None,
):
    safe_name(name)
    if namespace not in {"release", "vopr"} or retention_days not in {14, 90}:
        raise ValueError("invalid CI namespace or retention")
    epoch = int((time.time() if now is None else now) // WINDOW_SECONDS)
    tier = "short" if retention_days == 14 else "replay"
    entries = []
    for member, path in inventory(patterns):
        checksum = digest(path)
        key = f"{namespace}/{tier}/{epoch}/{checksum}"
        store.put(key, path)
        if digest(path) != checksum:
            raise ValueError("CI artifact changed during upload")
        entries.append(
            {
                "path": member,
                "sha256": checksum,
                "size": path.stat().st_size,
                "key": key,
            }
        )
    destination.mkdir(parents=True, exist_ok=True)
    receipt = {
        "schema_version": 1,
        "repository": repository,
        "run_id": str(run_id),
        "name": name,
        "namespace": namespace,
        "files": entries,
    }
    (destination / f"{RECEIPT_PREFIX}{name}.json").write_text(
        json.dumps(receipt, sort_keys=True) + "\n"
    )


def restore(
    store,
    *,
    staging,
    destination,
    repository,
    run_id,
    name="",
    pattern="",
    namespace="release",
):
    if namespace not in {"release", "vopr"} or bool(name) == bool(pattern):
        raise ValueError("select one artifact name or pattern and a valid namespace")
    receipts = sorted(staging.glob(f"{RECEIPT_PREFIX}*.json"))
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise ValueError("artifact destination must be empty")
    # Historical runs and explicitly disabled GCS uploads remain readable.
    if not receipts:
        if not any(path.is_file() for path in staging.rglob("*")):
            raise ValueError("downloaded artifact contains no files")
        for path in staging.rglob("*"):
            if path.is_symlink():
                raise ValueError("artifact symlinks are forbidden")
        shutil.copytree(staging, destination, dirs_exist_ok=True)
        return
    members = {}
    for path in receipts:
        receipt = json.loads(path.read_text())
        artifact = safe_name(receipt.get("name", ""))
        if (
            receipt.get("schema_version") != 1
            or receipt.get("repository") != repository
            or receipt.get("namespace") != namespace
            or receipt.get("run_id") != str(run_id)
            or path.name != f"{RECEIPT_PREFIX}{artifact}.json"
            or (name and artifact != name)
            or (pattern and not fnmatch.fnmatchcase(artifact, pattern))
        ):
            raise ValueError("CI receipt producer or artifact identity differs")
        files = receipt.get("files")
        if not isinstance(files, list) or not files:
            raise ValueError("CI receipt contains no files")
        for entry in files:
            member = safe_path(entry["path"])
            checksum = entry["sha256"]
            if (
                not re.fullmatch(r"[0-9a-f]{64}", checksum)
                or not re.fullmatch(
                    namespace + r"/(short|replay)/[0-9]+/" + checksum, entry["key"]
                )
                or type(entry["size"]) is not int
                or entry["size"] < 0
            ):
                raise ValueError("invalid CI object identity")
            if member in members:
                raise ValueError("duplicate CI artifact member")
            members[member] = entry
    # Mixing legacy archive files and receipts must fail closed, not silently
    # omit files when a controller changes during a matrix build.
    if {p for p in staging.rglob("*") if p.is_file()} != set(receipts):
        raise ValueError("mixed CI receipts and legacy artifact files")
    for member, entry in members.items():
        target = destination / member
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=target.parent, prefix=".ci-transfer-", delete=False
        ) as temporary:
            partial = Path(temporary.name)
        try:
            store.get(entry["key"], partial)
            if (
                partial.stat().st_size != entry["size"]
                or digest(partial) != entry["sha256"]
            ):
                raise ValueError("CI object content differs from receipt")
            partial.replace(target)
        finally:
            partial.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("upload", "restore"))
    parser.add_argument("--paths", default="")
    parser.add_argument("--name", default="")
    parser.add_argument("--pattern", default="")
    parser.add_argument("--namespace", choices=("release", "vopr"), default="release")
    parser.add_argument("--retention-days", type=int, choices=(14, 90), default=14)
    parser.add_argument(
        "--repository", default=os.environ.get("GITHUB_REPOSITORY"), required=False
    )
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument("--staging", type=Path)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    if not args.repository or not re.fullmatch(r"[1-9][0-9]*", str(args.run_id)):
        parser.error("repository and numeric run identity are required")
    # Legacy downloads do not require Google credentials.
    token = os.environ.get("CI_ARTIFACT_TOKEN", "")
    store = GCS(token) if token else None
    if args.command == "upload":
        if store is None:
            parser.error("upload requires GCS authentication")
        upload(
            store,
            patterns=args.paths,
            name=args.name,
            repository=args.repository,
            run_id=args.run_id,
            destination=args.destination,
            namespace=args.namespace,
            retention_days=args.retention_days,
        )
    else:
        restore(
            store,
            staging=args.staging,
            destination=args.destination,
            repository=args.repository,
            run_id=args.run_id,
            name=args.name,
            pattern=args.pattern,
            namespace=args.namespace,
        )


if __name__ == "__main__":
    main()
