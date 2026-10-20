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

"""Seed pinned source packages from the verified mirror or upstream.

Only Zig's expected package hash admits content. This caches sources, never
compiler output. Run after selecting ZIG_GLOBAL_CACHE_DIR and the compiler.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen
import subprocess
import tarfile
import tempfile

MANIFEST = Path(__file__).with_name("zig-dependencies.json")
MIRROR = "https://storage.googleapis.com/antfly-image-artifacts-zig-dependencies"
MIRROR_TOKEN_ENV = "ANTFLY_ZIG_MIRROR_TOKEN"


class MirrorRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if (
            not newurl.startswith(MIRROR + "/")
            or urlsplit(newurl)[:2] != urlsplit(req.full_url)[:2]
        ):
            raise ValueError("refusing authenticated mirror redirect to another origin")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def load_manifest(path: Path = MANIFEST) -> dict:
    manifest = json.loads(path.read_text())
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported Zig dependency inventory")
    hashes = set()
    for package in manifest["packages"]:
        identity = package["hash"]
        if (
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]+", identity)
            or identity in hashes
        ):
            raise ValueError("invalid or duplicate Zig package hash")
        if not package["url"].startswith("https://"):
            raise ValueError("Zig upstream packages require HTTPS")
        hashes.add(identity)
    return manifest


def download(source: str, target: Path) -> None:
    # Never send a cloud token to upstream, even when fallback is required.
    token = (
        os.environ.get(MIRROR_TOKEN_ENV) if source.startswith(MIRROR + "/") else None
    )
    request = (
        Request(source, headers={"Authorization": "Bearer " + token})
        if token
        else source
    )
    open_source = build_opener(MirrorRedirects()).open if token else urlopen
    for attempt in range(3):
        try:
            deadline = time.monotonic() + 120
            size = 0
            with (
                open_source(request, timeout=30) as response,
                target.open("wb") as output,
            ):
                while chunk := response.read(64 * 1024):
                    size += len(chunk)
                    if size > 64 * 1024 * 1024 or time.monotonic() > deadline:
                        raise ValueError(
                            "dependency download exceeds its size/time budget"
                        )
                    output.write(chunk)
            return
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            if isinstance(exc, HTTPError):
                exc.close()
                if exc.code != 429 and exc.code < 500:
                    raise
            if attempt == 2:
                raise
            time.sleep(attempt + 1)


def fetch_verified(
    zig: str,
    source: str,
    identity: str,
    package_dir: Path,
    archive_path: Path | None = None,
) -> Path:
    package_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="download-", dir=package_dir) as temp:
        archive = str(Path(source).resolve())
        if source.startswith("https://"):
            downloaded = archive_path or Path(temp) / "source.tar.gz"
            download(source, downloaded)
            archive = str(downloaded)
        # Use an isolated scratch project with --save so Zig materializes the
        # package root before recompressing it into the global archive cache.
        # Zig 0.17's no-save path otherwise recompresses the enclosing temp
        # directory, leaving an extra root that fails a later build's hash.
        scratch = Path(temp)
        (scratch / "build.zig").write_text("pub fn build() void {}\n")
        zon = scratch / "build.zig.zon"
        zon.write_text(
            '.{ .name = .antfly_zig, .version = "0.0.1", .fingerprint = 0xaf6720431d9566e1, .paths = .{"build.zig", "build.zig.zon"}, .dependencies = .{}, }\n'
        )
        subprocess.run(
            [
                zig,
                "fetch",
                "--save=verified_source",
                "--pkg-dir",
                str(scratch / "packages"),
                archive,
            ],
            cwd=scratch,
            env={
                **{
                    key: value
                    for key, value in os.environ.items()
                    if key != MIRROR_TOKEN_ENV
                },
                "ZIG_GLOBAL_CACHE_DIR": str(package_dir.parent),
            },
            check=True,
            capture_output=True,
            text=True,
            timeout=600,
        )
        hashes = re.findall(r'\.hash\s*=\s*"([^"\n]+)"', zon.read_text())
        if hashes != [identity]:
            raise ValueError(f"Zig package identity mismatch for {source}")
    cached = cached_package(package_dir, identity)
    if cached is None:
        raise ValueError(f"Zig did not cache the admitted package: {identity}")
    return cached


def cached_package(package_dir: Path, identity: str) -> Path | None:
    # Zig 0.17 keeps canonical archives here and materializes project packages
    # separately. Accept directories too when inspecting an unpacked fixture.
    for candidate in (package_dir / (identity + ".tar.gz"), package_dir / identity):
        if candidate.exists():
            return candidate
    return None


def check_remote_declarations(root: Path, manifest: dict) -> None:
    admitted = {
        (url, p["hash"])
        for p in manifest["packages"]
        for url in [p["url"], *p.get("declared_urls", [])]
    }

    def check(text: str, location: str) -> None:
        urls = re.findall(r'\.url\s*=\s*"([^"\n]+)"', text)
        pairs = re.findall(
            r'\.url\s*=\s*"([^"\n]+)"\s*,\s*\.hash\s*=\s*"([^"\n]+)"', text
        )
        if len(urls) != len(pairs) or any(pair not in admitted for pair in pairs):
            raise ValueError(
                f"remote package missing from pinned dependency inventory: {location}"
            )

    if root.is_file():
        with tarfile.open(root) as archive:
            root_manifest = root.name.removesuffix(".tar.gz") + "/build.zig.zon"
            found_root_manifest = False
            for member in archive:
                if member.isfile() and Path(member.name).name == "build.zig.zon":
                    found_root_manifest |= member.name == root_manifest
                    if member.size > 1024 * 1024:
                        raise ValueError("dependency manifest is unexpectedly large")
                    with archive.extractfile(member) as stream:
                        check(stream.read().decode(), member.name)
            if not found_root_manifest:
                raise ValueError(f"cached package has an invalid root: {root}")
    elif root.is_dir():
        for path in root.rglob("build.zig.zon"):
            if not any(part.startswith(".") for part in path.relative_to(root).parts):
                check(path.read_text(), str(path))
    else:
        raise ValueError(f"verified dependency source is missing: {root}")


def prepare(
    zig: str,
    manifest: dict,
    cache: Path,
    seed: Path | None = None,
    mirror_only: bool = False,
    offline: bool = False,
) -> None:
    packages = cache / "p"
    packages.mkdir(parents=True, exist_ok=True)
    for package in manifest["packages"]:
        identity = package["hash"]
        if cached := cached_package(packages, identity):
            try:
                check_remote_declarations(cached, manifest)
                continue
            except (ValueError, tarfile.TarError, EOFError, OSError):
                print(f"Refetching invalid cached package {identity}", flush=True)
                # Zig may retain an existing global archive during fetch. Remove
                # the invalid entry so a verified replacement can be installed.
                if cached.is_file():
                    cached.unlink()
        local_source = seed / f"{identity}.tar.gz" if seed else None
        sources = [] if offline else [f"{manifest['mirror']}/{identity}.tar.gz"]
        if local_source and local_source.is_file():
            sources.insert(0, str(local_source))
        if not mirror_only and not offline:
            sources.append(package["url"])
        failures = []
        for source in sources:
            try:
                admitted = fetch_verified(zig, source, identity, packages)
                check_remote_declarations(admitted, manifest)
                print(
                    f"Prepared Zig source package {identity} from {source}", flush=True
                )
                break
            except (
                OSError,
                ValueError,
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
            ) as exc:
                detail = (
                    exc.stderr
                    if isinstance(exc, subprocess.CalledProcessError)
                    else str(exc)
                )
                failures.append(f"{source}: {detail}")
        else:
            raise RuntimeError(
                "cannot prepare pinned Zig dependency:\n" + "\n".join(failures)
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument(
        "--root", type=Path, default=MANIFEST.resolve().parents[2] / "zig"
    )
    parser.add_argument("--cache", type=Path)
    parser.add_argument(
        "--seed", type=Path, help="Optional local verified source archives"
    )
    parser.add_argument("--mirror-only", action="store_true")
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    cache = args.cache or Path(
        os.environ.get("ZIG_GLOBAL_CACHE_DIR", "zig/.zig-cache/global")
    )
    manifest = load_manifest(args.manifest)
    check_remote_declarations(args.root.resolve(), manifest)
    prepare(
        args.zig, manifest, cache.resolve(), args.seed, args.mirror_only, args.offline
    )


if __name__ == "__main__":
    main()
