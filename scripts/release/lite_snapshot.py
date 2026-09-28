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

"""Seal and verify the exact Apache Lite registry package set."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
import tarfile
import zipfile
from pathlib import Path

from release_channels import normalize_release_version, python_version_from_release

NPM_PACKAGES = {
    "@antfly/embedded": "antfly-embedded",
    "@antfly/embedded-darwin-arm64": "antfly-embedded-darwin-arm64",
    "@antfly/embedded-linux-arm64": "antfly-embedded-linux-arm64",
    "@antfly/embedded-linux-x64": "antfly-embedded-linux-x64",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def npm_identity(path: Path) -> tuple[str, str]:
    with tarfile.open(path, "r:gz") as archive:
        source = archive.extractfile("package/package.json")
        if source is None:
            raise ValueError(f"missing npm package.json: {path}")
        manifest = json.load(source)
    return manifest["name"], manifest["version"]


def wheel_identity(path: Path) -> tuple[str, str]:
    with zipfile.ZipFile(path) as archive:
        metadata = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata) != 1:
            raise ValueError(f"expected one wheel METADATA: {path}")
        fields = {}
        for line in archive.read(metadata[0]).decode().splitlines():
            if line.startswith(("Name: ", "Version: ", "License-Expression: ")):
                key, value = line.split(": ", 1)
                fields[key] = value
        if fields.get("License-Expression") != "Apache-2.0":
            raise ValueError(f"wheel is not Apache-2.0: {path}")
        return fields["Name"], fields["Version"]


def expected_names(version: str) -> set[str]:
    names = {f"{stem}-{version}.tgz" for stem in NPM_PACKAGES.values()}
    py_version = python_version_from_release(version)
    names.update(
        f"antfly_embedded-{py_version}-py3-none-{platform}.whl"
        for platform in (
            "manylinux_2_28_x86_64",
            "manylinux_2_28_aarch64",
            "macosx_11_0_arm64",
        )
    )
    return names


def build(
    version: str, commit: str, npm_dir: Path, wheel_dir: Path, out_dir: Path
) -> None:
    version = normalize_release_version(version)
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("commit must be a full SHA-1")
    if out_dir.exists() and any(out_dir.iterdir()):
        raise ValueError(f"snapshot directory is not empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    sources = [*npm_dir.glob("*.tgz"), *wheel_dir.glob("*.whl")]
    if {item.name for item in sources} != expected_names(version):
        raise ValueError(
            "Lite snapshot package set differs from the release platform policy"
        )
    for source in sources:
        if source.suffix == ".tgz":
            name, package_version = npm_identity(source)
            if name not in NPM_PACKAGES or package_version != version:
                raise ValueError(f"unexpected npm package: {source}")
        else:
            name, package_version = wheel_identity(source)
            if name != "antfly-embedded" or package_version != python_version_from_release(
                version
            ):
                raise ValueError(f"unexpected wheel: {source}")
        shutil.copy2(source, out_dir / source.name)
    manifest = {
        "schema_version": 1,
        "version": version,
        "python_version": python_version_from_release(version),
        "commit": commit,
        "artifacts": [
            {"name": path.name, "size": path.stat().st_size, "sha256": sha256(path)}
            for path in sorted(out_dir.iterdir())
        ],
    }
    (out_dir / "lite-snapshot.json").write_text(json.dumps(manifest, indent=2) + "\n")


def verify(snapshot_dir: Path, version: str, commit: str) -> None:
    version = normalize_release_version(version)
    manifest = json.loads((snapshot_dir / "lite-snapshot.json").read_text())
    if (
        manifest.get("schema_version") != 1
        or manifest.get("version") != version
        or manifest.get("commit") != commit
    ):
        raise ValueError("Lite snapshot identity differs from the requested release")
    if manifest.get("python_version") != python_version_from_release(version):
        raise ValueError("Lite snapshot Python version differs")
    artifacts = manifest.get("artifacts")
    if (
        not isinstance(artifacts, list)
        or len(artifacts) != len(expected_names(version))
        or any(not isinstance(item, dict) for item in artifacts)
        or {item.get("name") for item in artifacts} != expected_names(version)
    ):
        raise ValueError("Lite snapshot artifact set differs")
    if {path.name for path in snapshot_dir.iterdir()} != expected_names(version) | {
        "lite-snapshot.json"
    }:
        raise ValueError("Lite snapshot contains extra or missing files")
    for item in artifacts:
        path = snapshot_dir / item["name"]
        if path.stat().st_size != item["size"] or sha256(path) != item["sha256"]:
            raise ValueError(f"Lite snapshot artifact digest differs: {path}")
        if path.suffix == ".tgz":
            name, package_version = npm_identity(path)
            if (
                NPM_PACKAGES.get(name) != path.name.removesuffix(f"-{version}.tgz")
                or package_version != version
            ):
                raise ValueError(f"Lite npm package identity differs: {path}")
        else:
            if wheel_identity(path) != (
                "antfly-embedded",
                python_version_from_release(version),
            ):
                raise ValueError(f"Lite wheel identity differs: {path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    build_parser = subcommands.add_parser("build")
    build_parser.add_argument("--version", required=True)
    build_parser.add_argument("--commit", required=True)
    build_parser.add_argument("--npm-dir", type=Path, required=True)
    build_parser.add_argument("--wheel-dir", type=Path, required=True)
    build_parser.add_argument("--out-dir", type=Path, required=True)
    verify_parser = subcommands.add_parser("verify")
    verify_parser.add_argument("--snapshot-dir", type=Path, required=True)
    verify_parser.add_argument("--version", required=True)
    verify_parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    try:
        if args.command == "build":
            build(args.version, args.commit, args.npm_dir, args.wheel_dir, args.out_dir)
        else:
            verify(args.snapshot_dir, args.version, args.commit)
    except (ValueError, KeyError, OSError) as exc:
        print(f"Lite snapshot: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
