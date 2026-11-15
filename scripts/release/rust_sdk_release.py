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

"""Seal and publish the independent Rust SDK without running source with credentials."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tarfile
import time
import tomllib
from pathlib import Path

from embedded_cargo import (
    crate_identity,
    digest,
    preflight,
    registry_checksum,
    run,
    stamp_manifest,
    upload,
    upload_metadata,
)

NAME = "antfly-sdk"


def identity(repo: Path, tag: str) -> tuple[str, str]:
    if not re.fullmatch(
        r"rs/antfly/sdk/v\d+\.\d+\.\d+(?:-(?:alpha|beta|rc)\.[1-9]\d*)?", tag
    ):
        raise ValueError("expected an rs/antfly/sdk/vX.Y.Z release tag")
    version = tag.removeprefix("rs/antfly/sdk/v")
    commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}"],
        cwd=repo,
        text=True,
    ).strip()
    subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "origin/main"],
        cwd=repo,
        check=True,
    )
    manifest = tomllib.loads(
        subprocess.check_output(
            ["git", "show", f"{commit}:rs/crates/sdk/Cargo.toml"], cwd=repo, text=True
        )
    )["package"]
    if (manifest["name"], manifest["version"], manifest["license"]) != (
        NAME,
        version,
        "Apache-2.0",
    ):
        raise ValueError("Rust SDK tag differs from its package identity")
    return version, commit


def verify(snapshot: Path, version: str, commit: str) -> Path:
    filename = f"{NAME}-{version}.crate"
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("expected a full source commit")
    if snapshot.is_symlink() or any(p.is_symlink() for p in snapshot.iterdir()):
        raise ValueError("SDK snapshot must not contain symlinks")
    if set(p.name for p in snapshot.iterdir()) != {filename, "sdk-snapshot.json"}:
        raise ValueError("SDK snapshot file set differs")
    metadata = json.loads((snapshot / "sdk-snapshot.json").read_text())
    path = snapshot / filename
    expected = {
        "schema_version": 1,
        "version": version,
        "commit": commit,
        "name": filename,
        "sha256": digest(path),
        "size": path.stat().st_size,
    }
    if metadata != expected or crate_identity(path) != (NAME, version, commit):
        raise ValueError("SDK snapshot identity or digest differs")
    upload_metadata(path)  # Validate registry request before granting credentials.
    return path


def build(repo: Path, stage: Path, snapshot: Path, version: str, commit: str) -> None:
    if snapshot.exists() and any(snapshot.iterdir()):
        raise ValueError("SDK snapshot directory must be empty")
    subprocess.run(
        ["git", "worktree", "add", "--detach", str(stage), commit], cwd=repo, check=True
    )
    manifest = stage / "rs/crates/sdk/Cargo.toml"
    manifest.write_text(stamp_manifest(manifest.read_text(), NAME, version, commit))
    target = stage / "rs/target"
    run(
        "cargo",
        "test",
        "--locked",
        "-p",
        NAME,
        "--all-features",
        cwd=stage / "rs",
        target=target,
    )
    run(
        "cargo",
        "package",
        "--locked",
        "--allow-dirty",
        "-p",
        NAME,
        cwd=stage / "rs",
        target=target,
    )
    snapshot.mkdir(parents=True, exist_ok=True)
    path = snapshot / f"{NAME}-{version}.crate"
    shutil.copy2(target / "package" / path.name, path)
    with tarfile.open(path, "r:gz") as archive:
        bundled_spec = archive.extractfile(f"{NAME}-{version}/openapi.yaml")
        if (
            bundled_spec is None
            or bundled_spec.read() != (stage / "openapi.yaml").read_bytes()
        ):
            raise ValueError(
                "Rust SDK bundled OpenAPI spec differs from the release source"
            )
    (snapshot / "sdk-snapshot.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "version": version,
                "commit": commit,
                "name": path.name,
                "sha256": digest(path),
                "size": path.stat().st_size,
            },
            sort_keys=True,
            indent=2,
        )
        + "\n"
    )
    verify(snapshot, version, commit)


def publish(snapshot: Path, version: str, commit: str) -> None:
    path = verify(snapshot, version, commit)
    checksum = digest(path)
    if not preflight({NAME: checksum}, version):
        return
    token = os.environ.get("CARGO_REGISTRY_TOKEN", "")
    if not token:
        raise ValueError("missing short-lived crates.io token")
    upload(path, upload_metadata(path), token)
    for attempt in range(30):
        observed = registry_checksum(NAME, version)
        if observed == checksum:
            return
        if observed is not None:
            raise ValueError("published SDK checksum differs")
        if attempt < 29:
            time.sleep(2)
    raise ValueError("published SDK did not become visible; retry the sealed snapshot")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("resolve", "build", "verify", "preflight", "publish")
    )
    parser.add_argument("--tag", required=True)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--stage", type=Path)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--commit")
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    version, commit = identity(args.repo.resolve(), args.tag)
    if args.commit is not None and args.commit != commit:
        raise ValueError("SDK tag moved after preparation")
    if args.command == "resolve":
        if args.github_output:
            with args.github_output.open("a") as output:
                output.write(f"version={version}\ncommit={commit}\n")
        return
    if args.snapshot is None:
        parser.error("snapshot is required")
    if args.command == "build":
        if args.stage is None:
            parser.error("build requires stage")
        build(
            args.repo.resolve(),
            args.stage.resolve(),
            args.snapshot.resolve(),
            version,
            commit,
        )
    elif args.command == "publish":
        publish(args.snapshot, version, commit)
    else:
        path = verify(args.snapshot, version, commit)
        if args.command == "preflight":
            missing = preflight({NAME: digest(path)}, version)
            if args.github_output:
                with args.github_output.open("a") as output:
                    output.write(f"has_packages={str(bool(missing)).lower()}\n")


if __name__ == "__main__":
    main()
