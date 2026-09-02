#!/usr/bin/env python3
"""Preflight every immutable npm package in a verified CLI snapshot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from collections.abc import Callable
from pathlib import Path

from build_cli_snapshot import NPM_PACKAGES
from registry.model import RegistryError
from registry.npm import dist_tag, version_integrity
from release_channels import normalize_release_version

IntegrityReader = Callable[[str, str], "str | None"]
TagReader = Callable[[str, str], "str | None"]


def npm_integrity(path: Path) -> str:
    digest = hashlib.sha512(path.read_bytes()).digest()
    return "sha512-" + base64.b64encode(digest).decode("ascii")


def preflight_package(
    package: str,
    version: str,
    tag: str,
    tarball: Path,
    integrity_reader: IntegrityReader = version_integrity,
    tag_reader: TagReader = dist_tag,
) -> None:
    expected = npm_integrity(tarball)
    published = integrity_reader(package, version)
    if published is None:
        print(f"npm preflight will create {package}@{version}")
        return
    if published != expected:
        raise SystemExit(
            f"{package}@{version} exists with different contents\n"
            f"registry: {published}\nlocal:    {expected}"
        )
    published_tag = tag_reader(package, tag)
    if published_tag != version:
        raise SystemExit(
            f"{package}@{version} exists, but dist-tag {tag} points to "
            f"{published_tag or 'nothing'}"
        )
    print(f"npm preflight verified {package}@{version} and dist-tag {tag}")


def snapshot_packages(snapshot_dir: Path, version: str) -> dict[str, Path]:
    manifest_path = snapshot_dir / "cli-snapshot.json"
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read CLI snapshot manifest: {exc}") from exc
    if (
        not isinstance(document, dict)
        or document.get("schema_version") != 1
        or document.get("version") != version
        or not isinstance(document.get("artifacts"), list)
    ):
        raise SystemExit("CLI snapshot manifest has an invalid npm release identity")

    packages: dict[str, Path] = {}
    for artifact in document["artifacts"]:
        if not isinstance(artifact, dict) or artifact.get("kind") != "npm":
            continue
        package = artifact.get("package")
        name = artifact.get("name")
        if (
            not isinstance(package, str)
            or package not in NPM_PACKAGES
            or not isinstance(name, str)
            or Path(name).name != name
            or package in packages
        ):
            raise SystemExit("CLI snapshot manifest has malformed npm artifacts")
        tarball = snapshot_dir / name
        if not tarball.is_file():
            raise SystemExit(f"CLI snapshot is missing npm tarball: {name}")
        packages[package] = tarball
    if set(packages) != set(NPM_PACKAGES):
        raise SystemExit(
            "CLI snapshot npm package set differs: "
            f"expected={sorted(NPM_PACKAGES)} actual={sorted(packages)}"
        )
    return packages


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()

    version = normalize_release_version(args.version)
    if not args.tag:
        parser.error("--tag must not be empty")
    try:
        for package, tarball in sorted(
            snapshot_packages(args.snapshot_dir, version).items()
        ):
            preflight_package(package, version, args.tag, tarball)
    except RegistryError as exc:
        raise SystemExit(str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
