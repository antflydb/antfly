#!/usr/bin/env python3
"""Verify an immutable CLI release snapshot before registry promotion."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


NPM_PACKAGES = {
    "@antfly/cli",
    "@antfly/cli-darwin-arm64",
    "@antfly/cli-linux-arm64",
    "@antfly/cli-linux-x64",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--version")
    parser.add_argument("--commit")
    args = parser.parse_args()

    manifest_path = args.snapshot_dir / "cli-snapshot.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != 1:
        raise SystemExit(
            f"unsupported CLI snapshot schema: {manifest.get('schema_version')}"
        )
    expected_version = args.version.removeprefix("v") if args.version else None
    if expected_version and manifest.get("version") != expected_version:
        raise SystemExit(
            f"snapshot version {manifest.get('version')} does not match {expected_version}"
        )
    if args.commit and manifest.get("commit") != args.commit:
        raise SystemExit(
            f"snapshot commit {manifest.get('commit')} does not match {args.commit}"
        )
    registry_versions = manifest.get("registry_versions")
    if not isinstance(registry_versions, dict) or registry_versions.get(
        "npm"
    ) != manifest.get("version"):
        raise SystemExit("CLI snapshot has invalid registry versions")
    python_version = registry_versions.get("python")
    if not isinstance(python_version, str) or not python_version:
        raise SystemExit("CLI snapshot has no Python registry version")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise SystemExit("CLI snapshot has no artifacts")
    expected_files = {"cli-snapshot.json"}
    kinds: dict[str, int] = {}
    npm_packages: set[str] = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise SystemExit("invalid CLI snapshot artifact entry")
        name = artifact.get("name")
        if not isinstance(name, str) or Path(name).name != name:
            raise SystemExit(f"invalid CLI snapshot artifact name: {name!r}")
        path = args.snapshot_dir / name
        if not path.is_file():
            raise SystemExit(f"missing CLI snapshot artifact: {name}")
        if path.stat().st_size != artifact.get("size"):
            raise SystemExit(f"size mismatch for CLI snapshot artifact: {name}")
        if sha256(path) != artifact.get("sha256"):
            raise SystemExit(f"SHA-256 mismatch for CLI snapshot artifact: {name}")
        kind = artifact.get("kind")
        if kind not in {"npm", "python"}:
            raise SystemExit(f"invalid CLI snapshot artifact kind for {name}: {kind!r}")
        kinds[str(kind)] = kinds.get(str(kind), 0) + 1
        package = artifact.get("package")
        if kind == "npm":
            if package not in NPM_PACKAGES:
                raise SystemExit(f"invalid npm package for {name}: {package!r}")
            npm_packages.add(str(package))
        elif (
            package != "antfly-cli" or artifact.get("package_version") != python_version
        ):
            raise SystemExit(f"invalid Python package metadata for {name}")
        expected_files.add(name)

    actual_files = {path.name for path in args.snapshot_dir.iterdir() if path.is_file()}
    if actual_files != expected_files:
        raise SystemExit(
            f"CLI snapshot file set mismatch: expected {sorted(expected_files)}, got {sorted(actual_files)}"
        )
    if kinds != {"npm": 4, "python": 3}:
        raise SystemExit(f"CLI snapshot artifact counts are invalid: {kinds}")
    if npm_packages != NPM_PACKAGES:
        raise SystemExit(
            f"CLI snapshot npm package set is invalid: {sorted(npm_packages)}"
        )
    print(f"verified CLI snapshot {manifest['version']} at {manifest['commit']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
