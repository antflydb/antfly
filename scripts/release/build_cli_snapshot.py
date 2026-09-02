#!/usr/bin/env python3
"""Build an immutable manifest for prebuilt CLI registry artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import tarfile
import zipfile
from pathlib import Path

from release_channels import normalize_release_version, python_version_from_release

NPM_PACKAGES = {
    "@antfly/cli": "antfly-cli",
    "@antfly/cli-darwin-arm64": "antfly-cli-darwin-arm64",
    "@antfly/cli-linux-arm64": "antfly-cli-linux-arm64",
    "@antfly/cli-linux-x64": "antfly-cli-linux-x64",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def npm_metadata(path: Path) -> dict[str, object]:
    with tarfile.open(path, "r:gz") as archive:
        member = archive.getmember("package/package.json")
        src = archive.extractfile(member)
        if src is None:
            raise SystemExit(f"cannot read package.json from {path}")
        return json.load(src)


def wheel_metadata(path: Path) -> tuple[str, str]:
    with zipfile.ZipFile(path) as wheel:
        metadata_files = [
            name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_files) != 1:
            raise SystemExit(
                f"expected one METADATA file in {path}, found {len(metadata_files)}"
            )
        metadata = wheel.read(metadata_files[0]).decode()
    name_match = re.search(r"^Name: (.+)$", metadata, re.MULTILINE)
    version_match = re.search(r"^Version: (.+)$", metadata, re.MULTILINE)
    if not name_match or not version_match:
        raise SystemExit(f"missing Name or Version metadata in {path}")
    return name_match.group(1), version_match.group(1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--npm-dir", type=Path, required=True)
    parser.add_argument("--python-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    version = normalize_release_version(args.version)
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        raise SystemExit(f"invalid release commit: {args.commit}")
    python_version = python_version_from_release(version)

    npm_files = sorted(args.npm_dir.glob("*.tgz"))
    wheels = sorted(args.python_dir.glob("*.whl"))
    if len(npm_files) != len(NPM_PACKAGES):
        raise SystemExit(
            f"expected {len(NPM_PACKAGES)} npm tarballs, found {len(npm_files)}"
        )
    if len(wheels) != 3:
        raise SystemExit(f"expected 3 Python wheels, found {len(wheels)}")

    artifacts: list[dict[str, object]] = []
    seen_npm: set[str] = set()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    for source in [*npm_files, *wheels]:
        destination = args.out_dir / source.name
        shutil.copy2(source, destination)
        artifact: dict[str, object] = {
            "name": destination.name,
            "size": destination.stat().st_size,
            "sha256": sha256(destination),
        }
        if source.suffix == ".tgz":
            metadata = npm_metadata(source)
            package_name = metadata.get("name")
            if package_name not in NPM_PACKAGES:
                raise SystemExit(f"unexpected npm package {package_name!r} in {source}")
            if metadata.get("version") != version:
                raise SystemExit(
                    f"npm package {package_name} has version {metadata.get('version')}, expected {version}"
                )
            expected_filename = f"{NPM_PACKAGES[str(package_name)]}-{version}.tgz"
            if source.name != expected_filename:
                raise SystemExit(
                    f"npm package {package_name} must be named {expected_filename}, got {source.name}"
                )
            seen_npm.add(str(package_name))
            artifact.update({"kind": "npm", "package": package_name})
        else:
            package_name, package_version = wheel_metadata(source)
            if package_name != "antfly-cli":
                raise SystemExit(
                    f"unexpected Python package {package_name!r} in {source}"
                )
            if package_version != python_version:
                raise SystemExit(
                    f"Python package has version {package_version}, expected {python_version}"
                )
            artifact.update(
                {
                    "kind": "python",
                    "package": package_name,
                    "package_version": package_version,
                }
            )
        artifacts.append(artifact)

    if seen_npm != set(NPM_PACKAGES):
        raise SystemExit(f"npm package set mismatch: {sorted(seen_npm)}")

    manifest = {
        "schema_version": 1,
        "version": version,
        "commit": args.commit,
        "registry_versions": {"npm": version, "python": python_version},
        "artifacts": sorted(artifacts, key=lambda artifact: str(artifact["name"])),
    }
    manifest_path = args.out_dir / "cli-snapshot.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"wrote {manifest_path} with {len(artifacts)} immutable artifacts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
