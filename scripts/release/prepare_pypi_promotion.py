#!/usr/bin/env python3
"""Select unpublished PyPI wheels while rejecting registry content drift."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import zipfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def wheel_version(path: Path) -> str:
    with zipfile.ZipFile(path) as wheel:
        metadata_files = [
            name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_files) != 1:
            raise SystemExit(
                f"expected one METADATA file in {path}, found {len(metadata_files)}"
            )
        for line in wheel.read(metadata_files[0]).decode().splitlines():
            if line.startswith("Version: "):
                return line.removeprefix("Version: ")
    raise SystemExit(f"wheel has no Version metadata: {path}")


def pypi_release_files(project: str, version: str) -> dict[str, str]:
    url = f"https://pypi.org/pypi/{quote(project, safe='')}/{quote(version, safe='')}/json"
    try:
        with urlopen(Request(url, headers={"Accept": "application/json"})) as response:
            payload = json.load(response)
    except HTTPError as exc:
        if exc.code == 404:
            return {}
        raise
    files: dict[str, str] = {}
    for item in payload.get("urls", []):
        filename = item.get("filename")
        digest = item.get("digests", {}).get("sha256")
        if isinstance(filename, str) and isinstance(digest, str):
            files[filename] = digest
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    wheels = sorted(args.snapshot_dir.glob("*.whl"))
    if not wheels:
        raise SystemExit("CLI snapshot contains no Python wheels")
    versions = {wheel_version(wheel) for wheel in wheels}
    if len(versions) != 1:
        raise SystemExit(
            f"CLI snapshot contains multiple Python versions: {sorted(versions)}"
        )
    registry_files = pypi_release_files(args.project, versions.pop())

    if args.out_dir.exists() and any(args.out_dir.iterdir()):
        raise SystemExit(f"PyPI promotion directory must be empty: {args.out_dir}")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    publish_count = 0
    for wheel in wheels:
        local_digest = sha256(wheel)
        registry_digest = registry_files.get(wheel.name)
        if registry_digest is not None:
            if registry_digest != local_digest:
                raise SystemExit(
                    f"{wheel.name} exists on PyPI with different contents\n"
                    f"PyPI: {registry_digest}\nlocal: {local_digest}"
                )
            print(f"{wheel.name} already has the same PyPI artifact; skipping")
            continue
        shutil.copy2(wheel, args.out_dir / wheel.name)
        publish_count += 1

    has_packages = "true" if publish_count else "false"
    if github_output := os.environ.get("GITHUB_OUTPUT"):
        with Path(github_output).open("a", encoding="utf-8") as output:
            output.write(f"has_packages={has_packages}\n")
    print(f"prepared {publish_count} wheel(s) for PyPI promotion")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
