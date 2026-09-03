#!/usr/bin/env python3
"""Prepare or verify an exact, ledger-defined PyPI release file set."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import time
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
    return parse_pypi_release_files(payload)


def parse_pypi_release_files(payload: object) -> dict[str, str]:
    if not isinstance(payload, dict) or not isinstance(payload.get("urls"), list):
        raise SystemExit("PyPI returned malformed release metadata")
    files: dict[str, str] = {}
    for item in payload["urls"]:
        if not isinstance(item, dict):
            raise SystemExit("PyPI returned a malformed release file entry")
        filename = item.get("filename")
        digests = item.get("digests")
        digest = digests.get("sha256") if isinstance(digests, dict) else None
        if (
            not isinstance(filename, str)
            or not filename
            or not isinstance(digest, str)
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
        ):
            raise SystemExit("PyPI returned a malformed release file entry")
        if filename in files:
            raise SystemExit(f"PyPI returned duplicate release file {filename}")
        files[filename] = digest
    return files


def expected_release_files(wheels: list[Path]) -> dict[str, str]:
    return {wheel.name: sha256(wheel) for wheel in wheels}


def missing_registry_files(
    expected: dict[str, str], registry: dict[str, str]
) -> set[str]:
    unexpected = sorted(set(registry) - set(expected))
    if unexpected:
        raise SystemExit(
            "PyPI version contains files absent from the release ledger: "
            + ", ".join(unexpected)
        )
    for filename in sorted(set(expected) & set(registry)):
        if registry[filename] != expected[filename]:
            raise SystemExit(
                f"{filename} exists on PyPI with different contents\n"
                f"PyPI: {registry[filename]}\nlocal: {expected[filename]}"
            )
    return set(expected) - set(registry)


def verify_complete_release(
    project: str,
    version: str,
    expected: dict[str, str],
    attempts: int,
    retry_seconds: float,
) -> None:
    for attempt in range(1, attempts + 1):
        missing = missing_registry_files(expected, pypi_release_files(project, version))
        if not missing:
            print(
                f"verified exact PyPI release {project}=={version} "
                f"with {len(expected)} file(s)"
            )
            return
        if attempt < attempts:
            print(
                "waiting for PyPI to expose expected files: "
                + ", ".join(sorted(missing))
            )
            time.sleep(retry_seconds)
    raise SystemExit(
        "PyPI version is missing release-ledger files: " + ", ".join(sorted(missing))
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--expected-version")
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--verify-complete", action="store_true")
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--retry-seconds", type=float, default=5.0)
    args = parser.parse_args()

    if args.attempts < 1:
        parser.error("--attempts must be positive")
    if args.retry_seconds < 0:
        parser.error("--retry-seconds must not be negative")
    if not args.verify_complete and args.out_dir is None:
        parser.error("--out-dir is required when preparing a promotion")

    wheels = sorted(args.snapshot_dir.glob("*.whl"))
    if not wheels:
        raise SystemExit("CLI snapshot contains no Python wheels")
    versions = {wheel_version(wheel) for wheel in wheels}
    if len(versions) != 1:
        raise SystemExit(
            f"CLI snapshot contains multiple Python versions: {sorted(versions)}"
        )
    version = versions.pop()
    if args.expected_version and version != args.expected_version:
        raise SystemExit(
            f"Python package version differs: expected={args.expected_version} "
            f"actual={version}"
        )
    expected_files = expected_release_files(wheels)
    if args.verify_complete:
        verify_complete_release(
            args.project,
            version,
            expected_files,
            args.attempts,
            args.retry_seconds,
        )
        return 0

    registry_files = pypi_release_files(args.project, version)
    missing_files = missing_registry_files(expected_files, registry_files)

    assert args.out_dir is not None
    if args.out_dir.exists() and any(args.out_dir.iterdir()):
        raise SystemExit(f"PyPI promotion directory must be empty: {args.out_dir}")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    for wheel in wheels:
        if wheel.name not in missing_files:
            print(f"{wheel.name} already has the same PyPI artifact; skipping")
            continue
        shutil.copy2(wheel, args.out_dir / wheel.name)

    publish_count = len(missing_files)
    has_packages = "true" if publish_count else "false"
    if github_output := os.environ.get("GITHUB_OUTPUT"):
        with Path(github_output).open("a", encoding="utf-8") as output:
            output.write(f"has_packages={has_packages}\n")
    print(f"prepared {publish_count} wheel(s) for PyPI promotion")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
