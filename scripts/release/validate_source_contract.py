#!/usr/bin/env python3
"""Validate that a historical source commit implements the release build contract."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path, PurePosixPath

CONTRACT_PATH = "scripts/release/build-contract.json"
SUPPORTED_SCHEMA = 1
REQUIRED_PATHS = {
    "build.zig",
    "scripts/install.sh",
    "scripts/packaging/build_zig_release_archive.sh",
    "scripts/packaging/package_cli_release.py",
    "scripts/packaging/test_cabi_packaging.py",
    "scripts/packaging/test_reproducible_tar.py",
    "scripts/release/build_cli_snapshot.py",
    "scripts/release/release_channels.py",
    "scripts/release/release_platforms.py",
    "scripts/release/verify_cli_snapshot.py",
    "scripts/release/platforms.json",
    "ts/package.json",
    "ts/packages/cli/package.json",
    "py/packages/cli/pyproject.toml",
    "openapi.yaml",
}


def git_object(repo_root: Path, commit: str, path: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "show", f"{commit}:{path}"],
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise SystemExit(
            f"source commit {commit} does not satisfy release build contract: missing {path}"
        )
    return result.stdout


def validate(repo_root: Path, commit: str) -> int:
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise SystemExit(f"invalid source commit: {commit}")
    try:
        contract = json.loads(git_object(repo_root, commit, CONTRACT_PATH))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid {CONTRACT_PATH} at source commit {commit}") from exc
    if (
        not isinstance(contract, dict)
        or contract.get("schema_version") != SUPPORTED_SCHEMA
    ):
        raise SystemExit(
            f"source commit uses an unsupported release build contract; controller supports schema {SUPPORTED_SCHEMA}"
        )
    paths = contract.get("required_source_paths")
    if not isinstance(paths, list) or not paths:
        raise SystemExit("release build contract has no required_source_paths")
    if not all(isinstance(path, str) for path in paths):
        raise SystemExit("release build contract contains a non-string path")
    if set(paths) != REQUIRED_PATHS or len(paths) != len(REQUIRED_PATHS):
        raise SystemExit(
            "release build contract does not declare the required builder inputs"
        )
    for path in paths:
        parsed = PurePosixPath(path) if isinstance(path, str) else None
        if parsed is None or parsed.is_absolute() or ".." in parsed.parts:
            raise SystemExit(f"release build contract has an unsafe path: {path!r}")
        git_object(repo_root, commit, path)
    return SUPPORTED_SCHEMA


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    schema = validate(repo_root, args.commit.lower())
    print(f"validated release build contract schema {schema} at {args.commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
