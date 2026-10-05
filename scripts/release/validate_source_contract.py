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

"""Validate that a historical source commit implements the release build contract."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path, PurePosixPath

CONTRACT_PATH = "scripts/release/build-contract.json"
SUPPORTED_SCHEMAS = (1, 2)
LEGACY_REQUIRED_PATHS = {
    "zig/build.zig",
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

APACHE_REQUIRED_PATHS = {
    "zig/embedded.build.zig",
    "zig/pkg/antfly-embedded/LICENSE",
    "zig/pkg/inference/build.zig",
    "zig/pkg/inference/LICENSE",
    "LICENSES/Apache-2.0.txt",
    "scripts/source_license_roots.json",
    "scripts/apache_engine_files.txt",
    "scripts/embedded_asset_licenses.json",
    "scripts/packaging/package_lite_release.py",
    "scripts/packaging/verify_lite_release.py",
    "scripts/packaging/test_lite_packaging.py",
    "scripts/packaging/test_release_licenses.py",
    "scripts/release/lite_snapshot.py",
    "ts/packages/embedded/package.json",
    "py/packages/embedded/pyproject.toml",
}
REQUIRED_PATHS = LEGACY_REQUIRED_PATHS | APACHE_REQUIRED_PATHS


def runtime_products(schema: int) -> tuple[str, ...]:
    if type(schema) is not int or schema not in SUPPORTED_SCHEMAS:
        raise SystemExit("unsupported release build contract schema")
    return ("server",) if schema == 1 else ("server", "lite", "inference")


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
    if not isinstance(contract, dict):
        raise SystemExit("invalid release build contract")
    schema = contract.get("schema_version")
    products = runtime_products(schema)
    if (schema == 2 or "runtime_products" in contract) and contract.get(
        "runtime_products"
    ) != list(products):
        raise SystemExit("release build contract has invalid runtime products")
    required_paths = LEGACY_REQUIRED_PATHS if schema == 1 else REQUIRED_PATHS
    paths = contract.get("required_source_paths")
    if not isinstance(paths, list) or not paths:
        raise SystemExit("release build contract has no required_source_paths")
    if not all(isinstance(path, str) for path in paths):
        raise SystemExit("release build contract contains a non-string path")
    if set(paths) != required_paths or len(paths) != len(required_paths):
        raise SystemExit(
            "release build contract does not declare the required builder inputs"
        )
    for path in paths:
        parsed = PurePosixPath(path) if isinstance(path, str) else None
        if parsed is None or parsed.is_absolute() or ".." in parsed.parts:
            raise SystemExit(f"release build contract has an unsafe path: {path!r}")
        git_object(repo_root, commit, path)
    return schema


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    schema = validate(repo_root, args.commit.lower())
    if args.github_output is not None:
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(f"build_contract_schema={schema}\n")
            output.write(f"runtime_products={json.dumps(runtime_products(schema))}\n")
    print(f"validated release build contract schema {schema} at {args.commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
