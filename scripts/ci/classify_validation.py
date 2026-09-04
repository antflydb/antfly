#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Classify a Git diff into the serial repository-validation scopes."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


SCOPES = (
    "format_zig",
    "format_go",
    "format_python",
    "format_typescript",
    "format_rust",
    "sdk",
    "memoryaf",
    "release",
)

SDK_PREFIXES = (
    "py/packages/sdk/",
    "py/packages/cli/",
    "ts/packages/sdk/",
    "go/pkg/sdk/",
    "rs/crates/sdk/",
    "specs/openapi/",
)
SDK_FILES = {
    "openapi.yaml",
    "rs/Cargo.toml",
    "rs/Cargo.lock",
    "ts/package.json",
    "ts/pnpm-lock.yaml",
    "ts/pnpm-workspace.yaml",
    "ts/tsconfig.base.json",
    "ts/turbo.json",
    "scripts/join_public_openapi.py",
    "scripts/join_openapi.py",
    "scripts/openapi_joiner.py",
    "scripts/public_openapi_overlays.py",
    "scripts/generate_graph_identifier_policy.py",
    "scripts/ci/check.sh",
    "scripts/ci/check_sdk_policy.py",
    "scripts/ci/sdk-policy.json",
    ".github/workflows/repository-validation.yml",
    ".github/workflows/py-pypi-publish.yml",
}
RELEASE_PREFIXES = (
    "scripts/packaging/",
    "scripts/release/",
    "ts/packages/cli",
)
RELEASE_FILES = {
    "scripts/install.sh",
    "scripts/publish-zig-runtime-dev.sh",
    "scripts/test_install_download_markers.sh",
    "scripts/test_quickstart_docs.py",
    "docs/cli-packaging.md",
    "docs/guides/quickstart.mdx",
    "RELEASE.md",
    "zig/Dockerfile.runtime",
    "zig/cloudbuild.manifest.yaml",
    "zig/cloudbuild.runtime.yaml",
    ".github/dependabot.yml",
    "scripts/ci/sdk-policy.json",
}
FORMAT_INFRASTRUCTURE = {
    "Makefile",
    "scripts/format.sh",
    "scripts/ci/sdk-policy.json",
    ".github/workflows/repository-validation.yml",
    "py/packages/sdk/pyproject.toml",
    "py/packages/sdk/uv.lock",
    "ts/biome.json",
    "ts/package.json",
    "ts/pnpm-lock.yaml",
    "rs/Cargo.toml",
}


def changed_paths(base: str, head: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", "-z", base, head],
        check=True,
        stdout=subprocess.PIPE,
    )
    return [path.decode() for path in result.stdout.split(b"\0") if path]


def classify(paths: list[str], force_all: bool = False) -> dict[str, bool]:
    scopes = {scope: force_all for scope in SCOPES}
    if force_all:
        return scopes

    for path in paths:
        suffix = Path(path).suffix
        scopes["format_zig"] |= suffix == ".zig"
        scopes["format_go"] |= suffix == ".go"
        scopes["format_python"] |= suffix == ".py"
        scopes["format_rust"] |= suffix == ".rs"
        scopes["format_typescript"] |= path.startswith("ts/")

        if path in FORMAT_INFRASTRUCTURE:
            for language in ("zig", "go", "python", "typescript", "rust"):
                scopes[f"format_{language}"] = True

        scopes["sdk"] |= path in SDK_FILES or path.startswith(SDK_PREFIXES)
        scopes["memoryaf"] |= path.startswith("go/pkg/memoryaf/")
        scopes["release"] |= (
            path in RELEASE_FILES
            or path.startswith(RELEASE_PREFIXES)
            or path.startswith(".github/workflows/")
        )

    return scopes


def write_outputs(scopes: dict[str, bool], output: Path | None) -> None:
    lines = [
        f"{name}={'true' if enabled else 'false'}" for name, enabled in scopes.items()
    ]
    text = "\n".join(lines) + "\n"
    if output is None:
        print(text, end="")
    else:
        with output.open("a") as destination:
            destination.write(text)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    force_all = args.all or not args.base or set(args.base) == {"0"}
    paths = [] if force_all else changed_paths(args.base, args.head)
    write_outputs(classify(paths, force_all), args.github_output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
