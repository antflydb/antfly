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

"""Classify a Git diff into the serial SDK CI scopes."""

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
    "typescript",
    "memoryaf",
    "release",
    "antfarm_e2e",
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
    "scripts/ci/check_toolchain_policy.py",
    "scripts/pyproject.toml",
    "scripts/uv.lock",
    ".github/workflows/py-pypi-publish.yml",
}
RELEASE_PREFIXES = (
    "py/packages/cli/",
    "scripts/packaging/",
    "scripts/release/",
    "ts/packages/cli/",
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
}
FORMAT_INFRASTRUCTURE = {
    "scripts/format.sh",
    "py/packages/sdk/pyproject.toml",
    "py/packages/sdk/uv.lock",
    "ts/biome.json",
    "ts/package.json",
    "ts/pnpm-lock.yaml",
    "rs/Cargo.toml",
}
FULL_VALIDATION_FILES = {
    "Makefile",
    "scripts/ci/check.sh",
    "scripts/ci/toolchain-policy.json",
    ".github/workflows/sdks-ci.yml",
}
FULL_VALIDATION_PREFIXES = (".github/actions/",)


def matches(path: str, files: set[str], prefixes: tuple[str, ...]) -> bool:
    return path in files or path.startswith(prefixes)


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
        if matches(path, FULL_VALIDATION_FILES, FULL_VALIDATION_PREFIXES):
            for scope in SCOPES:
                scopes[scope] = True
            continue

        suffix = Path(path).suffix
        scopes["format_zig"] |= suffix == ".zig"
        scopes["format_go"] |= suffix == ".go"
        scopes["format_python"] |= suffix == ".py"
        scopes["format_rust"] |= suffix == ".rs"
        scopes["format_typescript"] |= path.startswith("ts/")

        if path in FORMAT_INFRASTRUCTURE:
            for language in ("zig", "go", "python", "typescript", "rust"):
                scopes[f"format_{language}"] = True

        sdk_input = matches(path, SDK_FILES, SDK_PREFIXES)
        typescript_input = (
            path.startswith("ts/")
            or sdk_input
            and (path == "openapi.yaml" or path.startswith("specs/openapi/"))
        )
        antfarm_input = typescript_input or path.startswith("zig/pkg/antfly/antfarm/")

        scopes["sdk"] |= sdk_input
        # A checked-in embedded bundle change must rebuild its TypeScript
        # producer before the byte-for-byte and browser checks can run.
        scopes["typescript"] |= antfarm_input
        scopes["memoryaf"] |= path.startswith("go/pkg/memoryaf/")
        scopes["antfarm_e2e"] |= antfarm_input
        scopes["release"] |= matches(
            path, RELEASE_FILES, RELEASE_PREFIXES
        ) or path.startswith(".github/workflows/")

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
