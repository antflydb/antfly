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

"""Compile lake, public C API, and browser artifacts without server implementations."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


def stage_sources(repository: Path, destination: Path) -> int:
    listing = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=repository,
    )
    removed = 0
    for raw in listing.split(b"\0"):
        if not raw:
            continue
        relative = Path(raw.decode())
        # Build inputs and source-generation tooling; bindings and application
        # assets are unrelated to either compilation owner.
        if relative.parts[0] not in {
            "zig",
            "specs",
            "scripts",
            "LICENSES",
        } and relative.as_posix() not in {
            "LICENSE",
            "LICENSING.md",
            "THIRD_PARTY_NOTICES.md",
        }:
            continue
        source = repository / relative
        if not source.is_file():
            continue
        # No stubs or copied server source: even dormant literal imports must
        # belong to embedded, a shared library, or an explicit test capability.
        if relative.parts[:3] == ("zig", "pkg", "antfly"):
            removed += 1
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return removed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("build_flags", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    flags = args.build_flags
    if flags[:1] == ["--"]:
        flags = flags[1:]
    repository = Path(__file__).resolve().parents[2]
    (repository / ".zig-cache").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="antfly-embedded-isolated-", dir=repository / ".zig-cache"
    ) as directory:
        stage = Path(directory)
        removed = stage_sources(repository, stage)
        print(
            f"Staged embedded build: {removed} server files omitted",
            flush=True,
        )
        subprocess.run(
            [
                args.zig,
                "build",
                "--build-file",
                "embedded.build.zig",
                "libantfly",
                "capi-smoke",
                "capi-conformance",
                "embedded-capi-check",
                "embedded-lake-test",
                "embedded-package-test",
                "aws-credentials-test",
                "embedded-native-module-boundary-check",
                "embedded-wasm-module-boundary-check",
                "wasm-test",
                "-Dmetal=false",
                *flags,
            ],
            cwd=stage / "zig",
            check=True,
        )
        subprocess.run(
            [args.zig, "build", "-Dmetal=false", *flags],
            cwd=stage / "zig/pkg/inference",
            check=True,
        )


if __name__ == "__main__":
    main()
