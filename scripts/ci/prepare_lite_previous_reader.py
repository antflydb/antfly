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

"""Build the pinned pre-ownership reader for cross-version open regressions."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess
import sys

# Last main before resolver execution ownership / retired identities in #1114.
REVISION = "e8666a5b758e1e588a6b2b74b260212e984b40a9"
BUILD_SHA256 = "7c607d6bd765beda4d4b33a1ce41e4b0d5ec6fe6f9c531cdefb4081ecb15c26a"
FIXED_BUILD_SHA256 = "8113db112725765e1991bb7ca842a8427f6d3f76d2b7a4651487ef7993036103"


def prepare_previous_reader_build(previous: Path) -> None:
    # Backport only the redundant metadata-link removal from #1173:
    # https://github.com/antflydb/antfly/commit/d4d3a4a6ea22149aebe99bbdc68b4519426994dc
    # addEmbedded already links this object. Keep the historical reader and
    # resolver sources at REVISION; do not move the compatibility baseline.
    build = previous / "zig/embedded.build.zig"
    original = build.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    if digest == FIXED_BUILD_SHA256:
        return
    if digest != BUILD_SHA256:
        raise RuntimeError("unexpected pinned previous-reader embedded build contents")
    fixed = original.replace(
        b"    shared.build_info.link(embedded.libantfly_link_mod);\n", b"", 1
    )
    if hashlib.sha256(fixed).hexdigest() != FIXED_BUILD_SHA256:
        raise RuntimeError("unexpected previous-reader build repair result")
    build.write_bytes(fixed)


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    previous = root / ".worktrees" / "reader-baseline-e8666a5b75"
    if subprocess.run(
        ["git", "cat-file", "-e", f"{REVISION}^{{commit}}"],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode:
        subprocess.run(
            ["git", "fetch", "--no-tags", "--depth=1", "origin", REVISION],
            cwd=root,
            check=True,
        )
    if previous.exists():
        head = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=previous, text=True
        ).strip()
        if head != REVISION:
            raise RuntimeError(f"unexpected previous-reader worktree head: {head}")
    else:
        subprocess.run(
            ["git", "worktree", "add", "--detach", str(previous), REVISION],
            cwd=root,
            check=True,
        )
    prepare_previous_reader_build(previous)
    env = dict(
        os.environ,
        TZ="UTC0",
        ZIG_GLOBAL_CACHE_DIR=str(previous / "zig" / ".zig-global-cache"),
    )
    subprocess.run(
        [
            sys.executable,
            "tools/run_bounded_zig_build.py",
            "build",
            "capi",
            "-Dembedded-only=true",
            "-Dlite-local-inference-runtime=false",
            "-Dmetal=false",
            "-Dcuda=false",
            "-j1",
        ],
        cwd=previous / "zig",
        env=env,
        check=True,
    )
    extension = ".dylib" if sys.platform == "darwin" else ".so"
    library = previous / "zig" / "zig-out" / "lib" / f"libantfly{extension}"
    if not library.is_file():
        raise RuntimeError(f"previous-reader library missing: {library}")
    print(library)


if __name__ == "__main__":
    main()
