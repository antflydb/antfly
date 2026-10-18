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

import os
from pathlib import Path
import subprocess
import sys

# Last main before resolver execution ownership / retired identities in #1114.
REVISION = "e8666a5b758e1e588a6b2b74b260212e984b40a9"


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    previous = root / ".worktrees" / "reader-baseline-e8666a5b75"
    if subprocess.run(
        ["git", "cat-file", "-e", f"{REVISION}^{{commit}}"],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode:
        subprocess.run(["git", "fetch", "origin", REVISION], cwd=root, check=True)
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
