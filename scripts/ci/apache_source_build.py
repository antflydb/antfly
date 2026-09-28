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

"""Build Apache products after removing ELv2 server implementations."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from license_headers import ROOT, group_for


def stage(root: Path, destination: Path) -> int:
    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=root)
    removed = 0
    for entry in tracked.decode().split("\0"):
        if not entry:
            continue
        source = root / entry
        if not source.is_file():
            continue
        # Keep the Zig build manifest and package metadata: only server
        # implementation sources are removed. The build must still resolve
        # the same package graph and link the same Lite/inference targets.
        if (
            entry.startswith("zig/pkg/antfly/src/")
            and not entry.startswith("zig/pkg/antfly/src/search/snowball/generated/")
            and source.suffix in {".zig", ".c", ".cpp", ".h"}
            and group_for(entry, "all") == "elv2"
        ):
            removed += 1
            continue
        if not entry.startswith(("zig/", "scripts/", "LICENSES/")) and entry not in {
            "LICENSE",
            "THIRD_PARTY_NOTICES.md",
        }:
            continue
        target = destination / entry
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target, follow_symlinks=False)
    if removed == 0:
        raise RuntimeError("no ELv2 server implementations removed from staged source")
    return removed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", type=Path, help="keep the staged source for inspection")
    args = parser.parse_args()
    if args.keep:
        stage_dir = args.keep.resolve()
        stage_dir.mkdir(parents=True, exist_ok=True)
        temporary = None
    else:
        temporary = tempfile.TemporaryDirectory(prefix="antfly-apache-source-")
        stage_dir = Path(temporary.name)
    try:
        removed = stage(ROOT, stage_dir)
        print(f"removed {removed} ELv2 server implementation files", flush=True)
        environment = os.environ.copy()
        environment.pop("ZIG_LOCAL_CACHE_DIR", None)
        subprocess.run(
            ["zig", "build", "lite", "-Doptimize=Debug", "-Dmetal=false", "-j1"],
            cwd=stage_dir / "zig",
            env=environment,
            check=True,
        )
        subprocess.run(
            ["zig", "build", "-Doptimize=Debug", "-Dmetal=false", "-j1"],
            cwd=stage_dir / "zig/pkg/inference",
            env=environment,
            check=True,
        )
    finally:
        if temporary:
            temporary.cleanup()


if __name__ == "__main__":
    main()
