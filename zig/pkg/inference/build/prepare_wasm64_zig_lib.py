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

"""Prepare a cache-owned correction for Zig 0.17.0 WASM64 soft-float comparisons."""

from __future__ import annotations

import argparse
import ast
import errno
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile

OLD = "else if (builtin.cpu.arch.isAARCH64())\n    i32"
NEW = "else if (builtin.cpu.arch.isAARCH64() or builtin.cpu.arch == .wasm64)\n    i32"


def patch_compare_source(source: str) -> str:
    if source.count(OLD) != 1:
        raise RuntimeError(
            "unrecognized Zig 0.17.0 compiler_rt comparison ABI; revalidate the toolchain"
        )
    return source.replace(OLD, NEW, 1)


def clone_file(source: str, destination: str) -> str:
    # One cache-owned overlay per build graph. Hard links avoid duplicating the
    # toolchain on the same volume; cross-volume runners copy only once.
    try:
        os.link(source, destination)
    except OSError as error:
        if error.errno not in (errno.EXDEV, errno.EPERM, errno.EACCES):
            raise
        shutil.copy2(source, destination)
    return destination


def prepare(zig: str, destination: Path, comparison_source: Path) -> None:
    version = subprocess.check_output([zig, "version"], text=True).strip()
    if version != "0.17.0":
        raise RuntimeError(
            f"WASM64 ABI correction is only validated for Zig 0.17.0, got {version}"
        )
    environment = subprocess.check_output([zig, "env"], text=True)
    match = re.search(
        r'^\s*\.lib_dir\s*=\s*("(?:[^"\\]|\\.)*"),\s*$', environment, re.MULTILINE
    )
    if match is None:
        raise RuntimeError("missing Zig library directory")
    source = Path(ast.literal_eval(match[1])).resolve()
    if comparison_source.resolve() != (source / "compiler_rt/comparef.zig").resolve():
        raise RuntimeError(
            "comparison source must belong to the selected Zig installation"
        )
    corrected = patch_compare_source((source / "compiler_rt/comparef.zig").read_text())
    destination = destination.resolve()
    if destination == source or source in destination.parents:
        raise RuntimeError("the ABI overlay must not modify the installed toolchain")
    if destination.exists() and any(destination.iterdir()):
        raise RuntimeError("the ABI overlay destination must be empty")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="wasm64-lib-", dir=destination.parent
    ) as staging:
        staged = Path(staging) / "lib"
        shutil.copytree(source, staged, copy_function=clone_file)
        comparison = staged / "compiler_rt/comparef.zig"
        # copytree preserves read-only Nix directory modes. Directories are
        # cache-owned copies, so grant write access to the patch parent;
        # chmod on a hard-linked file would alter the installed toolchain.
        comparison.parent.chmod(comparison.parent.stat().st_mode | stat.S_IWUSR)
        # Break the hard link BEFORE writing: the installed library is immutable.
        comparison.unlink()
        comparison.write_text(corrected)
        if destination.exists():
            destination.rmdir()
        # macOS also requires a writable directory when moving it between
        # parents. This changes only the copied library root.
        staged.chmod(staged.stat().st_mode | stat.S_IWUSR)
        staged.rename(destination)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare a cache-owned Zig 0.17.0 WASM64 compiler-runtime ABI correction"
    )
    parser.add_argument("--zig", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--comparison-source", required=True, type=Path)
    args = parser.parse_args()
    prepare(args.zig, args.output, args.comparison_source)


if __name__ == "__main__":
    main()
