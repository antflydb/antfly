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

"""Copy-on-write source overlays and compiler-state parsing for cache checks."""

from pathlib import Path
import re
import shutil


def link_children(source, destination):
    destination.mkdir()
    for child in source.iterdir():
        if child.name not in {
            ".git",
            ".worktrees",
            ".zig-cache",
            ".zig-global-cache",
            "zig-out",
        }:
            (destination / child.name).symlink_to(
                child, target_is_directory=child.is_dir()
            )


def own(root, relative):
    path = root
    for part in Path(relative).parts[:-1]:
        path /= part
        if path.is_symlink():
            original = path.resolve()
            path.unlink()
            link_children(original, path)
    path = root / relative
    if path.is_symlink():
        original = path.resolve()
        path.unlink()
        shutil.copyfile(original, path)
    return path


def compile_state(output, kind, name):
    match = re.search(
        rf"compile {kind} {re.escape(name)} debug \S+ (cached|success)\b", output
    )
    if not match:
        raise RuntimeError(f"missing compiler result for {name}; inspect retained log")
    return match[1]
