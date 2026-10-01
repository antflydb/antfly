#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Release disposable Zig test link inputs between sequential CI build phases.

Call only when no build or test using this job-local cache is running. Keep
executables, generated sources, tools, small objects, and global dependencies.
Zig can report a cache hit without checking that its executable still exists;
removing an output directory leaves a live manifest pointing at a missing binary.
The default mode removes only completed test link inputs. A cache miss
recompiles them. --release-phase instead retires the entire private zig-local
cache, including manifests, after every user of that phase's cache has exited.
"""

import argparse
import os
import re
import shutil
from pathlib import Path


def prune(cache: Path, min_bytes: int = 64 * 1024 * 1024) -> int:
    outputs = cache / "o"
    if outputs.is_symlink():
        raise ValueError("cache output directory must not be a symlink")
    if not outputs.exists():
        return 0
    removed = 0
    for artifact in outputs.iterdir():
        if (
            artifact.is_symlink()
            or not artifact.is_dir()
            or not re.fullmatch(r"[0-9a-f]{32}", artifact.name)
        ):
            continue
        for output in artifact.iterdir():
            name = output.name.removesuffix(".exe")
            if not (
                (name == "test" or name.endswith("-tests"))
                and not output.is_symlink()
                and output.is_file()
                and os.access(output, os.X_OK)
            ):
                continue
            # Zig 0.16 names its completed compilation-unit link input _zcu.o;
            # older builds use .o. Keep the final executable and any unknown
            # siblings, including debug information and generator outputs.
            pruned = False
            for suffix in ("_zcu.o", ".o", "_zcu.obj", ".obj"):
                link_input = artifact / (name + suffix)
                if (
                    not link_input.is_symlink()
                    and link_input.is_file()
                    and link_input.stat().st_size >= min_bytes
                ):
                    link_input.unlink()
                    pruned = True
            removed += int(pruned)
    return removed


def release_completed_phase(cache: Path) -> None:
    """Release a quiescent CI phase's complete, private compiler cache.

    Self-hosted Debug emits only executables, so object-only pruning cannot
    bound disk use across phases. Remove manifests with outputs: retaining a
    manifest after removing its executable produces a false Zig cache hit.
    Global dependency caches and installed zig-out artifacts are outside this
    directory and remain available to subsequent phases.
    """
    cache = cache.absolute()
    if cache.name != "zig-local" or cache.resolve() != cache:
        raise ValueError("phase release requires a real job-owned zig-local directory")
    if cache.exists():
        shutil.rmtree(cache)
    cache.mkdir(parents=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cache", type=Path)
    parser.add_argument("--release-phase", action="store_true")
    args = parser.parse_args()
    if args.release_phase:
        release_completed_phase(args.cache)
        print("Released completed phase compiler cache")
    else:
        print(f"Pruned link inputs from {prune(args.cache)} completed test artifacts")
