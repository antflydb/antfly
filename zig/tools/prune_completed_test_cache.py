#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Release completed Zig test artifacts between sequential CI build phases.

Call only when no build or test using this job-local cache is running. Keep
compiler intermediates, generated sources, tools, small tests, and global dependencies.
"""

import argparse
import os
from pathlib import Path
import re
import shutil


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
            if (name == "test" or name.endswith("-tests")) and (
                not output.is_symlink()
                and output.is_file()
                and os.access(output, os.X_OK)
                and output.stat().st_size >= min_bytes
            ):
                shutil.rmtree(artifact)
                removed += 1
                break
    return removed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cache", type=Path)
    args = parser.parse_args()
    print(f"Removed {prune(args.cache)} completed test artifact directories")
