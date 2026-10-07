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

"""Release outputs whose selected build-graph consumers have all exited.

The caller must discard this invocation's entire local cache, including
manifests, before another build. Never apply this policy to a reusable cache.
"""

import argparse
import shutil
from pathlib import Path


def release(cache: Path, directories: list[Path]) -> int:
    cache = cache.resolve()
    if cache.name != "zig-local":
        raise ValueError("artifact release requires a disposable zig-local cache")
    outputs = cache / "o"
    directories = sorted({path.resolve() for path in directories})
    for path in directories:
        if (
            path.parent != outputs
            or len(path.name) != 32
            or any(c not in "0123456789abcdef" for c in path.name)
        ):
            raise ValueError(f"not a private compiler output directory: {path}")
    allocated = 0
    for path in directories:
        if not path.exists():
            continue
        for item in path.rglob("*"):
            if item.is_file() and not item.is_symlink():
                allocated += item.stat().st_blocks * 512
        shutil.rmtree(path)
    return allocated


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("directories", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        allocated = release(args.cache_dir, args.directories)
    except ValueError as error:
        parser.error(str(error))
    print(f"Released completed compiler outputs: {allocated / (1024 * 1024):.2f} MiB")


if __name__ == "__main__":
    main()
