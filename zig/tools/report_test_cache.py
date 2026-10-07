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

"""Report retained compiler artifacts, including sparse-file disk allocation."""

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path


def report(cache: Path) -> dict:
    groups = defaultdict(lambda: {"files": 0, "logical_bytes": 0, "allocated_bytes": 0})
    largest = []
    seen = set()
    for directory, _, names in os.walk(cache, followlinks=False):
        for name in names:
            path = Path(directory) / name
            if path.is_symlink():
                continue
            stat = path.stat()
            identity = (stat.st_dev, stat.st_ino)
            if identity in seen:
                continue
            seen.add(identity)
            if name.endswith(".a"):
                kind = "static archives"
            elif name.endswith(".o"):
                kind = "object files"
            elif name.endswith((".so", ".dylib")) or ".so." in name:
                kind = "shared libraries"
            elif stat.st_mode & 0o111:
                kind = "executables"
            else:
                kind = "other"
            allocated = stat.st_blocks * 512
            group = groups[kind]
            group["files"] += 1
            group["logical_bytes"] += stat.st_size
            group["allocated_bytes"] += allocated
            largest.append(
                {
                    "path": str(path.relative_to(cache)),
                    "logical_bytes": stat.st_size,
                    "allocated_bytes": allocated,
                }
            )
    return {
        "cache": str(cache),
        "allocated_bytes": sum(group["allocated_bytes"] for group in groups.values()),
        "groups": dict(sorted(groups.items())),
        "largest": sorted(
            largest, key=lambda item: item["allocated_bytes"], reverse=True
        )[:20],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cache", type=Path)
    args = parser.parse_args()
    if not args.cache.is_dir():
        parser.error(f"compiler cache does not exist: {args.cache}")
    print(json.dumps(report(args.cache), indent=2))


if __name__ == "__main__":
    main()
