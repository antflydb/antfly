#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
"""Audit the local engine's authored production imports, before its package move.

Named build modules are validated by the native/WASM builds. This check follows
literal relative imports, including imports inside otherwise lazy declarations;
only Zig test bodies are excluded. Missing sources fail rather than disappear
from the inventory. It deliberately does not infer licensing from directory names.
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path
import re

LITERALS = re.compile(
    r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|//[^\n]*|(?m:^[ \t]*\\\\[^\n]*)'
)
IMPORT = re.compile(r'@import\s*\(\s*"([^"\n]+)"\s*\)')
FORBIDDEN = ("raft/", "data/", "standalone/", "cmd/", "storage/hot_standby/")
SERVER_METADATA = {
    "api.zig",
    "server.zig",
    "table_provisioner.zig",
    "provision_contract.zig",
}


def mask_literals(source: str) -> str:
    return LITERALS.sub(
        lambda m: "".join("\n" if c == "\n" else " " for c in m[0]), source
    )


def production_imports(source: str) -> list[str]:
    """Skip comments, strings and complete test bodies, preserving lazy imports."""
    masked = mask_literals(source)
    stack: list[int] = []
    ends: dict[int, int] = {}
    for offset, char in enumerate(masked):
        if char == "{":
            stack.append(offset)
        elif char == "}":
            if not stack:
                raise ValueError("unbalanced Zig braces")
            ends[stack.pop()] = offset + 1
    if stack:
        raise ValueError("unbalanced Zig braces")
    excluded = []
    for test in re.finditer(r"\btest\s*\{", masked):
        brace = masked.index("{", test.start(), test.end())
        excluded.append((test.start(), ends[brace]))
    result = []
    for call in re.finditer(r"@import\b", masked):
        if any(start <= call.start() < end for start, end in excluded):
            continue
        match = IMPORT.match(source, call.start())
        if match is None:
            raise ValueError("production imports must declare a literal source owner")
        if match[1].endswith(".zig"):
            result.append(match[1])
    return result


def server_source(relative: str) -> bool:
    return (
        relative.startswith(FORBIDDEN)
        or (
            relative.startswith("metadata/")
            and (
                relative.removeprefix("metadata/") in SERVER_METADATA
                or relative.startswith("metadata/storage/")
            )
        )
        or relative == "system_catalog/server_call.zig"
    )


def audit(root: Path, entries: list[str]) -> dict[str, list[str]]:
    root = root.resolve()
    pending = collections.deque(root / entry for entry in entries)
    parents: dict[Path, Path | None] = {path: None for path in pending}
    graph: dict[str, list[str]] = {}
    while pending:
        path = pending.popleft()
        relative = path.relative_to(root).as_posix()
        if server_source(relative):
            chain = []
            cursor: Path | None = path
            while cursor is not None:
                chain.append(cursor.relative_to(root).as_posix())
                cursor = parents[cursor]
            raise ValueError(
                "engine imports server coordination: " + " -> ".join(reversed(chain))
            )
        dependencies = []
        for imported in production_imports(path.read_text()):
            dependency = (path.parent / imported).resolve()
            if not dependency.is_relative_to(root):
                raise ValueError(
                    f"{relative} imports outside its source owner: {imported}"
                )
            if not dependency.is_file():
                raise ValueError(f"{relative} imports missing source: {imported}")
            dependencies.append(dependency.relative_to(root).as_posix())
            if dependency not in parents:
                parents[dependency] = path
                pending.append(dependency)
        graph[relative] = dependencies
    return graph


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "pkg/antfly/src",
    )
    parser.add_argument("--entry", action="append")
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    try:
        graph = audit(
            args.root, args.entry or ["embedded_root.zig", "storage/db/db.zig"]
        )
    except (ValueError, OSError) as error:
        parser.exit(1, f"{error}\n")
    if args.json:
        args.json.write_text(json.dumps(graph, indent=2) + "\n")
    print(
        f"Embedded production boundary: {len(graph)} sources, no server coordination imports."
    )


if __name__ == "__main__":
    main()
